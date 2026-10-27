//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/tiered_storage.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <sstream>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
IOStatus IO(const Status& s) {
  if (s.ok()) return IOStatus::OK();
  if (s.IsNotFound()) return IOStatus::NotFound(s.ToString());
  if (s.IsCorruption()) return IOStatus::Corruption(s.ToString());
  if (s.IsInvalidArgument()) return IOStatus::InvalidArgument(s.ToString());
  if (s.IsAborted()) return IOStatus::Aborted(s.ToString());
  return IOStatus::IOError(s.ToString());
}
uint64_t Micros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
bool SegmentName(const std::string& name, uint64_t* number, uint64_t* id) {
  if (name.compare(0, 8, "segment-") != 0) return false;
  std::istringstream in(name.substr(8));
  char dash;
  return (in >> *number >> dash >> *id) && dash == '-' && in.eof() &&
         *id != UINT64_MAX &&
         name ==
             "segment-" + std::to_string(*number) + "-" + std::to_string(*id);
}
}  // namespace

class TierReader : public FSRandomAccessFile {
 public:
  TierReader(TieredStorage* storage, uint64_t number)
      : storage_(storage), number_(number) {}
  IOStatus Read(uint64_t offset, size_t size, const IOOptions& o, Slice* out,
                char* scratch, IODebugContext* d) const override {
    return storage_->ReadBlob(number_, offset, size, o, out, scratch, d);
  }
  IOStatus GetFileSize(uint64_t* size) override {
    return storage_->GetFileSize(storage_->Descriptor(number_), IOOptions(),
                                 size, nullptr);
  }

 private:
  TieredStorage* storage_;
  uint64_t number_;
};
class TierSequential : public FSSequentialFile {
 public:
  explicit TierSequential(std::unique_ptr<FSRandomAccessFile> file)
      : file_(std::move(file)) {}
  IOStatus Read(size_t n, const IOOptions& o, Slice* r, char* b,
                IODebugContext* d) override {
    IOStatus s = file_->Read(offset_, n, o, r, b, d);
    if (s.ok()) offset_ += r->size();
    return s;
  }
  IOStatus Skip(uint64_t n) override {
    uint64_t size;
    IOStatus s = file_->GetFileSize(&size);
    if (s.ok()) offset_ += std::min(n, size - std::min(size, offset_));
    return s;
  }

 private:
  std::unique_ptr<FSRandomAccessFile> file_;
  uint64_t offset_ = 0;
};

class TierWriter : public FSWritableFile {
 public:
  TierWriter(TieredStorage* s, uint64_t n) : storage_(s), number_(n) {}
  IOStatus Append(const Slice& bytes, const IOOptions& o,
                  IODebugContext* d) override {
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    auto blob = pin.blob;
    const auto append = [&]() -> Status {
      std::lock_guard<std::mutex> writer(blob->append);
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        if (blob->closing || blob->live.closed)
          return Status::IOError("closed tier writer");
        if (bytes.size() > UINT64_MAX - blob->live.size)
          return Status::InvalidArgument("tier blob length overflow");
      }
      size_t offset = 0;
      while (offset < bytes.size()) {
        auto& active = blob->caller;
        size_t take = std::min<uint64_t>(bytes.size() - offset,
                                         TieredStorage::kChunkSize);
        if (active.file)
          take = std::min<uint64_t>(
              take, TieredStorage::kExtentSize - active.extent.length);
        bool local;
        {
          std::lock_guard<std::mutex> lock(storage_->mutex_);
          if (!storage_->error_.ok()) return storage_->error_;
          if (storage_->stopping_) return Status::ShutdownInProgress();
          local = !blob->live.spilled;
          if (local) {
            const auto start = Micros();
            const uint64_t available = storage_->options_.staging_capacity -
                                       storage_->usage_ - storage_->reserved_;
            local = take <= available;
            storage_->quota_us_ += Micros() - start;
            if (local) {
              storage_->reserved_ += take;
              storage_->peak_ = std::max(
                  storage_->peak_, storage_->usage_ + storage_->reserved_);
            }
          }
        }
        if (!local) {
          bool spilled;
          {
            std::lock_guard<std::mutex> lock(storage_->mutex_);
            spilled = blob->live.spilled;
          }
          if (!spilled) {
            Status st = storage_->Freeze(number_, blob);
            if (!st.ok()) return st;
            st = storage_->Commit(number_, blob, [](TieredStorage::Version* v) {
              v->spilled = true;
              v->boundary = v->size;
              return Status::OK();
            });
            if (!st.ok()) return st;
            {
              std::lock_guard<std::mutex> lock(storage_->mutex_);
              ++storage_->spill_files_;
            }
            TEST_SYNC_POINT("MetaBypass::TierSuffixRegistered");
          }
        }
        if (local) TEST_SYNC_POINT("MetaBypass::TierQuotaReserved");
        Status st;
        if (!active.file) st = storage_->Register(number_, blob, local);
        const auto started = Micros();
        const bool attempted = st.ok();
        if (st.ok())
          st = active.file->Append(Slice(bytes.data() + offset, take), o, d);
        if (st.ok()) st = active.file->Flush(o, d);
        {
          std::lock_guard<std::mutex> lock(storage_->mutex_);
          if (local) {
            storage_->reserved_ -= take;
            // Failed Append may have written any prefix; retain all admission.
            storage_->usage_ += take;
            storage_->prefix_write_us_ += Micros() - started;
          } else {
            storage_->suffix_write_us_ += Micros() - started;
            if (attempted) ++storage_->suffix_appends_;
          }
          if (st.ok()) {
            active.crc =
                crc32c::Extend(active.crc, bytes.data() + offset, take);
            active.extent.length += take;
            blob->live.extents.back().length = active.extent.length;
            blob->live.size += take;
            if (!local) storage_->spill_bytes_ += take;
          }
        }
        if (!st.ok()) return st;
        offset += take;
        TEST_SYNC_POINT("MetaBypass::TierAppendVisible");
        if (active.extent.length == TieredStorage::kExtentSize) {
          st = storage_->Freeze(number_, blob);
          if (!st.ok()) return st;
        }
      }
      return Status::OK();
    };
    s = append();
    if (!s.ok() && !s.IsAborted()) storage_->Fail(s);
    return IO(s);
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo&, IODebugContext* d) override {
    return Append(b, o, d);
  }
  IOStatus PositionedAppend(const Slice&, uint64_t, const IOOptions&,
                            IODebugContext*) override {
    return IOStatus::NotSupported("tiered positioned append");
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t n, const IOOptions& o,
                            const DataVerificationInfo&,
                            IODebugContext* d) override {
    return PositionedAppend(b, n, o, d);
  }
  IOStatus Truncate(uint64_t n, const IOOptions& o,
                    IODebugContext* d) override {
    return n == GetFileSize(o, d) ? IOStatus::OK()
                                  : IOStatus::NotSupported("tiered truncation");
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    {
      std::lock_guard<std::mutex> writer(pin.blob->append);
      if (pin.blob->caller.file) s = pin.blob->caller.file->Flush(o, d);
    }
    if (!s.ok() && !s.IsAborted()) storage_->Fail(s);
    return IO(s);
  }
  IOStatus Sync(const IOOptions&, IODebugContext*) override {
    return Finish(false);
  }
  IOStatus Fsync(const IOOptions&, IODebugContext*) override {
    return Finish(false);
  }
  IOStatus Close(const IOOptions&, IODebugContext*) override {
    return Finish(true);
  }
  bool IsSyncThreadSafe() const override { return false; }
  uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    auto it = storage_->blobs_.find(number_);
    return it == storage_->blobs_.end() ? 0 : it->second->live.size;
  }
  void PrepareWrite(size_t, size_t, const IOOptions&,
                    IODebugContext*) override {}
  IOStatus Allocate(uint64_t, uint64_t, const IOOptions&,
                    IODebugContext*) override {
    return IOStatus::OK();
  }
  IOStatus RangeSync(uint64_t, uint64_t, const IOOptions&,
                     IODebugContext*) override {
    return IOStatus::OK();
  }

 private:
  IOStatus Finish(bool close) {
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    auto blob = pin.blob;
    TEST_SYNC_POINT("MetaBypass::TierSyncPinned");
    uint64_t length = 0;
    bool spilled = false;
    {
      std::lock_guard<std::mutex> writer(blob->append);
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        if (close) blob->closing = true;
        length = blob->live.size;
        spilled = blob->live.spilled;
      }
      s = storage_->Freeze(number_, blob);
      if (s.ok() && close)
        s = storage_->Commit(number_, blob, [](TieredStorage::Version* v) {
          v->closed = true;
          return Status::OK();
        });
    }
    // Never wait for the worker while holding append or commit.
    if (s.ok() && spilled) s = storage_->Wait(number_, blob, length, close);
    if (!s.ok() && !s.IsAborted()) storage_->Fail(s);
    return IO(s);
  }
  TieredStorage* storage_;
  uint64_t number_;
};

TieredStorage::TieredStorage(std::shared_ptr<FileSystem> fs,
                             const MetaBypassOptions& o)
    : FileSystemWrapper(std::move(fs)), options_(o) {}
TieredStorage::~TieredStorage() {
  Stop().PermitUncheckedError();
  if (staging_lock_)
    target()
        ->UnlockFile(staging_lock_, IOOptions(), nullptr)
        .PermitUncheckedError();
}
std::string TieredStorage::Descriptor(uint64_t n) const {
  return BlobFileName(options_.data_dir, n);
}
std::string TieredStorage::Path(const Extent& e) const {
  return (e.local ? options_.staging_dir : options_.data_dir) + "/" + e.name;
}
std::string TieredStorage::NewName(uint64_t n) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (extent_id_ >= UINT64_MAX - 1) return {};
  return "segment-" + std::to_string(n) + "-" + std::to_string(++extent_id_);
}
bool TieredStorage::IsBlob(const std::string& p, uint64_t* number) const {
  if (p.compare(0, options_.data_dir.size() + 1, options_.data_dir + "/") != 0)
    return false;
  uint64_t n;
  FileType t;
  if (!ParseFileName(p.substr(options_.data_dir.size() + 1), &n, &t) ||
      t != kBlobFile)
    return false;
  if (number) *number = n;
  return true;
}
TieredStorage::Pin::~Pin() {
  if (!storage_) return;
  std::lock_guard<std::mutex> lock(storage_->mutex_);
  for (const auto& name : names_) {
    auto it = storage_->readers_.find(name);
    if (--it->second == 0) storage_->readers_.erase(it);
  }
  --blob->operations;
  storage_->progress_.notify_all();
  storage_->work_.notify_one();
}
Status TieredStorage::Pin::Open(TieredStorage* storage, uint64_t n) {
  std::lock_guard<std::mutex> lock(storage->mutex_);
  if (!storage->error_.ok()) return storage->error_;
  if (storage->stopping_) return Status::ShutdownInProgress();
  auto it = storage->blobs_.find(n);
  if (it == storage->blobs_.end() || it->second->retiring)
    return Status::NotFound("tiered blob");
  storage_ = storage;
  blob = it->second;
  ++blob->operations;
  return Status::OK();
}
void TieredStorage::Pin::Ranges(const Version& version) {
  for (const auto& e : version.extents) {
    ++storage_->readers_[e.name];
    names_.push_back(e.name);
  }
}
uint64_t TieredStorage::DurableEnd(const Version& v, bool hdd) {
  uint64_t end = 0;
  for (const auto& e : v.extents) {
    if (e.start != end || (hdd && (e.local || e.open))) break;
    end += e.durable;
    if (e.durable != e.length) break;
  }
  return end;
}
bool TieredStorage::Complete(const Version& v) {
  return v.closed && DurableEnd(v, true) == v.size &&
         std::all_of(v.extents.begin(), v.extents.end(),
                     [](const Extent& e) { return !e.local && !e.open; });
}
bool TieredStorage::Equal(const Version& a, const Version& b) {
  if (a.size != b.size || a.closed != b.closed || a.spilled != b.spilled ||
      a.boundary != b.boundary || a.extents.size() != b.extents.size())
    return false;
  return std::equal(a.extents.begin(), a.extents.end(), b.extents.begin(),
                    [](const Extent& x, const Extent& y) {
                      return x.start == y.start && x.length == y.length &&
                             x.durable == y.durable && x.name == y.name &&
                             x.local == y.local && x.open == y.open &&
                             x.crc == y.crc;
                    });
}
std::string TieredStorage::Encode(const Version& v) {
  std::ostringstream out;
  out << v.generation << ' ' << v.size << ' ' << v.closed << ' ' << v.spilled
      << ' ' << v.boundary << ' ' << v.extents.size() << '\n';
  for (const auto& e : v.extents)
    out << e.start << ' ' << e.length << ' ' << e.durable << ' ' << kExtentSize
        << ' ' << e.local << ' ' << e.open << ' ' << e.crc << ' ' << e.name
        << '\n';
  const auto bytes = out.str();
  return "MBT3 " + std::to_string(crc32c::Value(bytes.data(), bytes.size())) +
         "\n" + bytes;
}
Status TieredStorage::Decode(const std::string& bytes, Version* v) {
  const auto newline = bytes.find('\n');
  if (newline == std::string::npos) return Status::Corruption("tier header");
  std::istringstream header(bytes.substr(0, newline));
  std::string magic, extra;
  uint32_t crc;
  if (!(header >> magic >> crc) || magic != "MBT3" || (header >> extra))
    return Status::Corruption("tier descriptor version");
  const auto body = bytes.substr(newline + 1);
  if (crc32c::Value(body.data(), body.size()) != crc)
    return Status::Corruption("tier descriptor checksum");
  std::istringstream in(body);
  unsigned closed, spilled;
  uint64_t count;
  if (!(in >> v->generation >> v->size >> closed >> spilled >> v->boundary >>
        count) ||
      closed > 1 || spilled > 1 || count > body.size() ||
      v->boundary > v->size || (!spilled && v->boundary != 0))
    return Status::Corruption("tier descriptor state");
  v->closed = closed != 0;
  v->spilled = spilled != 0;
  v->extents.clear();
  std::set<std::string> names;
  uint64_t end = 0;
  for (uint64_t i = 0; i < count; ++i) {
    Extent e;
    uint64_t capacity, number, id;
    unsigned local, open;
    if (!(in >> e.start >> e.length >> e.durable >> capacity >> local >> open >>
          e.crc >> e.name) ||
        capacity != kExtentSize || e.start != end || e.length > kExtentSize ||
        e.length > v->size - end || e.durable > e.length || local > 1 ||
        open > 1 || !SegmentName(e.name, &number, &id) ||
        !names.insert(e.name).second || (open && (i + 1 != count || closed)) ||
        (!open && e.durable != e.length))
      return Status::Corruption("tier range");
    e.local = local != 0;
    e.open = open != 0;
    if (e.local && spilled && e.start + e.length > v->boundary)
      return Status::Corruption("SSD range after spill boundary");
    end += e.length;
    v->extents.push_back(std::move(e));
  }
  if ((in >> extra) || end != v->size)
    return Status::Corruption("tier range coverage");
  return Status::OK();
}

Status TieredStorage::SyncCommitDirectory() {
  const auto start = Micros();
  Status s = SyncDir(target(), options_.data_dir);
  std::lock_guard<std::mutex> lock(mutex_);
  ++commit_dir_syncs_;
  commit_dir_sync_us_ += Micros() - start;
  return s;
}
Status TieredStorage::Commit(uint64_t n, const std::shared_ptr<Blob>& blob,
                             const Patch& patch) {
  const auto start = Micros();
  std::unique_lock<std::mutex> commit(blob->commit);
  Version next;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!error_.ok()) return error_;
    if (blob->retiring) return Status::Aborted("retiring tier blob");
    next = blob->durable;
  }
  Status s = patch(&next);
  if (!s.ok()) return s;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const bool pending = std::any_of(
        blob->live.extents.begin(), blob->live.extents.end(),
        [](const Extent& e) { return e.open && e.length > e.durable; });
    if (!pending && Equal(next, blob->durable)) {
      ++commit_skips_;
      metadata_us_ += Micros() - start;
      return Status::OK();
    }
  }
  if (next.generation == UINT64_MAX)
    return Status::Corruption("tier generation overflow");
  ++next.generation;
  // All payload sync/readback is already complete. Serialize only metadata.
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDependenciesSynced", &s);
  if (s.ok()) s = Write(target(), Descriptor(n) + ".tmp", Encode(next));
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorSynced", &s);
  if (s.ok())
    s = target()->RenameFile(Descriptor(n) + ".tmp", Descriptor(n), IOOptions(),
                             nullptr);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorRenamed", &s);
  if (s.ok()) s = SyncCommitDirectory();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierPublicationSynced", &s);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Apply the same identity-checked delta, preserving unrelated live tails.
    s = patch(&blob->live);
    if (s.ok()) {
      blob->live.generation = next.generation;
      blob->durable = std::move(next);
      progress_.notify_all();
      work_.notify_one();
    }
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    metadata_us_ += Micros() - start;
  }
  if (s.ok()) TEST_SYNC_POINT("MetaBypass::TierRouteDurable");
  return s;
}
Status TieredStorage::Register(uint64_t n, const std::shared_ptr<Blob>& blob,
                               bool local) {
  ActiveExtent active;
  active.extent.local = local;
  active.extent.name = NewName(n);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    active.extent.start = blob->live.size;
  }
  if (active.extent.name.empty())
    return Status::Corruption("tier extent id overflow");
  FileOptions options = blob->file_options;
  options.use_direct_writes = false;
  Status s = target()->NewWritableFile(Path(active.extent), options,
                                       &active.file, nullptr);
  // Register an existing durable empty identity, never a future path.
  if (s.ok()) s = active.file->Fsync(IOOptions(), nullptr);
  if (s.ok())
    s = local ? SyncDir(target(), options_.staging_dir) : SyncCommitDirectory();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierBeforeTailRegister", &s);
  const auto extent = active.extent;
  if (s.ok())
    s = Commit(n, blob, [extent](Version* v) {
      if (v->size != extent.start ||
          (!v->extents.empty() && v->extents.back().open))
        return Status::Corruption("tail registration overlaps");
      v->extents.push_back(extent);
      return Status::OK();
    });
  if (s.ok()) {
    blob->caller = std::move(active);
    TEST_SYNC_POINT("MetaBypass::TierTailRegistered");
  } else if (active.file) {
    // Registration may have renamed before its final directory sync failed.
    // Keep the file even when the publication result is uncertain.
    s.UpdateIfOk(active.file->Close(IOOptions(), nullptr));
  }
  return s;
}
Status TieredStorage::Seal(ActiveExtent* active) {
  const auto start = Micros();
  Status s = active->file->Fsync(IOOptions(), nullptr);
  if (!active->extent.local) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++extent_fsyncs_;
    extent_fsync_us_ += Micros() - start;
  }
  uint32_t actual = 0;
  if (s.ok() && !active->extent.local)
    s = Digest(target(), Path(active->extent), active->extent.length, &actual);
  if (s.ok() && !active->extent.local && actual != active->crc)
    s = Status::Corruption("HDD extent checksum");
  if (s.ok()) {
    s = active->file->Close(IOOptions(), nullptr);
    if (s.ok()) active->file.reset();
  }
  if (s.ok()) {
    active->extent.open = false;
    active->extent.durable = active->extent.length;
    active->extent.crc = active->crc;
    active->extent.frozen_at = Micros();
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierExtentSynced", &s);
  return s;
}
Status TieredStorage::Freeze(uint64_t n, const std::shared_ptr<Blob>& blob) {
  auto& active = blob->caller;
  if (!active.file) return Status::OK();
  Status s = Seal(&active);
  if (!s.ok()) return s;
  const auto extent = active.extent;
  s = Commit(n, blob, [extent](Version* v) {
    if (v->extents.empty() || v->extents.back().name != extent.name ||
        !v->extents.back().open)
      return Status::Corruption("freeze lost tail identity");
    v->extents.back() = extent;
    v->size = extent.start + extent.length;
    return Status::OK();
  });
  if (s.ok()) {
    active = ActiveExtent();
    TEST_SYNC_POINT("MetaBypass::TierSegmentFrozen");
  }
  return s;
}
Status TieredStorage::Initialize(bool restore) {
  restoring_ = restore;
  std::vector<std::string> data_names, local_names;
  Status s = target()->GetChildren(options_.data_dir, IOOptions(), &data_names,
                                   nullptr);
  if (!s.ok()) return s;
  s = target()->GetChildren(options_.staging_dir, IOOptions(), &local_names,
                            nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  if (!s.ok()) return s;
  std::set<std::string> referenced_local;
  for (const auto& name : data_names) {
    uint64_t n, id;
    if (SegmentName(name, &n, &id)) extent_id_ = std::max(extent_id_, id);
    if (!IsBlob(options_.data_dir + "/" + name, &n)) continue;
    std::string bytes;
    s = Read(target(), Descriptor(n), &bytes);
    if (!s.ok()) return s;
    auto blob = std::make_shared<Blob>();
    s = Decode(bytes, &blob->durable);
    if (!s.ok()) return s;
    if (restore) continue;
    blob->live = blob->durable;
    for (auto& e : blob->live.extents) {
      uint64_t owner, segment, size;
      if (!SegmentName(e.name, &owner, &segment) || owner != n)
        return Status::Corruption("tier range owner");
      s = target()->GetFileSize(Path(e), IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size < e.durable || size > kExtentSize ||
          (!e.open && size != e.length))
        return Status::Corruption("tier physical range length");
      if (restore && e.local)
        return Status::InvalidArgument("restore has SSD dependency");
      if (e.open) {
        // Actual bytes are process-crash candidates, NOT durable evidence.
        e.length = size;
        blob->live.size = e.start + size;
      }
      if (e.local) referenced_local.insert(e.name);
    }
    blobs_.emplace(n, std::move(blob));
  }
  for (const auto& name : local_names) {
    uint64_t n, id;
    FileType type;
    if (ParseFileName(name, &n, &type) && type == kBlobFile)
      return Status::Corruption("legacy raw staging layout");
    if (!SegmentName(name, &n, &id)) continue;
    if (restore)
      return Status::InvalidArgument("restore requires empty staging");
    extent_id_ = std::max(extent_id_, id);
    uint64_t size;
    s = target()->GetFileSize(options_.staging_dir + "/" + name, IOOptions(),
                              &size, nullptr);
    if (!s.ok()) return s;
    if (size > options_.staging_capacity - usage_)
      return Status::InvalidArgument("existing staging exceeds capacity");
    usage_ += size;
    // All current descriptors were decoded above. Checkpoints contain HDD
    // dependencies only, so an unreferenced SSD identity is safe to retire.
    if (!referenced_local.count(name)) retired_local_[name] = size;
  }
  peak_ = usage_;
  // No mutation precedes complete format/dependency validation.
  s = EnsureDir(target(), options_.staging_dir);
  if (s.ok())
    s = target()->LockFile(options_.staging_dir + "/METABYPASS-LOCK",
                           IOOptions(), &staging_lock_, nullptr);
  if (!s.ok()) return s;
  for (const auto& item : blobs_) {
    auto blob = item.second;
    if (!blob->live.extents.empty() && blob->live.extents.back().open) {
      auto& active = blob->caller;
      active.extent = blob->live.extents.back();
      s = Digest(target(), Path(active.extent), active.extent.length,
                 &active.crc);
      if (s.ok())
        s = target()->ReopenWritableFile(Path(active.extent), FileOptions(),
                                         &active.file, nullptr);
      if (s.ok()) s = Freeze(item.first, blob);
      if (!s.ok()) return s;
    }
    s = Commit(item.first, blob, [](Version* v) {
      v->closed = true;
      return Status::OK();
    });
    if (!s.ok()) return s;
  }
  return Status::OK();
}
void TieredStorage::Start() {
  worker_ = std::thread([this] { Run(); });
}
Status TieredStorage::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    work_.notify_all();
    progress_.notify_all();
  }
  if (worker_.joinable()) worker_.join();
  std::unique_lock<std::mutex> lock(mutex_);
  progress_.wait(lock, [&] {
    for (const auto& item : blobs_)
      if (item.second->operations) return false;
    return true;
  });
  lock.unlock();
  for (const auto& item : blobs_) {
    for (auto* active : {&item.second->caller, &item.second->migration}) {
      if (!active->file) continue;
      Status s = active->file->Close(IOOptions(), nullptr);
      active->file.reset();
      if (!s.ok()) Fail(s);
    }
  }
  return Error();
}
Status TieredStorage::Error() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return error_;
}
void TieredStorage::SetFailureHandler(std::function<void(const Status&)> f) {
  std::lock_guard<std::mutex> lock(mutex_);
  failure_ = std::move(f);
}
void TieredStorage::Cancel(const Status& s) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (error_.ok()) error_ = s;
  progress_.notify_all();
  work_.notify_all();
}
void TieredStorage::Fail(const Status& s) {
  std::function<void(const Status&)> f;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error_.ok()) error_ = s;
    f = failure_;
    progress_.notify_all();
    work_.notify_all();
  }
  if (f) f(s);
}

IOStatus TieredStorage::NewWritableFile(const std::string& p,
                                        const FileOptions& o,
                                        std::unique_ptr<FSWritableFile>* out,
                                        IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->NewWritableFile(p, o, out, d);
  std::lock_guard<std::mutex> lock(mutex_);
  if (!error_.ok()) return IO(error_);
  if (stopping_) return IO(Status::ShutdownInProgress());
  if (blobs_.count(n)) return IOStatus::Corruption("tiered blob number reused");
  blobs_[n] = std::make_shared<Blob>();
  blobs_[n]->file_options = o;
  out->reset(new TierWriter(this, n));
  return IOStatus::OK();
}
IOStatus TieredStorage::ReopenWritableFile(const std::string& p,
                                           const FileOptions& o,
                                           std::unique_ptr<FSWritableFile>* out,
                                           IODebugContext* d) {
  return IsBlob(p) ? IOStatus::NotSupported("tiered blob reopen")
                   : target()->ReopenWritableFile(p, o, out, d);
}
IOStatus TieredStorage::NewRandomAccessFile(
    const std::string& p, const FileOptions& o,
    std::unique_ptr<FSRandomAccessFile>* out, IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->NewRandomAccessFile(p, o, out, d);
  uint64_t size;
  IOStatus s = GetFileSize(p, IOOptions(), &size, d);
  if (s.ok()) out->reset(new TierReader(this, n));
  return s;
}
IOStatus TieredStorage::NewSequentialFile(
    const std::string& p, const FileOptions& o,
    std::unique_ptr<FSSequentialFile>* out, IODebugContext* d) {
  if (!IsBlob(p)) return target()->NewSequentialFile(p, o, out, d);
  std::unique_ptr<FSRandomAccessFile> file;
  IOStatus s = NewRandomAccessFile(p, o, &file, d);
  if (s.ok()) out->reset(new TierSequential(std::move(file)));
  return s;
}
IOStatus TieredStorage::GetFileSize(const std::string& p, const IOOptions& o,
                                    uint64_t* size, IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->GetFileSize(p, o, size, d);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = blobs_.find(n);
  if (it == blobs_.end() || it->second->retiring)
    return IOStatus::PathNotFound(p);
  *size = it->second->live.size;
  return IOStatus::OK();
}
IOStatus TieredStorage::FileExists(const std::string& p, const IOOptions& o,
                                   IODebugContext* d) {
  if (!IsBlob(p)) return target()->FileExists(p, o, d);
  uint64_t size;
  IOStatus s = GetFileSize(p, o, &size, d);
  return s.IsPathNotFound() ? IOStatus::NotFound(p) : s;
}
IOStatus TieredStorage::GetChildren(const std::string& p, const IOOptions& o,
                                    std::vector<std::string>* out,
                                    IODebugContext* d) {
  IOStatus s = target()->GetChildren(p, o, out, d);
  if (!s.ok() || p != options_.data_dir) return s;
  std::set<std::string> names(out->begin(), out->end());
  for (const auto& name : *out) {
    uint64_t n, id;
    if (SegmentName(name, &n, &id)) names.insert(BlobFileName("", n).substr(1));
    if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
      const auto base = name.substr(0, name.size() - 4);
      FileType type;
      if (ParseFileName(base, &n, &type) && type == kBlobFile)
        names.insert(base);
    }
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& item : blobs_)
      names.insert(BlobFileName("", item.first).substr(1));
    for (const auto& item : retired_local_) {
      uint64_t n, id;
      if (SegmentName(item.first, &n, &id))
        names.insert(BlobFileName("", n).substr(1));
    }
  }
  out->assign(names.begin(), names.end());
  return s;
}
IOStatus TieredStorage::SyncFile(const std::string& p, const FileOptions& f,
                                 const IOOptions& o, bool full,
                                 IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->SyncFile(p, f, o, full, d);
  TierWriter writer(this, n);
  return full ? writer.Fsync(o, d) : writer.Sync(o, d);
}
IOStatus TieredStorage::ReadVersion(const Version& v, uint64_t offset,
                                    size_t size, const IOOptions& o, Slice* out,
                                    char* scratch, IODebugContext* d) {
  *out = Slice();
  if (size == 0 || offset >= v.size) return IOStatus::OK();
  size = std::min<uint64_t>(size, v.size - offset);
  size_t done = 0;
  auto first = std::upper_bound(
      v.extents.begin(), v.extents.end(), offset,
      [](uint64_t position, const Extent& e) { return position < e.start; });
  if (first != v.extents.begin()) --first;
  for (auto it = first; it != v.extents.end(); ++it) {
    const auto& e = *it;
    if (offset >= e.start + e.length) continue;
    if (offset < e.start) return IOStatus::Corruption("tier read hole");
    const size_t take =
        std::min<uint64_t>(size - done, e.start + e.length - offset);
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s =
        target()->NewRandomAccessFile(Path(e), FileOptions(), &file, d);
    Slice bytes;
    if (s.ok())
      s = file->Read(offset - e.start, take, o, &bytes, scratch + done, d);
    if (!s.ok()) return s;
    if (bytes.size() != take) return IOStatus::Corruption("short tier range");
    if (bytes.data() != scratch + done)
      std::memcpy(scratch + done, bytes.data(), take);
    done += take;
    offset += take;
    if (done == size) break;
  }
  if (done != size) return IOStatus::Corruption("short tier map");
  *out = Slice(scratch, done);
  return IOStatus::OK();
}
IOStatus TieredStorage::ReadBlob(uint64_t n, uint64_t offset, size_t size,
                                 const IOOptions& o, Slice* out, char* scratch,
                                 IODebugContext* d) {
  Pin pin;
  Status s = pin.Open(this, n);
  if (!s.ok()) return IO(s);
  Version version;
  bool local = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto& live = pin.blob->live;
    version.size = live.size;
    if (size && offset < live.size) {
      const uint64_t end =
          offset + std::min<uint64_t>(size, live.size - offset);
      auto it =
          std::upper_bound(live.extents.begin(), live.extents.end(), offset,
                           [](uint64_t position, const Extent& e) {
                             return position < e.start;
                           });
      if (it != live.extents.begin()) --it;
      for (; it != live.extents.end() && it->start < end; ++it) {
        version.extents.push_back(*it);
        local |= it->local;
      }
    }
    // Snapshot and pin only the selected physical ranges atomically with route
    // publication. A point read is O(log N), not a copy/pin of the whole blob.
    pin.Ranges(version);
  }
  if (local)
    TEST_SYNC_POINT("MetaBypass::TierLocalReadPinned");
  else
    TEST_SYNC_POINT("MetaBypass::TierRemoteReadPinned");
  return ReadVersion(version, offset, size, o, out, scratch, d);
}
Status TieredStorage::DigestDurablePrefix(uint64_t n, uint64_t length,
                                          uint32_t* crc) {
  const auto start = Micros();
  uint64_t opens = 0;
  *crc = 0;
  const auto digest = [&]() -> Status {
    Pin pin;
    Status s = pin.Open(this, n);
    if (!s.ok()) return s;
    Version version;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      version = pin.blob->durable;
      if (DurableEnd(version, true) < length)
        return Status::Corruption("short durable tier prefix");
      pin.Ranges(version);
    }
    TEST_SYNC_POINT("MetaBypass::TierDigestPinned");
    uint64_t remain = length;
    for (const auto& e : version.extents) {
      if (!remain) break;
      const uint64_t take = std::min(remain, e.length);
      s = ExtendDigest(target(), Path(e), 0, take, crc, &opens);
      if (!s.ok()) return s;
      remain -= take;
      TEST_SYNC_POINT("MetaBypass::TierDigestExtentRead");
    }
    return remain ? Status::Corruption("short digest map") : Status::OK();
  };
  Status s = digest();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    digest_opens_ += opens;
    digest_us_ += Micros() - start;
  }
  return s;
}
Status TieredStorage::Wait(uint64_t n, const std::shared_ptr<Blob>& blob,
                           uint64_t length, bool closed) {
  const auto start = Micros();
  std::unique_lock<std::mutex> lock(mutex_);
  requests_[n] = std::max(requests_[n], length);
  work_.notify_one();
  TEST_SYNC_POINT("MetaBypass::TierBarrierWaiting");
  progress_.wait(lock, [&] {
    return !error_.ok() || stopping_ || blob->retiring ||
           (DurableEnd(blob->durable, true) >= length &&
            (!closed || Complete(blob->durable)));
  });
  barrier_us_ += Micros() - start;
  return !error_.ok()     ? error_
         : stopping_      ? Status::ShutdownInProgress()
         : blob->retiring ? Status::Aborted("retiring tier blob")
                          : Status::OK();
}
Status TieredStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  std::vector<std::unique_ptr<Pin>> pins;
  for (const auto& item : lengths) {
    auto pin = std::make_unique<Pin>();
    Status s = pin->Open(this, item.first);
    if (!s.ok()) return s;
    pins.push_back(std::move(pin));
  }
  std::unique_lock<std::mutex> lock(mutex_);
  for (const auto& item : lengths) {
    const auto& blob = *blobs_.at(item.first);
    if (!blob.live.closed || blob.live.size != item.second)
      return Status::InvalidArgument(
          "checkpoint requires closed complete blob");
    if (Complete(blob.durable))
      ++persist_skips_;
    else
      requests_[item.first] = std::max(requests_[item.first], item.second);
  }
  TEST_SYNC_POINT("MetaBypass::TierPersistRequested");
  work_.notify_one();
  progress_.wait(lock, [&] {
    if (!error_.ok() || stopping_) return true;
    for (const auto& pin : pins)
      if (pin->blob->retiring) return true;
    for (const auto& pin : pins)
      if (!Complete(pin->blob->durable)) return false;
    return true;
  });
  Status s = !error_.ok() ? error_
             : stopping_  ? Status::ShutdownInProgress()
                          : Status::OK();
  if (s.ok())
    for (const auto& pin : pins)
      if (pin->blob->retiring) s = Status::Aborted("retiring tier blob");
  lock.unlock();
  return s;
}
Status TieredStorage::Migrate(uint64_t n, const std::shared_ptr<Blob>& blob) {
  TEST_SYNC_POINT("MetaBypass::TierMigration");
  Extent source;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(blob->live.extents.begin(), blob->live.extents.end(),
                           [](const Extent& e) { return e.local && !e.open; });
    if (it == blob->live.extents.end()) return Status::OK();
    source = *it;
  }
  auto& active = blob->migration;
  if (blob->migration_source.empty()) {
    active.extent.start = source.start;
    active.extent.name = NewName(n);
    if (active.extent.name.empty())
      return Status::Corruption("tier extent id overflow");
    blob->migration_source = source.name;
    Status s = target()->NewWritableFile(Path(active.extent), FileOptions(),
                                         &active.file, nullptr);
    if (!s.ok()) return s;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++migration_starts_;
      if (source.frozen_at) migration_delay_us_ += Micros() - source.frozen_at;
    }
  }
  if (blob->migration_source != source.name)
    return Status::Corruption("migration source changed");
  const size_t take =
      std::min<uint64_t>(kChunkSize, source.length - active.extent.length);
  if (take) {
    std::vector<char> buffer(take);
    std::unique_ptr<FSRandomAccessFile> file;
    Status s = target()->NewRandomAccessFile(Path(source), FileOptions(), &file,
                                             nullptr);
    Slice bytes;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queued_bytes_ = take;
      peak_queued_bytes_ = std::max(peak_queued_bytes_, queued_bytes_);
    }
    if (s.ok())
      s = file->Read(active.extent.length, take, IOOptions(), &bytes,
                     buffer.data(), nullptr);
    if (s.ok() && bytes.size() != take)
      s = Status::Corruption("short staging source");
    if (s.ok()) s = active.file->Append(bytes, IOOptions(), nullptr);
    if (s.ok()) s = active.file->Flush(IOOptions(), nullptr);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queued_bytes_ = 0;
      if (s.ok()) migrated_ += take;
    }
    if (!s.ok()) return s;
    active.crc = crc32c::Extend(active.crc, bytes.data(), take);
    active.extent.length += take;
  }
  if (active.extent.length < source.length) {
    TEST_SYNC_POINT("MetaBypass::TierMigrationYield");
    return Status::OK();
  }
  if (active.crc != source.crc)
    return Status::Corruption("staging segment checksum");
  Status s = Seal(&active);
  if (s.ok()) s = SyncCommitDirectory();
  TEST_SYNC_POINT("MetaBypass::TierMigrationSealed");
  const auto destination = active.extent;
  if (s.ok())
    s = Commit(n, blob, [source, destination](Version* v) {
      auto it =
          std::find_if(v->extents.begin(), v->extents.end(),
                       [&](const Extent& e) { return e.name == source.name; });
      if (it == v->extents.end() || !it->local || it->open ||
          it->start != source.start || it->length != source.length)
        return Status::Corruption("migration lost source identity");
      *it = destination;
      return Status::OK();
    });
  if (s.ok()) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      retired_local_[source.name] = source.length;
    }
    active = ActiveExtent();
    blob->migration_source.clear();
    TEST_SYNC_POINT("MetaBypass::TierBeforeEvict");
  }
  return s;
}
Status TieredStorage::ReclaimLocal() {
  std::lock_guard<std::mutex> reclaim(reclamation_);
  std::string name;
  uint64_t size = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& item : retired_local_) {
      if (readers_.count(item.first)) continue;
      name = item.first;
      size = item.second;
      break;
    }
  }
  if (name.empty()) return Status::OK();
  Status s = target()->DeleteFile(options_.staging_dir + "/" + name,
                                  IOOptions(), nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierLocalUnlinked", &s);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierBeforeQuotaRelease", &s);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    usage_ -= size;
    retired_local_.erase(name);
    progress_.notify_all();
  }
  return s;
}
void TieredStorage::Run() {
  uint64_t cursor = 0;
  bool priority = true;
  std::unique_lock<std::mutex> lock(mutex_);
  const auto ready = [&](const std::shared_ptr<Blob>& blob) {
    return !blob->retiring &&
           std::any_of(blob->live.extents.begin(), blob->live.extents.end(),
                       [](const Extent& e) { return e.local && !e.open; });
  };
  const auto reclaimable = [&] {
    for (const auto& item : retired_local_)
      if (!readers_.count(item.first)) return true;
    return false;
  };
  while (error_.ok() && !stopping_) {
    work_.wait(lock, [&] {
      if (stopping_ || !error_.ok() || reclaimable()) return true;
      for (const auto& item : blobs_)
        if (ready(item.second)) return true;
      return false;
    });
    if (stopping_ || !error_.ok()) break;
    // Only this worker reclaims published-away SSD ranges.
    if (reclaimable()) {
      lock.unlock();
      Status s = ReclaimLocal();
      if (!s.ok()) Fail(s);
      lock.lock();
      continue;
    }
    auto selected = blobs_.end();
    auto it = blobs_.upper_bound(cursor);
    for (size_t count = 0; count < blobs_.size(); ++count) {
      if (it == blobs_.end()) it = blobs_.begin();
      if (ready(it->second) && (!priority || requests_.count(it->first))) {
        selected = it;
        break;
      }
      ++it;
    }
    if (selected == blobs_.end()) {
      priority = false;
      continue;
    }
    priority = !priority;
    cursor = selected->first;
    const auto n = selected->first;
    auto blob = selected->second;
    ++blob->operations;
    lock.unlock();
    Status s = Migrate(n, blob);
    if (!s.ok() && !s.IsAborted()) Fail(s);
    TEST_SYNC_POINT("MetaBypass::TierMigrationComplete");
    lock.lock();
    --blob->operations;
    auto request = requests_.find(n);
    if (request != requests_.end() &&
        DurableEnd(blob->durable, true) >= request->second)
      requests_.erase(request);
    progress_.notify_all();
  }
  progress_.notify_all();
}

Status TieredStorage::ReclaimBlob(uint64_t n, uint64_t* bytes) {
  std::shared_ptr<Blob> blob;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!error_.ok()) return error_;
    if (stopping_) return Status::ShutdownInProgress();
    auto& entry = blobs_[n];
    if (!entry) entry = std::make_shared<Blob>();
    blob = entry;
    if (blob->retiring) return Status::Corruption("duplicate blob retirement");
    blob->retiring = true;
    requests_.erase(n);
    progress_.notify_all();
    TEST_SYNC_POINT("MetaBypass::TierBlobRetiring");
    work_.notify_one();
    progress_.wait(lock, [&] { return !blob->operations; });
    if (!error_.ok()) return error_;
    if (stopping_) return Status::ShutdownInProgress();
    ++blob->operations;  // Stop waits for deletion, including final dirsync.
  }
  Status s = DeleteRetiredBlob(n, blob, bytes);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    --blob->operations;
    if (s.ok()) blobs_.erase(n);
    progress_.notify_all();
  }
  if (!s.ok()) Fail(s);
  return s;
}
Status TieredStorage::DeleteRetiredBlob(uint64_t n,
                                        const std::shared_ptr<Blob>& blob,
                                        uint64_t* bytes) {
  std::lock_guard<std::mutex> reclaim(reclamation_);
  for (auto* active : {&blob->caller, &blob->migration}) {
    if (!active->file) continue;
    Status s = active->file->Close(IOOptions(), nullptr);
    if (!s.ok()) return s;
    active->file.reset();
  }
  struct Payload {
    std::string path, name;
    uint64_t size;
    bool local;
  };
  std::vector<Payload> payloads;
  for (bool local : {false, true}) {
    const auto& dir = local ? options_.staging_dir : options_.data_dir;
    std::vector<std::string> names;
    Status s = target()->GetChildren(dir, IOOptions(), &names, nullptr);
    if (!s.ok()) return s;
    for (const auto& name : names) {
      uint64_t owner, id;
      if (!SegmentName(name, &owner, &id) || owner != n) continue;
      uint64_t size;
      s = target()->GetFileSize(dir + "/" + name, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      payloads.push_back({dir + "/" + name, name, size, local});
    }
  }
  *bytes = 0;
  for (const auto& e : payloads) *bytes += e.size;
  for (const auto& path : {Descriptor(n), Descriptor(n) + ".tmp"}) {
    uint64_t size = 0;
    Status stat = target()->GetFileSize(path, IOOptions(), &size, nullptr);
    if (!stat.ok() && !stat.IsNotFound() && !stat.IsPathNotFound()) return stat;
    if (stat.ok()) *bytes += size;
  }
  TEST_SYNC_POINT("MetaBypass::BeforeBlobDelete");
  Status s = target()->DeleteFile(Descriptor(n), IOOptions(), nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobDescriptorRemoved", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  TEST_SYNC_POINT("MetaBypass::BlobDescriptorSynced");
  if (!s.ok()) return s;
  payloads.push_back({Descriptor(n) + ".tmp", "", 0, false});
  for (const auto& e : payloads) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobPayloadDeleteStatus", &s);
    if (s.ok()) s = target()->DeleteFile(e.path, IOOptions(), nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
    if (!s.ok()) return s;
    TEST_SYNC_POINT("MetaBypass::BlobPayloadRemoved");
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobFinalDirSyncStatus", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& e : payloads) {
      if (e.local) {
        usage_ -= e.size;
        retired_local_.erase(e.name);
      }
    }
  }
  return s;
}
Status TieredStorage::SaveCheckpoint(
    const std::string& path, const std::map<uint64_t, uint64_t>& lengths) {
  std::ostringstream out;
  out << "MBTC3\n";
  std::vector<std::unique_ptr<Pin>> pins;
  for (const auto& item : lengths) {
    auto pin = std::make_unique<Pin>();
    Status s = pin->Open(this, item.first);
    if (!s.ok()) return s;
    Version version;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      version = pin->blob->durable;
      if (!Complete(version) || version.size != item.second)
        return Status::Corruption("checkpoint requires durable closed blob");
      pin->Ranges(version);
    }
    const auto encoded = Encode(version);
    out << item.first << ' ' << encoded.size() << '\n' << encoded;
    pins.push_back(std::move(pin));
  }
  return Write(target(), path, out.str());
}
Status TieredStorage::LoadCheckpoint(const std::string& path) {
  std::string bytes;
  Status s = Read(target(), path, &bytes);
  if (!s.ok()) return s;
  std::istringstream in(bytes);
  std::string magic;
  if (!std::getline(in, magic) || magic != "MBTC3")
    return Status::Corruption("tier checkpoint version");
  std::map<uint64_t, std::shared_ptr<Blob>> parsed;
  while (in >> std::ws && !in.eof()) {
    uint64_t n, length;
    if (!(in >> n >> length) || in.get() != '\n' || length == 0 ||
        length > bytes.size())
      return Status::Corruption("tier checkpoint entry");
    std::string encoded(length, '\0');
    if (!in.read(&encoded[0], length))
      return Status::Corruption("short tier checkpoint");
    auto blob = std::make_shared<Blob>();
    s = Decode(encoded, &blob->durable);
    if (!s.ok()) return s;
    if (!Complete(blob->durable))
      return Status::Corruption("checkpoint SSD/open dependency");
    for (const auto& e : blob->durable.extents) {
      uint64_t owner, id, size;
      if (!SegmentName(e.name, &owner, &id) || owner != n)
        return Status::Corruption("checkpoint range owner");
      s = target()->GetFileSize(Path(e), IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size != e.length)
        return Status::Corruption("checkpoint extent length");
      uint32_t crc;
      s = Digest(target(), Path(e), size, &crc);
      if (!s.ok()) return s;
      if (crc != e.crc) return Status::Corruption("checkpoint extent checksum");
    }
    blob->live = blob->durable;
    if (!parsed.emplace(n, blob).second)
      return Status::Corruption("duplicate checkpoint blob");
  }
  // A dropped SSD may have belonged to descriptors created after this point.
  // Preflight every descriptor format before changing the visible namespace.
  std::vector<std::string> names, obsolete;
  s = target()->GetChildren(options_.data_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  for (const auto& name : names) {
    uint64_t n;
    if (!IsBlob(options_.data_dir + "/" + name, &n)) continue;
    std::string encoded;
    s = Read(target(), Descriptor(n), &encoded);
    Version old;
    if (s.ok()) s = Decode(encoded, &old);
    if (!s.ok()) return s;
    if (!parsed.count(n)) obsolete.push_back(Descriptor(n));
  }
  // Offline restore only. Validate ALL references before replacing any route.
  // The outer Restore owns the data lock and durable RESTORING marker.
  for (const auto& item : parsed) {
    s = Write(target(), Descriptor(item.first) + ".tmp",
              Encode(item.second->durable));
    if (s.ok())
      s = target()->RenameFile(Descriptor(item.first) + ".tmp",
                               Descriptor(item.first), IOOptions(), nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDescriptorPublished", &s);
    if (!s.ok()) return s;
  }
  // Historical points own their encoded mapping and immutable HDD extents.
  // Only remove obsolete current descriptors, never historical payloads.
  for (const auto& obsolete_path : obsolete) {
    s = target()->DeleteFile(obsolete_path, IOOptions(), nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDescriptorRemoved", &s);
    if (!s.ok()) return s;
  }
  s = SyncDir(target(), options_.data_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDirectorySynced", &s);
  if (s.ok()) blobs_ = std::move(parsed);
  return s;
}
Status TieredStorage::ArchiveUnreferenced(
    const std::map<uint64_t, uint64_t>& referenced) {
  // Offline initialization: retain orphan bytes in HDD extents before freeing
  // staging, without registering the orphan in RocksDB's native MANIFEST.
  for (const auto& item : blobs_) {
    if (referenced.count(item.first)) continue;
    while (true) {
      bool local = false;
      for (const auto& e : item.second->live.extents) local |= e.local;
      if (!local) break;
      Status s = Migrate(item.first, item.second);
      if (!s.ok()) return s;
    }
  }
  while (!retired_local_.empty()) {
    Status s = ReclaimLocal();
    if (!s.ok()) return s;
  }
  return Status::OK();
}
void TieredStorage::AddStats(MetaBypassStats* stats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  stats->tier_task_wait_micros = 0;
  stats->tier_append_tasks = 0;
  stats->tier_append_wait_micros = 0;
  stats->tier_switch_wait_micros = 0;
  stats->staging_backpressure_micros = 0;
  stats->staging_bytes = usage_;
  stats->peak_staging_bytes = peak_;
  stats->staging_spill_files = spill_files_;
  stats->staging_spill_bytes = spill_bytes_;
  stats->tier_queued_bytes = queued_bytes_;
  stats->peak_tier_queued_bytes = peak_queued_bytes_;
  stats->migrated_blob_bytes = migrated_;
  stats->tier_persist_skips = persist_skips_;
  stats->tier_commit_skips = commit_skips_;
  stats->tier_checkpoint_digest_opens = digest_opens_;
  stats->tier_checkpoint_digest_micros = digest_us_;
  stats->tier_extent_fsyncs = extent_fsyncs_;
  stats->tier_extent_fsync_micros = extent_fsync_us_;
  stats->tier_commit_dir_syncs = commit_dir_syncs_;
  stats->tier_commit_dir_sync_micros = commit_dir_sync_us_;
  stats->tier_migration_starts = migration_starts_;
  stats->tier_migration_start_delay_micros = migration_delay_us_;
  stats->tier_prefix_write_micros = prefix_write_us_;
  stats->tier_suffix_write_micros = suffix_write_us_;
  stats->tier_suffix_appends = suffix_appends_;
  stats->tier_barrier_wait_micros = barrier_us_;
  stats->tier_metadata_commit_micros = metadata_us_;
  stats->tier_quota_admission_micros = quota_us_;
  for (const auto& item : blobs_) {
    for (const auto& e : item.second->live.extents) {
      if (e.local)
        stats->pending_blob_bytes += e.length;
      else
        ++stats->hdd_extents;
    }
  }
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
