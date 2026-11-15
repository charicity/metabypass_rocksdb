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
#include "rocksdb/env.h"
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
struct OnExit {
  std::function<void()> callback;
  ~OnExit() { callback(); }
};
std::string Hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string out;
  for (unsigned char c : bytes) {
    out += digits[c >> 4];
    out += digits[c & 15];
  }
  return out;
}
bool Token(const std::string& value) {
  return !value.empty() && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
bool SegmentName(const std::string& name, uint64_t* number, uint64_t* id,
                 std::string* incarnation = nullptr) {
  if (name.compare(0, 8, "segment-") != 0) return false;
  const auto first = name.find('-', 8);
  const auto second =
      first == std::string::npos ? first : name.find('-', first + 1);
  if (second == std::string::npos) return false;
  const auto token = name.substr(first + 1, second - first - 1);
  std::istringstream n(name.substr(8, first - 8)), i(name.substr(second + 1));
  if (!(n >> *number) || !n.eof() || !(i >> *id) || !i.eof() || !Token(token) ||
      *id == UINT64_MAX ||
      name != "segment-" + std::to_string(*number) + "-" + token + "-" +
                  std::to_string(*id))
    return false;
  if (incarnation) *incarnation = token;
  return true;
}
bool LocalName(const std::string& name, uint64_t* number) {
  if (name.size() <= 6 || name.substr(name.size() - 6) != ".local")
    return false;
  std::istringstream in(name.substr(0, name.size() - 6));
  return (in >> *number) && in.eof() &&
         name == std::to_string(*number) + ".local";
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
    const auto call_started = Micros();
    OnExit call_timing{[&] {
      const uint64_t elapsed = Micros() - call_started;
      unsigned bucket = 0;
      for (uint64_t value = elapsed; value > 1; value >>= 1) ++bucket;
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      ++storage_->append_calls_;
      storage_->append_us_ += elapsed;
      ++storage_->append_latency_[bucket];
    }};
    HddPurposeScope purpose(HddPurpose::kCaller);
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    auto blob = pin.blob;
    const auto append = [&]() -> Status {
      std::lock_guard<std::mutex> writer(blob->operation);
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
          local = storage_->Adaptive() ? storage_->pressure_->Pressured()
                                       : !blob->live.spilled;
          if (local) {
            const auto start = Micros();
            const uint64_t available = storage_->options_.staging_capacity -
                                       storage_->usage_ - storage_->reserved_;
            local = take <= available;
            if (!local && storage_->Adaptive()) ++storage_->quota_fallbacks_;
            storage_->quota_us_ += Micros() - start;
            if (local) {
              storage_->reserved_ += take;
              storage_->peak_ = std::max(
                  storage_->peak_, storage_->usage_ + storage_->reserved_);
            }
          }
        }
        if (storage_->Adaptive() && active.file &&
            active.extent.local != local) {
          Status st = !active.extent.local && local
                          ? storage_->EnqueueSeal(number_, blob, true)
                          : storage_->Freeze(number_, blob);
          if (!st.ok()) {
            if (local) {
              std::lock_guard<std::mutex> lock(storage_->mutex_);
              storage_->reserved_ -= take;
            }
            return st;
          }
        }
        if (!local && !storage_->Adaptive()) {
          bool spilled;
          {
            std::lock_guard<std::mutex> lock(storage_->mutex_);
            spilled = blob->live.spilled;
          }
          if (!spilled) {
            Status st = storage_->Freeze(number_, blob);
            if (!st.ok()) return st;
            st = storage_->Commit(
                number_, blob,
                [](TieredStorage::Version* v) {
                  v->spilled = true;
                  v->boundary = v->size;
                  return Status::OK();
                },
                TieredStorage::Reason::kSpill);
            if (!st.ok()) return st;
            {
              std::lock_guard<std::mutex> lock(storage_->mutex_);
              ++storage_->spill_files_;
            }
            TEST_SYNC_POINT("MetaBypass::TierSuffixRegistered");
          }
        }
        if (local) {
          TEST_SYNC_POINT("MetaBypass::TierQuotaReserved");
        }
        Status st;
        if (!active.file) st = storage_->Register(number_, blob, local);
        const auto started = Micros();
        const bool attempted = st.ok();
        std::unique_lock<std::mutex> physical(blob->append);
        if (st.ok())
          st = active.file->Append(Slice(bytes.data() + offset, take), o, d);
        // Flush this writer before Freeze can transfer it to the worker. The
        // outer Flush may already see its successor (or no caller writer).
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
            if (!local) {
              storage_->spill_bytes_ += take;
              storage_->direct_bytes_ += take;
            } else
              storage_->staged_bytes_ += take;
          }
        }
        physical.unlock();
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
    if (!s.ok() && !s.IsAborted() && !s.IsShutdownInProgress())
      storage_->Fail(s);
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
    const auto call_started = Micros();
    OnExit call_timing{[&] {
      const uint64_t elapsed = Micros() - call_started;
      unsigned bucket = 0;
      for (uint64_t value = elapsed; value > 1; value >>= 1) ++bucket;
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      ++storage_->flush_calls_;
      storage_->flush_us_ += elapsed;
      ++storage_->flush_latency_[bucket];
    }};
    HddPurposeScope purpose(HddPurpose::kCaller);
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    {
      std::lock_guard<std::mutex> writer(pin.blob->operation);
      if (pin.blob->caller.file) s = pin.blob->caller.file->Flush(o, d);
    }
    if (!s.ok() && !s.IsAborted() && !s.IsShutdownInProgress())
      storage_->Fail(s);
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
    HddPurposeScope purpose(HddPurpose::kBarrier);
    TieredStorage::Pin pin;
    Status s = pin.Open(storage_, number_);
    if (!s.ok()) return IO(s);
    auto blob = pin.blob;
    TEST_SYNC_POINT("MetaBypass::TierSyncPinned");
    uint64_t length = 0;
    bool spilled = false;
    {
      std::lock_guard<std::mutex> writer(blob->operation);
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        if (close) blob->closing = true;
        length = blob->live.size;
        spilled = blob->live.spilled;
      }
      s = storage_->Freeze(number_, blob);
      if (s.ok() && close)
        s = storage_->Commit(
            number_, blob,
            [](TieredStorage::Version* v) {
              v->closed = true;
              return Status::OK();
            },
            TieredStorage::Reason::kClose);
    }
    if (s.ok() && storage_->Adaptive())
      s = storage_->Wait(number_, blob, length, false, false);
    // Closed HDD-only blobs can finalize here; an outstanding prefix remains
    // the worker's responsibility. No writer lock is held across either path.
    if (s.ok() && !storage_->Adaptive() && spilled && close)
      s = storage_->Finalize(number_, blob);
    // Never wait for the worker while holding append or commit.
    if (s.ok() && !storage_->Adaptive() && spilled)
      s = storage_->Wait(number_, blob, length, close);
    if (!s.ok() && !s.IsAborted() && !s.IsShutdownInProgress())
      storage_->Fail(s);
    return IO(s);
  }
  TieredStorage* storage_;
  uint64_t number_;
};

TieredStorage::TieredStorage(std::shared_ptr<FileSystem> fs,
                             const MetaBypassOptions& o,
                             const std::string& identity,
                             std::shared_ptr<HddPressure> pressure)
    : FileSystemWrapper(fs),
      options_(o),
      pressure_(std::move(pressure)),
      identity_(Hex(identity)) {
  // Direct component callers receive the same single physical boundary as DB.
  if (!pressure_) {
    owns_observer_ = true;
    pressure_ = std::make_shared<HddPressure>(o.staging_latency_budget_us);
    target_ = ObserveHdd(std::move(fs), o.data_dir, o.backup_dir, pressure_, {},
                         o.staging_dir);
  }
}
void TieredStorage::SetIdentity(const std::string& identity) {
  std::lock_guard<std::mutex> lock(mutex_);
  identity_ = Hex(identity);
}
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
std::string TieredStorage::LocalDescriptor(uint64_t n) const {
  return options_.staging_dir + "/" + std::to_string(n) + ".local";
}
std::string TieredStorage::Tombstone(uint64_t n, const Version& v) const {
  return options_.data_dir + "/" + std::to_string(n) + "-" + v.incarnation +
         ".retired";
}
std::string TieredStorage::Path(const Extent& e) const {
  return (e.local ? options_.staging_dir : options_.data_dir) + "/" + e.name;
}
std::string TieredStorage::NewName(uint64_t n) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (extent_id_ >= UINT64_MAX - 1) return {};
  return "segment-" + std::to_string(n) + "-" + blobs_.at(n)->live.incarnation +
         "-" + std::to_string(++extent_id_);
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
Status TieredStorage::Pin::Open(TieredStorage* storage, uint64_t n,
                                bool read_only) {
  std::lock_guard<std::mutex> lock(storage->mutex_);
  if (!read_only && !storage->error_.ok()) return storage->error_;
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
    if (e.start != end || (hdd && (e.local || e.open || e.pending))) break;
    end += e.durable;
    if (e.durable != e.length) break;
  }
  return end;
}
bool TieredStorage::Complete(const Version& v) {
  return v.closed && DurableEnd(v, true) == v.size &&
         std::all_of(v.extents.begin(), v.extents.end(), [](const Extent& e) {
           return !e.local && !e.open && !e.pending;
         });
}
bool TieredStorage::Equal(const Version& a, const Version& b) {
  if (a.identity != b.identity || a.incarnation != b.incarnation ||
      a.retired != b.retired || a.size != b.size || a.closed != b.closed ||
      a.spilled != b.spilled || a.boundary != b.boundary ||
      a.extents.size() != b.extents.size())
    return false;
  return std::equal(a.extents.begin(), a.extents.end(), b.extents.begin(),
                    [](const Extent& x, const Extent& y) {
                      return x.start == y.start && x.length == y.length &&
                             x.durable == y.durable && x.name == y.name &&
                             x.origin == y.origin && x.local == y.local &&
                             x.open == y.open && x.pending == y.pending &&
                             x.crc == y.crc;
                    });
}
std::string TieredStorage::Encode(const Version& v, bool local) {
  std::ostringstream out;
  // The logical owner is explicitly stored even for empty objects.
  out << v.identity << ' ' << v.number << ' ' << v.incarnation << ' '
      << v.generation << ' ';
  if (local) {
    out << v.size << ' ' << v.closed << ' ' << v.extents.size() << '\n';
    for (const auto& e : v.extents)
      out << e.start << ' ' << e.length << ' ' << e.durable << ' '
          << kExtentSize << ' ' << e.local << ' ' << e.open << ' ' << e.pending
          << ' ' << e.crc << ' ' << e.name << ' ' << e.origin << '\n';
  } else {
    out << (v.retired  ? "RETIRED"
            : v.closed ? "COMPLETE"
                       : "PARTIAL")
        << ' ' << (v.closed ? v.size : 0) << ' ' << v.extents.size() << '\n';
    for (const auto& e : v.extents)
      out << e.start << ' ' << e.length << ' ' << e.crc << ' ' << e.name << ' '
          << e.origin << '\n';
  }
  const auto body = out.str();
  return std::string(local ? "MBL5 " : "MBT5 ") +
         std::to_string(crc32c::Value(body.data(), body.size())) + "\n" + body;
}
Status TieredStorage::Decode(const std::string& bytes, Version* v, bool local) {
  const auto split = bytes.find('\n');
  if (split == std::string::npos) return Status::Corruption("tier header");
  std::string magic, extra;
  uint32_t crc;
  std::istringstream header(bytes.substr(0, split));
  if (!(header >> magic >> crc) || magic != (local ? "MBL5" : "MBT5") ||
      (header >> extra))
    return Status::Corruption("tier format");
  const auto body = bytes.substr(split + 1);
  if (crc32c::Value(body.data(), body.size()) != crc)
    return Status::Corruption("tier checksum");
  *v = Version();
  std::istringstream in(body);
  uint64_t count;
  if (!(in >> v->identity >> v->number >> v->incarnation >> v->generation) ||
      !Token(v->identity) || !Token(v->incarnation) || !v->generation)
    return Status::Corruption("tier identity");
  if (local) {
    unsigned closed;
    if (!(in >> v->size >> closed >> count) || closed > 1)
      return Status::Corruption("local header");
    v->closed = closed;

  } else {
    std::string state;
    if (!(in >> state >> v->size >> count) ||
        (state != "PARTIAL" && state != "COMPLETE" && state != "RETIRED"))
      return Status::Corruption("remote header");
    v->closed = state == "COMPLETE";
    v->retired = state == "RETIRED";
    if ((!v->closed && v->size) || (v->retired && count))
      return Status::Corruption("remote state");
  }
  if (count > bytes.size()) return Status::Corruption("tier range count");
  uint64_t end = 0;
  for (uint64_t i = 0; i < count; ++i) {
    Extent e;
    if (local) {
      uint64_t maximum;
      unsigned medium, open, pending;
      if (!(in >> e.start >> e.length >> e.durable >> maximum >> medium >>
            open >> pending >> e.crc >> e.name >> e.origin) ||
          maximum != kExtentSize || medium > 1 || open > 1 || pending > 1)
        return Status::Corruption("local range");
      e.local = medium;
      e.open = open;
      e.pending = pending;
    } else {
      if (!(in >> e.start >> e.length >> e.crc >> e.name >> e.origin))
        return Status::Corruption("remote range");
      e.local = false;
      e.open = false;
      e.durable = e.length;
    }
    uint64_t owner, id;
    std::string token;
    if (!SegmentName(e.name, &owner, &id, &token) || owner != v->number ||
        token != v->incarnation ||
        !SegmentName(e.origin, &owner, &id, &token) || owner != v->number ||
        token != v->incarnation || e.length > kExtentSize ||
        e.start > UINT64_MAX - e.length || e.durable > e.length ||
        (!e.open && !e.pending && e.durable != e.length) ||
        (e.pending && (e.local || e.open)) ||
        (e.open && (v->closed || i + 1 != count)) ||
        (local ? e.start != end : e.start < end) || (!local && !e.length))
      return Status::Corruption("tier range identity/coverage");
    if (e.local && e.name != e.origin)
      return Status::Corruption("local range boundary");
    if (!e.local) v->spilled = true;
    end = e.start + e.length;
    v->extents.push_back(std::move(e));
  }
  if ((in >> extra) || (local && end != v->size) ||
      (!local && v->closed && !Complete(*v)))
    return Status::Corruption("tier range coverage");
  return Status::OK();
}

Status TieredStorage::SyncCommitDirectory(Reason reason) {
  const auto start = Micros();
  Status s = SyncDir(target(), options_.data_dir);
  std::lock_guard<std::mutex> lock(mutex_);
  ++commit_dir_syncs_;
  commit_dir_sync_us_ += Micros() - start;
  ++remote_metadata_.dirsyncs;
  remote_metadata_.dirsync_us += Micros() - start;
  remote_metadata_.reason_dirsync_us[static_cast<size_t>(reason)] +=
      Micros() - start;
  return s;
}
Status TieredStorage::RefreshMetadataBytes() {
  std::vector<std::string> names;
  Status s =
      target()->GetChildren(options_.staging_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  uint64_t bytes = 0;
  std::map<std::string, uint64_t> files;
  for (const auto& name : names) {
    uint64_t n;
    const auto base = name.size() > 4 && name.substr(name.size() - 4) == ".tmp"
                          ? name.substr(0, name.size() - 4)
                          : name;
    if (!LocalName(base, &n)) continue;
    uint64_t length;
    s = target()->GetFileSize(options_.staging_dir + "/" + name, IOOptions(),
                              &length, nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) {
      s.PermitUncheckedError();
      continue;
    }
    if (!s.ok()) return s;
    files.emplace(options_.staging_dir + "/" + name, length);
    bytes += length;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  local_metadata_files_ = std::move(files);
  local_metadata_bytes_ = bytes;
  peak_local_metadata_bytes_ = std::max(peak_local_metadata_bytes_, bytes);
  return Status::OK();
}
void TieredStorage::TrackMetadataBytes(const std::string& path, uint64_t size,
                                       const std::string& removed) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!removed.empty()) {
    auto previous = local_metadata_files_.find(removed);
    if (previous != local_metadata_files_.end()) {
      local_metadata_bytes_ -= previous->second;
      local_metadata_files_.erase(previous);
    }
  }
  auto it = local_metadata_files_.find(path);
  if (it != local_metadata_files_.end()) {
    local_metadata_bytes_ -= it->second;
    if (size)
      it->second = size;
    else
      local_metadata_files_.erase(it);
  } else if (size) {
    local_metadata_files_.emplace(path, size);
  }
  local_metadata_bytes_ += size;
  peak_local_metadata_bytes_ =
      std::max(peak_local_metadata_bytes_, local_metadata_bytes_);
}
void TieredStorage::RecheckMetadataBytes(const std::string& path) {
  // Only an error path needs stat. A failed Append may have written a prefix;
  // never charge the intended full encoding as if it were an observed length.
  uint64_t size;
  Status s = target()->GetFileSize(path, IOOptions(), &size, nullptr);
  if (s.ok())
    TrackMetadataBytes(path, size);
  else if (s.IsNotFound() || s.IsPathNotFound())
    TrackMetadataBytes(path, 0);
  // If even stat fails, retain the last observed accounting with the sticky
  // operation error; the next recovery scan reconstructs physical lengths.
  s.PermitUncheckedError();
}
Status TieredStorage::StoreSnapshot(const std::string& path,
                                    const Version& next, bool local,
                                    Reason reason) {
  const size_t index = static_cast<size_t>(reason);
  const auto snapshot_start = Micros();
  const bool standalone =
      !local && (reason == Reason::kGC || reason == Reason::kRecovery);
  OnExit timing{[&] {
    if (standalone) {
      const auto elapsed = Micros() - snapshot_start;
      std::lock_guard<std::mutex> lock(mutex_);
      remote_metadata_.micros += elapsed;
      remote_metadata_.reason_us[index] += elapsed;
      metadata_us_ += elapsed;
    }
  }};
  if (standalone) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++remote_metadata_.attempts;
    ++remote_metadata_.reasons[static_cast<size_t>(reason)];
  }
  const auto bytes = Encode(next, local);
  std::unique_ptr<FSWritableFile> file;
  Status s =
      target()->NewWritableFile(path + ".tmp", FileOptions(), &file, nullptr);
  if (local && s.ok()) TrackMetadataBytes(path + ".tmp", 0);
  if (s.ok()) s = file->Append(bytes, IOOptions(), nullptr);
  if (local && s.ok()) TrackMetadataBytes(path + ".tmp", bytes.size());
  if (s.ok()) {
    const auto start = Micros();
    s = file->Fsync(IOOptions(), nullptr);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& counters = local ? local_metadata_ : remote_metadata_;
    ++counters.fsyncs;
    const auto elapsed = Micros() - start;
    counters.fsync_us += elapsed;
    counters.reason_fsync_us[index] += elapsed;
  }
  if (file) s.UpdateIfOk(file->Close(IOOptions(), nullptr));
  if (local) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierLocalDescriptorSynced", &s);
  } else {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorSynced", &s);
  }
  if (s.ok()) {
    s = target()->RenameFile(path + ".tmp", path, IOOptions(), nullptr);
    if (local && s.ok()) {
      TrackMetadataBytes(path, bytes.size(), path + ".tmp");
    }
  }
  if (local) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierLocalDescriptorRenamed", &s);
  } else {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorRenamed", &s);
  }
  if (s.ok()) {
    const auto start = Micros();
    s = SyncDir(target(), local ? options_.staging_dir : options_.data_dir);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& counters = local ? local_metadata_ : remote_metadata_;
    ++counters.dirsyncs;
    const auto elapsed = Micros() - start;
    counters.dirsync_us += elapsed;
    counters.reason_dirsync_us[index] += elapsed;
    if (!local) {
      ++commit_dir_syncs_;
      commit_dir_sync_us_ += Micros() - start;
    }
  }
  if (local) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierLocalPublicationSynced", &s);
  } else {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierPublicationSynced", &s);
  }
  if (s.ok() && !local &&
      (reason == Reason::kGC || reason == Reason::kRecovery)) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++remote_metadata_.successes;
  }
  if (local && !s.ok()) {
    RecheckMetadataBytes(path + ".tmp");
    RecheckMetadataBytes(path);
  }
  return s;
}
Status TieredStorage::Commit(uint64_t n, const std::shared_ptr<Blob>& blob,
                             const Patch& patch, Reason reason) {
  const auto start = Micros();
  const size_t index = static_cast<size_t>(reason);
  OnExit timing{[&] {
    const auto elapsed = Micros() - start;
    std::lock_guard<std::mutex> lock(mutex_);
    local_metadata_.micros += elapsed;
    local_metadata_.reason_us[index] += elapsed;
    metadata_us_ += elapsed;
  }};
  std::unique_lock<std::mutex> commit(blob->commit);
  Version next;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++local_metadata_.attempts;
    ++local_metadata_.reasons[static_cast<size_t>(reason)];
    const auto waited = Micros() - start;
    local_metadata_.wait += waited;
    local_metadata_.reason_wait[index] += waited;
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
    if ((blob->local_present || blob->complete_ready) && !pending &&
        Equal(next, blob->durable)) {
      ++commit_skips_;
      ++local_metadata_.skips;
      return Status::OK();
    }
  }
  if (next.generation == UINT64_MAX)
    return Status::Corruption("local generation overflow");
  ++next.generation;
  s = StoreSnapshot(LocalDescriptor(n), next, true, reason);
  if (s.ok()) {
    TEST_SYNC_POINT("MetaBypass::TierLocalPersistedBeforeRoute");
  }
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    s = patch(&blob->live);
    if (s.ok()) {
      blob->live.generation = next.generation;
      blob->durable = std::move(next);
      blob->local_present = true;
      ++local_metadata_.successes;
      progress_.notify_all();
      work_.notify_one();
    }
  }
  if (s.ok()) {
    TEST_SYNC_POINT("MetaBypass::TierLocalRouteDurable");
  }
  return s;
}
Status TieredStorage::Publish(uint64_t n, const std::shared_ptr<Blob>& blob,
                              const Extent& extent) {
  const auto start = Micros();
  const Reason reason =
      extent.name == extent.origin ? Reason::kFreeze : Reason::kMigration;
  const size_t index = static_cast<size_t>(reason);
  OnExit timing{[&] {
    const auto elapsed = Micros() - start;
    std::lock_guard<std::mutex> lock(mutex_);
    remote_metadata_.micros += elapsed;
    remote_metadata_.reason_us[index] += elapsed;
    metadata_us_ += elapsed;
  }};
  std::unique_lock<std::mutex> commit(blob->remote_commit);
  Version next;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++remote_metadata_.attempts;
    ++remote_metadata_.reasons[index];
    const auto waited = Micros() - start;
    remote_metadata_.wait += waited;
    remote_metadata_.reason_wait[index] += waited;
    if (!error_.ok()) return error_;
    if (blob->retiring) return Status::Aborted("retiring tier blob");
    next = blob->remote;
  }
  auto it = std::lower_bound(
      next.extents.begin(), next.extents.end(), extent.start,
      [](const Extent& e, uint64_t offset) { return e.start < offset; });
  if (it != next.extents.end() && it->start == extent.start) {
    if (it->name != extent.name || it->origin != extent.origin ||
        it->length != extent.length || it->crc != extent.crc)
      return Status::Corruption("conflicting HDD publication");
    std::lock_guard<std::mutex> lock(mutex_);
    ++remote_metadata_.skips;
    ++commit_skips_;
    return Status::OK();
  }
  if ((it != next.extents.end() && extent.start + extent.length > it->start) ||
      (it != next.extents.begin() &&
       (it - 1)->start + (it - 1)->length > extent.start) ||
      next.closed)
    return Status::Corruption("overlapping HDD publication");
  next.extents.insert(it, extent);
  if (next.generation == UINT64_MAX)
    return Status::Corruption("HDD generation overflow");
  ++next.generation;
  Status s = SyncCommitDirectory(reason);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDependenciesSynced", &s);
  if (s.ok()) s = StoreSnapshot(Descriptor(n), next, false, reason);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (s.ok()) {
      blob->remote = std::move(next);
      ++remote_metadata_.successes;
    }
  }
  // No local commit lock is held during this publication, or vice versa.
  return s;
}
Status TieredStorage::Finalize(uint64_t n, const std::shared_ptr<Blob>& blob) {
  const auto start = Micros();
  const size_t index = static_cast<size_t>(Reason::kFinalize);
  {
    bool attempted = false;
    OnExit timing{[&] {
      if (attempted) {
        const auto elapsed = Micros() - start;
        std::lock_guard<std::mutex> lock(mutex_);
        remote_metadata_.micros += elapsed;
        remote_metadata_.reason_us[index] += elapsed;
        metadata_us_ += elapsed;
      }
    }};
    std::unique_lock<std::mutex> commit(blob->remote_commit);
    Version next;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!error_.ok()) return error_;
      if (blob->retiring) return Status::Aborted("retiring tier blob");
      if (blob->complete_ready || !Complete(blob->durable)) return Status::OK();
      next = blob->remote;
      next.size = blob->durable.size;
      next.closed = true;
      if (!Complete(next))
        return Status::Corruption("incomplete final HDD map");
      attempted = true;
      ++remote_metadata_.attempts;
      ++remote_metadata_.reasons[static_cast<size_t>(Reason::kFinalize)];
      const auto waited = Micros() - start;
      remote_metadata_.wait += waited;
      remote_metadata_.reason_wait[index] += waited;
    }
    Status s;
    if (!blob->remote.closed) {
      if (next.generation == UINT64_MAX)
        return Status::Corruption("HDD generation overflow");
      ++next.generation;
      s = StoreSnapshot(Descriptor(n), next, false, Reason::kFinalize);
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (s.ok()) {
        blob->remote = std::move(next);
        ++remote_metadata_.successes;
      }
    }
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT("MetaBypass::TierFinalHddPublished");
  const auto local_start = Micros();
  OnExit timing{[&] {
    const auto elapsed = Micros() - local_start;
    std::lock_guard<std::mutex> lock(mutex_);
    local_metadata_.micros += elapsed;
    local_metadata_.reason_us[index] += elapsed;
    metadata_us_ += elapsed;
  }};
  std::unique_lock<std::mutex> local(blob->commit);
  {
    const auto waited = Micros() - local_start;
    std::lock_guard<std::mutex> lock(mutex_);
    ++local_metadata_.attempts;
    ++local_metadata_.reasons[index];
    local_metadata_.wait += waited;
    local_metadata_.reason_wait[index] += waited;
  }
  Status s = target()->DeleteFile(LocalDescriptor(n), IOOptions(), nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  if (s.ok()) TrackMetadataBytes(LocalDescriptor(n), 0);
  if (s.ok()) {
    Status tmp =
        target()->DeleteFile(LocalDescriptor(n) + ".tmp", IOOptions(), nullptr);
    if (!tmp.ok() && !tmp.IsNotFound() && !tmp.IsPathNotFound())
      s = tmp;
    else
      TrackMetadataBytes(LocalDescriptor(n) + ".tmp", 0);
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierFinalLocalRemoved", &s);
  if (s.ok()) {
    const auto sync_start = Micros();
    s = SyncDir(target(), options_.staging_dir);
    const auto elapsed = Micros() - sync_start;
    std::lock_guard<std::mutex> lock(mutex_);
    ++local_metadata_.dirsyncs;
    local_metadata_.dirsync_us += elapsed;
    local_metadata_.reason_dirsync_us[index] += elapsed;
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierFinalLocalSynced", &s);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++local_metadata_.successes;
    blob->local_present = false;
    blob->complete_ready = true;
    progress_.notify_all();
  }
  return s;
}
Status TieredStorage::Register(uint64_t n, const std::shared_ptr<Blob>& blob,
                               bool local) {
  const auto phase_start = Micros();
  OnExit phase{[&, this] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& counter = local ? local_metadata_ : remote_metadata_;
    ++counter.registers;
    counter.register_us += Micros() - phase_start;
  }};
  ActiveExtent active;
  active.extent.local = local;
  active.extent.name = NewName(n);
  active.extent.origin = active.extent.name;
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
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierTailCreated", &s);
  // Register an existing durable empty identity, never a future path.
  if (s.ok()) s = active.file->Fsync(IOOptions(), nullptr);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierTailIdentitySynced", &s);
  if (s.ok())
    s = local ? SyncDir(target(), options_.staging_dir) : SyncCommitDirectory();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierBeforeTailRegister", &s);
  const auto extent = active.extent;
  if (s.ok())
    s = Commit(
        n, blob,
        [extent](Version* v) {
          if (v->size != extent.start ||
              (!v->extents.empty() && v->extents.back().open))
            return Status::Corruption("tail registration overlaps");
          v->extents.push_back(extent);
          return Status::OK();
        },
        Reason::kRegister);
  if (s.ok()) {
    if (Adaptive()) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (blob->has_admission && blob->last_admission_local != local) {
        if (local)
          ++to_ssd_;
        else
          ++to_hdd_;
      }
      blob->has_admission = true;
      blob->last_admission_local = local;
    }
    blob->caller = std::move(active);
    TEST_SYNC_POINT("MetaBypass::TierTailRegistered");
  } else if (active.file) {
    // Registration may have renamed before its final directory sync failed.
    // Keep the file even when the publication result is uncertain.
    s.UpdateIfOk(active.file->Close(IOOptions(), nullptr));
  }
  return s;
}
Status TieredStorage::Seal(ActiveExtent* active, bool incremental) {
  Status s;
  if (!active->synced) {
    const auto start = Micros();
    if (!active->extent.local) {
      TEST_SYNC_POINT("MetaBypass::TierBeforeHddExtentFsync");
    }
    s = active->file->Fsync(IOOptions(), nullptr);
    if (!active->extent.local) {
      std::lock_guard<std::mutex> lock(mutex_);
      ++extent_fsyncs_;
      extent_fsync_us_ += Micros() - start;
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierPayloadSynced", &s);
    if (!s.ok()) return s;
    active->synced = true;
  }
  if (!active->extent.local) {
    do {
      if (active->readback_offset == active->extent.length) break;
      const uint64_t end =
          active->readback_offset +
          std::min(kChunkSize, active->extent.length - active->readback_offset);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!error_.ok()) return error_;
      }
      // Readback is bulk even when a durability barrier requests the seal.
      HddPurposeScope purpose(HddPurpose::kBulk);
      s = ExtendDigest(target(), Path(active->extent), active->readback_offset,
                       end, &active->readback_crc);
      if (!s.ok()) return s;
      active->readback_offset = end;
      TEST_SYNC_POINT("MetaBypass::TierReadbackChunk");
      if (incremental && end < active->extent.length)
        return Status::TryAgain("seal readback quantum completed");
    } while (active->readback_offset < active->extent.length);
    if (active->readback_crc != active->crc)
      s = Status::Corruption("HDD extent checksum");
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierPayloadVerified", &s);
  }
  if (s.ok()) {
    s = active->file->Close(IOOptions(), nullptr);
    if (s.ok()) active->file.reset();
  }
  if (s.ok()) {
    active->extent.open = false;
    active->extent.pending = false;
    active->extent.durable = active->extent.length;
    active->extent.crc = active->crc;
    active->extent.frozen_at = Micros();
  }
  if (active->extent.local) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierLocalExtentSynced", &s);
  } else {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierExtentSynced", &s);
  }
  return s;
}
Status TieredStorage::EnqueueSeal(uint64_t n, const std::shared_ptr<Blob>& blob,
                                  bool install_ssd) {
  // Caller holds operation, but neither append nor either media commit lock.
  // A list node is allocated before any durable ownership transfer; splice
  // below is allocation-free. Capacity includes reservation and in-flight work.
  std::list<PendingSeal> prepared;
  prepared.push_back({n, blob, {}});
  prepared.front().barrier = HddPurposeScope::Current() == HddPurpose::kBarrier;
  const std::string identity = blob->caller.extent.name;
  const auto begin = Micros();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (pending_count_ >= 8 || blob->pending_seals >= 2) {
      TEST_SYNC_POINT("MetaBypass::TierSealQueueWaiting");
    }
    progress_.wait(lock, [&] {
      return !error_.ok() || stopping_ || blob->retiring ||
             (pending_count_ < 8 && blob->pending_seals < 2);
    });
    pending_wait_us_ += Micros() - begin;
    if (!error_.ok()) return error_;
    if (stopping_) return Status::ShutdownInProgress();
    if (blob->retiring) return Status::Aborted("retiring tier blob");
    if (!blob->caller.file || blob->caller.extent.name != identity)
      return Status::Corruption("pending caller identity changed");
    ++pending_count_;
    ++blob->pending_seals;
    peak_pending_count_ = std::max(peak_pending_count_, pending_count_);
  }
  bool transferred = false;
  OnExit release{[&] {
    if (transferred) return;
    std::lock_guard<std::mutex> lock(mutex_);
    --pending_count_;
    --blob->pending_seals;
    progress_.notify_all();
  }};
  ActiveExtent replacement;
  Status s;
  if (install_ssd) {
    replacement.extent.local = true;
    replacement.extent.start =
        blob->caller.extent.start + blob->caller.extent.length;
    replacement.extent.name = NewName(n);
    replacement.extent.origin = replacement.extent.name;
    if (replacement.extent.name.empty())
      return Status::Corruption("tier extent id overflow");
    FileOptions options = blob->file_options;
    options.use_direct_writes = false;
    s = target()->NewWritableFile(Path(replacement.extent), options,
                                  &replacement.file, nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierHandoffSsdCreated", &s);
    if (s.ok()) s = replacement.file->Fsync(IOOptions(), nullptr);
    if (s.ok()) s = SyncDir(target(), options_.staging_dir);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierHandoffSsdSynced", &s);
  }
  Extent pending = blob->caller.extent;
  pending.open = false;
  pending.pending = true;
  pending.crc = blob->caller.crc;
  const Extent next = replacement.extent;
  if (s.ok())
    s = Commit(
        n, blob,
        [pending, next, install_ssd](Version* v) {
          auto it = std::find_if(
              v->extents.begin(), v->extents.end(),
              [&](const Extent& e) { return e.name == pending.name; });
          if (it == v->extents.end() || !it->open || it + 1 != v->extents.end())
            return Status::Corruption("pending seal lost active identity");
          *it = pending;
          v->size = pending.start + pending.length;
          if (install_ssd) v->extents.push_back(next);
          return Status::OK();
        },
        Reason::kFreeze);
  if (!s.ok()) {
    if (replacement.file)
      s.UpdateIfOk(replacement.file->Close(IOOptions(), nullptr));
    return s;
  }
  TEST_SYNC_POINT("MetaBypass::TierHandoffPersisted");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Admission and retirement/Stop are serialized by mutex_. Once either
    // drain closes admission, never create a new task pin behind its queue
    // sweep. Keep the caller pin until rejected ownership is closed below.
    prepared.front().active = std::move(blob->caller);
    prepared.front().active.extent = pending;
    blob->caller = std::move(replacement);
    if (!error_.ok())
      s = error_;
    else if (stopping_)
      s = Status::ShutdownInProgress();
    else if (blob->retiring)
      s = Status::Aborted("retiring tier blob");
    else {
      ++blob->operations;
      pending_seals_.splice(pending_seals_.end(), prepared);
      if (install_ssd) {
        if (blob->has_admission && !blob->last_admission_local) ++to_ssd_;
        blob->has_admission = true;
        blob->last_admission_local = true;
        const auto response = pressure_->PressureAge();
        pressure_response_us_ += response;
        max_pressure_response_us_ =
            std::max(max_pressure_response_us_, response);
      }
      transferred = true;
      work_.notify_one();
    }
  }
  if (!s.ok()) {
    // MBL may already reference both files. Close without unlinking either;
    // recovery or GC owns their disposition. Never hold mutex_ across HDD I/O.
    Status close = prepared.front().active.file->Close(IOOptions(), nullptr);
    if (!close.ok()) Fail(close);
    return s;
  }
  TEST_SYNC_POINT("MetaBypass::TierHandoffQueued");
  return Status::OK();
}
Status TieredStorage::FinishSeal(PendingSeal* task) {
  Status s = Seal(&task->active, !recovering_);
  const Extent extent = task->active.extent;
  if (s.ok() && extent.length) s = Publish(task->number, task->blob, extent);
  if (s.ok())
    s = Commit(
        task->number, task->blob,
        [extent](Version* v) {
          auto it = std::find_if(
              v->extents.begin(), v->extents.end(),
              [&](const Extent& e) { return e.name == extent.name; });
          if (it == v->extents.end() || !it->pending ||
              it->start != extent.start || it->length != extent.length ||
              it->crc != extent.crc)
            return Status::Corruption("pending seal lost range identity");
          if (extent.length)
            *it = extent;
          else
            v->extents.erase(it);
          return Status::OK();
        },
        Reason::kFreeze);
  return s;
}
void TieredStorage::DropPending(const std::shared_ptr<Blob>& selected) {
  std::list<PendingSeal> cancelled;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = pending_seals_.begin(); it != pending_seals_.end();) {
      auto next = std::next(it);
      if (!selected || selected == it->blob)
        cancelled.splice(cancelled.end(), pending_seals_, it);
      it = next;
    }
  }
  for (auto& task : cancelled) {
    if (task.active.file) {
      Status s = task.active.file->Close(IOOptions(), nullptr);
      if (!s.ok()) Fail(s);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    --task.blob->operations;
    --task.blob->pending_seals;
    --pending_count_;
    progress_.notify_all();
  }
}
Status TieredStorage::Freeze(uint64_t n, const std::shared_ptr<Blob>& blob) {
  auto& active = blob->caller;
  if (!active.file) return Status::OK();
  const bool local = active.extent.local;
  if (!recovering_ && !local) return EnqueueSeal(n, blob);
  const auto phase_start = Micros();
  OnExit phase{[&, this] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& counter = local ? local_metadata_ : remote_metadata_;
    ++counter.freezes;
    counter.freeze_us += Micros() - phase_start;
  }};
  Status s = Seal(&active);
  if (!s.ok()) return s;
  const auto extent = active.extent;
  if (!extent.local && extent.length) s = Publish(n, blob, extent);
  if (!s.ok()) return s;
  s = Commit(
      n, blob,
      [extent](Version* v) {
        if (v->extents.empty() || v->extents.back().name != extent.name ||
            !v->extents.back().open)
          return Status::Corruption("freeze lost tail identity");
        // Recovery can find a registered empty tail. It owns no logical
        // range and must not become an ambiguous zero-length HDD range.
        if (extent.length)
          v->extents.back() = extent;
        else
          v->extents.pop_back();
        v->size = extent.start + extent.length;
        return Status::OK();
      },
      Reason::kFreeze);
  if (s.ok()) {
    if (!extent.length && extent.local) {
      std::lock_guard<std::mutex> lock(mutex_);
      retired_local_[extent.name] = 0;
      work_.notify_one();
    }
    active = ActiveExtent();
    TEST_SYNC_POINT("MetaBypass::TierSegmentFrozen");
  }
  return s;
}
Status TieredStorage::MergeRecovered(uint64_t n, const Version* local,
                                     const Version* remote, Version* merged) {
  if (!local) {
    if (!remote || !Complete(*remote) || remote->retired)
      return Status::Corruption("missing local recovery state");
    *merged = *remote;
    return Status::OK();
  }
  *merged = *local;
  // An applied HDD range is valid only with its independently durable proof.
  // Pure SSD maps and registered, not-yet-published HDD tails need no MBT.
  for (const auto& applied : local->extents) {
    if (applied.local || applied.open || applied.pending) continue;
    if (!remote) return Status::Corruption("missing HDD publication proof");
    auto proof = std::lower_bound(
        remote->extents.begin(), remote->extents.end(), applied.start,
        [](const Extent& e, uint64_t start) { return e.start < start; });
    if (proof == remote->extents.end() || proof->start != applied.start ||
        proof->name != applied.name || proof->origin != applied.origin ||
        proof->length != applied.length || proof->crc != applied.crc)
      return Status::Corruption("missing applied HDD range proof");
  }
  if (!remote) return Status::OK();
  if (local->identity != remote->identity ||
      local->incarnation != remote->incarnation || local->number != n ||
      remote->number != n || remote->retired)
    return Status::Corruption("tier recovery identity conflict");
  for (const auto& hdd : remote->extents) {
    auto it =
        std::find_if(merged->extents.begin(), merged->extents.end(),
                     [&](const Extent& e) { return e.start == hdd.start; });
    if (it == merged->extents.end())
      return Status::Corruption("unregistered HDD range");
    const bool suffix = !it->local && (it->open || it->pending) &&
                        it->name == hdd.name && it->origin == hdd.origin;
    const bool source = it->local && !it->open && it->name == hdd.origin;
    const bool applied = !it->local && !it->open && it->name == hdd.name &&
                         it->origin == hdd.origin;
    if ((!suffix && !source && !applied) ||
        (suffix ? (it->pending
                       ? (hdd.length != it->length || hdd.crc != it->crc)
                       : (hdd.length < it->durable || hdd.length > kExtentSize))
                : (it->length != hdd.length || it->crc != hdd.crc)))
      return Status::Corruption("HDD source proof mismatch");
    *it = hdd;
    if (suffix && it + 1 == merged->extents.end())
      merged->size = hdd.start + hdd.length;
  }
  if (remote->closed &&
      (!local->closed || remote->size != merged->size || !Complete(*merged)))
    return Status::Corruption("conflicting final length");
  return Status::OK();
}
Status TieredStorage::Preflight(bool restore) {
  return Recover(restore, false);
}
Status TieredStorage::Initialize(bool restore) {
  return Recover(restore, true);
}
Status TieredStorage::Recover(bool restore, bool apply) {
  using Snapshot = std::map<std::string, std::string>;
  const auto collect = [&](Snapshot* snapshot, std::vector<std::string>* data,
                           std::vector<std::string>* local) -> Status {
    for (bool ssd : {false, true}) {
      const auto& dir = ssd ? options_.staging_dir : options_.data_dir;
      auto* names = ssd ? local : data;
      Status st = target()->GetChildren(dir, IOOptions(), names, nullptr);
      if (st.IsNotFound() || st.IsPathNotFound()) {
        st.PermitUncheckedError();
        names->clear();
      } else if (!st.ok())
        return st;
      std::sort(names->begin(), names->end());
      std::string listing;
      for (const auto& name : *names) {
        listing += name + "\n";
        uint64_t n;
        const bool retired =
            name.size() > 8 && name.substr(name.size() - 8) == ".retired";
        if (!(ssd ? LocalName(name, &n) : IsBlob(dir + "/" + name, &n)) &&
            !retired)
          continue;
        std::string bytes;
        st = Read(target(), dir + "/" + name, &bytes);
        if (!st.ok()) return st;
        snapshot->emplace(dir + "/" + name, std::move(bytes));
      }
      snapshot->emplace(dir + "/", std::move(listing));
    }
    std::string latest;
    Status st = Read(target(), options_.backup_dir + "/LATEST", &latest);
    if (st.IsNotFound() || st.IsPathNotFound()) return Status::OK();
    if (!st.ok()) return st;
    snapshot->emplace(options_.backup_dir + "/LATEST", latest);
    std::istringstream points(latest);
    std::string name;
    unsigned roots = 0;
    while (points >> name) {
      if (++roots != 1) return Status::Corruption("multiple recovery roots");
      if (name.compare(0, 6, "point-") || name.find('/') != std::string::npos ||
          name.find("..") != std::string::npos)
        return Status::Corruption("recovery point name");
      const auto path = options_.backup_dir + "/" + name;
      std::string inventory;
      st = Read(target(), path + "/INVENTORY", &inventory);
      if (!st.ok()) return st;
      snapshot->emplace(path + "/INVENTORY", inventory);
      if (inventory.compare(0, 5, "MBI6\n"))
        return Status::Corruption("recovery inventory checksum/format");
      std::string map;
      st = Read(target(), path + "/BLOB-MAP", &map);
      if (!st.ok()) return st;
      snapshot->emplace(path + "/BLOB-MAP", std::move(map));
    }
    if (!points.eof()) return Status::Corruption("recovery point pointer");
    return Status::OK();
  };
  for (unsigned attempt = 0; attempt < 3; ++attempt) {
    Snapshot before, after;
    std::vector<std::string> data_names, local_names, after_data, after_local;
    Status status = collect(&before, &data_names, &local_names);
    std::map<uint64_t, Version> locals, remotes, retired;
    std::set<uint64_t> protected_blobs;
    std::map<uint64_t, std::shared_ptr<Blob>> parsed;
    std::map<std::string, uint64_t> garbage;
    uint64_t usage = 0, maximum_id = 0;
    const auto validate = [&]() -> Status {
      if (!status.ok()) return status;
      for (const auto& item : before) {
        uint64_t n;
        bool local = item.first.compare(0, options_.staging_dir.size() + 1,
                                        options_.staging_dir + "/") == 0;
        const auto name = item.first.substr(item.first.find_last_of('/') + 1);
        if (name == "BLOB-MAP") {
          std::map<uint64_t, Version> checkpoint;
          Status st = ParseCheckpoint(item.first, &checkpoint, !restore);
          if (!st.ok()) return st;
          const auto inventory_path =
              item.first.substr(0, item.first.find_last_of('/')) + "/INVENTORY";
          st = BindCheckpointInventory(before.at(inventory_path), item.second,
                                       checkpoint);
          if (!st.ok()) return st;
          for (const auto& row : checkpoint) protected_blobs.insert(row.first);
          continue;
        }
        bool tomb =
            name.size() > 8 && name.substr(name.size() - 8) == ".retired";
        if (!(local ? LocalName(name, &n) : IsBlob(item.first, &n)) && !tomb)
          continue;
        Version version;
        Status st = Decode(item.second, &version, local);
        if (!st.ok()) return st;
        if (version.identity != identity_)
          return Status::Corruption("tier database identity");
        if (tomb) {
          if (!version.retired ||
              Tombstone(version.number, version) != item.first ||
              !retired.emplace(version.number, version).second)
            return Status::Corruption("retirement identity");
        } else {
          if (version.number != n || version.retired ||
              !(local ? locals : remotes).emplace(n, std::move(version)).second)
            return Status::Corruption("descriptor owner");
        }
      }
      for (const auto& item : retired) {
        if (protected_blobs.count(item.first))
          return Status::Corruption("retired protected blob");
        for (const auto* versions : {&locals, &remotes}) {
          auto it = versions->find(item.first);
          if (it != versions->end() &&
              it->second.incarnation != item.second.incarnation)
            return Status::Corruption("retired incarnation conflict");
        }
      }
      for (const auto& item : locals)
        parsed[item.first] = std::make_shared<Blob>();
      for (const auto& item : remotes)
        parsed[item.first] = std::make_shared<Blob>();
      std::set<std::string> referenced;
      for (auto it = parsed.begin(); it != parsed.end();) {
        if (retired.count(it->first)) {
          it = parsed.erase(it);
          continue;
        }
        if (restore) {
          ++it;
          continue;
        }
        const auto l = locals.find(it->first), r = remotes.find(it->first);
        auto blob = it->second;
        Status st = MergeRecovered(
            it->first, l == locals.end() ? nullptr : &l->second,
            r == remotes.end() ? nullptr : &r->second, &blob->live);
        if (!st.ok()) return st;
        if (l != locals.end()) {
          for (const auto& old : l->second.extents) {
            if (!old.local) continue;
            auto replacement =
                std::find_if(blob->live.extents.begin(),
                             blob->live.extents.end(), [&](const Extent& e) {
                               return e.start == old.start && !e.local;
                             });
            if (replacement == blob->live.extents.end()) continue;
            uint64_t size;
            st = target()->GetFileSize(Path(old), IOOptions(), &size, nullptr);
            if (st.IsNotFound() || st.IsPathNotFound()) {
              st.PermitUncheckedError();
              continue;
            }
            if (!st.ok()) return st;
            if (size != old.length)
              return Status::Corruption("superseded SSD length");
            uint32_t crc;
            st = Digest(target(), Path(old), size, &crc);
            if (!st.ok()) return st;
            if (crc != old.crc)
              return Status::Corruption("superseded SSD checksum");
          }
        }
        blob->durable = blob->live;
        if (r != remotes.end())
          blob->remote = r->second;
        else {
          blob->remote.identity = identity_;
          blob->remote.number = it->first;
          blob->remote.incarnation = blob->live.incarnation;
        }
        blob->local_present = l != locals.end();
        blob->complete_ready =
            r != remotes.end() && r->second.closed && !blob->local_present;
        for (auto& e : blob->live.extents) {
          uint64_t size;
          st = target()->GetFileSize(Path(e), IOOptions(), &size, nullptr);
          if (!st.ok()) return st;
          if (size < e.durable || size > kExtentSize ||
              (!e.open && size != e.length))
            return Status::Corruption("tier physical range length");
          if (e.open) {
            e.length = size;
            blob->live.size = e.start + size;
          } else {
            uint32_t crc;
            st = Digest(target(), Path(e), size, &crc);
            if (!st.ok()) return st;
            if (crc != e.crc)
              return Status::Corruption("tier physical checksum");
          }
          if (e.local) referenced.insert(e.name);
        }
        ++it;
      }
      for (bool local : {false, true}) {
        for (const auto& name : local ? local_names : data_names) {
          uint64_t n, id;
          std::string incarnation;
          if (local) {
            FileType type;
            if (ParseFileName(name, &n, &type) && type == kBlobFile)
              return Status::Corruption("legacy raw staging layout");
          }
          if (!SegmentName(name, &n, &id, &incarnation)) continue;
          maximum_id = std::max(maximum_id, id);
          if (!local) continue;
          if (restore)
            return Status::InvalidArgument("restore requires empty staging");
          uint64_t size;
          Status st = target()->GetFileSize(options_.staging_dir + "/" + name,
                                            IOOptions(), &size, nullptr);
          if (!st.ok()) return st;
          if (size > options_.staging_capacity - usage)
            return Status::InvalidArgument("existing staging exceeds capacity");
          usage += size;
          if (!referenced.count(name) && !retired.count(n))
            garbage[name] = size;
        }
      }
      return Status::OK();
    };
    Status checked = validate();
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierPreflightDependenciesChecked",
                             &checked);
    Status recollected = collect(&after, &after_data, &after_local);
    if (!recollected.ok() && recollected.IsCorruption() && before == after &&
        status.IsCorruption())
      return status;
    if (!recollected.ok() || before != after) {
      checked.PermitUncheckedError();
      status.PermitUncheckedError();
      recollected.PermitUncheckedError();
      if (attempt + 1 != 3) continue;
      return Status::TryAgain("tier namespace changed during preflight");
    }
    if (!checked.ok()) return checked;
    if (!apply) return Status::OK();
    restoring_ = restore;
    Status st = EnsureDir(target(), options_.staging_dir);
    if (st.ok())
      st = target()->LockFile(options_.staging_dir + "/METABYPASS-LOCK",
                              IOOptions(), &staging_lock_, nullptr);
    if (!st.ok()) return st;
    st = RefreshMetadataBytes();
    if (!st.ok()) return st;
    if (restore) return Status::OK();
    blobs_ = std::move(parsed);
    usage_ = usage;
    peak_ = usage;
    extent_id_ = maximum_id;
    retired_local_ = std::move(garbage);
    recovering_ = true;
    OnExit recovered_mode{[&] { recovering_ = false; }};
    // All canonical metadata and required dependencies passed before writes.
    for (const auto& item : retired) {
      auto blob = std::make_shared<Blob>();
      blob->live = item.second;
      blob->durable = item.second;
      blob->remote = item.second;
      blob->retiring = true;
      uint64_t bytes;
      st = DeleteRetiredBlob(item.first, blob, &bytes);
      if (!st.ok()) return st;
    }
    for (const auto& item : blobs_) {
      auto blob = item.second;
      if (blob->complete_ready) continue;
      // Reconcile the already-validated remote deltas before sealing tails.
      const Version recovered = blob->durable;
      blob->local_present = false;
      st = Commit(
          item.first, blob,
          [recovered](Version* v) {
            const bool open = !v->extents.empty() && v->extents.back().open;
            const auto tail = open ? v->extents.back() : Extent();
            *v = recovered;
            if (open && !v->extents.empty() && v->extents.back().open &&
                tail.name == v->extents.back().name &&
                tail.length > v->extents.back().length) {
              v->extents.back().length = tail.length;
              v->size = tail.start + tail.length;
            }
            return Status::OK();
          },
          Reason::kRecovery);
      if (!st.ok()) return st;
      // Preflight validated every pending length and CRC before any mutation.
      // Rebuild one writer at a time, without waiting on a not-yet-started
      // worker.
      const auto ranges = blob->live.extents;
      for (const auto& extent : ranges) {
        if (!extent.pending) continue;
        PendingSeal task{item.first, blob, {}};
        task.active.extent = extent;
        task.active.crc = extent.crc;
        st = target()->ReopenWritableFile(Path(extent), FileOptions(),
                                          &task.active.file, nullptr);
        if (st.ok()) st = FinishSeal(&task);
        if (task.active.file)
          st.UpdateIfOk(task.active.file->Close(IOOptions(), nullptr));
        if (!st.ok()) return st;
      }
      if (!blob->live.extents.empty() && blob->live.extents.back().open) {
        auto& active = blob->caller;
        active.extent = blob->live.extents.back();
        st = Digest(target(), Path(active.extent), active.extent.length,
                    &active.crc);
        if (st.ok())
          st = target()->ReopenWritableFile(Path(active.extent), FileOptions(),
                                            &active.file, nullptr);
        if (st.ok()) st = Freeze(item.first, blob);
        if (!st.ok()) return st;
      }
      st = Commit(
          item.first, blob,
          [](Version* v) {
            v->closed = true;
            return Status::OK();
          },
          Reason::kRecovery);
      if (st.ok() && Complete(blob->durable)) st = Finalize(item.first, blob);
      if (!st.ok()) return st;
    }
    return Status::OK();
  }
  return Status::TryAgain("tier preflight");
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
  TEST_SYNC_POINT("MetaBypass::TierStopping");
  if (worker_.joinable()) worker_.join();
  DropPending();
  TEST_SYNC_POINT("MetaBypass::TierStopQueueDrained");
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
  if (owns_observer_) pressure_->Stop();
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
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error_.ok()) error_ = s;
    progress_.notify_all();
    work_.notify_all();
  }
  TEST_SYNC_POINT("MetaBypass::TierCancelled");
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
  auto blob = std::make_shared<Blob>();
  const auto incarnation = Hex(Env::Default()->GenerateUniqueId());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!error_.ok()) return IO(error_);
    if (stopping_) return IO(Status::ShutdownInProgress());
    if (identity_.empty())
      return IOStatus::InvalidArgument("tier identity not bound");
    if (blobs_.count(n))
      return IOStatus::Corruption("tiered blob number reused");
    blob->live.identity = identity_;
    blob->live.incarnation = incarnation;
    blob->live.number = n;
    blob->durable = blob->live;
    blob->remote = blob->live;
    blob->file_options = o;
    ++blob->operations;
    blobs_[n] = blob;
  }
  Status s = Commit(
      n, blob, [](Version*) { return Status::OK(); }, Reason::kRegister);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    --blob->operations;
    progress_.notify_all();
  }
  if (!s.ok()) {
    Fail(s);
    return IO(s);
  }
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
  // A background persistence failure must not hide the readable live view.
  Status s = pin.Open(this, n, true /* read_only */);
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
  if (local) {
    TEST_SYNC_POINT("MetaBypass::TierLocalReadPinned");
  } else {
    TEST_SYNC_POINT("MetaBypass::TierRemoteReadPinned");
  }
  return ReadVersion(version, offset, size, o, out, scratch, d);
}
Status TieredStorage::Wait(uint64_t n, const std::shared_ptr<Blob>& blob,
                           uint64_t length, bool closed, bool hdd) {
  const auto start = Micros();
  std::unique_lock<std::mutex> lock(mutex_);
  auto request =
      barriers_.insert(barriers_.end(), Barrier{n, length, hdd, closed});
  requests_[n] = std::max(requests_[n], length);
  const auto end = DurableEnd(blob->durable, hdd);
  (hdd ? h_debt_ : d_debt_) += length - std::min(length, end);
  work_.notify_one();
  TEST_SYNC_POINT("MetaBypass::TierBarrierWaiting");
  progress_.wait(lock, [&] {
    return !error_.ok() || stopping_ || blob->retiring ||
           (DurableEnd(blob->durable, hdd) >= length &&
            (!closed || (hdd ? blob->complete_ready : blob->durable.closed)));
  });
  barrier_us_ += Micros() - start;
  barriers_.erase(request);
  requests_.erase(n);
  for (const auto& barrier : barriers_)
    if (barrier.number == n)
      requests_[n] = std::max(requests_[n], barrier.length);
  return !error_.ok()     ? error_
         : stopping_      ? Status::ShutdownInProgress()
         : blob->retiring ? Status::Aborted("retiring tier blob")
                          : Status::OK();
}
Status TieredStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  std::vector<std::list<Barrier>::iterator> barriers;
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
  }
  for (const auto& item : lengths) {
    const auto& blob = *blobs_.at(item.first);
    if (blob.complete_ready)
      ++persist_skips_;
    else {
      barriers.push_back(barriers_.insert(
          barriers_.end(), Barrier{item.first, item.second, true, true}));
      requests_[item.first] = std::max(requests_[item.first], item.second);
      const auto end = DurableEnd(blob.durable, true);
      h_debt_ += item.second - std::min(item.second, end);
    }
  }
  TEST_SYNC_POINT("MetaBypass::TierPersistRequested");
  work_.notify_one();
  progress_.wait(lock, [&] {
    if (!error_.ok() || stopping_) return true;
    for (const auto& pin : pins)
      if (pin->blob->retiring) return true;
    for (const auto& pin : pins)
      if (!pin->blob->complete_ready) return false;
    return true;
  });
  Status s = !error_.ok() ? error_
             : stopping_  ? Status::ShutdownInProgress()
                          : Status::OK();
  if (s.ok())
    for (const auto& pin : pins)
      if (pin->blob->retiring) s = Status::Aborted("retiring tier blob");
  for (auto barrier : barriers) barriers_.erase(barrier);
  for (const auto& item : lengths) {
    requests_.erase(item.first);
    for (const auto& barrier : barriers_)
      if (barrier.number == item.first)
        requests_[item.first] = std::max(requests_[item.first], barrier.length);
  }
  lock.unlock();
  return s;
}
Status TieredStorage::Migrate(uint64_t n, const std::shared_ptr<Blob>& blob) {
  TEST_SYNC_POINT("MetaBypass::TierMigration");
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierMigrationSelected", &n);
  Extent source;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(blob->live.extents.begin(), blob->live.extents.end(),
                           [](const Extent& e) { return e.local && !e.open; });
    if (it != blob->live.extents.end()) source = *it;
  }
  if (source.name.empty()) return Finalize(n, blob);
  auto& active = blob->migration;
  if (blob->migration_source.empty()) {
    active.extent.start = source.start;
    active.extent.origin = source.name;
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
  Status s = Seal(&active, Adaptive());
  if (s.IsTryAgain()) return Status::OK();
  const auto publish_start = Micros();
  if (s.ok()) s = Publish(n, blob, active.extent);
  TEST_SYNC_POINT("MetaBypass::TierMigrationSealed");
  const auto destination = active.extent;
  if (s.ok())
    s = Commit(
        n, blob,
        [source, destination](Version* v) {
          auto it = std::find_if(
              v->extents.begin(), v->extents.end(),
              [&](const Extent& e) { return e.name == source.name; });
          if (it == v->extents.end() || !it->local || it->open ||
              it->start != source.start || it->length != source.length)
            return Status::Corruption("migration lost source identity");
          *it = destination;
          return Status::OK();
        },
        Reason::kMigration);
  if (s.ok()) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      retired_local_[source.name] = source.length;
    }
    active = ActiveExtent();
    blob->migration_source.clear();
    TEST_SYNC_POINT("MetaBypass::TierRouteDurable");
    TEST_SYNC_POINT("MetaBypass::TierBeforeEvict");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++migration_publish_calls_;
    migration_publish_us_ += Micros() - publish_start;
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
  uint64_t next_migration = 0;
  std::unique_lock<std::mutex> lock(mutex_);
  const auto ready = [&](const std::shared_ptr<Blob>& blob) {
    return !blob->retiring &&
           ((Complete(blob->durable) && !blob->complete_ready) ||
            std::any_of(blob->live.extents.begin(), blob->live.extents.end(),
                        [](const Extent& e) { return e.local && !e.open; }));
  };
  const auto reclaimable = [&] {
    for (const auto& item : retired_local_)
      if (!readers_.count(item.first)) return true;
    return false;
  };
  while (error_.ok() && !stopping_) {
    work_.wait(lock, [&] {
      if (stopping_ || !error_.ok() || reclaimable() || !pending_seals_.empty())
        return true;
      for (const auto& item : blobs_)
        if (ready(item.second)) return true;
      return false;
    });
    if (stopping_ || !error_.ok()) break;
    // Return one published-away SSD range's quota before scheduling more HDD
    // work, even when a barrier is still pending. Keep each turn bounded so
    // a reclamation backlog cannot drain ahead of that work.
    if (reclaimable()) {
      lock.unlock();
      Status s = ReclaimLocal();
      if (!s.ok()) Fail(s);
      lock.lock();
      if (stopping_ || !error_.ok()) break;
    }
    // Resolve each independently captured barrier to its earliest unsatisfied
    // range. An H request with an SSD hole must not be delayed by sealing a
    // later HDD range; D may skip an already durable SSD range.
    uint64_t priority_blob = 0;
    bool has_priority = false;
    std::string priority_pending;
    for (const auto& barrier : barriers_) {
      const auto found = blobs_.find(barrier.number);
      if (found == blobs_.end() || found->second->retiring) continue;
      const auto& blob = *found->second;
      for (const auto& e : blob.durable.extents) {
        if (e.start >= barrier.length) break;
        if (e.durable == e.length && (!barrier.hdd || (!e.local && !e.pending)))
          continue;
        if (e.pending || (barrier.hdd && e.local && !e.open)) {
          priority_blob = barrier.number;
          has_priority = true;
          if (e.pending) priority_pending = e.name;
        }
        break;
      }
      if (!has_priority && barrier.closed && !blob.complete_ready &&
          Complete(blob.durable)) {
        priority_blob = barrier.number;
        has_priority = true;
      }
      if (has_priority) break;
    }
    auto pending = pending_seals_.end();
    if (!has_priority && !pending_seals_.empty())
      pending = pending_seals_.begin();
    else if (!priority_pending.empty()) {
      pending =
          std::find_if(pending_seals_.begin(), pending_seals_.end(),
                       [&](const PendingSeal& task) {
                         return task.active.extent.name == priority_pending;
                       });
    }
    if (pending != pending_seals_.end()) {
      std::list<PendingSeal> running;
      running.splice(running.end(), pending_seals_, pending);
      auto& task = running.front();
      const bool barrier = task.barrier || requests_.count(task.number) != 0;
      lock.unlock();
      HddPurposeScope purpose(barrier ? HddPurpose::kBarrier
                                      : HddPurpose::kBulk);
      Status s = FinishSeal(&task);
      if (s.IsTryAgain()) {
        TEST_SYNC_POINT("MetaBypass::TierSealBeforeRequeue");
        lock.lock();
        // GC may have swept the queue while this readback quantum ran.
        // Requeue only while admission is open, under the same mutex used by
        // retirement/Stop; otherwise close and release the existing pin below.
        if (!stopping_ && error_.ok() && !task.blob->retiring) {
          pending_seals_.splice(pending_seals_.end(), running);
          continue;
        }
        lock.unlock();
        s = Status::OK();
      }
      if (!s.ok() && !s.IsAborted()) Fail(s);
      if (task.active.file) {
        Status close = task.active.file->Close(IOOptions(), nullptr);
        if (!close.ok()) Fail(close);
        task.active.file.reset();
      }
      lock.lock();
      --task.blob->operations;
      --task.blob->pending_seals;
      --pending_count_;
      TEST_SYNC_POINT("MetaBypass::TierSealCompleted");
      progress_.notify_all();
      continue;
    }
    auto selected = blobs_.end();
    if (has_priority && priority_pending.empty()) {
      auto requested = blobs_.find(priority_blob);
      if (requested != blobs_.end() && ready(requested->second))
        selected = requested;
    }
    auto it = blobs_.upper_bound(cursor);
    for (size_t count = 0; selected == blobs_.end() && count < blobs_.size();
         ++count) {
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
    priority = true;
    cursor = selected->first;
    const auto n = selected->first;
    auto blob = selected->second;
    const bool barrier = requests_.count(n) != 0;
    const bool copying =
        std::any_of(blob->live.extents.begin(), blob->live.extents.end(),
                    [](const Extent& e) { return e.local && !e.open; });
    if (Adaptive() && copying && !blob->migration.synced && !barrier) {
      auto now = Micros();
      TEST_SYNC_POINT_CALLBACK("MetaBypass::TierMigrationClock", &now);
      if (now < next_migration) {
        auto delay = next_migration - now;
        TEST_SYNC_POINT_CALLBACK("MetaBypass::TierThrottleDelay", &delay);
        const auto wait_started = Micros();
        TEST_SYNC_POINT("MetaBypass::TierThrottleWaiting");
        [[maybe_unused]] bool notified =
            work_.wait_for(lock, std::chrono::microseconds(delay)) ==
            std::cv_status::no_timeout;
        TEST_SYNC_POINT_CALLBACK("MetaBypass::TierThrottleAwakened", &notified);
        throttle_us_ += Micros() - wait_started;
        continue;
      }
      next_migration = now + (pressure_->Pressured() ? 250000 : 125000);
      TEST_SYNC_POINT_CALLBACK("MetaBypass::TierMigrationCreditGranted",
                               &cursor);
    }
    ++blob->operations;
    lock.unlock();
    HddPurposeScope purpose(barrier ? HddPurpose::kBarrier : HddPurpose::kBulk);
    Status s = Migrate(n, blob);
    if (!s.ok() && !s.IsAborted()) Fail(s);
    TEST_SYNC_POINT("MetaBypass::TierMigrationComplete");
    lock.lock();
    --blob->operations;
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
    lock.unlock();
    DropPending(blob);
    TEST_SYNC_POINT("MetaBypass::TierGcQueueDrained");
    lock.lock();
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
Status TieredStorage::CheckRetirement(uint64_t number) {
  std::string latest;
  Status s = Read(target(), options_.backup_dir + "/LATEST", &latest);
  if (s.IsNotFound() || s.IsPathNotFound()) return Status::OK();
  if (!s.ok()) return s;
  std::istringstream points(latest);
  std::string point;
  unsigned roots = 0;
  while (points >> point) {
    if (++roots != 1) return Status::Corruption("multiple recovery roots");
    if (point.compare(0, 6, "point-") || point.find('/') != std::string::npos ||
        point.find("..") != std::string::npos)
      return Status::Corruption("retirement point identity");
    std::string inventory;
    s = Read(target(), options_.backup_dir + "/" + point + "/INVENTORY",
             &inventory);
    if (!s.ok()) return s;
    if (inventory.compare(0, 5, "MBI6\n"))
      return Status::Corruption("retirement inventory checksum");
    const auto path = options_.backup_dir + "/" + point + "/BLOB-MAP";
    std::string map;
    s = Read(target(), path, &map);
    if (!s.ok()) return s;
    std::map<uint64_t, Version> versions;
    s = ParseCheckpoint(path, &versions, false);
    if (s.ok()) s = BindCheckpointInventory(inventory, map, versions);
    if (!s.ok()) return s;
    if (versions.count(number))
      return Status::Corruption("retirement of protected blob");
  }
  return points.eof() ? Status::OK() : Status::Corruption("retirement pointer");
}
Status TieredStorage::DeleteRetiredBlob(uint64_t n,
                                        const std::shared_ptr<Blob>& blob,
                                        uint64_t* bytes) {
  Status s = CheckRetirement(n);
  if (!s.ok()) return s;
  std::lock_guard<std::mutex> reclaim(reclamation_);
  for (auto* active : {&blob->caller, &blob->migration}) {
    if (!active->file) continue;
    s = active->file->Close(IOOptions(), nullptr);
    if (!s.ok()) return s;
    active->file.reset();
  }
  struct Payload {
    std::string path, name;
    uint64_t size;
    bool local;
  };
  std::vector<Payload> payloads;
  Version tomb;
  tomb.identity = identity_;
  tomb.number = n;
  tomb.incarnation = blob->live.incarnation;
  if (blob->remote.generation == UINT64_MAX)
    return Status::Corruption("retirement generation overflow");
  tomb.generation = blob->remote.generation + 1;
  tomb.retired = true;
  for (bool local : {false, true}) {
    const auto& dir = local ? options_.staging_dir : options_.data_dir;
    std::vector<std::string> names;
    s = target()->GetChildren(dir, IOOptions(), &names, nullptr);
    if (!s.ok()) return s;
    for (const auto& name : names) {
      uint64_t owner, id, size;
      std::string incarnation;
      if (!SegmentName(name, &owner, &id, &incarnation) || owner != n) continue;
      if (tomb.incarnation.empty()) tomb.incarnation = incarnation;
      if (tomb.incarnation != incarnation)
        return Status::Corruption("GC incarnation conflict");
      s = target()->GetFileSize(dir + "/" + name, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      payloads.push_back({dir + "/" + name, name, size, local});
    }
  }
  if (tomb.incarnation.empty())
    tomb.incarnation = Hex(Env::Default()->GenerateUniqueId());
  *bytes = 0;
  for (const auto& e : payloads) *bytes += e.size;
  const auto marker = Tombstone(n, tomb);
  // All accepted operations drained before this HDD-only retirement commit.
  {
    const auto start = Micros();
    std::unique_lock<std::mutex> remote(blob->remote_commit);
    {
      const auto waited = Micros() - start;
      std::lock_guard<std::mutex> lock(mutex_);
      remote_metadata_.wait += waited;
      remote_metadata_.reason_wait[static_cast<size_t>(Reason::kGC)] += waited;
    }
    s = StoreSnapshot(marker, tomb, false, Reason::kGC);
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRetirementSynced", &s);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::BeforeBlobDelete");
  const auto remove = [&](const std::string& path) -> Status {
    Status st = target()->DeleteFile(path, IOOptions(), nullptr);
    if (st.IsNotFound() || st.IsPathNotFound()) return Status::OK();
    return st;
  };
  for (const auto& file :
       {Descriptor(n), LocalDescriptor(n), Descriptor(n) + ".tmp",
        LocalDescriptor(n) + ".tmp", marker + ".tmp"}) {
    uint64_t length;
    Status stat = target()->GetFileSize(file, IOOptions(), &length, nullptr);
    if (stat.ok())
      *bytes += length;
    else if (!stat.IsNotFound() && !stat.IsPathNotFound())
      return stat;
    s = remove(file);
    if (!s.ok()) return s;
    if (file == LocalDescriptor(n) || file == LocalDescriptor(n) + ".tmp")
      TrackMetadataBytes(file, 0);
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobDescriptorRemoved", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  TEST_SYNC_POINT("MetaBypass::BlobDescriptorSynced");
  if (!s.ok()) return s;
  for (const auto& e : payloads) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobPayloadDeleteStatus", &s);
    if (s.ok()) s = remove(e.path);
    if (!s.ok()) return s;
    TEST_SYNC_POINT("MetaBypass::BlobPayloadRemoved");
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobFinalDirSyncStatus", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::TierRetirementPayloadSynced");
  s = remove(marker);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRetirementRemoved", &s);
  if (!s.ok()) return s;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& e : payloads)
      if (e.local) {
        usage_ -= e.size;
        retired_local_.erase(e.name);
      }
  }
  return Status::OK();
}
Status TieredStorage::SaveCheckpoint(
    const std::string& path, const std::map<uint64_t, uint64_t>& lengths) {
  std::ostringstream body;
  body << identity_ << ' ' << lengths.size() << '\n';
  std::vector<std::unique_ptr<Pin>> pins;
  for (const auto& item : lengths) {
    auto pin = std::make_unique<Pin>();
    Status s = pin->Open(this, item.first);
    if (!s.ok()) return s;
    Version version;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      version = pin->blob->remote;
      if (!pin->blob->complete_ready || !Complete(version) ||
          version.size != item.second)
        return Status::Corruption("checkpoint requires durable closed blob");
      pin->Ranges(version);
    }
    const auto encoded = Encode(version, false);
    body << item.first << ' ' << encoded.size() << '\n' << encoded;
    pins.push_back(std::move(pin));
  }
  const auto bytes = body.str();
  return Write(target(), path,
               "MBTC5 " +
                   std::to_string(crc32c::Value(bytes.data(), bytes.size())) +
                   "\n" + bytes);
}
Status TieredStorage::ParseCheckpoint(const std::string& path,
                                      std::map<uint64_t, Version>* parsed,
                                      bool validate_payload) {
  std::string bytes;
  Status s = Read(target(), path, &bytes);
  if (!s.ok()) return s;
  const auto split = bytes.find('\n');
  if (split == std::string::npos)
    return Status::Corruption("tier checkpoint header");
  std::string magic, extra, identity;
  uint32_t checksum;
  std::istringstream header(bytes.substr(0, split));
  if (!(header >> magic >> checksum) || magic != "MBTC5" || (header >> extra))
    return Status::Corruption("tier checkpoint format");
  const auto body = bytes.substr(split + 1);
  if (crc32c::Value(body.data(), body.size()) != checksum)
    return Status::Corruption("checkpoint checksum");
  std::istringstream in(body);
  uint64_t count;
  if (!(in >> identity >> count) || identity != identity_ ||
      count > bytes.size())
    return Status::Corruption("checkpoint identity/count");
  for (uint64_t i = 0; i < count; ++i) {
    uint64_t n, length;
    if (!(in >> n >> length) || in.get() != '\n' || !length ||
        length > bytes.size())
      return Status::Corruption("checkpoint entry");
    std::string encoded(length, '\0');
    if (!in.read(encoded.data(), length))
      return Status::Corruption("short checkpoint");
    Version version;
    s = Decode(encoded, &version, false);
    if (!s.ok()) return s;
    if (version.identity != identity_ || version.number != n ||
        version.retired || !Complete(version))
      return Status::Corruption("checkpoint incomplete/identity");
    for (const auto& e : version.extents) {
      if (!validate_payload) break;
      uint64_t size;
      uint32_t crc;
      s = target()->GetFileSize(Path(e), IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size != e.length)
        return Status::Corruption("checkpoint extent length");
      s = Digest(target(), Path(e), size, &crc);
      if (!s.ok()) return s;
      if (crc != e.crc) return Status::Corruption("checkpoint extent checksum");
    }
    if (!parsed->emplace(n, std::move(version)).second)
      return Status::Corruption("duplicate checkpoint blob");
  }
  if (in >> extra) return Status::Corruption("checkpoint trailing data");
  return Status::OK();
}
Status TieredStorage::BindCheckpointInventory(
    const std::string& inventory, const std::string& map,
    const std::map<uint64_t, Version>& versions) {
  if (inventory.compare(0, 5, "MBI6\n"))
    return Status::Corruption("checkpoint inventory format");
  std::istringstream entries(inventory.substr(5));
  std::set<std::string> names;
  std::set<uint64_t> blobs;
  std::string kind, file;
  uint64_t length;
  bool found_map = false;
  while (entries >> kind) {
    if (!(entries >> file >> length))
      return Status::Corruption("incomplete inventory entry");
    if (file.empty() || file.find('/') != std::string::npos ||
        file.find("..") != std::string::npos ||
        file.find('\\') != std::string::npos ||
        !names.insert(kind + ":" + file).second)
      return Status::Corruption("checkpoint inventory name/duplicate");
    if (kind == "D") {
      if (found_map || file != "BLOB-MAP" || length != map.size())
        return Status::Corruption("checkpoint inventory map binding");
      found_map = true;
    } else if (kind == "B") {
      uint64_t n;
      std::istringstream number(file);
      if (!(number >> n) || !number.eof() || file != std::to_string(n) ||
          !blobs.insert(n).second)
        return Status::Corruption("checkpoint inventory blob identity");
      auto it = versions.find(n);
      if (it == versions.end() || it->second.size != length)
        return Status::Corruption("checkpoint inventory blob set/length");
    } else if (kind != "I") {
      return Status::Corruption("checkpoint inventory entry kind");
    }
  }
  if (!entries.eof() || !found_map || blobs.size() != versions.size())
    return Status::Corruption("checkpoint inventory map/blob coverage");
  return Status::OK();
}
Status TieredStorage::PreflightCheckpoint(const std::string& path) {
  std::map<uint64_t, Version> parsed;
  return ParseCheckpoint(path, &parsed);
}
Status TieredStorage::OpenRecoveryView(const std::string& checkpoint) {
  std::vector<std::string> names;
  Status s =
      target()->GetChildren(options_.data_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  std::map<uint64_t, Version> versions;
  for (const auto& name : names) {
    uint64_t number;
    if (!IsBlob(options_.data_dir + "/" + name, &number)) continue;
    std::string bytes;
    s = Read(target(), options_.data_dir + "/" + name, &bytes);
    if (!s.ok()) continue;
    Version version;
    s = Decode(bytes, &version, false);
    // An unreadable incremental descriptor is a missing dependency, not a
    // reason to reject the trusted checkpoint or other readable blobs.
    if (!s.ok()) continue;
    if (version.retired || version.identity != identity_ ||
        version.number != number)
      continue;
    if (!version.closed && !version.extents.empty()) {
      const auto& last = version.extents.back();
      version.size = last.start + last.length;
    }
    versions.emplace(number, std::move(version));
  }
  if (!checkpoint.empty()) {
    std::map<uint64_t, Version> baseline;
    s = ParseCheckpoint(checkpoint, &baseline, false);
    if (!s.ok()) return s;
    for (auto& entry : baseline)
      versions[entry.first] = std::move(entry.second);
  }
  for (auto& entry : versions) {
    auto blob = std::make_shared<Blob>();
    blob->live = blob->durable = blob->remote = std::move(entry.second);
    blobs_[entry.first] = std::move(blob);
  }
  return Status::OK();
}
Status TieredStorage::LoadCheckpoint(const std::string& path, bool trusted) {
  std::map<uint64_t, Version> parsed;
  Status s = ParseCheckpoint(path, &parsed, !trusted);
  if (!s.ok()) return s;
  // Trusted restore already owns a fixed checkpoint; damaged incremental
  // metadata must not prevent finalizing its independently recovered prefix.
  if (!trusted) s = Preflight(true);
  if (!s.ok()) return s;
  std::vector<std::string> obsolete;
  for (bool local : {false, true}) {
    const auto& dir = local ? options_.staging_dir : options_.data_dir;
    std::vector<std::string> names;
    s = target()->GetChildren(dir, IOOptions(), &names, nullptr);
    if (!s.ok()) return s;
    for (const auto& name : names) {
      uint64_t n;
      const bool temporary =
          name.size() > 4 && name.substr(name.size() - 4) == ".tmp";
      const auto base = temporary ? name.substr(0, name.size() - 4) : name;
      const bool retired =
          base.size() > 8 && base.substr(base.size() - 8) == ".retired";
      if (local ? LocalName(base, &n) : IsBlob(dir + "/" + base, &n)) {
        if (local || temporary || !parsed.count(n))
          obsolete.push_back(dir + "/" + name);
      } else if (retired)
        obsolete.push_back(dir + "/" + name);
    }
  }
  for (const auto& item : parsed) {
    s = StoreSnapshot(Descriptor(item.first), item.second, false,
                      Reason::kRecovery);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDescriptorPublished", &s);
    if (!s.ok()) return s;
  }
  for (const auto& file : obsolete) {
    s = target()->DeleteFile(file, IOOptions(), nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDescriptorRemoved", &s);
    if (!s.ok()) return s;
    if (file.compare(0, options_.staging_dir.size() + 1,
                     options_.staging_dir + "/") == 0)
      TrackMetadataBytes(file, 0);
  }
  s = SyncDir(target(), options_.data_dir);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierRestoreDirectorySynced", &s);
  if (!s.ok()) return s;
  blobs_.clear();
  for (const auto& item : parsed) {
    auto blob = std::make_shared<Blob>();
    blob->live = blob->durable = blob->remote = item.second;
    blob->complete_ready = true;
    blobs_[item.first] = std::move(blob);
  }
  return Status::OK();
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
  pressure_->AddStats(stats);
  stats->blob_append_calls = append_calls_;
  stats->blob_append_micros = append_us_;
  stats->blob_flush_calls = flush_calls_;
  stats->blob_flush_micros = flush_us_;
  stats->blob_append_latency = append_latency_;
  stats->blob_flush_latency = flush_latency_;
  stats->adaptive_pressure_response_micros = pressure_response_us_;
  stats->adaptive_max_pressure_response_micros = max_pressure_response_us_;
  stats->adaptive_throttle_micros = throttle_us_;
  stats->tier_barrier_d_debt_bytes = d_debt_;
  stats->tier_barrier_h_debt_bytes = h_debt_;
  stats->direct_blob_bytes = direct_bytes_;
  stats->staged_blob_bytes = staged_bytes_;
  stats->adaptive_to_ssd = to_ssd_;
  stats->adaptive_to_hdd = to_hdd_;
  stats->adaptive_quota_fallbacks = quota_fallbacks_;
  stats->adaptive_pending_seals = pending_count_;
  stats->adaptive_peak_pending_seals = peak_pending_count_;
  stats->adaptive_pending_wait_micros = pending_wait_us_;
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
  stats->tier_local_commit_attempts = local_metadata_.attempts;
  stats->tier_local_commit_successes = local_metadata_.successes;
  stats->tier_local_commit_skips = local_metadata_.skips;
  stats->tier_local_commit_micros = local_metadata_.micros;
  stats->tier_local_commit_lock_wait_micros = local_metadata_.wait;
  stats->tier_local_commit_register = local_metadata_.reasons[0];
  stats->tier_local_commit_freeze = local_metadata_.reasons[1];
  stats->tier_local_commit_close = local_metadata_.reasons[2];
  stats->tier_local_commit_spill = local_metadata_.reasons[3];
  stats->tier_local_commit_migration = local_metadata_.reasons[4];
  stats->tier_local_commit_finalize = local_metadata_.reasons[5];
  stats->tier_local_commit_recovery = local_metadata_.reasons[6];
  stats->tier_local_commit_gc = local_metadata_.reasons[7];
  stats->tier_local_metadata_fsyncs = local_metadata_.fsyncs;
  stats->tier_local_metadata_fsync_micros = local_metadata_.fsync_us;
  stats->tier_local_metadata_dir_syncs = local_metadata_.dirsyncs;
  stats->tier_local_metadata_dir_sync_micros = local_metadata_.dirsync_us;
  stats->tier_local_register_calls = local_metadata_.registers;
  stats->tier_local_register_micros = local_metadata_.register_us;
  stats->tier_local_freeze_calls = local_metadata_.freezes;
  stats->tier_local_freeze_micros = local_metadata_.freeze_us;
  stats->tier_remote_commit_attempts = remote_metadata_.attempts;
  stats->tier_remote_commit_successes = remote_metadata_.successes;
  stats->tier_remote_commit_skips = remote_metadata_.skips;
  stats->tier_remote_commit_micros = remote_metadata_.micros;
  stats->tier_remote_commit_lock_wait_micros = remote_metadata_.wait;
  stats->tier_remote_commit_register = remote_metadata_.reasons[0];
  stats->tier_remote_commit_freeze = remote_metadata_.reasons[1];
  stats->tier_remote_commit_close = remote_metadata_.reasons[2];
  stats->tier_remote_commit_spill = remote_metadata_.reasons[3];
  stats->tier_remote_commit_migration = remote_metadata_.reasons[4];
  stats->tier_remote_commit_finalize = remote_metadata_.reasons[5];
  stats->tier_remote_commit_recovery = remote_metadata_.reasons[6];
  stats->tier_remote_commit_gc = remote_metadata_.reasons[7];
  stats->tier_remote_metadata_fsyncs = remote_metadata_.fsyncs;
  stats->tier_remote_metadata_fsync_micros = remote_metadata_.fsync_us;
  stats->tier_remote_metadata_dir_syncs = remote_metadata_.dirsyncs;
  stats->tier_remote_metadata_dir_sync_micros = remote_metadata_.dirsync_us;
  stats->tier_remote_register_calls = remote_metadata_.registers;
  stats->tier_remote_register_micros = remote_metadata_.register_us;
  stats->tier_remote_freeze_calls = remote_metadata_.freezes;
  stats->tier_remote_freeze_micros = remote_metadata_.freeze_us;
  stats->tier_migration_publish_calls = migration_publish_calls_;
  stats->tier_migration_publish_micros = migration_publish_us_;
  stats->tier_local_metadata_bytes = local_metadata_bytes_;
  stats->peak_tier_local_metadata_bytes = peak_local_metadata_bytes_;
  stats->tier_local_commit_register_micros = local_metadata_.reason_us[0];
  stats->tier_local_commit_register_lock_wait_micros =
      local_metadata_.reason_wait[0];
  stats->tier_local_commit_register_fsync_micros =
      local_metadata_.reason_fsync_us[0];
  stats->tier_local_commit_register_dir_sync_micros =
      local_metadata_.reason_dirsync_us[0];
  stats->tier_local_commit_freeze_micros = local_metadata_.reason_us[1];
  stats->tier_local_commit_freeze_lock_wait_micros =
      local_metadata_.reason_wait[1];
  stats->tier_local_commit_freeze_fsync_micros =
      local_metadata_.reason_fsync_us[1];
  stats->tier_local_commit_freeze_dir_sync_micros =
      local_metadata_.reason_dirsync_us[1];
  stats->tier_local_commit_close_micros = local_metadata_.reason_us[2];
  stats->tier_local_commit_close_lock_wait_micros =
      local_metadata_.reason_wait[2];
  stats->tier_local_commit_close_fsync_micros =
      local_metadata_.reason_fsync_us[2];
  stats->tier_local_commit_close_dir_sync_micros =
      local_metadata_.reason_dirsync_us[2];
  stats->tier_local_commit_spill_micros = local_metadata_.reason_us[3];
  stats->tier_local_commit_spill_lock_wait_micros =
      local_metadata_.reason_wait[3];
  stats->tier_local_commit_spill_fsync_micros =
      local_metadata_.reason_fsync_us[3];
  stats->tier_local_commit_spill_dir_sync_micros =
      local_metadata_.reason_dirsync_us[3];
  stats->tier_local_commit_migration_micros = local_metadata_.reason_us[4];
  stats->tier_local_commit_migration_lock_wait_micros =
      local_metadata_.reason_wait[4];
  stats->tier_local_commit_migration_fsync_micros =
      local_metadata_.reason_fsync_us[4];
  stats->tier_local_commit_migration_dir_sync_micros =
      local_metadata_.reason_dirsync_us[4];
  stats->tier_local_commit_finalize_micros = local_metadata_.reason_us[5];
  stats->tier_local_commit_finalize_lock_wait_micros =
      local_metadata_.reason_wait[5];
  stats->tier_local_commit_finalize_fsync_micros =
      local_metadata_.reason_fsync_us[5];
  stats->tier_local_commit_finalize_dir_sync_micros =
      local_metadata_.reason_dirsync_us[5];
  stats->tier_local_commit_recovery_micros = local_metadata_.reason_us[6];
  stats->tier_local_commit_recovery_lock_wait_micros =
      local_metadata_.reason_wait[6];
  stats->tier_local_commit_recovery_fsync_micros =
      local_metadata_.reason_fsync_us[6];
  stats->tier_local_commit_recovery_dir_sync_micros =
      local_metadata_.reason_dirsync_us[6];
  stats->tier_local_commit_gc_micros = local_metadata_.reason_us[7];
  stats->tier_local_commit_gc_lock_wait_micros = local_metadata_.reason_wait[7];
  stats->tier_local_commit_gc_fsync_micros = local_metadata_.reason_fsync_us[7];
  stats->tier_local_commit_gc_dir_sync_micros =
      local_metadata_.reason_dirsync_us[7];
  stats->tier_remote_commit_register_micros = remote_metadata_.reason_us[0];
  stats->tier_remote_commit_register_lock_wait_micros =
      remote_metadata_.reason_wait[0];
  stats->tier_remote_commit_register_fsync_micros =
      remote_metadata_.reason_fsync_us[0];
  stats->tier_remote_commit_register_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[0];
  stats->tier_remote_commit_freeze_micros = remote_metadata_.reason_us[1];
  stats->tier_remote_commit_freeze_lock_wait_micros =
      remote_metadata_.reason_wait[1];
  stats->tier_remote_commit_freeze_fsync_micros =
      remote_metadata_.reason_fsync_us[1];
  stats->tier_remote_commit_freeze_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[1];
  stats->tier_remote_commit_close_micros = remote_metadata_.reason_us[2];
  stats->tier_remote_commit_close_lock_wait_micros =
      remote_metadata_.reason_wait[2];
  stats->tier_remote_commit_close_fsync_micros =
      remote_metadata_.reason_fsync_us[2];
  stats->tier_remote_commit_close_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[2];
  stats->tier_remote_commit_spill_micros = remote_metadata_.reason_us[3];
  stats->tier_remote_commit_spill_lock_wait_micros =
      remote_metadata_.reason_wait[3];
  stats->tier_remote_commit_spill_fsync_micros =
      remote_metadata_.reason_fsync_us[3];
  stats->tier_remote_commit_spill_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[3];
  stats->tier_remote_commit_migration_micros = remote_metadata_.reason_us[4];
  stats->tier_remote_commit_migration_lock_wait_micros =
      remote_metadata_.reason_wait[4];
  stats->tier_remote_commit_migration_fsync_micros =
      remote_metadata_.reason_fsync_us[4];
  stats->tier_remote_commit_migration_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[4];
  stats->tier_remote_commit_finalize_micros = remote_metadata_.reason_us[5];
  stats->tier_remote_commit_finalize_lock_wait_micros =
      remote_metadata_.reason_wait[5];
  stats->tier_remote_commit_finalize_fsync_micros =
      remote_metadata_.reason_fsync_us[5];
  stats->tier_remote_commit_finalize_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[5];
  stats->tier_remote_commit_recovery_micros = remote_metadata_.reason_us[6];
  stats->tier_remote_commit_recovery_lock_wait_micros =
      remote_metadata_.reason_wait[6];
  stats->tier_remote_commit_recovery_fsync_micros =
      remote_metadata_.reason_fsync_us[6];
  stats->tier_remote_commit_recovery_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[6];
  stats->tier_remote_commit_gc_micros = remote_metadata_.reason_us[7];
  stats->tier_remote_commit_gc_lock_wait_micros =
      remote_metadata_.reason_wait[7];
  stats->tier_remote_commit_gc_fsync_micros =
      remote_metadata_.reason_fsync_us[7];
  stats->tier_remote_commit_gc_dir_sync_micros =
      remote_metadata_.reason_dirsync_us[7];
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
