//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/tiered_storage.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <limits>
#include <set>
#include <sstream>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
IOStatus IO(const Status& s) {
  return s.ok() ? IOStatus::OK() : IOStatus::IOError(s.ToString());
}
uint64_t Micros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
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
// RocksDB serializes each writable handle. Different handles reserve quota
// independently under mutex_ before physical SSD writes begin.
class TierWriter : public FSWritableFileOwnerWrapper {
 public:
  TierWriter(std::unique_ptr<FSWritableFile> f, TieredStorage* s, uint64_t n)
      : FSWritableFileOwnerWrapper(std::move(f)), storage_(s), number_(n) {}
  IOStatus Append(const Slice& b, const IOOptions& o,
                  IODebugContext* d) override {
    if (closed_) return IOStatus::IOError("closed tier writer");
    bool admitted = false;
    if (!remote_) {
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        if (!storage_->error_.ok()) return IO(storage_->error_);
        if (storage_->stopping_) return IO(Status::ShutdownInProgress());
        const uint64_t available = storage_->options_.staging_capacity -
                                   storage_->usage_ - storage_->reserved_;
        admitted = b.size() <= available;
        if (admitted) {
          storage_->reserved_ += b.size();
          storage_->peak_ =
              std::max(storage_->peak_, storage_->usage_ + storage_->reserved_);
        }
      }
      if (admitted) {
        TEST_SYNC_POINT("MetaBypass::TierQuotaReserved");
        IOStatus s = target()->Append(b, o, d);
        {
          std::lock_guard<std::mutex> lock(storage_->mutex_);
          storage_->reserved_ -= b.size();
          // An append error can leave a partial physical write. Conservatively
          // retain the entire reservation until the failed DB is closed.
          storage_->usage_ += b.size();
          storage_->local_.at(number_).size += b.size();
        }
        if (!s.ok()) storage_->Fail(s);
        if (s.ok()) size_ += b.size();
        return s;
      }
      // Freeze a raw prefix. Only the worker copies it and publishes the
      // durable remote route; no blob records or footer are interpreted here.
      IOStatus s = target()->Close(o, d);
      if (!s.ok()) {
        storage_->Fail(s);
        return s;
      }
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        storage_->local_.at(number_).closed = true;
        storage_->local_.at(number_).switching = true;
        ++storage_->spill_files_;
        storage_->spill_bytes_ += size_;
      }
      Status switched =
          storage_->Submit(number_, TieredStorage::TaskKind::kSwitch);
      if (!switched.ok()) return IO(switched);
      remote_ = true;
    }
    size_t offset = 0;
    while (offset < b.size()) {
      const size_t take =
          std::min<uint64_t>(b.size() - offset, TieredStorage::kExtentSize);
      Status s = storage_->Submit(number_, TieredStorage::TaskKind::kAppend,
                                  Slice(b.data() + offset, take));
      if (!s.ok()) return IO(s);
      offset += take;
      size_ += take;
    }
    return IOStatus::OK();
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo&, IODebugContext* d) override {
    return Append(b, o, d);
  }
  IOStatus PositionedAppend(const Slice&, uint64_t, const IOOptions&,
                            IODebugContext*) override {
    return IOStatus::NotSupported("tiered blob positioned append");
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t n, const IOOptions& o,
                            const DataVerificationInfo&,
                            IODebugContext* d) override {
    return PositionedAppend(b, n, o, d);
  }
  IOStatus Truncate(uint64_t n, const IOOptions&, IODebugContext*) override {
    return n == size_ ? IOStatus::OK()
                      : IOStatus::NotSupported("tiered blob truncation");
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    return remote_ ? IOStatus::OK() : target()->Flush(o, d);
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    if (remote_)
      return IO(storage_->Submit(number_, TieredStorage::TaskKind::kBarrier));
    IOStatus s = target()->Sync(o, d);
    return s.ok()
               ? IO(SyncDir(storage_->target(), storage_->options_.staging_dir))
               : s;
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    if (remote_) return Sync(o, d);
    IOStatus s = target()->Fsync(o, d);
    return s.ok()
               ? IO(SyncDir(storage_->target(), storage_->options_.staging_dir))
               : s;
  }
  IOStatus Close(const IOOptions& o, IODebugContext* d) override {
    if (closed_) return IOStatus::OK();
    IOStatus s =
        remote_ ? IO(storage_->Submit(number_, TieredStorage::TaskKind::kClose))
                : target()->Close(o, d);
    if (s.ok()) {
      closed_ = true;
      if (!remote_) storage_->Closed(number_);
    } else
      storage_->Fail(s);
    return s;
  }
  bool IsSyncThreadSafe() const override { return false; }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return remote_ ? IOStatus::OK() : target()->InvalidateCache(offset, length);
  }
  uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
    return size_;
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
  TieredStorage* storage_;
  uint64_t number_, size_ = 0;
  bool remote_ = false, closed_ = false;
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
std::string TieredStorage::LocalPath(uint64_t n) const {
  return BlobFileName(options_.staging_dir, n);
}
std::string TieredStorage::Descriptor(uint64_t n) const {
  return BlobFileName(options_.data_dir, n);
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
std::string TieredStorage::Encode(const Version& v) {
  std::ostringstream body;
  body << v.size << ' ' << v.sealed << '\n';
  for (const auto& e : v.extents) body << e.length << ' ' << e.name << '\n';
  const auto b = body.str();
  return "MBT2 " + std::to_string(crc32c::Value(b.data(), b.size())) + "\n" + b;
}
Status TieredStorage::Decode(const std::string& bytes, Version* v) {
  const auto newline = bytes.find('\n');
  if (newline == std::string::npos)
    return Status::Corruption("tiered descriptor header");
  std::istringstream header(bytes.substr(0, newline));
  std::string magic, extra;
  uint32_t crc;
  if (!(header >> magic >> crc) || magic != "MBT2" || (header >> extra))
    return Status::Corruption("tiered descriptor version");
  const auto body = bytes.substr(newline + 1);
  if (crc32c::Value(body.data(), body.size()) != crc)
    return Status::Corruption("tiered descriptor checksum");
  std::istringstream in(body);
  unsigned sealed;
  if (!(in >> v->size >> sealed) || sealed > 1)
    return Status::Corruption("tiered descriptor size");
  v->sealed = sealed != 0;
  v->extents.clear();
  uint64_t total = 0;
  while (in >> std::ws && !in.eof()) {
    Extent e;
    if (!(in >> e.length >> e.name) || e.length == 0 ||
        e.length > v->size - total || e.name.compare(0, 8, "segment-") != 0 ||
        e.name.find_first_not_of("segment-0123456789") != std::string::npos)
      return Status::Corruption("tiered extent");
    total += e.length;
    v->extents.push_back(std::move(e));
  }
  return total == v->size ? Status::OK()
                          : Status::Corruption("tiered extent length");
}
Status TieredStorage::Initialize(bool restore) {
  restoring_ = restore;
  Status s = EnsureDir(target(), options_.staging_dir);
  if (s.ok())
    s = target()->LockFile(options_.staging_dir + "/METABYPASS-LOCK",
                           IOOptions(), &staging_lock_, nullptr);
  if (!s.ok()) return s;
  std::vector<std::string> names;
  s = target()->GetChildren(options_.data_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  for (const auto& name : names) {
    uint64_t n;
    if (IsBlob(options_.data_dir + "/" + name, &n)) {
      if (restore) continue;
      std::string bytes;
      s = Read(target(), Descriptor(n), &bytes);
      if (!s.ok()) return s;
      Version v;
      s = Decode(bytes, &v);
      if (!s.ok()) return s;
      versions_[n] = Snapshot(std::move(v));
    } else if (name.compare(0, 8, "segment-") == 0) {
      std::istringstream parse(name.substr(8));
      uint64_t number, id;
      char dash;
      if (!(parse >> number >> dash >> id) || dash != '-' || !parse.eof() ||
          id == UINT64_MAX)
        return Status::Corruption("tiered segment name");
      extent_id_ = std::max(extent_id_, id);
    }
  }
  names.clear();
  s = target()->GetChildren(options_.staging_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  for (const auto& name : names) {
    uint64_t n;
    FileType t;
    if (!ParseFileName(name, &n, &t) || t != kBlobFile) continue;
    if (restore)
      return Status::InvalidArgument("tiered restore requires empty staging");
    uint64_t size;
    s = target()->GetFileSize(LocalPath(n), IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (versions_.count(n)) {
      s = target()->DeleteFile(LocalPath(n), IOOptions(), nullptr);
      if (!s.ok()) return s;
      continue;
    }
    if (size > options_.staging_capacity - usage_)
      return Status::InvalidArgument("existing staging exceeds capacity");
    usage_ += size;
    local_[n] = {size, true};
  }
  peak_ = usage_;
  durable_ = versions_;
  return SyncDir(target(), options_.staging_dir);
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
void TieredStorage::Closed(uint64_t n) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = local_.find(n);
  if (it != local_.end()) {
    it->second.closed = true;
    work_.notify_one();
  }
}
IOStatus TieredStorage::NewWritableFile(const std::string& p,
                                        const FileOptions& o,
                                        std::unique_ptr<FSWritableFile>* out,
                                        IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->NewWritableFile(p, o, out, d);
  std::lock_guard<std::mutex> lock(mutex_);
  if (local_.count(n) || versions_.count(n))
    return IOStatus::Corruption("tiered blob number reused");
  FileOptions local_options = o;
  local_options.use_direct_writes = false;
  IOStatus s = target()->NewWritableFile(LocalPath(n), local_options, out, d);
  if (s.ok()) {
    local_[n] = {};
    out->reset(new TierWriter(std::move(*out), this, n));
  }
  return s;
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
  std::unique_ptr<FSRandomAccessFile> f;
  IOStatus s = NewRandomAccessFile(p, o, &f, d);
  if (s.ok()) out->reset(new TierSequential(std::move(f)));
  return s;
}
IOStatus TieredStorage::GetFileSize(const std::string& p, const IOOptions& o,
                                    uint64_t* size, IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->GetFileSize(p, o, size, d);
  std::lock_guard<std::mutex> lock(mutex_);
  const auto local = local_.find(n);
  if (local != local_.end() && !local->second.evicting) {
    *size = local->second.size;
    return IOStatus::OK();
  }
  const auto remote = versions_.find(n);
  if (remote == versions_.end()) return IOStatus::PathNotFound(p);
  *size = remote->second->size;
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
  // Debris is projected as an obsolete logical blob, never as readable data.
  // Include descriptor temporaries even when every segment was already deleted.
  for (const auto& name : *out) {
    if (name.compare(0, 8, "segment-") == 0) {
      uint64_t n, id;
      char dash;
      std::istringstream in(name.substr(8));
      if (!(in >> n >> dash >> id) || dash != '-' || !in.eof())
        return IOStatus::Corruption("tiered segment name");
      names.insert(BlobFileName("", n).substr(1));
    } else if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
      const std::string base = name.substr(0, name.size() - 4);
      uint64_t n;
      FileType type;
      if (ParseFileName(base, &n, &type) && type == kBlobFile &&
          base == BlobFileName("", n).substr(1))
        names.insert(base);
    }
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& l : local_)
      names.insert(LocalPath(l.first).substr(options_.staging_dir.size() + 1));
  }
  out->assign(names.begin(), names.end());
  return s;
}
IOStatus TieredStorage::SyncFile(const std::string& p, const FileOptions& f,
                                 const IOOptions& o, bool full,
                                 IODebugContext* d) {
  uint64_t n;
  if (!IsBlob(p, &n)) return target()->SyncFile(p, f, o, full, d);
  LocalReadPin pin;
  std::unique_lock<std::mutex> lock(mutex_);
  auto local = local_.find(n);
  if (local == local_.end() || local->second.evicting) {
    if (!versions_.count(n)) return IOStatus::NotFound(p);
    lock.unlock();
    return IO(Submit(n, TaskKind::kBarrier));
  }
  IOStatus s = pin.Open(this, n, d);
  lock.unlock();
  if (s.ok()) s = target()->SyncFile(LocalPath(n), f, o, full, d);
  return s.ok() ? IO(SyncDir(target(), options_.staging_dir)) : s;
}
IOStatus TieredStorage::ReadVersion(const Version& v, uint64_t offset, size_t n,
                                    const IOOptions& o, Slice* out,
                                    char* scratch, IODebugContext* d) {
  *out = Slice();
  if (offset >= v.size) return IOStatus::OK();
  n = std::min<uint64_t>(n, v.size - offset);
  size_t done = 0;
  auto first = std::upper_bound(v.ends.begin(), v.ends.end(), offset);
  size_t index = first - v.ends.begin();
  uint64_t start = index == 0 ? 0 : v.ends[index - 1];
  for (; index < v.extents.size() && done < n; ++index) {
    const auto& e = v.extents[index];
    const size_t take = std::min<uint64_t>(n - done, start + e.length - offset);
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(options_.data_dir + "/" + e.name,
                                               FileOptions(), &file, d);
    if (!s.ok()) return s;
    Slice r;
    s = file->Read(offset - start, take, o, &r, scratch + done, d);
    if (!s.ok()) return s;
    if (r.size() != take) return IOStatus::Corruption("short tiered extent");
    if (r.data() != scratch + done) std::memcpy(scratch + done, r.data(), take);
    done += take;
    offset += take;
    start += e.length;
    if (done == n) break;
  }
  *out = Slice(scratch, done);
  return IOStatus::OK();
}
IOStatus TieredStorage::LocalReadPin::Open(TieredStorage* storage, uint64_t n,
                                           IODebugContext* d) {
  // Serialized with route publication and deletion by the caller's mutex_.
  IOStatus s = storage->target()->NewRandomAccessFile(storage->LocalPath(n),
                                                      FileOptions(), &file_, d);
  if (s.ok()) {
    storage_ = storage;
    number_ = n;
    ++storage_->local_.at(n).readers;
  }
  return s;
}
TieredStorage::LocalReadPin::~LocalReadPin() {
  // Some backends retain buffers or access the path until handle destruction.
  // Close the physical handle before releasing the I/O pin and waking reclaim.
  file_.reset();
  if (storage_) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    --storage_->local_.at(number_).readers;
    storage_->work_.notify_one();
  }
}
void TieredStorage::RemoteReadPin::Pin(TieredStorage* storage, uint64_t n) {
  storage_ = storage;
  number_ = n;
  ++storage_->remote_readers_[n];
}
TieredStorage::RemoteReadPin::~RemoteReadPin() {
  if (storage_) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    auto it = storage_->remote_readers_.find(number_);
    if (--it->second == 0) storage_->remote_readers_.erase(it);
    storage_->work_.notify_one();
  }
}
IOStatus TieredStorage::ReadBlob(uint64_t n, uint64_t offset, size_t size,
                                 const IOOptions& o, Slice* out, char* scratch,
                                 IODebugContext* d) {
  std::shared_ptr<const Version> v;
  // This scope outlives the lock scope, including every error return.
  LocalReadPin local;
  RemoteReadPin remote_pin;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (retiring_.count(n)) return IOStatus::NotFound("retiring blob");
    auto it = local_.find(n);
    if (it != local_.end() && !it->second.evicting) {
      IOStatus s = local.Open(this, n, d);
      if (!s.ok()) return s;
    } else {
      auto remote = versions_.find(n);
      if (remote == versions_.end()) return IOStatus::NotFound("tiered blob");
      v = remote->second;
      remote_pin.Pin(this, n);
    }
  }
  if (!local.file()) {
    TEST_SYNC_POINT("MetaBypass::TierRemoteReadPinned");
    return ReadVersion(*v, offset, size, o, out, scratch, d);
  }
  TEST_SYNC_POINT("MetaBypass::TierLocalReadPinned");
  IOStatus s = local.file()->Read(offset, size, o, out, scratch, d);
  // Ensure the read result survives closing files that return owned buffers.
  if (s.ok() && out->data() != scratch && !out->empty()) {
    std::memcpy(scratch, out->data(), out->size());
    *out = Slice(scratch, out->size());
  }
  return s;
}
std::shared_ptr<const TieredStorage::Version> TieredStorage::Snapshot(
    Version v) {
  v.ends.clear();
  v.ends.reserve(v.extents.size());
  uint64_t end = 0;
  for (const auto& e : v.extents) {
    end += e.length;
    v.ends.push_back(end);
  }
  return std::make_shared<const Version>(std::move(v));
}
Status TieredStorage::Commit(uint64_t n, Version v) {
  const std::string p = Descriptor(n);
  Status s = Write(target(), p + ".tmp", Encode(v));
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorSynced", &s);
  if (s.ok()) s = target()->RenameFile(p + ".tmp", p, IOOptions(), nullptr);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) {
    auto snapshot = Snapshot(std::move(v));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      versions_[n] = snapshot;
      durable_[n] = std::move(snapshot);
    }
    TEST_SYNC_POINT("MetaBypass::TierRouteDurable");
  }
  return s;
}
Status TieredStorage::Submit(uint64_t n, TaskKind kind, const Slice& bytes) {
  std::unique_lock<std::mutex> lock(mutex_);
  const uint64_t start = Micros();
  progress_.wait(lock, [&] {
    return !error_.ok() || stopping_ ||
           (tasks_.size() < 64 && bytes.size() <= kQueueLimit - queued_bytes_);
  });
  wait_us_ += Micros() - start;
  if (!error_.ok()) return error_;
  if (stopping_) return Status::ShutdownInProgress();
  if (retiring_.count(n)) return Status::Aborted("retiring tier blob");
  auto task = std::make_shared<Task>();
  task->number = n;
  task->kind = kind;
  task->bytes.assign(bytes.data() ? bytes.data() : "", bytes.size());
  queued_bytes_ += bytes.size();
  peak_queued_bytes_ = std::max(peak_queued_bytes_, queued_bytes_);
  tasks_.push_back(task);
  TEST_SYNC_POINT("MetaBypass::TierTaskQueued");
  work_.notify_one();
  progress_.wait(lock, [&] { return task->done || !error_.ok() || stopping_; });
  return !error_.ok() ? error_
         : task->done ? Status::OK()
                      : Status::ShutdownInProgress();
}
Status TieredStorage::AppendRemote(uint64_t n, const Slice& bytes) {
  Version v;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = versions_.find(n);
    if (it == versions_.end()) return Status::Corruption("missing HDD route");
    v = *it->second;
  }
  if (bytes.size() > UINT64_MAX - v.size)
    return Status::InvalidArgument("tier blob size overflow");
  auto& active = active_[n];
  size_t offset = 0;
  while (offset < bytes.size()) {
    if (!active.file) {
      if (extent_id_ == UINT64_MAX)
        return Status::Corruption("tier extent id exhausted");
      active.name =
          "segment-" + std::to_string(n) + "-" + std::to_string(++extent_id_);
      active.size = 0;
      active.crc = 0;
      Status s =
          target()->NewWritableFile(options_.data_dir + "/" + active.name,
                                    FileOptions(), &active.file, nullptr);
      if (!s.ok()) return s;
      v.extents.push_back({0, active.name});
    }
    const size_t take =
        std::min<uint64_t>(bytes.size() - offset, kExtentSize - active.size);
    Status s = active.file->Append(Slice(bytes.data() + offset, take),
                                   IOOptions(), nullptr);
    if (s.ok()) s = active.file->Flush(IOOptions(), nullptr);
    if (!s.ok()) return s;
    active.crc = crc32c::Extend(active.crc, bytes.data() + offset, take);
    active.size += take;
    v.extents.back().length += take;
    v.size += take;
    offset += take;
    if (active.size == kExtentSize) {
      s = SealExtent(&active);
      if (!s.ok()) return s;
    }
  }
  v.sealed = false;
  auto snapshot = Snapshot(std::move(v));
  {
    std::lock_guard<std::mutex> lock(mutex_);
    versions_[n] = std::move(snapshot);
    spill_bytes_ += bytes.size();
  }
  return Status::OK();
}
Status TieredStorage::SealExtent(ActiveExtent* active) {
  Status s = active->file->Fsync(IOOptions(), nullptr);
  uint32_t actual = 0;
  if (s.ok())
    s = Digest(target(), options_.data_dir + "/" + active->name, active->size,
               &actual);
  if (s.ok() && actual != active->crc)
    s = Status::Corruption("HDD extent checksum");
  if (s.ok()) s = active->file->Close(IOOptions(), nullptr);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierExtentSynced", &s);
  if (s.ok()) active->file.reset();
  return s;
}
Status TieredStorage::Barrier(uint64_t n, bool close) {
  auto active = active_.find(n);
  if (active != active_.end() && active->second.file) {
    Status s = SealExtent(&active->second);
    if (!s.ok()) return s;
    active_.erase(active);
  }
  Version v;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = versions_.find(n);
    if (it == versions_.end())
      return Status::Corruption("missing HDD barrier route");
    v = *it->second;
  }
  v.sealed = close || v.sealed;
  return Commit(n, std::move(v));
}
Status TieredStorage::Execute(const Task& task) {
  switch (task.kind) {
    case TaskKind::kAppend:
      return AppendRemote(task.number, task.bytes);
    case TaskKind::kBarrier:
      return Barrier(task.number, false);
    case TaskKind::kClose:
      return Barrier(task.number, true);
    case TaskKind::kSwitch: {
      uint64_t length = 0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = local_.find(task.number);
        if (it == local_.end() || it->second.evicting) return Status::OK();
        length = it->second.size;
      }
      return Migrate(task.number, length, true);
    }
  }
  return Status::Corruption("unknown tier task");
}
Status TieredStorage::AppendExtent(uint64_t n, const Slice& bytes, Version* v) {
  if (extent_id_ == UINT64_MAX)
    return Status::Corruption("tier extent id exhausted");
  const std::string name =
      "segment-" + std::to_string(n) + "-" + std::to_string(++extent_id_);
  Status s = Write(target(), options_.data_dir + "/" + name, bytes);
  uint32_t crc = 0;
  if (s.ok())
    s = Digest(target(), options_.data_dir + "/" + name, bytes.size(), &crc);
  if (s.ok() && crc != crc32c::Value(bytes.data(), bytes.size()))
    s = Status::Corruption("migrated extent checksum");
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierExtentSynced", &s);
  if (s.ok()) {
    v->extents.push_back({bytes.size(), name});
    v->size += bytes.size();
  }
  return s;
}
Status TieredStorage::Migrate(uint64_t n, uint64_t required, bool background) {
  TEST_SYNC_POINT("MetaBypass::TierMigration");
  Version v;
  std::shared_ptr<const Version> previous;
  bool closed = false, switching = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = versions_.find(n);
    if (it != versions_.end()) previous = it->second;
    auto l = local_.find(n);
    if (l != local_.end() && !l->second.evicting) {
      closed = l->second.closed && required == l->second.size;
      switching = l->second.switching;
    } else if (!previous || previous->size < required)
      return Status::Corruption("missing staging source");
    else
      required = 0;
  }
  if (!closed && required == 0 && previous) return Barrier(n, previous->sealed);
  if (closed && previous && previous->sealed && previous->size >= required) {
    Status s = SwitchReadsToRemote(n);
    return s.ok() ? ReclaimLocalAfterReads(n) : s;
  }
  if (previous) v = *previous;
  if (required < v.size) return Status::OK();
  std::unique_ptr<FSRandomAccessFile> source;
  Status s = target()->NewRandomAccessFile(LocalPath(n), FileOptions(), &source,
                                           nullptr);
  if (!s.ok()) return s;
  // Copy opaque bytes in bounded scheduling quanta. The writer was closed
  // before this task was admitted, so its physical prefix cannot change.
  std::vector<char> buffer(kExtentSize);
  while (v.size < required) {
    const size_t take = std::min<uint64_t>(required - v.size, buffer.size());
    Slice bytes;
    s = source->Read(v.size, take, IOOptions(), &bytes, buffer.data(), nullptr);
    if (!s.ok()) return s;
    if (bytes.size() != take) return Status::Corruption("short staging source");
    s = AppendExtent(n, bytes, &v);
    if (!s.ok()) return s;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      migrated_ += take;
    }
    bool yield = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!error_.ok()) return error_;
      yield = background && v.size < required;
    }
    if (yield) {
      // Only a complete frozen prefix authorizes a durable HDD route. Keep
      // intermediate progress in memory, leaving SSD authoritative on crash.
      v.sealed = false;
      auto snapshot = Snapshot(std::move(v));
      {
        std::lock_guard<std::mutex> lock(mutex_);
        versions_[n] = std::move(snapshot);
      }
      TEST_SYNC_POINT("MetaBypass::TierMigrationYield");
      return Status::OK();
    }
  }
  v.sealed = closed && !switching;
  // Commit publishes only after all extent and descriptor dependencies persist.
  s = Commit(n, std::move(v));
  source.reset();
  if (s.ok() && closed) s = SwitchReadsToRemote(n);
  if (s.ok() && closed) s = ReclaimLocalAfterReads(n);
  return s;
}
Status TieredStorage::SwitchReadsToRemote(uint64_t n) {
  Status s;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierBeforeEvict", &s);
  if (!s.ok()) return s;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = local_.find(n);
  if (it == local_.end()) return Status::OK();
  auto remote = durable_.find(n);
  if (!it->second.closed || remote == durable_.end() ||
      remote->second->size != it->second.size)
    return Status::Corruption("unsafe staging eviction");
  // Route new reads remotely while existing local handles drain. Otherwise
  // continuous readers could indefinitely prevent capacity reclamation.
  it->second.evicting = true;
  return Status::OK();
}
Status TieredStorage::ReclaimLocalAfterReads(uint64_t n) {
  uint64_t size;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = local_.find(n);
    if (it == local_.end()) return Status::OK();
    // The sole worker owns deletion. Published remote routing excludes new
    // local pins, and an existing pin keeps the accounting entry alive.
    if (!it->second.evicting || it->second.readers != 0) return Status::OK();
    size = it->second.size;
  }
  Status s = target()->DeleteFile(LocalPath(n), IOOptions(), nullptr);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  if (!s.ok()) return s;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    usage_ -= size;
    local_.erase(n);
    progress_.notify_all();
  }
  return Status::OK();
}
Status TieredStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  std::unique_lock<std::mutex> lock(mutex_);
  for (const auto& b : lengths) {
    auto local = local_.find(b.first);
    if (local != local_.end() && !local->second.evicting &&
        (!local->second.closed || local->second.size != b.second))
      return Status::InvalidArgument(
          "checkpoint requires closed complete blob");
    requests_[b.first] = std::max(requests_[b.first], b.second);
  }
  TEST_SYNC_POINT("MetaBypass::TierPersistRequested");
  work_.notify_one();
  progress_.wait(lock, [&] {
    if (!error_.ok() || stopping_) return true;
    for (const auto& b : lengths) {
      auto v = durable_.find(b.first);
      if (v == durable_.end() || v->second->size < b.second) return false;
    }
    return true;
  });
  return !error_.ok() ? error_
         : stopping_  ? Status::ShutdownInProgress()
                      : Status::OK();
}
Status TieredStorage::ReclaimBlob(uint64_t n, uint64_t* bytes) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!error_.ok()) return error_;
  if (stopping_) return Status::ShutdownInProgress();
  auto request = std::make_shared<ReclaimRequest>();
  if (!reclaim_.emplace(n, request).second)
    return Status::Corruption("duplicate blob reclamation");
  retiring_.insert(n);
  requests_.erase(n);
  work_.notify_one();
  TEST_SYNC_POINT("MetaBypass::TierBlobRetiring");
  progress_.wait(lock, [&] {
    if (request->done) return true;
    if (request->started) {
      // Cancellation cannot release the owner's capture/read gate while the
      // sole worker is still performing physical deletion or directory sync.
      if (!error_.ok() || stopping_) {
        TEST_SYNC_POINT("MetaBypass::TierReclaimAwaitingCompletion");
      }
      return false;
    }
    return !error_.ok() || stopping_;
  });
  if (!error_.ok()) return error_;
  if (!request->done) return Status::ShutdownInProgress();
  *bytes = request->bytes;
  return Status::OK();
}
Status TieredStorage::DeleteRetiredBlob(uint64_t n, uint64_t* bytes) {
  // The sole worker excludes migration; retiring_ excludes new physical I/O.
  active_.erase(n);
  // Snapshot every historical/orphan extent before unlinking the descriptor.
  std::vector<std::string> names, payloads;
  Status s =
      target()->GetChildren(options_.data_dir, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  const std::string prefix = "segment-" + std::to_string(n) + "-";
  for (const auto& name : names)
    if (name.compare(0, prefix.size(), prefix) == 0)
      payloads.push_back(options_.data_dir + "/" + name);
  payloads.push_back(LocalPath(n));
  payloads.push_back(Descriptor(n) + ".tmp");
  *bytes = 0;
  for (const auto& path : payloads) {
    uint64_t size = 0;
    s = target()->GetFileSize(path, IOOptions(), &size, nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) continue;
    if (!s.ok()) return s;
    *bytes += size;
  }
  uint64_t size = 0;
  s = target()->GetFileSize(Descriptor(n), IOOptions(), &size, nullptr);
  if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) return s;
  if (s.ok()) *bytes += size;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    versions_.erase(n);
    durable_.erase(n);
  }
  TEST_SYNC_POINT("MetaBypass::BeforeBlobDelete");
  s = target()->DeleteFile(Descriptor(n), IOOptions(), nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobDescriptorRemoved", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  TEST_SYNC_POINT("MetaBypass::BlobDescriptorSynced");
  if (!s.ok()) return s;
  for (const auto& path : payloads) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobPayloadDeleteStatus", &s);
    if (s.ok()) s = target()->DeleteFile(path, IOOptions(), nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
    if (!s.ok()) return s;
    TEST_SYNC_POINT("MetaBypass::BlobPayloadRemoved");
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BlobFinalDirSyncStatus", &s);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) s = SyncDir(target(), options_.staging_dir);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto local = local_.find(n);
    if (local != local_.end()) {
      usage_ -= local->second.size;
      local_.erase(local);
    }
  }
  return s;
}
void TieredStorage::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  bool task_turn = true, request_turn = true;
  uint64_t maintenance_cursor = 0;
  const auto task_pinned = [&](uint64_t n) {
    return std::any_of(
        tasks_.begin(), tasks_.end(),
        [&](const std::shared_ptr<Task>& task) { return task->number == n; });
  };
  const auto reclaim_ready = [&](uint64_t n) {
    auto local = local_.find(n);
    return !task_pinned(n) && !remote_readers_.count(n) &&
           (local == local_.end() || local->second.readers == 0);
  };
  while (error_.ok()) {
    work_.wait(lock, [&] {
      if (stopping_ || !error_.ok() || !requests_.empty() || !tasks_.empty())
        return true;
      for (const auto& item : reclaim_)
        if (reclaim_ready(item.first)) return true;
      for (const auto& l : local_)
        if (!retiring_.count(l.first) && l.second.closed &&
            (!l.second.evicting || l.second.readers == 0))
          return true;
      return false;
    });
    if (stopping_ || !error_.ok()) break;
    bool maintenance_ready = !requests_.empty();
    for (const auto& item : reclaim_)
      maintenance_ready |= reclaim_ready(item.first);
    for (const auto& local : local_)
      maintenance_ready |=
          !retiring_.count(local.first) && local.second.closed &&
          (!local.second.evicting || local.second.readers == 0);
    if (!tasks_.empty() && (task_turn || !maintenance_ready)) {
      task_turn = false;
      auto task = tasks_.front();
      tasks_.pop_front();
      lock.unlock();
      Status s = Execute(*task);
      if (!s.ok()) Fail(s);
      lock.lock();
      auto local = local_.find(task->number);
      if (s.ok() && task->kind == TaskKind::kSwitch && local != local_.end() &&
          !local->second.evicting) {
        tasks_.push_back(task);
      } else {
        queued_bytes_ -= task->bytes.size();
        task->done = true;
      }
      progress_.notify_all();
      continue;
    }
    task_turn = true;
    auto reclaim = reclaim_.begin();
    for (; reclaim != reclaim_.end(); ++reclaim)
      if (reclaim_ready(reclaim->first)) break;
    if (reclaim != reclaim_.end()) {
      const uint64_t number = reclaim->first;
      auto request = reclaim->second;
      request->started = true;
      lock.unlock();
      Status s = DeleteRetiredBlob(number, &request->bytes);
      if (!s.ok()) Fail(s);
      lock.lock();
      request->done = true;
      if (s.ok()) {
        reclaim_.erase(number);
        retiring_.erase(number);
      }
      progress_.notify_all();
      continue;
    }
    uint64_t n = 0, length = 0;
    bool evict = false;
    auto background = local_.end();
    auto candidate = local_.upper_bound(maintenance_cursor);
    for (size_t checked = 0; checked < local_.size(); ++checked) {
      if (candidate == local_.end()) candidate = local_.begin();
      if (!retiring_.count(candidate->first) && candidate->second.closed &&
          (!candidate->second.evicting || candidate->second.readers == 0)) {
        background = candidate;
        break;
      }
      ++candidate;
    }
    const bool requested =
        !requests_.empty() && (request_turn || background == local_.end());
    if (requested) {
      request_turn = false;
      auto it = requests_.upper_bound(maintenance_cursor);
      if (it == requests_.end()) it = requests_.begin();
      n = it->first;
      length = it->second;
      requests_.erase(it);
    } else {
      // Eligibility can be constrained by task/read pins. Never dereference
      // the end iterator if no maintenance candidate can currently run.
      if (background == local_.end()) continue;
      request_turn = true;
      n = background->first;
      length = background->second.size;
      evict = background->second.evicting;
    }
    maintenance_cursor = n;
    lock.unlock();
    Status s = evict ? ReclaimLocalAfterReads(n) : Migrate(n, length, true);
    if (!s.ok()) Fail(s);
    TEST_SYNC_POINT("MetaBypass::TierMigrationComplete");
    lock.lock();
    if (s.ok() && requested) {
      auto durable = durable_.find(n);
      if (durable == durable_.end() || durable->second->size < length)
        requests_[n] = std::max(requests_[n], length);
    }
    progress_.notify_all();
  }
  // Stop/cancel wakes every waiter; physical HDD handles remain worker-owned
  // through teardown. Closing them does not publish an unsynced logical tail.
  lock.unlock();
  for (auto& extent : active_) {
    if (!extent.second.file) continue;
    Status s = extent.second.file->Close(IOOptions(), nullptr);
    if (!s.ok()) Fail(s);
  }
  active_.clear();
  lock.lock();
  tasks_.clear();
  queued_bytes_ = 0;
  progress_.notify_all();
}
Status TieredStorage::SaveCheckpoint(
    const std::string& path, const std::map<uint64_t, uint64_t>& lengths) {
  std::ostringstream out;
  out << "MBTC2\n";
  std::map<uint64_t, std::shared_ptr<const Version>> snapshots;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& b : lengths) {
      auto it = durable_.find(b.first);
      if (it == durable_.end() || it->second->size != b.second ||
          !it->second->sealed)
        return Status::Corruption("checkpoint requires durable closed blob");
      snapshots.emplace(b.first, it->second);
    }
  }
  for (const auto& b : lengths) {
    const auto& source = *snapshots.at(b.first);
    Version v;
    // Bind only the exact byte length supplied by the native checkpoint.
    uint64_t remain = b.second;
    for (const auto& e : source.extents) {
      if (!remain) break;
      const auto take = std::min(remain, e.length);
      v.extents.push_back({take, e.name});
      remain -= take;
    }
    v.sealed = source.sealed && source.size == b.second;
    v.size = b.second;
    const auto bytes = Encode(v);
    out << b.first << ' ' << bytes.size() << '\n' << bytes;
  }
  return Write(target(), path, out.str());
}
Status TieredStorage::LoadCheckpoint(const std::string& path) {
  std::string bytes;
  Status s = Read(target(), path, &bytes);
  if (!s.ok()) return s;
  std::istringstream in(bytes);
  std::string magic;
  if (!std::getline(in, magic) || magic != "MBTC2")
    return Status::Corruption("tier checkpoint version");
  std::map<uint64_t, std::shared_ptr<const Version>> versions;
  while (in >> std::ws && !in.eof()) {
    uint64_t n, length;
    if (!(in >> n >> length) || in.get() != '\n' || length > bytes.size())
      return Status::Corruption("tier checkpoint entry");
    std::string encoded(length, '\0');
    if (!in.read(&encoded[0], length))
      return Status::Corruption("short tier checkpoint");
    Version v;
    s = Decode(encoded, &v);
    if (!s.ok()) return s;
    if (!versions.emplace(n, Snapshot(std::move(v))).second)
      return Status::Corruption("duplicate tier blob");
  }
  versions_ = std::move(versions);
  durable_ = versions_;
  return Status::OK();
}
Status TieredStorage::ArchiveUnreferenced(
    const std::map<uint64_t, uint64_t>& referenced) {
  // A crash can leave a file with no surviving WAL/manifest reference. Retain
  // its raw bytes remotely, but do not register it as a live native blob.
  // Otherwise orphan local files can consume the entire admission budget.
  while (true) {
    auto it = local_.begin();
    while (it != local_.end() && referenced.count(it->first)) ++it;
    if (it == local_.end()) return Status::OK();
    const uint64_t number = it->first, size = it->second.size;
    std::unique_ptr<FSRandomAccessFile> source;
    Status s = target()->NewRandomAccessFile(LocalPath(number), FileOptions(),
                                             &source, nullptr);
    if (!s.ok()) return s;
    Version v;
    std::vector<char> buffer(kExtentSize);
    while (v.size < size) {
      const size_t take = std::min<uint64_t>(size - v.size, buffer.size());
      Slice bytes;
      s = source->Read(v.size, take, IOOptions(), &bytes, buffer.data(),
                       nullptr);
      if (!s.ok()) return s;
      if (bytes.size() != take) return Status::Corruption("short orphan blob");
      s = AppendExtent(number, bytes, &v);
      if (!s.ok()) return s;
    }
    v.sealed = true;
    s = Commit(number, std::move(v));
    if (!s.ok()) return s;
    source.reset();
    it->second.closed = true;
    s = SwitchReadsToRemote(number);
    if (s.ok()) s = ReclaimLocalAfterReads(number);
    if (!s.ok()) return s;
  }
}

void TieredStorage::AddStats(MetaBypassStats* stats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  stats->staging_bytes = usage_;
  stats->staging_spill_files = spill_files_;
  stats->staging_spill_bytes = spill_bytes_;
  stats->tier_queued_bytes = queued_bytes_;
  stats->peak_tier_queued_bytes = peak_queued_bytes_;
  for (const auto& v : versions_)
    stats->hdd_extents += v.second->extents.size();
  stats->peak_staging_bytes = peak_;
  stats->migrated_blob_bytes = migrated_;
  stats->staging_backpressure_micros = wait_us_;
  for (const auto& l : local_) {
    auto v = versions_.find(l.first);
    stats->pending_blob_bytes +=
        l.second.size -
        std::min(l.second.size, v == versions_.end() ? 0 : v->second->size);
  }
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
