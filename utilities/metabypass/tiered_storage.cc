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

#include "db/blob/blob_log_format.h"
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
class TierWriter : public FSWritableFileOwnerWrapper {
 public:
  TierWriter(std::unique_ptr<FSWritableFile> f, TieredStorage* s, uint64_t n)
      : FSWritableFileOwnerWrapper(std::move(f)), storage_(s), number_(n) {}
  IOStatus Append(const Slice& b, const IOOptions& o,
                  IODebugContext* d) override {
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      if (!storage_->error_.ok()) return IO(storage_->error_);
      // Reservations are made outside DB locks. Footer space is included in
      // each local file's accounting, including deferred flush generations.
      if (b.size() > storage_->options_.staging_capacity - storage_->usage_)
        return IOStatus::NoSpace("blob staging capacity");
    }
    IOStatus s = target()->Append(b, o, d);
    if (s.ok()) {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      storage_->local_[number_].size += b.size();
      storage_->usage_ += b.size();
      storage_->reserved_ -= std::min<uint64_t>(storage_->reserved_, b.size());
      storage_->peak_ = std::max(storage_->peak_, storage_->usage_);
    }
    return s;
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
  IOStatus Truncate(uint64_t n, const IOOptions& o,
                    IODebugContext* d) override {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    if (n != storage_->local_[number_].size)
      return IOStatus::NotSupported("tiered blob truncation");
    return target()->Truncate(n, o, d);
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    IOStatus s = target()->Sync(o, d);
    return s.ok()
               ? IO(SyncDir(storage_->target(), storage_->options_.staging_dir))
               : s;
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    IOStatus s = target()->Fsync(o, d);
    return s.ok()
               ? IO(SyncDir(storage_->target(), storage_->options_.staging_dir))
               : s;
  }
  IOStatus Close(const IOOptions& o, IODebugContext* d) override {
    IOStatus s = target()->Close(o, d);
    if (s.ok()) storage_->Closed(number_);
    return s;
  }

 private:
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
  return "MBT1 " + std::to_string(crc32c::Value(b.data(), b.size())) + "\n" + b;
}
Status TieredStorage::Decode(const std::string& bytes, Version* v) {
  const auto newline = bytes.find('\n');
  if (newline == std::string::npos)
    return Status::Corruption("tiered descriptor header");
  std::istringstream header(bytes.substr(0, newline));
  std::string magic, extra;
  uint32_t crc;
  if (!(header >> magic >> crc) || magic != "MBT1" || (header >> extra))
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
      versions_[n] = std::move(v);
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
    if (size >
        options_.staging_capacity - std::min(options_.staging_capacity, usage_))
      return Status::InvalidArgument("existing staging exceeds capacity");
    usage_ += size;
    local_[n] = {size, false};
  }
  peak_ = usage_;
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
Status TieredStorage::Reserve(uint64_t bytes,
                              const std::function<Status()>& rotate) {
  if (bytes > options_.staging_capacity)
    return Status::InvalidArgument("batch exceeds blob staging capacity");
  std::unique_lock<std::mutex> lock(mutex_);
  if (!error_.ok()) return error_;
  const auto fits = [&] {
    const uint64_t footer = local_.size() * BlobLogFooter::kSize;
    const uint64_t free = options_.staging_capacity - usage_;
    return footer <= free && bytes <= free - footer;
  };
  if (!fits()) {
    const uint64_t start = Micros();
    lock.unlock();
    Status s = rotate();
    lock.lock();
    if (!s.ok()) return s;
    TEST_SYNC_POINT("MetaBypass::StagingWaiting");
    progress_.wait(lock, [&] { return !error_.ok() || stopping_ || fits(); });
    wait_us_ += Micros() - start;
  }
  if (!error_.ok()) return error_;
  if (stopping_) return Status::ShutdownInProgress();
  reserved_ = bytes;
  peak_ = std::max(peak_, usage_ + reserved_);
  return Status::OK();
}
void TieredStorage::ReleaseReservation() {
  std::lock_guard<std::mutex> lock(mutex_);
  reserved_ = 0;
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
  IOStatus s = target()->NewWritableFile(LocalPath(n), o, out, d);
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
  if (local != local_.end()) {
    *size = local->second.size;
    return IOStatus::OK();
  }
  const auto remote = versions_.find(n);
  if (remote == versions_.end()) return IOStatus::NotFound(p);
  if (!restoring_ && !remote->second.sealed)
    return IOStatus::Corruption("active staging blob missing; use Restore");
  *size = remote->second.size;
  return IOStatus::OK();
}
IOStatus TieredStorage::FileExists(const std::string& p, const IOOptions& o,
                                   IODebugContext* d) {
  if (!IsBlob(p)) return target()->FileExists(p, o, d);
  uint64_t size;
  return GetFileSize(p, o, &size, d);
}
IOStatus TieredStorage::GetChildren(const std::string& p, const IOOptions& o,
                                    std::vector<std::string>* out,
                                    IODebugContext* d) {
  IOStatus s = target()->GetChildren(p, o, out, d);
  if (!s.ok() || p != options_.data_dir) return s;
  std::set<std::string> names(out->begin(), out->end());
  // Orphan extents also protect allocated blob numbers after a crash.
  for (const auto& name : *out)
    if (name.compare(0, 8, "segment-") == 0) {
      uint64_t n;
      std::istringstream in(name.substr(8));
      if (in >> n) names.insert(BlobFileName("", n).substr(1));
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
  std::lock_guard<std::mutex> lock(mutex_);
  if (!local_.count(n))
    return versions_.count(n) ? IOStatus::OK() : IOStatus::NotFound(p);
  IOStatus s = target()->SyncFile(LocalPath(n), f, o, full, d);
  return s.ok() ? IO(SyncDir(target(), options_.staging_dir)) : s;
}
IOStatus TieredStorage::ReadVersion(const Version& v, uint64_t offset, size_t n,
                                    const IOOptions& o, Slice* out,
                                    char* scratch, IODebugContext* d) {
  *out = Slice();
  if (offset >= v.size) return IOStatus::OK();
  n = std::min<uint64_t>(n, v.size - offset);
  size_t done = 0;
  uint64_t start = 0;
  for (const auto& e : v.extents) {
    if (offset >= start + e.length) {
      start += e.length;
      continue;
    }
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
IOStatus TieredStorage::ReadBlob(uint64_t n, uint64_t offset, size_t size,
                                 const IOOptions& o, Slice* out, char* scratch,
                                 IODebugContext* d) {
  Version v;
  std::unique_ptr<FSRandomAccessFile> local;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (local_.count(n) && !local_.at(n).evicting) {
      // Opening is serialized with eviction; the handle pins this read only.
      IOStatus s =
          target()->NewRandomAccessFile(LocalPath(n), FileOptions(), &local, d);
      if (!s.ok()) return s;
      ++local_[n].readers;
    } else {
      auto it = versions_.find(n);
      if (it == versions_.end()) return IOStatus::NotFound("tiered blob");
      v = it->second;
    }
  }
  if (!local) return ReadVersion(v, offset, size, o, out, scratch, d);
  IOStatus s = local->Read(offset, size, o, out, scratch, d);
  // Ensure the read result survives closing files that return owned buffers.
  if (s.ok() && out->data() != scratch) {
    std::memcpy(scratch, out->data(), out->size());
    *out = Slice(scratch, out->size());
  }
  local.reset();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    --local_[n].readers;
    work_.notify_one();
  }
  return s;
}
Status TieredStorage::Commit(uint64_t n, const Version& v) {
  const std::string p = Descriptor(n);
  Status s = Write(target(), p + ".tmp", Encode(v));
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierDescriptorSynced", &s);
  if (s.ok()) s = target()->RenameFile(p + ".tmp", p, IOOptions(), nullptr);
  if (s.ok()) s = SyncDir(target(), options_.data_dir);
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    versions_[n] = v;
  }
  return s;
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
  bool closed = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = versions_.find(n);
    if (it != versions_.end()) v = it->second;
    auto l = local_.find(n);
    if (l == local_.end())
      return v.size >= required ? Status::OK()
                                : Status::Corruption("missing staging source");
    closed = l->second.closed && required == l->second.size;
  }
  if (closed && v.sealed && v.size >= required) return Evict(n);
  if (required < v.size) return Status::OK();
  std::string recovery_footer;
  if (closed) {
    uint64_t end, count;
    bool sealed;
    Status checked =
        ScanBlob(target(), LocalPath(n), required, &end, &count, &sealed);
    if (!checked.ok()) return checked;
    if (!sealed) {
      // Shutdown may close a complete active file without flushing its
      // memtable. The owner has released it; seal only the remote view.
      BlobLogFooter footer;
      footer.blob_count = count;
      footer.EncodeTo(&recovery_footer);
    }
  }
  std::unique_ptr<FSRandomAccessFile> source;
  Status s = target()->NewRandomAccessFile(LocalPath(n), FileOptions(), &source,
                                           nullptr);
  if (!s.ok()) return s;
  // Each extent is bounded. Candidate lengths have already passed native
  // record validation; closed-file migration is checked before eviction.
  std::array<char, 256 * 1024> buffer;
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
      yield = background && !requests_.empty() && v.size < required;
    }
    if (yield) {
      // A bounded extent is the scheduling quantum. Preserve copied progress
      // and let a newly requested recovery dependency run before this file.
      v.sealed = false;
      TEST_SYNC_POINT("MetaBypass::TierMigrationYield");
      return Commit(n, v);
    }
  }
  if (!recovery_footer.empty()) {
    s = AppendExtent(n, recovery_footer, &v);
    if (!s.ok()) return s;
  }
  v.sealed = closed;
  s = Commit(n, v);
  source.reset();
  if (s.ok() && closed) s = Evict(n);
  return s;
}
Status TieredStorage::Evict(uint64_t n) {
  Status s;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::TierBeforeEvict", &s);
  if (!s.ok()) return s;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = local_.find(n);
  if (it == local_.end()) return Status::OK();
  auto remote = versions_.find(n);
  if (!it->second.closed || remote == versions_.end() ||
      !remote->second.sealed ||
      (remote->second.size != it->second.size &&
       remote->second.size - std::min(remote->second.size, it->second.size) !=
           BlobLogFooter::kSize))
    return Status::Corruption("unsafe staging eviction");
  // Route new reads remotely while existing local handles drain. Otherwise
  // continuous readers could indefinitely prevent capacity reclamation.
  it->second.evicting = true;
  if (it->second.readers != 0) return Status::OK();
  s = target()->DeleteFile(LocalPath(n), IOOptions(), nullptr);
  if (!s.ok()) return s;
  usage_ -= it->second.size;
  local_.erase(it);
  progress_.notify_all();
  return SyncDir(target(), options_.staging_dir);
}
Status TieredStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  std::unique_lock<std::mutex> lock(mutex_);
  for (const auto& b : lengths)
    requests_[b.first] = std::max(requests_[b.first], b.second);
  TEST_SYNC_POINT("MetaBypass::TierPersistRequested");
  work_.notify_one();
  progress_.wait(lock, [&] {
    if (!error_.ok() || stopping_) return true;
    for (const auto& b : lengths) {
      auto v = versions_.find(b.first);
      if (v == versions_.end() || v->second.size < b.second) return false;
    }
    return true;
  });
  return !error_.ok() ? error_
         : stopping_  ? Status::ShutdownInProgress()
                      : Status::OK();
}
void TieredStorage::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (error_.ok()) {
    work_.wait(lock, [&] {
      if (stopping_ || !error_.ok() || !requests_.empty()) return true;
      for (const auto& l : local_)
        if (l.second.closed && l.second.readers == 0) return true;
      return false;
    });
    if (stopping_ || !error_.ok()) break;
    uint64_t n = 0, length = 0;
    const bool background = requests_.empty();
    if (!requests_.empty()) {
      auto it = requests_.begin();
      n = it->first;
      length = it->second;
      requests_.erase(it);
    } else
      for (const auto& l : local_)
        if (l.second.closed && l.second.readers == 0) {
          n = l.first;
          length = l.second.size;
          break;
        }
    lock.unlock();
    Status s = Migrate(n, length, background);
    if (!s.ok()) Fail(s);
    lock.lock();
    progress_.notify_all();
  }
}
Status TieredStorage::SaveCheckpoint(
    const std::string& path, const std::map<uint64_t, uint64_t>& lengths) {
  std::ostringstream out;
  out << "MBTC1\n";
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& b : lengths) {
      auto it = versions_.find(b.first);
      if (it == versions_.end() || it->second.size < b.second)
        return Status::Corruption("unmigrated checkpoint blob");
      Version v = it->second;
      // Bind exactly the validated prefix, never an unvalidated newer tail.
      uint64_t remain = b.second;
      std::vector<Extent> extents;
      for (const auto& e : v.extents) {
        if (!remain) break;
        const auto take = std::min(remain, e.length);
        extents.push_back({take, e.name});
        remain -= take;
      }
      v.extents = std::move(extents);
      v.sealed = v.sealed && v.size == b.second;
      v.size = b.second;
      const auto bytes = Encode(v);
      out << b.first << ' ' << bytes.size() << '\n' << bytes;
    }
  }
  return Write(target(), path, out.str());
}
Status TieredStorage::LoadCheckpoint(const std::string& path) {
  std::string bytes;
  Status s = Read(target(), path, &bytes);
  if (!s.ok()) return s;
  std::istringstream in(bytes);
  std::string magic;
  if (!std::getline(in, magic) || magic != "MBTC1")
    return Status::Corruption("tier checkpoint version");
  std::map<uint64_t, Version> versions;
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
    if (!versions.emplace(n, std::move(v)).second)
      return Status::Corruption("duplicate tier blob");
  }
  versions_ = std::move(versions);
  return Status::OK();
}
Status TieredStorage::SealRecovery(uint64_t n, uint64_t end,
                                   const Slice& footer) {
  // Recovery is exclusive. Copy the validated logical prefix into immutable
  // extents, then add a separate footer. Existing extent bytes never change.
  Version v;
  auto existing = versions_.find(n);
  if (existing != versions_.end()) {
    uint64_t remain = end;
    for (const auto& e : existing->second.extents) {
      if (!remain) break;
      const uint64_t take = std::min(remain, e.length);
      v.extents.push_back({take, e.name});
      v.size += take;
      remain -= take;
    }
    if (existing->second.sealed &&
        existing->second.size == end + footer.size()) {
      Status s = Commit(n, existing->second);
      if (!s.ok()) return s;
      std::lock_guard<std::mutex> lock(mutex_);
      auto local = local_.find(n);
      if (local == local_.end()) return Status::OK();
      s = target()->DeleteFile(LocalPath(n), IOOptions(), nullptr);
      if (s.ok()) {
        usage_ -= local->second.size;
        local_.erase(local);
      }
      if (s.ok()) s = SyncDir(target(), options_.staging_dir);
      return s;
    }
  }
  std::array<char, 256 * 1024> buffer;
  while (v.size < end) {
    Slice bytes;
    const size_t take = std::min<uint64_t>(end - v.size, buffer.size());
    Status s =
        ReadBlob(n, v.size, take, IOOptions(), &bytes, buffer.data(), nullptr);
    if (!s.ok()) return s;
    if (bytes.size() != take) return Status::Corruption("short recovery blob");
    s = AppendExtent(n, bytes, &v);
    if (!s.ok()) return s;
  }
  Status s = AppendExtent(n, footer, &v);
  v.sealed = true;
  if (s.ok()) s = Commit(n, v);
  if (s.ok()) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = local_.find(n);
      if (it != local_.end()) it->second.closed = true;
    }
    // Recovery may discard an invalid tail, so local size can differ.
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = local_.find(n);
    if (it != local_.end()) {
      s = target()->DeleteFile(LocalPath(n), IOOptions(), nullptr);
      if (s.ok()) {
        usage_ -= it->second.size;
        local_.erase(it);
      }
    }
  }
  return s;
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
    std::array<char, 256 * 1024> buffer;
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
    s = Commit(number, v);
    if (!s.ok()) return s;
    source.reset();
    it->second.closed = true;
    s = Evict(number);
    if (!s.ok()) return s;
  }
}

void TieredStorage::AddStats(MetaBypassStats* stats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  stats->staging_bytes = usage_;
  stats->peak_staging_bytes = peak_;
  stats->migrated_blob_bytes = migrated_;
  stats->staging_backpressure_micros = wait_us_;
  for (const auto& l : local_) {
    auto v = versions_.find(l.first);
    stats->pending_blob_bytes +=
        l.second.size -
        std::min(l.second.size, v == versions_.end() ? 0 : v->second.size);
  }
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
