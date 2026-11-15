//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/native_files.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <limits>
#include <set>
#include <utility>

#include "rocksdb/utilities/metabypass.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
thread_local HddPurpose hdd_purpose = HddPurpose::kCaller;
uint64_t HddNow() {
  uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
  TEST_SYNC_POINT_CALLBACK("MetaBypass::HddClock", &now);
  return now;
}
}  // namespace
HddPurposeScope::HddPurposeScope(HddPurpose purpose) : previous_(hdd_purpose) {
  hdd_purpose = purpose;
}
HddPurposeScope::~HddPurposeScope() { hdd_purpose = previous_; }
HddPurpose HddPurposeScope::Current() { return hdd_purpose; }
HddPressure::HddPressure(uint64_t budget_us) : budget_(budget_us) {
  monitor_ = std::thread([this] { Monitor(); });
}
HddPressure::~HddPressure() { Stop(); }
void HddPressure::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
    wake_.notify_all();
  }
  if (monitor_.joinable()) monitor_.join();
}
uint64_t HddPressure::Begin(Kind kind, uint64_t bytes, unsigned area) {
  const auto purpose = HddPurposeScope::Current();
  const bool sensitive = ((kind == Kind::kAppend || kind == Kind::kFlush) &&
                          purpose == HddPurpose::kCaller) ||
                         ((kind == Kind::kSync || kind == Kind::kDirSync) &&
                          purpose != HddPurpose::kBulk);
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = NowLocked();
  if (area >= 2 && active_hdd_++ == 0) last_progress_ = now;
  const auto id = ++next_;
  active_.emplace(id, Operation{now, bytes, kind, sensitive, area, purpose});
  wake_.notify_one();
  return id;
}
uint64_t HddPressure::NowLocked() {
  // Sample only after taking mutex_: another operation may have completed
  // while this thread was waiting. Clamp injected or platform clock regressions
  // so every elapsed-time subtraction shares one monotonic timeline.
  clock_ = std::max(clock_, HddNow());
  return clock_;
}
void HddPressure::Evaluate(uint64_t now, bool new_progress) {
  const bool stalled = active_hdd_ != 0 && now - last_progress_ >= 1000000;
  bool latency = false;
  for (const auto& item : active_)
    latency |= item.second.area >= 2 && item.second.sensitive &&
               now - item.second.start >= budget_;
  if (stalled || latency) {
    last_bad_ = now;
    healthy_progress_ = 0;
    if (!pressure_) {
      pressure_ = true;
      entered_at_ = now;
      ++counters_.entered;
      if (latency) ++counters_.latency_entries;
      if (stalled) ++counters_.stall_entries;
    }
  }
  if (new_progress && pressure_ && now - entered_at_ >= 1000000 &&
      now - last_bad_ >= 2000000 && healthy_progress_ >= 3) {
    pressure_ = false;
    ++counters_.recovered;
    counters_.residence_us += now - entered_at_;
  }
}
void HddPressure::End(uint64_t id, bool success) {
  TEST_SYNC_POINT("MetaBypass::HddEndBeforeLock");
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = NowLocked();
  auto it = active_.find(id);
  Evaluate(
      now);  // Detect a slow call even if it completed before a timer tick.
  const auto& op = it->second;
  const size_t index = static_cast<size_t>(op.kind);
  if (op.area >= 2) {
    ++counters_.calls[index];
    counters_.micros[index] += now - op.start;
    --active_hdd_;
  }
  if (success) {
    if (op.area >= 2) {
      counters_.bytes[index] += op.bytes;
      last_progress_ = now;
      ++healthy_progress_;
    }
    if (op.kind == Kind::kAppend) {
      counters_.written_bytes[op.area] += op.bytes;
      counters_.purpose_write_bytes[static_cast<size_t>(op.purpose)] +=
          op.bytes;
    }
  }
  if (!success && op.kind == Kind::kAppend) ++counters_.failed_writes;
  const bool hdd_progress = success && op.area >= 2;
  active_.erase(it);
  Evaluate(now, hdd_progress);
  // Also wake fixture quiescence waiters sharing the observer condition.
  wake_.notify_all();
}
void HddPressure::RecordSsdWrite(unsigned area, HddPurpose purpose,
                                 uint64_t bytes, bool success) {
  assert(area < 2);
  std::lock_guard<std::mutex> lock(ssd_mutex_);
  if (success) {
    ssd_counters_.written_bytes[area] += bytes;
    ssd_counters_.purpose_write_bytes[static_cast<size_t>(purpose)] += bytes;
  } else {
    ++ssd_counters_.failed_writes;
  }
}
bool HddPressure::Pressured() {
  TEST_SYNC_POINT("MetaBypass::HddPressureBeforeLock");
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = NowLocked();
  Evaluate(now);
  return pressure_;
}
uint64_t HddPressure::PressureAge() {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = NowLocked();
  return pressure_ ? now - entered_at_ : 0;
}
HddPressure::Counters HddPressure::Snapshot() {
  // Keep each completed write paired with its purpose in every snapshot.
  std::scoped_lock lock(mutex_, ssd_mutex_);
  const auto now = NowLocked();
  Counters result = counters_;
  for (size_t i = 0; i < result.written_bytes.size(); ++i)
    result.written_bytes[i] += ssd_counters_.written_bytes[i];
  for (size_t i = 0; i < result.purpose_write_bytes.size(); ++i)
    result.purpose_write_bytes[i] += ssd_counters_.purpose_write_bytes[i];
  result.failed_writes += ssd_counters_.failed_writes;
  if (pressure_) result.residence_us += now - entered_at_;
  return result;
}
void HddPressure::AddStats(MetaBypassStats* stats) {
  const auto counters = Snapshot();
  stats->hdd_append_calls = counters.calls[0];
  stats->hdd_append_micros = counters.micros[0];
  stats->hdd_flush_calls = counters.calls[1];
  stats->hdd_flush_micros = counters.micros[1];
  stats->hdd_sync_calls = counters.calls[2];
  stats->hdd_sync_micros = counters.micros[2];
  stats->hdd_dirsync_calls = counters.calls[3];
  stats->hdd_dirsync_micros = counters.micros[3];
  stats->hdd_read_calls = counters.calls[4];
  stats->hdd_read_micros = counters.micros[4];
  stats->io_index_write_bytes = counters.written_bytes[0];
  stats->io_staging_write_bytes = counters.written_bytes[1];
  stats->io_data_write_bytes = counters.written_bytes[2];
  stats->io_backup_write_bytes = counters.written_bytes[3];
  stats->io_caller_write_bytes = counters.purpose_write_bytes[0];
  stats->io_bulk_write_bytes = counters.purpose_write_bytes[1];
  stats->io_barrier_write_bytes = counters.purpose_write_bytes[2];
  stats->io_failed_writes = counters.failed_writes;
  stats->hdd_pressure_entries = counters.entered;
  stats->hdd_latency_pressure_entries = counters.latency_entries;
  stats->hdd_stall_pressure_entries = counters.stall_entries;
  stats->hdd_pressure_recoveries = counters.recovered;
  stats->hdd_pressure_residence_micros = counters.residence_us;
}
void HddPressure::Monitor() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stop_) {
    if (active_hdd_ == 0)
      wake_.wait(lock, [&] { return stop_ || active_hdd_ != 0; });
    else
      wake_.wait_for(lock, std::chrono::milliseconds(5));
    if (!stop_) Evaluate(NowLocked());
  }
}
namespace {
template <class F>
IOStatus ObserveIO(HddPressure* pressure, unsigned area, HddPressure::Kind kind,
                   uint64_t bytes, F&& operation) {
  if (area < 2) {
    if (kind != HddPressure::Kind::kAppend) return operation();
    const auto purpose = HddPurposeScope::Current();
    IOStatus s = operation();
    pressure->RecordSsdWrite(area, purpose, bytes, s.ok());
    return s;
  }
  const auto id = pressure->Begin(kind, bytes, area);
  IOStatus s = operation();
  pressure->End(id, s.ok());
  return s;
}
class ObservedWrite : public FSWritableFileWrapper {
 public:
  ObservedWrite(std::unique_ptr<FSWritableFile> f,
                std::shared_ptr<HddPressure> p, unsigned area)
      : FSWritableFileWrapper(f.get()),
        file_(std::move(f)),
        pressure_(std::move(p)),
        area_(area) {}
  IOStatus Append(const Slice& b, const IOOptions& o,
                  IODebugContext* d) override {
    return Call(HddPressure::Kind::kAppend, b.size(),
                [&] { return target()->Append(b, o, d); });
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo& v, IODebugContext* d) override {
    return Call(HddPressure::Kind::kAppend, b.size(),
                [&] { return target()->Append(b, o, v, d); });
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t at, const IOOptions& o,
                            IODebugContext* d) override {
    return Call(HddPressure::Kind::kAppend, b.size(),
                [&] { return target()->PositionedAppend(b, at, o, d); });
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t at, const IOOptions& o,
                            const DataVerificationInfo& v,
                            IODebugContext* d) override {
    return Call(HddPressure::Kind::kAppend, b.size(),
                [&] { return target()->PositionedAppend(b, at, o, v, d); });
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    return Call(HddPressure::Kind::kFlush, 0,
                [&] { return target()->Flush(o, d); });
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    return Call(HddPressure::Kind::kSync, 0,
                [&] { return target()->Sync(o, d); });
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    return Call(HddPressure::Kind::kSync, 0,
                [&] { return target()->Fsync(o, d); });
  }

 private:
  template <class F>
  IOStatus Call(HddPressure::Kind k, uint64_t bytes, F f) {
    return ObserveIO(pressure_.get(), area_, k, bytes, std::move(f));
  }
  std::unique_ptr<FSWritableFile> file_;
  std::shared_ptr<HddPressure> pressure_;
  unsigned area_;
};
class ObservedRead : public FSRandomAccessFileWrapper {
 public:
  ObservedRead(std::unique_ptr<FSRandomAccessFile> f,
               std::shared_ptr<HddPressure> p, unsigned area)
      : FSRandomAccessFileWrapper(f.get()),
        file_(std::move(f)),
        pressure_(std::move(p)),
        area_(area) {}
  IOStatus Read(uint64_t at, size_t n, const IOOptions& o, Slice* b,
                char* scratch, IODebugContext* d) const override {
    const auto id = pressure_->Begin(HddPressure::Kind::kRead, n, area_);
    IOStatus s = target()->Read(at, n, o, b, scratch, d);
    pressure_->End(id, s.ok());
    return s;
  }

  IOStatus MultiRead(FSReadRequest* requests, size_t count, const IOOptions& o,
                     IODebugContext* d) override {
    uint64_t bytes = 0;
    for (size_t i = 0; i < count; ++i) bytes += requests[i].len;
    const auto id = pressure_->Begin(HddPressure::Kind::kRead, bytes, area_);
    IOStatus s = target()->MultiRead(requests, count, o, d);
    bool success = s.ok();
    for (size_t i = 0; i < count; ++i) success &= requests[i].status.ok();
    pressure_->End(id, success);
    return s;
  }
  IOStatus ReadAsync(FSReadRequest& request, const IOOptions& o,
                     std::function<void(FSReadRequest&, void*)> callback,
                     void* arg, void** handle, IOHandleDeleter* deleter,
                     IODebugContext* d) override {
    auto pressure = pressure_;
    const auto id =
        pressure->Begin(HddPressure::Kind::kRead, request.len, area_);
    auto finished = std::make_shared<std::atomic<bool>>(false);
    IOStatus s = target()->ReadAsync(
        request, o,
        [pressure, id, finished, callback](FSReadRequest& result,
                                           void* context) {
          if (!finished->exchange(true)) pressure->End(id, result.status.ok());
          callback(result, context);
        },
        arg, handle, deleter, d);
    if (!s.ok() && !finished->exchange(true)) pressure->End(id, false);
    return s;
  }
  bool SubmitReadAsync(FSReadRequest& request, const IOOptions& o,
                       std::function<void(FSReadRequest&)> callback,
                       IODebugContext* d) override {
    auto pressure = pressure_;
    const auto id =
        pressure->Begin(HddPressure::Kind::kRead, request.len, area_);
    return target()->SubmitReadAsync(
        request, o,
        [pressure, id, callback](FSReadRequest& result) {
          pressure->End(id, result.status.ok());
          callback(result);
        },
        d);
  }

 private:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::shared_ptr<HddPressure> pressure_;
  unsigned area_;
};
class ObservedRandomRW : public FSRandomRWFileWrapper {
 public:
  ObservedRandomRW(std::unique_ptr<FSRandomRWFile> f,
                   std::shared_ptr<HddPressure> p, unsigned area)
      : FSRandomRWFileWrapper(f.get()),
        file_(std::move(f)),
        pressure_(std::move(p)),
        area_(area) {}
  IOStatus Write(uint64_t at, const Slice& b, const IOOptions& o,
                 IODebugContext* d) override {
    return ObserveIO(pressure_.get(), area_, HddPressure::Kind::kAppend,
                     b.size(), [&] { return target()->Write(at, b, o, d); });
  }
  IOStatus Read(uint64_t at, size_t n, const IOOptions& o, Slice* b,
                char* scratch, IODebugContext* d) const override {
    return ObserveIO(pressure_.get(), area_, HddPressure::Kind::kRead, n,
                     [&] { return target()->Read(at, n, o, b, scratch, d); });
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(pressure_.get(), area_, HddPressure::Kind::kFlush, 0,
                     [&] { return target()->Flush(o, d); });
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(pressure_.get(), area_, HddPressure::Kind::kSync, 0,
                     [&] { return target()->Sync(o, d); });
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(pressure_.get(), area_, HddPressure::Kind::kSync, 0,
                     [&] { return target()->Fsync(o, d); });
  }

 private:
  std::unique_ptr<FSRandomRWFile> file_;
  std::shared_ptr<HddPressure> pressure_;
  unsigned area_;
};
class ObservedSequential : public FSSequentialFileWrapper {
 public:
  ObservedSequential(std::unique_ptr<FSSequentialFile> f,
                     std::shared_ptr<HddPressure> p, unsigned area)
      : FSSequentialFileWrapper(f.get()),
        file_(std::move(f)),
        pressure_(std::move(p)),
        area_(area) {}
  IOStatus Read(size_t n, const IOOptions& o, Slice* b, char* scratch,
                IODebugContext* d) override {
    const auto id = pressure_->Begin(HddPressure::Kind::kRead, n, area_);
    IOStatus s = target()->Read(n, o, b, scratch, d);
    pressure_->End(id, s.ok());
    return s;
  }

 private:
  std::unique_ptr<FSSequentialFile> file_;
  std::shared_ptr<HddPressure> pressure_;
  unsigned area_;
};
class ObservedDirectory : public FSDirectoryWrapper {
 public:
  ObservedDirectory(std::unique_ptr<FSDirectory> f,
                    std::shared_ptr<HddPressure> p, unsigned area)
      : FSDirectoryWrapper(std::move(f)),
        pressure_(std::move(p)),
        area_(area) {}
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    const auto id = pressure_->Begin(HddPressure::Kind::kDirSync, 0, area_);
    IOStatus s = FSDirectoryWrapper::Fsync(o, d);
    pressure_->End(id, s.ok());
    return s;
  }
  IOStatus FsyncWithDirOptions(const IOOptions& o, IODebugContext* d,
                               const DirFsyncOptions& opts) override {
    const auto id = pressure_->Begin(HddPressure::Kind::kDirSync, 0, area_);
    IOStatus s = FSDirectoryWrapper::FsyncWithDirOptions(o, d, opts);
    pressure_->End(id, s.ok());
    return s;
  }

 private:
  std::shared_ptr<HddPressure> pressure_;
  unsigned area_;
};
class ObservedFS : public FileSystemWrapper {
 public:
  ObservedFS(std::shared_ptr<FileSystem> fs, std::string data,
             std::string backup, std::shared_ptr<HddPressure> p,
             std::string index, std::string staging)
      : FileSystemWrapper(std::move(fs)),
        data_(std::move(data)),
        backup_(std::move(backup)),
        index_(std::move(index)),
        staging_(std::move(staging)),
        pressure_(std::move(p)) {}
  const char* Name() const override { return "MetaBypassObservedHdd"; }
  IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* out,
                           IODebugContext* d) override {
    IOStatus s = target()->NewWritableFile(path, o, out, d);
    if (s.ok() && Area(path) < 4)
      out->reset(new ObservedWrite(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus ReuseWritableFile(const std::string& path, const std::string& old,
                             const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* out,
                             IODebugContext* d) override {
    IOStatus s = target()->ReuseWritableFile(path, old, o, out, d);
    if (s.ok() && Area(path) < 4)
      out->reset(new ObservedWrite(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus ReopenWritableFile(const std::string& path, const FileOptions& o,
                              std::unique_ptr<FSWritableFile>* out,
                              IODebugContext* d) override {
    IOStatus s = target()->ReopenWritableFile(path, o, out, d);
    if (s.ok() && Area(path) < 4)
      out->reset(new ObservedWrite(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus NewRandomAccessFile(const std::string& path, const FileOptions& o,
                               std::unique_ptr<FSRandomAccessFile>* out,
                               IODebugContext* d) override {
    IOStatus s = target()->NewRandomAccessFile(path, o, out, d);
    if (s.ok() && Area(path) >= 2 && Area(path) < 4)
      out->reset(new ObservedRead(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus NewRandomRWFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSRandomRWFile>* out,
                           IODebugContext* d) override {
    IOStatus s = target()->NewRandomRWFile(path, o, out, d);
    if (s.ok() && Area(path) < 4)
      out->reset(new ObservedRandomRW(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus NewSequentialFile(const std::string& path, const FileOptions& o,
                             std::unique_ptr<FSSequentialFile>* out,
                             IODebugContext* d) override {
    IOStatus s = target()->NewSequentialFile(path, o, out, d);
    if (s.ok() && Area(path) >= 2 && Area(path) < 4)
      out->reset(
          new ObservedSequential(std::move(*out), pressure_, Area(path)));
    return s;
  }
  IOStatus SyncFile(const std::string& path, const FileOptions& f,
                    const IOOptions& o, bool full, IODebugContext* d) override {
    if (Area(path) < 2 || Area(path) == 4)
      return target()->SyncFile(path, f, o, full, d);
    const auto id = pressure_->Begin(HddPressure::Kind::kSync, 0, Area(path));
    IOStatus s = target()->SyncFile(path, f, o, full, d);
    pressure_->End(id, s.ok());
    return s;
  }
  IOStatus NewDirectory(const std::string& path, const IOOptions& o,
                        std::unique_ptr<FSDirectory>* out,
                        IODebugContext* d) override {
    IOStatus s = target()->NewDirectory(path, o, out, d);
    if (s.ok() && Area(path) >= 2 && Area(path) < 4)
      out->reset(new ObservedDirectory(std::move(*out), pressure_, Area(path)));
    return s;
  }

 private:
  unsigned Area(const std::string& path) const {
    const std::array<const std::string*, 4> roots{&index_, &staging_, &data_,
                                                  &backup_};
    for (unsigned i = 0; i < roots.size(); ++i) {
      const auto& root = *roots[i];
      if (!root.empty() &&
          (path == root || path.compare(0, root.size() + 1, root + "/") == 0))
        return i;
    }
    return 4;
  }
  std::string data_, backup_, index_, staging_;
  std::shared_ptr<HddPressure> pressure_;
};
}  // namespace
std::shared_ptr<FileSystem> ObserveHdd(std::shared_ptr<FileSystem> fs,
                                       const std::string& data,
                                       const std::string& backup,
                                       std::shared_ptr<HddPressure> p,
                                       const std::string& index,
                                       const std::string& staging) {
  return std::make_shared<ObservedFS>(std::move(fs), data, backup, std::move(p),
                                      index, staging);
}
Status Read(FileSystem* fs, const std::string& path, std::string* data) {
  Status s = fs->FileExists(path, IOOptions(), nullptr);
  if (!s.ok()) return s;
  std::unique_ptr<FSSequentialFile> f;
  s = fs->NewSequentialFile(path, FileOptions(), &f, nullptr);
  data->clear();
  std::array<char, 65536> buf;
  while (s.ok()) {
    Slice part;
    s = f->Read(buf.size(), IOOptions(), &part, buf.data(), nullptr);
    if (!s.ok() || part.empty()) break;
    data->append(part.data(), part.size());
  }
  return s;
}
Status Write(FileSystem* fs, const std::string& path, const Slice& data) {
  std::unique_ptr<FSWritableFile> f;
  Status s = fs->NewWritableFile(path, FileOptions(), &f, nullptr);
  if (!s.ok()) return s;
  s = f->Append(data, IOOptions(), nullptr);
  if (s.ok()) s = f->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(f->Close(IOOptions(), nullptr));
  return s;
}
Status EnsureDir(FileSystem* fs, const std::string& path) {
  Status s = fs->CreateDirIfMissing(path, IOOptions(), nullptr);
  if (s.ok())
    s = SyncDir(fs, std::filesystem::path(path).parent_path().string());
  return s;
}
Status SyncDir(FileSystem* fs, const std::string& path) {
  std::unique_ptr<FSDirectory> d;
  Status s = fs->NewDirectory(path, IOOptions(), &d, nullptr);
  if (s.ok()) s = d->Fsync(IOOptions(), nullptr);
  return s;
}
Status Copy(FileSystem* fs, const std::string& from, const std::string& to,
            uint64_t length) {
  std::unique_ptr<FSSequentialFile> in;
  std::unique_ptr<FSWritableFile> out;
  Status s = fs->NewSequentialFile(from, FileOptions(), &in, nullptr);
  if (s.ok()) s = fs->NewWritableFile(to, FileOptions(), &out, nullptr);
  if (!s.ok()) return s;
  std::array<char, 65536> buf;
  while (s.ok() && length) {
    Slice part;
    s = in->Read(std::min<uint64_t>(length, buf.size()), IOOptions(), &part,
                 buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) {
      s = Status::Corruption("short file", from);
      break;
    }
    s = out->Append(part, IOOptions(), nullptr);
    length -= part.size();
  }
  if (s.ok()) s = out->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(out->Close(IOOptions(), nullptr));
  return s;
}
Status StageCopy(FileSystem* fs, const std::string& from, const std::string& to,
                 uint64_t length) {
  std::unique_ptr<FSSequentialFile> in;
  std::unique_ptr<FSWritableFile> out;
  Status s = fs->NewSequentialFile(from, FileOptions(), &in, nullptr);
  if (s.ok()) s = fs->NewWritableFile(to, FileOptions(), &out, nullptr);
  if (!s.ok()) return s;
  std::array<char, 65536> buf;
  uint64_t copied = 0;
  while (s.ok() && copied < length) {
    Slice part;
    const size_t requested = std::min<uint64_t>(length - copied, buf.size());
    s = in->Read(requested, IOOptions(), &part, buf.data(), nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::StageCopyReadStatus", &s);
    if (!s.ok()) break;
    if (part.empty() || part.size() > requested) {
      s = Status::Corruption("invalid staged copy read length", from);
      break;
    }
    s = out->Append(part, IOOptions(), nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::StageCopyAppendStatus", &s);
    copied += part.size();
  }
  s.UpdateIfOk(out->Close(IOOptions(), nullptr));
  TEST_SYNC_POINT_CALLBACK("MetaBypass::StageCopyCloseStatus", &s);
  return s;
}
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc) {
  *crc = 0;
  return ExtendDigest(fs, path, 0, length, crc);
}
Status ExtendDigest(FileSystem* fs, const std::string& path, uint64_t begin,
                    uint64_t end, uint32_t* crc, uint64_t* opens) {
  if (begin > end) return Status::Corruption("digest prefix shrank", path);
  uint64_t size;
  Status s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
  if (!s.ok()) return s;
  if (size < end) return Status::Corruption("short file", path);
  if (begin == end) return Status::OK();
  std::unique_ptr<FSRandomAccessFile> in;
  if (opens) ++*opens;
  s = fs->NewRandomAccessFile(path, FileOptions(), &in, nullptr);
  std::array<char, 65536> buf;
  while (s.ok() && begin < end) {
    Slice part;
    s = in->Read(begin, std::min<uint64_t>(end - begin, buf.size()),
                 IOOptions(), &part, buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) return Status::Corruption("short file", path);
    *crc = crc32c::Extend(*crc, part.data(), part.size());
    begin += part.size();
  }
  return s;
}
Status RemoveDir(FileSystem* fs, const std::string& path) {
  std::vector<std::string> files;
  Status s = fs->GetChildren(path, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& f : files) {
    if (f == "." || f == "..") continue;
    s = fs->DeleteFile(path + "/" + f, IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
  return fs->DeleteDir(path, IOOptions(), nullptr);
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
