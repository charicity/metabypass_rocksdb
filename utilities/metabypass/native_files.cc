//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/native_files.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <utility>

#include "rocksdb/utilities/metabypass.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
thread_local HddPurpose hdd_purpose = HddPurpose::kCaller;
uint64_t IoNow() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
}  // namespace
HddPurposeScope::HddPurposeScope(HddPurpose purpose) : previous_(hdd_purpose) {
  hdd_purpose = purpose;
}
HddPurposeScope::~HddPurposeScope() { hdd_purpose = previous_; }
HddPurpose HddPurposeScope::Current() { return hdd_purpose; }
void IoStats::Record(Area area, Kind kind, uint64_t micros, uint64_t bytes,
                     HddPurpose purpose, bool success) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (area == Area::kBackup) {
    const size_t index = static_cast<size_t>(kind);
    ++calls_[index];
    micros_[index] += micros;
  }
  if (kind != Kind::kAppend) return;
  if (!success) {
    ++failed_writes_;
    return;
  }
  if (area == Area::kBackup) {
    backup_write_bytes_ += bytes;
  } else {
    index_write_bytes_ += bytes;
  }
  purpose_write_bytes_[static_cast<size_t>(purpose)] += bytes;
}
void IoStats::AddStats(MetaBypassStats* stats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  stats->hdd_append_calls = calls_[static_cast<size_t>(Kind::kAppend)];
  stats->hdd_flush_calls = calls_[static_cast<size_t>(Kind::kFlush)];
  stats->hdd_sync_calls = calls_[static_cast<size_t>(Kind::kSync)];
  stats->hdd_dirsync_calls = calls_[static_cast<size_t>(Kind::kDirSync)];
  stats->hdd_read_calls = calls_[static_cast<size_t>(Kind::kRead)];
  stats->hdd_append_micros = micros_[static_cast<size_t>(Kind::kAppend)];
  stats->hdd_flush_micros = micros_[static_cast<size_t>(Kind::kFlush)];
  stats->hdd_sync_micros = micros_[static_cast<size_t>(Kind::kSync)];
  stats->hdd_dirsync_micros = micros_[static_cast<size_t>(Kind::kDirSync)];
  stats->hdd_read_micros = micros_[static_cast<size_t>(Kind::kRead)];
  stats->io_index_write_bytes = index_write_bytes_;
  stats->io_backup_write_bytes = backup_write_bytes_;
  stats->io_caller_write_bytes =
      purpose_write_bytes_[static_cast<size_t>(HddPurpose::kCaller)];
  stats->io_bulk_write_bytes =
      purpose_write_bytes_[static_cast<size_t>(HddPurpose::kBulk)];
  stats->io_barrier_write_bytes =
      purpose_write_bytes_[static_cast<size_t>(HddPurpose::kBarrier)];
  stats->io_failed_writes = failed_writes_;
}
namespace {
using Area = IoStats::Area;
using Kind = IoStats::Kind;

template <class F>
IOStatus ObserveIO(IoStats* stats, Area area, Kind kind, uint64_t bytes,
                   F&& operation) {
  const bool timed = area == Area::kBackup;
  if (!timed && (area != Area::kIndex || kind != Kind::kAppend))
    return operation();
  const auto purpose = HddPurposeScope::Current();
  const uint64_t start = timed ? IoNow() : 0;
  IOStatus s = operation();
  stats->Record(area, kind, timed ? IoNow() - start : 0, bytes, purpose,
                s.ok());
  return s;
}
void RecordRead(IoStats* stats, uint64_t start, bool success) {
  stats->Record(Area::kBackup, Kind::kRead, IoNow() - start, 0,
                HddPurpose::kCaller, success);
}
class ObservedWrite : public FSWritableFileWrapper {
 public:
  ObservedWrite(std::unique_ptr<FSWritableFile> file,
                std::shared_ptr<IoStats> stats, Area area)
      : FSWritableFileWrapper(file.get()),
        file_(std::move(file)),
        stats_(std::move(stats)),
        area_(area) {}
  IOStatus Append(const Slice& b, const IOOptions& o,
                  IODebugContext* d) override {
    return Call(Kind::kAppend, b.size(),
                [&] { return target()->Append(b, o, d); });
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo& v, IODebugContext* d) override {
    return Call(Kind::kAppend, b.size(),
                [&] { return target()->Append(b, o, v, d); });
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t at, const IOOptions& o,
                            IODebugContext* d) override {
    return Call(Kind::kAppend, b.size(),
                [&] { return target()->PositionedAppend(b, at, o, d); });
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t at, const IOOptions& o,
                            const DataVerificationInfo& v,
                            IODebugContext* d) override {
    return Call(Kind::kAppend, b.size(),
                [&] { return target()->PositionedAppend(b, at, o, v, d); });
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    return Call(Kind::kFlush, 0, [&] { return target()->Flush(o, d); });
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    return Call(Kind::kSync, 0, [&] { return target()->Sync(o, d); });
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    return Call(Kind::kSync, 0, [&] { return target()->Fsync(o, d); });
  }

 private:
  template <class F>
  IOStatus Call(Kind kind, uint64_t bytes, F&& operation) {
    return ObserveIO(stats_.get(), area_, kind, bytes,
                     std::forward<F>(operation));
  }
  std::unique_ptr<FSWritableFile> file_;
  std::shared_ptr<IoStats> stats_;
  Area area_;
};
class ObservedRead : public FSRandomAccessFileWrapper {
 public:
  ObservedRead(std::unique_ptr<FSRandomAccessFile> file,
               std::shared_ptr<IoStats> stats)
      : FSRandomAccessFileWrapper(file.get()),
        file_(std::move(file)),
        stats_(std::move(stats)) {}
  IOStatus Read(uint64_t at, size_t n, const IOOptions& o, Slice* b,
                char* scratch, IODebugContext* d) const override {
    const uint64_t start = IoNow();
    IOStatus s = target()->Read(at, n, o, b, scratch, d);
    RecordRead(stats_.get(), start, s.ok());
    return s;
  }

  IOStatus MultiRead(FSReadRequest* requests, size_t count, const IOOptions& o,
                     IODebugContext* d) override {
    const uint64_t start = IoNow();
    IOStatus s = target()->MultiRead(requests, count, o, d);
    bool success = s.ok();
    for (size_t i = 0; i < count; ++i) success &= requests[i].status.ok();
    RecordRead(stats_.get(), start, success);
    return s;
  }
  IOStatus ReadAsync(FSReadRequest& request, const IOOptions& o,
                     std::function<void(FSReadRequest&, void*)> callback,
                     void* arg, void** handle, IOHandleDeleter* deleter,
                     IODebugContext* d) override {
    auto stats = stats_;
    const uint64_t start = IoNow();
    auto finished = std::make_shared<std::atomic<bool>>(false);
    IOStatus s = target()->ReadAsync(
        request, o,
        [stats, start, finished, callback](FSReadRequest& result,
                                           void* context) {
          if (!finished->exchange(true))
            RecordRead(stats.get(), start, result.status.ok());
          callback(result, context);
        },
        arg, handle, deleter, d);
    if (!s.ok() && !finished->exchange(true))
      RecordRead(stats.get(), start, false);
    return s;
  }
  bool SubmitReadAsync(FSReadRequest& request, const IOOptions& o,
                       std::function<void(FSReadRequest&)> callback,
                       IODebugContext* d) override {
    auto stats = stats_;
    const uint64_t start = IoNow();
    return target()->SubmitReadAsync(
        request, o,
        [stats, start, callback](FSReadRequest& result) {
          RecordRead(stats.get(), start, result.status.ok());
          callback(result);
        },
        d);
  }

 private:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::shared_ptr<IoStats> stats_;
};
class ObservedRandomRW : public FSRandomRWFileWrapper {
 public:
  ObservedRandomRW(std::unique_ptr<FSRandomRWFile> file,
                   std::shared_ptr<IoStats> stats, Area area)
      : FSRandomRWFileWrapper(file.get()),
        file_(std::move(file)),
        stats_(std::move(stats)),
        area_(area) {}
  IOStatus Write(uint64_t at, const Slice& b, const IOOptions& o,
                 IODebugContext* d) override {
    return ObserveIO(stats_.get(), area_, Kind::kAppend, b.size(),
                     [&] { return target()->Write(at, b, o, d); });
  }
  IOStatus Read(uint64_t at, size_t n, const IOOptions& o, Slice* b,
                char* scratch, IODebugContext* d) const override {
    return ObserveIO(stats_.get(), area_, Kind::kRead, 0,
                     [&] { return target()->Read(at, n, o, b, scratch, d); });
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(stats_.get(), area_, Kind::kFlush, 0,
                     [&] { return target()->Flush(o, d); });
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(stats_.get(), area_, Kind::kSync, 0,
                     [&] { return target()->Sync(o, d); });
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(stats_.get(), area_, Kind::kSync, 0,
                     [&] { return target()->Fsync(o, d); });
  }

 private:
  std::unique_ptr<FSRandomRWFile> file_;
  std::shared_ptr<IoStats> stats_;
  Area area_;
};
class ObservedSequential : public FSSequentialFileWrapper {
 public:
  ObservedSequential(std::unique_ptr<FSSequentialFile> file,
                     std::shared_ptr<IoStats> stats)
      : FSSequentialFileWrapper(file.get()),
        file_(std::move(file)),
        stats_(std::move(stats)) {}
  IOStatus Read(size_t n, const IOOptions& o, Slice* b, char* scratch,
                IODebugContext* d) override {
    const uint64_t start = IoNow();
    IOStatus s = target()->Read(n, o, b, scratch, d);
    RecordRead(stats_.get(), start, s.ok());
    return s;
  }

 private:
  std::unique_ptr<FSSequentialFile> file_;
  std::shared_ptr<IoStats> stats_;
};
class ObservedDirectory : public FSDirectoryWrapper {
 public:
  ObservedDirectory(std::unique_ptr<FSDirectory> file,
                    std::shared_ptr<IoStats> stats)
      : FSDirectoryWrapper(std::move(file)), stats_(std::move(stats)) {}
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    return ObserveIO(stats_.get(), Area::kBackup, Kind::kDirSync, 0,
                     [&] { return FSDirectoryWrapper::Fsync(o, d); });
  }
  IOStatus FsyncWithDirOptions(const IOOptions& o, IODebugContext* d,
                               const DirFsyncOptions& opts) override {
    return ObserveIO(stats_.get(), Area::kBackup, Kind::kDirSync, 0, [&] {
      return FSDirectoryWrapper::FsyncWithDirOptions(o, d, opts);
    });
  }

 private:
  std::shared_ptr<IoStats> stats_;
};
class ObservedFS : public FileSystemWrapper {
 public:
  ObservedFS(std::shared_ptr<FileSystem> fs, std::string index,
             std::string backup, std::shared_ptr<IoStats> stats)
      : FileSystemWrapper(std::move(fs)),
        index_(std::move(index)),
        backup_(std::move(backup)),
        stats_(std::move(stats)) {}
  const char* Name() const override { return "MetaBypassObservedIo"; }
  IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* out,
                           IODebugContext* d) override {
    IOStatus s = target()->NewWritableFile(path, o, out, d);
    const Area area = PathArea(path);
    if (s.ok() && area != Area::kOther)
      out->reset(new ObservedWrite(std::move(*out), stats_, area));
    return s;
  }
  IOStatus ReuseWritableFile(const std::string& path, const std::string& old,
                             const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* out,
                             IODebugContext* d) override {
    IOStatus s = target()->ReuseWritableFile(path, old, o, out, d);
    const Area area = PathArea(path);
    if (s.ok() && area != Area::kOther)
      out->reset(new ObservedWrite(std::move(*out), stats_, area));
    return s;
  }
  IOStatus ReopenWritableFile(const std::string& path, const FileOptions& o,
                              std::unique_ptr<FSWritableFile>* out,
                              IODebugContext* d) override {
    IOStatus s = target()->ReopenWritableFile(path, o, out, d);
    const Area area = PathArea(path);
    if (s.ok() && area != Area::kOther)
      out->reset(new ObservedWrite(std::move(*out), stats_, area));
    return s;
  }
  IOStatus NewRandomAccessFile(const std::string& path, const FileOptions& o,
                               std::unique_ptr<FSRandomAccessFile>* out,
                               IODebugContext* d) override {
    IOStatus s = target()->NewRandomAccessFile(path, o, out, d);
    if (s.ok() && PathArea(path) == Area::kBackup)
      out->reset(new ObservedRead(std::move(*out), stats_));
    return s;
  }
  IOStatus NewRandomRWFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSRandomRWFile>* out,
                           IODebugContext* d) override {
    IOStatus s = target()->NewRandomRWFile(path, o, out, d);
    const Area area = PathArea(path);
    if (s.ok() && area != Area::kOther)
      out->reset(new ObservedRandomRW(std::move(*out), stats_, area));
    return s;
  }
  IOStatus NewSequentialFile(const std::string& path, const FileOptions& o,
                             std::unique_ptr<FSSequentialFile>* out,
                             IODebugContext* d) override {
    IOStatus s = target()->NewSequentialFile(path, o, out, d);
    if (s.ok() && PathArea(path) == Area::kBackup)
      out->reset(new ObservedSequential(std::move(*out), stats_));
    return s;
  }
  IOStatus SyncFile(const std::string& path, const FileOptions& f,
                    const IOOptions& o, bool full, IODebugContext* d) override {
    return ObserveIO(stats_.get(), PathArea(path), Kind::kSync, 0,
                     [&] { return target()->SyncFile(path, f, o, full, d); });
  }
  IOStatus NewDirectory(const std::string& path, const IOOptions& o,
                        std::unique_ptr<FSDirectory>* out,
                        IODebugContext* d) override {
    IOStatus s = target()->NewDirectory(path, o, out, d);
    if (s.ok() && PathArea(path) == Area::kBackup)
      out->reset(new ObservedDirectory(std::move(*out), stats_));
    return s;
  }

 private:
  Area PathArea(const std::string& path) const {
    const auto under = [&](const std::string& root) {
      return !root.empty() && path.compare(0, root.size(), root) == 0 &&
             (path.size() == root.size() || path[root.size()] == '/');
    };
    if (under(index_)) return Area::kIndex;
    if (under(backup_)) return Area::kBackup;
    return Area::kOther;
  }
  const std::string index_, backup_;
  std::shared_ptr<IoStats> stats_;
};
}  // namespace
std::shared_ptr<FileSystem> ObserveIo(std::shared_ptr<FileSystem> fs,
                                      const std::string& index,
                                      const std::string& backup,
                                      std::shared_ptr<IoStats> stats) {
  return std::make_shared<ObservedFS>(std::move(fs), index, backup,
                                      std::move(stats));
}
Status Read(FileSystem* fs, const std::string& path, std::string* data) {
  std::unique_ptr<FSSequentialFile> f;
  Status s = fs->NewSequentialFile(path, FileOptions(), &f, nullptr);
  if (s.IsPathNotFound()) return Status::NotFound(path);
  if (!s.ok()) return s;
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
Status ParseLatestPoint(const std::string& latest, std::string* name) {
  Slice point(latest);
  if (!point.empty() && point[point.size() - 1] == '\n') point.remove_suffix(1);
  const Slice prefix("point-");
  if (!point.starts_with(prefix))
    return Status::Corruption("invalid published pointer");
  Slice digits = point;
  digits.remove_prefix(prefix.size());
  uint64_t generation;
  if (digits.empty() || digits[0] == '0' ||
      !ConsumeDecimalNumber(&digits, &generation) || !digits.empty()) {
    return Status::Corruption("invalid published pointer");
  }
  *name = point.ToString();
  return Status::OK();
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
  uint64_t size;
  Status s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
  if (!s.ok()) return s;
  if (size < length) return Status::Corruption("short file", path);
  if (length == 0) return Status::OK();
  std::unique_ptr<FSRandomAccessFile> in;
  s = fs->NewRandomAccessFile(path, FileOptions(), &in, nullptr);
  std::array<char, 65536> buf;
  uint64_t offset = 0;
  while (s.ok() && offset < length) {
    Slice part;
    s = in->Read(offset, std::min<uint64_t>(length - offset, buf.size()),
                 IOOptions(), &part, buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) return Status::Corruption("short file", path);
    *crc = crc32c::Extend(*crc, part.data(), part.size());
    offset += part.size();
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
