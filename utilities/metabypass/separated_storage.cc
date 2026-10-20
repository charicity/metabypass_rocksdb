//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/separated_storage.h"

#include <algorithm>
#include <array>
#include <limits>

#include "file/filename.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
// Blob files are installed through the index directory's native barrier.
// Include the routed payload directory in that same durability contract.
class PayloadDirectory : public FSDirectoryWrapper {
 public:
  PayloadDirectory(std::unique_ptr<FSDirectory> index,
                   std::unique_ptr<FSDirectory> data)
      : FSDirectoryWrapper(std::move(index)), data_(std::move(data)) {}
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus s = data_->Fsync(options, dbg);
    if (s.ok()) s = FSDirectoryWrapper::Fsync(options, dbg);
    return s;
  }
  IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* dbg,
                               const DirFsyncOptions&) override {
    return Fsync(options, dbg);
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus s = data_->Close(options, dbg);
    s.UpdateIfOk(FSDirectoryWrapper::Close(options, dbg));
    return s;
  }

 private:
  std::unique_ptr<FSDirectory> data_;
};
}  // namespace
SeparatedStorage::SeparatedStorage(std::shared_ptr<FileSystem> fs,
                                   std::string index, std::string data,
                                   std::shared_ptr<TieredStorage> tier,
                                   std::shared_ptr<SstStorage> sst)
    : FileSystemWrapper(sst    ? std::shared_ptr<FileSystem>(sst)
                        : tier ? std::shared_ptr<FileSystem>(tier)
                               : fs),
      index_(std::move(index)),
      data_(std::move(data)),
      tier_(std::move(tier)),
      sst_(std::move(sst)) {}
SeparatedStorage::~SeparatedStorage() {
  if (lock_)
    target()->UnlockFile(lock_, IOOptions(), nullptr).PermitUncheckedError();
}
Status SeparatedStorage::Lock() {
  Status s = EnsureDir(target(), data_);
  if (s.ok())
    s = target()->LockFile(data_ + "/METABYPASS-LOCK", IOOptions(), &lock_,
                           nullptr);
  return s;
}
std::string SeparatedStorage::BlobPath(uint64_t n) const {
  return BlobFileName(data_, n);
}
bool SeparatedStorage::IsBlob(const std::string& path) const {
  if (path.compare(0, index_.size() + 1, index_ + "/") != 0) return false;
  uint64_t n;
  FileType type;
  return ParseFileName(path.substr(index_.size() + 1), &n, &type) &&
         type == kBlobFile;
}
std::string SeparatedStorage::Map(const std::string& path) const {
  return IsBlob(path) ? data_ + path.substr(index_.size()) : path;
}
#define MB_OPEN(method, type)                                               \
  IOStatus SeparatedStorage::method(                                        \
      const std::string& p, const FileOptions& o, std::unique_ptr<type>* r, \
      IODebugContext* d) {                                                  \
    return target()->method(Map(p), o, r, d);                               \
  }
MB_OPEN(NewSequentialFile, FSSequentialFile)
MB_OPEN(NewRandomAccessFile, FSRandomAccessFile)
MB_OPEN(NewWritableFile, FSWritableFile)
MB_OPEN(ReopenWritableFile, FSWritableFile)
#undef MB_OPEN
IOStatus SeparatedStorage::NewDirectory(const std::string& p,
                                        const IOOptions& o,
                                        std::unique_ptr<FSDirectory>* r,
                                        IODebugContext* d) {
  IOStatus s = target()->NewDirectory(p, o, r, d);
  if (!s.ok() || p != index_) return s;
  std::unique_ptr<FSDirectory> data;
  s = target()->NewDirectory(data_, o, &data, d);
  if (s.ok()) r->reset(new PayloadDirectory(std::move(*r), std::move(data)));
  return s;
}
IOStatus SeparatedStorage::FileExists(const std::string& p, const IOOptions& o,
                                      IODebugContext* d) {
  return target()->FileExists(Map(p), o, d);
}
IOStatus SeparatedStorage::GetFileSize(const std::string& p, const IOOptions& o,
                                       uint64_t* r, IODebugContext* d) {
  return target()->GetFileSize(Map(p), o, r, d);
}
IOStatus SeparatedStorage::DeleteFile(const std::string& p, const IOOptions& o,
                                      IODebugContext* d) {
  // Retention is intentional, including failed-write and obsolete files.
  return IsBlob(p) ? IOStatus::OK() : target()->DeleteFile(p, o, d);
}
IOStatus SeparatedStorage::SyncFile(const std::string& p, const FileOptions& f,
                                    const IOOptions& o, bool full,
                                    IODebugContext* d) {
  return target()->SyncFile(Map(p), f, o, full, d);
}
IOStatus SeparatedStorage::GetChildren(const std::string& p, const IOOptions& o,
                                       std::vector<std::string>* r,
                                       IODebugContext* d) {
  IOStatus s = target()->GetChildren(p, o, r, d);
  if (!s.ok() || p != index_) return s;
  std::vector<std::string> blobs;
  s = target()->GetChildren(data_, o, &blobs, d);
  if (!s.ok()) return s;
  for (const auto& b : blobs)
    if (IsBlob(index_ + "/" + b)) r->push_back(b);
  return s;
}
Status SeparatedStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  if (tier_) return tier_->Persist(lengths);
  for (const auto& blob : lengths) {
    uint64_t size;
    Status s = target()->GetFileSize(BlobPath(blob.first), IOOptions(), &size,
                                     nullptr);
    if (!s.ok()) return s;
    if (size != blob.second)
      return Status::Corruption("native blob size mismatch");
    s = target()->SyncFile(BlobPath(blob.first), FileOptions(), IOOptions(),
                           true, nullptr);
    if (!s.ok()) return s;
  }
  return SyncDir(target(), data_);
}
Status SeparatedStorage::ReclaimBlob(uint64_t number, uint64_t* bytes) {
  if (tier_) return tier_->ReclaimBlob(number, bytes);
  *bytes = 0;
  Status s =
      target()->GetFileSize(BlobPath(number), IOOptions(), bytes, nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) return SyncDir(target(), data_);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::BeforeBlobDelete");
  s = target()->DeleteFile(BlobPath(number), IOOptions(), nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
  if (s.ok()) s = SyncDir(target(), data_);
  return s;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
