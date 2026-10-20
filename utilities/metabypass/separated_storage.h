//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <map>

#include "rocksdb/file_system.h"
#include "utilities/metabypass/native_files.h"
#include "utilities/metabypass/sst_storage.h"
#include "utilities/metabypass/tiered_storage.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
// Blob payload ownership, routing and recovery are isolated from mirroring.
// The data-directory lock excludes recovery from live writers. Persistence
// uses named-file sync, never the DB or partition-manager mutex.
class SeparatedStorage : public FileSystemWrapper {
 public:
  SeparatedStorage(std::shared_ptr<FileSystem> fs, std::string index,
                   std::string data,
                   std::shared_ptr<TieredStorage> tier = nullptr,
                   std::shared_ptr<SstStorage> sst = nullptr);
  ~SeparatedStorage() override;
  const char* Name() const override { return "MetaBypassSeparatedStorage"; }
  Status Lock();
  TieredStorage* tier() const { return tier_.get(); }
  SstStorage* sst() const { return sst_.get(); }
  Status Persist(const std::map<uint64_t, uint64_t>& lengths);
  // Called only after the backup owner has excluded all recovery roots.
  Status ReclaimBlob(uint64_t number, uint64_t* bytes);
  std::string BlobPath(uint64_t number) const;
  std::string Map(const std::string& path) const;
  bool IsBlob(const std::string& path) const;
  IOStatus NewSequentialFile(const std::string&, const FileOptions&,
                             std::unique_ptr<FSSequentialFile>*,
                             IODebugContext*) override;
  IOStatus NewRandomAccessFile(const std::string&, const FileOptions&,
                               std::unique_ptr<FSRandomAccessFile>*,
                               IODebugContext*) override;
  IOStatus NewWritableFile(const std::string&, const FileOptions&,
                           std::unique_ptr<FSWritableFile>*,
                           IODebugContext*) override;
  IOStatus ReopenWritableFile(const std::string&, const FileOptions&,
                              std::unique_ptr<FSWritableFile>*,
                              IODebugContext*) override;
  IOStatus NewDirectory(const std::string&, const IOOptions&,
                        std::unique_ptr<FSDirectory>*,
                        IODebugContext*) override;
  IOStatus FileExists(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus GetFileSize(const std::string&, const IOOptions&, uint64_t*,
                       IODebugContext*) override;
  IOStatus GetChildren(const std::string&, const IOOptions&,
                       std::vector<std::string>*, IODebugContext*) override;
  IOStatus DeleteFile(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus SyncFile(const std::string&, const FileOptions&, const IOOptions&,
                    bool, IODebugContext*) override;

 private:
  std::string index_, data_;
  std::shared_ptr<TieredStorage> tier_;
  std::shared_ptr<SstStorage> sst_;
  FileLock* lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
