//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <condition_variable>
#include <deque>
#include <thread>

#include "rocksdb/file_system.h"
#include "utilities/metabypass/native_files.h"
#include "utilities/metabypass/sst_tiering.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
// Owns placement and immutable objects, but delegates all heat and policy work.
// Lock order: mutation_ -> mutex_ -> Entry::mutex. File reads take only an
// Entry lock to pin a handle and perform I/O after releasing it. Background
// mutation I/O never holds mutex_ or any DB/backup lock.
class SstStorage : public FileSystemWrapper {
 public:
  SstStorage(std::shared_ptr<FileSystem> fs, std::string index,
             const MetaBypassOptions& options, SstClock clock = SstNowMicros);
  ~SstStorage() override;
  const char* Name() const override { return "MetaBypassSstStorage"; }
  // Called with exclusive data and backup directory locks, before recovery.
  Status Initialize(const std::string& identity, bool restore);
  Status Protect(const std::string& point, const NativeState& state);
  Status RestoreTable(const std::string& source, uint64_t number,
                      uint64_t size);
  void SetFailureHandler(std::function<void(const Status&)> handler);
  void Start();
  void Stop();
  void AddStats(MetaBypassStats* stats) const;
  bool IsTable(const std::string& path, uint64_t* number = nullptr) const;
  IOStatus NewRandomAccessFile(const std::string&, const FileOptions&,
                               std::unique_ptr<FSRandomAccessFile>*,
                               IODebugContext*) override;
  IOStatus NewSequentialFile(const std::string&, const FileOptions&,
                             std::unique_ptr<FSSequentialFile>*,
                             IODebugContext*) override;
  IOStatus GetChildren(const std::string&, const IOOptions&,
                       std::vector<std::string>*, IODebugContext*) override;
  IOStatus GetChildrenFileAttributes(const std::string&, const IOOptions&,
                                     std::vector<FileAttributes>*,
                                     IODebugContext*) override;
  IOStatus FileExists(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus GetFileSize(const std::string&, const IOOptions&, uint64_t*,
                       IODebugContext*) override;
  IOStatus DeleteFile(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus LinkFile(const std::string&, const std::string&, const IOOptions&,
                    IODebugContext*) override;
  IOStatus SyncFile(const std::string&, const FileOptions&, const IOOptions&,
                    bool, IODebugContext*) override;
  void SupportedOps(int64_t& operations) override { operations = 0; }

 private:
  friend class SstLogicalReader;
  friend class SstStorageTest;
  struct Handle {
    std::unique_ptr<FSRandomAccessFile> file;
  };
  struct Entry {
    mutable std::mutex mutex;
    uint64_t number = 0, size = 0, changed = 0;
    uint32_t crc = 0, in_rounds = 0, out_rounds = 0;
    bool hot = true, live = true;
    std::string object;
    size_t readers = 0;
    std::shared_ptr<Handle> handle;
  };
  struct Retired {
    std::shared_ptr<Handle> handle;
    std::string path;
    uint64_t size;
  };
  std::shared_ptr<Entry> Find(uint64_t number) const;
  Status Discover();
  Status SavePlacement(uint64_t changed_number = 0, int hot = -1);
  Status LoadPlacement();
  Status MakeObject(const std::string& source, const std::shared_ptr<Entry>& e);
  Status OpenHandle(const std::shared_ptr<Entry>& entry, bool hot,
                    std::shared_ptr<Handle>* handle);
  Status Migrate(const SstMigrationIntent& intent);
  Status CopyLimited(const std::string& source, const std::string& destination,
                     uint64_t size, uint32_t expected, uint64_t promotion = 0);
  void Tick();
  void Run();
  void Execute();
  void Reap();
  std::string Local(uint64_t number) const;
  std::string Path(const std::shared_ptr<Entry>& entry) const;
  const std::string index_, backup_, store_;
  const SstTieringOptions options_;
  const SstClock clock_;
  SstSampler sampler_;
  std::string identity_;
  uint64_t epoch_ = 0;
  mutable std::mutex mutex_;
  std::mutex mutation_;
  std::condition_variable wake_;
  std::map<uint64_t, std::shared_ptr<Entry>> entries_;
  std::deque<SstMigrationIntent> queue_;
  std::set<uint64_t> desired_;
  uint64_t migrating_ = 0;
  std::vector<Retired> retired_;
  // Failed promotion copies remain charged until the cold map and deletion
  // are durably reconciled. At most one reservation exists per file.
  std::map<uint64_t, uint64_t> orphaned_;
  SstTieringStats stats_;
  std::function<void(const Status&)> failure_;
  bool stopping_ = false;
  std::thread worker_, executor_;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
