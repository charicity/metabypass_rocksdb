//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/native_files.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
// Routes logical data-dir blobs to bounded local staging or immutable remote
// extents. Only the migration worker writes remote extents during normal use.
class TieredStorage : public FileSystemWrapper {
 public:
  TieredStorage(std::shared_ptr<FileSystem>, const MetaBypassOptions&);
  ~TieredStorage() override;
  const char* Name() const override { return "MetaBypassTieredStorage"; }
  Status Initialize(bool restore);
  void Start();
  Status Stop();
  Status Error() const;
  void Cancel(const Status&);
  void SetFailureHandler(std::function<void(const Status&)>);
  Status Reserve(uint64_t bytes, const std::function<Status()>& rotate);
  void ReleaseReservation();
  Status Persist(const std::map<uint64_t, uint64_t>&);
  Status SaveCheckpoint(const std::string&,
                        const std::map<uint64_t, uint64_t>&);
  Status LoadCheckpoint(const std::string&);
  Status ArchiveUnreferenced(const std::map<uint64_t, uint64_t>&);
  Status SealRecovery(uint64_t number, uint64_t end, const Slice& footer);
  void AddStats(MetaBypassStats*) const;
  bool IsBlob(const std::string&, uint64_t* = nullptr) const;
  IOStatus NewWritableFile(const std::string&, const FileOptions&,
                           std::unique_ptr<FSWritableFile>*,
                           IODebugContext*) override;
  IOStatus ReopenWritableFile(const std::string&, const FileOptions&,
                              std::unique_ptr<FSWritableFile>*,
                              IODebugContext*) override;
  IOStatus NewRandomAccessFile(const std::string&, const FileOptions&,
                               std::unique_ptr<FSRandomAccessFile>*,
                               IODebugContext*) override;
  IOStatus NewSequentialFile(const std::string&, const FileOptions&,
                             std::unique_ptr<FSSequentialFile>*,
                             IODebugContext*) override;
  IOStatus GetFileSize(const std::string&, const IOOptions&, uint64_t*,
                       IODebugContext*) override;
  IOStatus FileExists(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus GetChildren(const std::string&, const IOOptions&,
                       std::vector<std::string>*, IODebugContext*) override;
  IOStatus SyncFile(const std::string&, const FileOptions&, const IOOptions&,
                    bool, IODebugContext*) override;

 private:
  friend class TieredStorageTest;
  friend class TierWriter;
  friend class TierReader;
  struct Extent {
    uint64_t length;
    std::string name;
  };
  struct Version {
    uint64_t size = 0;
    bool sealed = false;
    std::vector<Extent> extents;
    // Exclusive logical end of each extent, built before publication.
    std::vector<uint64_t> ends;
  };
  struct Local {
    uint64_t size = 0;
    bool closed = false;
    // Physical local I/O pins, not the lifetime of logical TierReaders.
    uint64_t readers = 0;
    bool evicting = false;
  };
  // Stack-owned per-I/O pin. Open requires mutex_; destruction must not hold
  // it.
  class LocalReadPin {
   public:
    LocalReadPin() = default;
    ~LocalReadPin();
    LocalReadPin(const LocalReadPin&) = delete;
    LocalReadPin& operator=(const LocalReadPin&) = delete;
    IOStatus Open(TieredStorage*, uint64_t, IODebugContext*);
    FSRandomAccessFile* file() const { return file_.get(); }

   private:
    TieredStorage* storage_ = nullptr;
    uint64_t number_ = 0;
    std::unique_ptr<FSRandomAccessFile> file_;
  };
  static std::string Encode(const Version&);
  static Status Decode(const std::string&, Version*);
  std::string LocalPath(uint64_t) const;
  std::string Descriptor(uint64_t) const;
  static std::shared_ptr<const Version> Snapshot(Version);
  Status Commit(uint64_t, Version);
  Status Migrate(uint64_t, uint64_t, bool background);
  Status AppendExtent(uint64_t, const Slice&, Version*);
  void Run();
  void Fail(const Status&);
  IOStatus ReadBlob(uint64_t, uint64_t, size_t, const IOOptions&, Slice*, char*,
                    IODebugContext*);
  IOStatus ReadVersion(const Version&, uint64_t, size_t, const IOOptions&,
                       Slice*, char*, IODebugContext*);
  void Closed(uint64_t);
  // Single migration worker, or exclusive offline recovery, only. Both stages
  // acquire mutex_ internally; callers must release it first.
  Status SwitchReadsToRemote(uint64_t);
  Status ReclaimLocalAfterReads(uint64_t);
  const MetaBypassOptions options_;
  mutable std::mutex mutex_;
  std::condition_variable work_, progress_;
  std::map<uint64_t, Local> local_;
  std::map<uint64_t, std::shared_ptr<const Version>> versions_;
  std::map<uint64_t, uint64_t> requests_;
  uint64_t usage_ = 0, reserved_ = 0, peak_ = 0, migrated_ = 0, wait_us_ = 0;
  uint64_t extent_id_ = 0;
  bool stopping_ = false, restoring_ = false;
  Status error_;
  std::function<void(const Status&)> failure_;
  std::thread worker_;
  FileLock* staging_lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
