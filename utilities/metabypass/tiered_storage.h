//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/native_files.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
// Routes logical data-dir blobs to bounded local staging or immutable remote
// extents. Frozen prefixes migrate independently of caller-owned HDD suffixes.
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
  // Submit an already-qualified whole-object retirement to the sole worker.
  Status ReclaimBlob(uint64_t number, uint64_t* bytes);
  Status Persist(const std::map<uint64_t, uint64_t>&);
  // Hash a fixed durable prefix while pinning its extents against reclamation.
  Status DigestDurablePrefix(uint64_t number, uint64_t length, uint32_t* crc);
  Status SaveCheckpoint(const std::string&,
                        const std::map<uint64_t, uint64_t>&);
  Status LoadCheckpoint(const std::string&);
  Status ArchiveUnreferenced(const std::map<uint64_t, uint64_t>&);
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
  static constexpr uint64_t kChunkSize = 4 * 1024 * 1024;
  static constexpr uint64_t kExtentSize = 32 * 1024 * 1024;
  static constexpr uint64_t kQueueLimit = 16 * 1024 * 1024;
  struct Extent {
    uint64_t start = 0, length = 0, durable = 0;
    std::string name;
    bool local = false, open = true;
    uint32_t crc = 0;
    uint64_t frozen_at = 0;  // Runtime scheduling diagnostic only.
  };
  struct Version {
    uint64_t size = 0, generation = 0, boundary = 0;
    bool closed = false, spilled = false;
    std::vector<Extent> extents;
  };
  struct ActiveExtent {
    std::unique_ptr<FSWritableFile> file;
    Extent extent;
    uint32_t crc = 0;
  };
  struct Blob {
    // Lock order: append -> commit -> mutex_. Worker never takes append.
    std::mutex append, commit;
    Version live, durable;
    FileOptions file_options;
    ActiveExtent caller;     // Serialized by append.
    ActiveExtent migration;  // Sole migration worker.
    std::string migration_source;
    uint64_t operations = 0;
    bool retiring = false, closing = false;
  };
  // A whole logical operation pins its Blob. Reads additionally pin the exact
  // physical identities selected by their fixed snapshot.
  class Pin {
   public:
    Pin() = default;
    ~Pin();
    Status Open(TieredStorage*, uint64_t, bool read_only = false);
    void Ranges(const Version&);  // mutex_ held by caller
    std::shared_ptr<Blob> blob;

   private:
    TieredStorage* storage_ = nullptr;
    std::vector<std::string> names_;
  };
  using Patch = std::function<Status(Version*)>;
  static std::string Encode(const Version&);
  static Status Decode(const std::string&, Version*);
  static uint64_t DurableEnd(const Version&, bool hdd);
  static bool Equal(const Version&, const Version&);
  static bool Complete(const Version&);
  std::string Descriptor(uint64_t) const;
  std::string Path(const Extent&) const;
  std::string NewName(uint64_t);
  Status Commit(uint64_t, const std::shared_ptr<Blob>&, const Patch&);
  Status Register(uint64_t, const std::shared_ptr<Blob>&, bool local);
  Status Freeze(uint64_t, const std::shared_ptr<Blob>&);
  Status Seal(ActiveExtent*);
  Status Wait(uint64_t, const std::shared_ptr<Blob>&, uint64_t, bool closed);
  Status Migrate(uint64_t, const std::shared_ptr<Blob>&);
  Status ReclaimLocal();
  Status DeleteRetiredBlob(uint64_t, const std::shared_ptr<Blob>&, uint64_t*);
  Status SyncCommitDirectory();
  void Run();
  void Fail(const Status&);
  IOStatus ReadBlob(uint64_t, uint64_t, size_t, const IOOptions&, Slice*, char*,
                    IODebugContext*);
  IOStatus ReadVersion(const Version&, uint64_t, size_t, const IOOptions&,
                       Slice*, char*, IODebugContext*);
  const MetaBypassOptions options_;
  mutable std::mutex mutex_;
  std::mutex reclamation_;
  std::condition_variable work_, progress_;
  std::map<uint64_t, std::shared_ptr<Blob>> blobs_;
  std::map<std::string, uint64_t> readers_;
  // Published-away SSD sources remain charged until unlink AND dirsync.
  std::map<std::string, uint64_t> retired_local_;
  std::map<uint64_t, uint64_t> requests_;
  uint64_t usage_ = 0, reserved_ = 0, peak_ = 0, migrated_ = 0;
  uint64_t extent_id_ = 0, queued_bytes_ = 0, peak_queued_bytes_ = 0;
  uint64_t spill_files_ = 0, spill_bytes_ = 0;
  uint64_t extent_fsyncs_ = 0, extent_fsync_us_ = 0;
  uint64_t commit_dir_syncs_ = 0, commit_dir_sync_us_ = 0;
  uint64_t persist_skips_ = 0, commit_skips_ = 0;
  uint64_t digest_opens_ = 0, digest_us_ = 0;
  uint64_t migration_starts_ = 0, migration_delay_us_ = 0;
  uint64_t prefix_write_us_ = 0, suffix_write_us_ = 0, suffix_appends_ = 0;
  uint64_t barrier_us_ = 0, metadata_us_ = 0, quota_us_ = 0;
  bool stopping_ = false, restoring_ = false;
  Status error_;
  std::function<void(const Status&)> failure_;
  std::thread worker_;
  FileLock* staging_lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
