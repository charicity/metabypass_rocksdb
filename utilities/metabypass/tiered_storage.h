//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
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
  TieredStorage(std::shared_ptr<FileSystem>, const MetaBypassOptions&,
                const std::string& identity = {},
                std::shared_ptr<HddPressure> pressure = {});
  void SetIdentity(const std::string&);
  Status Preflight(bool restore);
  Status PreflightCheckpoint(const std::string&);
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
  Status SaveCheckpoint(const std::string&,
                        const std::map<uint64_t, uint64_t>&);
  Status LoadCheckpoint(const std::string&, bool trusted = false);
  // Offline view: accepts partial remote descriptors and reads only covered
  // ranges. Does not initialize workers, rewrite descriptors or touch staging.
  // A nonempty checkpoint overlays its fixed blob mappings on this view.
  Status OpenRecoveryView(const std::string& checkpoint);
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
  friend class Backup;
  friend class TierWriter;
  friend class TierReader;
  static constexpr uint64_t kChunkSize = 4 * 1024 * 1024;
  static constexpr uint64_t kExtentSize = 32 * 1024 * 1024;
  static constexpr uint64_t kQueueLimit = 16 * 1024 * 1024;
  struct Extent {
    uint64_t start = 0, length = 0, durable = 0;
    std::string name, origin;
    bool local = false, open = true;
    // Non-appendable HDD ownership awaiting real sync/readback/publication.
    bool pending = false;
    uint32_t crc = 0;
    uint64_t frozen_at = 0;  // Runtime scheduling diagnostic only.
  };
  struct Version {
    uint64_t size = 0, generation = 0, boundary = 0, number = 0;
    std::string identity, incarnation;
    bool retired = false;
    bool closed = false, spilled = false;
    std::vector<Extent> extents;
  };
  struct ActiveExtent {
    std::unique_ptr<FSWritableFile> file;
    Extent extent;
    uint32_t crc = 0, readback_crc = 0;
    uint64_t readback_offset = 0;
    bool synced = false;
  };
  struct Blob {
    // Media commit locks never nest. Neither holds mutex_ across I/O.
    // Worker never takes append or holds commit across HDD I/O.
    std::mutex operation, append, commit, remote_commit;
    Version live, durable, remote;
    bool complete_ready = false, local_present = false;
    FileOptions file_options;
    ActiveExtent caller;     // Serialized by append.
    ActiveExtent migration;  // Sole migration worker.
    std::string migration_source;
    uint64_t operations = 0;
    bool retiring = false, closing = false;
    uint64_t pending_seals = 0;
    // Caller admission history is independent of migrated physical extents.
    // Protected by mutex_; only the caller operation gate changes it.
    bool has_admission = false, last_admission_local = false;
  };
  struct PendingSeal {
    uint64_t number;
    std::shared_ptr<Blob> blob;
    ActiveExtent active;
    bool barrier = false;
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
  static std::string Encode(const Version&, bool local = true);
  static Status Decode(const std::string&, Version*, bool local = true);
  static uint64_t DurableEnd(const Version&, bool hdd);
  static bool Equal(const Version&, const Version&);
  static bool Complete(const Version&);
  std::string Descriptor(uint64_t) const;
  std::string LocalDescriptor(uint64_t) const;
  std::string Tombstone(uint64_t, const Version&) const;
  enum class Reason {
    kRegister,
    kFreeze,
    kClose,
    kSpill,
    kMigration,
    kFinalize,
    kRecovery,
    kGC,
    kCount
  };
  struct MetadataCounters {
    uint64_t attempts = 0, successes = 0, skips = 0, micros = 0, wait = 0;
    uint64_t fsyncs = 0, fsync_us = 0, dirsyncs = 0, dirsync_us = 0;
    uint64_t registers = 0, register_us = 0, freezes = 0, freeze_us = 0;
    std::array<uint64_t, 8> reasons{}, reason_us{}, reason_wait{},
        reason_fsync_us{}, reason_dirsync_us{};
  };
  Status StoreSnapshot(const std::string&, const Version&, bool, Reason);
  Status Publish(uint64_t, const std::shared_ptr<Blob>&, const Extent&);
  Status Finalize(uint64_t, const std::shared_ptr<Blob>&);
  Status Recover(bool restore, bool apply);
  Status MergeRecovered(uint64_t, const Version*, const Version*, Version*);
  Status RefreshMetadataBytes();
  void TrackMetadataBytes(const std::string&, uint64_t,
                          const std::string& removed = {});
  void RecheckMetadataBytes(const std::string&);
  static Status BindCheckpointInventory(const std::string&, const std::string&,
                                        const std::map<uint64_t, Version>&);
  Status CheckRetirement(uint64_t);
  Status ParseCheckpoint(const std::string&, std::map<uint64_t, Version>*,
                         bool validate_payload = true);
  std::string Path(const Extent&) const;
  std::string NewName(uint64_t);
  Status Commit(uint64_t, const std::shared_ptr<Blob>&, const Patch&,
                Reason reason = Reason::kRecovery);
  Status Register(uint64_t, const std::shared_ptr<Blob>&, bool local);
  Status Freeze(uint64_t, const std::shared_ptr<Blob>&);
  Status Seal(ActiveExtent*, bool incremental = false);
  Status EnqueueSeal(uint64_t, const std::shared_ptr<Blob>&,
                     bool install_ssd = false);
  Status FinishSeal(PendingSeal*);
  void DropPending(const std::shared_ptr<Blob>& = {});
  bool Adaptive() const {
    return options_.staging_policy == MetaBypassStagingPolicy::kAdaptive;
  }
  Status Wait(uint64_t, const std::shared_ptr<Blob>&, uint64_t, bool closed,
              bool hdd = true);
  Status Migrate(uint64_t, const std::shared_ptr<Blob>&);
  Status ReclaimLocal();
  Status DeleteRetiredBlob(uint64_t, const std::shared_ptr<Blob>&, uint64_t*);
  Status SyncCommitDirectory(Reason reason = Reason::kRegister);
  void Run();
  void Fail(const Status&);
  IOStatus ReadBlob(uint64_t, uint64_t, size_t, const IOOptions&, Slice*, char*,
                    IODebugContext*);
  IOStatus ReadVersion(const Version&, uint64_t, size_t, const IOOptions&,
                       Slice*, char*, IODebugContext*);
  const MetaBypassOptions options_;
  std::shared_ptr<HddPressure> pressure_;
  bool owns_observer_ = false;
  std::string identity_;
  MetadataCounters local_metadata_, remote_metadata_;
  uint64_t local_metadata_bytes_ = 0, peak_local_metadata_bytes_ = 0;
  std::map<std::string, uint64_t> local_metadata_files_;
  uint64_t migration_publish_calls_ = 0, migration_publish_us_ = 0;
  mutable std::mutex mutex_;
  std::mutex reclamation_;
  std::condition_variable work_, progress_;
  std::map<uint64_t, std::shared_ptr<Blob>> blobs_;
  std::map<std::string, uint64_t> readers_;
  // Published-away SSD sources remain charged until unlink AND dirsync.
  std::map<std::string, uint64_t> retired_local_;
  struct Barrier {
    uint64_t number, length;
    bool hdd, closed;
  };
  std::list<Barrier> barriers_;
  // Advisory maximum for diagnostics; every waiter keeps its own predicate.
  std::map<uint64_t, uint64_t> requests_;
  bool recovering_ = false;
  std::list<PendingSeal> pending_seals_;
  uint64_t pending_count_ = 0, peak_pending_count_ = 0, pending_wait_us_ = 0;
  uint64_t usage_ = 0, reserved_ = 0, peak_ = 0, migrated_ = 0;
  uint64_t extent_id_ = 0, queued_bytes_ = 0, peak_queued_bytes_ = 0;
  std::array<uint64_t, 64> append_latency_{}, flush_latency_{};
  uint64_t append_calls_ = 0, append_us_ = 0, flush_calls_ = 0, flush_us_ = 0;
  uint64_t pressure_response_us_ = 0, max_pressure_response_us_ = 0,
           throttle_us_ = 0;
  uint64_t d_debt_ = 0, h_debt_ = 0;
  uint64_t direct_bytes_ = 0, staged_bytes_ = 0;
  uint64_t to_ssd_ = 0, to_hdd_ = 0, quota_fallbacks_ = 0;
  uint64_t spill_files_ = 0, spill_bytes_ = 0;
  uint64_t extent_fsyncs_ = 0, extent_fsync_us_ = 0;
  uint64_t commit_dir_syncs_ = 0, commit_dir_sync_us_ = 0;
  uint64_t persist_skips_ = 0, commit_skips_ = 0;
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
