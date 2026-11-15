//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {
// Whole-SST placement applies only to the MetaBypass index directory.
enum class SstTieringMode { kDisabled, kObserveOnly, kAdaptive };
struct SstTieringOptions {
  SstTieringMode mode = SstTieringMode::kDisabled;
  // Soft logical SST budget on SSD. Ordinary writes may exceed it until a
  // complete recovery point protects their SSTs. Zero is invalid when enabled.
  uint64_t ssd_capacity_bytes = 0;
  uint32_t reserve_percent = 10;
  // Samples file reads attributed to foreground Get, after block-cache misses.
  // OS page-cache hits are included; these are not physical disk I/O counters.
  uint32_t sample_one_in = 64;
  size_t sample_buffer_capacity = 4096;
  uint64_t interval_ms = 1000;
  uint64_t heat_half_life_ms = 10000;
  uint32_t promote_rounds = 2;
  uint32_t demote_rounds = 3;
  uint64_t min_residency_ms = 10000;
  double replacement_margin = 0.25;
  uint64_t migration_bytes_per_sec = 32 * 1024 * 1024;
  size_t migration_queue_capacity = 64;
};
struct SstTieringStats {
  std::string error;
  uint64_t ssd_bytes = 0, peak_ssd_bytes = 0, hdd_bytes = 0;
  uint64_t protected_bytes = 0, unprotected_bytes = 0;
  uint64_t reserved_bytes = 0, pending_delete_bytes = 0;
  uint64_t sampled_reads = 0, dropped_samples = 0, scan_reads = 0;
  uint64_t promotions = 0, demotions = 0;
  uint64_t promoted_bytes = 0, demoted_bytes = 0;
  uint64_t migration_errors = 0, queued_migrations = 0, oversized_files = 0;
  uint64_t observed_promotions = 0, observed_demotions = 0;
  uint64_t copied_bytes = 0, reused_links = 0;
  uint64_t over_budget_micros = 0;
  double cutoff_score = 0;
};
// Selected before Open; an empty staging_dir disables both policies.
enum class MetaBypassStagingPolicy { kAlways, kAdaptive };

struct MetaBypassOptions {
  // Absolute, normalized, disjoint directories. Both remain required when
  // Options::enable_blob_files is false; data_dir retains identity and locks.
  std::string data_dir;
  std::string backup_dir;
  // Charged pending event memory, including the event being applied. A single
  // file operation exceeding this capacity fails before primary file I/O.
  size_t queue_capacity = 64 * 1024 * 1024;
  // Wake the worker on this many pending bytes or after interval_ms.
  size_t batch_bytes = 256 * 1024;
  uint64_t interval_ms = 1000;
  // Start a checkpoint this long after the first uncovered write or native
  // file change. Later work does not postpone the deadline. Idle databases
  // have no checkpoint timer. Explicit barriers bypass this batching delay.
  uint64_t checkpoint_interval_ms = 30000;
  // Optional fast blob staging. Requires Options::enable_blob_files=true.
  // Empty routes blobs directly to data_dir when blob files are enabled.
  // With staging enabled, data_dir and backup_dir must survive staging loss.
  std::string staging_dir;
  // Required when staging_dir is set. Logical bytes, excluding filesystem
  // allocation overhead. Full staging falls back to HDD; kAdaptive may
  // admit later chunks to SSD again when capacity becomes available.
  uint64_t staging_capacity = 0;
  // Always stages until quota fallback. Adaptive starts on HDD and routes
  // subsequent chunks to SSD while observed HDD pressure is high.
  MetaBypassStagingPolicy staging_policy = MetaBypassStagingPolicy::kAlways;
  // Caller Append/Flush and explicit durability I/O latency budget. Bulk
  // background I/O instead uses the fixed one-second no-progress detector.
  uint64_t staging_latency_budget_us = 20000;
  // Reclaim native obsolete whole blobs after all recovery roots release them.
  // Set before Open. Disabling retains remaining files, not deleted history.
  bool enable_blob_file_gc = true;
  SstTieringOptions sst_tiering;
};
struct MetaBypassStats {
  Status error;
  // Logical blob payload admissions; migration is counted separately below.
  // Log2 microsecond latency buckets: bucket 0 includes 0/1 us, bucket k
  // includes [2^k, 2^(k+1)-1] us. Complete API call latency includes admission.
  std::array<uint64_t, 64> blob_append_latency{}, blob_flush_latency{};
  uint64_t blob_append_calls = 0, blob_append_micros = 0;
  uint64_t blob_flush_calls = 0, blob_flush_micros = 0;
  uint64_t direct_blob_bytes = 0, staged_blob_bytes = 0;
  uint64_t adaptive_to_ssd = 0, adaptive_to_hdd = 0,
           adaptive_quota_fallbacks = 0;
  uint64_t adaptive_pending_seals = 0, adaptive_peak_pending_seals = 0;
  uint64_t adaptive_pending_wait_micros = 0;
  uint64_t adaptive_pressure_response_micros = 0,
           adaptive_max_pressure_response_micros = 0;
  uint64_t adaptive_throttle_micros = 0;
  uint64_t tier_barrier_d_debt_bytes = 0, tier_barrier_h_debt_bytes = 0;
  uint64_t hdd_pressure_entries = 0, hdd_pressure_recoveries = 0;
  uint64_t hdd_latency_pressure_entries = 0, hdd_stall_pressure_entries = 0;
  uint64_t hdd_pressure_residence_micros = 0;
  // Successful filesystem Append/PositionedAppend bytes, not NAND writes.
  // Failed calls can have an unknown partial write, counted by
  // io_failed_writes.
  uint64_t io_index_write_bytes = 0, io_staging_write_bytes = 0;
  uint64_t io_data_write_bytes = 0, io_backup_write_bytes = 0;
  uint64_t io_caller_write_bytes = 0, io_bulk_write_bytes = 0,
           io_barrier_write_bytes = 0;
  uint64_t io_failed_writes = 0;
  uint64_t hdd_append_calls = 0, hdd_flush_calls = 0, hdd_sync_calls = 0;
  uint64_t hdd_dirsync_calls = 0, hdd_read_calls = 0;
  uint64_t hdd_append_micros = 0, hdd_flush_micros = 0, hdd_sync_micros = 0;
  uint64_t hdd_dirsync_micros = 0, hdd_read_micros = 0;
  uint64_t staging_bytes = 0;
  uint64_t peak_staging_bytes = 0;
  uint64_t pending_blob_bytes = 0;
  uint64_t migrated_blob_bytes = 0;
  uint64_t staging_backpressure_micros = 0;
  uint64_t sync_write_micros = 0;
  uint64_t queued_bytes = 0;
  uint64_t peak_queued_bytes = 0;
  uint64_t backpressure_micros = 0;
  uint64_t mirrored_bytes = 0;
  // WAL payload bytes applied to the mirror; included in mirrored_bytes.
  uint64_t mirrored_wal_bytes = 0;
  uint64_t recovery_points = 0;
  uint64_t checkpoint_wait_micros = 0;
  // Attempts and cumulative phase times, including failed attempts. Capture
  // includes the native flush when Direct Write is enabled. The flush counter
  // measures that entire native call, not just the flush job. Publish includes
  // Persist, checksums and the durable pointer update.
  uint64_t checkpoint_captures = 0;
  uint64_t checkpoint_capture_micros = 0, checkpoint_flush_micros = 0;
  uint64_t checkpoint_persist_micros = 0, checkpoint_publish_micros = 0;
  uint64_t staging_spill_files = 0, staging_spill_bytes = 0;
  uint64_t tier_queued_bytes = 0, peak_tier_queued_bytes = 0;
  uint64_t hdd_extents = 0;
  // Steady-clock timestamp (not wall clock) and candidate construction time.
  uint64_t last_publish_micros = 0;
  uint64_t last_build_micros = 0;
  // Time from the oldest newly covered event to publication, including
  // batching.
  uint64_t last_point_lag_micros = 0;
  // Logical sizes of work at capture plus published points; hardlinks count
  // per path. Concurrent mirror progress after capture is not included.
  uint64_t retained_index_bytes = 0;
  // One retired point remains pending until its directory is fully removed.
  // Bytes are the point's logical size, not live disk usage during deletion.
  uint64_t pending_gc_points = 0;
  uint64_t pending_gc_bytes = 0;
  uint64_t gc_micros = 0;
  // File lengths, not allocated blocks. Reclaimed bytes count only completed
  // deletion and directory sync; pending bytes include only known lengths.
  uint64_t pending_blob_gc_files = 0, pending_blob_gc_bytes = 0;
  uint64_t reclaimed_blob_files = 0, reclaimed_blob_bytes = 0;
  uint64_t blob_gc_micros = 0;
  // Cumulative successful candidate file copies; hardlinks do not count.
  uint64_t candidate_copied_bytes = 0;
  SstTieringStats sst_tiering;
  // Cumulative staging task wait from enqueue until completion or interruption.
  // Excludes queue admission, measured by staging_backpressure_micros.
  uint64_t tier_task_wait_micros = 0;
  // Physical staging extent Fsync attempts/time, including failures but not
  // checksum verification or Close. Disabled staging leaves these at zero.
  uint64_t tier_extent_fsyncs = 0;
  uint64_t tier_extent_fsync_micros = 0;
  // Commit dependency and descriptor publication directory sync attempts/time,
  // including failures. Excludes staging eviction and garbage collection.
  uint64_t tier_commit_dir_syncs = 0;
  uint64_t tier_commit_dir_sync_micros = 0;
  // Satisfied blob entries omitted from Persist's queue, and identical durable
  // versions omitted from Commit. Validation and caller watermarks still apply.
  uint64_t tier_persist_skips = 0;
  uint64_t tier_commit_skips = 0;
  // Admitted Append tasks and subsets of tier_task_wait_micros. These waits
  // overlap background I/O and must not be added to I/O time as wall time.
  uint64_t tier_append_tasks = 0;
  uint64_t tier_append_wait_micros = 0;
  uint64_t tier_switch_wait_micros = 0;
  // Durable-prefix checkpoint checksum physical open attempts and whole-call
  // time, including failures and pin acquisition. Disabled staging: zero.
  uint64_t tier_checkpoint_digest_opens = 0;
  uint64_t tier_checkpoint_digest_micros = 0;
  // Pipeline diagnostics. Cumulative times overlap and are not wall time.
  uint64_t tier_suffix_appends = 0;
  uint64_t tier_migration_starts = 0;
  uint64_t tier_migration_start_delay_micros = 0;
  uint64_t tier_prefix_write_micros = 0, tier_suffix_write_micros = 0;
  uint64_t tier_barrier_wait_micros = 0;
  uint64_t tier_metadata_commit_micros = 0;
  // Admission computation only: insufficient quota spills without waiting.
  uint64_t tier_quota_admission_micros = 0;
  // V4 metadata: cumulative attempts/time (including failures); timings
  // overlap. File byte gauges include canonical and temporary SSD metadata,
  // not filesystem block allocation or payload quota.
  uint64_t tier_local_commit_attempts = 0;
  uint64_t tier_local_commit_successes = 0;
  uint64_t tier_local_commit_skips = 0;
  uint64_t tier_local_commit_micros = 0;
  uint64_t tier_local_commit_lock_wait_micros = 0;
  uint64_t tier_local_commit_register = 0;
  uint64_t tier_local_commit_freeze = 0;
  uint64_t tier_local_commit_close = 0;
  uint64_t tier_local_commit_spill = 0;
  uint64_t tier_local_commit_migration = 0;
  uint64_t tier_local_commit_finalize = 0;
  uint64_t tier_local_commit_recovery = 0;
  uint64_t tier_local_commit_gc = 0;
  uint64_t tier_local_metadata_fsyncs = 0;
  uint64_t tier_local_metadata_fsync_micros = 0;
  uint64_t tier_local_metadata_dir_syncs = 0;
  uint64_t tier_local_metadata_dir_sync_micros = 0;
  uint64_t tier_local_register_calls = 0;
  uint64_t tier_local_register_micros = 0;
  uint64_t tier_local_freeze_calls = 0;
  uint64_t tier_local_freeze_micros = 0;
  uint64_t tier_remote_commit_attempts = 0;
  uint64_t tier_remote_commit_successes = 0;
  uint64_t tier_remote_commit_skips = 0;
  uint64_t tier_remote_commit_micros = 0;
  uint64_t tier_remote_commit_lock_wait_micros = 0;
  uint64_t tier_remote_commit_register = 0;
  uint64_t tier_remote_commit_freeze = 0;
  uint64_t tier_remote_commit_close = 0;
  uint64_t tier_remote_commit_spill = 0;
  uint64_t tier_remote_commit_migration = 0;
  uint64_t tier_remote_commit_finalize = 0;
  uint64_t tier_remote_commit_recovery = 0;
  uint64_t tier_remote_commit_gc = 0;
  uint64_t tier_remote_metadata_fsyncs = 0;
  uint64_t tier_remote_metadata_fsync_micros = 0;
  uint64_t tier_remote_metadata_dir_syncs = 0;
  uint64_t tier_remote_metadata_dir_sync_micros = 0;
  uint64_t tier_remote_register_calls = 0;
  uint64_t tier_remote_register_micros = 0;
  uint64_t tier_remote_freeze_calls = 0;
  uint64_t tier_remote_freeze_micros = 0;
  uint64_t tier_migration_publish_calls = 0;
  uint64_t tier_migration_publish_micros = 0;
  uint64_t tier_local_metadata_bytes = 0;
  uint64_t peak_tier_local_metadata_bytes = 0;
  // Per-reason elapsed times include failed attempts and no-op lock waits.
  // Timings overlap across threads/phases and are not additive savings.
  uint64_t tier_local_commit_register_micros = 0;
  uint64_t tier_local_commit_register_lock_wait_micros = 0;
  uint64_t tier_local_commit_register_fsync_micros = 0;
  uint64_t tier_local_commit_register_dir_sync_micros = 0;
  uint64_t tier_local_commit_freeze_micros = 0;
  uint64_t tier_local_commit_freeze_lock_wait_micros = 0;
  uint64_t tier_local_commit_freeze_fsync_micros = 0;
  uint64_t tier_local_commit_freeze_dir_sync_micros = 0;
  uint64_t tier_local_commit_close_micros = 0;
  uint64_t tier_local_commit_close_lock_wait_micros = 0;
  uint64_t tier_local_commit_close_fsync_micros = 0;
  uint64_t tier_local_commit_close_dir_sync_micros = 0;
  uint64_t tier_local_commit_spill_micros = 0;
  uint64_t tier_local_commit_spill_lock_wait_micros = 0;
  uint64_t tier_local_commit_spill_fsync_micros = 0;
  uint64_t tier_local_commit_spill_dir_sync_micros = 0;
  uint64_t tier_local_commit_migration_micros = 0;
  uint64_t tier_local_commit_migration_lock_wait_micros = 0;
  uint64_t tier_local_commit_migration_fsync_micros = 0;
  uint64_t tier_local_commit_migration_dir_sync_micros = 0;
  uint64_t tier_local_commit_finalize_micros = 0;
  uint64_t tier_local_commit_finalize_lock_wait_micros = 0;
  uint64_t tier_local_commit_finalize_fsync_micros = 0;
  uint64_t tier_local_commit_finalize_dir_sync_micros = 0;
  uint64_t tier_local_commit_recovery_micros = 0;
  uint64_t tier_local_commit_recovery_lock_wait_micros = 0;
  uint64_t tier_local_commit_recovery_fsync_micros = 0;
  uint64_t tier_local_commit_recovery_dir_sync_micros = 0;
  uint64_t tier_local_commit_gc_micros = 0;
  uint64_t tier_local_commit_gc_lock_wait_micros = 0;
  uint64_t tier_local_commit_gc_fsync_micros = 0;
  uint64_t tier_local_commit_gc_dir_sync_micros = 0;
  uint64_t tier_remote_commit_register_micros = 0;
  uint64_t tier_remote_commit_register_lock_wait_micros = 0;
  uint64_t tier_remote_commit_register_fsync_micros = 0;
  uint64_t tier_remote_commit_register_dir_sync_micros = 0;
  uint64_t tier_remote_commit_freeze_micros = 0;
  uint64_t tier_remote_commit_freeze_lock_wait_micros = 0;
  uint64_t tier_remote_commit_freeze_fsync_micros = 0;
  uint64_t tier_remote_commit_freeze_dir_sync_micros = 0;
  uint64_t tier_remote_commit_close_micros = 0;
  uint64_t tier_remote_commit_close_lock_wait_micros = 0;
  uint64_t tier_remote_commit_close_fsync_micros = 0;
  uint64_t tier_remote_commit_close_dir_sync_micros = 0;
  uint64_t tier_remote_commit_spill_micros = 0;
  uint64_t tier_remote_commit_spill_lock_wait_micros = 0;
  uint64_t tier_remote_commit_spill_fsync_micros = 0;
  uint64_t tier_remote_commit_spill_dir_sync_micros = 0;
  uint64_t tier_remote_commit_migration_micros = 0;
  uint64_t tier_remote_commit_migration_lock_wait_micros = 0;
  uint64_t tier_remote_commit_migration_fsync_micros = 0;
  uint64_t tier_remote_commit_migration_dir_sync_micros = 0;
  uint64_t tier_remote_commit_finalize_micros = 0;
  uint64_t tier_remote_commit_finalize_lock_wait_micros = 0;
  uint64_t tier_remote_commit_finalize_fsync_micros = 0;
  uint64_t tier_remote_commit_finalize_dir_sync_micros = 0;
  uint64_t tier_remote_commit_recovery_micros = 0;
  uint64_t tier_remote_commit_recovery_lock_wait_micros = 0;
  uint64_t tier_remote_commit_recovery_fsync_micros = 0;
  uint64_t tier_remote_commit_recovery_dir_sync_micros = 0;
  uint64_t tier_remote_commit_gc_micros = 0;
  uint64_t tier_remote_commit_gc_lock_wait_micros = 0;
  uint64_t tier_remote_commit_gc_fsync_micros = 0;
  uint64_t tier_remote_commit_gc_dir_sync_micros = 0;
};
// An observation of native write visibility, not backup durability or an ACK
// frontier. Identity is stable for this DB; session changes on every Open.
struct MetaBypassRecoveryProgress {
  std::string db_identity;
  std::string open_session_id;
  uint64_t visible_sequence = 0;
};
// Continuous native sequence prefix recovered from a trusted checkpoint and
// surviving WALs. Time bounds are optional wall-clock microseconds, estimated
// from visibility samples; they are not an acknowledgement or durability SLA.
struct MetaBypassRecoveryReport {
  uint64_t checkpoint_sequence = 0;
  uint64_t recovered_sequence = 0;
  // native_replay_end includes EOF, truncation and CRC stops that native
  // point-in-time recovery does not expose separately to its WalFilter.
  std::string stop_reason;
  bool has_time_lower_bound = false;
  bool has_time_upper_bound = false;
  uint64_t time_lower_bound_micros = 0;
  uint64_t time_upper_bound_micros = 0;
};
// Experimental single-CF entry point. All mutation goes through this object;
// no mutable underlying DB is exposed. Iterators must be destroyed before
// Close. Write/Get/SyncBackup/Flush/CompactRange can be called concurrently.
// Open and Restore require exclusive application ownership. Close excludes
// other operations except GetRecoveryProgress; destruction remains exclusive.
class MetaBypassDB {
 public:
  // Follows Options::enable_blob_files (default false). When disabled, full
  // values stay in SSTs in index_dir, subject to sst_tiering placement. When
  // enabled, uses Integrated BlobDB with min_blob_size=0 and no value
  // relocation. Direct Write and nonempty staging_dir require
  // enable_blob_files=true. Direct Write follows
  // Options::enable_blob_direct_write and requires concurrent memtable writes
  // and MemPurge to be disabled. Direct Write checkpoints flush through the
  // native engine and omit WALs; otherwise checkpoints retain full-value WALs.
  // Crash recovery with Direct Write is guaranteed through Restore of a
  // published point, not through unregistered active blobs in the primary DB.
  // New databases, same-mode reopen and Restore are supported; switching an
  // existing database between BlobDB and non-BlobDB modes is not guaranteed.
  // Options::env must outlive this object.
  static Status Open(const Options& options, const MetaBypassOptions& bypass,
                     const std::string& index_dir,
                     std::unique_ptr<MetaBypassDB>* result);
  // Restores the latest trusted checkpoint, then replays the continuous
  // recoverable WAL prefix through native RocksDB recovery. The destination
  // must be empty or marked by an interrupted Restore. data_dir and backup_dir
  // must survive and be exclusively owned; staging must be empty. Incremental
  // blob values are read without modifying their sources and flushed into the
  // destination before tiered descriptors are finalized. Call Open afterwards.
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir);
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir,
                        MetaBypassRecoveryReport* report);
  ~MetaBypassDB();
  // In all supported modes, sync=true waits for a complete backup point.
  // A failed remote barrier can follow a successful primary mutation.
  Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  Status Delete(const WriteOptions&, const Slice& key);
  // Requires WAL for the complete batch; rejects disableWAL and batches with
  // a WAL termination point before any mutation.
  Status Write(const WriteOptions&, WriteBatch*);
  Status Get(const ReadOptions&, const Slice& key, std::string* value);
  Iterator* NewIterator(const ReadOptions&);
  Status Flush(const FlushOptions&);
  Status CompactRange(const CompactRangeOptions&, const Slice*, const Slice*);
  // Publishes a point covering writes completed before this call. Direct Write
  // permits a native flush; other modes do not force one.
  // Concurrent writes may or may not be included.
  Status SyncBackup();
  MetaBypassStats GetBackupStats() const;
  // Copies the immutable Open identity/session and native visible sequence.
  // Safe concurrently with writes and Close (but not object destruction).
  // Does not wait for backup, synchronize files, or alter backup error state.
  // Null output returns InvalidArgument; a closed DB returns
  // ShutdownInProgress. On error, output is unchanged. Stop external samplers
  // before Close.
  Status GetRecoveryProgress(MetaBypassRecoveryProgress* output) const;
  // Exclusive with mutation/read operations; progress queries are serialized.
  // Publishes before closing the DB,
  // then drains mirrored close operations.
  // Destroy this object afterwards to release the directory locks.
  Status Close();

 private:
  struct Impl;
  explicit MetaBypassDB(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};
}  // namespace ROCKSDB_NAMESPACE
