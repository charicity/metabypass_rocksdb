//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

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
struct MetaBypassOptions {
  // Absolute, normalized directory, disjoint from the index directory.
  std::string backup_dir;
  // Independent limits in bytes for WAL and metadata events, including
  // reservations and events being applied. An event exceeding its queue's
  // limit fails before primary file I/O.
  size_t wal_queue_capacity = 896 * 1024 * 1024;
  size_t metadata_queue_capacity = 128 * 1024 * 1024;
  // Wake the worker on this many pending bytes across both queues or after
  // interval_ms.
  size_t batch_bytes = 512 * 1024 * 1024;
  uint64_t interval_ms = 1000;
  // Start a checkpoint this long after the first uncovered write or native
  // file change. Later work does not postpone the deadline. Idle databases
  // have no checkpoint timer. Explicit barriers bypass this batching delay.
  uint64_t checkpoint_interval_ms = 40000;
  SstTieringOptions sst_tiering;
  // Set before Open/Restore. Disable the periodic PROGRESS sampler and
  // recovery time estimation, including bounds in a resumed restore marker.
  // Sequence recovery is unchanged; Restore still clears old samples once.
  bool enable_recovery_time_sampling = true;
};
struct MetaBypassStats {
  Status error;
  // HDD operation counters cover only paths in the backup directory.
  // Successful filesystem Append/PositionedAppend bytes, not NAND writes.
  // Failed calls can have an unknown partial write, counted by
  // io_failed_writes.
  uint64_t io_index_write_bytes = 0, io_backup_write_bytes = 0;
  uint64_t io_caller_write_bytes = 0, io_bulk_write_bytes = 0,
           io_barrier_write_bytes = 0;
  uint64_t io_failed_writes = 0;
  uint64_t hdd_append_calls = 0, hdd_flush_calls = 0, hdd_sync_calls = 0;
  uint64_t hdd_dirsync_calls = 0, hdd_read_calls = 0;
  uint64_t hdd_append_micros = 0, hdd_flush_micros = 0, hdd_sync_micros = 0;
  uint64_t hdd_dirsync_micros = 0, hdd_read_micros = 0;
  uint64_t sync_write_micros = 0;
  uint64_t queued_bytes = 0;
  // The sum of the individual queue high-water marks bounds the peak.
  uint64_t peak_queued_bytes = 0;
  uint64_t backpressure_micros = 0;
  uint64_t mirrored_bytes = 0;
  // WAL payload bytes applied to the mirror; included in mirrored_bytes.
  uint64_t mirrored_wal_bytes = 0;
  uint64_t recovery_points = 0;
  uint64_t checkpoint_wait_micros = 0;
  // Attempts and cumulative phase times, including failed attempts. Publish
  // includes file synchronization and the durable pointer update.
  uint64_t checkpoint_captures = 0;
  uint64_t checkpoint_capture_micros = 0, checkpoint_publish_micros = 0;
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
  // Cumulative successful candidate file copies; hardlinks do not count.
  uint64_t candidate_copied_bytes = 0;
  SstTieringStats sst_tiering;
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
  // Values remain inline in the WAL and SSTs. Whole SSTs can be relocated
  // through sst_tiering. Options::env must outlive this object.
  static Status Open(const Options& options, const MetaBypassOptions& bypass,
                     const std::string& index_dir,
                     std::unique_ptr<MetaBypassDB>* result);
  // Restores the latest trusted checkpoint, then replays the continuous
  // recoverable WAL prefix through native RocksDB recovery. The destination
  // must be empty or marked by an interrupted Restore. Restore exclusively
  // owns the destination and backup directories for the duration of the call.
  // Call Open afterwards.
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir);
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir,
                        MetaBypassRecoveryReport* report);
  ~MetaBypassDB();
  // sync=true waits for a complete backup point.
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
  // Publishes a point covering writes completed before this call without
  // forcing a memtable flush. Concurrent writes may also be included.
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
  Status WriteImpl(const WriteOptions&, WriteBatch*, bool check_batch);
  struct Impl;
  explicit MetaBypassDB(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};
}  // namespace ROCKSDB_NAMESPACE
