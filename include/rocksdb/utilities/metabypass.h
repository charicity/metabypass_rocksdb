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
  // Absolute, normalized, disjoint directories.
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
  uint64_t checkpoint_interval_ms = 5000;
  // Optional fast blob staging. Empty preserves the original storage mode.
  // With staging enabled, data_dir and backup_dir must survive staging loss.
  std::string staging_dir;
  // Required when staging_dir is set. Logical bytes, excluding filesystem
  // allocation overhead. Full staging switches the current Blob to HDD.
  uint64_t staging_capacity = 0;
  // Reclaim native obsolete whole blobs after all recovery roots release them.
  // Set before Open. Disabling retains remaining files, not deleted history.
  bool enable_blob_file_gc = true;
  SstTieringOptions sst_tiering;
};
struct MetaBypassStats {
  Status error;
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
};
// Experimental single-CF entry point. All mutation goes through this object;
// no mutable underlying DB is exposed. Iterators must be destroyed before
// Close. Write/Get/SyncBackup/Flush/CompactRange can be called concurrently.
// Open, Restore and Close require exclusive application ownership.
class MetaBypassDB {
 public:
  // Uses Integrated BlobDB with min_blob_size=0. Direct Write follows
  // Options::enable_blob_direct_write and requires concurrent memtable writes
  // and MemPurge to be disabled. Direct Write checkpoints flush through the
  // native engine and omit WALs; otherwise checkpoints retain full-value WALs.
  // Crash recovery with Direct Write is guaranteed through Restore of a
  // published point, not through unregistered active blobs in the primary DB.
  // Options::env must outlive this object.
  static Status Open(const Options& options, const MetaBypassOptions& bypass,
                     const std::string& index_dir,
                     std::unique_ptr<MetaBypassDB>* result);
  // Copies only the published native index files to an empty directory and
  // verifies file checksums. Tiered mode also requires empty staging (or a
  // resumable restore), and reads values remotely without full rehydration.
  // Does not modify the published backup. data_dir must be
  // exclusively owned. Blob contents are never rewritten. A failed
  // restore can be retried on its marked destination. Call Open afterwards
  // to execute native RocksDB recovery.
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir);
  ~MetaBypassDB();
  // In both storage modes, sync=true waits for a complete backup point.
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
  // permits a native flush; the ordinary BlobDB path does not force one.
  // Concurrent writes may or may not be included.
  Status SyncBackup();
  MetaBypassStats GetBackupStats() const;
  // Exclusive with all other operations. Publishes before closing the DB,
  // then drains mirrored close operations.
  // Destroy this object afterwards to release the directory locks.
  Status Close();

 private:
  struct Impl;
  explicit MetaBypassDB(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};
}  // namespace ROCKSDB_NAMESPACE
