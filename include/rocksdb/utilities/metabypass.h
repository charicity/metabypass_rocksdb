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
struct MetaBypassOptions {
  // Absolute, normalized, disjoint directories. Values are retained forever.
  std::string data_dir;
  std::string backup_dir;
  // Charged pending event memory, including the event being applied. A single
  // file operation exceeding this capacity fails before primary file I/O.
  size_t queue_capacity = 64 * 1024 * 1024;
  // Wake the worker on this many pending bytes or after interval_ms.
  size_t batch_bytes = 256 * 1024;
  uint64_t interval_ms = 1000;
  // Optional fast blob staging. Empty preserves the original storage mode.
  // With staging enabled, data_dir and backup_dir must survive staging loss.
  std::string staging_dir;
  // Required when staging_dir is set. Logical bytes, excluding filesystem
  // allocation overhead. Oversized batches are rejected before DB writes.
  uint64_t staging_capacity = 0;
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
  uint64_t recovery_points = 0;
  // Cumulative bytes scanned by incremental blob persistence validation.
  // Reference checks and native index parsing are additional work.
  uint64_t validated_blob_bytes = 0;
  uint64_t reused_tables = 0;
  // Steady-clock timestamp (not wall clock) and candidate construction time.
  uint64_t last_publish_micros = 0;
  uint64_t last_build_micros = 0;
  // Time from the oldest newly covered event to publication, including
  // batching.
  uint64_t last_point_lag_micros = 0;
  // Logical sizes of work at capture plus published points; hardlinks count
  // per path. Concurrent mirror progress after capture is not included.
  uint64_t retained_index_bytes = 0;
};
// Experimental single-CF entry point. All mutation goes through this object;
// no mutable underlying DB is exposed. Iterators must be destroyed before
// Close. Write/Get/SyncBackup/Flush/CompactRange can be called concurrently.
// Open, Restore and Close require exclusive application ownership.
class MetaBypassDB {
 public:
  // Forces blob direct writes and min_blob_size=0. The caller must disable
  // allow_concurrent_memtable_write. Rejects ordinary existing databases.
  // Options::env must outlive this object.
  static Status Open(const Options& options, const MetaBypassOptions& bypass,
                     const std::string& index_dir,
                     std::unique_ptr<MetaBypassDB>* result);
  // Copies only the published native index files to an empty directory and
  // prepares blob metadata. Tiered mode also requires empty staging (or a
  // resumable restore), and reads values remotely without full rehydration.
  // Does not modify the published backup. data_dir must be
  // exclusively owned; blob files may be sealed during preparation. A failed
  // restore can be retried on its marked destination. Call Open afterwards
  // to execute native RocksDB recovery.
  static Status Restore(const Options& options, const MetaBypassOptions& bypass,
                        const std::string& empty_index_dir);
  ~MetaBypassDB();
  // In both storage modes, sync=true waits for a complete backup point.
  // A failed remote barrier can follow a successful primary mutation.
  Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  Status Delete(const WriteOptions&, const Slice& key);
  Status Write(const WriteOptions&, WriteBatch*);
  Status Get(const ReadOptions&, const Slice& key, std::string* value);
  Iterator* NewIterator(const ReadOptions&);
  Status Flush(const FlushOptions&);
  Status CompactRange(const CompactRangeOptions&, const Slice*, const Slice*);
  // Publishes a point covering writes completed before this call. No flush
  // is forced; concurrent writes may or may not be included.
  Status SyncBackup();
  MetaBypassStats GetBackupStats() const;
  // Exclusive with all other operations. Closes DB, then drains the backup.
  // Destroy this object afterwards to release the directory locks.
  Status Close();

 private:
  struct Impl;
  explicit MetaBypassDB(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};
}  // namespace ROCKSDB_NAMESPACE
