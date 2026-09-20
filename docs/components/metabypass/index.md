# Metabypass research prototype

Metabypass separates values from a RocksDB native index and asynchronously
backs up the native index files. It is a single-column-family research utility,
not a production backup service or a compatible reader for Ceph backup folders.
The public C++ entry point is `rocksdb/utilities/metabypass.h`.

## Ownership and API

`SeparatedStorage` routes `.blob` files to `data_dir`, retains every blob,
validates referenced records and owns exclusive recovery/sealing. Named-file
sync and directory sync establish blob dependencies without taking a DB mutex.
`Backup` intercepts successful index file mutations, mirrors them in one bounded
in-memory queue and publishes independent recovery points. It never writes blob
payloads. `MetaBypassDB` owns the two wrappers, their composite Env and the DB;
it exposes only the supported operations. No DB public virtual API or existing
DB write/recovery implementation is changed.

```cpp
rocksdb::Options options;
options.create_if_missing = true;
options.allow_concurrent_memtable_write = false;
rocksdb::MetaBypassOptions backup;
backup.data_dir = "/experiment/data";
backup.backup_dir = "/experiment/backup";
std::unique_ptr<rocksdb::MetaBypassDB> db;
auto s = rocksdb::MetaBypassDB::Open(options, backup, "/experiment/index", &db);
if (s.ok()) s = db->Put(rocksdb::WriteOptions(), "key", "value");
if (s.ok()) s = db->SyncBackup();
// Check every returned Status. Destroy iterators before Close.
if (db) {
  auto close = db->Close();
  if (s.ok()) s = close;
}
db.reset();  // releases data and backup directory locks
```

The utility enables Blob Direct Write, sets `min_blob_size=0` and disables blob
GC. Put, Get, Delete, WriteBatch, iterators, Flush and CompactRange are supported.
WAL cannot be disabled. Transactions, Merge, wide columns, range deletion,
SingleDelete, timestamps, multiple CFs, ingestion, dynamic options and C/Java
bindings are not exposed. Ordered writes are required: concurrent memtable
writes, pipelined writes, unordered writes and two write queues are rejected.
Separate WAL/CF paths, WAL recycling/archival, WAL filters and WAL tracking,
manual WAL flush, direct/mmap
writes, custom comparators, compaction filters, persistent statistics and an
external SST file manager are not supported. The DB's own Blob Direct Write
validation also applies.

Use absolute normalized, disjoint directories without filesystem aliases.
A new instance requires empty data/backup directories. A normal existing DB
cannot be attached. Existing Metabypass instances require matching native
IDENTITY markers in all three directories. Keep the configured Env alive until
the wrapper is destroyed. Only the wrapper may mutate these directories.

## Mirror and recovery points

The backup contains `work/`, `point-N/` directories, and an atomically replaced
`LATEST` pointer. `work/` is never an authorized restore input. No durable file
operation journal exists. `INVENTORY` contains lengths and CRC32C checksums of
native files and blob prefixes (including the footer for MANIFEST-registered
blobs); it is dependency metadata, not an operation log.
The pointer includes the inventory checksum. IDENTITY binds the directories.

A worker processes a finite prefix of events, flushes its file handles, parses
native MANIFEST edits (including complete atomic groups) and complete WAL
records, and checks referenced SST sizes, native checksums and blob references.
It then synchronizes the required blob prefixes. Immutable SSTs are hardlinked
inside the backup, with a copy fallback when links are unsupported. WAL and
MANIFEST are independent copies limited to complete native record boundaries.
CURRENT is generated independently; IDENTITY and completed OPTIONS are copied.
Temporary files are mirrored as required for control-file renames.

Candidate files, the candidate directory, and then the replacement pointer and
its parent directory are synced in that order. The latest and preceding point
remain retained. Old points are deleted only after publication. Orphan
candidates are reclaimed on the next open. The normal RocksDB checkpoint flow
is not invoked and publication does not request a memtable flush.

All event payloads, including events in flight, are charged before the primary
operation. The defaults are 64 MiB of charged pending events, a 256 KiB trigger
or a 1000 ms timer. Charges include event structures, names and payload bytes;
allocator bookkeeping, RocksDB buffers and validation scratch memory are
additional. An individual file operation larger than the configured queue is
rejected before execution. The writer and queue lock serialize event order;
the worker does not acquire that lock or DB locks. A blocked backup permits
foreground progress until the queue fills. A backup error is sticky, wakes
waiters, stops publication and rejects subsequent writes while reads remain
available. Explicit SyncBackup and Close report it.

SyncBackup waits for a point covering writes completed before its call. It
should be called after the writes whose backup is required. Normal Close first
closes/destroys the primary DB, then drains the mirror and publishes the final
point. Concurrent Close is not supported. There is no fixed RPO: writes after
the last published point may be lost.

## Offline recovery

After destroying the original wrapper and removing or losing the main index:

```cpp
auto s = rocksdb::MetaBypassDB::Restore(options, backup, "/experiment/new-index");
if (s.ok()) {
  s = rocksdb::MetaBypassDB::Open(options, backup, "/experiment/new-index", &db);
}
```

Restore takes exclusive data and backup locks, verifies the published inventory
and all referenced prefixes, copies native files to the empty destination, and
prepares the blob state before RocksDB replays any WAL or generates recovery
SSTs. It validates every required blob before the first mutation. Valid extra
records beyond the checkpoint are preserved. Unreferenced incomplete tails
can be discarded; required corruption is an error. Sealing copies a valid
prefix to a synced temporary file, adds a native footer, then replaces the blob
atomically. The original backup index files remain unchanged.

Recovery rewrites only the destination's native MANIFEST to register previously
unflushed blob files and advance the next file number beyond every retained
blob, including orphan files. Interrupted preparation is retryable: an identity
marker distinguishes an in-progress restore from an arbitrary nonempty target.
Call Restore again on that target before Open. It verifies/copies from the
published point again; already sealed blobs are recognized. Restore never falls
back silently from corruption in the selected published point.

Blob retention causes unbounded data-directory growth. The first implementation
rescans native metadata and referenced records to validate candidates; this is
an explicit performance cost. Scratch buffers for blob validation/copy are
bounded, while native WAL record decoding and MANIFEST state require memory
proportional to their content. `retained_index_bytes` reports logical file sizes
for the working mirror plus the two retained points, counting hardlinks at each
path, not physical allocated storage.

## Validation and benchmarks

See the [validation record](validation.md) for measured results and known
repository test failures.

`metabypass_test` exercises close/reopen, complete index loss, unflushed WAL blob
references, overwrites/deletes/WriteBatch, compaction and rotations, bounded queue
ordering/backpressure, foreground progress with blocked backup, sticky failure,
corruption rejection, publication-stage SIGKILL and retry after interrupted
sealing. Each test process is capped at 60 seconds. The child-process tests
execute a fresh binary, avoiding fork of a live RocksDB worker pool. Expected
values exist only in the test process and are never recovery input.

`db_bench` has a narrow standalone mode so unsupported DB methods cannot bypass
the C++ wrapper. The following modes use `--db`, `--num`, `--value_size`, `--sync`,
and the `--metabypass_*` flags. Other workload flags do not configure this mode.
All parent directories must already exist.

```sh
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j8 db_bench
./db_bench --metabypass_mode=baseline --db=/experiment/base-index \
  --metabypass_data_dir=/experiment/base-data --num=100000 --value_size=1024
./db_bench --metabypass_mode=write --db=/experiment/index \
  --metabypass_data_dir=/experiment/data --metabypass_backup_dir=/experiment/backup \
  --num=100000 --value_size=1024
./db_bench --metabypass_mode=restore --db=/experiment/restored \
  --metabypass_data_dir=/experiment/data --metabypass_backup_dir=/experiment/backup
./db_bench --metabypass_mode=verify --db=/experiment/restored \
  --metabypass_data_dir=/experiment/data --metabypass_backup_dir=/experiment/backup \
  --num=100000 --value_size=1024
```

Baseline uses the same separated Blob Direct Write configuration without backup.
Compare `num / foreground_us` for throughput overhead. Report `sync_us`,
`close_us`, `last_point_build_us`, `last_point_lag_us`, `backpressure_us`, `queue_peak_bytes`, `mirrored_bytes`,
logical `retained_index_bytes`, and `restore_us` separately. Measure physical
space externally (e.g. `du`) so hardlink sharing and filesystem allocation are
not confused. Restore time includes opening RocksDB and native WAL recovery.
Repeat with fresh directories and report host/storage/workload parameters.
Directory-local SIGKILL experiments do not simulate power loss, controller
caches, torn sectors or the original Ceph data lifecycle; no real power-failure
guarantee follows from these results.
