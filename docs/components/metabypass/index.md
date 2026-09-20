# Metabypass research prototype

Metabypass separates values from a RocksDB native index and asynchronously
backs up the native index files. It is a single-column-family research utility,
not a production backup service or a compatible reader for Ceph backup folders.
The public C++ entry point is `rocksdb/utilities/metabypass.h`.

## Ownership and API

`SeparatedStorage` routes `.blob` files to `data_dir`, retains every blob,
validates referenced records and owns exclusive recovery/sealing. Named-file
sync and directory sync establish blob dependencies without taking a DB mutex.
`Backup` intercepts successful index file mutations and mirrors them through
in-memory WAL and metadata queues with one shared capacity budget. It publishes
independent recovery points and never writes blob payloads. `MetaBypassDB` owns the two wrappers, their composite Env and the DB;
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

The mirror worker processes a finite prefix of events, flushes its handles and
captures an independent candidate directory. Closed SSTs are hardlinked, with
a copy fallback; mutable files are copied at the captured lengths. It then
continues consuming events while a second worker validates the candidate.
There is at most one candidate in capture/validation, in addition to the two
published points. While validation is busy, later events accumulate in the
working mirror rather than in additional candidate directories.

The validator parses native MANIFEST edits (including complete atomic groups),
finds complete WAL record boundaries, and checks referenced SST sizes and
content dependencies. It truncates its private WAL/MANIFEST copies to these
boundaries, removes unneeded captured files, generates CURRENT and synchronizes
blob dependencies before publication. Incomplete native groups defer publication
until a later event boundary; they do not replace the preceding point.

Validation is incremental within one process. Unchanged immutable SSTs reuse
validated references and checksums. WALs still undergo native record parsing,
but previously checked records do not repeat blob reference validation. Blob
persistence validation and inventory CRC computation advance over new complete records and the final
footer, without rereading previously validated prefixes in that pass. Newly created
SSTs still validate their references, even when those values previously
appeared in a WAL. Index inventory CRCs
also extend over appended bytes. Creation, truncation and rename change a
file's cache identity; vanished index files are pruned from the caches. A
shorter required prefix is recomputed rather than using a longer prefix's CRC.

Caches are volatile and assume exclusive ownership of append-only blobs and
immutable SSTs. They are not a continuous media scrub: latent corruption of
previously validated bytes may be detected at restore rather than at the next
publication. Cached checksums retain the original expected content; they do not
bless changed old bytes. Restart discards all caches, and offline restore checks
all published native files and required blob prefixes from disk. The backup
format and restore compatibility are unchanged.

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
rejected before execution. Two channels encapsulate their own operation mutex,
event queue and capacity wait: WAL, and metadata (SST, MANIFEST, CURRENT,
OPTIONS, IDENTITY and temporary files). Each operation mutex serializes primary
operations through enqueue within its channel; neither worker acquires it or DB
locks. Cross-channel rename acquires both operation mutexes and enqueues one
metadata control event. One short coordination mutex protects both queues,
global sequence numbers and the shared capacity budget; it never spans backup
I/O. The single mirror worker merges queue heads by accepted sequence, with no
WAL priority or reserved capacity. Each capture follows a complete applied
sequence prefix. A slow primary SST operation therefore does not hold the WAL
operation mutex, but shared capacity and mirror I/O can still block WAL.
On failure, queued events release their charges while outstanding primary
operations retain theirs until completion. Pointer publication and sticky
failure recording share a separate mutex. A blocked validator permits the
mirror to drain; blocked mirror/candidate-copy I/O can still fill the queue. A backup error is sticky, wakes
waiters, stops publication and rejects subsequent writes while reads remain
available. Explicit SyncBackup and Close report it.

The backup uses separate wait channels for mirror work, candidate validation,
per-channel queue capacity, and published progress. An enqueue notifies an idle mirror only
when its work predicate is satisfied.
A blocked capacity reservation requests immediate draining, even below the
batch threshold. Below-threshold traffic otherwise uses the interval timer.
Publication and errors wake every SyncBackup waiter; error and shutdown paths
also release the relevant worker and capacity waits. Per-event consumption
only notifies a producer actually waiting for enough capacity.

SyncBackup waits for a point covering writes completed before its call. It
should be called after the writes whose backup is required. Normal Close first
closes/destroys the primary DB, then drains the mirror and publishes the final
point, then joins both workers. SyncBackup waits for validation and publication,
not just mirror application. Concurrent Close is not supported. There is no fixed RPO: writes after
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

Blob retention causes unbounded data-directory growth. Candidate capture still copies mutable files and validation still parses native
WAL/MANIFEST metadata; those costs remain despite content-cache reuse. Scratch buffers for blob validation/copy are
bounded, while native WAL record decoding and MANIFEST state require memory
proportional to their content. `retained_index_bytes` reports logical file sizes
for the working mirror at capture plus the two retained points, counting
hardlinks at each path, not physical allocated storage. Later concurrent mirror
progress is not included. `validated_blob_bytes` counts cumulative bytes read
by incremental blob persistence validation (reference checks are additional);
`reused_tables` counts successful SST validation-cache reuse.

## Validation and benchmarks

See [pipeline validation](pipeline-validation.md) for the two-thread and
incremental-validation tests and performance comparison.

See the [validation record](validation.md) for measured results and known
repository test failures.
The [content-scan ablation experiment](experiments/README.md) measures recovery
point costs with content validation removed in a separate experimental binary.

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

See [foreground-time profiling](foreground-profile.md) for the measured cause
of the pipeline's foreground regression and notification-only controls.


See [notification fix validation](notification-validation.md) for the wait
protocol, concurrency coverage, and before/after measurements.

See [the four-version performance comparison](version-comparison.md) for
no-backup, original, incremental, and notification-fixed results.

See the [Chinese implementation, execution-flow and test report](optimization-summary.zh-CN.md) for a
compact list of implemented improvements, their rationale, and measured effects.

See [WAL/SST scheduling experiments](scheduling-ab.md) for isolated A/B tests of
operation-lock separation, WAL capacity headroom, and bounded-window priority.

See [dual-channel validation](channel-validation.md) for concurrency coverage and
the comparison with the notification and experimental split-lock versions.

For the fresh four-version comparison including backup disabled, see
[channels versus no backup](channel-baseline-comparison.md).
