# Strong tiered blob storage

Metabypass can stage Blob Direct Write payloads on a fast filesystem and migrate
native blob bytes to a separate slow filesystem. This is an opt-in, experimental,
single-column-family configuration. Non-tiered databases retain their direct payload placement. Both modes now
wait for a complete backup point on synchronous writes; see
[direct storage](direct-storage.md). Converting an existing database to tiered
storage is not supported.

```cpp
MetaBypassOptions bypass;
bypass.staging_dir = "/fast/blob-staging";
bypass.staging_capacity = 256ULL * 1024 * 1024;
bypass.data_dir = "/slow/blobs";
bypass.backup_dir = "/slow/index-backup";
```

All directories, including the primary index directory, must be absolute,
normalized and disjoint. The application must place both slow directories outside
the fast device's failure domain. A different pathname alone does not establish
an independent failure domain. The backend must implement file and directory
synchronization and atomic rename. S3 is not supported by this version.

## Completion contracts

| Operation | Successful completion |
|---|---|
| `WriteOptions::sync=false` | Primary write completed; remote publication may lag |
| `WriteOptions::sync=true` | A complete remote point covers the write |
| `SyncBackup()` | A complete remote point covers writes completed before the call |
| `Close()` | Primary DB closed and final complete remote point published |

The tiered synchronous-write contract is stronger than native RocksDB's local
WAL sync: the primary index and staging device may subsequently be lost. A write
whose primary operation succeeds but whose remote barrier fails returns an error;
the primary mutation may already be visible. There is no rollback promise.
Concurrent later writes can be covered by the same barrier. Ordinary writes do
not wait for a periodic remote point, except through bounded-capacity backpressure
or shared resource contention. There is no fixed recovery point objective.

## Ownership and scheduling

`SeparatedStorage` owns a `TieredStorage` filesystem wrapper. RocksDB still sees
ordinary numbered blob paths; the wrapper provides local reads or a logical
random-access view over remote extents. The index mirror remains responsible only
for native index files. It retains its WAL/metadata queues and its mirror and
validation threads. One additional worker performs blob migration.

The validator computes and validates required blob prefixes, then requests their
remote persistence. Requested dependencies take precedence over unrelated closed
files. Only one candidate is outstanding. Waiting for blob persistence does not
hold the DB mutex or stop the mirror from applying further events. It can delay
publication and explicit synchronization. Capture still copies mutable index
files and can temporarily delay the mirror itself.

Tiered foreground write admission is serialized before entering RocksDB. This
keeps byte reservations and pressure rotation outside DB locks. It is a deliberate
first-version cost, to be included in benchmarks. Synchronous-write barriers are
waited after releasing the admission lock.

## Remote representation

Remote payload extents contain unmodified ranges of native blob bytes. Extents
are immutable and bounded to 256 KiB per file. A checksum-protected `MBT1`
descriptor assembles them into a logical blob. A descriptor is replaced only
after its new extents and directory entries are durable. Newly copied extents
are read back and checksummed before committing the descriptor.

A recovery point includes a checksummed `BLOB-MAP`, binding the exact extent view
and validated length of each dependency. Its inventory also checksums each
logical blob prefix. A descriptor may later advance without changing the old
point's view. Only a complete record prefix is published; extent boundaries
inside that prefix need not themselves be record boundaries. No custom file
operation log is replayed during recovery.

Local reads pin a staging file for an individual I/O operation. Eviction waits
for these reads, publishes the remote view first, and then removes the local
file. Cached logical readers subsequently resolve the remote view. An active
local blob cannot silently fall back to a shorter remote prefix.

The remote store retains all blob payloads, including historical extents. This
version does not implement blob GC or remote extent consolidation. Small
synchronous writes can create many extents and descriptors; complete-point
publication is intentionally more expensive than simply synchronizing a WAL.

## Capacity and failures

`staging_capacity` is mandatory in tiered mode (minimum 8 KiB). It accounts for
logical staged file bytes and in-flight admission, not filesystem allocation,
page cache, or unrelated files. Admission uses a conservative batch-size and
per-record bound; an oversized batch is rejected before primary writes. Existing
files also require footer headroom.

Closed, fully migrated files can be evicted. Under pressure, the caller seals
active and deferred blob files while retaining their memtable generation
ownership. This is the only pressure-specific core addition; it does not switch
or flush a memtable. Publication and synchronous writes do not otherwise force
blob rotation.

Migration and backup failures share a terminal error state, wake capacity and
barrier waiters, stop publication and reject new writes. Reads that still have a
valid source remain available. Explicit synchronization and close report the
error. Backend calls must eventually complete: this wrapper cannot cancel an
arbitrarily hung filesystem call.

## Recovery

Normal `Open` uses the primary index and WAL. It does not replace a broken primary
with an older backup. Missing unprotected staging dependencies are errors.

To recover from complete fast-device loss, use `Restore` with empty replacement
index and staging directories and the original slow directories. Recovery checks
the published pointer, inventory, blob map and referenced payload checksums.
It creates separate recovery descriptors/footer extents instead of editing old
payload extents. Logical remote reads avoid loading the entire database into the
staging budget. Allocated blob numbers, including orphan extent names, advance
the native file-number high-water mark before new writes.

Restore is exclusive and resumable on its marked destination. The published
index point and its extent mapping remain unchanged. Corruption in a published
point is reported; no silent fallback to an older point is performed.

## Validation and measurements

`metabypass_test` includes full fast-directory loss, SIGKILL at migration and
publication boundaries, bounded staging without memtable flush, migration errors,
blocked migration with foreground progress, compressed batches, and reopen tests.
Fault injection and directory removal do not establish real power-loss safety.

`db_bench` accepts `--metabypass_staging_dir` and
`--metabypass_staging_capacity`. `--metabypass_slow_write_delay_us` injects latency
per slow writable-file append/sync operation for controlled experiments. This
models neither device bandwidth nor all filesystem calls. Outputs separate
foreground, final sync, close, p99 write latency, migration bytes, staging peak,
capacity waits and recovery-point lag. The benchmark's latency sample vector is
outside the storage budget and is used in all comparison configurations.

See [the implementation test report](tiered-storage-report.zh-CN.md) for measured
results, commands and environmental limitations.
