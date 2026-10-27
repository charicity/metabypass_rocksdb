# Bounded segmented staging and independent HDD suffixes

Ordinary Integrated BlobDB produces blobs in native flush and compaction.
Direct Write produces blob records in the foreground and keeps its per-record
Flush contract. Staging changes physical blob storage, not native record or
checkpoint boundaries.

Enable staging with disjoint `staging_dir` and positive `staging_capacity`.
Physical SSD payload segments are at most 32 MiB. Admission reserves payload
bytes under the accounting mutex before writing; retained, migrating and
read-pinned SSD segments stay charged. Filesystem block rounding, metadata,
WAL/index space and HDD space are separate. Insufficient admission permanently
switches that blob to an HDD suffix beginning at its current logical length.
Large Appends split into bounded chunks and do not require the whole record to
fit in staging.

A full SSD segment is flushed, synced and closed, and its frozen range is
published. The worker can migrate it while the logical blob remains open and
the caller writes another segment. The worker yields after each 4 MiB copy
quantum, alternating explicit-barrier priority with round-robin blob service.
Its one data buffer is at most 4 MiB, below the fixed 16 MiB limit; queued work
contains metadata only. A yield does not seal or publish a partial extent.

After spilling, the caller writes and flushes independent HDD suffix extents
directly. It never waits for prefix migration just to Append, and it never
sends a per-record Append task to the worker. The worker and caller own distinct
physical writers and CRC accumulators. Full HDD extents and Sync/Close tails
still require Fsync, whole-extent CRC readback and Close. These real I/O costs
remain possible on the foreground path.

## Publication and lifetime

Each blob has an append mutex, a descriptor commit mutex and state protected by
the global accounting mutex, acquired in that order. The worker never takes the
append mutex. No payload copying, Fsync/readback, worker wait or pin drain occurs
under the descriptor commit mutex. The global mutex never covers physical I/O.

A new caller-owned segment is created and its empty identity synced before its
identity, logical start and maximum length are durably registered. Only then
may it accept bytes. Metadata commits merge an identity-checked range delta
into the latest generation, retaining concurrent unrelated tail changes.

Migration copies a frozen SSD segment, checks its expected CRC, Fsyncs and
rereads the entire HDD extent, closes it and syncs its dependency directory.
It then Fsyncs a temporary descriptor, renames it, and syncs the publication
directory before changing the readable route. Existing snapshot readers keep
physical source pins; new readers use the published route. SSD unlink and
staging-directory sync must both succeed before quota is released. Uncertain
publication preserves source and destination files.

Every operation pins its logical blob; reads and checkpoint digests additionally
pin the physical identities in their fixed snapshot. Whole-blob retirement
rejects new operations, cancels dependent barrier waits and drains existing
operations before deletion. Stop waits for in-flight physical operations.
The first I/O error is sticky and wakes all waiters.

## Durability and recovery

`V` is the completed Append/Flush-visible end. `D` is the contiguous durable end
across SSD and HDD. `H` is the contiguous sealed HDD end; a durable HDD suffix
cannot advance `H` over an SSD prefix. Each barrier captures its own target.
SSD-mode Sync/Fsync establishes `D`; spilled-mode Sync/Fsync waits for `H`.
SSD-mode Close freezes the tail and publishes logical closure without waiting
for migration. Spilled Close additionally waits for all data on HDD and a
closed, fully sealed descriptor.

Persist and checkpoints require closed blobs with exact lengths and fully
durable HDD mappings. Synchronous writes and SyncBackup retain their native
recovery-point publication barriers. Restore can lose both index and SSD
staging, relying only on HDD data and backup. It validates the selected mapping
and all HDD dependencies before publishing descriptors; later mixed descriptors
are removed without deleting their HDD payloads. The existing durable RESTORING
marker prevents Open from observing partial restore publication and permits
idempotent retries. Historical points retain their own mappings and extents.

The new formats are MBN3 (roots), MBI3 (inventory), MBT3 (range descriptor) and
MBTC3 (checkpoint map). Old formats are rejected before destructive changes;
there is no legacy decoder or conversion path. MBT3 contains a CRC-protected
generation, logical state and explicit contiguous ranges. Each range records
its tier, unique file identity, valid length, durable length, maximum size,
open/frozen state and sealed CRC. Only the last range may be open.

On process-crash recovery, the registered open tail's physical length determines
candidate visible bytes, bounded by its registered maximum and durable minimum.
File length alone never establishes durability or validates a CRC. Recovery
performs real synchronization/sealing before promoting these bytes. Closed
ranges must have their exact registered physical length; truncation or invalid
coverage is corruption. Recovery does not interpret blob records or repair
native MANIFESTs.

## Diagnostics

Existing staging usage and peak counters include retained payload; the peak
also includes outstanding reservations. Migration bytes count actual copies.
`tier_suffix_appends` counts direct HDD Append attempts, and
`staging_spill_bytes` counts their successful payload bytes. The former worker
Append/Switch task and task-wait counters are zero in the pipeline.

`tier_migration_starts` counts segments whose first copy begins.
`tier_migration_start_delay_micros` sums freeze-to-first-copy delay.
`tier_prefix_write_micros` and `tier_suffix_write_micros` sum caller SSD/HDD
Append/Flush time. `tier_barrier_wait_micros` measures HDD barrier waits;
`tier_metadata_commit_micros` includes commit-lock acquisition and descriptor
publication. `tier_quota_admission_micros` measures quota checks/reservations,
not backpressure waiting: unsuccessful admission spills immediately.

Extent Fsync and metadata directory-sync counters include failed attempts.
Satisfied Persist requests and identical commits skip redundant persistence.
Checkpoint CRC reads retain one fixed mapping/pin for the complete call and
open each needed extent once. This does not replace extent seal readback.
All cumulative times can overlap; do not add them as exclusive wall-time phases.
Cross-version adapters report unsupported statistics as null.
