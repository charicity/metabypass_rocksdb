# SSD-local metadata and bounded segmented staging

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

Each blob has an append mutex, independent SSD and HDD metadata commit mutexes,
and state protected by the global accounting mutex. A commit acquires only its
own medium's mutex before the short state lock. The two commit mutexes never
nest, and the worker never takes the append mutex. Payload copying, Fsync and
readback take place before metadata commits. Metadata Fsync/rename/dirsync hold
only that medium's commit mutex. No HDD operation holds the SSD commit mutex;
the global mutex never covers physical I/O or waits.

A new SSD blob creates only its MBL4 snapshot. Segment registration, freezing,
SSD-mode Sync/Fsync and writer Close persist SSD metadata without touching or
waiting for HDD metadata. This guarantee concerns these tier-writer operations:
native checkpoint publication, synchronous Write, SyncBackup and database Close
retain their complete HDD barriers.

A new caller-owned segment is created and its empty identity synced before its
identity, logical start and maximum length are durably registered. Only then
may it accept bytes. Metadata commits merge an identity-checked range delta
into the latest generation, retaining concurrent unrelated tail changes.

Migration copies a frozen SSD segment, checks its expected CRC, Fsyncs and
rereads the entire HDD extent, closes it and syncs its dependency directory.
It then Fsyncs a temporary descriptor, renames it, and syncs the publication
directory, then merges the matching range into a new SSD-local snapshot.
Only successful local publication changes the readable route and applied
durability watermarks. Prefix and suffix owners merge range deltas against the
latest local state; an older HDD generation never replaces unrelated changes.
Existing snapshot readers keep
physical source pins; new readers use the published route. SSD unlink and
staging-directory sync must both succeed before quota is released. Uncertain
publication preserves source and destination files.

Every operation pins its logical blob; reads and checkpoint digests additionally
pin the physical identities in their fixed snapshot. Whole-blob retirement
rejects new operations, cancels dependent barrier waits and drains existing
operations before deletion. GC first writes a durable HDD retirement tombstone,
including for a pure SSD blob. It removes both descriptors, temporary files and
payloads, syncs both directories, then removes and syncs the tombstone. Recovery
replays this sequence without resurrecting a half-deleted blob, after checking
all protected checkpoint references. Stop waits for in-flight physical operations.
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

The formats are MBN4 (roots), MBI4 (inventory), MBL4 (SSD-local recovery),
MBT4 (HDD publication) and MBTC4 (checkpoint map). Old formats are rejected
before destructive changes; there is no legacy decoder or conversion path.
Both snapshots contain a CRC-protected database identity, blob number,
incarnation and independent generation. MBL4 describes contiguous logical
ranges, closure/spill state and each physical range's identity, medium,
durable length, open/frozen state and CRC. Only its last range may be open.
MBT4 describes only sealed HDD ranges with their original SSD identity (or
self-identity for caller-owned HDD suffixes). PARTIAL maps may contain holes;
COMPLETE maps must be closed, contiguous and exact, including empty blobs.
MBTC4 embeds only COMPLETE maps.

Open validates the union of both descriptor namespaces before recovery writes
or cleanup, then repeats validation under ownership locks. An SSD range can
be replaced only by its matching HDD publication proof; an already-applied HDD
range in MBL4 must still have that independent MBT4 proof. An open registered
HDD suffix can match a sealed publication of the same physical identity and
start within its registered bounds. Retained SSD sources are checked even if
HDD publication supersedes them. Every protected map is bound to its inventory
by the unique D entry's length/CRC and the exact B entries' lengths/CRCs.
Namespace changes during dependency validation cause retry rather than treating
concurrent publication or GC as stable corruption.

Once all closed ranges are on HDD, the finalizer durably publishes COMPLETE,
removes the local snapshot and syncs the SSD directory before satisfying
Persist/checkpoint. HDD publication followed by local failure retains the
relevant files and reports the first error; recovery reconciles the two
independently numbered generations using exact range/source identity.

On process-crash recovery, the registered open tail's physical length determines
candidate visible bytes, bounded by its registered maximum and durable minimum.
File length alone never establishes durability or validates a CRC. Recovery
performs real synchronization/sealing before promoting these bytes. It then
durably closes every abandoned logical writer at its exact recovered length,
including already-frozen, already-migrated and empty blobs. A registered empty
physical tail is removed from the logical map rather than published as an
ambiguous zero-length HDD range. Closed
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

V4 metadata telemetry adds local/remote commit attempts, successes, skips,
elapsed time and lock wait. Reasons are register, freeze, close, spill,
migration, finalize, recovery and gc; each has an attempt count plus elapsed,
lock-wait, metadata-file Fsync and metadata-directory-sync microseconds.
No-op attempts include their real lock wait and elapsed time. Failed attempts
are counted. Direct suffix sealing is a remote freeze; worker publication is
a remote migration. Finalize includes the independent HDD COMPLETE publication
and SSD snapshot removal phases. Payload Fsync/readback remains in extent and
Register/Freeze phase counters, not metadata-file Fsync.

Full Register/Freeze phase and migration-publication times are also exposed.
The latter starts after extent sealing and includes HDD publication and local
reconciliation. SSD metadata byte gauges measure logical file lengths of
canonical and temporary MBL4 files, not allocated blocks or payload quota.
Normal commits maintain a per-file ledger from successful writes/renames;
they do not enumerate the SSD namespace or stat unrelated blobs. Recovery scans
once. A failed partial write uses only that transaction's SSD file stats to
reconcile the gauge; if stat also fails, the last observed value remains with
the sticky operation error until recovery. Peaks are observed retained lengths.

GC commit fields measure the durable HDD tombstone snapshot; subsequent
payload/descriptor deletion remains part of existing GC operation totals.
It does not represent an SSD snapshot commit. Finalize separately measures its
local snapshot cleanup because completion barriers wait for that phase.
