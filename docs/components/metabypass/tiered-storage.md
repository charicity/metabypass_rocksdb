# Bounded blob staging and HDD spill

Ordinary Integrated BlobDB creates blob files during native memtable flush and
compaction. Foreground Put/Write stores the full value in WAL and memtable;
staging admission is performed by blob file Append, including background flush
and compaction writers.

Enable staging with a disjoint `staging_dir` and positive `staging_capacity`.
The capacity is a hard bound on logical SSD blob bytes plus outstanding local
append reservations. Reservation and capacity accounting share a mutex, so
multiple blob writers cannot each spend the same free bytes. File system
allocation overhead, WAL/index space, memory and HDD capacity are separate.

When the next append cannot fit, that file switches once to HDD. Its existing
SSD prefix becomes immutable. An independent worker copies and fsyncs that
prefix, persists a versioned descriptor and switches new reads to HDD. The
current file then sends subsequent appends through the worker's bounded queue.
It never returns to SSD. A value or batch larger than the staging cap succeeds
without requiring the entire record to fit locally. No DB write-side staging
reservation or forced memtable flush is used to obtain capacity.

The worker schedules and buffers at most 4 MiB per chunk, with a fixed 16 MiB
remote queue. It aggregates writes into fixed 32 MiB physical extents across
migration yields; no new configuration is required. A full extent is fsynced,
verified with a whole-extent CRC, and closed. Every descriptor commit first
seals the remaining tail, syncs the data directory if new extents were added,
then fsyncs the temporary descriptor, renames it, and syncs the directory before
publishing the durable route. Committed extents are never appended again.
File Sync/Close establishes
a durable descriptor boundary; crash recovery uses only committed descriptor
prefixes and ignores uncommitted tail extents. Flush must make its blob payload
durable before native RocksDB installs an SST referencing it. Worker operations
do not wait for themselves, so background flush and compaction do not require a
foreground thread to free staging capacity.

A physical SSD read pins its opened handle. After the route switches, new reads
use HDD immediately; an older pinned read may finish using SSD. That pin delays
SSD unlink and quota reclamation but does not block later HDD appends. Remote
reads hold an immutable mapping version until completion. Whole-file deletion
waits for both read lifetimes.

`SyncBackup` and synchronous writes publish native recovery points on HDD.
For a WAL-only point no blob migration is needed: its full-value WAL is sufficient.
For points containing blob references, required closed blob files and descriptors
are persisted before publication. Restore can lose both index and staging while
retaining HDD data/backup. It verifies the published mapping and uses native
RocksDB recovery; it does not scan records, seal blobs or repair MANIFESTs.

Formats are `MBN2` (database/backup), `MBI2` (inventory) and `MBT2` (mapping).
Older formats are rejected before modifying the source directories. There is no
compatibility migration path. MBT2 already supports variable extent lengths;
existing 4 MiB extents and new 32 MiB extents can coexist without a format change.

Observe `peak_staging_bytes`, `staging_spill_files`, `staging_spill_bytes`,
`tier_queued_bytes`, `peak_tier_queued_bytes`, `hdd_extents`, migration bytes and
sync latency together. `mirrored_wal_bytes` isolates WAL append traffic from
`mirrored_bytes`, which includes SST and metadata append traffic as well.
Full-value WAL increases mirror traffic; HDD service time
can increase flush latency even while staging stays below its hard cap. Current
behavior is summarized in the [component entry point](index.md).

Staging persistence counters are cumulative since open and default to zero when
staging is disabled. `tier_task_wait_micros` measures time from queue admission
to task completion or interruption; `staging_backpressure_micros` continues to
measure admission wait only. `tier_extent_fsyncs` and
`tier_extent_fsync_micros` count physical extent Fsync attempts and time, excluding
CRC verification and Close. `tier_commit_dir_syncs` and
`tier_commit_dir_sync_micros` count Commit dependency/publication directory sync
attempts and time, excluding eviction and GC. Counts include failed attempts.
The native-checkpoint driver and db_bench emit all five fields. An uninterrupted
256 MiB migration uses eight data Fsync calls and two Commit directory syncs;
explicit intermediate barriers can create shorter extents.

### Redundant persistence and checkpoint reads

Persist still validates local closure and exact local length before omitting
already durable blob entries from the work queue. Every caller waits for its
own requested lengths; a smaller satisfied caller cannot remove another
caller's larger request. Commit omits I/O only for identical length, sealed
state, and extent names/lengths, with no open extent writer. Sealing and changed
mappings retain payload Fsync/readback/Close, dependency directory sync,
descriptor Fsync/rename, and publication directory sync ordering.

Checkpoint CRC computation pins a fixed durable version for the entire call,
including gaps between extents. It reads the requested prefix sequentially,
opening each needed physical extent once. The pin excludes GC until all handles
are closed, including error paths. This does not replace extent seal readback
or change the backup cache's length-based verification trigger.

`tier_persist_skips` counts satisfied blob entries, `tier_commit_skips` counts
identical commits, and `tier_append_tasks` counts admitted Append tasks.
`tier_append_wait_micros` and `tier_switch_wait_micros` are subsets of
`tier_task_wait_micros`, with the same enqueue-to-completion/interruption scope.
`tier_checkpoint_digest_opens` counts physical reader open attempts and
`tier_checkpoint_digest_micros` covers entire durable-prefix checksum calls,
including failures and pin acquisition. These counters are zero with staging
disabled. Version-comparison adapters emit null for unavailable fields.
Cumulative waits overlap background I/O and do not represent additive savings.
