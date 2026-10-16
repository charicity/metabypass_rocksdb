# Whole-file Blob GC

`MetaBypassOptions::enable_blob_file_gc` defaults to true. Set it before Open;
false retains remaining blobs but does not undo completed deletion. Both modes
use the same native MANIFEST dependency interpretation, so a reclaimed database
can reopen or restore with GC disabled. Native value relocation remains off;
`CompactRange` with `BlobGarbageCollectionPolicy::kForce` is rejected. A partially
live blob can remain indefinitely. There is no fixed reclamation deadline.

## Qualification and publication

RocksDB supplies obsolete-file requests based on its registered blob metadata
and full scans. Its delete request becomes a deduplicated metadata
channel retirement event. The mirror records the event sequence without
unlinking any blob. A later fully validated, durably published recovery point
must cover that sequence. Both LATEST roots and any retired point whose cleanup
has not completed must release the file. Existing inventories retain their exact
original roots; opening a newer binary does not trim old points in place.

MANIFEST inspection applies additions, cumulative garbage and SST oldest-blob
links in replay order using the native Version retention rules. A legacy SST
may have no registered oldest blob; inspection tolerates that native case,
while actual WAL and SST references still require valid payloads. Before any
recovery mutation, all required references and blob records are validated.
Recovery moves required historical blob additions before the original MANIFEST
records, preserving their totals/checksums and all other record fields, and
adds verified registrations for blobs previously known only through indexes.
This repairs legacy SST links without rewriting SSTs, WALs, or moving values.
Repeated preparation is idempotent.

Metabypass's internal native Open also carries verified sealed-blob additions
into the cumulative recovery SST edit: native recovery may otherwise discard
WAL-only blob metadata before replay creates the SST that references it. Only
Metabypass forces `avoid_flush_during_recovery=false` and
`reuse_manifest_on_open=false`, so recovery publishes that metadata with SSTs
in a fresh MANIFEST, without duplicate historical additions. Ordinary RocksDB
Open callers do not supply this internal input and retain their behavior.

All root handoffs, candidate lifetime and delete eligibility share the backup
mutex. Candidate lifetime covers Capture before its pointer is installed and
validation after the pointer moves into a local variable. Claiming a batch
sets a gate that excludes new Capture until physical deletion and directory
sync complete. Neither backup nor tier locks are held across physical GC I/O.
Mirroring continues while the gate is held.

Online reads retain the native RocksDB lifetime protection: active/sealed
memtables protect direct-write blobs, and all live Versions protect SST blob
metadata. Recovery establishes those registrations before exposing the DB.
Iterators, including ones that have not sought yet, keep their native owners
alive; no global read lock delays unrelated blob reclamation. Existing tier
read pins also protect physical local and remote I/O. Iterators must still be
destroyed before the DB under the normal RocksDB lifetime contract.

The existing GC worker processes at most 16 logical blobs per batch. Retired
point cleanup has priority and is not blocked by candidates or the blob gate:
publication can therefore wait for the retired slot without a circular wait.
At the configured interval, an unchanged write boundary can create a new point
only if the older retained point still pins a blob that the newest point has
released. A root that still needs the blob is never evicted just to enable GC.
Online GC does not force Flush or compaction.

## Physical deletion and interruption

Direct mode unlinks the qualified file and synchronizes its directory. Staging
mode submits retirement to the existing migration worker. Marking retiring
blocks new reads and migration requests; local and remote per-I/O pins must
drain. The worker inventories the descriptor, its temporary, every historical
or orphan segment for that exact blob number, and local staging. It removes the
published mapping and descriptor, synchronizes the data directory, then deletes
payload files and synchronizes both data and staging directories. A descriptor
sync failure stops before payload deletion. Missing paths are idempotent only
within a qualified retirement; validation still rejects missing dependencies.

Any other I/O failure records the existing sticky error and wakes waiters. The
capture gate is released even on failure; the error prevents further publication
or deletion. A tier request already claimed by its worker waits for physical I/O
to finish even if unrelated failure or cancellation arrives; an unclaimed request
can be cancelled without deleting its files. Tasks are volatile. After a crash or
restore, native full-scan
classification must qualify them again. Orphan segments and canonical
`.blob.tmp` descriptors are projected as logical blob numbers for that scan
and recovery's next-file-number calculation, but are never made readable as
valid descriptors. No persisted task crosses a restored allocation epoch.

Close first closes the DB, drains/joins backup workers, then stops the tier
worker. It releases the mutex while waiting for an already claimed GC batch;
unqualified requests may remain on disk. Quiet checkpoint advancement stops
on Close. Directory locks remain held until all these users are gone.

## Measurement and tests

`pending_blob_gc_files/bytes` describe applied pending retirements and known
file lengths (orphan lengths may be unknown until qualified inventory).
`reclaimed_blob_files/bytes` count only completed deletion plus directory sync.
Staging reclaimed bytes include the physical descriptor, historical segments
and local staging lengths. These are byte lengths, not allocated disk blocks.
`blob_gc_micros` is cumulative qualified reclamation time, including tier waits.
`db_bench --metabypass_blob_file_gc=false` disables the feature; node JSON samples
and the ordinary Metabypass summary expose all five counters.

Tests named `BlobGC*` in `metabypass_test` cover direct and staging reclamation,
SST tiering composition, retained partial files and iterators, native dependency
rules, Capture/validator lifetimes, the physical-delete/Capture gate, Close, local
and remote read pins, in-flight migration, descriptor/payload/directory-sync
failures, debris replay and file-number reuse. Memtable and recovered-SST lazy iterators, legacy missing/late additions,
recovery option overrides, and both quiet-point idle branches have explicit
coverage. POSIX tests kill
the writer at descriptor and payload deletion boundaries, lose SSD state, and
restore repeatedly from HDD. Existing staging migration, WAL-only recovery,
recovery-interruption and SST tiering tests remain part of the regression scope.
