# WAL Append coalescing

The mirror worker coalesces already queued WAL Append events, up to 64 events
and 64 KiB. It never waits to form a batch. Empty and oversized Append events,
metadata operations, and control operations remain individual operations.
The global sequence must be adjacent and the file name identical; intervening
create, close, truncate, rename or delete events therefore delimit file lifetimes.

Selection holds the queue mutex; concatenation and filesystem I/O do not.
All selected queue charges remain reserved until the physical operation returns.
The mirror reuses a 64 KiB scratch buffer. The existing drain-window boundary and
any existing candidate sequence split batches. A new candidate installed while
I/O is in flight captures accepted_, which cannot precede the already selected
batch. Capture still runs after the entire original drain, not after each batch.
MirrorReady, SyncBackup publication waiting, synchronization, verification,
file formats and public options are unchanged.

## Test hooks and failure semantics

Apply, ApplySequence and ApplyStatus run once per logical event, in global order.
For a batch these are logical preflight hooks: the next hook does not imply that
the previous event has already reached the physical file. WalAppendBatch is a
separate physical pre-write hook carrying {first_sequence, event_count, bytes}.
SyncPoint processing does not disable coalescing.

If logical preflight fails, only its accepted prefix is submitted to Append.
A failure writing that prefix takes precedence over the later injected logical
failure. Only a fully successful physical Append advances applied_ and mirrored
byte counters for its logical events. Partial physical writes are never retried
or acknowledged; the sticky-error path prevents publication, wakes waiters and
discards remaining events. Selected charges are released exactly once, including
unprocessed suffix events. Existing published recovery points stay immutable.

The unit tests exercise selection limits, content, control and file-lifetime
boundaries, logical first/middle/last faults, partial filesystem writes, error
precedence, retained in-flight charges, producer and SyncBackup waiters, and
Restore of the previous point after failure.
