# Native checkpoint driver contract (v1)

The Linux-only executable is `mb_native`. Build it twice: current source for
B/D/S, pinned upstream `abeebd9630f11bd08c28b7bd43c7bdfc62050654` for U.
The CMake target uses Release, portable CPU instructions and no RTTI. No gflags
or JSON input parser is required. `build.py --out=/absolute/external/build`
builds both adapters, writes `build.json`, and defaults to 48 compiler jobs.
It retries only recognized OOM failures at 32/16/8 jobs without cleaning.
For a remote source export without `.git`, invoke CMake directly and preserve
the source export identity and both compile_commands.json/CMakeCache.txt files.

## CLI

Every argument is `--name=value`; unknown flags, malformed numbers and booleans
fail. Required: `--index`, `--state`; B/D/S also require `--data`; D/S require
`--backup`; S requires `--staging`. Paths must be absolute and disjoint.
The state file must live outside database directories and survive index loss.
The controller owns directories, isolation, cgroups, affinity, overall timeouts,
raw stdout retention, OPTIONS capture and device telemetry.

| Flag | Default / supported values |
|---|---|
| variant | D; U/B/D/S |
| action | run; prepare/run/verify/restore/append-reopen/crashwrite |
| workload | insert; insert/overwrite/randomread/hotspot/scan/mixed/sync/gc/sst |
| keys / ops | 1024 / 1024 |
| value-size / threads | 1024 / 1; value >=32 bytes |
| warmup-seconds / measure-seconds | 0 / 0; integer seconds; zero uses operation quota |
| sync / flush / fresh / gc | false / true / false / true |
| staging-capacity | 268435456 logical bytes (S only) |
| sst-mode / sst-budget | disabled / 0; observe/adaptive require positive budget |
| disable-auto-compactions | false; SST-only preparation and read windows use true |
| cache-bytes | 268435456; SST mechanism runs can use 65536 |
| flush-every | 0; nonzero per-thread completed operations between explicit flushes |
| seed | 20261017 |
| direct-write | false; true requires current binary, disables concurrent memtable writes |
| checkpoint-interval-ms / mirror-interval-ms | 1000 / 1000; historical protocol defaults, both positive |
| gc-wait-seconds | 0; maximum 60, periodic stats after work |
| crash-stage | empty; write/publish/migration gates, only D/S crashwrite |
| verify-acks | empty; optional text file of `key version` lines for crash verification |
| verify-samples | 0; zero preserves full verification; positive selects min(requested, keys) unique keys |

`prepare` creates a new database and writes each key once (version 1),
forcibly ignoring warmup/measure duration and forcing ops=keys. Every state
entry must be version 1 at completion, and a prepared event reports key count. `run`
opens an existing database unless `fresh=true`. Insert traverses all keys once
when untimed; its operation count is keys, not ops. Other untimed workloads
split the total ops quota across workers. Timed workloads run until their common
deadline. Each worker owns a disjoint key range for mutations. Warmup uses the
same semantics and updates the expected state; it has its own sample event.

By default (`verify-samples=0`), `verify` fully checks presence/deletion, key,
exact version, checksum and total key count. `restore` calls native Metabypass
Restore, then Open and verification using the selected verification mode.
`append-reopen` updates key 0 synchronously, closes, opens, verifies and updates
state. Controller must first isolate original index/staging and supply the new
empty index/staging paths when requesting restore. Data/backup stay on HDD.


A positive `verify-samples` applies identically to `verify`, post-Restore
verification and `append-reopen`. The sample always includes key 0 (including
after append), the last key, each worker partition's first/last key, and the
first live and deleted key of each partition when those categories exist.
Category selection scans only the in-memory sidecar. An independent xorshift64
PRNG, seeded by `seed XOR 0xd1b54a32d192ed03` with that constant as the zero-seed
fallback, fills the unique sample; emitted keys are sorted. Reject a budget
smaller than the mandatory set. Every selected Get retains presence/deletion,
length, key, exact version and checksum assertions. Sample mode performs no
full-database Get loop or iterator/key-count scan, even when a small population
fits entirely in the sample. Passing a sample does not establish correctness of
unselected keys or actual database key count.

Sampling and `verify-acks` are incompatible and fail before running the action.
Omitting `verify-samples`, or setting it to zero, preserves the historical full
verification, ACK and resume semantics and emits no sample-verification event.
The controller forwards this optional flag only when its spec contains it;
legacy commands remain unchanged.

`crashwrite` repeatedly puts synchronously in each worker's range; use
`measure-seconds=30` for a kill window. Each successful Put emits and flushes an
ACK event. Controller durably records ACKs outside the measured database. It
must stop the driver with SIGKILL and then use `verify-acks` during recovery.
ACK validation checks the entire prepared dataset and requires each acknowledged
version or a later valid version, allowing successful but unacknowledged writes.
It records observed versions back into state. This action performs only Puts;
ACK verification is not a generic crash protocol for mixed deletes.

For deterministic crash boundaries, pass `crash-stage=write|publish|migration`.
The experiment-only filesystem wrapper arms after Open. Write gates immediately
after the first successful ACK; publish gates immediately before the physical
`backup/LATEST` rename; migration gates before creation of the first HDD
`data/segment-*` extent. It emits `ready` with stage/path and blocks reading
stdin. The controller must retain an open stdin pipe, persist all ACKs and the
ready event, then SIGKILL. A `c` byte releases the gate; EOF fails the process.
Migration requires S, and a 2 MiB staging capacity plus a >2 MiB flush output
ensures the gate represents prefix migration rather than a no-prefix spill.
Use `flush-every=4096,value-size=1024` or larger while retaining the original
prepared state. These gates describe precise file-operation boundaries; they
do not certify every internal state transition or physical power loss.


GC runs exactly three generations, each with ops mutations followed by Flush,
CompactRange and SyncBackup; callers set ops=keys and measure-seconds=0 to replace every key. Nonzero
measure-seconds applies separately to each generation and is not used by the
formal protocol. It emits
three foreground samples. `gc-wait-seconds=60` observes post-work convergence.
SST workload moves the 80% hot region from the first to last quarter halfway
through a timed window. Plain hotspot keeps 80% traffic in the first quarter.
Scan uses 100 rows at most per iterator seek and reports actual row count.
Mixed uses 90% Get / 5% Put / 5% Delete, selected independently per operation.

## State and correctness

The sidecar is little endian: 8 ASCII bytes `MBNPST01`, uint64 key count, uint64
value size, then key-count uint64 states. Zero means never written; high bit set
means deleted; low 63 bits retain a monotonic key version across deletions.
Keys are 16-character zero-padded hexadecimal. Values contain little-endian key
and version, deterministic payload, and an FNV-1a checksum of all preceding
value bytes. State is atomically replaced and file/directory fsynced outside
lifecycle timing. Do not reuse a state file with a different dataset.

## Output and measurements

Each machine record is one stdout line beginning `MB_NATIVE_JSON `, followed by
JSON with `protocol="metabypass-native-perf-v1"` and `event`:

- `options`: variant and effective_options (fixed engine settings and overrides).
  Preserve actual native OPTIONS files as well.
- `phase`: name and elapsed_us; open, restore, first_verified_read, verify,
  flush, sync_backup, close, lifecycle, generation_flush/generation_compact.
- `sample`: name=warmup/foreground, ops, elapsed_us, ops_per_sec, p50_us,
  p95_us, p99_us, hist_us and per-Get/Put/Delete histograms, scanned_rows.
- `stats`: boundary=snapshot/foreground_begin/foreground_end/warmup_begin/warmup_end,
  cumulative counters for this process; native flushes, compactions,
  flush_data_bytes, compaction_output_bytes and wal_bytes, native_blob_deletes
  (B-only successful physical deletion count, atomic and cumulative), plus D/S fields
  named exactly as MetaBypassStats members (including checkpoint_captures,
  checkpoint_capture_micros, checkpoint_flush_micros, checkpoint_persist_micros,
  checkpoint_publish_micros) and `sst_`-prefixed tiering members.
- `resources`: process lifetime user/system CPU microseconds and peak_rss_kib.
- `ready`: stage and physical path at the armed crash boundary.
- `ack`: numeric key and version, only after a successful synchronous Put.
- `verification`: emitted exactly once by each successful sampled verify,
  restore or append-reopen action. The transport `protocol` remains
  `metabypass-native-perf-v1`; `verification_protocol` is
  `metabypass-key-sample-v1`, `mode` is `sample`, `population` is the sidecar
  key count, `requested` is the positive budget, `actual` is
  `min(requested, population)`, and `derived_seed` is the seed above. `keys`
  contains exactly `actual` sorted unique in-range numeric keys.
  `live_checked`, `deleted_checked` and `unwritten_checked` count selected
  categories and sum to `actual`; `full_keycount_checked` is false.
  The controller requires policy identity, count/type consistency, a sorted
  unique in-range list containing mandatory partition boundaries, and matching
  `done.verified_keys`. Missing, duplicate or mismatched events fail validation;
  full/non-verifying actions reject this event. Category representatives are
  selected by the driver using its sidecar, not reconstructed from DB scans.
- `done`: status=pass, variant/action/workload and verified_keys. In sampled
  verification this is the actual number checked, not the full population;
  historical full-mode counting is unchanged.
- `error`: status=failed and message; exit status 1. Successful exit is 0.

Multiple stats events are snapshots, never additive. The last stats snapshot is
emitted before Close, so WAL/mirror/checkpoint/staging/GC counters exclude Close
cleanup bytes. Post-Close output contains phase/resources only; lifecycle and
process CPU/RSS include Close. U/B's explicit sync_backup phase is a no-op;
their synchronous write promise is WAL durability. D/S adds durable HDD backup.
Do not label the two promises equal. Default mutable lifecycle separately times
foreground, Flush, SyncBackup and Close, excludes Open and state serialization;
warmup and optional GC wait are included in lifecycle when requested and must
be reported separately. Read-only actions do not explicitly Flush/SyncBackup;
Metabypass Close still applies its native close barrier.

Histograms accumulate individual operations over all workers. Entries are
`[inclusive_upper_microseconds,count]`; buckets are exact below 64 us and have
at most about 3.125% width above that. Percentiles are upper bucket bounds.
Merge histogram counts across generations; never average window percentiles.
Get checksum validation and Put payload generation are outside operation
latency but inside foreground throughput. Scan latency includes per-row
validation. CPU and throughput therefore include common driver work and are
not engine-only CPU cost. Sample start includes thread startup; warmup uses a
separate worker set. Restore's first verified read includes validation.

B routes native `.blob` paths into its HDD directory, merges directory listing
for recovery/GC, and fsyncs both directories at index directory barriers.
Native file deletion is forwarded, without Metabypass retention. It is an
experimental filesystem adapter, and its recovery/deletion behavior must pass
smoke gates before B participates in comparisons.

The newer DirectWrite comparison profile explicitly selects a 5000 ms checkpoint
interval and DirectWrite=true for its recommended arm; these driver defaults
remain 1000 ms/false to reproduce the historical controller. Mirror interval
remains independent. DirectWrite publication uses a native flush and a WAL-free
inventory, and supports fault recovery through Restore of a published point;
active Blob WAL local crash reopen is outside that contract.
Optional post-work GC observation emits a background_tail phase and remains
inside lifecycle timing. The checkpoint timing counters overlap and must not
be summed as disjoint phases.
