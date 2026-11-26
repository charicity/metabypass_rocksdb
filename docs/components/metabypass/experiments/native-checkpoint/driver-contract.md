# Native checkpoint driver contract (v2)

The Linux-only executable is `mb_native`. Build it against the current fork for
D and pinned upstream `abeebd9630f11bd08c28b7bd43c7bdfc62050654` for U.
The CMake target uses Release, portable CPU instructions and no RTTI. No gflags
or JSON input parser is required. `build.py --out=/absolute/external/build`
builds both adapters, writes `build.json`, and defaults to 48 compiler jobs.
It retries only recognized OOM failures at 32/16/8 jobs without cleaning.
For a remote source export without `.git`, invoke CMake directly and preserve
the source export identity and both compile_commands.json/CMakeCache.txt files.

## CLI

Every argument is `--name=value`; unknown flags, malformed numbers and booleans
fail. Both groups require `--index` and `--state`. D also requires
`--backup`. Database paths must be absolute, normalized and disjoint.
The state file must live outside database directories and survive index loss.
The controller owns directories, isolation, cgroups, affinity, overall timeouts,
raw stdout retention, OPTIONS capture and device telemetry.

| Flag | Default / supported values |
|---|---|
| variant | D, with U and D supported |
| action | run, with prepare/run/verify/restore/append-reopen/crashwrite supported |
| workload | insert, with insert/overwrite/randomread/hotspot/scan/mixed/sync/sst supported |
| keys / ops | 1024 / 1024 |
| value-size / threads | 1024 / 1. Minimum value size is 32 bytes. |
| batch-size | 1. Larger batches require untimed single-thread insert prepare/run. |
| warmup-seconds / measure-seconds | 0 / 0 in whole seconds. Zero uses the operation quota. |
| sync / flush / fresh | false / true / false |
| sst-mode / sst-budget | disabled / 0. Observe and adaptive require D and a positive budget. |
| disable-auto-compactions | false. SST-only preparation and read windows use true. |
| cache-bytes | 268435456. The SST experiment uses 65536. |
| flush-every | 0. Nonzero sets the per-thread operation count between explicit flushes. |
| seed | 20261017 |
| checkpoint-interval-ms / mirror-interval-ms | 1000 / 1000. Both must be positive. |
| crash-stage | empty. D crashwrite requires write or publish. |
| verify-acks | empty. Optional text file of `key version` lines for crash verification |
| verify-samples | 0 for full verification. Positive selects min(requested, keys) unique keys. |

The `MB_UPSTREAM` adapter opens U through ordinary RocksDB. The current
adapter opens D through MetaBypass. Both store inline values. D uses a 56 MiB
WAL queue, an 8 MiB metadata queue and a 256 KiB mirror batch trigger.
The output records effective options and sets D-only fields to null for U.
Archive native OPTIONS and LOG files to check the library's configuration.

`prepare` creates a new database and writes each key once (version 1),
forcibly ignoring warmup/measure duration and forcing ops=keys. Every state
entry must be version 1 at completion, and a prepared event reports key count. `run`
opens an existing database unless `fresh=true`. Insert traverses all keys once
when untimed; its operation count is keys, not ops. Other untimed workloads
split the total ops quota across workers. Timed workloads run until their common
deadline. Each worker owns a disjoint key range for mutations. Warmup uses the
same semantics and updates the expected state; it has its own sample event.

`batch-size=1` retains individual Put calls. Larger sizes group consecutive
insert keys into one WriteBatch, including a partial final batch, and split
batches at `flush-every` boundaries. Batched mode rejects timed windows,
warmup, other workloads/actions and multiple workers. The maximum size is
65536 records and 64 MiB of key/value payload per requested batch. Each
successful Write commits the sidecar versions for all its keys. The sync
flag applies to each WriteBatch. Verification uses the usual sidecar protocol;
invoke verifying actions with the default batch size.

Options, samples and done events record `batch_size`; samples also record
`write_calls`. Throughput and histogram counts remain logical record counts.
For batches, each record is assigned its batch's complete Write latency,
without division by record count, and `latency_scope="write_batch"` makes
this explicit. Individual operations use `latency_scope="operation"`.
Payload and WriteBatch construction remain outside engine latency but inside
foreground throughput and lifecycle timing.

By default (`verify-samples=0`), `verify` fully checks presence/deletion, key,
exact version, checksum and total key count. `restore` calls native Metabypass
Restore, then Open and verification using the selected verification mode.
`append-reopen` updates key 0 synchronously, closes, opens, verifies and updates
state. The controller first isolates the original index, then supplies a
new empty index path. The backup stays on HDD.

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
Full verification emits no sample-verification event. The controller forwards
the sample flag when its workload specification contains it.

`crashwrite` repeatedly puts synchronously in each worker's range; use
`measure-seconds=30` for a kill window. Each successful Put emits and flushes an
ACK event. Controller durably records ACKs outside the measured database. It
must stop the driver with SIGKILL and then use `verify-acks` during recovery.
ACK validation checks the entire prepared dataset and requires each acknowledged
version or a later valid version, allowing successful but unacknowledged writes.
It records observed versions back into state. This action performs only Puts;
ACK verification is not a generic crash protocol for mixed deletes.

Pass `crash-stage=write|publish` to set the crash boundary. The experiment
filesystem wrapper arms after Open. The write gate waits after the first
successful ACK. The publish gate waits before the physical `backup/LATEST`
rename. Both emit a `ready` event with stage and path and read stdin.
The controller keeps stdin open, persists the ACKs and ready event, then sends
SIGKILL to the authenticated process group. A `c` byte releases the gate.
EOF fails the driver.

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
JSON with `protocol="metabypass-native-perf-v2"` and `event`:

- `options`: variant and effective_options (fixed engine settings and overrides).
  Preserve actual native OPTIONS files as well.
- `phase`: name and elapsed_us; open, restore, first_verified_read, verify,
  flush, sync_backup, close and lifecycle.
- `sample`: name=warmup/foreground, ops, elapsed_us, ops_per_sec, p50_us,
  p95_us, p99_us, hist_us and per-Get/Put/Delete histograms, scanned_rows.
- `stats`: boundary=snapshot/foreground_begin/foreground_end/warmup_begin/warmup_end,
  cumulative counters for this process; native flushes, compactions,
  flush_data_bytes, compaction_output_bytes and wal_bytes. D also reports WAL,
  metadata queue, checkpoint, retired-point collection and generic I/O
  counters named after MetaBypassStats members, plus `sst_`-prefixed tiering
  members. The generic counters include bytes by index/backup and
  caller/bulk/barrier, failed writes, and HDD file-operation calls and times.
- `resources`: process lifetime user/system CPU microseconds and peak_rss_kib.
- `ready`: stage and physical path at the armed crash boundary.
- `ack`: numeric key and version, only after a successful synchronous Put.
- `verification`: emitted exactly once by each successful sampled verify,
  restore or append-reopen action. The transport `protocol` remains
  `metabypass-native-perf-v2`; `verification_protocol` is
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
  verification this is the actual number checked.
- `error`: status=failed and message; exit status 1. Successful exit is 0.

Multiple stats events are cumulative snapshots. The final successful snapshot
follows Close, so it includes Close work. U's explicit SyncBackup phase is a
no-op. Synchronous writes on U persist the WAL. On D they also wait for backup
coverage. The driver times foreground, Flush, SyncBackup and Close separately.
Lifecycle begins after Open and ends after Close. Warmup contributes to
lifecycle and has its own event. Read-only runs skip explicit Flush and
SyncBackup. Metabypass Close still applies its native close barrier.

Histograms accumulate individual operations over all workers. Entries are
`[inclusive_upper_microseconds,count]`; buckets are exact below 64 us and have
at most about 3.125% width above that. Percentiles are upper bucket bounds.
Get checksum validation and Put payload generation are outside operation
latency but inside foreground throughput. Scan latency includes per-row
validation. CPU and throughput therefore include common driver work and are
not engine-only CPU cost. Sample start includes thread startup; warmup uses a
separate worker set. Restore's first verified read includes validation.

Checkpoint phase counters are cumulative and can overlap.
