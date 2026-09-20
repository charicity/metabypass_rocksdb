# WAL/SST scheduling A/B experiment

Research variants of commit `bbd60f2ca`, the notification-optimized Metabypass.
These historical experiments did not change production sources or the public API. Generated candidate
sources and binaries live under `/tmp/mb-scheduling-ab`; `build.py` reads the pinned commit by default and provides the
reproducible source transformation, with assertions guarding replacement sites.
These are experimental implementations, not a supported new backup mode.
A follow-up `priority_outside` moves partitioning of the already detached,
private window outside the queue mutex; all other priority semantics remain
identical.

## Variants

- `current`: unchanged production utility.
- `split`: separate WAL and non-WAL primary-operation mutexes. File creation,
  append, truncate, close and deletion select the same lane. Rename takes both
  mutexes. The accepted-event queue remains FIFO. Multiple capacity waiters
  require a waiter count and capacity broadcasts instead of the production
  single-waiter charge predicate.
- `reserve`: `split`, plus non-WAL reservations leave one eighth of total
  queue capacity available for WAL. This restricts total charged occupancy
  when admitting metadata, not a separate unbounded buffer. An individual
  metadata event larger than seven eighths may use the original total limit,
  preserving the original maximum single-operation size. This exception
  means the WAL headroom is not an absolute guarantee.
- `priority`: `reserve`, plus WAL-first stable partition of append runs in a
  finite captured event window. Non-append events are barriers. Per-file
  append order is preserved. All window events must finish before advancing
  `applied_` or capturing a candidate. New arrivals wait for the next window;
  continuous WAL arrivals therefore cannot starve that window's SST events.
  Partitioning currently runs under the queue mutex and adds scheduling work.

All variants retain charged pending/in-flight memory, native file mirroring,
blob dependency synchronization, incremental content validation, immutable
candidate capture, atomic publication, previous-point retention and offline
full validation. Splitting locks permits two lanes' primary file operations
to overlap; it does not create multiple mirror I/O threads. A blocked candidate
copy still prevents the sole mirror from draining. The priority variant is
intentionally conservative and cannot bypass a control-operation barrier.

## Workloads and measurements

The driver uses one writer, 1 KiB values, no compression, WAL enabled,
`sync=false`, separated Blob Direct Write and a 1,000 ms interval.

- `default`: decimal keys, default RocksDB memtable/compaction settings,
  64 MiB queue, 256 KiB trigger.
- `mixed`: 128-byte keys, 256 KiB write buffer and target SST size, 1 MiB
  level base, L0 compaction trigger 2, four background jobs; 64 MiB queue.
- `pressure`: the mixed workload with a 2 MiB queue, 256 KiB trigger.

`pressure` names the small-queue configuration; the measured counters determine
whether capacity pressure actually occurred. Event listeners report flush and
compaction counts completed during the foreground loop, demonstrating whether
the workload exercised SST interference. These workloads are not directly
comparable with the earlier version-comparison numbers.

Five rotated trials per workload/variant follow one 10k warmup for each pair.
Each measured trial performs 100k puts. All Put calls are timed with the same
steady clock in all variants; percentiles are nearest-rank-like array indices,
not percentile estimates inferred from aggregate throughput. Foreground wall
clock includes key generation and latency collection, but excludes Open,
SyncBackup and Close. Total is the sum of those three measured phases.
Thread user/system CPU and context switches cover the foreground loop.
`backpressure_us` includes all producers, not only the foreground writer.
Final point lag is not a continuous-lag distribution or fixed RPO guarantee.

Every successful write closes, deletes the entire main index, restores with
`current/driver`, checks every key/value, writes an additional key, closes,
reopens and checks it. Additional SIGKILL trials kill the writer after an
explicitly completed SyncBackup without Close, delete the index, and perform
the same restore checks. Publication-stage interruptions, WriteBatch,
overwrite/delete and corruption checks are in the existing unit suite.

Each write/restore/test child has a 60-second timeout. Run on isolated private
directories; only those generated directories are removed. Binaries are built
before benchmarks; do not overlap compiler/test jobs with timings. The ext4
paths share storage, and host cache/hypervisor interference is not controlled.
These are process-crash experiments, not real power-failure qualification.

## Reproduction

The build uses an existing compatible release archive and its generated
`make_config.mk` flags. All four Metabypass utility translation units override
the archive's copies in each executable. The source public header and core
build must agree. Prepare that archive separately with
`AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j<N> static_lib` if unavailable.

```sh
python3 docs/components/metabypass/experiments/scheduling-ab/build.py
python3 docs/components/metabypass/experiments/scheduling-ab/run.py
python3 docs/components/metabypass/experiments/scheduling-ab/refine.py
python3 docs/components/metabypass/experiments/scheduling-ab/run.py \
  --variants=current,split,priority_outside \
  --output=/tmp/mb-scheduling-followup-results.json
python3 docs/components/metabypass/experiments/scheduling-ab/profile.py
python3 docs/components/metabypass/experiments/scheduling-ab/run.py \
  --executable=profile-driver --trials=3 \
  --output=/tmp/mb-scheduling-profile-results.json
python3 docs/components/metabypass/experiments/scheduling-ab/checks.py
python3 docs/components/metabypass/experiments/scheduling-ab/checks.py \
  --variants=priority_outside --output=outside-checks.json
for variant in split reserve priority priority_outside; do
  timeout 60 /tmp/mb-scheduling-ab/$variant/check-test \
    '--gtest_filter=MetaBypassTest.AB*' --gtest_repeat=100
done
```

Profiling times only the foreground thread's primary-operation lock acquisition,
queue lock acquisition in Reserve/Finish/Error, and capacity condition wait.
It includes acquisition execution cost as well as contention. Timers perturb
execution; use the uninstrumented series for performance claims. Profiling
binaries reuse the same non-backup utility objects as the main trials.

`checks.py` uses the checkout's existing compatible debug shared RocksDB and
test libraries. It compiles all candidate utility objects and the test source
into each executable so candidate class layouts agree. It runs all existing
26 cases once per variant and three new isolation/error tests ten times each
for split variants. These tests use sync points, not timed sleeps. No production
source, library, or existing test executable is overwritten.

## Saved evidence

- `results.json`: first uninstrumented four-variant series.
- `followup-results.json`: fresh current/split controls and outside-lock priority.
- `profile-results.json`: separate instrumented series; do not use its elapsed
  times as the headline throughput comparison.
- `checks.json`, `outside-checks.json`, `stress.json`: exact test commands/results.
- `environment.json`: toolchain, filesystem, source and binary fingerprints.

The focused interpretation is in [the experiment report](../../scheduling-ab.md).

## Formal dual-channel comparison

`channels.py` builds the pinned `current` and `split` controls, then the live
`channels` production sources, and runs the same five-trial comparison and
recovery checks. It accepts `--library`, `--out`, `--output` and `--trials`.
The saved formal results and validation ledger are `../channel-results.json`
and `../channel-checks.json`; see [the formal report](../../channel-validation.md).
Historical `checks.py` also uses the pinned 26-test source, so later production
channel tests are not accidentally injected into old single-queue variants.

`no_backup.py` adds a separated-storage-only baseline to the saved channel
controls and runs a fresh rotated four-version comparison. Baseline primary
verification is recorded separately from backup recovery; see
[the baseline comparison](../../channel-baseline-comparison.md).
