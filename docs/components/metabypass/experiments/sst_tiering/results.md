# SST tiering validation record

This record separates correctness evidence from performance measurements. The
source checkout was based on `2d3acc00c15902f26789e27dc2c5e7e6fd1ca788`
with uncommitted Metabypass changes. All results below are from 2026-09-29.
The formal Release experiment completed five workloads, seven configurations
and three independent repeats (105 trials). A v3 strict smoke ran, and its
preserved stdout was subsequently re-evaluated under the corrected mechanism
coverage rule. All performance values here are from one machine with both
paths on the same ext4 device; they do not establish real HDD performance or
SSD-device capacity savings.

## Correctness and build checks

The strict build used `AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48` for the
relevant test binaries. The selected stress cases used binaries rebuilt with
`COERCE_CONTEXT_SWITCH=1` in the same strict mode. Reproduction build commands
are:

```sh
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48 sst_tiering_test metabypass_test
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 COERCE_CONTEXT_SWITCH=1 \
  make -j48 sst_tiering_test metabypass_test
```

Each test case had a 60 s timeout. The 81-case current
fixture run used:

```sh
build_tools/gtest-parallel --workers=8 --timeout_per_test=60 \
  --output_dir=/tmp/sst-strict-fixture-r1 \
  --dump_json_test_results=/tmp/sst-strict-fixture-r1.json \
  ./sst_tiering_test ./metabypass_test
```

It passed 80/81 cases. `MetaBypassTest.TieredCrashBoundariesAndCompleteFastStorageLoss`
timed out at 60 s; the exact-case retry with two workers passed in 24.45 s.
The first-round timeout remains recorded as a timeout, not rewritten as a
first-round pass. The selected five concurrency and storage cases were then
run with `COERCE_CONTEXT_SWITCH=1`, eight workers and 100 repeats per case:

```sh
build_tools/gtest-parallel --workers=8 --repeat=100 --timeout_per_test=60 \
  --output_dir=/tmp/sst-coerce-r1 \
  --dump_json_test_results=/tmp/sst-coerce-r1.json \
  --gtest_filter='SstStorageTest.ReadPinsDelayUnlinkButNotRouteSwitch:SstStorageTest.ExistingReaderSwitchesBothDirectionsAndSurvivesDelete:SstStorageTest.CopyAllowsPolicyAndForegroundProgressAndCancelsDeletedFile:MetaBypassTest.SstRandomizedConcurrentReadersCompactionAndBlobStaging:MetaBypassTest.SstForegroundFileMissesPromoteButScansDoNot' \
  ./sst_tiering_test ./metabypass_test
```

All 500 invocations passed without timeout. The 81-case fixture followed an
earlier 77-case implementation round with seven persistent failures, a 79-case
fix round, and an 81-case round in which the new foreground-read test wrongly
assumed the newest SST must be cold. In that earlier 81-case round, its failure
persisted through 8/2/1/1-worker retries; the fixture was changed to select
an actual cold, capacity-eligible SST. The current 81-case fixture results above
follow that correction. The earlier failures are retained in the audit table.

Six related filesystem/blob test binaries were run in strict mode with eight
workers and a 60 s per-case timeout:

```sh
build_tools/gtest-parallel --workers=8 --timeout_per_test=60 \
  --output_dir=/tmp/sst-fs-r1 \
  --dump_json_test_results=/tmp/sst-fs-r1.json \
  ./db_blob_direct_write_test ./env_basic_test ./env_test \
  ./io_posix_test ./fault_injection_fs_test ./error_handler_fs_test
```

Of 261 runnable cases, 258 passed. Three strict-Status failures persisted over
8/2/1/1-worker rounds: `EnvTest.WriteStringToFileCloseFailureDeletesFile`,
`TestAsyncRead.ReadAsync`, and `EnvTestMisc.StaticDestruction`. A clean
`git archive` checkout of the same HEAD was built with
`AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48 env_test`; the same three
exact cases failed with `SIGABRT` in all four retry rounds. These failures are
reproducible in the unmodified baseline and are not attributed to SST tiering.

`make check-sources` passed. `BUCK` was regenerated from `src.mk`; no BUCK
build was run. CMake configured a Debug build successfully in
`/tmp/metabypass-sst-cmake-verify` (the log ends with `Configuring done`,
`Generating done`); CMake compilation was not run. The exact CMake invocation
was not captured in the available log.

## Local smoke history

The local smoke commands used the same-host, same-device roots under `/tmp` and
no injected read delay. They check correctness and mechanism feasibility; they
are not HDD performance measurements. The v1 runner invocation can be
reproduced with:

```sh
python3 docs/components/metabypass/experiments/sst_tiering/run.py \
  --binary ./db_bench --profile smoke \
  --ssd-root /tmp/mb-sst-smoke-ssd \
  --hdd-root /tmp/mb-sst-smoke-hdd \
  --output /tmp/mb-sst-smoke.json
```

The v1 calibration produced eight SSTs totaling 186,171 logical bytes. With a
64 KiB block cache, the SST/cache ratio was 2.841, below the protocol's 4x
capacity-pressure gate. The runner stopped before trials with
`capacity_preflight_failed`; this was a fixture sizing failure, not a passed
mechanism experiment. The v2 run changed the smoke cache to 32 KiB and used
new `mb-sst-smoke-v2-*` paths. Its ratio was 5.681 and all 35 trials completed
their value verification. The v2 report nevertheless ended as
`coverage_incomplete`: adaptive hotspot/25% and scan/25% each had measured SST
samples (129 and 125) but zero migrations in the measured windows. Their
cumulative summaries had one promotion and eight demotions. The v2 output did
not separate fill from warmup migrations, so it cannot establish whether those
two trials had already settled during warmup. This is why v3 adds phase
snapshots and a conservative cold-live-byte lower bound; v2 remains a recorded
failure under its original protocol. No v1/v2 performance numbers are promoted
to the formal comparison.

The v3 strict smoke used fresh `mb-sst-smoke-v3-*` roots and output, the 32 KiB
cache, and the same 35 one-repeat smoke configurations. All 35 trial value
checks were valid and preflight passed (eight SSTs, SST/cache ratio 5.681).
Its original report ended `coverage_incomplete`: mixed/adaptive/75% sampled
110 measured SST reads and completed six measured migrations (one promotion,
five demotions), but a final compaction left the conservative cold-live lower
bound at zero. The original runner incorrectly required a cold file at the
final snapshot even when a migration completed during measurement. The
original JSON remains unchanged at `/tmp/mb-sst-smoke-v3.json`.

The corrected contract requires measured sampling and either a completed
measured migration or a warmup migration followed by positive cold-live
evidence at measurement end. This retains the rejection of fill-only migration
and unsampled measured phases. The [independent audit JSON](v3-smoke-audit.json)
reparsed all 35 raw stdout records; it changed exactly one mechanism verdict,
changed no validity verdict, and evaluates the preserved trial set as
`complete` (15/15 adaptive coverage). The original binary SHA-256 at start
and end matched, which the audit verifies before allowing `complete`. This
is a reinterpretation of the v3
run, not a fresh execution or a Release performance result. The switch
promotion proxy was observed in 3/3 trials; it is diagnostic and not an
acceptance gate. The audit can be reproduced with:

```sh
python3 docs/components/metabypass/experiments/sst_tiering/audit_existing.py \
  --input /tmp/mb-sst-smoke-v3.json \
  --output /tmp/mb-sst-smoke-v3-audit-recheck.json
```

## Audit summary

The following compact summary preserves the key outcomes if temporary raw
files are later removed. Hashes identify the original JSON evidence; the
individual logs and per-case output remain at the listed `/tmp` paths for now.

| Evidence | Recorded outcome | JSON SHA-256 |
|---|---|---|
| `/tmp/sst-strict-r{1..4}.json` | Initial 77 cases: round 1 69 pass/7 fail/one timeout; retries left seven persistent implementation failures. | See files |
| `/tmp/sst-strict-fixed-r{1,2}.json` | 79 cases: 78 pass/one 60 s timeout; exact retry passed. | See files |
| `/tmp/sst-strict-final-r{1..4}.json` | Earlier 81-case fixture: round 1 79 pass/one foreground-fixture fail/one timeout; timeout retry passed, foreground case persisted because it selected an SST that was hot. | See files |
| `/tmp/sst-strict-fixture-r1.json` | Current fixture: 80 pass, one 60 s timeout. | `6cfd4d84ef5907233ded887afe41e2a4f1d62e338aa385bfdf31bece3f74db7e` |
| `/tmp/sst-strict-fixture-r2.json` | Exact timeout retry: one pass in 24.45 s. | `b36da86a2ba1c3135f9b273e6d6ee18551322b6aabd80ec46bb3085ae752267d` |
| `/tmp/sst-fs-r{1..4}.json` | 261 cases: 258 pass, same three env strict-Status failures in every retry. | Round 1: `fd052d35401e46f21592015ad3a5d8a9eb0d6c6325881056f3ba5203e36eb3e4` |
| `/tmp/sst-env-head-r{1..4}.json` | Clean HEAD baseline: the same three env cases failed with `SIGABRT` in all four rounds. | Round 1: `55bd9470e44069967db8a34d6d75963d7251373e016fbf85105b608d61971e9d` |
| `/tmp/sst-coerce-r1.json` | Five cases x 100 repeats: 500 pass, zero failures/timeouts. | `16b0f185a2a46e41920c6830751de065d6b962c28e409ae737e8522c6d513b1b` |
| `/tmp/mb-sst-smoke.json` | v1 calibration 8 SST, ratio 2.841; no trials, preflight failed. | `e0396c9633a96a62d7b16c06e6ca987794953f7719d1b0662964a22abb7cf178` |
| `/tmp/mb-sst-smoke-v2.json` | v2 35/35 value-valid trials; 13/15 adaptive trials met the old measured-migration gate; report `coverage_incomplete`. | `79cbdc6f3f1e1217f0ccc40d0c23fa7be9712321bd2c8e53547d7ca29d9a68fa` |
| `/tmp/mb-sst-smoke-v3.json` | v3 35/35 value-valid trials, 14/15 old-rule adaptive coverage; original report `coverage_incomplete`. | `3d7707c13b7cd85fed73bbc33f98f8f58c7d100d814759c0f05490ece3459b5a` |
| [`v3-smoke-audit.json`](v3-smoke-audit.json) | Raw-stdout reparse of unchanged v3: 35/35 valid, 15/15 corrected-rule adaptive coverage, one mechanism verdict changed; `complete` audit verdict. | Original JSON hash embedded in audit |
| `/tmp/mb-sst-standard-v1.json` | Release standard: 105/105 valid, 45/45 adaptive covered, report `complete`; complete raw stdout/stderr and all windows. | `2cd3e913b3eca9223fcebb61c515a3c46ae5711f7610c4ed3697714b00350ba2` |
| [`standard-summary.json`](standard-summary.json) | Compact 105-trial, 35-group and all-window summary derived from the raw Release JSON. | `45b364b1bfab2f4dda6f98de835412c116cea676559175caa1e8452a06cce1d4` |

The v1, v2 and v3 smoke JSONs include every per-trial command, raw stdout/stderr,
binary SHA-256, working-tree source digest, timeout/exit status and inode-dedup
space scan. Their binary SHA-256 values differ, so they are not paired
performance samples. The physical-space scan counts each `(device, inode)`
once across `work/`, published points and the cold store. As both local roots
were on one device, `ssd_physical_saved_bytes` is null by design.

## Release standard experiment

The test agent built `db_bench` with
`AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 db_bench` (exit 0; log
`/tmp/sst-release-dbench-v1-build.log`). The runner used this exact command,
with stdout/stderr captured in `/tmp/mb-sst-standard-v1.log`:

```sh
python3 docs/components/metabypass/experiments/sst_tiering/run.py \
  --binary ./db_bench --profile standard \
  --ssd-root /tmp/mb-sst-standard-v1-ssd \
  --hdd-root /tmp/mb-sst-standard-v1-hdd \
  --output /tmp/mb-sst-standard-v1.json
```

It started at `2026-09-29T20:14:20.312182Z` and ended at
`2026-09-29 21:14:52 UTC`, exiting 0 with `status=complete`. The output log
SHA-256 is `14225fd8d0227ced4c2bc4723e13b9c073227e93e15852083b0afb7c53c5ee4a`.
The binary SHA-256 was
`f1acff2f65423b030adc3ef10e20fd3205a1ded6746a333f3677f6c8653289f9`
both before and after; the source identity records HEAD
`2d3acc00c15902f26789e27dc2c5e7e6fd1ca788` and working-tree source
digest `859d1c5e54815e9e71b1273dd3c4e991da0152a8d9d582d85061e59e15d509fe`.
To reproduce the compact summary from the preserved raw JSON:

```sh
python3 docs/components/metabypass/experiments/sst_tiering/summarize_standard.py \
  --input /tmp/mb-sst-standard-v1.json \
  --output /tmp/mb-sst-standard-v1-summary-recheck.json
```

Read-only `lscpu` reported x86_64, two Intel Xeon Gold 5318Y 2.10 GHz
sockets, 24 cores per socket and 48 logical CPUs (one thread per core).
`df -T /tmp /home/ceph-lj/metabypass_rocksdb` reported both roots on
`/dev/sdc2` ext4. No read delay was injected (`read_delay_us=0`); the run did
not clear the global page cache. Each trial used one foreground thread,
100,000 keys, 1 KiB values, 20,000 warmup operations and 240,000 measured
operations at a fixed offered rate of 10,000 operations/s. The 64 KiB block
cache was much smaller than the calibrated 2,234,745 logical SST bytes
(25 SSTs, ratio 34.10). The frozen nominal 25/50/75% SST budgets were
558,686/1,117,372/1,676,058 bytes, each with at least 25 eligible files.
The policy keeps a 10% reserve, so these nominal budgets are not hard caps.

All 105 trial value checks passed, including expected updates/deletes in the
mixed workload; there were zero invalid trials, timeouts or migration errors.
All 45 adaptive trials met the measured-sampling plus migration/cold-state
coverage contract, all observe trials sampled reads, and disabled trials did
not sample. The switch workload recorded a post-switch promotion in all nine
adaptive trials. These checks do not simulate whole-SSD loss; the dedicated
strict crash tests above cover the published-point recovery contract.

The table gives the median of three trial values. For P99, each trial value is
the median of its measured **window P99s**; brackets show the minimum and
maximum across the three trials. The adjacent maximum-window column preserves
shorter tail spikes and likewise shows its three-trial median and range.
There is no exact pooled whole-trial P99 because the benchmark clears latency
samples after each window. Get/s is total measured Gets divided by actual
measured-phase wall time, not a maximum-throughput test. Space columns are
post-trial paths: SST logical bytes, SSD-path SST allocated blocks, and global
allocated blocks deduplicated by `(device, inode)` across all paths. Units are
MiB; all per-trial values and every measured window are preserved in
[`standard-summary.json`](standard-summary.json).

| Workload | Mode/budget | Get/s median | Window P99 μs: median [range] | Max-window P99 μs: median [range] | SSD SST logical MiB | SSD SST allocated MiB | Global unique allocated MiB |
|---|---|---:|---:|---:|---:|---:|---:|
| uniform | disabled | 9999 | 24 [24-30] | 25 [25-31] | 2.131 | 2.195 | 107.020 |
| uniform | observe/25% | 9999 | 25 [24-31] | 27 [25-32] | 2.131 | 2.195 | 107.031 |
| uniform | observe/50% | 9999 | 24 [24-25] | 26 [26-27] | 2.131 | 2.195 | 107.031 |
| uniform | observe/75% | 9999 | 30 [25-32] | 33 [27-33] | 2.131 | 2.195 | 107.031 |
| uniform | adaptive/25% | 9999 | 24 [24-25] | 26 [26-26] | 0.472 | 0.488 | 105.324 |
| uniform | adaptive/50% | 9999 | 25 [24-25] | 26 [26-27] | 0.909 | 0.938 | 105.773 |
| uniform | adaptive/75% | 9999 | 25 [24-32] | 26 [26-33] | 1.433 | 1.477 | 106.312 |
| hotspot | disabled | 9999 | 24 [24-24] | 26 [26-29] | 2.131 | 2.195 | 107.020 |
| hotspot | observe/25% | 9999 | 24 [24-25] | 26 [26-27] | 2.131 | 2.195 | 107.031 |
| hotspot | observe/50% | 9999 | 25 [24-25] | 26 [26-26] | 2.131 | 2.195 | 107.031 |
| hotspot | observe/75% | 9999 | 25 [25-30] | 27 [26-31] | 2.131 | 2.195 | 107.031 |
| hotspot | adaptive/25% | 9999 | 24 [24-32] | 26 [26-33] | 0.472 | 0.488 | 105.324 |
| hotspot | adaptive/50% | 9999 | 24 [24-24] | 26 [26-26] | 0.909 | 0.938 | 105.773 |
| hotspot | adaptive/75% | 9999 | 24 [24-25] | 26 [26-27] | 1.433 | 1.477 | 106.312 |
| switch | disabled | 9999 | 24 [24-24.5] | 25 [25-27] | 2.131 | 2.195 | 107.020 |
| switch | observe/25% | 9999 | 24 [24-31] | 26 [26-32] | 2.131 | 2.195 | 107.031 |
| switch | observe/50% | 9999 | 24.5 [24-25] | 27 [26-27] | 2.131 | 2.195 | 107.031 |
| switch | observe/75% | 9999 | 25 [24-31] | 27 [26-32] | 2.131 | 2.195 | 107.031 |
| switch | adaptive/25% | 9999 | 24 [24-25] | 26 [26-27] | 0.472 | 0.488 | 105.324 |
| switch | adaptive/50% | 9999 | 24 [24-32] | 26 [26-32] | 0.909 | 0.938 | 105.773 |
| switch | adaptive/75% | 9999 | 24 [24-24] | 26 [26-26] | 1.433 | 1.477 | 106.312 |
| scan | disabled | 9997 | 8 [8-8] | 23 [15-23] | 2.131 | 2.195 | 107.020 |
| scan | observe/25% | 9997 | 8 [8-8] | 23 [22-23] | 2.131 | 2.195 | 107.031 |
| scan | observe/50% | 9997 | 8 [8-8] | 21 [18-21] | 2.131 | 2.195 | 107.031 |
| scan | observe/75% | 9997 | 8 [8-8] | 23 [23-24] | 2.131 | 2.195 | 107.031 |
| scan | adaptive/25% | 9997 | 9 [8-9.5] | 15 [9-23] | 0.472 | 0.488 | 105.324 |
| scan | adaptive/50% | 9997 | 8 [8-9] | 20 [9-24] | 0.909 | 0.938 | 105.773 |
| scan | adaptive/75% | 9997 | 8 [8-9] | 8 [8-23] | 1.433 | 1.477 | 106.312 |
| mixed | disabled | 9024 | 28 [28-29] | 30 [30-30] | 1.987 | 1.992 | 124.180 |
| mixed | observe/25% | 8920 | 25 [25-26] | 32 [29-33] | 1.987 | 1.992 | 124.191 |
| mixed | observe/50% | 8944 | 20.5 [20.5-27] | 30 [30-30] | 1.987 | 1.992 | 124.191 |
| mixed | observe/75% | 8956 | 25 [25-26.5] | 30 [29-32] | 1.987 | 1.992 | 124.191 |
| mixed | adaptive/25% | 8970 | 26 [25-27] | 31 [30-33] | 1.987 | 1.992 | 124.191 |
| mixed | adaptive/50% | 8951 | 25 [23-27] | 31 [30-33] | 1.987 | 1.992 | 124.191 |
| mixed | adaptive/75% | 8977 | 26 [21-26] | 30 [30-31] | 1.013 | 1.016 | 123.215 |

For the stable and switching hotspot workloads, the same two-stage window
aggregation gives the following Get P50/P95 and workload-key-group P99 values
in microseconds. Hot/cold means the requested key fell inside/outside the
current hot quarter; it does not identify the SST or physical read device.
Observe-mode and per-window P50/P95/hot/cold values remain in the JSON.

| Workload | Mode/budget | Get P50 μs | Get P95 μs | Hot-key P99 μs | Cold-key P99 μs |
|---|---|---:|---:|---:|---:|
| hotspot | disabled | 22 [22-23] | 24 [23-24] | 24 [24-24] | 24 [24-25] |
| hotspot | adaptive/25% | 23 [22-23] | 24 [24-24] | 24 [24-32] | 25 [24-32] |
| hotspot | adaptive/50% | 23 [23-23] | 24 [24-24] | 24 [24-24] | 25 [25-25] |
| hotspot | adaptive/75% | 22 [22-23] | 24 [23-24] | 24 [24-24] | 25 [24-25] |
| switch | disabled | 22 [22-23] | 23 [23-24] | 23 [23-24] | 24 [24-25] |
| switch | adaptive/25% | 23 [23-23] | 24 [24-25] | 24 [24-25] | 25 [25-25] |
| switch | adaptive/50% | 22 [22-22] | 24 [23-24] | 24 [24-31] | 25 [24-32] |
| switch | adaptive/75% | 22 [22-23] | 23 [23-24] | 24 [24-24] | 24 [24-24] |

For the four read-oriented workloads, disabled and observe each retained
2.131 MiB logical SST on the SSD path (2.195 MiB allocated). Adaptive at
25/50/75% had median SSD-path occupancy 0.472/0.909/1.433 MiB logical and
0.488/0.938/1.477 MiB allocated. Their median logical SSD-path reductions
were 1.659/1.222/0.698 MiB and allocated-path reductions
1.707/1.258/0.719 MiB against paired disabled trials. The global
inode-deduplicated physical totals also fell from 107.020 MiB to
105.324/105.773/106.312 MiB. These are observed same-device path and total
changes, not measured savings on a separate SSD device; the JSON intentionally
sets all 90 non-disabled `ssd_physical_saved_bytes` values to null.
The category scan also records HDD SST, blob, temporary and metadata paths.
For uniform trials, median HDD SST logical paths were 6.394 MiB in disabled
and 8.525 MiB in observe/adaptive; blob paths were 102.235 MiB. For mixed,
the corresponding figures were 5.956 versus 7.943 MiB HDD SST and 118.851
MiB blob. Temporary-path bytes were zero in all 105 post-trial scans.
Tiering creates more hardlinked HDD path names, so these logical category
totals cannot be added as physical disk usage. The table's global physical
column deduplicates shared inodes, and the summary retains per-category
allocated blocks and metadata bytes for every trial.

Mixed writes and compaction did not maintain the same capacity result. Its
disabled SSD SST path held 1.987 MiB logical. Adaptive 25% ended at 1.987 MiB
in two repeats and 0.003 MiB in one; adaptive 50% ended at 1.987 MiB in all
three; adaptive 75% ended at 1.013 MiB in two and 1.987 MiB in one. The
measured over-budget counter recorded 24-25 s in every adaptive mixed trial,
while it recorded zero in adaptive read-oriented trials. Thus migration was
observed in every adaptive mixed trial, but this high-churn workload did not
demonstrate stable SSD-budget control. The post-process path scan and final
in-process tier summary are separate snapshots and can differ while migrations
settle at shutdown.

Across 45 adaptive trials, measured windows recorded 325 promotions plus
demotions, 15,013,222 promoted bytes, 13,797,622 demoted bytes, and 220 s of
over-budget time (all from mixed). Measured-window peak publication lag was at
most 2.156 s in adaptive, 2.104 s in observe and 1.776 s in disabled trials.
Sampled process CPU time ranged from 12.24 to 30.25 s and sampled peak RSS
from 18,252 to 29,224 KiB across all trials. The switch promotion-response
proxy was observed in 9/9 adaptive trials: median 10.998 s at 25%, 2.997 s
at 50%, and 3.997 s at 75%; their three-repeat ranges were 9.997-10.998 s,
2.997-2.998 s and 3.997-8.998 s. The proxy marks a promotion window after
the key-group switch, not recovery of individual Get latency.

At the fixed offered rate, read-oriented Get throughput stayed near 10,000/s
across modes; mixed achieved about 8,912-9,028 Gets/s as some operations were
writes, deletes, flushes or compactions. Window P99 medians overlap widely
across modes and budgets. These measurements do not show a reliable latency or
throughput win; no maximum-throughput, separate-device or real-HDD experiment
was run. The hot/cold latency categories are workload key groups, not actual
per-Get SSD/HDD file locations. Observe-only includes full tiering routing,
protected links, placement metadata, sampling and policy decisions, so its
comparison with disabled is not a pure sampling-cost isolation.
