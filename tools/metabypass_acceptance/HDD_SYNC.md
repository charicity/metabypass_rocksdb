# HDD sync comparison

This adapter uses frozen `controller-9f4dc2d` modules through `PYTHONPATH` and
patches a build-directory copy of `driver-9475062`. Frozen files are never
modified. The generated driver preserves `protocol.h` and selects the queue
fields from the target core's public options at compile time.

From the repository root:

```sh
export PYTHONPATH="$PWD/reports/e157-8g-20261117-e928/delivery/frozen-sources/controller-9f4dc2d/tools/metabypass_acceptance"
python3 -m unittest discover -s tools/metabypass_acceptance -p 'test_*.py' -v
python3 tools/metabypass_acceptance/hdd_sync_compare.py plan
python3 tools/metabypass_acceptance/hdd_sync_compare.py plan --case=hdd-sync-preflight
```

Build each arm in separate external build/artifact directories. Use clean
checkouts, each with its own matching headers and shared library. Baseline is
`52c3ca19ea8bab43e60adfb1e268387435315a85`; final must contain all three fixes.
The following placeholders denote absolute paths and full commits:

```sh
python3 tools/metabypass_acceptance/prepare_driver.py \
  --core=/path/core52 --core-commit=52c3ca19ea8bab43e60adfb1e268387435315a85 \
  --build=/path/build-base --artifacts=/path/artifacts-base \
  --has-recovery-time-sampling=0
python3 tools/metabypass_acceptance/prepare_driver.py \
  --core=/path/final --core-commit=FINAL_FULL_COMMIT \
  --build=/path/build-final --artifacts=/path/artifacts-final \
  --has-recovery-time-sampling=1
```

`--plan` prints commands without building. The default is release, strict Status,
no RTTI/diagnostics, clang-14, 48 build jobs. After OOM, retry with 32/16/8 jobs,
keeping the same `--build` path and using a new `--artifacts` directory on each
retry (for example `--jobs=32 --artifacts=/path/artifacts-final-j32`). This
preserves successful build objects and the previous attempt's logs; an existing
artifacts directory is intentionally rejected. The two builds compile the identical patched
driver source with explicit `MB_HAS_RECOVERY_TIME_SAMPLING=0/1`. Macro 0 rejects
sampling=false. Macro 1 applies the public option. Ready and recovery JSON record
the actual setting. Build manifests record source/protocol/patch/binary/library
hashes, commands and adjacent-library resolution; the runner validates binary
hashes and matching updated driver/protocol sources before running.
`--driver-source-commit` must match each build manifest's `adapter_commit`;
`--harness-commit` must match the actual controller checkout commit. Aggregation
checks these actual records as well as cross-round source consistency.

On node-ssd, use existing, empty 32GiB cgroup v1 and user-owned task accounting
roots. Both drivers and their adjacent libraries belong under the SSD accounting
root. No sudo, global cache/swap changes, existing directory overwrite, or
unrelated process termination is required. The inherited controller creates
exclusive sample directories and guards 64/128GiB SSD/HDD task caps and
20/100GiB minimum free space. It observes swapping without changing swap.

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py run \
  --case=hdd-sync-preflight \
  --driver=/SSD_TASK/artifacts-final/metabypass_acceptance \
  --baseline-driver=/SSD_TASK/artifacts-base/metabypass_acceptance \
  --build-manifest=/SSD_TASK/artifacts-final/build-manifest.json \
  --baseline-build-manifest=/SSD_TASK/artifacts-base/build-manifest.json \
  --source-commit=FINAL_FULL_COMMIT --driver-source-commit=ADAPTER_FULL_COMMIT \
  --harness-commit=ADAPTER_FULL_COMMIT \
  --ssd-root=/SSD_TASK --hdd-root=/HDD_TASK \
  --ssd-accounting-root=/SSD_TASK --hdd-accounting-root=/HDD_TASK \
  --output=/HDD_TASK/preflight
```

After all eight checks pass, use the same command with `--case=hdd-sync --round=1`
and a new output directory; then run round 2 with another new output. There are
48 formal samples: two versions x two values (64/1024 bytes) x Blob off/on x
Put/Batch8/Batch128 x two rounds. Same-configuration arms are adjacent; config
index alternates the first arm and round 2 reverses it. Only final/baseline
comparisons are emitted. Sampling is true for core52, false for final.

Both arms use CP40s/mirror5s/batch512MiB/queue1GiB/retain1. On split-queue
cores the explicit 1 GiB budget is 896 MiB for WAL and 128 MiB for metadata.
The driver reports the combined capacity in `mirror_queue_capacity`. Omitting
the flag uses the target core's default queue capacities. Formal load is
1,048,576 16-byte keys, warm65s/measure180s, 4,096 full-value recovery samples.
Short checks use Batch128, 16,384 keys, warm1s/measure7s and 256 samples, preserving
the real CP/mirror intervals. Short checks must recover incremental writes;
formal samples additionally require a new periodic checkpoint during load.
Compression is off, WAL on, sync false, cache 256MiB, buffers 128MiB x2,
background jobs 4, min_blob_size 0, Direct Write/staging/SST tiering/diagnostics
off. Frozen acceptance checks validate the effective ready configuration,
complete-batch sequence and value sample counts. Time bounds and controller
RPO windows are absent for the optimized arm; raw telemetry is retained.

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py aggregate \
  --round-ledgers=/HDD_TASK/round1/results.json /HDD_TASK/round2/results.json \
  --output=/HDD_TASK/summary
```

Outputs include 48 observation rows, 24 within-round pairs and 12 cross-round
summaries. Failed samples keep evidence, with no automatic performance retry.
Two rounds' median/min/max are descriptive observations, not confidence
intervals. Recovery checks sample full values and do not certify every key.

## One-round write breakdown

Add `--write-breakdown` to the short-check and round-1 controller commands.
The driver collects these measurements during the write phase only. Report
the 24 formal rows and 12 paired comparisons with:

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py aggregate \
  --write-breakdown --round-ledgers=/HDD_TASK/write-round1/results.json \
  --output=/HDD_TASK/write-summary
```

The same driver source is used for both cores. To build only the driver, add
`--reuse-core-artifacts=/SSD_PREVIOUS/ARTIFACTS` to `prepare_driver.py`, with
that arm's original `--core`, `--core-commit`, and `--build` values and a new
`--artifacts` directory. The reused core library and compile records stay
associated with the original core commit. Enable the writer's measurements
with `--write_breakdown=1` when invoking the driver directly.

- WAL and MemTable timings come from the writer's PerfContext on a reproducible
  pseudo-random sample of about one in 64 calls. Native WAL timing includes
  synchronous mirror admission. Calls are assigned by successful return time.
- Flush and compaction use native RocksDB Statistics job times and byte counts.
  The report gives the first and last in-window snapshot times and the gap to
  SIGKILL. A job can run across a snapshot boundary. Cumulative background
  task time can overlap foreground time and other tasks.
- Redundancy conversion covers WAL mirroring and checkpoints. The driver
  observes both Append overloads on active HDD mirror WAL files, including
  buffered write time. Reports retain native mirror bytes, queue backpressure,
  checkpoint copy bytes, phase times and observed publication durations.
  Checkpoint publish includes Persist. Aggregate HDD calls cover other paths.
- Data offload covers ordinary Blob values written directly to HDD. The report
  includes Blob append/sync durations and bytes within flush and compaction.
  These timings and bytes overlap the native jobs. Blob-off is not applicable.

Short checks require positive foreground WAL and MemTable times and observed
mirror WAL append calls, bytes, and time during the measurement window. Blob-on
also requires positive native Blob bytes since the writer opened, including
prefill, and the original check for nonempty Blob files.

Both arms enable the same counters. Statistics and sampling can affect measured
throughput, so compare this round's arms under the same instrumentation.

## BE40 repeat and MB40 supplement

The instrumented BE40 campaign uses the same two strict Release builds and the
same run arguments as above. The baseline MB and BE40 share the core52 driver.
The optimized MB uses core `0330a5e84`. Do not pass the MB recovery-time option
to BE. The new cases automatically enable write breakdown for all writers.

- `plan --case=hdd-sync-be40` lists 48 formal samples. Round 1 runs the three
  groups together (36 samples), rotating the first arm each configuration.
  Round 2 repeats only BE40 (12 samples).
- `run --case=hdd-sync-be40-preflight` performs 12 small recovery checks with
  the real periods. BE must restore its initial backup and observe real SST,
  metadata, file sync and directory sync operations. Blob-on must also observe
  native Blob writing and copied Blob bytes. MB checks mirror WAL and recovery
  past its initial sequence.
- Once short checks pass, run `--case=hdd-sync-be40 --round=1` and then
  `--case=hdd-sync-be40 --round=2` in separate output directories. All formal
  workloads use 65 seconds of warmup plus 180 seconds of measurement. Require
  actual periodic backup completion. BE uses WAL backup, sync publication,
  shared files, no pre-backup flush and one point after each completed purge.
  Retain crash-window inventory observations separately.

BE uses a backup-only Env to observe rolling cumulative destination Append
calls, successful bytes and time by file type (WAL, SST, Blob, other data files,
and metadata). File Sync/Fsync and directory sync have separate counters.
Business Blob placement is still measured with the native Blob statistics.
BE's backup Blob writes belong to the backup breakdown. BE create interval
includes capture, copy and publication, and purge covers cleanup. MB mirror
and checkpoint counters keep their original semantics. Engine-specific values
are kept in separate columns.

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py aggregate \
  --case=hdd-sync-be40 \
  --round-ledgers=/HDD_TASK/round1/results.json /HDD_TASK/round2/results.json \
  --mb-history-ledger=/PATH/TO/PREVIOUS_MB_BREAKDOWN/round1/results.json \
  --output=/HDD_TASK/summary
```

Use the instrumented `dfab99721` MB round-1 ledger as history. The report exports
48 new rows, 36 same-round pairs, 12 same-configuration BE repeats and 24
same-version MB historical comparisons. Repeat comparisons retain both raw
observations, observed ranges and valid counts. Historical rows stay outside
the new sample ledger and pass counts.

## Foreground WAL stages

Use the same instrumented Release library and driver for all three groups.
Build them with `prepare_driver.py --fine-wal` so the library and driver both
include the diagnostic gate. The runtime writer flag selects whether that gate
records stage timings. The driver manifest must have `fine_wal: true`.
The single-core case uses
`--driver`, `--build-manifest`, and the core, driver, and harness commit
arguments above. It has no separate baseline driver. The three groups are:

| Group | Engine | Fine mode | Recovery sampling |
| --- | --- | --- | --- |
| mb40-fine-off | MB40 | Matched reference | Off |
| mb40-fine-on | MB40 | All foreground stages | Off |
| be40-fine-on | BE40 | Primary append only | N/A |

`plan --case=hdd-sync-fine-wal` lists nine formal samples, with the three
groups rotating within each 64B Blob-off Put/Batch8/Batch128 configuration.
`run --case=hdd-sync-fine-wal-preflight` uses Batch8, 16,384 keys, and the
existing short recovery settings in all three groups. It verifies the same
recovery and backup-I/O requirements plus positive fine counts on active
paths. Use `run --case=hdd-sync-fine-wal` once those short checks pass. The
formal case uses the same large MB queue and 65s/180s workload intervals.

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py aggregate \
  --case=hdd-sync-fine-wal --round-ledgers=/HDD_TASK/results.json \
  --output=/HDD_TASK/summary
```

The coarse timing remains enabled for all groups. Fine timers cover a
deterministic approximately 1/1024 subset of the same samples. The fine-off
reference records native API, WAL and MemTable counters for the same selection
without stage timers. The report keeps those matched means separate from
the coarse means used for the disturbance check.

- Full-window throughput uses final ACK counters. Fine timing and the
  backup-filtered observation use the sampled ACK intervals through the last
  exported snapshot. For the filtered view, drop whole intervals that overlap
  observed backup intervals, extended by two seconds on either side.
- Report stage occurrence counts, raw nanoseconds, per-sampled-call and
  per-sampled-record means, and primary Append bytes. Keep p95 empty-clock
  timing with the raw results. Stage means at or below that interval remain
  unresolved. Native stopwatch scopes can overlap individual stage timers.
- Defer attributing stage costs whenever fine-on changes MB throughput by
  more than 3% in either direction or changes the coarse mean API latency by
  at least one quarter of the absolute MB-off/BE gap. The report always
  retains observed timings and coverage for both windows.

## MB40 WAL payload A/B

Use `--case=hdd-sync-wal-payload` for the four formal rows. Put runs baseline
then optimized. Batch8 runs optimized then baseline. Both workloads use 64B
values, Blob off, and the same CP40s/mirror5s/512MiB-batch/1GiB-queue settings,
with 65 seconds warmup and 180 seconds measurement. Recovery time sampling
is disabled on both arms. The coarse write breakdown is enabled on both
writers, with the fine WAL probes compiled out.

Use two strict ordinary builds with the same derived driver source and
protocol and with `--has-recovery-time-sampling=1` on each build. Supply the
paths and manifests via `--baseline-driver`, `--baseline-build-manifest`,
`--driver`, and `--build-manifest`. The default baseline is `1e7dd4903` in
these cases. Pass the optimized core using `--source-commit`. The original
controller, storage and cgroup options still apply.

- `plan --case=hdd-sync-wal-payload` lists the four rows and exact order.
- `run --case=hdd-sync-wal-payload-preflight` runs the two arms with Batch8,
  16,384 keys, warmup 1s/measurement 7s and 256 full-value checks.
- `run --case=hdd-sync-wal-payload` runs the four formal rows in a new output
  directory after preflight. It verifies measured periodic publication and
  recovery from the surviving HDD after isolating the SSD files, using the
  same 4096 sampled full values and incremental range checks.

```sh
python3 tools/metabypass_acceptance/hdd_sync_compare.py aggregate \
  --case=hdd-sync-wal-payload --round-ledgers=/HDD_TASK/results.json \
  --output=/HDD_TASK/summary
```

The report compares full-window throughput and coarse API/WAL timing for
each pair. A separate sampled view drops intervals overlapping observed
checkpoint intervals padded by two seconds at each end. The raw interval
table shows what was excluded. Queue and RSS peaks use sampled observations;
writer wait4 RSS is also included as the lifetime high-water mark. Outputs
retain publication events, recovery and resource metrics.
