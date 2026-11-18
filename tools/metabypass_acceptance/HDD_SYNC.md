# HDD sync comparison

This adapter uses frozen `controller-9f4dc2d` modules through `PYTHONPATH` and
patches a build-directory copy of `driver-9475062`. Frozen files are never
modified. It does not change `protocol.h` or probe headers for capabilities.

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

Both arms use CP40s/mirror5s/batch512MiB/queue1GiB/retain1. Formal load is
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
