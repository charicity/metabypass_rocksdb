# Performance across Metabypass versions

All four versions use separated Blob Direct Write with 1 KiB values, decimal
keys, one writer, no compression, WAL enabled, and sync=false. The no-backup
baseline is the existing `metabypass_mode=baseline`, as requested: it retains
the same value placement, but does not instantiate the Backup module. It is
not an upstream RocksDB build with inline SST values.

- **No backup:** current release binary, backup disabled.
- **Original:** original Metabypass, commit `e7663cf90`, serial mirroring and full validation.
- **Incremental:** two-thread pipeline and incremental validation, before notification scheduling was fixed.
- **Fixed:** pipeline, incremental validation and the production notification fix.

Recovery points in every backup version are published atomically only after
complete validation. Incremental validation does not mean partially publishing
unvalidated recovery points.

## Method

Same Ubuntu 22.04 KVM host, 48 exposed Xeon Gold 5318Y vCPUs, shared ext4
volume for index/data/backup. Five trials per workload and version, after
warmup. The three backup versions were interleaved with rotated order; the
no-backup group ran subsequently with alternating 50k/200k sizes. No other
build/test process overlapped these measurements. Cache state and hypervisor
interference were not controlled. Results are workload-specific and not a
statistical claim about small differences.

Values below are medians in milliseconds. Total means foreground + explicit
backup sync + close, excluding Open. Its median is taken from per-run sums,
so it need not equal the sum of separate phase medians. No-backup total does
not include producing a recovery point and is used to quantify backup cost.

## Default queue, 200,000 writes

64 MiB queue, 256 KiB batching trigger, 1,000 ms interval.

| Version | Foreground | Backup sync | Close | Total |
|---|---:|---:|---:|---:|
| No backup | 1450.221 | 0.000 | 371.644 | 1838.736 |
| Original | 1758.965 | 2271.554 | 2383.223 | 6424.727 |
| Incremental | 2406.206 | 228.149 | 1069.817 | 3681.201 |
| Fixed | 1711.112 | 462.691 | 1048.085 | 3232.892 |

Fixed versus each reference (positive means more time):

| Reference | Foreground time change | Total time change |
|---|---:|---:|
| No backup | +18.0% | +75.8% |
| Original | -2.7% | -49.7% |
| Incremental | -28.9% | -12.2% |

The fixed foreground time is close to original Metabypass, with a nominal
2.7% reduction whose range overlaps the original measurements. The substantial
gain over original is in the complete backup lifecycle, about 49.7% less time.
Against the incremental version, foreground time decreases 28.9%, equivalent
to about 40.6% higher foreground operations/second; total time decreases 12.2%.
Compared with no backup, foreground still costs 18.0% more time and the
complete lifecycle 75.8% more. These costs include creating a validated backup.

The fixed explicit sync median increases from 228 to 463 ms versus the
incremental version. It therefore remains important to report completion time
as well as the earlier foreground finish; the totals above include this wait.

## Other workloads

| Workload | Version | Foreground (ms) | Total (ms) | Backpressure (ms) |
|---|---|---:|---:|---:|
| 50k_default | No backup | 373.455 | 499.219 | 0.000 |
| 50k_default | original | 431.621 | 1678.541 | 0.000 |
| 50k_default | incremental | 596.154 | 1194.962 | 0.000 |
| 50k_default | fixed | 431.346 | 1095.116 | 0.000 |
| 50k_pressure | No backup | 373.455 | 499.219 | 0.000 |
| 50k_pressure | original | 790.518 | 2094.709 | 319.191 |
| 50k_pressure | incremental | 577.332 | 1159.078 | 0.000 |
| 50k_pressure | fixed | 451.619 | 1087.056 | 0.000 |

The pressure case uses a 2 MiB queue and 64 KiB batch trigger. The same 50k
no-backup measurements are shown as a reference because that mode has no
backup queue. For 50k/default, fixed and original foreground medians are
essentially equal; total time is 34.8% lower. For 50k/pressure, fixed reduces
original foreground time by 42.9% and total by 48.1%, while measured capacity
backpressure falls from 319 ms to zero. Against incremental, pressure-case
foreground time falls 21.8% and total 6.2%.

## Verification and reproduction

All 45 measured backup writes, 45 restores after removal of the primary index
and 45 full value verifications passed. Every restore used the original reader.
All ten measured no-backup writes passed. Warmups are excluded from summaries.
This is not a power-failure or real-device durability qualification.

[Backup raw results](experiments/version-results.json) and
[no-backup raw results](experiments/no-backup-results.json) record exact commands,
outputs, medians, ranges and binary hashes. The production code was unchanged
during this comparison. The source identities match the notification fix.

```sh
python3 docs/components/metabypass/experiments/run_version_benchmark.py \
  --original=/tmp/metabypass-scan-ablation-bin/baseline \
  --incremental=/tmp/metabypass-scan-ablation-bin/async-incremental \
  --fixed=/tmp/mb-notify-fixed-db-bench --output=/tmp/mb-version-results.json
python3 docs/components/metabypass/experiments/run_no_backup_benchmark.py \
  --binary=/tmp/mb-notify-fixed-db-bench --output=/tmp/mb-no-backup-results.json
```
