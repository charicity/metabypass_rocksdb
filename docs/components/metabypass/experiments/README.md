# Recovery-point content-scan ablation

This experiment measures the cost of content rescanning in `Backup::Publish`.
The patch is an ablation for timing, not a supported configuration or a safe
incremental-validation implementation. The production sources are unchanged.

## Compared paths

Baseline is the ordinary release `db_bench` from commit `e7663cf90`. The
experiment applies `skip_content_scans.patch` only to `backup.cc` in a separate
build directory. Both use `AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j8 db_bench`.

The experiment removes:

- `Dependencies`: the additional WAL/WriteBatch traversal and per-reference
  blob header, key, length and payload CRC validation.
- `ValidateTable`: SST checksum verification, entry iteration and blob reference
  validation.
- The blob content scan in `Persist`.
- Whole-file/prefix `Digest` reads for the native-file and blob inventory.

It retains MANIFEST parsing, atomic edit-group checks, WAL record-boundary
parsing with the native reader, SST size/closed checks, mirror updates, SST
hardlinks, mutable-file copying, file and directory sync, atomic pointer
publication, retention, queue limits and backpressure. Thus this is removal of
content-validation rescans, not removal of all native metadata reading.

Without dependency extraction, the experiment enumerates and syncs every blob
file in the retained data directory. This conservatively includes extra files;
blob sync is not silently dropped. The clean append-only workloads below have
no deliberately orphaned blob files.

The inventory starts with `EXPERIMENT-NO-CONTENT-VALIDATION`, has no blob
checksums, and uses placeholder index checksums. The unchanged restore parser
rejects it as `invalid inventory`. Experimental points are deliberately not
valid backups. Restoring the original implementation requires real validation
or a separately designed incremental validation mechanism, not just removing
this marker.

## Reproduction

Create two release binaries from the same source revision. Build and save the
baseline, apply the patch to a disposable source/build directory, then build
and save the experiment. Do not mix debug/release objects or benchmark during
compilation. For example, in the disposable checkout:

```sh
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j8 db_bench
cp db_bench /tmp/metabypass-baseline-bench
patch -p1 < /absolute/path/to/skip_content_scans.patch
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j8 db_bench
cp db_bench /tmp/metabypass-no-scans-bench
python3 /absolute/path/to/run_scan_ablation.py \
  --baseline /tmp/metabypass-baseline-bench \
  --experiment /tmp/metabypass-no-scans-bench \
  --output /tmp/metabypass-scan-ablation-results.json
```

The runner uses fresh directories and five paired trials per scenario,
reversing baseline/experiment order on alternating trials. Each subprocess is
capped at 60 seconds. A 1000-write warmup precedes measurements. Each measured
write run is followed by full key/value verification using the live primary
index; experimental points are also checked for rejection by the ordinary
restore implementation before verification. This is a performance ablation,
not a crash-recovery validation of the experimental path.

Workloads are 50,000 and 200,000 inserts with default queue settings, plus
50,000 inserts with a 2 MiB queue and 64 KiB trigger. Each uses one writer,
decimal-string keys, 1024-byte values, no compression, WAL enabled and
`sync=false`. The normal trigger is 256 KiB or 1000 ms, capacity 64 MiB.

All paths reside on the same ext4 volume of an Ubuntu 22.04 KVM guest (Linux
5.15, GCC 11.4, 48 exposed Xeon Gold 5318Y vCPUs, about 31 GiB RAM). Cache
contents and host-level interference are not controlled. There is no storage
throttling or power interruption. Builds were finished before measurement.

## Results (2026-09-20)
All values below are medians of five runs, in milliseconds. The final point
metrics measure the last publication, not the mean over all publications.
The write + sync + close row is the median of each run's sum, not the sum
of three independent medians.

### 50k_default
| Metric (ms) | Baseline | No content scans | Time reduction |
| --- | ---: | ---: | ---: |
| Foreground writes | 425.460 | 427.865 | -0.6% |
| Explicit backup sync | 556.906 | 148.389 | 73.4% |
| Close | 692.792 | 132.322 | 80.9% |
| Final point build | 323.656 | 43.260 | 86.6% |
| Final point lag | 661.771 | 100.468 | 84.8% |
| Backpressure | 0.000 | 0.000 | n/a |
| Write + sync + close | 1672.576 | 732.754 | 56.2% |

Foreground ranges (min--max): baseline 417.654--457.459 ms; experiment 416.073--451.905 ms.

### 200k_default
| Metric (ms) | Baseline | No content scans | Time reduction |
| --- | ---: | ---: | ---: |
| Foreground writes | 1759.017 | 1720.879 | 2.2% |
| Explicit backup sync | 2169.545 | 194.368 | 91.0% |
| Close | 2337.510 | 198.419 | 91.5% |
| Final point build | 1114.728 | 51.457 | 95.4% |
| Final point lag | 2288.864 | 141.137 | 93.8% |
| Backpressure | 0.000 | 0.000 | n/a |
| Write + sync + close | 6300.145 | 2110.758 | 66.5% |

Foreground ranges (min--max): baseline 1683.155--1948.149 ms; experiment 1665.013--1813.564 ms.

### 50k_pressure
| Metric (ms) | Baseline | No content scans | Time reduction |
| --- | ---: | ---: | ---: |
| Foreground writes | 781.745 | 448.248 | 42.7% |
| Explicit backup sync | 609.003 | 120.389 | 80.2% |
| Close | 682.404 | 134.273 | 80.3% |
| Final point build | 314.695 | 43.350 | 86.2% |
| Final point lag | 650.499 | 98.807 | 84.8% |
| Backpressure | 340.556 | 9.468 | 97.2% |
| Write + sync + close | 2071.100 | 704.898 | 66.0% |

Foreground ranges (min--max): baseline 763.588--803.705 ms; experiment 440.826--473.485 ms.

The default-queue cases had zero backpressure in both versions. Foreground
changes (-0.6% time reduction at 50k, +2.2% at 200k) are small relative to
run-to-run variation; this experiment does not show a convincing foreground
speedup without queue pressure. Point construction, however, fell by 86.6%
and 95.4%, respectively.

With the 2 MiB queue, foreground time fell by 42.7% (about 1.74x throughput)
and accumulated backpressure fell by 97.2%. Peak charged queue size was
2,097,095 bytes for both variants. Faster publication allows the worker to
resume consuming queued events sooner.

All 30 measured writes and all 30 full live-index verifications succeeded.
The 15 experimental backups were rejected by the ordinary restore parser as
expected. The two warmup writes also succeeded. Both variants mirrored exactly
2,876,871 bytes in the 50k workloads and 11,631,177 bytes in the 200k workload;
no blob files were copied into backup. Publication timing can change the
preceding point retained and its space usage even for identical mirrored
bytes. No SIGKILL or recovery-success claim is made for the ablated variant.

Raw commands, output, timings, ranges and binary SHA-256 hashes are saved in
[results.json](results.json). [run_scan_ablation.py](run_scan_ablation.py) is
the runner; [skip_content_scans.patch](skip_content_scans.patch) is the exact
experimental change. No production option or public API was added. The
experiment release build passed; the runner completed with exit code 0 and
was syntax checked. Existing functional/full-suite results for the unchanged
production implementation are in [the validation record](../validation.md);
they were not rerun or presented as tests of this intentionally weakened path.

The result identifies repeated content validation as a substantial background
cost in these workloads. It does not show that simply removing it preserves
recovery guarantees, nor compare performance with Ceph. An equivalent safe
optimization would need to maintain dependency and checksum state
incrementally and validate each new or changed extent without rescanning all
old data. This experiment does not implement that optimization.
