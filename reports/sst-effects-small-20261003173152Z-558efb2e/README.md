# Small SSD/HDD SST run — partial result

Run UUID `53d08154-15df-4ae7-8574-f04a94daed26` on node `8001`, task `sst-effects-small-20261003173152Z-558efb2e`. Start: `2026-10-03T17:42:52.180778+00:00`; stop: `2026-10-03T17:57:44.516799+00:00` (runner elapsed 892.336 s). Status: `blocked_or_failed`.

The 90-minute runner stopped early after a dedicated cgroup OOM kill. No rerun, key scaling, cgroup change, or cleanup was performed. This is a partial experiment result, not a complete matrix.

## Result

- Local unit suite: 33/33 passed. Node Python 3.8 unit suite: 33/33 passed.
- Calibration and the separate 10-second adaptive recovery timing probe passed; probe RTO was 44.047722 s. That planned fault probe is separate from the six formal RPO samples.
- Formal performance matrix: **3 valid of 18 planned**. Four rounds were attempted; the first three passed, then `trial-1-switch-adaptive` exited `-9` (not timed out). It had no `ready` event and no `phase_end`; cgroup `oom_kill` advanced by 1. The remaining 14 rounds were not started.
- Formal fault recovery: **0 valid of 6**; the six formal fault samples were not started.

| Attempted valid pair | Fixed ops/s | Saturated ops/s | Get response P99 | Steady SSD SST |
|---|---:|---:|---:|---:|
| uniform / disabled | 107.0 | 189.8 | 10.24 ms | 21.6 MiB |
| uniform / adaptive | 107.0 | 166.3 | 40.96 ms | 9.5 MiB |
| switch / disabled | 107.0 | 192.2 | 20.48 ms | 21.6 MiB |
| switch / adaptive | invalid: OOM before ready | n/a | n/a | n/a |

Each valid mode/workload has only one repeat, so these measurements do not support a stable performance conclusion. The complete partial table and measurement caveats are in [summary.md](summary.md); detailed histograms and all command records are in [summary.json](summary.json).

## Identity and evidence

- Frozen source archive SHA256: `a67f5fc5e825d8f387a5d9deb9181892faaf42568e906aa21b9711d24c91b6e4`; 2,427 paths verified. `node_run.py` SHA256: `fc65f170d6a88d3b52eb97c2dacc6dca92186d0d0ae455cfe35b1f5ac535e8e8`.
- `db_bench` SHA256 at start and end: `2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76` (130,330,744 bytes; remote inode 18641735).
- Dedicated cgroup: `/sys/fs/cgroup/memory/mbsst-small-20261002`, 50,331,648-byte limit. Final read showed max usage 50,335,744 B, OOM kill count 1, kmem current/max 31,019,008/32,194,560 B, no remaining tasks or child groups. See [memory-evidence.json](metadata/memory-evidence.json) and [cgroup-final-readonly.txt](metadata/cgroup-final-readonly.txt).
- Failed trial data remains on node-ssd at `/home/lj/ssd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/trial-1-switch-adaptive` (27 MiB) and node HDD at `/home/lj/hdd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/trial-1-switch-adaptive` (1.1 GiB), owned by `lj:lj`; it was not downloaded or removed.

## Files

- Raw runner JSON, samples, ACKs, stdout/stderr and reports: [raw-node/ssd/task/control](raw-node/ssd/task/control). The unchanged raw `runner.json` SHA256 is `29180972838f781363b6e8bfd76d938ece8a01a5b994a6885156845ebf51d8c4`.
- Compressed report/control/unit archive (no DB data or binary): [ssd-task-reports.tar.gz](raw-node/ssd-task-reports.tar.gz).
- Local and node unit logs: [unit/local](unit/local) and [raw-node/ssd/task/unit](raw-node/ssd/task/unit).
- Frozen source files: [source/frozen](source/frozen). Exact benchmark commands, source/binary IDs are in the raw runner JSON and summary JSON.
- Charts: [throughput PNG](summary-saturated_throughput_ops_s.png), [SSD SST PNG](summary-sampled_measured_median_ssd_sst_allocated_bytes.png), with SVG versions alongside.

Summary generation completed offline with `node_summary.py`; the original remote `runner.json` remains unchanged. The path-remapped analysis input and its integrity metadata are retained under `metadata/`.
