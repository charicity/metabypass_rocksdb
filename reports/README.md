# Small SST experiment reports

These reports record the SSD/HDD experiments on `node-ssd` (hostname `8001`).
Each linked README contains the full protocol, run identity, results and caveats.

| Experiment | Result | Committed artifacts |
| --- | --- | --- |
| [Initial small run](sst-effects-small-20261003173152Z-558efb2e/README.md) | Partial: 3/18 performance trials, 0/6 formal fault samples; stopped after a cgroup OOM kill. | [Summary](sst-effects-small-20261003173152Z-558efb2e/summary.md), [throughput PNG](sst-effects-small-20261003173152Z-558efb2e/summary-saturated_throughput_ops_s.png), [SSD SST PNG](sst-effects-small-20261003173152Z-558efb2e/summary-sampled_measured_median_ssd_sst_allocated_bytes.png). |
| [CPU-affinity / kmem A/B](sst-sourcefix-ab-20261004/README.md) | Four paired trials; all 16 copy/run commands succeeded, no OOM kills. CPU binding was associated with lower residual kernel memory; the mechanism and exact cause of the earlier OOM remain unproven. | [Controller](sst-sourcefix-ab-20261004/run_ab.py), [offline summarizer](sst-sourcefix-ab-20261004/metadata/summarize_ab.py). |
| [Final small run](sst-effects-small-final-20261003-2810448d/README.md) | Complete: 18/18 performance trials and 6/6 formal fault samples valid; no OOM kills; elapsed 78m22s (4701.612s). | [Summary](sst-effects-small-final-20261003-2810448d/summary.md), [throughput PNG](sst-effects-small-final-20261003-2810448d/summary-saturated_throughput_ops_s.png), [SSD SST PNG](sst-effects-small-final-20261003-2810448d/summary-sampled_measured_median_ssd_sst_allocated_bytes.png). |

The final run observed a 56.1% reduction in steady measured-phase SSD SST
allocation, with lower saturated throughput and higher GET latency in adaptive
mode. Peak SSD SST allocation did not show the same reduction. Cache residency
was partial (70%/75% in calibration), so this cannot be described as a cold-cache
experiment. Six formal RPO observations do not support an RPO P99 estimate.

Git retains this index, the three report READMEs, the two SST Markdown summaries
and PNG charts, and the two A/B Python scripts. Raw logs, `summary.json`, SVGs,
source/control archives and benchmark binaries remain outside Git. Local raw
paths below are relative to this directory; remote paths are on `node-ssd`.

| Experiment | Local raw evidence | Remote raw evidence |
| --- | --- | --- |
| Initial | `sst-effects-small-20261003173152Z-558efb2e/raw-node/ssd/task/control/` | `/home/lj/ssd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/control/` |
| A/B | `sst-sourcefix-ab-20261004/raw-node/` | `/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/` |
| Final | `sst-effects-small-final-20261003-2810448d/raw-node/control/` | `/home/lj/ssd/metabypass_test/sst-effects-small-final-20261003-2810448d/control/` |

SHA-256 values below were computed from the retained local evidence:

| Evidence file (relative to `reports/`) | SHA-256 |
| --- | --- |
| `sst-effects-small-20261003173152Z-558efb2e/raw-node/ssd/task/control/runner.json` | `29180972838f781363b6e8bfd76d938ece8a01a5b994a6885156845ebf51d8c4` |
| `sst-sourcefix-ab-20261004/raw-node/ab-report.json` | `41c87824f966a6953e0697754fa2292ca2b53de55e720c9daa0379a6f7c11860` |
| `sst-effects-small-final-20261003-2810448d/raw-node/control/runner.json` | `2a25a427a622c9e23c3f558c4cadf3ba9f71b8ef4168d638f3d081baa579eac6` |
| `sst-effects-small-20261003173152Z-558efb2e/source/frozen/source.tar.gz` | `a67f5fc5e825d8f387a5d9deb9181892faaf42568e906aa21b9711d24c91b6e4` |
| `sst-effects-small-final-20261003-2810448d/metadata/control-file-manifest.json` | `b5cd14901791f3a0925471b32653dc9465f494e8d01d4d2e9cb4aa234b6b5737` |

The report READMEs record the common runtime binary SHA-256
`2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76`
and each frozen source/controller identity. License headers and ASCII compatibility cleanup were applied to the
two archived A/B scripts for repository inclusion; their runtime identities
remain those recorded in the original logs, rather than the hashes of these
adjusted archive copies. Raw experiment evidence was left unchanged.
