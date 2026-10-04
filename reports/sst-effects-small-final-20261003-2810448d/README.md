# Small SST real SSD/HDD experiment

Status: `complete` on `8001`; run UUID `6668e725-7dca-4df3-901d-7e028d0626e7`.

Started 2026-10-03T19:40:55Z; runner elapsed 4701.612s; derived finish 2026-10-03T20:59:16.611933+00:00; 90-minute deadline 2026-10-03T21:10:55+00:00.
Profile: 1,000,000 keys, 16-byte keys, 1,024-byte values; cgroup memory limit 50,331,648 B (48 MiB), queue 2,097,152 B (2 MiB), CPU list 0,1,2,3, warmup/measured/saturated 30/60/15 s.

The planned 3 workloads × 2 modes × 3 repeats completed: **18/18 trials valid**; all warmup/measured/saturated phases, mechanism and cache gates passed: `True`. The six formal fault samples completed **6/6 valid**, each with a valid first-60-second read: `True`. OOM-kill deltas were zero across all commands: `True`.

## Calibration and mincore evidence

Calibration was accepted by the frozen gate: `True`. The original 1.5× available-file-cache target was not met at this small data size: 22,671,661 B of seed SST logical bytes versus a 33,005,568 B available-file-cache proxy (0.687×). The run proceeded under the approved fallback gate: one complete actual mincore sample per measured mode over all 22 live routed SSTs, with 100% byte coverage and weighted residency of 70.0% (disabled) and 75.0% (adaptive), both at or below the 90% limit. This is partial cache residency, not a strong cold-cache run; sampled device I/O includes blob and SST traffic and cannot be attributed only to SST reads.

| Calibration command | Process read / written bytes | `/dev/sdb` SSD read / written | `/dev/sdc1` HDD read / written |
|---|---:|---:|---:|
| seed-1000000 | 3,027,853,312 / 1,275,572,224 | 29,921,280 / 102,809,600 | 2,998,009,856 / 1,173,499,904 |
| cal-1000000-disabled | 4,436,619,264 / 22,929,408 | 55,508,992 / 1,159,168 | 4,381,102,080 / 28,717,056 |
| cal-1000000-adaptive | 4,457,336,832 / 38,039,552 | 39,333,888 / 21,041,152 | 4,418,002,944 / 42,975,232 |
| cal-1000000-observe | 4,475,068,416 / 23,035,904 | 71,618,560 / 2,670,592 | 4,403,458,048 / 27,664,384 |

The first numeric pair is the first-to-last sampled per-process `/proc/<pid>/io` `read_bytes/write_bytes`; device pairs are whole-device disk counters sampled at the command endpoints, so they may include other host I/O. The raw sample logs preserve both. The seed and all three calibration commands observed I/O on SSD and HDD.

## Formal repeated results

| Workload | Mode | Valid repeats | Measured ops/s median [range] | Saturated ops/s median [range] | GET service P99 µs median [range] | GET response P99 µs median [range] | Measured-phase SSD SST MiB median [range] | Exit run-scope HDD allocation MiB median [range] |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| uniform | disabled | 3/3 | 112.00 [112.00, 112.00] | 194.30 [192.37, 194.88] | 10,240.0 [10,240.0, 10,240.0] | 10,240.0 [10,240.0, 12,288.0] | 21.64 [21.64, 21.64] | 2,088.54 [2,088.54, 2,088.54] |
| uniform | adaptive | 3/3 | 112.00 [112.00, 112.00] | 172.43 [170.05, 175.36] | 16,384.0 [16,384.0, 16,384.0] | 20,480.0 [20,480.0, 28,672.0] | 9.50 [9.50, 9.50] | 2,088.55 [2,088.55, 2,088.55] |
| switch | disabled | 3/3 | 112.00 [112.00, 112.00] | 227.30 [225.36, 228.76] | 10,240.0 [10,240.0, 10,240.0] | 10,240.0 [10,240.0, 10,240.0] | 21.64 [21.64, 21.64] | 2,088.54 [2,088.54, 2,088.54] |
| switch | adaptive | 3/3 | 112.00 [112.00, 112.00] | 201.30 [200.48, 204.60] | 16,384.0 [16,384.0, 16,384.0] | 20,480.0 [20,480.0, 32,768.0] | 9.50 [9.50, 9.50] | 2,088.55 [2,088.55, 2,088.55] |
| mixed | disabled | 3/3 | 112.00 [111.97, 112.00] | 186.14 [185.65, 189.52] | 24,576.0 [24,576.0, 24,576.0] | 98,304.0 [98,304.0, 262,144.0] | 21.67 [21.67, 21.67] | 2,089.29 [2,089.29, 2,089.29] |
| mixed | adaptive | 3/3 | 111.91 [111.76, 112.00] | 154.86 [154.80, 161.49] | 40,960.0 [40,960.0, 40,960.0] | 196,608.0 [163,840.0, 229,376.0] | 9.52 [9.52, 9.53] | 2,089.27 [2,089.27, 2,089.28] |

Service and response histograms remain separate for GET, PUT, and DELETE in `summary.json`; response latency includes queue delay. The table labels GET service and response P99 separately. `summary.md` is the unmodified Markdown output from frozen `node_summary.py`.

Space values are physical allocated bytes, not logical file sizes. The frozen summarizer deduplicates physical totals by device+inode; per-trial SSD/HDD space and run-scope totals are separate, and the latter includes retained/seed data. The JSON contains every repeat, observed min/max ranges, the paired disabled-minus-adaptive comparisons, and per-sample completeness/errors.

## Size of the observed effect

Adaptive reduced the **steady measured-phase SSD SST footprint** from 21.64 MiB to 9.50 MiB: 12.14 MiB, or 56.1%. The **sampled peak SSD SST footprint did not fall by 56%**: both modes have a 21.64 MiB median peak; paired peak differences have a 0 MiB median (0–0.063 MiB range), consistent with the initialized/transition footprint remaining present.

Run-scope physical global-unique space includes shared seed and retained data: about 2.1 GiB, 2,132.80 MiB disabled versus 2,120.67 MiB adaptive. The paired saving was 12.13 MiB median (12.13–13.14 MiB), about 0.57%; this is an experiment-footprint measure, not the per-trial business-data saving. Run-scope HDD physical allocation was 2,088.54 MiB in both modes; paired HDD differences were effectively zero (-8 KiB to +20 KiB). The existing recovery image is already present on HDD, so the HDD delta is not evidence that total stored data fell by 56%.

Per-trial exit allocation excludes the shared seed and reports the trial's SSD+HDD physical unique bytes. Disabled used 1,118,212,096 B median (1,066.41 MiB, about 1.041 GiB); adaptive used 1,105,494,016 B (1,054.28 MiB, about 1.030 GiB). The paired saving was 12,718,080 B median (12.13–13.14 MiB), about 1.14% of the disabled median. Per-trial HDD allocation was 1,094,983,680 B median disabled and 1,094,991,872 B adaptive; the paired HDD difference was -8 KiB to +20 KiB, effectively zero. This per-trial denominator describes the trial's roughly 1.04 GiB dataset and keeps the shared seed out of the business-data comparison.

## Adaptive mechanism and budget counters

Across the nine adaptive trials, mechanism coverage reports 28 measured migrations median (20–38) and 131 measured-window sample reads (111–182); all nine coverage records passed. The measured-phase controller `sampled_reads` counter is a separate count, with median 197 (177–274). The measured-phase counters reported 24 promotions (20–29), 36 demotions (32–41), and zero migration errors. Promoted bytes were 25,438,939 median (21,193,537–30,744,747); demoted bytes were 38,151,897 (33,909,450–43,460,834). `observed_promotions` and `observed_demotions` remained zero; these are separate counters from the completed promotion/demotion counters above.

`over_budget_micros` was 3.0 seconds median (2.0–4.0 seconds) during the 60-second measured phase: 5.0% median (3.33–6.67%). `last_point_lag_micros` was 12.27 seconds median (1.75–12.72); `recovery_points` was 1 median (1–64). The report has no explicit convergence flag or convergence threshold, so convergence status is **unknown**; the lag and recovery-point counters are reported as recorded, not used as proof of convergence. Measured-phase peak queued bytes were 15,619–21,295 B against the 2 MiB queue, with zero staging backpressure.

## Formal fault recovery and ACK interpretation

| Mode | Cut s | Last pre-fault ACK seq | Recovered marker / ACK seq | Lost acknowledged batches | Lost business bytes | RPO ms | RTO ms | First 60s read valid |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| disabled | 60 | 6721 | 6644 / 6644 | 77 | 80,080 | 688.163 | 42,935.227 | True |
| disabled | 75 | 8401 | 8306 / 8306 | 95 | 98,800 | 848.942 | 42,455.984 | True |
| disabled | 90 | 10081 | 10046 / 10046 | 35 | 36,400 | 313.223 | 43,459.466 | True |
| adaptive | 60 | 6721 | 6657 / 6657 | 64 | 66,560 | 572.316 | 43,063.628 | True |
| adaptive | 75 | 8401 | 8228 / 8228 | 173 | 179,920 | 1,545.395 | 43,822.867 | True |
| adaptive | 90 | 10081 | 10017 / 10017 | 64 | 66,560 | 572.131 | 42,713.155 | True |

`last_acked_seq` is the highest externally received node_v1 ACK before the injected cut. In all six cuts, the pre-fault ACK journal sync completed before the cut and its last external sequence matched `last_acked_seq`. `recovered_marker` is the database recovery marker; `last_recovered_ack` is the highest pre-fault ACK represented by that marker. Lost batches/bytes count pre-fault ACK records with seq greater than the recovered marker. RPO is measured from the last recovered ACK timestamp to the injected fault timestamp; RTO is taken from the recovered event. The standalone 10-second timing probe is excluded from these six formal samples.

The first-60-second recovery read completed 6,720 operations in each sample at approximately 112 ops/s with zero unfinished operations. P99 values below are histogram bucket upper bounds, not interpolated percentiles. Service and response remain separate; response includes queue delay.

| Mode | Cut s | Throughput ops/s | GET service P99 upper bound ms | GET response P99 upper bound ms |
|---|---:|---:|---:|---:|
| disabled | 60 | 112.00 | 12.288 | 163.840 |
| disabled | 75 | 112.00 | 14.336 | 196.608 |
| disabled | 90 | 112.00 | 12.288 | 196.608 |
| adaptive | 60 | 112.00 | 20.480 | 393.216 |
| adaptive | 75 | 112.00 | 24.576 | 917.504 |
| adaptive | 90 | 112.00 | 20.480 | 327.680 |

## Retained fault records and identity

There are 64 command records and 7 planned SIGKILL write records (one separate timing probe and six formal cuts). Each is recorded as `exit_code=-9`, `fault_injected=true`, `timed_out=false`, and `oom_kill_delta=0`; all six formal recovery commands exited successfully. Nonplanned invalid commands: 0.

- Binary SHA-256 at start and end: `2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76`.
- Frozen source manifest SHA-256: `3a9e91e86fbd2743d473ef09199b490f1341db7e84a83a3e0535a2a081cceda2`; source file count: 2427; RocksDB head: `89c50c289b89f6a763b7e9ba6c90451a50353bbc`.
- `node_run.py`: `e1effa72cfe4945e805bdb23cb01b18b4346bb9eb2fc81948afdda1bbee627ee`; `node_cache.py`: `6787add99b5869a5bb60d64c2d71b9466561c655d190c568e258587b4966baea`; frozen `node_summary.py`: `afb46e51d6e711928d77ddeba004777352702350190577d824c8498dc8bc0e89`.
- Original raw runner JSON SHA-256: `2a25a427a622c9e23c3f558c4cadf3ba9f71b8ef4168d638f3d081baa579eac6`. The full 324-file control archive totals 207,910,979 bytes and passed checksum dry-run against node-ssd.
- The full raw JSON, all stdout/stderr, ACK journals, per-second samples, process identity records and commands are in `raw-node/control/`. Failed/faulted command artifacts are preserved; no retries were run.
- Remote task root: `/home/lj/ssd/metabypass_test/sst-effects-small-final-20261003-2810448d/control`; data roots remain on node-ssd at the recorded SSD/HDD run roots and were not downloaded.
- Summary source input remaps only remote control-log paths to their local archive copies; raw `runner.json` remains unchanged. Mapping and both hashes are recorded in `metadata/summary-input-provenance.json`.

## Generated artifacts

- Frozen summarizer output: `summary.json` and `summary.md`.
- Calibration gate and I/O evidence: `metadata/calibration-evidence.json`.
- Downloaded-control SHA-256 manifest: `metadata/control-file-manifest.json`.
- Summary input path/hash mapping: `metadata/summary-input-provenance.json`.
- `summary-saturated_throughput_ops_s.png` / `.svg`.
- `summary-sampled_measured_median_ssd_sst_allocated_bytes.png` / `.svg`.
