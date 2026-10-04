# Node SSD/HDD SST tiering results

Status: `complete`; host: `8001`.

| Workload | Mode | Valid repeats | Fixed ops/s median [range] | Saturated ops/s median [range] | Get response P99 us median [range] | Steady SSD SST MiB median |
|---|---|---:|---:|---:|---:|---:|
| uniform | disabled | 3/3 | 112.0 [112.0, 112.0] | 194.3 [192.4, 194.9] | 10,240.0 [10,240.0, 12,288.0] | 21.6 |
| uniform | adaptive | 3/3 | 112.0 [112.0, 112.0] | 172.4 [170.1, 175.4] | 20,480.0 [20,480.0, 28,672.0] | 9.5 |
| switch | disabled | 3/3 | 112.0 [112.0, 112.0] | 227.3 [225.4, 228.8] | 10,240.0 [10,240.0, 10,240.0] | 21.6 |
| switch | adaptive | 3/3 | 112.0 [112.0, 112.0] | 201.3 [200.5, 204.6] | 20,480.0 [20,480.0, 32,768.0] | 9.5 |
| mixed | disabled | 3/3 | 112.0 [112.0, 112.0] | 186.1 [185.6, 189.5] | 98,304.0 [98,304.0, 262,144.0] | 21.7 |
| mixed | adaptive | 3/3 | 111.9 [111.8, 112.0] | 154.9 [154.8, 161.5] | 196,608.0 [163,840.0, 229,376.0] | 9.5 |

## Trial kernel-memory and CPU evidence

| Trial | Effective helper CPUs | Baseline kmem MiB | Sampled peak kmem MiB | Increase MiB | Peak FDs | Peak threads |
|---|---|---:|---:|---:|---:|---:|
| trial-1-uniform-disabled | 0,1,2,3 | 5.1 | 7.2 | 2.1 | 59.0 | 7.0 |
| trial-1-uniform-adaptive | 0,1,2,3 | 5.1 | 7.2 | 2.2 | 60.0 | 9.0 |
| trial-1-switch-disabled | 0,1,2,3 | 5.3 | 7.7 | 2.4 | 59.0 | 7.0 |
| trial-1-switch-adaptive | 0,1,2,3 | 5.5 | 7.8 | 2.4 | 60.0 | 9.0 |
| trial-1-mixed-disabled | 0,1,2,3 | 5.5 | 7.7 | 2.2 | 64.0 | 7.0 |
| trial-1-mixed-adaptive | 0,1,2,3 | 5.9 | 8.3 | 2.4 | 64.0 | 9.0 |
| trial-2-uniform-adaptive | 0,1,2,3 | 6.7 | 8.3 | 1.6 | 60.0 | 9.0 |
| trial-2-uniform-disabled | 0,1,2,3 | 7.2 | 8.6 | 1.4 | 59.0 | 7.0 |
| trial-2-switch-adaptive | 0,1,2,3 | 7.0 | 8.9 | 1.9 | 60.0 | 9.0 |
| trial-2-switch-disabled | 0,1,2,3 | 7.1 | 8.6 | 1.5 | 59.0 | 7.0 |
| trial-2-mixed-adaptive | 0,1,2,3 | 7.1 | 9.5 | 2.4 | 65.0 | 9.0 |
| trial-2-mixed-disabled | 0,1,2,3 | 8.0 | 9.8 | 1.8 | 64.0 | 7.0 |
| trial-3-uniform-disabled | 0,1,2,3 | 7.8 | 9.0 | 1.2 | 59.0 | 7.0 |
| trial-3-uniform-adaptive | 0,1,2,3 | 7.3 | 9.0 | 1.7 | 60.0 | 9.0 |
| trial-3-switch-disabled | 0,1,2,3 | 7.4 | 9.0 | 1.6 | 59.0 | 7.0 |
| trial-3-switch-adaptive | 0,1,2,3 | 7.4 | 9.3 | 1.9 | 61.0 | 9.0 |
| trial-3-mixed-disabled | 0,1,2,3 | 7.9 | 9.9 | 2.0 | 65.0 | 7.0 |
| trial-3-mixed-adaptive | 0,1,2,3 | 7.5 | 9.7 | 2.2 | 65.0 | 9.0 |

## Fault recovery samples

| Mode | Cut s | Valid | ACK seq | Recovered marker | Lost batches | Lost bytes | RPO ms | RTO ms |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| disabled | 60 | True | 6721 | 6644 | 77 | 80080 | 688.2 | 42,935.2 |
| disabled | 75 | True | 8401 | 8306 | 95 | 98800 | 848.9 | 42,456.0 |
| disabled | 90 | True | 10081 | 10046 | 35 | 36400 | 313.2 | 43,459.5 |
| adaptive | 60 | True | 6721 | 6657 | 64 | 66560 | 572.3 | 43,063.6 |
| adaptive | 75 | True | 8401 | 8228 | 173 | 179920 | 1,545.4 | 43,822.9 |
| adaptive | 90 | True | 10081 | 10017 | 64 | 66560 | 572.1 | 42,713.2 |

## Retained failed or faulted commands

| Name | Exit | Timeout | Parse/phase errors |
|---|---:|---|---|
| recovery-probe-write | -9 | False | [] |
| rpo-disabled-60-write | -9 | False | [] |
| rpo-disabled-75-write | -9 | False | [] |
| rpo-disabled-90-write | -9 | False | [] |
| rpo-adaptive-60-write | -9 | False | [] |
| rpo-adaptive-75-write | -9 | False | [] |
| rpo-adaptive-90-write | -9 | False | [] |

## Measurement limits

- Phase P99 uses cumulative histogram bucket upper bounds; no percentile averaging.
- Response latency includes queue delay for completed requests; unfinished_ops must be read alongside latency.
- Hot/cold groups and placement counters are proxies, not per-request device attribution.
- Device I/O includes blob and SST traffic and cannot separate them.
- Kmem/cache/rss are separate evidence: memory.stat cache/rss does not cover total charges. Missing kmem is unknown; an empty tasks file does not reset residual charges.
- Small helper affinity defaults to CPUs 0,1,2,3 before cgroup entry; legacy inherits. Per-thread last CPU and FD counts are read-only sampled diagnostics.
- Cache gate uses globally routed live SST residency, not request miss rate; cache dominated and missing/partial evidence are distinct.
- Small uses measured-phase per-trial cache gates; a single 10s adaptive recovery timing probe is separate from six formal fault samples with 60s first reads.
- Calibration cache gate requires both modes' post-warmup routed SST mincore samples with >=90% live-byte coverage and <=90% resident fraction; anonymous RSS/cache capacity is supplementary only.
- Primary physical space covers each trial SSD+HDD with global inode deduplication; run_scope separately includes seed/active/retained failures for experiment peak and free-space monitoring.
- Space saving is null for failed or incomplete samples; physical totals deduplicate dev+inode.
- RPO has six fault samples only; report all samples and ranges, not P99.

Commands, source identity, configuration, every failure/timeout and detailed trial histograms are preserved in the JSON summary and raw report.
