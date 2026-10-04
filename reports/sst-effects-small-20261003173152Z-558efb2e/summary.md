# Node SSD/HDD SST tiering results

Status: `blocked_or_failed`; host: `8001`.

| Workload | Mode | Valid repeats | Fixed ops/s median [range] | Saturated ops/s median [range] | Get response P99 us median [range] | Steady SSD SST MiB median |
|---|---|---:|---:|---:|---:|---:|
| uniform | disabled | 1/3 | 107.0 [107.0, 107.0] | 189.8 [189.8, 189.8] | 10,240.0 [10,240.0, 10,240.0] | 21.6 |
| uniform | adaptive | 1/3 | 107.0 [107.0, 107.0] | 166.3 [166.3, 166.3] | 40,960.0 [40,960.0, 40,960.0] | 9.5 |
| switch | disabled | 1/3 | 107.0 [107.0, 107.0] | 192.2 [192.2, 192.2] | 20,480.0 [20,480.0, 20,480.0] | 21.6 |
| switch | adaptive | 0/3 | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a |
| mixed | disabled | 0/3 | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a |
| mixed | adaptive | 0/3 | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a [n/a, n/a] | n/a |

## Fault recovery samples

| Mode | Cut s | Valid | ACK seq | Recovered marker | Lost batches | Lost bytes | RPO ms | RTO ms |
|---|---:|---|---:|---:|---:|---:|---:|---:|

## Retained failed or faulted commands

| Name | Exit | Timeout | Parse/phase errors |
|---|---:|---|---|
| recovery-probe-write | -9 | False | [] |
| trial-1-switch-adaptive | -9 | False | [&#x27;missing/duplicate phase_end: warmup&#x27;, &#x27;missing/duplicate phase_end: measured&#x27;, &#x27;missing/duplicate phase_end: saturated&#x27;] |

Run error: small formal correctness/timeout failure; sample retained

## Measurement limits

- Phase P99 uses cumulative histogram bucket upper bounds; no percentile averaging.
- Response latency includes queue delay for completed requests; unfinished_ops must be read alongside latency.
- Hot/cold groups and placement counters are proxies, not per-request device attribution.
- Device I/O includes blob and SST traffic and cannot separate them.
- Cache gate uses globally routed live SST residency, not request miss rate; cache dominated and missing/partial evidence are distinct.
- Small uses measured-phase per-trial cache gates; a single 10s adaptive recovery timing probe is separate from six formal fault samples with 60s first reads.
- Calibration cache gate requires both modes' post-warmup routed SST mincore samples with >=90% live-byte coverage and <=90% resident fraction; anonymous RSS/cache capacity is supplementary only.
- Primary physical space covers each trial SSD+HDD with global inode deduplication; run_scope separately includes seed/active/retained failures for experiment peak and free-space monitoring.
- Space saving is null for failed or incomplete samples; physical totals deduplicate dev+inode.
- RPO has six fault samples only; report all samples and ranges, not P99.

Commands, source identity, configuration, every failure/timeout and detailed trial histograms are preserved in the JSON summary and raw report.
