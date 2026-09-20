# Channel versions versus backup disabled

This is a fresh four-version comparison, not an extrapolation from the earlier
200,000-key db_bench measurements. All versions use separated Blob Direct Write,
`min_blob_size=0`, retained blobs, WAL enabled, `sync=false`, no compression,
one writer and 1 KiB values. Each measured run writes 100,000 keys. Five trials
per workload/version follow 10,000-key warmups, with version order rotated.
Builds and tests do not overlap timing runs. Host cache and hypervisor noise
are not controlled; small differences with overlapping ranges are inconclusive.

- `baseline`: `SeparatedStorage` plus raw RocksDB DB, without a Backup instance,
  event interception, backup queues, mirror worker or recovery-point publisher.
- `current`: notification-optimized single-operation-lock, single-queue version.
- `split`: experimental separate WAL/metadata operation locks, one queue.
- `channels`: formal separate WAL/metadata channel objects, sharing capacity and
  one globally ordered mirror worker.

The three backup binaries are the exact binaries used in the preceding channel
comparison; their hashes are preserved in the raw results. The baseline source
is generated from the same latency driver by replacing the Metabypass wrapper
with raw DB operations through the separated-storage Env. Its loop, key/value
generation, per-Put clocks, latency storage and event listeners are unchanged.
Close includes DB Close and DB destruction in both baseline and backup modes.
Baseline has no backup synchronization; the measured no-op timing is effectively
zero. Initial Open is excluded in all versions.

Default uses decimal keys, the default memtable settings and a 64 MiB backup
budget. Mixed uses 128-byte keys, a 256 KiB write buffer and target SST size,
1 MiB level base, L0 trigger 2 and four background jobs. Pressure is mixed with
a 2 MiB backup budget. The baseline has no queue, so its mixed and pressure
configurations are identical; they are measured separately alongside their
respective controls to capture run-to-run variation.

Backup total time includes constructing a validated recovery point; baseline
total does not. Their ratio quantifies the additional service cost, not equal
recovery guarantees. Total is calculated for each run before taking the median,
so it need not equal the sum of separate phase medians.

After each backup write, the runner deletes the whole primary index and restores
with the old notification reader, verifies every value, appends and reopens.
Baseline verification retains its primary index and checks all values, append
and reopen; it is not counted as backup recovery. SIGKILL recovery cases run only
for backup versions, after confirmed SyncBackup. This is a directory/process
crash experiment, not real power-loss qualification.

Reproduction, using the compatible release archive and saved channel controls:

```sh
python3 docs/components/metabypass/experiments/scheduling-ab/no_backup.py \
  --controls /tmp/mb-channels-ab --out /tmp/mb-channels-no-backup \
  --trials 5 --output /tmp/mb-channels-no-backup-results.json
```

The generator records the baseline source hash, binary hash and exact link
command. It reuses the matching utility objects and release link flags from
`build.json`. Production sources are unchanged by this experiment.

## Results

All values are five-trial medians. Positive overhead means more elapsed time
than the no-backup baseline in that workload. Raw samples, ranges, exact commands
and binary identities are in
[`channel-no-backup-results.json`](experiments/channel-no-backup-results.json).

| Workload | Version | Foreground ms | Foreground overhead | Put p99 us | Total ms | Total overhead |
|---|---|---:|---:|---:|---:|---:|
| default | baseline | 741.744 | +0.0% | 14.019 | 944.893 | +0.0% |
| default | current | 845.387 | +14.0% | 18.721 | 1795.735 | +90.0% |
| default | split | 889.200 | +19.9% | 19.795 | 1865.378 | +97.4% |
| default | channels | 843.027 | +13.7% | 19.163 | 1775.714 | +87.9% |
| mixed | baseline | 1986.457 | +0.0% | 29.361 | 2015.032 | +0.0% |
| mixed | current | 2222.592 | +11.9% | 39.624 | 2786.207 | +38.3% |
| mixed | split | 2186.276 | +10.1% | 33.287 | 2773.582 | +37.6% |
| mixed | channels | 2196.186 | +10.6% | 35.003 | 2772.633 | +37.6% |
| pressure | baseline | 1990.346 | +0.0% | 29.162 | 2021.083 | +0.0% |
| pressure | current | 2196.006 | +10.3% | 38.178 | 2802.528 | +38.7% |
| pressure | split | 2192.578 | +10.2% | 33.527 | 2750.370 | +36.1% |
| pressure | channels | 2187.028 | +9.9% | 33.626 | 2782.373 | +37.7% |

Channels still add **13.7% / 10.6% / 9.9% foreground elapsed time** for default /
mixed / pressure versus backup disabled. Total elapsed increases **87.9% /
37.6% / 37.7%**. Put p99 increases **36.7% / 19.2% / 15.3%**. Splitting the
channels does not remove payload copying, primary-file interception, shared
coordination, memory reservations or competition with backup I/O. Sync and
Close also have to complete validated recovery-point publication.

The improvements measured against the old notification implementation do not
mean backup is free or faster than disabling it. This fresh series again shows
mixed-workload tail-latency benefits over notification-only, but not a consistent
win over the experimental split-lock version. Small aggregate differences and
changing rankings across runs should not be read as a universal speedup.

All **60 measured writes** and 12 warmups passed. Backup verification passed
**57/57** times (45 measured writes, nine warmups, three post-Sync SIGKILL
writers). Baseline primary-content/append/reopen verification passed **18/18**
times (15 measured and three warmups). Baseline checks retain the primary index
and do not demonstrate recovery after its loss. Python syntax checks and
`git diff --check` passed; no production implementation was changed in this
comparison, so the full repository suites were not rerun.
