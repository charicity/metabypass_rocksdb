# HDD sync comparison

This driver compares current MetaBypass with RocksDB BackupEngine using inline
values and SSTs. Build from this checkout with CMake and a C++20 toolchain.

```sh
cmake -S tools/metabypass_acceptance -B /tmp/mb-accept-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/mb-accept-build --target metabypass_acceptance --parallel 48
```

Pass fresh, disjoint directory paths for each arm. Place the backup directories
on the HDD you intend to measure. Both arms use the same default write format,
compression disabled, WAL enabled, shared table files where applicable, and
synchronous backup publication. BackupEngine keeps one backup after each purge.
The final checkpoint follows an explicit flush so the restore also exercises
SSTs. Leave `--sst-budget-bytes` unset for the matched comparison. Pass a
positive budget to an MB arm when measuring adaptive SST placement.

```sh
bin=/tmp/mb-accept-build/metabypass_acceptance
"$bin" --arm mb --index /SSD/run1/mb-index --backup /HDD/run1/mb-backup \
  --restore /SSD/run1/mb-restore --records 100000 --value-bytes 64 \
  --batch 8 --seed 31 --checkpoint-records 50000 > /tmp/mb-run1.json
"$bin" --arm be --index /SSD/run1/be-index --backup /HDD/run1/be-backup \
  --restore /SSD/run1/be-restore --records 100000 --value-bytes 64 \
  --batch 8 --seed 31 --checkpoint-records 50000 > /tmp/be-run1.json
```

Each record's key and full value are deterministic. Match the seed, count,
value size, batch size and checkpoint stride across runs. A stride of zero
requests only the final checkpoint. Running with `--batch 1` exercises the
Put API. Larger batches use WriteBatch. The tool leaves its artifacts in place.
Each arm restores into its own destination through the corresponding backup
API, opens that database and compares every key and value in order. Any extra
key, missing key or different value fails the run.

JSON reports load throughput, write API duration, sampled WAL and memtable
timings, flush and compaction counters, checkpoint latency, BackupEngine purge
latency, and restore and verification time. Load throughput includes intermediate
backup barriers and ends before the final flush and backup. Durable load
throughput includes the final flush and backup. Metabypass
checkpoints also retain their native periodic schedule. I/O counters cover the
index and backup paths, with call counts and durations for the backup path.
Metabypass additionally reports queue, mirroring and checkpoint counters.
`--sample-every 0` disables foreground PerfContext sampling. The default
samples every 64th write call starting with the first.

## WAL stages

Build in a separate directory when enabling WAL probes so the engine and
driver use the same compile-time setting.

```sh
cmake -S tools/metabypass_acceptance -B /tmp/mb-accept-fine \
  -DCMAKE_BUILD_TYPE=Release -DROCKSDB_MB_WAL_BREAKDOWN=ON
cmake --build /tmp/mb-accept-fine --target metabypass_acceptance --parallel 48
/tmp/mb-accept-fine/metabypass_acceptance --arm mb --fine-wal \
  --index /SSD/run2/mb-index --backup /HDD/run2/mb-backup \
  --restore /SSD/run2/mb-restore --records 100000 --value-bytes 64 \
  --batch 8 --seed 31 --checkpoint-records 50000 > /tmp/mb-run2.json
```

The same flag works with the BE arm and fresh paths. Fine mode instruments
every 16th foreground sample and reports raw per-stage counts, nanoseconds and
bytes. It also reports aggregate background stages for the instrumented load.
Stages may overlap. An overflow counter identifies discarded foreground traces.
Use the ordinary binary for throughput runs without the engine probe overhead.
