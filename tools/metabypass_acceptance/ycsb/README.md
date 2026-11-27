# MetaBypass and BackupEngine YCSB-cpp experiments

The Linux experiment uses [YCSB-cpp at 6c94fdb](https://github.com/ls4154/YCSB-cpp/tree/6c94fdb52350de66321ef5a8b529cf1b685e2338).
Upstream `CoreWorkload` generates the initial data and runs the A–F
transactions. All engines use the same single-field adapter. The raw field
occupies exactly `fieldlength` bytes in RocksDB. Update replaces that field.
Workload F calls Read and then Update for one logical operation.

## Build

Use CMake 3.18 or newer, a C++20 compiler, OpenSSL development headers,
Git, `patch` and Python 3.8 or newer. The build applies a small lock fix to
the upstream latest-key generator for concurrent workload D. It stores the
patch digest and leaves the checkout available as the source of A–F profiles.

```sh
tools_dir=tools/metabypass_acceptance/ycsb
build=/path/to/ycsb-build
cmake -S "$tools_dir" -B "$build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$build" --parallel 48
ctest --test-dir "$build" -j8 --output-on-failure
ycsb_src="$build/_deps/ycsb_cpp-src"
```

Use `-DFETCHCONTENT_SOURCE_DIR_YCSB_CPP=/path/to/checkout` for a local
checkout at the pinned commit. For a historical core, keep this experiment
source and pass `-DROCKSDB_SOURCE_DIR=/path/to/core`. When using a Git source
archive, pass its full `-DROCKSDB_CORE_COMMIT=...`. An existing compatible
strict-Status shared library can be selected with
`-DROCKSDB_LIBRARY=/path/to/librocksdb.so`. Match it to the core headers. The
runner checks the loaded library's digest, records its path and checks its
embedded commit when the library supplies one.

## Generate once and reuse

Create one fixture for each initial record count and value size. Archive
`dataset.bin` and the JSON from the generation command for subsequent runs.

```sh
"$build/metabypass_ycsb" dataset /path/to/datasets/1kib 1048576 1024
```

The command calls upstream Load in one thread, recording its output in
insertion order. The schema has one constant-size field, hashed keys with the
`user` prefix and 20 padded decimal digits, and insert start zero. Each
sample replays the file into a fresh database. After Flush, it checks every
key and the complete value in bytewise order and checks for extra records.
The ready event stores the fixture SHA-256, observed logical SHA-256, count
and native sequence. This verification is part of preparation outside the
measurement window. Reuse the exact file for every compared core, engine
and round, including different workloads and thread counts using the same
data specification. The timed transaction streams use upstream randomness.

## Run and pair

```sh
python3 "$tools_dir/run.py" run \
  --driver "$build/metabypass_ycsb" \
  --dataset /path/to/datasets/1kib/dataset.bin \
  --workload "$ycsb_src/workloads/workloada" \
  --engine mb --sample mb40-A-1kib-16t-r1 \
  --ssd-root /path/to/ssd/task --hdd-root /path/to/hdd/task \
  --threads 16 --checkpoint-ms 40000 --recover

python3 "$tools_dir/run.py" pair \
  /path/to/hdd/task/mb40-A-1kib-16t-r1/result.json \
  /path/to/hdd/task/be40-A-1kib-16t-r1/result.json
```

Use `--engine be` for BackupEngine and `--engine native` for RocksDB without
backups. The controller defaults to 65 seconds of warmup, 180 seconds of
measurement and 4096 distinct recovery samples. MB uses a 1-second mirror and
the 512 MiB threshold with 896 MiB WAL and 128 MiB metadata queues. Both backup
engines retain one published point. All arms use three 128 MiB write buffers,
a 256 MiB cache, four background jobs, no compression and asynchronous WAL
writes. Blob, staging and tiering are disabled.

The two roots must exist and each sample gets new directories. For a node-ssd
run, pass `--cgroup /sys/fs/cgroup/memory/mbgc-smallmem --memory-gib 32` to
use the existing cgroup v1 group. The controller stays outside that group and
records memory usage and process swap observations. It checks the configured
limit, group membership, swappiness and SSD/HDD free space. The result contains
the commands, JSONL events, process status, LOG and per-stage metrics.
Successful samples remove their database files by default. Failures keep their
directories and logs, and `--keep-data` also keeps successful scenes.

The controller sends SIGKILL after the writer reports the measurement window.
Throughput counts successful operations completed in the window and visible
when the driver snapshots the counters. Latency
percentiles are logical-operation histogram bucket upper bounds. The adapter
also reports Get, Put and Scan calls and value bytes.

With `--recover`, the controller isolates the SSD foreground directory and
restores into a fresh destination from the original HDD backup using the
existing page-cache state. Recovery reports the Restore, Open and sampled
verification times. For workloads C, D and E, original records retain their
initial values, so sampled original keys are checked byte for byte. Recovery
exactly at the initial sequence allows the same check for all workloads.
Otherwise A, B and F samples verify readability and width for the initial
keys. Recovered update bytes have no independent oracle in that case.

Pairing requires matching fixture and observed digests, successful initial
verification, and matching YCSB revision and patch, adapter, workload, shared
database settings, memory budget and measurement window. Engine, core and
backup settings are comparison variables. Historical hand-written YCSB
ledgers are outside this protocol.

## YCSB-A and YCSB-D checkpoint SST study

```sh
python3 "$tools_dir/sst_checkpoint_study.py" run \
  --driver "$build/metabypass_ycsb" --ldb "$build/rocksdb/tools/ldb" \
  --dataset /path/to/datasets/1kib/dataset.bin \
  --workload "$ycsb_src/workloads/workloada" --output /tmp/ycsb-a-sst-r1

python3 "$tools_dir/sst_checkpoint_study.py" summarize \
  --ldb "$build/rocksdb/tools/ldb" --output /tmp/ycsb-a-sst-r1
```

Use an `ldb` from the same core build as the driver. For the integrated build,
configure CMake with `-DWITH_CORE_TOOLS=ON` and build the `ldb` target. Use a
new results directory on a local filesystem with room for the kept foreground
and backup data.
Defaults are 16 threads, 65 seconds of warmup, 180 seconds of measurement and
40 seconds between checkpoints. Override them with `--threads`,
`--warmup-seconds`, `--measure-seconds` and `--checkpoint-ms`.
Use the upstream `workloadd` profile as `--workload` and a new output
directory for Workload D.

The observer archives checkpoint metadata in `points/`. `levels.csv` lists
each level, SST totals, new file numbers and same-number moves. Measurement
totals use intervals ending at a confirmed measurement-window publication.
Differences across a MANIFEST rollover still appear in the level data.
`points.csv` gives WAL and metadata sizes and tracks whether the MANIFEST
prefix covers short-lived SSTs for each interval. `summary.json` gives byte
shares for L1+ and L2+ and lists any final point unconfirmed by the writer.
All sizes are logical file bytes.

Pass `--front-root` and `--back-root` together to use two existing data
directories on local mounts. Each run gets unique sample directories. Raw
writer logs are archived under `back/ycsb-a/` for Workload A and
`back/ycsb-d/` for Workload D. Pass `--cgroup` and `--memory-gib` to use the
runner's existing cgroup v1 checks and declared memory limit. Pass
`--cleanup-data` to remove this run's foreground and backup data after
successful collection and summarization.

Use `--sst-async-min-level -1` for the default with staging disabled. Set
`--sst-async-min-level 5 --sst-async-delay-ms 1000` to stage compaction outputs
at level 5 or higher after a one-second delay. The same two flags apply to
`run.py run --engine mb`. The runner passes only these settings via the
per-process `MB_YCSB_SST_ASYNC_MIN_LEVEL` and
`MB_YCSB_SST_ASYNC_DELAY_MS` environment overrides. Recovery opens with
staging disabled. For db_bench, the corresponding flags are
`--metabypass_sst_async_min_level` and `--metabypass_sst_async_delay_ms`.

The writer JSONL includes backup snapshots in the initial backup, periodic
progress, and measurement boundary events. The observer adds the snapshots
to `points.csv` by published point count and records the actual sampling
timestamps. In `summary.json`, `measurement_backup.counter_deltas` uses the
two measurement boundary snapshots and `p99_ns` is the YCSB histogram bucket
upper bound. Checkpoint copy bytes are successful SST Append bytes attributed
to each confirmed candidate. Ready bytes came from staging that had already
synchronized before that candidate began. The overall queue peak upper bound
is the sum of the WAL and metadata high-water marks. The sampled WAL queue
maximum has a separate field. Cancelled bytes cover admitted jobs evicted
before I/O. The MANIFEST analysis covers transient SSTs between publications.
