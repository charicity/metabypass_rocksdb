# Pipelined and incremental validation

This change separates mirror consumption from candidate validation and avoids
rechecking immutable content on every publication. The production backup
format, pointer protocol and offline corruption checks remain unchanged.

## Implementation and invariants

The mirror captures one independent candidate at an applied event boundary.
Mutable index files are copied; closed SSTs are pinned by hardlink or copied
when links are unavailable. A separate validator selects complete native
records, validates dependencies and publishes. The mirror can continue through
flush, compaction and deletion while those pinned files are checked. At most
one candidate is being captured/validated, so a slow validator coalesces later
mirror states instead of accumulating candidate directories.

The queue mutex is never held across backup filesystem I/O. A separate
publication mutex orders pointer commit with recording sticky errors;
successful foreground producers do not acquire it. Both worker threads are
joined at close. A sync waiter is satisfied only after its event boundary has
been validated and published. Shutdown rejects an incomplete final state.

SST reference/checksum caches are keyed by file identity and length. File
creation, truncation and rename invalidate identity. WALs retain counts and
blob dependencies of already checked complete records; native WAL parsing and
checksums are still performed, but old records do not repeat blob payload
validation. Blob caches retain a verified record boundary, record count and
prefix CRC, and extend over new records or a footer during persistence
validation. First-time validation of a new SST still checks all its blob
references, even when those values previously appeared in a WAL. Inventory CRCs for index
files also extend over appended bytes. Deleted index files are removed from
caches; blob metadata follows the intentionally retained blob files.

The cache lifetime is one open instance. Restore checks all bytes against the
published inventory, and reopening starts with empty caches. This assumes
exclusive ownership: immutable SSTs and already written blob prefixes must not
be modified externally. Old media corruption is not proactively scrubbed on
every publication; the original expected CRC is preserved so offline recovery
still rejects it. This differs from a periodic full scrub and is documented in
the component guide.

## Functional validation

The final suite has 20 tests, including the original 16 recovery, corruption,
queue, SIGKILL and retry tests. Four new tests cover:

- A blocked validator while the mirror drains more WAL bytes than the whole
  queue capacity, followed by flush/compaction, bounded point retention and
  restore of pinned data.
- A mirror failure during blocked validation waking sync waiters and preventing
  pointer replacement.
- One appended record validating only its new blob tail, unchanged SSTs reusing
  validation, multi-buffer payloads, empty keys/values, and successful restore
  with appended data.
- Corruption of a new blob tail after an earlier validated prefix being rejected.

Normal and `ASSERT_STATUS_CHECKED=1` builds passed all 20 tests. Each test
process is capped at 60 seconds. The cache tests use synchronization and
counters rather than timing assertions. The complete suite also passed 100
repetitions per test with forced context switching (2000 successful executions).
After test-helper deduplication and large/empty-value coverage refinements,
400 focused repetitions also passed, and all 20 final tests passed again in
normal and status-checking builds. No sanitizer or non-Linux run is claimed.

## Repository checks

The ordinary full `make check` completed 3320 shards with 29 failures: two
previously reproduced Bloom filter expectation failures, 26 external-SST
nonzero-sequence ingestion shards and `c_test`. The latter 27 failed on
`/dev/shm` with direct-I/O `Invalid argument` errors. A representative ingestion
failure was reproduced with the old baseline library excluding Metabypass.
All 32 cases in that ingestion parameter group and `c_test` then passed on
ext4 under `/tmp`; the remaining ordinary-suite failures are the two known
Bloom tests. The prefetch test passed in this tmpfs run.

The status-checking full suite completed 3301 shards on ext4 with the same
three previously reproduced baseline failures: the two Bloom filter tests
and `PrefetchTest/PrefetchTest.Basic/0`. There were no unchecked-Status failures.
The Metabypass suite was also rebuilt and rerun separately after the full
suite to include the final test-helper and large-value coverage refinements.

Python checks, `ldb_test.py`, `db_crashtest_test.py`, `rocksdb_dump_test.sh`,
source checks, formatting, BUCK target regeneration and diff whitespace
checks passed. Workflow YAML validation still lacks Ruby. C API link completeness passed (1745 functions),
but compatibility against `main` still reports the branch's existing missing
21 functions and 5 enum/typedef symbols. The C API generator/source is unchanged
by this work. Neither full check is described as all-green.

## Benchmark reproduction

Use three release binaries built with
`AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j8 db_bench`: baseline from `e7663cf90`,
the final implementation, and a final implementation build with
[the full-scan comparison patch](experiments/pipeline_full_scan.patch).
That comparison keeps the same two-thread pipeline and restores full content
validation at every point. It produces valid backups, unlike the earlier
[scan-removal ablation](experiments/README.md).

Run [run_pipeline_benchmark.py](experiments/run_pipeline_benchmark.py) with
`--baseline`, `--pipeline`, `--incremental` pointing to those binaries and
`--output` naming a JSON result file. It rotates execution order and uses five
trials per version/workload, fresh directories and a 1000-write warmup. All
subprocesses have a 60-second timeout.

Workloads use 50,000 and 200,000 inserts with the 64 MiB default queue, and
50,000 inserts with a 2 MiB queue / 64 KiB trigger. Values are 1024 bytes,
keys are decimal strings, with one writer, no compression, WAL enabled and
`sync=false`. Each measured run is closed, its entire primary index is deleted,
and backup plus blobs are used for restore and full key/value verification.
Incremental points are restored with the original baseline reader to check
format compatibility, then verified with the incremental implementation.

Builds and tests finish before timing. The environment is the same Ubuntu
22.04 KVM guest / ext4 volume described in the original validation record;
cache state and interference outside this task are not controlled. These are
short directory-based runs, not a sustained-load or power-failure guarantee.

## Measured results (2026-09-20)
Medians of five trials, in milliseconds. "Pipeline" retains full scans;
"Incremental" is the final production implementation. Total is the median of
each run's foreground + explicit backup sync + close sum, excluding Open.
The final point after write/close includes first-time validation of the newly
flushed SST, so it is not an unchanged-SST cache-hit measurement.

### 50k_default

| Metric (ms) | Original | Pipeline | Incremental |
| --- | ---: | ---: | ---: |
| Foreground writes | 429.416 | 596.551 | 610.556 |
| Explicit backup sync | 582.877 | 632.514 | 190.049 |
| Close | 696.569 | 764.974 | 404.559 |
| Final point build | 321.010 | 348.044 | 277.980 |
| Final point lag | 663.923 | 732.190 | 371.605 |
| Backpressure | 0.000 | 0.000 | 0.000 |
| Write + sync + close | 1716.752 | 2008.634 | 1210.937 |

Peak charged queue bytes (original / pipeline / incremental): 3,779,566 / 1,080,557 / 1,080,557.

### 200k_default

| Metric (ms) | Original | Pipeline | Incremental |
| --- | ---: | ---: | ---: |
| Foreground writes | 1812.140 | 2546.488 | 2559.750 |
| Explicit backup sync | 2319.042 | 1514.757 | 283.338 |
| Close | 2350.857 | 2385.740 | 1065.481 |
| Final point build | 1117.841 | 1139.893 | 911.260 |
| Final point lag | 2302.624 | 2334.188 | 1013.695 |
| Backpressure | 0.000 | 0.000 | 0.000 |
| Write + sync + close | 6499.713 | 6407.957 | 3918.148 |

Peak charged queue bytes (original / pipeline / incremental): 16,334,973 / 3,285,695 / 2,097,444.

### 50k_pressure

| Metric (ms) | Original | Pipeline | Incremental |
| --- | ---: | ---: | ---: |
| Foreground writes | 773.536 | 601.920 | 636.580 |
| Explicit backup sync | 607.206 | 594.593 | 195.523 |
| Close | 688.205 | 753.048 | 404.634 |
| Final point build | 315.442 | 342.240 | 272.151 |
| Final point lag | 654.624 | 719.523 | 371.604 |
| Backpressure | 324.890 | 0.000 | 0.000 |
| Write + sync + close | 2068.585 | 1940.791 | 1267.075 |

Peak charged queue bytes (original / pipeline / incremental): 2,097,095 / 1,080,557 / 1,080,557.

### Reusing already validated SSTs

The subsequent verify phase reopens the restored DB, reads all values, and
closes it. Open validates the existing SST with an empty cache; the final
close point then reuses that unchanged SST. Its point-build medians are:

| Workload | Original (ms) | Pipeline (ms) | Incremental (ms) |
| --- | ---: | ---: | ---: |
| 50k_default | 323.253 | 348.167 | 65.804 |
| 200k_default | 1116.464 | 1135.048 | 66.998 |
| 50k_pressure | 317.141 | 342.632 | 65.582 |

This corresponds to about 80--94% less point-build time when reusing an already
validated SST. Every incremental verify run reported `reused_tables=1`. This
is an in-process cache-hit result; a cold open still validates the data.

Across the write phases, the final implementation reduces write + sync + close
by 29.5%, 39.7% and 38.7%, respectively. With the 2 MiB queue it removes the
observed 324.890 ms median backpressure and reduces foreground time by 17.7%.

There is a material tradeoff: with the default queue and no backpressure,
foreground time increases by 41--42%. The pipeline alone is also slower on
that metric. These single-volume runs do not demonstrate universal foreground
speedup; parallel backup work competes with the foreground, and the experiment
does not isolate CPU, locking and device/journal contributions. The stronger
results are reduced backup completion time, queue occupancy and repeated
content-scan work. Further tuning would require profiling rather than claiming
all writes got faster.

`validated_blob_bytes` was 53,038,952 for every 50k write and 212,288,952 for
every 200k write in the incremental version: the persistence-validation pass
advanced over the retained blob data once. This counter excludes separate
reference checks for new WAL records and new SSTs and is not total read I/O.
All variants mirrored 2,876,871 index bytes for 50k writes and 11,631,177 bytes
for 200k writes. Neither pipeline removes validation or generates the unsafe
inventory marker used by the earlier ablation.

All 45 measured writes, 45 complete-index-loss restores and 45 full value
verifications succeeded, as did the three warmups. All 15 incremental backups
were restored by the original reader, then reopened and verified by the new
implementation. Restore timings in the raw data therefore use the original
reader for that variant and must not be presented as incremental-restore
performance. No blob payload file was copied into any backup directory.

[Raw results](experiments/pipeline-results.json) contain commands, output,
per-trial values/ranges, binary hashes and production/test source fingerprints.
The benchmark runner finished with exit code 0. No production sources were
changed to run the full-scan comparison; its patch was applied only in a
separate release build and that build was restored afterwards.
