# Metabypass validation record

This records the Linux directory-based prototype validation on 2026-09-20.
It does not establish power-loss durability or equivalence to Ceph recovery.

## Functional and concurrency coverage

The final `metabypass_test` contains 16 tests. All pass in the normal debug
build. All 16 also passed 100 repetitions each with
`COERCE_CONTEXT_SWITCH=1` (1600 successful executions). Each test process has
a 60-second timeout.

Coverage includes normal close, complete primary-index deletion, restore,
append and reopen; unflushed blob references after SIGKILL; atomic fragmented
WAL batches; overwrite and deletion; compaction and file rotation; concurrent
write/flush/sync; queue capacity and ordering; foreground progress while backup
is blocked; failure waking a blocked writer; sticky errors with reads still
available; retention of the preceding point; corruption in published index
files, blob payloads and footers; blob-header mismatch; interruption at point
publication stages; retry after interrupted blob sealing; and preservation of
filesystem append verification information. Expected application values are
test data, never an input to recovery.

Representative commands, in separate build directories to avoid mixed flags:

```sh
AUTO_CLEAN=1 make -j8 metabypass_test
build_tools/gtest-parallel ./metabypass_test --timeout_per_test=60 --workers=8
AUTO_CLEAN=1 COERCE_CONTEXT_SWITCH=1 make -j8 metabypass_test
build_tools/gtest-parallel ./metabypass_test --timeout_per_test=60 --workers=8 -r100
AUTO_CLEAN=1 make -j12 J=24 DRIVER='timeout 60' check
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j8 J=16 DRIVER='timeout 60' check
```

## Repository checks

The normal full check completed 3320 test shards, with three failing shards:

| Test | Observed failure |
| --- | --- |
| `DBBloomFilterTest.MutatingRibbonFilterPolicy` | Expected 7 bits/key; observed 8.248, tolerance 0.3 |
| `DBBloomFilterTest.MutableFilterPolicy` | Same bits/key mismatch |
| `PrefetchTest/PrefetchTest.Basic/0` | Prefetch size about 1.319 million versus threshold 1.887 million, followed by a segmentation fault during failure cleanup |

All three failures were independently reproduced using a shared library linked
from the unchanged core objects, excluding every new Metabypass object. `ldd`
confirmed the baseline library was selected. These failures were not changed
as part of this implementation; the full suite is not reported as green.

The `ASSERT_STATUS_CHECKED=1` full check completed 3301 shards and failed
the same three tests, with the same Bloom and prefetch values. Its test
selection differs from the normal build. The final Metabypass-specific suite
was rebuilt separately after this run to include the latest header-validation
test; all 16 final tests passed with status checking enabled.

The release `db_bench` build (`DEBUG_LEVEL=0`, no RTTI), CMake Release
configuration with tests/tools, source checks, formatting checks and
`git diff --check` passed. BUCK was generated from the source lists and a
second generation produced identical bytes. The normal BUCK check script
skips an uncommitted BUCK file, so the explicit regeneration comparison was
used. New untracked sources were separately checked for formatting, ASCII and
the dual-license header. Full `make check` stops at failing C++ tests; later
stages are not implicitly claimed to have passed.

The skipped post-C++ Python, `ldb_test.py`, `db_crashtest_test.py` and
`rocksdb_dump_test.sh` checks were run separately and passed. Workflow YAML
validation could not run because Ruby is absent. `check-c-api-gen` confirmed
link completeness (1745 functions), then failed compatibility against `main`:
21 functions and 5 enum/typedef symbols are absent on this branch. The C API
sources and generator are unchanged by this work; the subsequent generator
staleness step was not reached. These environment/branch checks remain open.

Only Linux/GCC was exercised. Windows, macOS, other compilers, sanitizers and
real storage power interruption were not tested in this environment.

## Release benchmark

Environment: Ubuntu 22.04, Linux 5.15.0-191, GCC 11.4, KVM guest exposing
48 Intel Xeon Gold 5318Y vCPUs and approximately 31 GiB RAM. All directories
were on the same ext4 logical volume under `/tmp`; these were not independent
failure domains. Other builds and the test suites had finished before timing.
The host's physical storage and interference outside this task were not
controlled. No cache dropping was used.

The release `db_bench` standalone modes wrote 50,000 decimal-string keys with
1024-byte values, one foreground writer, no compression, WAL enabled and
`sync=false`. Baseline used the same separated Blob Direct Write storage
without the backup module. Three trials used fresh directories. Every backed
trial closed normally, removed its entire primary index directory, restored
from backup plus retained blobs, reopened, and verified all 50,000 values.
No `.blob` file appeared in the backup. SIGKILL coverage is provided by the
separate functional tests, not these throughput trials.

| Metric | Trial 1 | Trial 2 | Trial 3 |
| --- | ---: | ---: | ---: |
| Baseline foreground (ms) | 383.444 | 384.690 | 376.011 |
| Backup foreground (ms) | 446.651 | 444.195 | 426.026 |
| Explicit backup sync (ms) | 697.495 | 681.343 | 682.450 |
| Backup close (ms) | 738.578 | 756.272 | 750.324 |
| Final point construction (ms) | 350.241 | 354.280 | 347.245 |
| Final point event-to-publication lag (ms) | 705.201 | 720.972 | 715.714 |
| Restore including native Open (ms) | 736.686 | 734.702 | 760.752 |
| Peak charged queue bytes | 4,587,681 | 4,444,622 | 4,347,973 |
| Backpressure (ms) | 0 | 0 | 0 |

Median foreground time increased from 383.444 ms to 444.195 ms (+15.8%),
equivalent to a 13.7% foreground throughput reduction. This excludes the
separately reported explicit sync and close costs, and is a short-run result,
not a sustained-throughput estimate. Point statistics describe the last
publication, not a latency distribution or an RPO bound.

Each write trial mirrored 2,876,871 bytes. Logical retained index size after
write/close was 3,975,888--3,975,890 bytes, counting hardlinks per path. After
restore and verification (which open/close and publish new points), `du -s -B1`
reported 1,224,704 physical bytes for backup, 1,200,128 for the restored index,
and 53,047,296--53,051,392 for retained blob storage. These physical and logical
measurements are at different lifecycle stages and must not be subtracted to
infer hardlink savings.

A separate 50,000-write trial used a 2 MiB queue and 64 KiB batch trigger:
foreground 862.141 ms, accumulated backpressure 419.417 ms, peak charged queue
2,097,095 bytes (below the 2,097,152-byte limit), explicit sync 697.669 ms,
close 765.022 ms, final point construction 356.153 ms and lag 730.289 ms.
Restore took 757.275 ms and all values verified. The normal trials used the
64 MiB queue, 256 KiB trigger and 1000 ms interval defaults.

Use the commands in [the component guide](index.md#validation-and-benchmarks)
with `--num=50000 --value_size=1024` to repeat a trial, using fresh absolute
directories. For the pressure trial add
`--metabypass_queue_capacity=2097152 --metabypass_batch_bytes=65536` to write.
Remove only the experiment's primary index after close and before restore.
The deterministic verification workload is independent of recovery metadata.

The validation rescans and immutable copies are measurable costs. Longer runs,
mixed workloads, larger live data, compaction-heavy workloads, independently
throttled backup storage and real power-loss testing remain necessary before
making deployment or hardware durability claims. Blob retention remains
unbounded by design.
