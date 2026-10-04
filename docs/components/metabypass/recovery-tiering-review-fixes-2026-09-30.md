# Recovery and tiering review fixes (2026-09-30)

This records four fixes to the final MetaBypass feature code and their Linux
validation. The published recovery-point format was not changed.

## Changes

- `MetaBypassDB::Write` rejects a WriteBatch with a WAL termination point before
  modifying the DB. Such a batch could otherwise apply entries that were not
  recorded in the WAL and resurrect deleted keys after SSD loss.
- Closed staging blobs can begin background migration while local readers are
  active. New reads switch to the durable remote version; the local file is
  removed only after existing local readers drain. Continuous overlapping reads
  no longer prevent migration from starting.
- Offline SST restore prepares all cold objects and commits one placement map
  for the complete verified SST set, including an empty set. A failed prepare
  or placement commit leaves the restore retryable under
  `METABYPASS-RESTORING`. The next normal open removes unreferenced objects.
- Remote blob reads pin a shared immutable extent version and use cumulative
  extent ends to locate the first extent by binary search. Reads no longer copy
  every extent name under the global mutex.

## Regression coverage and results

Six new cases cover the rejected WAL boundary; extent reads across boundaries,
checkpoint prefixes and pinned versions; migration with a pinned local read;
one SST placement commit for 16 cold tables with reopen reads; an empty SST
map; and prepare/commit failures followed by restore retry. The full
`db_blob_direct_write_test` binary (32 cases) was also included.

The strict build used:

```sh
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48 metabypass_test sst_tiering_test db_blob_direct_write_test
```

The first complete run covered 119 enabled cases in these three binaries with
8 workers and a 60-second limit per case: 117 passed, one timed out and one
failed. `MetaBypassTest.TieredCrashBoundariesAndCompleteFastStorageLoss` passed
on its exact-case second round (22.850 seconds). The new
`SstStorageTest.RestorePrepareAndPlacementFailuresCanRetry` failed in all four
initial rounds because its assertion expected `NotFound` for a missing source,
while POSIX `stat(ENOENT)` returned `PathNotFound`. After the test accepted both
missing-path status codes, a strict rebuild and complete 18-case
`sst_tiering_test` run passed. Thus all 119 unique cases have passed in strict
mode across the initial runs and corrected SST run; the initial failure and
timeout remain part of the record. The first two build attempts exposed test
compile errors, which were corrected before the successful third build.

The six new cases and five existing concurrency cases ran 100 times each with
`COERCE_CONTEXT_SWITCH=1`, strict status checking, 8 workers and the same
per-case timeout: **1100 passed, 0 failed, 0 timed out**, with no retry. A
separate strict coerce build passed. `make check-sources` and `git diff --check`
passed, and an independent code review reported no findings.

Exact runner commands, selected case names, per-round JSON and logs are in the
local validation record at
`/tmp/metabypass_rocksdb_validation.HOFCHv/validation_report.md` and its
`round1/` through `round4/`, `sstfix-round1/` and `stress-round1/`
directories. This `/tmp` evidence may not persist after the host is cleaned.

These checks do not measure throughput or P99 latency. They did not use
separate SSD/HDD devices, real power interruption, or non-Linux platforms. No
performance gain or physical-device durability result is claimed here.
