# Identity retry and option validation fixes

The review of `7247edbf..dd0991c84` identified three lifecycle/configuration
problems. The fixes do not change the write path, queue scheduling, backup
format, or recovery-point publication.

## Changes

- Reopen still verifies `METABYPASS` against `IDENTITY` and both directory
  owners, but does not truncate and rewrite the verified marker. An I/O error
  or interruption during that unnecessary rewrite could previously make a
  healthy database fail every subsequent open with an identity mismatch.
- A resumed Restore verifies and preserves `METABYPASS-RESTORING`. It still
  synchronizes the destination directory before copying. Previously, failure
  during its rewrite could turn a resumable destination into one that failed
  the next retry's ownership check. A fresh restore still creates the marker;
  successful completion removes it as before.
- Open and Restore reject `write_identity_file=false` and a configured
  `compaction_service` with `NotSupported`, before acquiring directory locks or
  modifying directories. The identity file is required by the ownership
  protocol. Remote compaction output installation requires a cross-boundary
  rename that the mirror does not support. Local compaction remains supported.

This fix preserves existing valid markers; it does not repair markers already
corrupted by earlier executions or redesign first-time initialization.

## Regression coverage

Three new tests use the existing Metabypass fixture:

- `RejectIdentityAndRemoteCompactionBeforeDirectoryChanges`: both entry points
  reject both options without creating index/data/backup directories; corrected
  options allow creation, writing and recovery.
- `ReopenPreservesValidatedIdentityMarker`: inject failure after truncation if
  the marker is rewritten; verify reopen performs no such write and a subsequent
  ordinary open can still read the data.
- `RestoreRetryPreservesValidatedProgressMarker`: interrupt copying CURRENT,
  verify Open demands a restore retry, then inject failure on rewriting the
  progress marker. Restore succeeds without that rewrite, reads the recovered
  value, accepts another write and survives another reopen.

All three tests fail against the production implementation at `dd0991c84`.
The fault wrapper models a file-operation failure, not physical power loss.

## Validation

Validation on the Linux VM, 2026-09-20 (each focused test limited to 60 seconds):

| Check | Result |
| --- | --- |
| Regression tests against pre-fix production code | All 3 fail as expected |
| Ordinary Metabypass suite | 37/37 pass |
| `ASSERT_STATUS_CHECKED=1` Metabypass suite | 37/37 pass |
| `COERCE_CONTEXT_SWITCH=1`, 100 repetitions of each new test | 300/300 pass |
| Normal full `make check` | 3,322 shards, 7 failures |
| Status full `make check` | 3,303 shards, 6 failures |
| Isolated tmpfs retries of timeout-related cases | Normal 8/8; Status 7/7 pass |
| Formatting, source and BUCK consistency | Pass |
| Python, ldb, db_crashtest and dump checks | Pass |
| Workflow YAML | Blocked: Ruby is unavailable |
| C API compatibility | Historical 21 missing functions and 5 missing symbols relative to `main`; link completeness passes |

The full suites do **not** pass. Their failures match signatures already
recorded in [channel validation](channel-validation.md):

- Both builds: two BloomFilter assertions (8.248 bits/key versus 7 +/- 0.3),
  and `PrefetchTest.Basic/0` readahead assertion followed by a segmentation
  fault during failure handling.
- Normal: four timed-out shards (remote compaction, external SST ingestion,
  and two column-family shards).
- Status: three timed-out shards (remote compaction and two column-family
  shards).

The full checks use a 60-second shard limit. Some column-family shards contain
multiple tests and hit this aggregate limit after a slow `BulkAddDrop`.
Separate tmpfs retries use a 60-second **per-test** limit. They cover
`CorruptedOutputVerifyOutputFlags`, both format variants of `BulkAddDrop`,
`ReadDroppedColumnFamily`, and `FlushCloseWALFiles`; normal also covers
`ZeroAndNonZeroSeqno/2`. These retries pass but do not erase the full-run failures.
There were no Metabypass failures in either full run. Source/script checks were
run separately because the full targets stop after test failures.

Reproduction commands (use distinct build directories for Status/coerce):

```sh
build_tools/rocksptest.sh metabypass_test --timeout_per_test=60
ASSERT_STATUS_CHECKED=1 build_tools/rocksptest.sh metabypass_test --timeout_per_test=60
COERCE_CONTEXT_SWITCH=1 build_tools/rocksptest.sh metabypass_test -w8 -r100 \
  --timeout_per_test=60 \
  --gtest_filter='*RejectIdentityAndRemoteCompactionBeforeDirectoryChanges:*ReopenPreservesValidatedIdentityMarker:*RestoreRetryPreservesValidatedProgressMarker'
AUTO_CLEAN=1 make -j24 check J=24 \
  TEST_TMPDIR=/tmp/mb-review-fix-normal-tests DRIVER='timeout 60'
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j24 check J=24 \
  TEST_TMPDIR=/tmp/mb-review-fix-status-tests DRIVER='timeout 60'
```

No throughput improvement is claimed: the changes affect option validation,
Open and Restore, not the Put/WAL/queue critical path. No new cross-platform or
physical power-loss testing was performed.

Follow-up: [unlimited shard rerun and failure diagnosis (Chinese)](unlimited-shard-check.zh-CN.md)
reruns the seven normal-build failures without a timeout and explains the remaining assertions/crash.
