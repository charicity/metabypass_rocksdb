# SST and blob migration lifecycle

Whole-SST tiering and blob staging share the following migration contract. They
keep separate workers, route representations, handle ownership and accounting;
there is no common executor or state machine. See [SST tiering](sst-tiering.md)
and [blob staging](tiered-storage.md) for their placement and scheduling policies.

The recovery boundary is a complete published remote point. After permanent
loss of all SSD index and staging data, restore uses that point and its durable
dependencies. Ordinary writes remain best effort relative to remote publication:
this lifecycle adds no foreground fsync or new wait to ordinary writes. A
migration descriptor or placement map alone is not a complete recovery point.

## Stages and ownership

| Stage | Immutable SST | Blob staging |
|---|---|---|
| Prepare and validate target | Promotion copies and verifies a complete SST; demotion revalidates the existing protected object | `AppendExtent` copies bounded extents and reads them back for checksum validation |
| Persist dependencies | Promotion fsyncs the copy and installs/syncs its directory entry; `SavePlacement` persists the selected placement | Extent files and directory entries persist before `Commit` persists the descriptor; an active prefix can be committed independently |
| Switch new I/O | `Migrate` updates `Entry::hot` and the shared current handle after successful placement persistence | `SwitchReadsToRemote` requires a closed local source and a complete sealed remote version before setting `Local::evicting` |
| Drain old I/O | Each physical read owns a shared `Handle` pin; `Retired` retains the replaced handle | Each successful local open increments a per-I/O count owned by stack `LocalReadPin`; destruction closes the handle before releasing the count |
| Delete source and account | `Reap` unlinks a retired local SST only after old shared pins drain, then releases its pending-delete charge | `ReclaimLocalAfterReads` deletes only a switched source with zero local I/O pins, then releases its staging charge |

These phases can reuse an already prepared or committed target. Repeating the
blob switch is safe, and reclaiming an already removed local entry succeeds.
The single blob worker uses an explicit reclaim path after route switching and
sleeps while old local I/O drains; releasing a pin wakes it. Offline orphan
archival follows the same switch/reclaim stages under exclusive ownership.
`MetaBypass::TierBeforeEvict` still injects failure before the blob route switch,
after remote descriptor persistence, so an injected failure retains local data.

`Entry::readers` counts logical SST reader objects. It does not count physical
I/O and cannot authorize an unlink. A logical reader survives either direction
of SST migration and takes the current shared physical handle for its next I/O.
Blob logical readers instead resolve their route per read, and open a fresh
physical local handle while the local route is enabled. Their per-I/O count is
not interchangeable with SST logical-reader accounting. Both paths copy results
owned by a physical handle into caller scratch before closing that handle.

An active or partially migrated blob retains its full local source. Neither a
prefix descriptor nor a complete unsealed copy authorizes whole-file deletion.
A normally closed source must match the sealed remote length, allowing a remote
footer synthesized when shutdown closes a complete active blob without one.
Exclusive `SealRecovery` is a separate recovery operation: it may discard an
invalid local tail after committing the validated prefix and a recovery footer;
it is not an online whole-file migration and retains its existing cleanup rules.
SSTs are complete immutable files and may migrate in either direction. Their
protected remote object remains available after promotion and local deletion.

## Persistence and deletion failures

A failed persistence call does not prove that disk metadata is unchanged. For
example, rename can succeed before a directory fsync reports failure. Publication
of the in-memory route still requires success; existing retry, orphan accounting,
placement reload and marked offline-restore handling remain responsible for the
possibly changed disk state. This refactor does not roll back disk metadata or
change the complete-point recovery protocol.

Blob deletion failure retains the local entry and its charge. After successful
deletion the entry and charge are removed before staging-directory sync; a sync
failure is still reported and does not restore the charge for a deleted file.
SST failed unlink retains its pending-delete charge for later `Reap`. Interrupted
installed promotion stays reserved/orphaned until placement is safely republished
and cleanup succeeds. These are the existing component-specific accounting rules.

## Regression scenario matrix

Test names below identify coverage; the matrix is not a new test-result report.
`TieredStorageTest` and `MetaBypassTest` are in `metabypass_test`, and
`SstStorageTest` is in `sst_tiering_test`.

| Scenario | Blob coverage | SST coverage |
|---|---|---|
| Active/prefix target must retain source | `TieredStorageTest.ActiveAndPartialMigrationRetainLocalSource` | Whole files only; `SstStorageTest.UnprotectedCannotEvictAndOversizedCannotPromote` |
| Commit failure preserves usable source | `TieredStorageTest.DescriptorCommitFailureRetainsValidLocalSource` | `SstStorageTest.PlacementFailuresKeepOriginalReaderAndRetry`; `SstStorageTest.DamagedProtectionNeverEvictsTheValidSsdCopy` |
| Old I/O blocks deletion while new I/O uses target | `TieredStorageTest.MigratePinnedLocalReadBeforeReclaimingSpace` | `SstStorageTest.ReadPinsDelayUnlinkButNotRouteSwitch` |
| Read error releases physical pin and allows reclaim | `TieredStorageTest.LocalReadErrorReleasesPinBeforeMigration` | Shared handle pin is scoped to `SstLogicalReader::Read`; route-pin coverage above |
| Bidirectional migration/logical-reader survival | Blob migration is local to remote; `TieredStorageTest.ExtentBoundariesCheckpointAndPinnedVersion` covers immutable view pinning | `SstStorageTest.ExistingReaderSwitchesBothDirectionsAndSurvivesDelete` |
| Installed target remains charged until safe cleanup | Staging accounting asserted by the pinned-read and commit-failure cases | `SstStorageTest.FailedInstalledPromotionRemainsChargedUntilSafeCleanup` (including its later compaction-delete path) |
| Permanent complete SSD loss recovers published point | `MetaBypassTest.TieredSyncSurvivesCompleteFastStorageLoss`; `MetaBypassTest.TieredCrashBoundariesAndCompleteFastStorageLoss` | `MetaBypassTest.SstCompleteSsdLossRestoresExactlyPublishedState`; `SstStorageTest.CrashAtEveryDemotionAndPromotionPersistenceBoundary` |

Blocked-I/O tests use deterministic SyncPoints and release/join their threads
before checking assertions. Directory deletion and injected errors exercise the
protocol, but do not simulate physical device power-loss behavior.

## Validation (2026-10-01)

Strict Status-checking builds and the complete two-binary scope covered 90 cases.
The first round passed 89 cases; the existing
`MetaBypassTest.TieredCrashBoundariesAndCompleteFastStorageLoss` timed out at
60.001 s. Its exact-case retry with two workers passed in 22.865 s; the first
round's timeout remains recorded. No assertion failures occurred. Nine migration
and concurrency cases each ran 100 times with `COERCE_CONTEXT_SWITCH=1`:
900 passes, zero failures and zero timeouts. `make check-sources` and
`git diff --check` passed. Incremental ReviewAgent review reported "No findings."

Core build and runner commands (output arguments omitted) were:

```sh
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48 metabypass_test sst_tiering_test
ASSERT_STATUS_CHECKED=1 build_tools/gtest-parallel ./metabypass_test ./sst_tiering_test --workers=8 --timeout_per_test=60
AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 COERCE_CONTEXT_SWITCH=1 make -j48 metabypass_test sst_tiering_test
```

The coerce runner retained eight workers and the 60-second per-case timeout,
adding `--repeat=100` and the nine-case `--gtest_filter`. The exact filters,
retry command, output arguments and measured results are in
`/tmp/metabypass_migration_lifecycle_validation.UNDwbV/validation_report.md`.
Preserved runner logs and result JSON are under that directory's
`strict-round1/`, `strict-round2/` and `coerce-round1/` subdirectories.
These checks did not measure performance or use real separate SSD/HDD devices,
and did not test physical power loss.
