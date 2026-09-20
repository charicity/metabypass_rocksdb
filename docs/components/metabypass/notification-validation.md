# Notification scheduling fix

This implements the synchronization fix identified by
[foreground profiling](foreground-profile.md). It preserves the two-thread
mirror/validation pipeline and all native-file and blob validation.

## Implementation

All wait predicates and their state are protected by the existing queue mutex.
There are now four condition variables with distinct recipients:

| Channel | Waiter | Notifications |
|---|---|---|
| Mirror | One mirror worker | Actionable queued work, capture readiness, explicit sync, capacity pressure, failure or stop |
| Validator | One validation worker | Candidate handoff, mirror exit or failure |
| Capacity | At most one producer, serialized by `operations_` | Sufficient released capacity, failure or stop |
| Publication | Any number of SyncBackup callers | Successful publication, failure or stop |

`WakeMirror` checks both whether the worker is waiting and whether its predicate
is satisfied. A successful enqueue therefore avoids condition-variable calls
when the worker is already processing, or when it would only wake and wait
again below the batching threshold. Below-threshold events still drain on the
interval timer. SyncBackup explicitly requests immediate processing.

Capacity reservations record the blocked event's charge. This is necessary
because an event can exceed the remaining capacity before the accumulated
queue reaches `batch_bytes`. The blocked reservation requests immediate
consumption, and the mirror notifies the producer only when enough charge has
been released. Failed Apply operations record the sticky error before releasing
waiters through the failure path.

The mirror's final close wait is notified when validation finishes. Mirror
exit releases an idle validator. Errors release every relevant wait channel;
publication broadcasts to all sync callers, whose individual goals are checked
under the mutex. Notifications are performed while holding the predicate mutex,
so no transition between testing a predicate and sleeping can lose a wakeup.
No filesystem I/O was moved under that mutex.

No public API, backup format, persistent metadata, build source list, blob
retention, or validation policy changes are involved.

## Regression tests

Six new cases supplement the existing twenty:

- Below-threshold writes issue no mirror notification and drain on Stop.
- Below-threshold writes drain on the interval timer without an explicit sync.
- A reservation blocked below `batch_bytes` forces immediate draining.
- Mirror failure wakes that blocked reservation and propagates through sync
  and shutdown.
- Close drains and joins while validation is deliberately blocked.
- Four simultaneous SyncBackup callers all wake on publication and on injected
  publication failure; recovery retains the preceding valid point.

The tests coordinate with sync points and the shared TestGate helper rather
than sleep-based ordering. Existing coverage includes blocked validation with
continued mirroring, capacity ordering, publication failures, WAL/manifest
rotation, compaction, overwritten/deleted data, and SIGKILL recovery.

## Reproduction

`experiments/run_notification_benchmark.py` compares the pre-fix and fixed
release db_bench executables. It rotates execution order, records five trials
for each workload, deletes each primary index after writing, restores using
the pre-fix reader, then verifies every value with the matching writer version.
All subprocesses are limited to 60 seconds. The performance data is collected
after builds and correctness tests finish.

## Correctness and repository checks

- Normal and `ASSERT_STATUS_CHECKED=1`: all 26 Metabypass tests passed.
- `COERCE_CONTEXT_SWITCH=1`, 100 repetitions of all 26 cases: 2,600 passed.
  Each child had a 60-second limit. An earlier invocation mistakenly used
  the runner's global `--timeout=60` option and was interrupted; the successful
  run restarted from scratch with `--timeout_per_test=60`.
- Both complete `make check` configurations were executed, with 24 test jobs
  each and a 60-second shard limit, on ext4. Normal: 3,321 shards; status-checked:
  3,302 shards. The three Metabypass shards passed in each configuration.
- **The full checks are not all green.** Each had ten failed shards: the two
  previously reproduced BloomFilter assertions, the previously reproduced
  Prefetch assertion/cleanup crash, and seven shard timeouts. The seven timed-out
  cases (one compaction-service, four SST-ingestion, two column-family cases)
  all passed on isolated tmpfs retry in both configurations. The compaction
  case also passed with the old baseline library. Retrying on another
  filesystem does not retroactively turn the original ext4 checks green.
- Format, BUCK regeneration consistency, source checks, Python syntax, ldb,
  db_crashtest, dump, and whitespace checks passed. C API link completeness
  passed for 1,745 declarations. Workflow YAML validation remains blocked by
  missing Ruby, and C API compatibility against `main` reports the branch's
  existing 21 missing functions and five missing enum/typedef symbols.
  This change does not edit C API declarations or generated definitions.

The known Bloom tests are `DBBloomFilterTest.MutatingRibbonFilterPolicy` and
`DBBloomFilterTest.MutableFilterPolicy` (8.248 bits/key versus expected 7,
tolerance 0.3). The Prefetch failure is `PrefetchTest/PrefetchTest.Basic/0`
(1.31891e6 bytes versus minimum 1.88744e6, followed by a cleanup segfault).
Both signatures match the saved pre-change baseline logs.

[Check records](experiments/notification-checks.json) include exact commands,
failed shard identities, related-test results, log hashes and source hashes.
The normal, status-checked, coerce and release build directories used identical
copies of the changed production and test source files. No sanitizer,
non-Linux or real power-failure result is claimed.

## Release performance results

Five trials per variant/workload, medians in milliseconds. Total is the median
of each run's write + explicit sync + close sum; it excludes Open and is not
the sum of the independent phase medians. All builds and correctness test
processes had exited before timings began. Both variants used the same shared
ext4 volume, 1 KiB values, sync=false, and a single writer. No host-noise or
cold-cache guarantee is implied.

| Workload | Foreground before / after | Foreground reduction | Total before / after | Total reduction |
|---|---:|---:|---:|---:|
| 50k_default | 663.535 / 457.786 | 31.0% | 1282.113 / 1174.363 | 8.4% |
| 200k_default | 2484.953 / 1870.012 | 24.7% | 3797.238 / 3305.702 | 12.9% |
| 50k_pressure | 600.645 / 471.095 | 21.6% | 1245.098 / 1127.103 | 9.5% |

The pressure workload uses a 2 MiB queue and 64 KiB batch threshold; defaults
use 64 MiB and 256 KiB. All measured backpressure counters were zero in both
versions. Deterministic unit tests separately exercise actual capacity stalls.

Explicit sync is longer after the fix: its medians change from 203 to 284 ms
(50k default), 261 to 370 ms (200k default), and 217 to 267 ms (50k pressure).
The foreground finishes sooner, leaving more backup work at the explicit
sync boundary. The complete write/sync/close interval still decreases in every
workload. Close medians remain approximately 405--425 ms for 50k writes and
1,060 ms for 200k writes. Final published-point lag medians remain approximately
372--392 ms and 1,010 ms respectively; these samples are not an RPO bound.

All 30 measured writes, 30 complete-primary-index-loss restores and 30 full
value verifications passed, plus two write warmups. Every restore used the
pre-fix reader. Mirrored bytes and blob persistence-validation byte counts
match between versions for each workload; no content scan was disabled.

The release binaries are identified in the [raw results](experiments/notification-results.json),
which include commands, outputs, ranges, hashes and source fingerprints.
The fixed binary SHA256 is
`207e2c90cfd3413b94bb700b4f877a95969ecc961f7c0fb6e06b49c25bf37ea0`.
