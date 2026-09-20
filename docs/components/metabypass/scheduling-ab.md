# WAL/SST admission and scheduling experiments

The tested changes are isolated research variants of the notification-optimized
Metabypass at `bbd60f2ca`. No scheduling change is enabled in the production
utility. See the [experiment package](experiments/scheduling-ab/README.md) for
source transformations, driver, reproduction commands and workload definitions.

## Hypotheses

The existing primary-operation mutex spans reservation, event copying, the
primary file operation and enqueue. Consequently a background SST producer
waiting for backup capacity can prevent foreground WAL admission. Independent
WAL and metadata mutexes can remove that dependency, without changing the
number of foreground event copies or queue-lock acquisitions.

The additional hypotheses are that reserving queue headroom protects WAL
admission, and that prioritizing independent WAL appends reduces head-of-line
blocking. Neither hypothesis implies that recovery points can publish before
their SST dependencies are complete.

## Implementations and constraints

| Variant | Change from current |
|---|---|
| current | Unmodified notification-optimized utility |
| split | Separate WAL/non-WAL primary-operation mutexes; FIFO mirror; multiple capacity waiters |
| reserve | Split mutexes plus one-eighth WAL admission headroom, retaining the total memory limit |
| priority | Reserve plus WAL-first stable partition inside append-only runs of finite event windows |
| priority_outside | Priority with partitioning moved outside the queue mutex |

Creation, close, truncation, rename and deletion are ordering barriers for
priority scheduling. Rename takes both operation mutexes. Each file's append
order remains stable. The complete captured window must finish before its
progress is eligible for candidate capture. Existing native-file validation,
blob dependency synchronization and atomic complete-point publication remain.
These variants retain one mirror worker and one validator: they are not a
literal port of Ceph's two independently queued mirror workers.

Non-WAL events larger than the reserved admission budget can still consume the
original queue capacity. This preserves the maximum accepted file-operation
size, but means headroom is not guaranteed for that exceptional case. The
reserve variant uses a total-occupancy admission threshold for metadata, not
separate physical memory pools. Under mixed workloads, it may move waiting
from WAL to SST producers. Counters labeled backpressure include both.

The priority scheduler operates in finite windows and does not starve SSTs
inside a window. It cannot preempt an in-progress filesystem call or candidate
copy. The implementation uses temporary scheduling storage; the existing
charged-event cap does not account for every allocator/scratch byte.

## Measurement method

Same host and shared ext4 volume, single foreground writer, 1 KiB values,
WAL enabled, sync=false, no compression. Each measured run writes 100,000 keys;
variant order rotates between trials. Each variant/workload has a 10,000-key
warmup. There is no cache dropping or control of hypervisor interference.
No experiment build or test overlaps the timed benchmark series.

`default` uses decimal keys, default memtable settings and a 64 MiB queue.
`mixed` uses 128-byte keys, a 256 KiB memtable and target SST size, a 1 MiB
level base and L0 compaction trigger 2. `pressure` uses that mixed workload
with a 2 MiB queue. All use a 256 KiB batch trigger and a 1,000 ms interval.
Do not compare these numbers directly with earlier benchmarks that used
another key size, write count or small-queue trigger.

Every Put is timed identically in all variants. Reported p99 is the median of
per-run p99s, not a percentile of all pooled operations. Total means foreground
plus explicit backup sync plus close, excluding Open. Its median is taken from
per-run sums. Final recovery-point lag is a single end-of-run sample, not a
continuous lag distribution. Throughput results use uninstrumented binaries;
a separate instrumented series diagnoses foreground lock/capacity costs.

## First series: four variants, five trials each

| Workload | Variant | Foreground ms | p99 us | Total ms | Backpressure ms |
|---|---|---:|---:|---:|---:|
| default | current | 899.952 | 22.567 | 1865.830 | 0.000 |
| default | split | 879.368 | 21.446 | 1863.695 | 0.000 |
| default | reserve | 842.213 | 18.263 | 1787.654 | 0.000 |
| default | priority | 834.171 | 18.492 | 1775.690 | 0.000 |
| mixed | current | 2190.110 | 36.274 | 2774.422 | 0.000 |
| mixed | split | 2190.121 | 32.217 | 2788.451 | 0.000 |
| mixed | reserve | 2196.145 | 34.215 | 2766.272 | 0.000 |
| mixed | priority | 2222.855 | 35.898 | 2798.651 | 0.000 |
| pressure | current | 2240.057 | 36.347 | 2810.207 | 0.000 |
| pressure | split | 2210.315 | 33.055 | 2784.753 | 0.000 |
| pressure | reserve | 2196.159 | 34.241 | 2859.684 | 14.321 |
| pressure | priority | 2205.986 | 35.237 | 2786.198 | 17.987 |

The mixed and small-queue cases complete about 63 foreground flushes and
63 compactions per run. The default case reports zero completed foreground flushes or compactions;
it supplies no measured evidence of SST/WAL primary-operation interference.

In the mixed case, split reduces p99 by 11.2%, but foreground elapsed time is
essentially unchanged and total time is 0.5% longer. Small-queue split reduces
p99 by 9.1%, with only a 1.3% foreground-time reduction. Reserve does not
consistently beat split; its small-queue median total time is 1.8% longer than
current and cumulative backpressure rises to 14.3 ms. Priority does not
establish a general throughput advantage in mixed traffic.

Default-case differences must not be attributed to removal of SST interference
on the basis of these measurements. Individual default current
runs range from 827 to 996 ms. The second series repeats a fresh current control
rather than treating the first medians as a fixed reference.

[Raw first-series results](experiments/scheduling-ab/results.json) include every
command, trial, percentile, phase timing and restoration result. All 60 measured
runs, 12 warmups and four post-sync SIGKILL cases restored successfully.

## Follow-up: fresh controls and sorting outside the queue mutex

The detached window is private to the mirror; admission can proceed while it
is partitioned without holding the queue mutex. Five new rotated trials
compare current, split and this refinement.

| Workload | Variant | Foreground ms | p99 us | Total ms |
|---|---|---:|---:|---:|
| default | current | 904.532 | 22.055 | 1873.657 |
| default | split | 874.725 | 19.914 | 1803.637 |
| default | priority_outside | 866.123 | 18.995 | 1815.442 |
| mixed | current | 2194.318 | 37.864 | 2764.516 |
| mixed | split | 2176.401 | 31.974 | 2756.513 |
| mixed | priority_outside | 2173.149 | 33.420 | 2788.354 |
| pressure | current | 2194.286 | 37.693 | 2784.404 |
| pressure | split | 2201.402 | 31.834 | 2726.286 |
| pressure | priority_outside | 2183.626 | 33.973 | 2783.865 |

Split again improves mixed p99 (15.6%) and small-queue p99 (15.5%). Its
foreground elapsed changes are -0.8% and +0.3%, respectively: there is no
substantial foreground-throughput gain. Moving priority partitioning outside
the queue mutex does not beat split p99 in either mixed workload and leaves
mixed total time 0.9% above current. This makes the recommendation independent
of the first priority implementation holding the queue mutex while sorting.

[Raw follow-up results](experiments/scheduling-ab/followup-results.json) contain
45 measured runs, nine warmups and three post-sync SIGKILL cases; every restore
and subsequent write/reopen verification passed.

## Foreground-only instrumentation

A separate three-trial series measures foreground operation-lock acquisition,
queue-lock acquisition in Error/Reserve/Finish, and capacity-condition waiting.
All durations below are cumulative milliseconds per 100k-write run, shown as
medians. Acquisition includes uncontended call cost and contention; this is
not a pure off-CPU or kernel wait counter. Instrumentation perturbs execution.

| Workload | Variant | Operation lock ms | Queue locks ms | Foreground capacity ms | All-producer capacity ms |
|---|---|---:|---:|---:|---:|
| mixed | current | 73.188 | 30.656 | 0.000 | 0.000 |
| mixed | split | 10.585 | 37.144 | 0.000 | 0.000 |
| mixed | reserve | 10.988 | 26.733 | 0.000 | 0.000 |
| mixed | priority | 10.185 | 35.788 | 0.000 | 0.000 |
| pressure | current | 75.965 | 25.965 | 0.000 | 4.065 |
| pressure | split | 9.328 | 25.709 | 0.000 | 0.000 |
| pressure | reserve | 10.004 | 33.191 | 0.000 | 16.116 |
| pressure | priority | 9.747 | 47.780 | 0.000 | 17.989 |

Splitting admission removes a measurable foreground operation-lock cost,
consistent with the deterministic blocking tests. Queue acquisition cost
remains, and the uninstrumented total elapsed time does not fall in proportion
to the removed lock cost. Other execution and waiting, and changes to concurrent
scheduling, still contribute; these measurements do not identify every residual
bottleneck or form an exact additive wall-time decomposition.

Foreground capacity medians are zero in all four small-queue variants, although
individual trials stall (up to 21.6 ms in current in this instrumented series).
Reserve and priority nevertheless show 16.1 and 18.0 ms of median all-producer
capacity waiting. Those aggregate counters therefore include substantial
background-producer pressure; they cannot be read as foreground latency.
Use the uninstrumented series for throughput and tail-latency claims.

[Raw instrumented results](experiments/scheduling-ab/profile-results.json)
include 36 measured runs, 12 warmups and four post-sync SIGKILL cases; all
restores and write/reopen checks passed.

## Recommendation

**Keep the production default unchanged for now.** The strongest repeatable
benefit is lower p99 under mixed WAL/SST traffic from splitting admission
locks, not a large write-throughput speedup. If promoting a variant later,
start with split alone; it adds less complexity and has the clearest observed
benefit. Capacity reservation and priority scheduling are not justified by
these workloads. This does not rule out different results with slower backup
storage, sustained saturation, other queue sizes or different WAL/SST ratios.

The approximately 18% backup overhead in the previous no-backup comparison
cannot be claimed eliminated: this experiment compares backup implementations,
and still performs batch validation, capacity reservation, event copying,
enqueue and background complete-point validation. It does not measure that
old workload with backup disabled again.

## Correctness evidence

The unchanged reader restores each run after removal of the entire main index,
checks every key/value, appends a new key, closes/reopens, and verifies it again.
Separate SIGKILL writers stop after a confirmed SyncBackup and before Close;
the same full-index-loss recovery check follows. These do not establish a
physical power-failure guarantee.

All 26 existing unit cases passed for each of five variants (130 cases). Three
additional deterministic tests ran ten times for each split variant (120
cases), then 100 repetitions each (1,200 further cases). They prove WAL can
pass a blocked SST primary operation and a blocked SST capacity reservation,
and that mirror failure wakes both lane capacity waiters. Existing cases cover
publication-stage crashes, rotations, WriteBatch, overwrite/delete, corruption,
shutdown and interrupted blob recovery. These are normal debug builds; no new
sanitizer or full-repository gate result is claimed for the experimental forks.

Source checks, Python syntax, report links and whitespace checks passed. The
source check exposed an existing hardcoded namespace in the earlier profiling
driver; it was changed to `ROCKSDB_NAMESPACE`, with no production behavior change.
[Environment and fingerprints](experiments/scheduling-ab/environment.json)
record the archive, executables and generated candidate source identities.
[Test records](experiments/scheduling-ab/checks.json),
[refined-priority checks](experiments/scheduling-ab/outside-checks.json), and
[100-repeat results](experiments/scheduling-ab/stress.json) retain exact outputs.
Across all three series, 185 complete-index-loss restores passed, including
warmups, instrumentation runs and 11 post-sync SIGKILL cases.

正式 WAL/metadata 双通道实现与新一轮对照见[双通道验证记录](channel-validation.md)；本页保留原实验结果。
