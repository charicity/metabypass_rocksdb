# Locating the foreground-write regression

This is the historical pre-fix investigation. The subsequent production fix
and its validation are described in [notification scheduling](notification-validation.md).

The dominant regression is the foreground's unconditional condition-variable
broadcast after every mirrored append. The two-thread pipeline keeps mirror
consumption active while validation runs, and adds a validator waiting on the
same condition variable. This increases expensive notifications, wakeups and
contention. It is not primarily time waiting for queue capacity, nor an
unavoidable cost of asynchronously validating recovery points.

This is a diagnosis of commit `296800123`, not a production optimization.
Production files remain unchanged. A notification-only experimental control
removed approximately 91% of the extra foreground time in this workload,
while retaining the pipeline and all content validation.

## Workload and controls

Same Ubuntu/KVM host and shared ext4 volume as
[pipeline validation](pipeline-validation.md): one writer, 200,000 decimal
keys, 1,024-byte values, no compression, WAL enabled, sync=false, 64 MiB queue,
256 KiB batch threshold, 1,000 ms interval. No cache dropping or control of
hypervisor interference. Builds completed before measurements. Each child
process was capped at 60 seconds and used a private data directory.

First, five alternating runs of the original release db_bench binaries
reproduced the regression: baseline median 1.764 s versus incremental 2.478 s.
A small driver then measured the same Put loop, excluding Open, SyncBackup,
Close, and reporting. It additionally sampled `getrusage(RUSAGE_THREAD)`.
Only utility objects varied; the rest of RocksDB came from the same release
archive, with tcmalloc linked in all driver variants.

Variants:

- **Baseline:** original serial mirror/validation utility at `e7663cf90`.
- **Current:** production two-thread pipeline and incremental validation.
- **Separate CV:** current, with a dedicated validator condition variable;
  candidate handoff, error, and mirror exit notify it explicitly.
- **Notification control:** separate CV plus a producer broadcast only on
  primary error or when queued bytes meet the configured batch threshold.
  Below-threshold events retain the existing timed drain. Sync/close,
  validation, persistence, and publication work are retained.

The last variant is a causal control, not a proposed ready-to-merge fix.
It has not undergone concurrency, small-queue, shutdown/error stress, or the
repository's full correctness gates. Do not apply its patch as a supported
optimization based on throughput alone.

## Uninstrumented foreground results

Final confirmation series, five trials per variant, rotated execution order.
All values are independent medians; CPU columns need not sum to wall time.

| Metric | Baseline | Current | Notification control |
|---|---:|---:|---:|
| Foreground wall time (ms) | 1,810.675 | 2,497.744 | 1,870.383 |
| Foreground user CPU (ms) | 900.359 | 1,093.852 | 918.116 |
| Foreground system CPU (ms) | 894.275 | 1,329.179 | 914.163 |
| Voluntary context switches | 261 | 5,903 | 3,050 |

Current adds 687.069 ms over baseline. Changing only notification routing and
triggering recovers 627.361 ms, or 91.3% of that difference. This is a 25.1%
reduction relative to current foreground time, with about 3.3% remaining over
baseline. These are workload-specific diagnostic results, not a general
throughput guarantee or a claim about end-to-end backup completion.

An earlier five-trial series separately measured the CV-only change:
baseline/current/separate-CV medians were 1,791 / 2,522 / 2,376 ms. Separating
the validator alone helps, but does not remove the mirror's below-threshold
wakeups.

The earlier series also measured median wall-minus-thread-CPU time of
9.9 / 70.8 / 36.0 ms respectively. This residual includes descheduling and
waits; it is not an I/O-wait counter. Most of the regression was increased
CPU execution, particularly in the kernel, rather than long off-CPU stalls.

## Call-site measurements

A Linux/glibc preload library timed calls only on the foreground thread and
only during the Put loop. It intercepted mutex lock acquisition, condition
broadcast, and write/pwrite/pread calls. Address resolution identifies the
broadcast caller as `Backup::Finish`, and writes as
`PosixWritableFile::Append`. Only `write` occurred among the measured I/O
calls in this foreground workload.

Corrected, three-trial confirmation series, medians in milliseconds:

| Timed operation | Baseline | Current | Notification control |
|---|---:|---:|---:|
| Entire foreground loop | 1,955.265 | 2,759.812 | 2,027.539 |
| `Backup::Finish` broadcast | 13.224 | 602.933 | 1.760 |
| All intercepted mutex acquisitions | 64.522 | 180.987 | 132.413 |
| Primary file `write` calls | 927.651 | 934.944 | 941.477 |

Baseline and current each call the foreground broadcast 200,000 times and
`write` 400,001 times. The notification control calls broadcast only
21,942--30,246 times across these trials. Its broadcasts are also cheaper
because the validator is no longer a recipient and the mirror is more often
already working rather than waiting below threshold.

Broadcast time increases by about 590 ms, accounting for roughly 73% of the
instrumented wall-time difference; mutex acquisition adds about 116 ms.
Primary write-call time differs by only about 7 ms. These median differences
are approximate attribution, not an exact additive accounting: instrumentation
changes scheduling, other operations are not timed, and independent medians
are not paired sums. Use the uninstrumented table for performance comparisons.
The mutex bucket includes acquisition overhead and contention, not exclusively
sleeping on a lock. The remaining difference is not individually attributed.

## Why the existing code causes this

1. `Backup::Finish` holds the queue mutex and calls `cv_.notify_all()` for
   every successful append, even below `batch_bytes`.
2. `Backup::Run` waits on that CV with a batching predicate. Notification
   does not mean its predicate is satisfied: it may wake, reacquire the queue
   mutex, find too little work, and wait again.
3. `Backup::ValidateLoop` also waits on the same CV, although a normal enqueue
   does not create a candidate for it. The mirror additionally broadcasts
   after each applied event.
4. In the old serial design, the worker spends substantial time validating
   outside the queue lock. During that time it is not a CV waiter, so many
   foreground broadcasts are cheap and mirroring is deferred until later.
5. The pipeline drains more continuously. With a large queue and no capacity
   pressure, the foreground does not gain from that draining but pays more
   notification and shared-lock overhead. A small queue can still benefit
   because avoiding capacity stalls outweighs this overhead.

`backpressure_micros` starts after acquiring the queue mutex and counts only
capacity waiting. Zero backpressure therefore does not imply zero
notification, mutex acquisition, scheduler, or kernel overhead.

This diagnosis does not claim each extra broadcast necessarily triggers a
kernel wake: that depends on waiter state. The measured call-time increase,
thread CPU/context-switch changes, and notification-only controls establish
the aggregate cost without requiring that assumption.

## Storage and correctness cross-checks

Three uninstrumented tmpfs trials per variant yielded foreground medians of
1,204 / 2,676 / 1,840 ms for baseline/current/separate-CV. The regression
persists without ext4 block-device writes, supporting a synchronization
explanation. Tmpfs has different timing and durability semantics; this is
not a substitute for a real storage benchmark, nor proof that storage has
zero influence on ext4 runs.

For the first confirmation trial of baseline/current/notification-control,
the runner removed the entire primary index, restored from the backup and
retained blob directory using the unchanged production reader, and verified
all 200,000 values. All three passed. This checks the experimental output for
this workload only; it is not concurrency or crash-safety qualification.

No hardware perf sampling was used (`perf` unavailable, perf_event_paranoid=4).
No production fix, full-suite rerun, sanitizer result, or power-loss guarantee
is claimed in this investigation. The next implementation target is separate
wait channels and predicate-aware/batched notifications, followed by explicit
error, backpressure, SyncBackup and shutdown concurrency tests.

## Artifacts

[Experiment README](experiments/foreground-profile/README.md) describes the
scripts, exact raw data, binary identities and diagnostic patch. The early
baseline hook rows lack instrumentation and are explicitly excluded; all
call-site comparisons above use the corrected `confirmation.json` series.
