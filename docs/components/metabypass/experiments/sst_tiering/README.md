# SST tiering experiment protocol

Build `db_bench` from this checkout using the repository's release build
procedure, then run the script with two existing parent directories. The
runner creates fresh child directories only. It never formats or mounts a
device, clears the global page cache, or discards a failed trial from its JSON.

```sh
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 db_bench
python3 docs/components/metabypass/experiments/sst_tiering/run.py \
  --binary ./db_bench --profile smoke \
  --ssd-root /tmp/mb-sst-smoke-ssd \
  --hdd-root /tmp/mb-sst-smoke-hdd \
  --output /tmp/mb-sst-smoke.json
python3 docs/components/metabypass/experiments/sst_tiering/run.py \
  --binary ./db_bench --profile standard --repeats 3 \
  --ssd-root /path/to/ssd/mb-sst-trials \
  --hdd-root /path/to/hdd/mb-sst-trials \
  --output /path/to/results/mb-sst-standard.json
```

Both roots must be empty and disjoint before a run. The runner removes only
its successful child trials after measuring space, limiting peak disk usage.
Use `--keep-data` to retain all trial directories. Failed trial directories
are retained for diagnosis. Use a new output filename and empty roots for each
run. The smoke profile is one repeat with 8,192 keys, 15,000 measured operations
and shortened evaluation, heat-decay and residency intervals with a 32 KiB
block cache; it checks
feasibility and cannot substitute for the standard result. The standard
profile uses at least three independent repeats, 100,000 keys, 240,000
measured operations, a 64 KiB block cache and the default policy timing. Each
run uses one foreground thread, a fixed seed, fixed offered operation rate and the
same fill/warmup/measurement/verification counts. Change the seed or cache
explicitly only for a new protocol run.

The runner first creates a disabled calibration dataset. Its SSD SST logical
file sizes freeze the 25%, 50% and 75% budgets for every workload and repeat.
The configurations are disabled (all SST on SSD), observe-only at each budget,
and adaptive at each budget. Observe includes tiering routing, protected links,
placement metadata, sampling and policy decisions, so its difference from
disabled is the full observation path. Workloads are uniform random point
reads, stable hotspot, hotspot switch, point reads with periodic full scan,
and mixed point reads with versioned updates, deletes, explicit flushes and
compaction. Every `Get`, scan value and final key is checked against a
deterministic expected version; deleted keys must return NotFound. Values
encode both key and version,
so repeated writes cannot hide an incorrect old value. Fill writes force
multiple SSTs and the block cache is smaller than the index working set.

For each nonuniform workload, 80% of point keys come from the active quarter
and 20% from its disjoint complement. Separate random draws choose the group
and the key. The measured summary records hot/cold Get counts and which decimal
key endings appeared in cold Gets. The runner rejects a missing or skewed
distribution, including cold keys confined to two endings.

`MB_SST_JSON` lines delimit fill, warmup, measured and verification phases.
Measured windows report Get throughput and P50/P95/P99, hot/cold percentiles,
sample and migration deltas, SSD/HDD logical SST bytes, time over budget,
policy cutoff and backup publication lag. The hot/cold percentile groups are
defined by the workload's current hot key range; they are not per-request
SSD/HDD location measurements. The runner captures wall time, sampled process
CPU and peak RSS; it stores exact commands, binary hash, source identity, all
raw stdout/stderr, exit codes, timeouts and parse errors.
Its space scan distinguishes SSD SST, HDD SST, blob, temporary and metadata
paths. Physical allocated blocks use `(device, inode)` globally across all
hardlinked work, point and store paths; category physical totals may overlap
only when one inode spans categories, and the global total never double counts.
Use the global physical total for total-space comparisons and the SSD SST
category for SSD path occupancy. `ssd_physical_saved_bytes` is null when the
roots share a device, because moving a path on one device is not a measured
SSD capacity saving. The calibration and per-trial data are recorded
without dropping failed samples.

The optional `--read-delay-us N` delays random-access `Read`/`MultiRead` calls
to `.sst` under `backup/sst-store`. It changes only the selected cold SST read
path and is for mechanism checks; it is not a model of a physical HDD. The
report records whether SSD and HDD roots are on the same device. A local same
device run, with or without injection, cannot establish real-device speedup.

Before using a performance comparison, check the JSON `status`, `preflight`
and `mechanism_coverage`. The preflight requires at least four SSTs, an SST
working set four times the block cache and an eligible file at every budget.
The `phase_stats` records cumulative tier counters after fill, warmup and
measurement; the runner reports migration deltas for each phase. Adaptive
coverage requires measured SST read samples and either a completed migration
during measurement, or a warmup migration with cold live bytes still evidenced
at measurement end. A measured migration proves activity during that phase
even if subsequent compaction leaves every live SST hot at the final snapshot.
The cold-live lower bound is
`max(0, protected_bytes + unprotected_bytes - ssd_sst_bytes)`.
It is conservative because SSD accounting can include reservations and pending
deletes. `hdd_sst_bytes` includes protection copies of hot files, so its
positive value alone is not cold-placement evidence. A migration confined to
fill, or a measured phase without samples, is insufficient. Stable placement
completed during warmup can count even when no migration occurs during the
measured interval. The switch ends its prior window before changing keys.
`mechanism_basis` identifies `measured_migration`, `warmup_settled_cold`,
`measured_sampling` (observe-only), or `none` per trial. These are mechanism
coverage checks, not performance conclusions. Existing raw JSON can be
rechecked without running db_bench by using `audit_existing.py --input OLD.json
--output NEW-audit.json`; the audit contains the original JSON hash and a
compact verdict for every trial while leaving the original report untouched.
It retains the benchmark binary SHA-256 from both the start and end of the
original run; a mismatch or missing end hash prevents a `complete` audit.
`hotspot_adaptation_us` is the first post-switch promotion window's end
relative to the switch; null means no observed promotion. This is a
placement-response proxy, not proof that the new hot keys saw lower latency.
No post-switch promotion can be expected if the selected working sets already
fit at the budget; it is recorded as null and does not alone invalidate a
trial. The report counts observed post-switch promotions separately.
If the window is too short for two promotion rounds, three demotion rounds and
minimum residency, increase the fixed measured operation count for a new
experiment. Report any unobserved behavior explicitly. Live value verification
does not replace the dedicated published-point whole-SSD-loss recovery tests.

## Evidence

See [the validation record](results.md) for current correctness, build and
smoke evidence and the completed same-device Release comparison. Its
[compact 105-trial summary](standard-summary.json) preserves every measured
window as well as all three repeats for each of 35 workload/configuration
groups. Reproduce it from the raw JSON with:

```sh
python3 docs/components/metabypass/experiments/sst_tiering/summarize_standard.py \
  --input /tmp/mb-sst-standard-v1.json \
  --output /tmp/mb-sst-standard-v1-summary-recheck.json
```

The summary reports each trial's median and maximum window P99, then the
median and range across repeats. These window percentiles cannot be combined
into an exact whole-trial P99. The local run used one ext4 device for both
paths and no injected read delay; it measures mechanism and path occupancy,
not physical HDD behavior or SSD-device capacity savings. The fixed offered
operation rate is not a maximum-throughput test.

## node-ssd physical-device protocol (`node_v1`)

`node_run.py` is a separate protocol; it does not change the fixed-operation
`run.py` or its old reports. Deploy the release `db_bench`, this script directory
and a JSON source manifest captured from the actual build checkout. Python 3.8+
with the standard library is sufficient. Put the executable on SSD (the HDD
mount can be `noexec`) and put control/output files outside the two experiment
parents. The preconfigured private cgroup must be empty, with writable `tasks`,
a 96 MiB limit and `memory.swappiness=0`. The runner does not change it.

```sh
python3 /path/on/ssd/scripts/node_run.py \
  --binary /path/on/ssd/db_bench \
  --source-manifest /path/to/control/source-manifest.json \
  --ssd-root /home/lj/ssd/metabypass_test \
  --hdd-root /home/lj/hdd/metabypass_test \
  --cgroup /sys/fs/cgroup/memory/mbsst-20261001 \
  --output /path/to/control/node-results.json
python3 /path/on/ssd/scripts/node_summary.py \
  --input /path/to/control/node-results.json \
  --output /path/to/control/node-summary.json
```

Existing experiments may be present in both parents. Each run creates a new
UUID child; every trial creates fresh owned children below that child. Ownership
records contain UUID, hostname, UID, PID and Linux process start time. Cleanup
requires a matching direct owned child and a successful result. Failed, timed
out and incomplete samples stay on disk and in the report. Process termination
checks PID/start time/process group and targets only the task's own process
group. The controller and monitor stay outside the cgroup. An independent
exec helper validates and applies any requested CPU affinity before joining
`tasks` and before launching seed creation, dataset copying,
benchmark opening or recovery, so their subsequently created threads inherit
the cgroup and affinity. The default legacy profile inherits the controller's
allowed CPU set without binding. No `preexec_fn`, privileged operations or global cache changes are
used. A watchdog kills a running owned group at its round/global deadline.

Calibration begins the four-hour hard budget and may use at most 30 minutes.
A disabled seed uses 4,000,000 16-byte keys and 1 KiB values, then Flush,
CompactRange, SyncBackup and Close. A complete attempt includes the seed,
disabled/adaptive saturation calibration and diagnostic observe. Failure of
cache/pressure gates triggers at most one complete 6,000,000-key recalibration;
both attempts share the same 30-minute deadline. Missing evidence blocks formal
work rather than shortening calibration or changing the cgroup.

An independent monitor outside the cgroup uses read-only lazy `mmap` and Linux
`mincore` every 10 seconds, querying all pages without reading mapped SST data,
using `fadvise`, evicting cache or modifying source files. Disabled mode queries
`index/*.sst`. Tiered mode checks `SST-PLACEMENT` identity/CRC and chooses the
current hot SSD path or cold immutable HDD object once per table number. It
excludes protection copies and a cold route's leftover SSD copy; unmatched new
SSD SST files must agree with current live/unprotected byte telemetry. Placement
changes, missing/replaced files and ambiguous live sets produce partial samples
with unknown resident fraction, never a zero-resident claim.

Both disabled and adaptive must have complete post-warmup/saturated samples with
at least 90% logical live SST coverage and at most 90% resident bytes. Raw samples
retain per-file path/route, covered/resident bytes, coverage, timestamp and CPU
cost. The supplemental capacity ratio also requires SST logical bytes at least
1.5 times `96 MiB - minimum calibrated saturated RssAnon - block cache`; this
anonymous-RSS proxy is not substituted for actual SST residency evidence. Total
cgroup cache and device I/O include blob pages and cannot prove SST residency.
Missing actual physical reads on either device, failed calibration or missing
adaptive read samples and migration/cold-placement evidence also block formal
work. An OOM requires investigation of benchmark RSS; the runner never changes the
cgroup limit. `memory.failcnt` is recorded as pressure evidence, not an OOM
count. Both modes explicitly use an 8 MiB queue, a 256 KiB batch, a one-second
background interval and a 64 KiB block cache.

The seed is read-only. Each fresh dataset uses ordinary `copy2` for its first
inode and recreates only that trial's internal hardlinks on the same destination
device. It never shares writable inodes with the seed or another trial. The
three DB/data/backup directories retain identity and relative publication
metadata; no absolute-path text rewriting is performed. A new-path Open and
published-point restore smoke must pass before the physical experiment.
Calibration runs disabled/adaptive/observe at the same frozen 50% SST budget,
with a 60-second route preheat at offered 10,000 ops/s and a 30-second saturated
phase. Observe is diagnostic only. The formal offered rate is frozen once at
70% of the lower disabled/adaptive actual saturation throughput. Calibration
preheat can fall behind its offered rate; it is not a performance comparison.

Formal work is exactly uniform/switch/mixed x disabled/adaptive x three repeats
(18 independent copies), with mode order reversed in repeat two. Every round
uses 60 seconds of warmup, 120 seconds at the frozen rate and 30 seconds of
saturation. Switch changes the active hot quarter halfway through fixed
measurement, with an 80/20 hot/complement selection; saturation retains the
new hot quarter. Mixed explicitly flushes every 10 seconds. Phase output reports
completed and unfinished offered operations; response latency includes delay
behind the offered schedule for completed requests. For fixed-rate phases,
`late_ops` counts completed requests whose service start is more than
`ceil(1000000 / target_ops_per_sec)` microseconds after the scheduled arrival.
`late_fraction` is late_ops divided by completed ops; both are null for saturated
phases or zero completed requests. `unfinished_ops` instead counts scheduled
requests not completed by phase end. The runner estimates copy
and execution time from calibration, requires each round including copy to fit
eight minutes, and reserves enough remaining global budget for all remaining
rounds and the six recovery samples. It stops with a retained blocked report
if the plan cannot fit; it does not shorten phases or reduce repeats.
Missing formal mechanism coverage is kept visible, excluded from valid
performance comparisons, and produces `complete_without_mechanism`.

Process RSS/CPU/I/O, FD count, thread count, per-thread last processor and allowed
CPU set, cgroup memory/cache/queue-related statistics and dynamically
resolved `/proc/diskstats` devices are sampled about once per second. Physical
space is scanned in a monitor thread every 5-10 seconds, with scan CPU/wall time
recorded. Cgroup v1 `memory.kmem.usage_in_bytes`, `memory.kmem.max_usage_in_bytes`
and `memory.kmem.failcnt` are read with the one-second numeric samples.
`memory.kmem.slabinfo` is retained at preflight, each exec's beginning/end and
alongside the 5-10 second space scans. Missing or unreadable kernel-memory and
process-resource evidence is explicitly unknown rather than zero. Per-thread
processor values are the last scheduled CPU, so polling is not a complete trace
of migrations. Each trial's SSD and HDD are one primary `(st_dev, st_ino)` deduplicated
scope using `st_blocks * 512`; SST/blob/temp/metadata category counts can overlap
when one inode has paths in several categories. `run_scope` separately includes
the seed, active trial and retained failures on both devices and is used for
experiment capacity monitoring, not per-trial cost comparisons. Racing unlinks
make a sample explicitly incomplete and cannot become zero saved bytes. The
summary reports measured steady SSD SST occupancy, sampled maximum, exit and
all-SSD occupancy plus paired HDD/total-physical differences. Polling peaks are
sampled, not exact. Device I/O mixes blob and SST traffic; heat and placement
counters do not identify the device serving each request.

Six additional independent samples cover disabled/adaptive at 60/75/90 seconds
after the writer's `ready.steady_time_us` on the shared Linux monotonic clock,
not after stdout consumption. Ready timestamps must lie between process start
and receipt; pipe lag, actual fault elapsed time and cut jitter are retained.
`rpo_write` atomically writes a business value and sequence
marker, then emits `ack{seq,steady_time_us,business_bytes}`. The external
controller journals ACKs outside all DB directories, flushes and fsyncs the
journal before injecting SIGKILL without Close, and retains stdout and command
identity. Recovery uses a new empty SSD target and the trial's HDD published
point; it cannot use the original SSD files. `recovered` reports the recovered
marker, validated matching business value, monotonic timestamp and RTO to first
marker/business read, followed by `first60s_read` performance. Lost batches and
bytes are counted only from externally acknowledged sequences above the
recovered marker. With loss, RPO is fault time minus the last recovered ACK time;
without loss it is zero. A marker beyond the last ACK maps to the last actually
received ACK. With no recovered ACK, RPO is unknown and a lower bound is stated.
All six samples and their ranges are listed; they do not establish RPO P99.

New events carry `protocol=node_v1`. `histogram_schema.upper_bounds_us` defines
193 latency bins with four subdivisions per doubling. Every `window` and
`phase_end` carries separate Get/Put/Delete service and response histograms;
`phase_end` is cumulative for the entire phase. Summary percentiles use those
actual counts and bucket upper bounds, never averages of window percentiles.
Raw commands, wrapper commands, binary SHA-256 at start/end, source manifest,
preflight mount/device/cgroup state, all failures/timeouts and every recovery
sample remain in JSON. The summarizer emits Markdown and standard matplotlib
SVG/PNG figures when matplotlib is already installed; otherwise it emits
standalone technical SVGs without installing dependencies.

### Explicit smaller protocol (`--profile small`)

The default remains `--profile legacy`. `small` starts a new independent task,
UUID and report; it does not resume the previous four-hour calibration. Use a
separate, manually prepared 48 MiB cgroup with writable empty `tasks` and
`swappiness=0`; the old 96 MiB group is kept. The runner verifies the requested
limit and never creates, resizes or substitutes a cgroup.

Small helpers default to Linux CPU affinity `0,1,2,3`, the four selected physical
cores on node-ssd. This limits the CPUs used by all seed/copy/DB/recovery threads
while keeping the controller and monitor outside the memory group. Use
`--cpu-list 0-3` (or another explicit comma-separated list/range) to declare a
different allowed set. Preflight and the helper reject requested CPUs outside
the parent's allowed set before launching work or writing `tasks`; they never
change system CPU settings. The helper verifies the effective affinity before
cgroup entry and reports it with the parent allowed set in source identity and
each command. This is a benchmark execution setting and should be kept identical
across compared modes. Legacy still defaults to no explicit binding.

An empty `tasks` file does not imply zero memory charges. The preflight and every
exec retain the full usage, residual kmem and remaining headroom against the
fixed limit. Residual charges are not reset or ignored and do not by themselves
trigger an arbitrary threshold rejection. `memory.stat` cache/rss counters do
not account for every charge; retained per-CPU slab caches can remain charged
after FDs close and processes exit. The sampled kmem peak and increase over the
exec baseline are distinct from the cgroup's lifetime max counter. Slab rows,
FD/thread samples and a fresh-group CPU-affinity A/B are evidence for diagnosis;
residual slab usage alone does not prove an FD leak. No cgroup counters or kernel
caches are cleared by the runner.

```sh
python3 /path/on/ssd/scripts/node_run.py --profile small \
  --binary /path/on/ssd/db_bench \
  --source-manifest /path/to/control/frozen-source-manifest.json \
  --ssd-root /home/lj/ssd/metabypass_test \
  --hdd-root /home/lj/hdd/metabypass_test \
  --cgroup /sys/fs/cgroup/memory/NEW_PRIVATE_48M_GROUP \
  --output /path/to/control/new-small-results.json
```

The fixed dataset has 1,000,000 16-byte keys and 1 KiB values. There is no automatic
larger seed or lower memory fallback. Queue capacity is 2 MiB; the 256 KiB batch,
one-second backup interval, 64 KiB block cache, 1 MiB memtable/SST target, and
strategy timings remain the same. The strategy evaluates every second with a
10-second heat half-life, a 10-second minimum residency, two promotion rounds,
three demotion rounds and one-in-64 read sampling. The three workload types,
50% SST budget, 80/20 hot-quarter selection and mixed flush every 10 seconds
are preserved. This is a new scale/duration protocol, not an equivalent replay
of the earlier performance or RPO distribution.

The hard total deadline is 90 minutes, beginning with calibration and including
seed creation, all ordinary copies, the recovery probe and final recovery.
Waiting does not extend it. Calibration gets at most 15 minutes and creates one
seed, runs disabled/adaptive saturation calibration and observe once each,
and makes one additional independent adaptive recovery timing probe. Calibration
uses warmup 30 seconds, no fixed measurement and saturation 15 seconds. Both
main modes must pass actual routed-SST `mincore` coverage/residency, device read
I/O and adaptive mechanism gates. The old 1.5 SST/cache capacity ratio is retained
only as a diagnostic; it is not a hard gate for `small`. Missing or partial
residency evidence differs from complete samples dominated by cache. Neither
condition authorizes a cgroup change or dataset expansion.

The single recovery probe is killed at driver ready + 10 seconds and restores
from HDD into a fresh empty SSD target using the unchanged production checksum
and recovery path. It runs a 10-second read preheat solely to measure complete
Restore/Open/Close overhead. It is reported separately and contributes no formal
RPO sample; a recovered zero marker does not establish zero RPO. Writer timeout
includes measured startup plus cut time and margin. The probe's restore is
bounded by the common 15-minute calibration deadline rather than an assumed
120-second full-path timeout.

Formal work remains 18 independent rounds: three workloads, two modes and three
repeats with paired alternating order. Each round uses warmup 30 seconds,
fixed measurement 60 seconds and saturation 15 seconds. The uniform offered rate
is frozen at 70% of the slower disabled/adaptive measured calibration saturation
throughput. A complete round forecast is
`max(actual calibration copy + execute) - 45 + 105 + 30 seconds`; it includes
observed Open/Close costs and one 30-second safety margin, with no extra scale
factor. The hard per-round limit remains eight minutes. Each measured phase gets
its own residency gate. Correct samples without mechanism or non-full-cache
evidence remain in the report and are excluded from valid performance comparisons;
they do not silently disappear or become zero saved bytes. Global SST residency
is not the cache-miss rate of hot requests.

Formal recovery still has six separate samples at ready + 60/75/90 seconds for
each mode, with 60 seconds of first-read performance. Remaining-budget checks
reserve *all* remaining samples and include ordinary copy, measured writer
startup, fault wait, complete Restore/Open, 60-second reads, Close and margin.
The same full-path costs set writer/restore timeouts. A single adaptive probe
and the larger calibration overhead provide shared conservative estimates;
this is not a guarantee that both modes have identical recovery cost. Actual
failure, OOM, partial results and timeout evidence is retained. If the full
remaining plan cannot fit 90 minutes, the runner stops with a blocked report
and does not reduce repeats, fault cuts or read durations.

The report records the profile, fixed parameters, actual limits and both
absolute deadlines. Summaries preserve invalid phase/schema diagnostics while
still reporting intact sibling phases. Missing or truncated sample logs are
recorded as unknown measurements. SVG/PNG charts and repeat counts use only
valid comparisons; the raw report retains every attempted sample.
