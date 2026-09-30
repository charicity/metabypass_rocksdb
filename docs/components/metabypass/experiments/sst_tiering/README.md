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
