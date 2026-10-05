# Whole-SST SSD/HDD tiering

See the shared [migration lifecycle contract and scenario matrix](migration-lifecycle.md)
for persistence, route switching, physical I/O draining and reclamation.

Metabypass can place complete immutable index SST files in a fast index
directory or in the backup directory's `sst-store`. The cold store holds the
same protected SST content used for online reads and published recovery points.
The ordinary RocksDB compaction policy, blob placement, blob reference checks,
and point-publication protocol still govern their respective work. Blob garbage
collection is outside this feature.

## Contract and configuration

`MetaBypassOptions::sst_tiering` is independent of blob staging. Its default
`SstTieringMode::kDisabled` leaves all SSTs in the SSD index directory while
Metabypass still protects them in the backup. `kObserveOnly` runs the tiering
routing, sampling, decision thread, protected SST links and placement metadata,
but records migration intentions without moving SSTs. Its overhead is the
whole observation path, not a pure sampling ablation. `kAdaptive`
moves only protected immutable SSTs. New ordinary writes may temporarily exceed
the soft SSD SST budget; a usable published point and safe file lifetime take
priority over budget enforcement. An SSD loss can restore a complete published
point; writes after that point may be lost. A normal live `work/` mirror is not
an authorized restore source.

The SSD budget counts SST bytes only. The default policy reserves 10% of it for
new files, evaluates every 1 s, and halves prior heat every 10 s. It samples
one in 64 SST-backed foreground `Get` file reads after block-cache misses.
These reads may hit the operating system page cache, so they are not physical
device-read counts. Scans have a separate count and do not promote files;
background I/O does not train the point-read heat score. The policy scores
sampled reads relative to SST size and uses two rounds for promotion, three
for demotion, 10 s minimum residence and a 0.25 replacement sorting bias. One
migration worker copies at a default 32 MiB/s limit.

The project's current recommended profile when enabling SST tiering is
`default80`: set the nominal SSD SST budget to 80% of a fixed baseline's
full-SSD logical SST bytes. The 10% reserve leaves an effective policy budget
of approximately 72% of that baseline, subject to integer rounding and whole-SST
placement. This is not 80% of the SSD device capacity or of the hot data, and
the library does not recalculate it as the database grows. The caller measures
and fixes the baseline, explicitly enables `kAdaptive`, and supplies the
absolute `ssd_capacity_bytes`; the library default remains `kDisabled`.

The [two-device tradeoff report](../../../reports/sst-tradeoff-20261004/README.md)
records the choice and its limits. In those read trials, `default80` saved
28.05% of allocated SSD SST space with 3.85-6.63% saturated throughput loss;
the second uniform repeat's fixed-rate response P99 reached 131.072 ms.
This recommendation does not establish a stable latency sweet spot.

```cpp
// Fixed full-SSD logical SST baseline: 1 GiB, measured before tiering.
constexpr uint64_t baseline_sst_bytes = 1ULL << 30;
rocksdb::MetaBypassOptions bypass;
bypass.data_dir = "/experiment/hdd/data";
bypass.backup_dir = "/experiment/hdd/backup";
bypass.sst_tiering.mode = rocksdb::SstTieringMode::kAdaptive;
// floor(baseline * 80 / 100), without overflowing baseline * 80.
bypass.sst_tiering.ssd_capacity_bytes =
    baseline_sst_bytes / 100 * 80 + baseline_sst_bytes % 100 * 80 / 100;
bypass.sst_tiering.reserve_percent = 10;
bypass.sst_tiering.promote_rounds = 2;
bypass.sst_tiering.demote_rounds = 3;
bypass.sst_tiering.min_residency_ms = 10000;
bypass.sst_tiering.replacement_margin = 0.25;
// Nominal: 858993459 bytes; effective: 773094114 bytes (about 72%).
// Other sampling, heat, evaluation and migration parameters keep their defaults.
// Pass an SSD path as MetaBypassDB::Open's index_dir argument.
```

Use absolute, normalized, disjoint directories. All parent directories must
exist. `ssd_capacity_bytes` is required for observe and adaptive modes.
`MetaBypassDB::GetBackupStats().sst_tiering` reports placement, sampled and
dropped reads, scan reads, migration counts and bytes, budget time, and policy
observations. Its byte counts are logical SST accounting. For physical space,
inspect allocated blocks once per device/inode across index, backup `work/`,
published `point-*` and `sst-store`; hardlinks are not extra physical copies.

The feature is experimental and scoped to the C++ MetaBypass wrapper. It does
not add a general RocksDB option, C or Java API, or an automatic layout for
existing ordinary databases. Offline restore must use an empty or marked
retry destination and the same durable data and backup directories. The data
and backup directories must outlive loss of the SSD index. Local directory
deletion tests do not establish power-failure durability of a real device.
An existing tiered library with placement metadata cannot be silently reopened
in disabled mode. Observe mode can still read cold files left by an earlier
adaptive run.

During offline SSD-loss restore, the wrapper verifies the published point,
prepares every referenced cold SST object, then writes one placement map before
native RocksDB recovery reads the logical SST files. An empty SST set writes an
empty map. If preparation or the map commit fails, the `METABYPASS-RESTORING`
marker keeps the destination closed to normal opens; retrying restore from the
same published point rebuilds the map. Unreferenced objects from an interrupted
attempt are removed by the normal placement load after a successful restore.

## Validation status

The experiment protocol and commands are in
[experiments/sst_tiering/README.md](experiments/sst_tiering/README.md).
The [validation record](experiments/sst_tiering/results.md) contains the
completed same-device Release run: 105/105 value-valid trials, 45/45 adaptive
mechanism coverage, and the full three-repeat [window-level summary](experiments/sst_tiering/standard-summary.json).
Read-oriented adaptive trials reduced SSD-path SST occupancy at all three
budgets; mixed write/compaction trials did not show stable budget control.
The benchmark's fixed offered rate and shared ext4 device cannot establish
maximum throughput, real-HDD latency or separate-SSD capacity savings. Its
P99 values are window-level percentiles, not exact whole-trial percentiles.
A mechanism run with no measured SST read samples or qualifying migration/
cold-state evidence is marked as incomplete coverage even when value checks
pass. Published-point SSD-loss recovery is validated by the dedicated
`metabypass_test` cases; the benchmark's final verification checks its live
index and does not simulate whole-SSD loss.
