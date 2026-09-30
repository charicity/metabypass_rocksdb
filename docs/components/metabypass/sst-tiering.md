# Whole-SST SSD/HDD tiering

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
for demotion, 10 s minimum residence and a 25% replacement advantage. One
migration worker copies at a default 32 MiB/s limit.

```cpp
rocksdb::MetaBypassOptions bypass;
bypass.data_dir = "/experiment/hdd/data";
bypass.backup_dir = "/experiment/hdd/backup";
bypass.sst_tiering.mode = rocksdb::SstTieringMode::kAdaptive;
bypass.sst_tiering.ssd_capacity_bytes = 512ULL * 1024 * 1024;
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
