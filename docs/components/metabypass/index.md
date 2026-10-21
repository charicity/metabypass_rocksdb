# Metabypass native checkpoints

Metabypass is an experimental single-column-family RocksDB wrapper. It uses
Integrated BlobDB (`enable_blob_files=true`, `min_blob_size=0`). The native
`enable_blob_direct_write` default remains false: full values enter the WAL and
memtable before native flush creates blob files and SST blob references.
Setting it true restores direct Blob writes and requires
`allow_concurrent_memtable_write=false`; native WAL/memtable records then hold
Blob references. Value relocation is disabled;
whole-file blob reclamation is supported.

```cpp
rocksdb::Options options;
options.create_if_missing = true;
options.enable_blob_direct_write = true;
options.allow_concurrent_memtable_write = false;
rocksdb::MetaBypassOptions backup;
backup.data_dir = "/experiment/data";
backup.backup_dir = "/experiment/backup";
std::unique_ptr<rocksdb::MetaBypassDB> db;
auto s = rocksdb::MetaBypassDB::Open(options, backup, "/experiment/index", &db);
if (s.ok()) s = db->Put(rocksdb::WriteOptions(), "key", "value");
if (s.ok()) s = db->SyncBackup();
if (db) {
  auto close = db->Close();
  if (s.ok()) s = close;
}
db.reset();
```

Check every Status. The supplied Env must outlive the wrapper. Destroy iterators
before Close. Write, Get, SyncBackup, Flush, and CompactRange may run
concurrently; Open, Restore, and Close require exclusive application ownership.
All mutations must pass through the wrapper. Transactions, Merge, multiple
column families, dynamic configuration, and mutable access to the underlying DB
are outside this API.

Use absolute normalized disjoint index, data, backup, and optional staging
directories with exclusive ownership. The current database/backup format marker
is `MBN2`, inventory version is `MBI2`, and tier mapping version is `MBT2`.
Open and Restore reject older or unmarked nonempty directories before modifying
them. There is no automatic migration or old-format recovery path.

## Capturing a recovery point

The backup mirror drains bounded WAL and metadata event queues independently of
the checkpoint coordinator. Queue memory includes in-flight events; a single
file operation larger than `queue_capacity` fails before primary file I/O.
The mirror preserves accepted event order across both channels.

The coordinator calls native `DisableFileDeletions` and
`GetLiveFilesStorageInfo`. With DirectWrite off it uses the no-flush threshold
and includes WALs. With DirectWrite on it requests a native flush and excludes
WALs from the captured inventory. RocksDB supplies the
consistent live file set, captured lengths, and replacement CURRENT contents.
The coordinator waits for the corresponding mirror fence, copies mutable
prefixes and hardlinks or copies immutable files into a candidate, then releases
the native deletion pin. Ordinary WAL-only points remain valid with
DirectWrite off. DirectWrite points contain the native flush result, Blob
references, and no WAL dependency. In both modes all required Blob/SST/metadata
bytes must be durable on HDD before LATEST is published.

`MetaBypassOptions::checkpoint_interval_ms` defaults to 5000 and controls only
periodic point capture; `interval_ms` continues to control mirror batching.
SyncBackup, synchronous writes and Close request barriers independently of the
periodic interval. The interval is a scheduling target, not a fixed RPO.

The candidate persists its required blob bytes, native file inventory and
checksums before replacing LATEST durably. Backup does not parse business WAL
records, scan SST blob references, seal blob files, or repair MANIFEST records.
The native RocksDB recovery path interprets the copied index.

At most one candidate is being prepared. Candidate, latest, previous, and
retired-point roots protect their blob dependencies until released. A separate
GC worker deletes retired point directories; physical blob deletion also waits
for native obsolescence and all recovery roots. A blocked coordinator permits
the mirror to drain. A backup failure is sticky, wakes waiters and rejects
subsequent mutations while reads remain available.

`SyncBackup` and `WriteOptions::sync=true` wait for a durable HDD recovery point
covering completed writes. A remote failure can be reported after the primary
mutation succeeded. Close completes its final backup barrier while the DB is
still alive, then closes the DB and joins the workers. Writes newer than the
latest published point may be lost after a crash; there is no fixed RPO.

## Restoring

After releasing the original wrapper, call Restore with an empty destination
index and then Open it. Restore verifies the selected inventory and copies its
native files. With DirectWrite off, native Open replays full-value WAL records.
With DirectWrite on, the published flush and its Blob dependencies supply the
restored data. Fault recovery is supported from a published point through
Restore. Reopening a crashed local index whose WAL references an active Blob
file is outside the DirectWrite contract. No blob sealing or destination
MANIFEST repair occurs. Restore preserves its published source, supports a
marked interrupted destination, and reports corruption without silently choosing
an older point.

With staging enabled, both index and staging can be lost together. Published
blob mappings and payload extents on HDD supply values without full SSD
rehydration. The data and backup directories must survive that loss.

## Storage and metrics

[Blob staging](tiered-storage.md) describes its hard quota and active-file spill.
[Whole-file GC](blob-gc.md) describes root protection.
[Whole-SST tiering](sst-tiering.md) describes disabled, observe-only and adaptive
modes. [Migration lifecycle](migration-lifecycle.md) records read pin boundaries.

`checkpoint_wait_micros` records native checkpoint coordination wait time.
`mirrored_bytes` counts all mirrored append payloads, including metadata and SSTs;
`mirrored_wal_bytes` isolates full-value WAL append payloads. `staging_spill_files` and
`staging_spill_bytes` describe files switched to HDD and their spilled bytes;
`tier_queued_bytes` / `peak_tier_queued_bytes` describe pending HDD worker payload;
`hdd_extents` counts HDD extents. Staging peak includes reserved local append
bytes and must remain within `staging_capacity`. Queue, sync, migration, point
lag, candidate copy and GC metrics remain separately observable.
`retained_index_bytes` counts logical paths, including hardlink sharing, rather
than allocated disk blocks. Physical capacity should be measured externally.

The standalone db_bench `baseline`, `write`, `verify`, `restore`, and
`sst_tiering` modes accept the explicit native DirectWrite switch. Write mode times
foreground WAL writes, explicit Flush, SyncBackup and Close separately, so the
flush phase actually exercises blob staging/spill. Its SST experiment output
remains JSON. Example build:

```sh
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 db_bench
./db_bench --metabypass_mode=write --db=/experiment/index \
  --metabypass_data_dir=/experiment/data \
  --metabypass_backup_dir=/experiment/backup --num=100000 --value_size=1024 \
  --enable_blob_direct_write=true --allow_concurrent_memtable_write=false \
  --metabypass_checkpoint_interval_ms=5000 --sync=false
```

`metabypass_test` covers WAL-only recovery, no forced flush, concurrent writes,
checkpoint boundaries and root lifetimes, oversized spill, quota competition,
read pins, sticky failures, corruption and SIGKILL boundaries.
`sst_tiering_test` covers SST routing and policy lifetimes.

The [DirectWrite comparison protocol](experiments/direct-write/README.zh-CN.md)
provides four independent arms, three rounds, mechanism gates and a report
template. No new performance result is claimed before execution.

See the [DirectWrite implementation and validation handoff](direct-write-validation.zh-CN.md)
for this change's verification status, retained failures and pending work.
The [native checkpoint validation record](native-checkpoint-validation.zh-CN.md)
preserves the earlier DirectWrite-disabled implementation's results.

The experiment reports and data below this directory describe earlier measured
revisions. Reports mentioning DirectWrite, record scanners, blob sealing or
incremental validation caches are historical evidence, not current behavior or
format compatibility claims. Their numeric results must not be attributed to
the native checkpoint implementation without rerunning the experiments.

See [历史实验分类与证据索引](experiments/history.zh-CN.md) for the classified
reports, source data, protocol boundaries, retained failures, and evidence availability.
