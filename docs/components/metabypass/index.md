# Metabypass native checkpoints

Metabypass is an experimental single-column-family RocksDB wrapper. Values stay
inline in the WAL and SSTs. The primary index and backup each have an absolute,
normalized directory with exclusive ownership. Optional [whole-SST tiering](sst-tiering.md)
places protected, immutable SSTs in the backup directory's `sst-store`.

```cpp
rocksdb::Options options;
options.create_if_missing = true;
rocksdb::MetaBypassOptions backup;
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

The supplied Env must outlive the wrapper. Destroy iterators before Close.
Write, Get, SyncBackup, Flush, and CompactRange can run concurrently. Open,
Restore, and Close require exclusive application ownership. All mutations go
through the wrapper. The write API supports WAL-backed Put and Delete batches.

The index and backup ownership markers use `MBN7`. Each published `LATEST`
names a `point-<generation>` containing an `MBI7` inventory of native files
and a `RECOVERY` record with the checkpoint sequence and WAL floor. Open and
Restore require the current format. Start new experiments with fresh
directories.

## Capturing a recovery point

The mirror drains independent WAL and metadata queues while the checkpoint
coordinator runs. Their limits default to 896 MiB and 128 MiB respectively and
include reservations and in-flight events. An event that exceeds its queue's
limit fails before primary I/O. The mirror preserves event order across both
queues. It wakes after 512 MiB of combined pending data or 1000 ms by default.

The coordinator uses native `DisableFileDeletions` and
`GetLiveFilesStorageInfo` to capture live files including WALs without forcing
a memtable flush. It waits for the mirror fence, copies mutable prefixes,
and links or copies immutable files into a candidate. It syncs the candidate's
files and metadata before replacing `LATEST`. The published point and retained
WALs protect the recovery prefix while the GC worker removes retired points.
A backup failure is sticky, wakes waiters, and rejects later writes.

`checkpoint_interval_ms` defaults to 40000 ms and controls periodic capture.
SyncBackup, synchronous writes, and Close request barriers independently of
that timer. A successful barrier covers completed writes. A remote failure
can be returned after the primary mutation succeeds. Close publishes a final
point, closes the primary DB, and drains close operations. The application
releases directory locks when it destroys the wrapper.

## Restoring

After releasing the original wrapper, call Restore with the surviving backup
and an empty destination index, then Open the destination. Restore verifies
and copies the selected point and uses native RocksDB recovery to replay the
continuous prefix of surviving WALs. A marked interrupted destination can be
retried. Corruption of the selected point is reported to the caller.

Recovery time sampling defaults to enabled. Set
`enable_recovery_time_sampling=false` before Open and Restore to omit periodic
PROGRESS appends and recovery-time estimates. Restore still clears PROGRESS once
when finalizing the sequence timeline. The recovered sequence is available in
`MetaBypassRecoveryReport`.

## Benchmarks and metrics

`MetaBypassStats` exposes queue, checkpoint, point lag, candidate copy, GC, and
filesystem I/O counters. `mirrored_wal_bytes` isolates WAL payload from total
mirrored bytes. `retained_index_bytes` adds logical file lengths per path.
Measure physical space using allocated blocks deduplicated by device and inode.

The standalone `db_bench` modes are `baseline`, `write`, `verify`, `restore`,
and `sst_tiering`. Write mode measures foreground writes, Flush, SyncBackup, and
Close separately.

```sh
AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 db_bench
./db_bench --metabypass_mode=write --db=/experiment/index \
  --metabypass_backup_dir=/experiment/backup --num=100000 --value_size=1024
```

- [Quick correctness checks](experiments/quick/README.zh-CN.md)
- [Native checkpoint performance](experiments/native-checkpoint/README.zh-CN.md)
- [SST placement experiments](experiments/sst_tiering/README.md)
- [WAL and BackupEngine diagnostics](../../../tools/metabypass_acceptance/HDD_SYNC.md)

Reports from earlier revisions retain the original measurement scope and
format names. See the [historical experiment index](experiments/README.md).
