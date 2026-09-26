# Direct slow-storage writes with index protection

The main research configuration writes Blob Direct Write payloads directly to
`data_dir` on the slow device. Native SST/WAL/MANIFEST files remain in the primary
index directory on the fast device, and Metabypass copies native index files to
`backup_dir` on the slow device. Leave `staging_dir` empty. Fast blob staging and
migration remain optional and are excluded from this experiment.

```cpp
MetaBypassOptions bypass;
bypass.data_dir = "/slow/blobs";
bypass.backup_dir = "/slow/index-backup";
// staging_dir stays empty; there is no blob migration worker or staging budget.
```

## Completion contracts

Both direct and tiered modes now use the same successful-write contract:

- `sync=false`: the primary write completes without waiting for a backup point.
  Queue capacity, the direct slow payload write, and shared I/O resources can
  still delay it. This is not a promise of nonblocking writes.
- `sync=true`: the primary synchronous write completes, then a complete backup
  point covering that write is published. The wait holds no tiered admission
  lock. In direct mode this strengthens the previous local-sync-only contract.
- `SyncBackup()`: a published complete point covers writes completed before the
  call. Successful close finishes primary close and final publication.

The slow payloads and index backup must survive loss of the entire fast device.
No fixed RPO is promised for asynchronous writes. An error from the backup
barrier can follow an already visible primary mutation; it does not imply
rollback. Errors remain sticky, and ordinary reopen does not silently choose an
older backup. Explicit restore uses only a published complete point.

This changes synchronous-write latency for existing direct-mode instances but
requires no format migration or new option. `sync_write_micros` measures the
post-primary backup barrier in either mode; concurrent waits can overlap, so its
sum is not elapsed wall time. Synchronous writes do not force a memtable flush.

## Experiments

See the [comprehensive execution plan](direct-storage-test-plan.zh-CN.md).
The standalone [driver and runner](experiments/direct/) compile the same source
against the fork and unmodified upstream v11.8.1. This external experiment has
its own CMake project; it is not linked into the RocksDB library or db_bench.
Existing db_bench direct-mode commands continue to work, but their narrow
sequential workload is not a replacement for the planned comparison matrix.

No performance results are supplied with this change. Test and benchmark
execution was explicitly deferred to a separate task by the user.
