# SSD index and HDD blob storage

Without staging, MetaBypass routes ordinary Integrated BlobDB files directly to
`data_dir`, while keeping native index files in `index_dir`. WAL and memtable
contain full values when `enable_blob_direct_write=false` (the native default).
With `enable_blob_direct_write=true`, writes create Blob references directly;
set `allow_concurrent_memtable_write=false`. Both modes use `min_blob_size=0`.

A synchronous write and SyncBackup both wait for a durable HDD recovery point.
Checkpoint capture uses native live-file enumeration with file deletion disabled
and uses a no-flush WAL-inclusive inventory when DirectWrite is off. When it
is on, capture requests a native flush and a WAL-free inventory; publication
waits for all required HDD Blob, SST and metadata bytes. WAL-only recovery is
supported in the default false mode. Close performs
its final checkpoint barrier before destroying the native DB.

Restore copies and verifies native files, then ordinary RocksDB Open replays the
WAL when present. DirectWrite fault recovery starts from a published point
through Restore; active-Blob WAL local crash reopen is outside this contract.
No business WAL/SST/blob scanner, blob sealing or MANIFEST repair is part of
the backup or restore path. The current format rejects old databases and backups
without migration. See the [component entry point](index.md) for API, format,
metrics and benchmark details, and [blob GC](blob-gc.md) for whole-file retention.
