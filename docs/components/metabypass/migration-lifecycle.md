# Migration lifetimes

Blob and SST migration share the need to publish a durable destination before
switching reads, but have different capacity and writer contracts.

Blob Append atomically reserves from a hard SSD quota. A quota miss freezes the
current local prefix and permanently switches that file to HDD. A separate
worker persists the prefix and route, then accepts HDD appends in 4 MiB extents.
Old local read handles pin SSD unlink only; they do not hold the file's HDD
append path. HDD read pins retain immutable descriptor versions. Sync/Close
publishes a durable boundary before native RocksDB can reference the blob in an
installed SST. Recovery ignores unsynced appended tails.

SST disabled mode preserves ordinary SSD index placement. Observe-only mode
records policy decisions without moving files. Adaptive mode moves complete
immutable SSTs protected by a published recovery point. Its SSD budget remains a
soft logical placement budget and is distinct from the hard blob staging quota.
Promotions/demotions publish routing before reclaiming the old physical source;
read handles opened before that change retain their source until completion.

Checkpoint candidates pin native deletion across live-file capture and preserve
their root through publication or cancellation. Whole-file GC requires native
obsolescence, all backup roots released, and all physical read/migration pins
completed. See [staging](tiered-storage.md), [SST tiering](sst-tiering.md), and
[GC](blob-gc.md) for the associated contracts and metrics.
