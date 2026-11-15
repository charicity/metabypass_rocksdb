* MetaBypass now retains one published checkpoint and uses MBN6/MBI6 metadata without backup payload digests. Older formats are rejected without automatic migration.
* MetaBypass recovery passes ordinary WAL batches through without rebuilding their values, reducing memory use for large batches.
* MetaBypass advises dropping cached pages for durable independent checkpoint copies, while preserving active files and shared SST cache residency.
