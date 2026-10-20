# Whole-file blob reclamation

MetaBypass disables value relocation and lets ordinary Integrated BlobDB identify
obsolete whole blob files. `enable_blob_file_gc=true` is the default; disabling
it retains remaining files and does not resurrect earlier deletions.

Native live-file capture runs while file deletion is disabled. A candidate
protects captured blobs before that pin is released. Latest, previous, candidate
and retired recovery roots continue protecting payload files while publication,
restore and point cleanup need them. An eligible retirement is claimed only
after every root releases it. Native iterators and read pins retain their normal
read lifetimes. GC cannot race a newly captured file set into referencing removed
payloads.

A dedicated worker completes physical deletion and directory sync. Tiered GC
removes the descriptor and all extents while retaining in-flight local/remote
read lifetimes. Cancellation and Close wait for an already claimed deletion to
finish. Interrupted deletion is rediscovered by native obsolete-file cleanup and
tier debris projection on reopen.

`pending_blob_gc_files/bytes` report known eligible retirements;
`reclaimed_blob_files/bytes` advance only after deletion and directory sync.
`blob_gc_micros` records deletion time. Retired point directory metrics are
separate from payload reclamation. Failures use the sticky backup error path.

Recovery uses native full-value WAL and ordinary BlobDB metadata. No repair of
historical blob registration, business reference scanning, or value relocation
is performed. See [native checkpoint design](index.md).
