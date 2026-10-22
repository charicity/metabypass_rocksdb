# metabypass--

Current naming: this historical `metabypass--` baseline is now [Metabypass](components/metabypass/research-baseline.zh-CN.md); the original definition below is retained.

This experimental variant starts from `fb0f5092f97aa3ba7b7a56e5a331a87a30946c67`
and restores the five stage1 backup optimization files exactly from
`reports/dw-optimization-20261022/stage1/provenance/source.tar.gz`
(SHA-256: `a05b09e53a899378849adef0243ac790735cbdb35aca77be4a2b7a7bb6368e8b`).

It additionally preserves direct-write rollback records and touched managers
when batch transformation fails after appending blob records. A targeted
regression test checks partial transformation failure across two column families,
garbage counts and bytes, subsequent writes, and reopen visibility.

Stage2 background blob-file sealing and stage3 batched Flush optimizations are
excluded. There are no public API changes.
