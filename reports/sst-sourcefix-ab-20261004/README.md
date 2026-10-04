# Small SST CPU-affinity / kmem A/B

This bounded diagnostic compared four paired trials on `node-ssd` with the same validated 1M-key seed, binary, adaptive SST capacity, offered rate, cache and queue settings. A inherited the host's allowed CPU set without binding. B used the frozen `node_run.py` helper to apply CPUs 0-3 before joining the cgroup and execing the copy or benchmark command. Order was A1/B1 through A4/B4.

The run completed at 2026-10-03T19:33:32+00:00 after 402.905 seconds (start 2026-10-03T19:26:49Z). All eight independent copies and all eight benchmark commands exited successfully. Each copy reported 170 files, 1117867930 bytes copied, 44 within-destination hardlinks recreated, and `shared_with_seed=false`. Each measured phase completed 1,070 operations in 10 seconds. There were zero OOM kills and no command timeouts.

| Metric | A: inherited CPUs | B: CPUs 0-3 |
| --- | ---: | ---: |
| Initial cgroup usage / kmem | 0 / 0 B | 0 / 0 B |
| End idle usage / kmem | 32083968 / 21217280 B | 32174080 / 4845568 B |
| Cgroup peak usage | 50335744 B | 50331648 B |
| Cgroup kmem high-water after runs 1-4 | 10137600, 13672448, 18915328, 23343104 B | 6082560, 6410240, 6991872, 7118848 B |
| Highest sampled process VmRSS | 30789632 B | 30679040 B |
| Highest sampled process RssAnon | 17461248 B | 17457152 B |
| Peak threads / FDs | 9 / 60 | 9 / 61 |
| OOM kill counter delta | 0 | 0 |
| `memory.failcnt` delta | 1923339 | 2462517 |
| `memory.kmem.slabinfo` at command end | Populated; see class estimates below | Populated; see class estimates below |

A's kmem high-water rose over the four runs to 23343104 B; B reached 7118848 B and ended with 4845568 B residual kmem. Process RSS/RssAnon peaks were similar. This is evidence that unrestricted CPU affinity is associated with materially larger residual kernel memory in this workload and may explain part of the 48 MiB headroom gap. The probe does not identify why the allocation differed. Treat the result as a strong diagnostic signal, not proof of a specific kernel mechanism or the exact cause of an earlier OOM.

## Per-command slabinfo evidence

The runner captured populated cgroup-end slab tables for all 16 copy/run commands (2,451 bytes after copy; 2,665 bytes after benchmark runs). The independent initial group and final idle snapshots are empty; those do not describe command-end state. The table reports four selected classes after each run. `active payload` is active object count × object size; `active slab pages` is active slabs × pages per slab × 4,096. Both are nominal slabinfo estimates, not exact memcg charges.

| Run | radix_tree_node active objects / payload / slab pages | buffer_head active objects / payload / slab pages | kmalloc-4k active objects / payload / slab pages |
| --- | ---: | ---: | ---: |
| A1 | 8197 / 4787048 / 5341184 B | 2970 / 308880 / 352256 B | 9 / 36864 / 65536 B |
| B1 | 2716 / 1586144 / 1933312 B | 3512 / 365248 / 397312 B | 16 / 65536 / 65536 B |
| A2 | 11200 / 6540800 / 7471104 B | 5554 / 577616 / 626688 B | 17 / 69632 / 98304 B |
| B2 | 3080 / 1798720 / 2031616 B | 2004 / 208416 / 274432 B | 16 / 65536 / 65536 B |
| A3 | 14294 / 8347696 / 9568256 B | 10141 / 1054664 / 1089536 B | 26 / 106496 / 163840 B |
| B3 | 3164 / 1847776 / 1966080 B | 3255 / 338520 / 409600 B | 24 / 98304 / 98304 B |
| A4 | 17367 / 10142328 / 11796480 B | 15288 / 1589952 / 1617920 B | 27 / 110592 / 196608 B |
| B4 | 3591 / 2097144 / 2359296 B | 3484 / 362336 / 417792 B | 24 / 98304 / 98304 B |

At A4-run, `radix_tree_node` was 17367 active objects (10142328 B nominal object payload; 11796480 B active slab pages), versus 3591 (2097144 B; 2359296 B) at B4-run. `buffer_head` was 15288 active objects at A4-run and 3484 at B4-run. A4-run also listed `ext4_inode_cache`, `proc_inode_cache`, `pid`, `signal_cache`, `sighand_cache`, `files_cache`, and `task_delay_info`; see the raw report for all class rows. These end snapshots support a class-level association with larger kernel metadata under A, especially radix-tree nodes, while not identifying why that allocation differed. Slabinfo object/slab estimates do not equal `memory.kmem.usage_in_bytes`; active object counts alone do not establish a file-descriptor leak.

Both groups ran close to the fixed 48 MiB limit and accumulated `memory.failcnt`, but neither recorded an OOM kill. At the final idle read, A and B tasks/cgroup.procs were empty. A/B commands never targeted the third fixed-test cgroup; it was subsequently used by the separately authorized formal small run.

## CPU sampling detail

The B helper itself initially started with the inherited 0-95 set. The one-second sampler recorded one initial tick for each B command before the helper's `cpu_affinity` event: each shows an unrestricted allowed set and a last CPU outside 0-3. The event records `applied_before_cgroup=true`; the helper then joined the cgroup and execed the child. All 191 later B samples show allowed CPUs 0-3 and last processors within 0-3. These initial samples remain in the raw report. The raw all-sample CPU union includes CPUs 0, 1, 2, 3, 8, 48, 58; the post-affinity workload CPU set is 0-3. A inherited CPUs 0-95 and observed processors across 46 CPUs.

The controller reused the frozen `Runner` API with `profile=legacy` as a template so A could remain unbound. Therefore JSON `profile_parameters` and `limits` fields show controller-template defaults, including 96 MiB and an 8 MiB queue; they are not the A/B configuration. Actual values are in `ab_protocol`, exact command argv, and the 48 MiB/swappiness=0 cgroup snapshots. Raw command and sample data have not been edited to hide these fields or startup samples.

## Identity and artifacts

- Binary SHA-256 at start and end: `2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76`.
- Frozen controller SHA-256: `e1effa72cfe4945e805bdb23cb01b18b4346bb9eb2fc81948afdda1bbee627ee`; source manifest SHA-256: `3a9e91e86fbd2743d473ef09199b490f1341db7e84a83a3e0535a2a081cceda2`.
- Validated seed: `/home/lj/ssd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/seed-1000000` and matching HDD path `/home/lj/hdd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/seed-1000000`; its prior run recorded success and 22 SSD SSTs.
- New trial data remains on node-ssd under `/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/ssd-data/node-sst-846a5774-e232-4dce-b28c-74fea19f8571` and `/home/lj/hdd/metabypass_test/sst-sourcefix-ab-20261004/hdd-data/node-sst-846a5774-e232-4dce-b28c-74fea19f8571`. The remote roots held 39,140,860 SSD bytes and 8,758,174,497 HDD bytes after the probe. The large data and benchmark binary were not downloaded.
- Remote raw report: `/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/ab-report.json`.
- Local unchanged raw report: `raw-node/ab-report.json` (SHA-256 `41c87824f966a6953e0697754fa2292ca2b53de55e720c9daa0379a6f7c11860`). Full argv, stdout, stderr, ACK, process and one-second sample logs are in `raw-node/node-control-846a5774-e232-4dce-b28c-74fea19f8571/`.
- Idle A/B cgroup `memory.stat` and kmem state: `metadata/ab-cgroups-postrun.json`.
- Derived metrics: `summary.json`.
