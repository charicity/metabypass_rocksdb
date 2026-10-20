# Native checkpoint 改造验收记录

记录日期：2026-10-16 UTC。当前状态：**本轮指定的实现、6 个 binary 功能回归、coerce 重复压力和本地 release db_bench 验收已完成**。
本报告区分代码约定、静态审阅和实际执行结果；旧二进制的部分通过结果不能证明修复后的工作树通过。

本次修改基于提交 `b65a213c23a895853b7f01dd287a590f0da2fe9d`，包含尚未提交的工作树修改。
临时构建、测试和实验证据统一保存在 `/tmp/metabypass-native-checkpoint/`；这些路径是本机证据，
不随仓库分发。下文保留执行版本衔接、完整 case 清单、各轮结果、命令和日志路径。

## 实现范围与验收边界

当前实现采用普通 Integrated BlobDB，完整 value 写入原生 WAL 和 memtable，强制
`min_blob_size=0`，拒绝显式 DirectWrite。原生 no-flush live-file 枚举、删除 pin 和镜像 fence
确定候选文件集合、长度及 CURRENT；镜像线程与 checkpoint coordinator 分离。
候选、latest、previous 和退休点保留 blob roots。同步写及 SyncBackup 仍等待 HDD 备份屏障；
Close 在销毁原生 DB 前完成最后屏障。恢复交给原生 RocksDB，不执行业务 WAL/SST/blob 扫描、
blob 封口或 MANIFEST 修补。数据库、inventory 和 tier 映射升级版本，旧格式预检拒绝且不迁移。

Blob staging 在后台 Append 原子预留硬配额；不足时将当前文件单向切到 HDD，独立 worker
持久化固定前缀及路由，后续追加按 4 MiB extent 聚合。旧 SSD 物理读 pin 只延迟源文件回收，
不阻塞 HDD 追加；恢复以 durable descriptor 前缀为准。保留 whole-file GC 和 SST
的 disabled、observe-only、adaptive 模式。本轮没有扩展事务、Merge、多 CF 或动态 API。
详细设计以 [组件入口](index.md)、[staging](tiered-storage.md) 和 [GC](blob-gc.md) 为准。

以上为实现范围和代码约定；功能、重复压力与本地基准证据见下文。

## 审阅与运行中发现的问题

下表的“已修”表示代码及对应测试已修改，不表示最终二进制已通过该测试。
最近的 Review Agent 审阅返回 `No findings`，证据为本次工具对话记录；没有独立磁盘审阅报告。

| 已修问题 | 对应回归或审计方式 |
| --- | --- |
| 旧 inventory 在拒绝前可能被 WriteFormat/RESTORING 流程修改 | `MetaBypassTest.RejectLegacyInventoryWithoutChangingPublishedFiles`；递归比较目录和文件字节，校验 LATEST CRC 后进入版本检查 |
| `allow_2pc` 会让原生枚举强制 flush，违背当前 API 范围与 no-flush 约定 | `MetaBypassTest.RejectUnsupportedWritesAndOptions`；Open/Restore 拒绝且目录不变 |
| foreign staging identity 检查晚于清理，可能删除其他库的同名 blob | `MetaBypassTest.TieredOpenRejectsForeignStagingBeforeCleanup`、`TieredRestoreRejectsForeignStagingIdentity` |
| GC maintenance-ready 与任务 pin 判断不一致，可能跳过待处理 barrier 或访问空迭代器 | `TieredStorageTest.QueuedBarriersPinBlobBeforeReclamation`、`QueuedBarrierFailureWakesTasksAndReclamation`；统一 reclaim-ready 判据 |
| 原生重复删除已不存在的 WAL 导致 sticky mirror error，正常清理被误报备份失败 | `MetaBypassTest.RepeatedNativeDeletionDoesNotPoisonMirror`；保留底层第二次 missing 状态，镜像删除幂等 |
| 切换 staging 布局时提前读取不存在的 staging identity，绕过布局错误分类 | `MetaBypassTest.TieredRejectsFormatChangesAndInvalidBudget`；布局兼容检查前移 |
| 旧 Close 测试等待已移除 hooks | `CloseDrainsWithValidatorBlocked`、`BlobGCCloseDrainsClaimedDeletion` 改观察实际 SyncWaiting 屏障 |
| 旧 memtable iterator 测试假定其值为 BlobIndex | `BlobGCMemtableLazyIteratorRetainsFullValueWithoutBlobPin` 验证 blob 已回收仍可读旧完整值；`BlobGCRecoveredSstLazyIteratorAndIdleRoots` 保留真实 SST/blob 文件保护断言 |
| Close 后的新 checkpoint 可能掩盖并发边界候选错误 | `NativeSnapshotPinsFilesAcrossConcurrentFlush` 在原 DB 仍打开时只读打开指定 point，验证 boundary 存在、after 不存在，再验证最终恢复 |
| 旧 checkpoint 测试允许保存未封闭前缀 | `TieredStorageTest.ExtentBoundariesCheckpointAndPinnedVersion` 明确拒绝前缀、全长成功，且拒绝不改写已有 checkpoint；保留旧版本读 pin 覆盖 |
| 本地 Flush 成功后才发生异步 HDD 迁移错误，旧测试错误要求 Flush 直接失败 | `TieredMigrationFailurePreservesPublishedPoint` 通过 gate 明确 Flush 成功；SyncBackup、stats、后续写、Close 均为 sticky IOError，LATEST 不变，主库读 new、恢复读 old |
| 缺失 SST 的 POSIX stat 错误为 IOError，旧断言仅接受 NotFound | `MissingReferencedSstFailsCapture` 检查允许的文件缺失状态、后续 sticky 状态一致及 LATEST 不变 |
| 同步 Put 已发布第三点，测试又请求新屏障而自己持有退休 GC gate，造成自锁 | `RetiredPointCleanupDoesNotDelaySyncOrMirror` 删除冗余屏障，保留发布、退休点、第四点等待及镜像排空全部断言 |
| 8 个崩溃阶段合并在同一 60 秒 case，且 child 末尾 SIGKILL 可能掩盖 hook 未命中 | 下一测试版本拆为 8 个 `TieredCrashCompleteFastStorageLoss*` 独立 case；各阶段操作及断言保留，非空 hook 未命中明确错误退出 |


## 已保存的旧二进制首轮

运行目录：`/tmp/metabypass-native-checkpoint/run-20261016T105957Z/`。
请求清单为 `requested-case-inventory.json`，每例日志在 `round-1/logs/`，
取消后的逐例状态及原因在 `round-1-cancelled-summary.json`。
调度日志为 `/tmp/metabypass-native-checkpoint/test-runner.log`。

该轮共请求 231 个 case，使用 8 个 worker，每个 case 限时 60 秒。
发现确定性源代码问题后，旧二进制运行被取消，原始结果保留如下：

| PASS | FAIL | TIMEOUT | CANCELLED | NOT_RUN | 合计 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | 25 | 38 | 8 | 120 | 231 |

此轮状态为 `CANCELLED_AFTER_SOURCE_FIXES`，不是通过，也不是完整失败分类。
多例日志首先报告 `index/000004.log`（另有其他 WAL 编号）unlink ENOENT；
格式路径错误及已移除的 hooks 也已独立识别。后续通过不得覆盖本轮 FAIL/TIMEOUT；
CANCELLED 不计作失败，NOT_RUN 不计作通过。

## 正式功能回归与测试版本衔接

strict debug 构建 `build-debug-8.log` 成功，6 个目标均为最新。
`run-20261016T111840Z/` 完整执行 232 个 case，每例 60 秒，按 8、2、1、1 worker
重试精确失败或超时 case；`final-summary.json` 与各 `round-N/results.json` 保留结果：

| 轮次 | PASS | FAIL | TIMEOUT | 说明 |
| --- | ---: | ---: | ---: | --- |
| 1 | 228 | 2 | 2 | 完整 232 case |
| 2 | 1 | 2 | 1 | 仅重试首轮 4 例；原合并崩溃 case 通过后停止重试 |
| 3 | 0 | 2 | 1 | 仅重试剩余 3 例 |
| 4 | 0 | 2 | 1 | 旧测试版本最终保留 3 个持续失败/超时 |

持续的 3 例为 `MissingReferencedSstFailsCapture`、
`TieredMigrationFailurePreservesPublishedPoint`、`RetiredPointCleanupDoesNotDelaySyncOrMirror`，
分别对应文件错误分类断言、异步迁移契约断言及测试自身 GC gate 自锁。
这些历史 FAIL/TIMEOUT 保留，没有改写为 PASS。
原 `TieredCrashBoundariesAndCompleteFastStorageLoss` 首轮超时、第二轮通过也保留原记录。

随后只修改测试，生产代码保持相同，`build-debug-9.log` 重编 `metabypass_test` 成功。
新测试版本修复上述 3 例，并把原合并崩溃 case 拆成 8 个独立阶段。
`run-20261016T112615Z/` 的精确 11 例首轮全部 PASS，无需重试；清单在
`round-1/cases.json`，逐例结果在 `round-1/results.json`，汇总在 `final-summary.json`。

因此，当前有效功能清单为 **239 例：原版本未变更且首轮通过的 228 例，加最终测试版本的
11 例**。这是一组相同生产代码下、明确记录测试版本衔接的覆盖证据，
不是“一次运行 239 例首轮全通过”。范围包含 `metabypass_test`、`sst_tiering_test`、
`checkpoint_test`、`db_blob_basic_test`、`db_blob_compaction_test`、`db_blob_corruption_test`；
不等于全仓库 `make check`。

另有误以 `--help` 调用 runner 而启动的 `run-20261016T112424Z/`，随后取消，保存
15 PASS、8 CANCELLED、209 NOT_RUN；该运行不作为验收依据。

## 验证结果总表

本节日志均相对 `/tmp/metabypass-native-checkpoint/`。

| 验证项目 | 当前状态 | 命令或证据位置 |
| --- | --- | --- |
| strict debug 构建 | build 8、测试修改后的 build 9 均成功 | `build-debug-8.log`、`build-debug-9.log`；`AUTO_CLEAN=1 ASSERT_STATUS_CHECKED=1 make -j48` |
| source/format 检查 | 最终检查均 exit 0，之后生产源未修改 | `check-sources-final-2.log`、`format-final-2.log` |
| 6 个 binary 的功能回归 | 当前有效 239 例均有通过证据，版本衔接及历史失败见上节 | `run-20261016T111840Z/`、`run-20261016T112615Z/` |
| 新并发边界的 coerce 重复压力验证 | 构建成功；10 例各 100 次，共 1000 PASS，0 FAIL/TIMEOUT，无重试 | `build-coerce-1.log`、`run-20261016T112937Z/final-summary.json`；`COERCE_CONTEXT_SWITCH=1` |
| release db_bench 与恢复验证 | 构建成功，4 场景共 16 条命令 exit 0，写后及恢复后逐 key 校验通过 | `bench-20261016T114152Z/build-release-db_bench.log`、`results-with-prefix-corrected.json`（同一 bench 目录） |
| 修复后最终静态审阅 | 最近聚焦审阅无新增 findings；不替代运行验收 | 本次 Review Agent 工具对话记录，无磁盘报告 |

## Coerce 重复压力结果

`COERCE_CONTEXT_SWITCH=1` strict 构建成功，证据为 `build-coerce-1.log`。
运行 `run-20261016T112937Z/` 使用 8 个 worker，每次独立执行限时 60 秒，
下列 10 个 case 各重复 100 次。9 个来自 `metabypass_test`，1 个来自 `sst_tiering_test`，
合计 **1000 PASS、0 FAIL、0 TIMEOUT，无需重试**。

| binary | 精确 case | 次数 |
| --- | --- | ---: |
| `metabypass_test` | `MetaBypassTest.NativeSnapshotPinsFilesAcrossConcurrentFlush` | 100 |
| `metabypass_test` | `MetaBypassTest.TieredConcurrentWritersFlushAndSpillWithinHardCap` | 100 |
| `metabypass_test` | `MetaBypassTest.ConcurrentBatchesFlushAndSync` | 100 |
| `metabypass_test` | `MetaBypassTest.AllSyncWaitersWakeOnPublicationOrFailure` | 100 |
| `metabypass_test` | `TieredStorageTest.ConcurrentAppendReservationsCannotOversubscribeQuota` | 100 |
| `metabypass_test` | `TieredStorageTest.PinnedLocalReadDoesNotBlockSpilledFileAppend` | 100 |
| `metabypass_test` | `TieredStorageTest.CrashAfterRouteCommitAndUnsyncedHddTailUsesDurablePrefix` | 100 |
| `metabypass_test` | `TieredStorageTest.QueuedBarriersPinBlobBeforeReclamation` | 100 |
| `metabypass_test` | `TieredStorageTest.QueuedBarrierFailureWakesTasksAndReclamation` | 100 |
| `sst_tiering_test` | `SstStorageTest.ReadPinsDelayUnlinkButNotRouteSwitch` | 100 |

完整重复清单为 `requested-case-inventory.json`，逐次结果及日志路径在
`round-1/results.json`，最终汇总为 `final-summary.json`，均位于上述运行目录。
范围覆盖 native candidate 边界、默认并发写与 spill、配额竞争、local/remote 路由与读 pin、
queued barrier/GC、失败唤醒及 durable 前缀崩溃恢复。它与功能回归分别记录，
不把历史失败或取消记录改写为通过。

## Release db_bench 与模拟丢失恢复

`AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 db_bench` 构建 exit 0，日志位于
`/tmp/metabypass-native-checkpoint/bench-20261016T114152Z/build-release-db_bench.log`。
权威汇总为同目录 `results-with-prefix-corrected.json`；下表按该修正汇总核对。
原 `results.json` 将用户 payload 字节误标为 logical blob 字节，已保留供审计，
不作为本表 spill 比例的分母依据。

每个场景执行 write、verify-after-write、restore、verify-after-restore，合计
**16 条命令全部 exit 0，两次逐 key 验证均通过**。模拟丢失通过将本任务的 index 改名保留；
staged 场景同时将 staging 改名保留，再从 HDD 角色目录恢复，没有覆盖原始实验目录。
write 阶段包含显式 Flush，实际生成普通 BlobDB blob 文件。

| 场景与工作量 | sync | Put p99 (us) | 同步屏障均值 (us) | WAL 镜像字节 | staging peak / cap (bytes) | spill files / bytes / 比例 | HDD extents |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| `direct_async`，4096 × 4096 B | false | 41 | 不适用 | 16,890,303 | 0 / 未启用 | 0 / 0 / 不适用 | 0 |
| `staged_async`，4096 × 4096 B | false | 43 | 不适用 | 16,890,303 | 0 / 65,536 | 1 / 16,923,624 / 100% | 5 |
| `staged_sync`，64 × 4096 B | true | 673,050 | 247,123.47 | 263,790 | 0 / 65,536 | 1 / 264,372 / 100% | 1 |
| `staged_prefix_async`，4096 × 4096 B | false | 42 | 不适用 | 16,890,303 | 2,097,152 / 2,097,152 | 1 / 16,923,624 / 100% | 5 |

同步屏障均值只在 `sync=true` 场景计算，为 `sync_write_us / ops`：
15,815,902 / 64 = 247,123.47 us；它是备份屏障累计等待分量的均值，
不等于完整 Put 平均延迟。p99 来自写操作延迟样本。

64 KiB staging 场景 peak 为 0，是因为底层 blob Append 大于该 cap，文件立即走 HDD。
这两场验证了小 cap 下成功写入与恢复，不能单独证明实际 SSD 前缀迁移。
追加的 2 MiB 场景实际占满 2,097,152 字节后切换，验证本地前缀路径；
其 spill 比例为 100%，共 5 个 extent。

比例定义为 `staging_spill_bytes / sum(INVENTORY 中 B 行的 logical blob file size)`，
并与 staged 模式 MBT2 descriptor 的 logical size 核对。它包含 blob framing 等文件字节，
不是 `num * value_size`。4096 条工作量的用户 payload 为 16,777,216 字节，
blob 文件为 16,923,624 字节；64 条工作量分别为 262,144 与 264,372 字节。
spill bytes 包含被切换的本地前缀和后续 HDD 追加，因此 100% 描述最终整个文件走 HDD。

完整命令参数、每条命令耗时、exit code、模拟改名步骤与日志路径均保存在权威汇总的
`scenarios[].commands` / `filesystem_steps` 中；4 个场景子目录分别保留
`write.log`、`verify-after-write.log`、`restore.log`、`verify-after-restore.log`。
原 3 场景与追加前缀场景的原始结果文件也保留，可沿汇总中的来源路径追溯。

## 验收结论与测量边界

本轮指定范围完成：当前有效功能 239 例具有通过证据，coerce 重复压力
1000 次全部通过，release 构建和 4 场景写入/校验/模拟丢失恢复的 16 条命令全部成功。
最终 source/format 和 diff 检查通过，最近聚焦静态审阅无新增 findings。
历史失败、超时、取消及测试版本衔接按上述记录保留。

所有本地实验路径均在 `/tmp` 下同一 filesystem，目录角色不代表真实 SSD/HDD 设备分离。
这些小样本支持本机功能与开销观察，不能外推真实 SSD/HDD 吞吐、迁移带宽、介质延迟或故障域。
特别是同步场景 247,123.47 us 的屏障均值和 673,050 us 的 Put p99，仅为本场测量；
没有对应旧版本/硬件基线，不作性能改善或回归结论。

本轮验收不是全仓库 `make check`，也不是真实硬件性能评测或断电持久性认证。
旧 DirectWrite、内容扫描与增量验证缓存报告描述历史实现，其数值和恢复行为不适用于本轮。
