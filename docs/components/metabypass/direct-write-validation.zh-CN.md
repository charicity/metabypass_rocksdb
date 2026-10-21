# DirectWrite 实施与关键验证交接

记录日期：2026-10-20 UTC。基线为 `23e242cba`，结果对应本轮修改后的工作树。
本轮按用户要求执行关键单元测试和功能验证；全仓库单测与正式设备性能实验留待新会话。
普通 strict 回归 **303/303 首轮通过**，Python 合约测试 **33/33 通过**，
db_bench 的 DirectWrite 开关两组写入、Restore、校验通过，native-driver 三种存储模式
各 run/verify 共六次调用通过。并发压力的四个未变更用例各 100 次首轮通过，
新增 GC 用例修正测试等待后重新执行 100 次也全部首轮通过；原失败与版本衔接见下文。

本机证据根目录为 `/tmp/metabypass-direct-write-20261020-I0I29I/tests/`，
最终普通 strict 证据位于其 `strict-final/` 子目录。整个 tests 与 C API 日志的本地持久归档位置为
[`experiments/direct-write/results/20261020-I0I29I/validation-evidence.tar.gz`](experiments/direct-write/results/20261020-I0I29I/validation-evidence.tar.gz)，
校验文件为同目录的
[`validation-evidence.tar.gz.sha256`](experiments/direct-write/results/20261020-I0I29I/validation-evidence.tar.gz.sha256)。
该 results 目录被 git 忽略，跨机器交接时须单独复制归档并核验摘要，不能假定 clone 包含它。
历史失败与无效验证一并保留。

## 实施范围与职责

恢复使用原生 `Options::enable_blob_direct_write` 开关，默认仍为 `false`，不新增同义配置。
显式启用时要求 `allow_concurrent_memtable_write=false`。普通 `sync=false` 写入
由 RocksDB 把 value 直接写入 Blob，WAL 和 memtable 保存引用；本轮的性能目标是减少
普通写的完整 value WAL 与镜像开销，但尚未测得正式性能收益。
默认关闭路径仍使用完整 value WAL 与原生 no-flush 检查点。

新增 `LiveFilesStorageInfoOptions::include_wal_files`，默认 `true` 保持既有行为。
设为 `false` 必须同时设 `wal_size_for_flush=0`，仅支持可写、单活跃 CF、无 2PC、
无 WAL MANIFEST tracking、WAL 未锁定的配置；捕获期间 CF 生命周期变化也拒绝。
原生 flush 完成后，在原生锁保护下取得同一 Version 的 SST、已登记 Blob、MANIFEST
及 CURRENT 等信息，然后跳过 WAL 捕获。后续写可以生成新活动 Blob，
这些尚未登记的依赖不进入该检查点，不要求全局活动 Blob 数量清零。

Metabypass 协调检查点请求、镜像 fence、候选持久化和发布；RocksDB 决定逻辑文件生命周期
及一致可恢复集合。调用原生 flush 是推动就绪的受控操作，符合原生管理与事件驱动原则。
Metabypass 不解析业务 WAL、不扫描 SST 内部 Blob 引用、不自行封口 Blob，
也不修补 MANIFEST。候选及恢复点的 inventory/checksum、物理镜像和根保护仍是其职责。
Blob staging、整文件 GC 与 SST 冷热分层的配置和职责保持原设计。

## 五秒调度与同步语义

`checkpoint_interval_ms` 默认 `5000`，独立于镜像批处理的 `interval_ms`。
首次未覆盖的已完成写或原生文件变化建立 dirty 起点；后续写不会反复推迟这一期限。
到期时请求一次原生捕获，同一时间只有一个候选；同一边界内的请求可以合并。
低流量写后停写也会推进检查点，完成后没有新变化就进入空闲，不持续制造无用检查点。

五秒是调度目标，不是承诺五秒内完成 HDD 发布，也不是固定 RPO。
原生 flush、镜像排空、I/O 持久化或失败都会影响实际发布时间。
`SyncBackup`、`sync=true` 写入和 Close 独立于周期期限请求屏障，成功返回前必须已经发布
至少覆盖其已完成写目标的 HDD 恢复点。后续写可以一并被覆盖，不要求精确历史截断。
未发布的候选不能提前满足同步成功；错误为 sticky，唤醒等待者并拒绝后续变更。

## 恢复与兼容契约

用户确认的故障恢复入口是**已发布检查点的 Restore**。
DirectWrite 候选仅在其原生一致性集合依赖的全部 Blob、SST、元数据及对应镜像屏障
完成 HDD 持久化后，才持久更新 LATEST。Append 返回、单个文件 fsync 或封口
均不单独代表候选可发布；必须使用原生枚举确认已登记的依赖集合。

DirectWrite 恢复点不依赖 WAL，恢复由原生 RocksDB 解释复制出的索引。
最后发布点之后的写可能丢失；本轮没有扩展“对含活动 Blob 引用的本地 WAL 直接 crash Open”
恢复能力。staging 模式可以从保留下来的 HDD data/backup 恢复，不要求原 SSD index/staging 存活。
Restore 不静默回退到旧点，不封口 Blob，不重写目标 MANIFEST 修补引用。

现有 `MBN2` 数据库/备份、`MBI2` inventory 和 `MBT2` tier 格式继续使用；
默认开关为 false。早期格式或无标记的非空目录仍在修改前拒绝，本轮不提供迁移工具。
正确性与使用约束详见[组件入口](index.md)和[直接写入存储说明](direct-storage.md)。

## 发现的问题与修复

首次 Metabypass 回归有四例在 8、2、1、1 worker 四轮中持续失败，
旧记录保留于 `cpp-metabypass/summary.json` 与 `cpp-metabypass/rounds/`。
最终重新构建后的结果不覆盖这些历史失败。

三例在测试 helper 中只读打开单 SST 检查点后 Get 返回 NotFound：
`DirectWriteCapturesAndCoalescesConcurrentBarriers`、
`DirectWriteLowTrafficCheckpointThenIdle`、`CheckpointDeadlineDoesNotSlideWithLaterWrites`。
根因是 `OpenForReadOnly(Options)` 可能选择既有 `CompactedDBImpl` 优化路径，
该 Get 路径不支持普通 BlobIndex，却返回 NotFound。
helper 改用 DBOptions 加 CF descriptors 的标准只读重载，明确拒绝 WAL，
并逐 SST 断言恢复出的最新序列号不小于文件最大序列号；原值和不存在断言全部保留。
这是测试读取入口问题，不是本轮 WAL-free 检查点缺失数据；没有用可写 Open 掩盖它，
也没有在本轮扩大修改范围去改造 CompactedDBImpl。

`DirectWriteIteratorSurvivesFlushAndRetirement` 暴露了原生延期 GC 缺陷：
VersionSet 将 obsolete Blob 交给清理任务后，旧 memtable 迭代器仍保护文件，
本轮清理因此保留它，却没有释放 `files_grabbed_for_purge_` 标记；后续完整扫描持续跳过该文件。
修复在清理结束的原生互斥锁内，仅释放本任务 obsolete 列表与实际延期列表交集中的标记，
不会清除另一清理任务的占用，也不会解除读者保护。
新增 `DirectWriteProtectedObsoleteBlobCanBePurgedAfterIteratorRelease` 原生回归，
验证实际延期事件、保护期间重复完整扫描后仍存在且可读、迭代器释放后可真正删除。
Metabypass 原测试的回收断言和操作保持不变，没有删断言或增加 sleep。

## 验证证据

以下路径均相对本机证据根目录；计数以逐例日志和汇总为准。

| 项目 | 实际结果 | 证据 |
| --- | --- | --- |
| 普通 strict 关键 C++ 回归 | 303/303 首轮 PASS，无持续失败；8 worker，每例 60 秒 | `strict-final/cpp-cases/summary.json`、同目录 inventory 与 rounds；构建 `strict-final-build.log` |
| Python 驱动/协议合约 | 33/33 PASS | `python-cases/summary.json`、同目录逐例日志 |
| db_bench 开关功能 | DirectWrite true/false 两组 write、Restore、verify 均成功；核对 effective options | `strict-final/dbbench-smoke-runner.log`、`strict-final/dbbench-smoke/`、`strict-final/dbbench-smoke-final.sh` |
| native-driver 三种存储小样本 | 无 staging、充足 staging、8 KiB 强制 spill 三组 run/verify 共六次调用 PASS，每组 verify 16 keys | `strict-final/native-driver-smoke-cap8192/`、`strict-final/native-driver-smoke-cap8192-runner.log` |
| coerce 并发重复压力 | 原首轮 500 次为 499 PASS、1 FAIL、0 TIMEOUT，精确 retry2 通过；四个未变更用例各 100 次首轮 PASS，GC 修正后另行 100/100 首轮 PASS，无重试或超时 | `strict-final/coerce-repeat-100/summary.json`、`strict-final/coerce-gc-fixed-100/summary.json`；重建 `strict-final/coerce-gc-fixed-build.log` |
| C API 生成与一致性 | regen 与 generated verify 成功；不等同于 strict c_test 运行通过 | `/tmp/metabypass-native-codegen-verification.log`（从工具输出转录的审计记录） |
| source/diff 检查 | 最终普通 strict 阶段通过 | `strict-final/check-sources.log`、`strict-final/git-diff-check.log` |
| 独立静态审阅 | 最终无遗留 finding；不替代运行验证 | 本次审阅代理对话记录 |

普通 strict 范围包含 Metabypass、SST tiering、原生 checkpoint 与 Blob 相关的关键 case，
完整 binary/case 清单在汇总目录；303 不代表全仓库测试数。
各 binary 的数量为 `metabypass_test` 134、`checkpoint_test` 51、
`db_blob_basic_test` 45、`db_blob_direct_write_test` 33、`db_blob_compaction_test` 21、
`sst_tiering_test` 18、`db_blob_corruption_test` 1，均为首轮 PASS。
运行统一使用 strict Status checking。C API 生成工具验证不能替代 `c_test`；
没有关闭 strict 来换取该测试的通过。

coerce 首轮唯一失败是新增原生 GC 用例的 repeat index 0：立即读取 `deferred` 时仍为 0，
该次原始失败保留，不能把重试通过改写成 500 次稳定通过。原因是 `TEST_WaitForPurge`
只等待独立调度的 purge worker，不等待 compaction 线程同步执行的清理尾部；
手动 compaction 返回时，延期回调可能尚未执行。测试改为通过现有 SyncPoint 回调和
condition variable 在 20 秒内等待实际延期事件，保留计数、保护期间扫描/读取和释放后
真实删除的全部断言，未修改生产逻辑。其余四个并发用例各 100 次首轮通过，
只对修改后的 GC 用例保持 strict 与 `COERCE_CONTEXT_SWITCH=1` 重建并执行 100 次，
全部首轮 PASS，无重试、无超时。重测仍使用 8 worker，每次独立调用限时 60 秒；
最终 source/diff 检查通过。该次测试源 SHA256 为
`14cded570cf3d3959b8621bc9694cd2b868e313647e6fcdf2785a141783f269a`。

四个未变更的压力用例是
`MetaBypassTest.DirectWriteCapturesAndCoalescesConcurrentBarriers`、
`MetaBypassTest.DirectWriteLowTrafficCheckpointThenIdle`、
`MetaBypassTest.CheckpointDeadlineDoesNotSlideWithLaterWrites`、
`CheckpointTest.LiveFilesWithoutWALConcurrentDirectWrite`。
修正后重测的是
`DBBlobDirectWriteTest.DirectWriteProtectedObsoleteBlobCanBePurgedAfterIteratorRelease`。
上述结果来自明确衔接的两次测试版本，不描述为一次 500 次首轮全通过。

首次 native-driver 手动构建遗漏了与 strict 库一致的 Status ABI 宏，导致无效运行，
证据保留在较早的 `native-driver-build/` 和 `native-driver-smoke/`；
该结果不计入功能通过，也不归因为引擎恢复失败。最终驱动使用匹配的
`ROCKSDB_ASSERT_STATUS_CHECKED` 宏重建。另一次 smoke 将容量设为 4096 B，
低于合法最小值 8192 B，属于测试参数错误；原日志保留在 `strict-final/native-driver-smoke/`，
不计为有效功能失败。纠正后的三组均在 options JSON 中确认 DirectWrite=true、
`checkpoint_interval_ms=1`；强制 spill 组实际观测 `spill_files=2`、`spill_bytes=17276`。
小样本目录中的 index/data/staging 只是 `/tmp` 下的存储角色，不能证明真实 SSD/HDD 的
吞吐、介质延迟或断电恢复能力。db_bench 两组参数也不构成正式性能对照，本文不报告性能收益。

## 新会话执行交接

远端 `lj@node-ssd` 的本任务独占控制目录为
`/home/lj/mb-direct-write-20261020-I0I29I`。其中 `original-source` 是原版源导出；
原始实验 driver 必须随原版一起保留，不能假定 `git archive HEAD` 包含此前被忽略的 driver。
原版基线构建曾因 g++ 9 编译错误失败，属于非 OOM；应优先使用已确认的 clang 14，
保留失败日志、构建命令、源/driver 摘要与新构建身份，不通过降低并发掩盖编译错误。
原生 C API codegen 使用过同父目录独立副本 `codegen-source-native`，未修改 `original-source`。

新会话先核对目录所有权、现存进程、构建/源摘要和已有结果，不覆盖历史实验目录。
真实设备父目录为 `/home/lj/{ssd,hdd}/metabypass_test`，现有 8 GiB cgroup 的空闲与归属
必须重新确认；继续使用 lj 现有权限，不改全局 swap、缓存、挂载或调度设置。

完整三臂、三轮及 162 样本矩阵见[DirectWrite 比较协议](experiments/direct-write/README.zh-CN.md)，
结果填写[报告模板](experiments/direct-write/report-template.zh-CN.md)。
三臂为 `original1s`、`newfalse5s`、`newtrue5s`，每臂 54 样本；原版 1000 ms，
新版两臂均 5000 ms，18 个场景只生成 36 项配对比较。
`newtrue5s/newfalse5s` 比较可用 DirectWrite 配置组合，因为前者必须关闭并发 memtable 写，
后者开启；不能归因为只改变一个 boolean 的纯因果收益。
`newtrue5s/original1s` 包含版本和检查点间隔的总体变化，不单独归因检查点间隔。
仅 formal completed、162 个预期样本全部有效且所有比较三轮齐备时允许总体排名。
上述最初关键验证阶段没有执行该正式矩阵，也没有执行全仓库 `make check`；
后续执行状态和结论以各独立输出目录报告为准。

### 2026-10-21 当前性能执行协议

原 CLI 默认仍为 4 GiB/30 秒。用户已批准本轮三臂统一通过显式参数
`--write-bytes 8589934592 --seconds 180 --verify-samples 4096` 执行：8 GiB 数据集，覆盖/混合 180 秒，
插入一次性完成 8 GiB、不定时；本轮仅调整实验 driver 和控制协议，engine 行为不变；原版引擎固定23e242cba。
原 `direct-formal-01` 在 13 个有效样本后因 insert/4t/ample/newfalse5s 前台仅 1 个发布点停止，
随后 8 GiB/30 秒 `direct-qualification-01` 首项 overwrite/4t/ample/newfalse5s 为 0 个前台发布点，
也已停止；这些失败结果及现场独立保留，不计入后续通过项。

用户随后要求抽样并取消仍在全量校验的 qualification02；旧取消结果与现场保留，旧全量版
`qualify180.py` 和 `formal180.py` 作废。先保留 pristine 原 driver/SHA，并从它派生仅含同等抽样
逻辑的兼容 driver，两版共用纯选择 helper；导出新源码冻结，以 Clang14 Release `-j48`
在新独占控制目录构建两版，再完成 162 项采样 smoke。通过后用新独立采样 harness 执行六项资格检查，顺序 overwrite false/true、mixed false/true、
insert false/true，均 round 1、4 线程、16 GiB ample staging。保留原前台机制门槛、
4096 抽样 verify、HDD Restore、append-reopen 和 1800 秒进展停滞 watchdog，任一失败即停止。
六项全部有效通过并记录 `qualification_completed` 后，才允许在新输出目录完整运行 162 样本；
资格检查仅为诊断，禁止总体排名，不与正式样本混合。命令及身份归档要求见
[当前比较协议](experiments/direct-write/README.zh-CN.md)。

本轮 verify、Restore 后校验、append-reopen 使用相同 `metabypass-key-sample-v1` 策略：
实际 min(4096, keys)，唯一排序集合含 key0/末 key/各分区首尾和存在的 live/deleted 代表，
独立派生 seed 补齐；检查选中 key 的存在/删除、长度、版本与 checksum，完全跳过全库 Get
和 iterator/keycount 扫描。每 action 的完整列表、实际数量、分类数及派生 seed 均归档，
小库全 key 入样仍不做 iterator；预算不足或与 ACK 验证组合时拒绝。
`--verify-samples=0` 保留历史 full 语义，抽样通过不证明全库完整性或实际 keycount。
