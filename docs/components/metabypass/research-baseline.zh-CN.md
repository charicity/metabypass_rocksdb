# 当前 Metabypass 研究基线与验证交接

更新日期：2026-10-22。本文件定义分支调整后的术语、比较范围和待执行计划。
本次只完成批准计划的步骤 1–3：归档旧工作区、调整研究分支、同步实验驱动与文档。
步骤 4 的新基线正确性测试和 HDD Restore，以及步骤 5 的同批次 36 样本性能比较均未执行；
不得将旧报告写成新分支已经通过验证。

## 名称、分支与源码身份

现在的 **Metabypass** 指 Stage1 备份优化，加上原生 DirectWrite（DW）兼容支持及正确性修复。
它保留原生 DW 开/关两种合法配置，但不包含 Stage2 后台 Blob 封口优化或 Stage3 批量 Flush 优化。
“支持原生 DW”与“包含 DW 性能优化”是不同范围；DW 开启时仍需
`allow_concurrent_memtable_write=false`，关闭时使用 true。

下表为本次调整完成时的三个研究分支和起始提交；后续实验必须另外冻结实际源清单和产物摘要。

| 当前名称 | 当前分支 | 本次起始完整 SHA | 旧名称及范围映射 |
|---|---|---|---|
| Metabypass | `metabypass` | `de28572b42895744085c9be3a1308fcaf00a8d7c` | 原 `codex/metabypass-minus-minus` / `metabypass--`；Stage1 + 原生 DW 兼容及正确性修复，无 Stage2/3 |
| Metabypass with DW | `metabypass-with-DW` | `3f333d250ac3f5afb33a0149d51cb698f675ae6f` | 原主工作区的含 DW 优化 Metabypass；保留 Stage1/2/3，本次将 21 个文件的既有修改完整归档到该提交 |
| RocksDB++ | `codex/rocksdb-plus-plus` | `cf2e80d42eab8f6c225e0917f547be06ec9ecb02` | 原 RocksDB++，名称不变；官方基线 + Stage2/3 原生 DW 优化，不含 Metabypass 备份服务 |

官方 RocksDB 对照固定为 v11.8.1 提交
`abeebd9630f11bd08c28b7bd43c7bdfc62050654`，不使用可能移动的分支名代替该提交。
主工作目录现在检出 `metabypass`；原 minus worktree 保留在
`reports/metabypass-minus-minus-20261022/worktree`，以 `de28572b42895744085c9be3a1308fcaf00a8d7c`
detached，不另建同名新分支。历史目录名、报告名及数值保持原样。

本次仅从 `metabypass-with-DW` 精确同步
[`driver.cc`](experiments/native-checkpoint/driver.cc) 和
[`driver-contract.md`](experiments/native-checkpoint/driver-contract.md)。
这两份文件提供已验证的官方 BlobDB / `MB_UPSTREAM` 适配、实际 Options 输出和 Put/WriteBatch
测量边界；同步驱动不引入该归档分支的引擎 DW 优化。
本次不修改 engine、公共 API、磁盘格式或备份算法，也不清理 `SealForSpace` 等遗留代码。

## 历史全量测试：保留结论，不转移验证身份

历史原始证据与完整结论见
[`reports/full-unit-20261022/run-20261022T052753Z/REPORT.md`](../../../reports/full-unit-20261022/run-20261022T052753Z/REPORT.md)。
该报告针对归档前的原主工作区：报告记录的 HEAD 是
`fb0f5092f97aa3ba7b7a56e5a331a87a30946c67`，并保留当时 staged/unstaged 补丁及其摘要。
这些工作区修改现由旧工作区归档提交 `3f333d250ac3f5afb33a0149d51cb698f675ae6f` 保存；
原报告的 HEAD 不能单独代表被测工作区，也不能将其结果算作当前 `metabypass` 分支通过。

| 历史检查 | 真实结论 |
|---|---|
| 主严格套件 | 40,850 个启用 case 最终均至少一次 pass；首轮 40,694 pass、156 个 60 秒 timeout。第 2 轮 155 pass、1 timeout，第 3 轮最后 1 个通过；未执行不需要的第 4 轮。首次 timeout 保留，原始 `make check` 退出码仍为 2，不是整体通过。 |
| 未执行范围 | 154 个 disabled case 未执行；154 个空 shard 单独统计，不能算通过。 |
| 严格模式默认排除的 GTest | `env_test`、`range_locking_test`、`testutil_test` 共 163 个启用 case：140 首轮 pass、23 个按 8/2/1/1 workers 四轮持续失败，报 `Failed to check Status` / SIGABRT。 |
| 默认排除的 `c_test` | 四次均在 `compactall` 阶段以 SIGABRT / 134 中止；后续内部阶段未覆盖，不算整个 binary 通过。 |
| `check-format` | 失败（2），报告当时 `native-checkpoint/driver.cc` 的 4 处格式差异。 |
| `check-c-api-gen` | 失败（2）；完整性独立检查 1,747 个声明通过，但相对当时本地 `main` 的兼容检查报告 21 个函数和 5 个符号/枚举/typedef 移除，后续 staleness 未执行，另未解析到 `CLANG_CXX`。 |
| `check-workflow-yaml` | 环境缺 Ruby，失败（2）；未安装依赖。 |
| 其他附属项 | 当时 Python 语法、`check-sources`、独立 C API completeness、`check-buck-targets` 通过；仅限该报告的源码和环境。 |

历史性能与构建参数可查
[`dw-stage3-baseline-20261022/README.zh-CN.md`](../../../reports/dw-stage3-baseline-20261022/README.zh-CN.md)、
[原 36 样本执行报告](../../../reports/dw-stage3-baseline-20261022/execution-report.zh-CN.md)及
[冻结结果汇总](../../../reports/dw-stage3-baseline-20261022/summary-verified/summary.zh-CN.md)。
这些是原 Stage3 比较，保留原名称和数值，不替代本基线的新正确性结果或新 36 样本。
`reports/` 是已有本地实验归档，独立于本次文档改动；新克隆若未带归档，应先取得原证据。

## 下一会话：先执行步骤 4，再执行步骤 5

以下是已批准计划的交接范围，本次没有运行其中任何构建、单测、Restore 或性能实验。

步骤 4 必须针对实际冻结的新 `metabypass` 源码执行完整 binary 的严格测试：
`metabypass_test`、`sst_tiering_test`、`checkpoint_test`、`db_blob_direct_write_test`。
不能只挑新用例或用历史目标测试代替完整 binary。遵循 `CLAUDE.md` 的
`ASSERT_STATUS_CHECKED=1`、每 case 60 秒、首轮 8 workers、仅精确失败/timeout 以 2/1/1 workers
重试的规则，保留各轮原结果，不将重试通过改写成首轮通过。

另对 DW 关闭、开启各做一次 HDD Restore 全量正确性验证：先写入 65,536 keys、1 KiB value，
完成 `SyncBackup` 后释放原 wrapper。保持原 HDD data/backup，隔离原 SSD index（如有 staging 也隔离），
向新的空目录 Restore；使用 `--verify-samples=0` 的全量校验语义，检查存在性、长度、key、版本、
checksum 和总 key 数。恢复全量核验后追加写入，关闭并重开，再验证追加结果及原数据。
恢复必须依赖 HDD 内容；4096-key reopen 抽样不能代替该门禁。两种 DW 的门禁均通过后，才进入性能步骤。

步骤 5 在同一次冻结、同批次实验中运行以下三种配置，每种均覆盖 DW 开/关：

| 配置 | 驱动/源 | 比较角色 |
|---|---|---|
| 官方 BlobDB | 固定官方提交，`MB_UPSTREAM`，`variant=B` | 对外主基线；DW 开和关均进入主表 |
| Metabypass | 当前 `metabypass` 同一源码，`variant=D`，备份开启、无 staging | 对外主测对象；DW 开和关均进入主表 |
| 内部 native | 与 Metabypass 完全相同源码，`variant=B`，关闭备份 | 内部诊断备份成本，不替代官方基线 |

总矩阵为 **3 配置 × 2 DW × Put / WriteBatch128 × 3 轮 = 36 个性能样本**。
两种 DW 都报告 Metabypass/官方 BlobDB 的比较；内部同源码 native 用于解释备份成本。
RocksDB++ 和 `metabypass-with-DW` 在此处保留为独立研究分支，不增加到本轮 36 个样本中。

每个配置/DW/负载独立建库，同轮 seed 相同、轮换顺序串行执行；不混入历史批次。
固定沿用既有 4 GiB 插入协议：4,194,304 keys、16B key、1 KiB value、单 writer、sync=false，
seeds 1729/1730/1731，256 MiB block cache、128 MiB write buffer、最多 2 个 write buffer，
256 MiB Blob、4 background jobs、64 MiB 备份 queue，无压缩、无 staging、无 SST tiering，
CPU 0–3、现有 8 GiB cgroup、SSD index/SST/WAL 和 HDD Blob。
备份检查点 5000 ms、镜像 1000 ms；native Blob 值搬迁 GC 关闭
（`enable_blob_garbage_collection=false`），Metabypass whole-blob GC 开启，
auto compaction 开启（`disable_auto_compactions=false`）。
以实际 OPTIONS/LOG、argv、源/二进制 SHA 和遥测归档核对，不从请求参数推断配置已生效。
每个性能样本 Close 后以独立进程 reopen，抽查 4096 个 key 的存在性、长度、版本与 checksum。
备份配置每个样本的前台必须至少 15 秒，且前台边界内至少新增 2 个恢复点；
不满足时标记覆盖不足并保留现场，不能删去样本或单独延长该配置冒充通过。
其余测量边界和资源约束引用上述既有报告及
[驱动契约](experiments/native-checkpoint/driver-contract.md)，新矩阵须单独冻结，不能直接运行旧六臂控制器。

报告保留各组 Q/T 中位数及延迟分位数、尾部 Flush/SyncBackup/Close 分项、WAL 字节与镜像量，
以及恢复点 lag。lifecycle 包含前台和最终 Flush、SyncBackup、Close，排除 Open、状态序列化及
独立校验/Restore；这些额外阶段分别归档，不混入性能总时。末端恢复点 lag 仅是原始快照，
不能冒称运行期间最坏 RPO。

吞吐和 lifecycle 总时均先算同轮 target/reference 比值，再取三轮比值的中位数；
同时保留每轮分子、分母、比值与范围。吞吐损失为 `100*(1-Qtarget/Qreference)`，
lifecycle 增加为 `100*(Ttarget/Treference-1)`，保留负号表示改善。
同轮 Q/T 配对比值中任一个偏离三轮比值中位数超过 5%，或各组任一 Q/T 原始样本偏离本组
三轮中位数超过 5%，均标为 flags 并降级解释。跨轮方向反转单独记录为诊断，不额外引入自动降级条件。
5% 是工程筛查线，不是统计显著性结论。**不自动补测**，调查或任何追加实验另行决定。
不得沿用旧 controller 的自动追加轮次策略，也不得把历史跨批次 median 比值冒称本轮配对结果。
