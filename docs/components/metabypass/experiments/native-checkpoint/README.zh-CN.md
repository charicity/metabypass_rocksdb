# 原生 WAL、检查点和 SST 分层实验

当前协议为 `metabypass-native-perf-v2`。U 使用固定 upstream RocksDB v11.8.1
的普通内联值路径。D 使用当前内联 MetaBypass，主库在 SSD，WAL 与恢复点备份在
HDD。旧实验的结果、日志和协议见[历史原生检查点验证记录](../../native-checkpoint-validation.zh-CN.md)。

## 构建和执行

`build.py` 在仓库外的新目录建立两个 Release 驱动，默认使用 48 个编译任务，仅在
识别到 OOM 后依次用 32、16、8 个任务重试。上游固定为提交
`abeebd9630f11bd08c28b7bd43c7bdfc62050654`。离线源码导出也可直接使用 CMake
构建，保存导出源身份、编译命令及 CMake 缓存。控制器读取 v2 `build.json` 中的
`upstream_commit` 以及 `binaries.current`、`binaries.upstream` 的实际路径和
SHA256。

```bash
python3 "$TOOLS/build.py" --out "$CONTROL/build"
python3 "$TOOLS/run.py" preflight --profile formal
python3 "$TOOLS/run.py" plan --profile formal
python3 "$TOOLS/run.py" gates --profile formal \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/gates"
python3 "$TOOLS/run.py" run --profile formal \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/results"
```

`preflight` 与 `plan` 为只读。输出目录必须是新目录。`gates` 运行功能和
SIGKILL 门禁。`calibrate` 运行功能门禁及两组共同校准，然后冻结负载。`run`
重新校准并完成核心矩阵、故障恢复、SST 专项和阈值复测。使用 `smoke` 时，
显式提供属于当前 UID 的临时 SSD/HDD 父目录。smoke 允许同盘，用小数据和短
窗口验证功能，报告标记为 smoke。

正式运行默认总上限 28800 秒。默认阶段预算分别为 preflight 1800、calibration
5400、core 9000、specials 5400、recovery 3600、reserve 3600 秒。
`--stage-budgets-json` 接受包含这六个阶段的正数秒数配置，总计最多 28800 秒。
`--no-time-limits` 解除全局和阶段时限，保留单驱动停滞检测。报告将解除的时限
记为 JSON `null`，并保存实际控制命令。

正式验收使用独立的 `metabypass_test`、`sst_tiering_test` 和 `checkpoint_test`
严格回归审计。审计记录 `assert_status_checked: true`、`timeout_per_test_seconds:
60`、`retry_workers: [8, 2, 1, 1]`、`status: "pass"` 及两个空数组
`persistent_failures`、`incomplete_cases`。

## 核心与 SST 专项

- 核心有 63 个样本，按轮轮换 U/D。每组包含 1 KiB 的单线程及四线程插入、4 KiB
  单线程插入、四线程随机覆盖、随机读、四线程随机读、热点读、100 条扫描、混合
  读写和同步写。U 还执行全 HDD 插入诊断。共同校准将插入 payload 冻结在
  512 MiB 至 1 GiB，将同步写冻在 128 至 512 次。
- 两组分别准备并全量核验 12 GiB 只读库。测量窗口继续逐值校验 Get/scan。
  只读预热 15 秒、测量 60 秒。90% Get、5% Put、5% Delete 的混合负载预热
  30 秒、测量 120 秒。
- D 的 `disabled`、`observe`、`adaptive` SST 模式各跑三轮。专项使用
  64 KiB 块缓存，禁用自动 compaction，准备期间定期 Flush 形成多个 SST。
  首个有效 disabled 样本从 SSD 主库至少四个 SST 的去重逻辑字节计算 65% 的
  容量预算。observe 和 adaptive 使用同一冻结预算，并分别要求观察到读采样及
  计划或实际 demotion。正式窗口预热 30 秒、测量 120 秒，测量中途移动热点。
- D 每个 SST 专项执行 HDD 恢复、全量验证、追加和重开。未达到机制覆盖的原始
  样本标记 incomplete 并保留现场。

U 的同步写持久化原生 WAL。D 的同步写还等待备份点覆盖。D/U 的延迟比包含额外的
备份服务。报告记录前台吞吐、全线程直方图分位数、生命周期、完整进程 CPU/RSS、
按 inode 去重的文件空间、设备 diskstats 与 SST 的 mincore 驻留。设备计数包含
主机上的其他 I/O。各核心场景以三轮同轮配对报告中位数及范围。吞吐下降 10%、
生命周期增加 10% 或 P99 增加 20% 触发余量阶段的独立复测。

## 恢复和数据归属

可用性门禁覆盖 D 的仅 WAL 恢复和带 SST 的检查点恢复，以及 U/D 的并发混合和
同步写。D 的 SIGKILL 门禁覆盖写入 ACK 和 `backup/LATEST` 重命名前两个边界。
控制器 fsync 外部 ACK 和 ready 证据，只向 PID、starttime、进程组及 session
匹配的本轮进程发送信号。恢复必须覆盖所有已确认版本，允许更晚的有效写入。

恢复前，控制器将外层 SSD 主库容器重命名隔离，再建立新的空数据库目录，使用
HDD 备份执行 Restore 和 Open。归属 marker 在容器外层。成功的独立样本在
归档 OPTIONS、计时、统计及原始日志后清理数据库。静态库和失败现场保留。
正式预检按每盘 40 GiB 在线峰值并留出各盘总容量 20% 的余量。每个驱动启动前
重新检查余量。正式 cgroup 必须为空且归属明确。活进程身份变化、全局 diskstats
或 cgroup 遥测缺失会使样本 incomplete。只有当前遥测窗口证明同一拥有的进程已
退出或正在退出时，才将相应的 `/proc` 退出竞态归入预期事件。

`results.json` 保留原始样本、失败账本及逐进程记录。另有
`frozen-protocol.json`、`summary.json` 和 `report.zh-CN.md`。
`python3 "$TOOLS/analyze.py" "$CONTROL/results/results.json"` 生成详细配对和恢复报告。
驱动参数和计时边界见[驱动契约](driver-contract.md)。

## 续跑

`resume.py plan|run` 接受以显式 `--no-time-limits` 启动的 v2 正式运行。源
运行须已退出且原始核心样本和恢复门禁均通过。提供源 `results.json`、
`frozen-protocol.json` 及原控制工具身份清单的 SHA256，以及相同的 build manifest。
工具身份清单为文件名到 SHA256 的 JSON 对象，可在部署控制器时记录。使用
`resume.py plan` 审查待测专项和独立尝试，再对新的输出目录执行
`resume.py run`。未运行的专项仍使用原冻结轮次。原 incomplete 记录及其证据
保留，成功的独立尝试只进入尝试统计。禁用组尚无有效样本时，续跑可以从首个
待测的 disabled 样本建立 SST 容量预算。原始静态库可在验证归属和状态后用于独立
只读复测。
