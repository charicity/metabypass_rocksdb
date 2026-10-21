# 新版 native-checkpoint 实验

新增 DirectWrite 比较采用[三臂、三轮162样本协议](../direct-write/README.zh-CN.md)，默认新推荐臂
开启 DirectWrite、检查点 5 秒。本文原 U/B/D/S 协议保留 false/1 秒以复现历史实验；
驱动另支持 `--direct-write`、`--checkpoint-interval-ms` 与 `--mirror-interval-ms`。

本目录独立验证当前原生 WAL、检查点、staging、整文件 GC 和 SST 分层。
协议为 `metabypass-native-perf-v1`，不读取旧 DirectWrite 的校准次数，不覆盖历史结果。
背景与旧结果见[历史实验总览](../history.zh-CN.md)，驱动参数和测量边界见
[driver-contract.md](driver-contract.md)。生产 API 不作扩展。

正式运行限于 `lj@node-ssd`（hostname `8001`），SSD/HDD 分别使用
`/home/lj/{ssd,hdd}/metabypass_test`，每次创建新的 UUID 子目录；控制结果目录必须在这两个数据父目录之外。
所有数据库子进程先应用同一 CPU affinity，再加入既有
`/sys/fs/cgroup/memory/mbgc-smallmem`；控制器保持在组外。
预检要求该组 tasks 为空、限制恰为 8 GiB、swappiness 为 0，并保存残留 cache、failcnt 和系统 swap 状态。
工具不使用 sudo、不清缓存、不修改 cgroup 或其他系统设置。

U 使用固定 RocksDB v11.8.1 提交 `abeebd9630f11bd08c28b7bd43c7bdfc62050654`，inline value、全部文件在 SSD；
B 使用当前引擎和普通 BlobDB，仅将 `.blob` 路由至 HDD；D 增加 HDD 备份、关闭 staging；S 增加 256 MiB staging。
B 的适配器完整转发原生删除，并在可用性门禁通过三代整库覆盖及 compaction 实际观察 `native_blob_deletes>0`。
所有组关闭压缩，256 MiB block cache、128 MiB write buffer、最多两个 write buffer、四个后台任务。
BlobDB 组关闭 Direct Write，`min_blob_size=0`，完整值进入 WAL；关闭 Blob 值搬迁 GC。

## 构建与入口

首次编译与正式实测分离计时。`build.py --out=/absolute/external/build` 构建两个 Release 驱动并输出 `build.json`。
远端无 `.git` 的 source export 可分别调用 CMake；必须保留导出源身份、编译命令和实际二进制 SHA256。
runner 接受离线 manifest，不调用 git。manifest 至少包含：

```json
{
  "protocol": "metabypass-native-perf-v1",
  "upstream_commit": "abeebd9630f11bd08c28b7bd43c7bdfc62050654",
  "source": {"export_manifest": "absolute path and digest, or full git identity"},
  "binaries": {
    "current": {"path": "/absolute/current/mb_native", "sha256": "actual digest"},
    "upstream": {"path": "/absolute/upstream/mb_native", "sha256": "actual digest"}
  }
}
```

以下命令在正式目标主机执行；`$CONTROL` 代表本任务新建的独占控制目录，`$TOOLS` 指向本目录的部署副本。
输出路径必须从未存在，不能复用即使为空的目录。

```bash
python3 "$TOOLS/run.py" preflight --profile formal
python3 "$TOOLS/run.py" plan --profile formal > "$CONTROL/provisional-plan.json"
python3 "$TOOLS/run.py" gates --profile formal --no-time-limits \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/gates-01"
python3 "$TOOLS/run.py" calibrate --profile formal --no-time-limits \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/calibration-01"
python3 "$TOOLS/run.py" run --profile formal --no-time-limits \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/results-01"
```

`preflight` 与 `plan` 为只读。`gates` 只运行小规模可用性与精确 SIGKILL 恢复门禁，不启动性能矩阵。
`calibrate` 在可用性门禁之后运行四组共同的写入/同步校准并冻结矩阵、输出预算预测；不创建 12 GiB 静态数据。
`run` 在同一次正式会话重新执行门禁及校准，自动完成矩阵、故障恢复、专项和阈值复测。
本次用户已明确取消总时长与阶段预算上限，因此使用显式 `--no-time-limits`。
该模式仍执行原定 120 个核心样本、21 个专项样本及三轮重复，不缩小数据、改变负载或取消异常复测。
总 deadline 和全部阶段 cap 均为无限，预测耗时仅供参考，不因预测超过八小时或分项额度拒绝执行。
报告与冻结协议将 `budget_seconds`、`remaining_seconds` 和各阶段 cap 写为 JSON `null`，
并保留 `time_limit_policy` 和包含该 flag 的原始控制命令；不会输出非法的 `Infinity`。
原有单次驱动超时在此模式改为异常停滞检测：stdout、进程 CPU 或 I/O 计数持续推进就刷新 watchdog，
不会在单次操作正常推进时按总耗时截断，也不会按阶段剩余额度裁剪 watchdog。
目录归属、磁盘余量、cgroup 独占、配额和数据正确性约束仍然有效。

为复现之前的受限协议，省略 `--no-time-limits` 时仍默认总上限 28800 秒、原阶段分配和原有 elapsed watchdog。
此时 `--budget-seconds` 只能缩短总上限；不会自动缩小正式数据或减少重复。

阶段预算默认依次为 preflight 1800、calibration 5400、core 9000、specials 5400、recovery 3600、reserve 3600 秒。
若用户明确批准在八小时内调剂阶段预算，可显式传入 `--stage-budgets-json /absolute/stage-budgets.json`；
文件必须恰有上述六个 key，每项为正的有限秒数，总和不得超过 28800。
该参数不会增加整体 hard deadline，不改变矩阵、三轮重复或数据规模，也不会自动选择新的预算。
预检、冻结协议和结果报告会保留实际预算、配置文件原文/SHA256 和控制命令，便于核对批准内容。
例如可审阅的调剂文件如下；使用前须确认用户已经批准该具体分配：

```json
{"preflight":1200,"calibration":3300,"core":10200,"specials":6900,"recovery":3600,"reserve":3600}
```

本地功能联调只能使用 `--profile smoke`，且必须显式提供已经存在、属于当前 uid 的临时 SSD/HDD 父目录。
smoke 使用相同矩阵结构但较小数据和较短窗口，允许同盘，不加入正式 cgroup；任何结果均不作为硬件性能结论。

六程序严格回归由独立 case runner 执行。`--regression-audit` 接受保留原始 case 清单/日志引用的规范包装：

```json
{
  "status": "pass",
  "assert_status_checked": true,
  "timeout_per_test_seconds": 60,
  "retry_workers": [8, 2, 1, 1],
  "binaries": ["metabypass_test", "sst_tiering_test", "checkpoint_test",
               "db_blob_basic_test", "db_blob_compaction_test", "db_blob_corruption_test"],
  "persistent_failures": [],
  "incomplete_cases": []
}
```

## 测量与验收

核心矩阵各三轮，按轮次轮换 U/B/D/S 顺序。共同校准将 insert payload 冻结为 512 MiB 至 1 GiB、同步写冻结为 128 至 512 次。
覆盖 1 KiB 的 1/4 线程插入、4 KiB 单线程插入、1 GiB 四线程随机覆盖、四组 12 GiB 静态数据的随机/热点/100 条扫描、
U/B/S 四线程随机读、90/5/5 混合、同步写及 U 全 HDD 写入诊断。
热点访问使用互斥冷热区，80% 请求访问前 25% key。只读预热 15 秒、测量 60 秒；混合预热 30 秒、测量 120 秒。
每次写入使用独立库；静态数据每组只建一次并全量核验一次，各只读窗口中的 Get/scan 仍逐值校验。

值编码 key、单调版本和校验码；删除后的版本仍保留，不把重插入的旧值当作正确。
写入前台、Flush、SyncBackup、Close 与 lifecycle 分别计时，Open 另列。
分位数从全线程合并的操作直方图计算；GC 三代合并频次，不平均各代 P99；warmup 不进入 measured histogram。
CPU/RSS 覆盖完整数据库子进程；吞吐包含共同驱动的 payload/校验工作，操作延迟边界见驱动契约。

staging 专项比较 256 MiB 与 64 KiB；门禁另外覆盖真实驻留、2 MiB 前缀迁移和超容量单个 value。
GC 开/关各三轮，每轮 1 GiB 初始库、三个整库换代、每代 Flush/CompactRange/SyncBackup，结束观察最多 60 秒。
SST 专项使用 64 KiB cache、禁用自动 compaction 以保留足够 SST；disabled 当前 index 中至少四个 SST 的逻辑字节乘 65% 冻结预算，
不把 HDD 检查点副本重复加进容量。disabled/observe/adaptive 各三轮，预热 30 秒、测量 120 秒并切换热点。
没有真实回收、采样或迁移的专项标为 incomplete 并保留现场，继续其余独立项。

恢复先重命名原 SSD index/staging 的外层归属容器，在新的空 `db/` 中恢复；只使用 HDD 保留内容，之后全量核验、追加、关闭重开再核验。
外层 marker 不进入引擎数据目录。写入、LATEST 发布和 staging 前缀迁移使用实验文件系统的精确 ready gate；
控制器 fsync 外部 ACK 和 ready 证据后，仅向 PID/starttime/session 均匹配的本任务进程组发 SIGKILL。
ACK 恢复允许未确认但已经成功的更新版本；仍必须包含所有已确认写入及全量正确校验码。
这些是指定文件操作边界和进程故障实验，不是物理断电认证。

成功的独立样本先保存实际 OPTIONS、原始 stdout/stderr、阶段计时、统计和遥测，再删除其数据库容器；
四组静态库保持至实验结束，所有失败/未覆盖样本保留。因此在线峰值预估为 SSD 40 GiB、HDD 100 GiB，
另要求两盘至少保留各自总容量 20% 的余量；每个进程启动前再次检查空间。
SSD 峰值包含 U 的 12 GiB 数据、B/D/S 索引、S staging、当前独立样本及恢复时隔离原件；HDD 包含三个 12 GiB Blob 数据及 D/S 检查点和当前样本。
失败现场累计可能增长，达到余量门槛立即停止，绝不清理失败来继续。

采集 `/proc` CPU/RSS/io、diskstats、memory cgroup 状态、swap 计数、按 inode 去重空间和 mincore 驻留。
mincore 单独统计 SST 与原始 Blob/HDD `segment-*` payload，不读数据来制造预热。
退出边界可能在同一次采样中出现 `/proc/<pid>/io` EACCES 或进程文件消失。
仅当该采样确认已启动子进程的 PID、starttime、进程组及 session 一致，且观察到同身份 Z/X 状态、`/proc/stat` flags 的 `PF_EXITING=0x4` 或 `Popen.poll()` 已退出，
才把 ENOENT/ESRCH/EACCES 原始错误连同身份、errno 和退出证据记入 `expected_exit_errors`。
活进程缺失、身份变化、全局 diskstats 或 cgroup 遥测缺失仍使样本 incomplete；稍后成功退出不能追认早期采样错误。
12 GiB 大于 8 GiB 本身不证明冷读；应结合设备实际读量与驻留比例解释。
设备总计数可能包含其他进程，不直接称为 SST 流量或引擎写放大。

数据错误、恢复失败和配额越界封禁对应配置的性能排名，现场保留；共同安全约束、总 deadline、空间或 cgroup 归属异常停止整个实验。
每组必须具备三轮有效原始样本才计算完整配对退化；复测不替换原失败轮次。
整体 incomplete 不掩盖其余已通过功能门禁且三轮完整的核心比较。
吞吐下降使用 `1-candidate/reference`；延迟/资源增加使用 `candidate/reference-1`，报告逐轮配对值及中位数/min/max。
D/B 表示核心备份成本，S/D 表示 staging 成本，D、S/U 包含布局与服务差异。
U/B 同步写只承诺 WAL 持久化，D/S 还承诺 HDD 备份覆盖；其比值称为额外服务成本。
三轮仅作筛查，超过吞吐 10%、lifecycle 10% 或 P99 20% 会在余量预算内独立复测。

结果目录包含 `preflight.json`、完整 `build.json`、`frozen-protocol.json`、逐进程目录、`results.json` 失败账本、
`summary.json` 配对统计和 `report.zh-CN.md`。可重复汇总：

```bash
python3 "$TOOLS/summary.py" "$CONTROL/results-01/results.json"
python3 -m unittest discover -s "$TOOLS" -p test_runner.py -v
```

## v7 事故与独立续跑

真实内核为 `5.4.0-216-generic #236-Ubuntu`。v7 的 `0349-append-reopen` 已完成全量核验并正常退出，
但 sample 26 的 `/proc/1871853/io` 返回 EACCES，导致控制器保守保留为 incomplete。
该次采样原始 stat flags 为 `4194308=0x00400004`，已经设置 `PF_EXITING`，虽然 state 仍为 R。
Linux 官方 [sched.h](https://github.com/torvalds/linux/blob/v5.4/include/linux/sched.h) 定义此位，
[do_exit](https://github.com/torvalds/linux/blob/v5.4/kernel/exit.c) 在释放内存与 cgroup、形成 zombie 之前设置它。
新控制器只接受同次采样的该位作为退出证据，不重试、不用 CPU 停滞或最终成功追认早期错误。
原 v7 结果、incomplete 行及故障目录保持原状。

`resume.py` 专用于已审计的这次停止位置：继承 135 行（8 校准、120 核心、6 专项通过及 1 专项 incomplete）和 21 个通过门禁。
它逐条核对冻结 spec、唯一轮次、源码/构建与二进制身份、命令、原始 stdout、OPTIONS 哈希、核验和恢复记录；
源控制器必须退出，现有目录归属、设备、CPU affinity、cgroup 独占与空间余量必须仍满足要求。
续跑不重新校准、建静态库或执行已通过的样本；SST 容量沿用 `15337964` 字节。
仅余下 14 个未执行专项进入原 measured 矩阵；原事故场景的新样本用独立 `phase=attempt`，不补齐缺失的原始轮次。
触发的性能复测仍使用独立 `phase=retest`。因此其余专项通过时原始有效样本为 140/141，整体仍为 incomplete。

新输出目录、新 UUID 和新 owned 数据容器与 v7 分开，失败现场不清理。复测需要旧静态库时，先验证旧 UUID/角色边界及 marker，
确认 sidecar 的全部版本仍为初始版本一，并在 cgroup 内全量核验；只允许只读复测，不清理旧库。
原生 Open 可能生成 WAL/OPTIONS，故要求逻辑内容和 sidecar 不变，不要求整个目录字节不变。
旧静态库的 SSD/HDD 空间按其已验证源根分类；跨结果目录的原始文件链接保留来源。

先在控制器外记录源 `results.json`、`frozen-protocol.json` 及 v7 工具身份清单的 SHA256，再使用明确参数审阅只读计划：

```bash
python3 "$TOOLS/resume.py" plan \
  --source "$CONTROL/formal-v7/results.json" --source-sha256 "$SOURCE_SHA256" \
  --protocol-sha256 "$PROTOCOL_SHA256" \
  --source-tools-manifest "$CONTROL/controller-formal-v7-identity.json" \
  --source-tools-sha256 "$SOURCE_TOOLS_SHA256" \
  --build-manifest "$CONTROL/build/build.json" \
  --regression-audit "$CONTROL/regression-audit.json" \
  --output "$CONTROL/formal-v8"
```

审阅后用同样参数将 `plan` 改为 `run`，实际执行时再次完整验证，输出目录必须尚不存在。
`resume-plan.json` 保存执行清单，`resume_provenance` 保存源文件哈希、原失败账本及两处 raw 根。
独立边界测试为 `python3 -m unittest test_runner test_resume -v`。

`analyze.py RESULTS_JSON` 输出中文详细报告及 JSON。GC 三代 Flush/Compact 事件分别求和，与最终 Flush/SyncBackup 分列；
每代 SyncBackup 没有独立 phase，报告不捏造耗时归因。默认历史总览链接按报告实际目录计算，也可用 `--history-link` 指定。
