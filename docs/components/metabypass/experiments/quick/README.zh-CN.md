# Metabypass 开发期间快速性能测试计划

## 目标与边界

每次只测当前 Metabypass、同版本关闭备份基线、原版 RocksDB v11.8.1；历史里程碑直接导入已保存的结果。本计划用于开发回归筛查和解释备份成本，不用于真实 HDD 稳态吞吐、断电可靠性或统计显著性认证。

日常测试预算为 **30–60 分钟上限**，不含首次编译。实际完成时间取决于机器、校准后的写入数和恢复速度，可以提前完成，不为凑时间增加负载。正式矩阵在本次工具开发中不执行；小样本验证记录见文末。

| 对象 | 二进制与布局 | 结论口径 |
|---|---|---|
| `current` | 当前源码；索引不加延迟；Blob 与备份加延迟；关闭 staging | 待测方案 |
| `baseline` | 同一当前二进制；相同 Blob Direct Write 与保留策略；没有 Backup | 备份模块的附加成本 |
| `upstream` | 原版 v11.8.1，固定提交 `abeebd9630f11bd08c28b7bd43c7bdfc62050654`；普通 inline value；整个 DB 路径加延迟 | 包含布局差异的整体方案比较 |

两个基线均不生成备份点，因此总耗时比不能解释为“相同恢复保证下的加速比”。本轮统一 `sync=false`；最新 Metabypass 的 `sync=true` 包含备份发布屏障，与旧版完成语义不同，不混入本矩阵。

## 环境、负载与时间控制

使用同一份独立 C++ 驱动分别连接两个 Release 静态库，保持负载生成和计时边界一致。独立 CMake 工程不修改生产库 API，也不接入主项目的 Make/BUCK 测试目标。正式运行期间不编译、不并行运行其他测试。

- Linux 本地磁盘，建议使用 `/tmp` 下独立目录；先用 `findmnt -T /tmp` 确认它不是 tmpfs/NFS。所有逻辑快慢目录使用同一文件系统，只有延迟注入策略不同。
- 默认固定到当前允许 CPU 集合的前 8 个核，可用 `--cpus 0,1,...` 显式指定；以当前进程实际允许的 CPU 编号为准。
- 单写线程，1 KiB 固定 value，WAL 开启，无压缩，无 Blob GC。每次从全新空目录开始，结束后核对全部 key/value。不清全局页缓存。
- 默认 key 为递增整数的十进制字符串，与早期实验一致；这不是固定宽度字典序 `fillseq`。压力场景将 key 补齐到 128 字节。

| 场景 | 延迟 | 配置 |
|---|---:|---|
| `control` | 0 | 默认 RocksDB 参数；备份队列 64 MiB，触发阈值 256 KiB，间隔 1 秒 |
| `slow` | 1 ms/目标操作 | 同上 |
| `pressure` | 1 ms/目标操作 | write buffer / 目标 SST 各 256 KiB；L0 触发值 2；层级基数 1 MiB；后台任务 4；备份队列 2 MiB、阈值 64 KiB |

延迟作用于选定路径的 writable `Append`、`Sync`、`Fsync` 和命名文件 `SyncFile`，每次同步只计一次。三种模式共用实现。记录调用数和实际等待微秒；等待合计可因后台并行超过墙钟时间。不注入读延迟、目录同步延迟，不模拟带宽、寻道或断电。

每场景先对三个对象各跑 2,000 次写入并验证，作为预热和校准。以最慢对象的 `foreground + sync + close` 推算约 30 秒的写入数，然后在该场景内统一冻结：最少 2,000 次，`control` 最多 200,000 次，其余最多 50,000 次。不能分别为三个对象调整次数。

- `standard`：每组 5 次正式样本，共 45 次写入及相应校验，预算 60 分钟。
- `quick`：每组 3 次正式样本，共 27 次写入及相应校验，预算 30 分钟。
- `smoke`：每组 32 次预热、64 次正式写入各一次，预算 5 分钟；仅验证工具可用。

独立的 `backpressure` 档位是当前版本的机制验证，不加入上述三方矩阵，也不用于速度排名。它只运行一个 `current` 正式样本：16,384 次 Put，128 字节 key、1 KiB value，无预热和校准。保持默认大 memtable / SST 配置，备份队列容量为 2 MiB、批量阈值为 64 KiB、间隔为 1 秒。仅对 `backup` 路径的目标 writable `Append`、`Sync`、`Fsync` 和命名 `SyncFile` 每次注入 1 ms 延迟；`index` 和 `data` 路径不注入延迟。默认总预算 5 分钟、单子进程 180 秒，可用现有参数覆盖。

对象顺序按场景和轮次轮换。单子进程默认 180 秒超时，且受全局剩余预算约束。超时立即停止该进程组，标记 `incomplete` 并保留原始输出及失败数据。任何写入、恢复、内容核对或追加重开失败也会停止本轮；不丢弃失败后挑选新样本补齐。

## 新窗口执行步骤

以下命令均从仓库根目录执行。可直接把本文件交给新窗口中的代理，要求只执行本节并交付报告。

### 1. 准备或复用 Release 驱动

```bash
cd /home/ceph-lj/metabypass_rocksdb
python3 docs/components/metabypass/experiments/quick/build.py \
  --out /tmp/metabypass-quick-build
```

构建器从本地 Git 固定提交导出上游源码，不需要网络。默认最多 16 个编译任务，并按可用内存和 CPU 数进一步限制；必要时显式传 `--jobs 8`。两个库依次构建，构建日志与 `build.json` 留在构建目录。独立输出避免混用仓库根目录的 Debug 对象。再次执行会复用 CMake 编译缓存。

如果本轮交付的 `/tmp/metabypass-quick-build/build.json` 仍在，且源码没有变化，可直接进入下一步。runner 会核对生产/实验 C++ 源码身份、驱动身份及两个二进制哈希；不匹配则要求重建，不静默使用旧二进制。

### 2. 先运行脚本自检及小样本

```bash
python3 -m unittest discover \
  -s docs/components/metabypass/experiments/quick -p test_runner.py -v
python3 docs/components/metabypass/experiments/quick/run.py \
  --build /tmp/metabypass-quick-build/build.json \
  --data-parent /tmp --output /tmp/metabypass-quick-smoke-new \
  --profile smoke
```

`--output` 必须是尚不存在的目录；如重跑，请更换后缀。成功时退出码为 0，JSON 的 `state` 为 `complete`，9 个正式小样本均通过验证。小样本中的后台 flush/compaction 或背压为 0 是允许的，不作为机制覆盖证明。

### 3. 选择一个正式档位

默认执行 60 分钟预算的 5 次版本：

```bash
python3 docs/components/metabypass/experiments/quick/run.py \
  --build /tmp/metabypass-quick-build/build.json \
  --data-parent /tmp --output /tmp/metabypass-quick-standard-new \
  --profile standard
```

时间紧张时，将 `--profile standard` 改为 `--profile quick`，并更换输出目录。不要在同一轮中根据测量结果改变样本数。

后续提交的回归测试应复用首份正式结果的冻结写入数：

```bash
python3 docs/components/metabypass/experiments/quick/run.py \
  --build /tmp/metabypass-quick-build/build.json \
  --data-parent /tmp --output /tmp/metabypass-quick-next-new \
  --profile standard \
  --counts-from /tmp/metabypass-quick-standard-new/results.json
```

`--counts-from` 仅接受同协议、完整的 `quick` 或 `standard` 三方比较报告；`smoke` 和 `backpressure` 不能使用此参数，也不能作为写入数来源。CPU 集合、物理机器、文件系统及后台负载也须匹配才能解释跨次变化；复用次数本身不保证环境可比。发生源码改动后先重新执行构建步骤。

### 独立背压机制验证

完成上面的构建步骤后，使用一个新的输出目录运行：

```bash
python3 docs/components/metabypass/experiments/quick/run.py \
  --build /tmp/metabypass-quick-build/build.json \
  --data-parent /tmp --output /tmp/metabypass-quick-backpressure-new \
  --profile backpressure
```

该档位只对 `current` 的 `backpressure` 场景运行一次，不自动加入 30–60 分钟正式矩阵。硬验收要求 16,384 次写入全部成功、前台 Put 循环期间的背压差值及 Close 后的总背压均大于零、队列峰值大于零且不超过报告中的 2 MiB 容量、实际发生备份路径延迟，并且至少发布一个恢复点。写入后仍移除本轮索引，执行 Restore + Open、全量 key/value 核对、追加写入和重开核对。任何一项失败都会留下 `incomplete` 和原始日志，不自动扩量或补跑。`report.md` 标记 `mechanism_validation` 和 `current-only`，记录写入数、恢复、前台/总背压、队列峰值/容量与时间，不提供三方速度比。

## 验证、报告与验收

每次写入先结束备份屏障和 Close/销毁，再记录磁盘占用和 OPTIONS。当前版本移除本次测试的主索引，调用 Restore + Open，逐条读回全部数据，追加一条新记录，再关闭、重开并核对新记录。两个基线保留主索引，执行同样的全量核对与追加重开。只删除 runner 自己新建的测试子目录；成功数据默认清理，失败数据保留，可用 `--keep-data` 保留全部数据。

`report.md` 提供以下逐场景、逐对象结果，均为中位数及最小—最大范围：

1. 前台吞吐、Put P99、完整写入生命周期总时间。
2. 当前版本 Restore + Open/WAL 回放时间；全量读取核对不包含在恢复时间内。基线恢复时间为 N/A。
3. 写入子进程 CPU 时间、峰值 RSS，以及验证前的磁盘实际分配字节数。磁盘占用按 inode 去重，避免硬链接重复计数；它不是设备写入量或写放大。

总时间逐次相加后再取中位数，排除 Open、延迟样本排序和验证；Close 包含对象销毁。CPU/RSS 覆盖整个写入子进程，因此包含 Open 与排序等开销。P99 只覆盖 Put，不含 key 生成。无备份基线 `sync_us=0`，并不意味着具备备份屏障的服务保证。

`results.json` 保存构建身份、环境、协议参数、真实 OPTIONS、运行命令、逐次 stdout/stderr、失败记录、物理空间及辅助指标。队列峰值、背压、镜像字节、发布点数、最后构建/滞后时间均保留。最后一次备份点滞后不解释为最大 RPO。

**验收要求：** 所有预定样本完成且内容验证通过；当前版本至少发布一个恢复点；慢盘场景实际发生延迟注入；报告的成功样本数与档位一致。压力场景额外查看前台 flush、compaction、背压覆盖计数：零表示未覆盖该机制，不能以场景名称宣称已经覆盖。若需强化压力，另开协议版本，不改变本轮冻结参数。

后续同协议、同环境测量中，吞吐下降超过 10%、总时间增加超过 10% 或 P99 增加超过 20% 时触发一次独立复测。这些是人工开发筛查阈值，不自动判定回归，也不代表统计显著性；当前工具不把旧实验差异自动转换成告警。

历史表自动从 `version-results.json`、`no-backup-results.json`、`channel-no-backup-results.json` 导入，保留源文件哈希和原文环境说明。旧表中的 `current` 是当时的通知优化版，不能当作现在的 HEAD。各历史实验独立显示，不计算跨实验、跨环境或与本轮慢盘结果的加速比；缺失记录标注缺失。

## 本轮交付验证

2026-09-26 已完成以下可行性检查，**没有执行 `quick` / `standard` 正式矩阵，也没有执行仓库全量测试**：

- 当前库与固定上游库的两个独立 Release 驱动构建成功，可复用 `/tmp/metabypass-quick-build/build.json`。
- 4 项脚本自检全部通过：验证失败不计入统计、总时间按逐次合计汇总、超时保留输出、硬链接空间去重。
- `smoke` 的 18 次预热/正式小样本及对应验证全部通过，耗时约 90 秒。结果保存在 `/tmp/metabypass-quick-smoke-validation/results.json`，不是正式性能基准。
- 三个对象各执行一次 5,000 次、无注入延迟的 `pressure` 定向写入并验证成功。当前版本观察到 2 次前台 flush、0 次前台 compaction、0 背压；不宣称后两项已覆盖。记录在 `/tmp/metabypass-quick-feasibility.json`。
- 定向破坏测试目录的备份 `LATEST` 后恢复被拒绝；子进程超时和全局预算耗尽均退出非零，保留 `incomplete` 报告且没有成功样本。分别见 `/tmp/metabypass-quick-timeout-validation/` 和 `/tmp/metabypass-quick-budget-validation/`。
- `make check-sources`、新文件 ASCII/空白/Python 语法检查及 `git diff --check` 通过。`make format-auto` 因本机缺少 clang-format/clang-format-diff 未能执行。

2026-09-26 对新增的 `backpressure` 档位完成独立验证：两个 Release 驱动增量构建通过，脚本自检 9/9 通过。独立运行两次均为 `complete` / `mechanism_validation`，各完成 16,384 次写入、全值核对、追加与重开核对；队列峰值均为 2,097,152 字节，等于 2 MiB 容量，前台及总背压均大于零，确认该场景真正触发了背压。

| 独立运行 | 前台 / 总背压 | 恢复点 | Restore + Open | 完整运行时间 | 结果 |
|---|---:|---:|---:|---:|---|
| 1 | 10,110,478 / 10,110,478 微秒 | 7 | 1,056,361 微秒 | 22.311 秒 | `/tmp/metabypass-backpressure-validation-1/report.md`、`results.json` |
| 2 | 10,204,902 / 10,204,902 微秒 | 7 | 1,057,123 微秒 | 22.583 秒 | `/tmp/metabypass-backpressure-validation-2/report.md`、`results.json` |

两项 CLI 负例均以退出码 2 拒绝，未产生输出目录或样本：`backpressure --counts-from`，以及让 `quick --counts-from` 读取机制验证报告。仍未执行 `quick` / `standard` 正式矩阵或仓库全量测试。

这些临时结果随 `/tmp` 生命周期保留；正式运行后请将结果目录另行归档。新窗口应以第 3 节的正式命令生成首份可用于未来回归比较的基准档案。
