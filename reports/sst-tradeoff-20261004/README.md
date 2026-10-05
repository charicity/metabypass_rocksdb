# SST 冷热策略的空间与性能取舍

归档范围：本 README 随仓库提交；下文链接的 `raw-node/`、`analysis/`、`deployment/`、`preflight/` 和 `unit/` 为本地保留的实验产物，不纳入本次提交。复现报告分析需要另行取得这些归档，仅克隆仓库不足以复现。

状态：主矩阵 28/28 轮、独立 conservative90 补测 8/8 轮均完成且有效，两个 run 完整性均 intact。合计 36 个真实样本；从主启动到补测自然退出 142.78 分钟，在用户批准的 150 分钟上限内。用户当前采用 default80，作为项目启用 SST 分层时的推荐默认 profile；库自身仍默认 Disabled，需显式启用并指定绝对字节预算。default65 保留为附带吞吐与延迟条件的替代候选。

## 冻结的策略矩阵

现有分层以整张 SST 为单位。全部沿用尽力一致性和 SSD 全盘丢失后的恢复协议，本轮只使用已有配置参数，不修改生产 C++。

| 策略 | 名义 SSD 预算 / 基准 SST | 有效策略上限 | 降级轮数 | 最短驻留 | 冷文件排序偏置 margin |
|---|---:|---:|---:|---:|---:|
| disabled_ssd | 全 SSD | 不适用 | 不适用 | 不适用 | 不适用 |
| default50 | 50% | 约 45% | 3 | 10 秒 | 0.25 |
| default65 | 65% | 约 58.5% | 3 | 10 秒 | 0.25 |
| default80 | 80% | 约 72% | 3 | 10 秒 | 0.25 |
| default90 | 90% | 约 81% | 3 | 10 秒 | 0.25 |
| conservative50 | 50% | 约 45% | 8 | 30 秒 | 1.0 |
| conservative65 | 65% | 约 58.5% | 8 | 30 秒 | 1.0 |
| conservative90（独立 v2 补测） | 90% | 约 81% | 8 | 30 秒 | 1.0 |

保留 10% reserve；有效上限有整数舍入，实际占用还有整张 SST 粒度。超预算时降级绕过驻留/连续轮数保护；保守策略主要改变预算内的后续置换。margin 是排序偏置，不能当作严格成对替换阈值。热衰减 10 秒、采样 1/64、评价周期 1 秒、升级 2 轮、复制限速 32 MiB/s 均保持相同；复制限速不覆盖所有校验与持久化 I/O。

主矩阵采用前 7 策略 × 均匀读 / 热点切换 × 2 次独立重复，共 28 轮；第二次反转整组执行顺序。每次直接从同一个完整 seed 克隆独立 inode，禁止复用默认策略预置的放置。百万键、16 B key、1 KiB value、无压缩、48 MiB cgroup、swappiness=0、CPU 0–3、64 KiB block cache、2 MiB 备份队列；固定率 112 ops/s。

均匀读预热/固定/饱和 30/45/15 秒；热点切换为 45/120/15 秒，80% 读取访问四分之一键空间，固定段中点切到下一季度。切换后保留 60 秒观察适应。主矩阵硬截止 7200 秒，每轮复制+DB 上限 480 秒，不重试、不缩短失败轮的负载时长，保留全部证据。

## 来源与验证

本端 HEAD `10841d702c901db3aa7ab258131a261e113676ce`；冻结 C/C++/头文件 1429/1429 与当前本端一致，见 [source-check.json](preflight/source-check.json)。远端 Release 二进制 SHA256 `2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76`；源码 manifest SHA256 `3a9e91e86fbd2743d473ef09199b490f1341db7e84a83a3e0535a2a081cceda2`。新实验 Python 源文件单独冻结起止哈希，不用相同 Git HEAD 代替内容核对。

主矩阵 v1 的最终 23 项针对性单测在本端及 node-ssd Python 3.8 均首轮通过，8 workers、每用例 60 秒，无重试；先前 21 项旧版本的测试记录单独保留。审阅发现的已释放 inode 误判、Python helper 身份冻结缺口、极早 OOM 停止检查均已修复后重新测试。现有 cgroup 初始 kmem 残留约 9.45 MiB、总计费约 30.73 MiB（含缓存），并非全新组；未修改系统配置或全局缓存。

[完整启动命令](deployment/launch.json)、[部署文件及哈希](deployment/manifest.json)、[任务与矩阵标识](deployment/task.json)、[本端最终单测](unit/local/run-20261004T152729Z/results.json)、[远端最终单测](unit/node/run-20261004T152922Z/results.json)。

远端任务唯一根目录：SSD/HDD 各自 `metabypass_test/sst-tradeoff-20261004-e864d612`。控制记录在 SSD 任务的 `control/`，数据库进程和复制进入专用 cgroup，监控在组外。

## 完整实测结果

| run | UUID | 设计 | 有效 / 实测 | driver 活跃时间 |
|---|---|---|---:|---:|
| M：主矩阵 v1 | `79d1244c-460e-4f4c-b06f-5f8d8fcbe9e1` | 前 7 策略 × uniform/switch × 2 | 28 / 28 | 99.57 分钟 |
| S：独立补测 v2 | `c770a180-6a52-43c6-aa14-4599bb4ae5fc` | disabled_ssd/conservative90 × uniform/switch × 2 | 8 / 8 | 24.84 分钟 |

两个 run 均 complete、intact。28 轮分层策略均有正式期实际热/冷放置及读取采样覆盖，覆盖不要求每个阶段有新增迁移；其余 8 轮为全 SSD 基线。二进制、manifest、5 个 Python 文件的起止哈希和 seed 最终核验一致，没有失败、未执行轮、OOM 或最终完整性错误。所有 36 轮采样文件均严格对应自身 UUID，解析错误为 0；所有 36 个配对均使用同 run、同负载、同 repeat 的唯一 disabled 基线。原始终态显示进程退出、cgroup 无任务、oom_kill 为 0。

两 driver 活跃时间共 124.41 分钟；两 run 之间部署、单测及下载等约 18.38 分钟。补测自然完成约 17:53:43 UTC，从主启动到退出共 142.78 分钟；之后的终态核查时间不计为运行结束。

[完整逐轮 JSON](analysis/combined/tradeoff-summary.json)、[逐轮 Markdown](analysis/combined/tradeoff-summary.md)、[README 数字与 SHA 证据](analysis/combined/readme-evidence.json)。主报告原件仍在 [analysis/main](analysis/main/tradeoff-summary.md)。工具按每个 run 枚举全部 8 策略 × 两负载 × 两重复，因此列出 28 个 unmeasured：M 有意不测 conservative90 的 4 个组合，S 有意不测其余 6 个分层策略的 24 个组合。这是各 run 设计缺项，实际要求的全部 36 轮已经完成，不能将其读成失败或合计遗漏。mixed 不在本轮请求覆盖集内。

以下 `r1 / r2` 保留独立重复，不合并不同 run 的重复基线。SSD 空间是固定率测量段完整空间扫描的中位值，uniform 每轮 4 次、switch 每轮 12 次；switch 包含过渡，不声称已经收敛。空间按 allocated bytes 计、单位 MiB；节省以实际占用计算，不能用名义预算替代。固定率为 112 ops/s。P99 从累计 Get 直方图重新计算，单位 ms，为桶上界；两次 P99 不平均，范围不是置信区间。表内 conservative90 只与 S 基线配对，其它分层策略只与 M 基线配对。[全部 8 策略观测范围表](analysis/combined/strategy-ranges.md) 另列各轮值的最小/最大，仅供扫描，不是合并 P99 或置信区间。

### 均匀读

| 策略（run） | SSD SST MiB r1 / r2 | SST 节省 % r1 / r2 | SSD 总 MiB r1 / r2 | SSD 总节省 % r1 / r2 |
|---|---:|---:|---:|---:|
| disabled_ssd（M） | 21.64 / 21.64 | 0.00 / 0.00 | 26.15 / 26.15 | 0.00 / 0.00 |
| disabled_ssd（S） | 21.64 / 21.64 | 0.00 / 0.00 | 26.15 / 26.15 | 0.00 / 0.00 |
| default50（M） | 9.50 / 9.50 | 56.09 / 56.09 | 14.01 / 14.01 | 46.41 / 46.41 |
| default65（M） | 12.03 / 12.54 | 44.41 / 42.07 | 16.85 / 17.05 | 35.56 / 34.81 |
| default80（M） | 15.57 / 15.57 | 28.05 / 28.05 | 20.08 / 20.08 | 23.20 / 23.20 |
| default90（M） | 16.59 / 16.59 | 23.37 / 23.37 | 21.09 / 21.09 | 19.35 / 19.33 |
| conservative50（M） | 9.50 / 9.50 | 56.09 / 56.09 | 14.01 / 14.01 | 46.43 / 46.41 |
| conservative65（M） | 12.54 / 12.54 | 42.07 / 42.07 | 17.04 / 17.05 | 34.82 / 34.81 |
| conservative90（S） | 16.39 / 16.69 | 24.29 / 22.87 | 20.89 / 21.20 | 20.09 / 18.92 |

| 策略（run） | 饱和吞吐 ops/s r1 / r2 | 吞吐降幅 % r1 / r2 | 固定率响应 P99 ms r1 / r2 | 固定段迁移次数 r1 / r2 |
|---|---:|---:|---:|---:|
| disabled_ssd（M） | 194.19 / 194.18 | 0.00 / 0.00 | 12.288 / 12.288 | 0 / 0 |
| disabled_ssd（S） | 193.63 / 190.13 | 0.00 / 0.00 | 10.240 / 12.288 | 0 / 0 |
| default50（M） | 176.52 / 149.79 | 9.10 / 22.86 | 20.480 / 163.840 | 25 / 26 |
| default65（M） | 176.48 / 176.68 | 9.12 / 9.01 | 20.480 / 24.576 | 33 / 22 |
| default80（M） | 184.79 / 181.31 | 4.84 / 6.63 | 24.576 / 131.072 | 19 / 32 |
| default90（M） | 189.82 / 176.39 | 2.25 / 9.17 | 28.672 / 114.688 | 21 / 24 |
| conservative50（M） | 170.52 / 170.68 | 12.19 / 12.11 | 20.480 / 98.304 | 8 / 13 |
| conservative65（M） | 172.65 / 176.57 | 11.09 / 9.07 | 24.576 / 196.608 | 11 / 11 |
| conservative90（S） | 176.26 / 180.64 | 8.97 / 4.99 | 131.072 / 24.576 | 12 / 14 |

### 热点切换

| 策略（run） | SSD SST MiB r1 / r2 | SST 节省 % r1 / r2 | SSD 总 MiB r1 / r2 | SSD 总节省 % r1 / r2 |
|---|---:|---:|---:|---:|
| disabled_ssd（M） | 21.64 / 21.64 | 0.00 / 0.00 | 26.15 / 26.15 | 0.00 / 0.00 |
| disabled_ssd（S） | 21.64 / 21.64 | 0.00 / 0.00 | 26.15 / 26.15 | 0.00 / 0.00 |
| default50（M） | 9.50 / 9.50 | 56.09 / 56.09 | 14.01 / 14.01 | 46.43 / 46.41 |
| default65（M） | 12.54 / 12.54 | 42.07 / 42.07 | 17.04 / 17.05 | 34.82 / 34.81 |
| default80（M） | 15.57 / 15.57 | 28.05 / 28.05 | 20.08 / 20.08 | 23.21 / 23.20 |
| default90（M） | 16.59 / 17.20 | 23.37 / 20.54 | 21.09 / 21.70 | 19.35 / 17.00 |
| conservative50（M） | 9.50 / 9.50 | 56.09 / 56.09 | 14.01 / 14.01 | 46.43 / 46.41 |
| conservative65（M） | 12.54 / 12.54 | 42.07 / 42.07 | 17.04 / 17.05 | 34.82 / 34.81 |
| conservative90（S） | 17.20 / 15.57 | 20.54 / 28.05 | 21.71 / 20.08 | 16.99 / 23.20 |

| 策略（run） | 饱和吞吐 ops/s r1 / r2 | 吞吐降幅 % r1 / r2 | 固定率响应 P99 ms r1 / r2 | 固定段迁移次数 r1 / r2 |
|---|---:|---:|---:|---:|
| disabled_ssd（M） | 226.78 / 224.55 | 0.00 / 0.00 | 10.240 / 10.240 | 0 / 0 |
| disabled_ssd（S） | 222.45 / 222.64 | 0.00 / 0.00 | 10.240 / 10.240 | 0 / 0 |
| default50（M） | 201.46 / 203.39 | 11.17 / 9.42 | 20.480 / 20.480 | 44 / 42 |
| default65（M） | 206.29 / 204.46 | 9.03 / 8.95 | 16.384 / 16.384 | 67 / 66 |
| default80（M） | 213.81 / 215.91 | 5.72 / 3.85 | 20.480 / 16.384 | 45 / 42 |
| default90（M） | 210.02 / 210.22 | 7.39 / 6.38 | 57.344 / 20.480 | 28 / 17 |
| conservative50（M） | 205.66 / 202.86 | 9.31 / 9.66 | 20.480 / 20.480 | 18 / 22 |
| conservative65（M） | 210.57 / 206.68 | 7.15 / 7.96 | 20.480 / 163.840 | 24 / 34 |
| conservative90（S） | 209.37 / 213.25 | 5.88 / 4.22 | 20.480 / 49.152 | 22 / 30 |

所有 36 轮固定段 unfinished 均为 0，仍有响应排队延迟。M 中 uniform 基线 late 0–0.179%、分层 1.052–3.095%；switch 基线 0.030–0.074%、分层 0.335–1.868%。S 的 conservative90 uniform late 3.016/1.429%、switch 1.004/1.533%；其同期基线分别为 0/0.060% 和 0.022/0.045%。service 与 response 独立记录：36 轮基线 service P99 为 10.240ms，分层为 14.336–20.480ms。较大的 response P99 还包括等待计划发起时间产生的延迟。conservative90 的固定窗口 response P99 峰值为 uniform 327.680/81.920ms、switch 327.680/327.680ms，不能只看整个阶段的 P99。完整 P50/P95/P99、窗口峰值、late/unfinished、I/O 与复制字节均保留在逐轮报告。

![同期基线配对的 SSD 节省与性能代价](analysis/combined/tradeoff-presentation.png)

[简洁图 SVG](analysis/combined/tradeoff-presentation.svg)、[36 点映射及输入 SHA](analysis/combined/presentation-points.json)。每负载实测 n=18，策略颜色、重复形状，无逐点长标签；两 run 的基线虽会重叠，全部保留。原始逐 run 图也保留：[SSD SST PNG](analysis/combined/tradeoff-ssd-sst.png)、[SVG](analysis/combined/tradeoff-ssd-sst.svg)、[SSD 总占用 PNG](analysis/combined/tradeoff-ssd-total.png)、[SVG](analysis/combined/tradeoff-ssd-total.svg)。原始图标题中的未测数是各 run 设计缺项，含义如上。

### HDD、全盘占用与迁移边界

这套 mini 数据库的 blob 与保护备份主要在 HDD；全 SSD 基线表示当前 SST 全放 SSD，并非全库都在 SSD。两个 run 的闭库基线 HDD 实占均为 1044.238MiB，分层轮均为 1044.246MiB，即比同期基线多 8KiB；不能把 HDD 总量当作实际冷 SST 量，因为热 SST 也有保护备份。基线双盘按 inode 去重总占用 1066.391MiB；M 分层闭库总占用为 uniform 1053.250–1061.344MiB、switch 1054.262–1061.957MiB。S conservative90 为 uniform 1061.344/1060.332MiB、switch 1060.945/1061.344MiB。这些数只统计当前 trial，排除保留的 seed 和其它 trial，不是整台设备可用空间。双盘总节省约 0.4–1.2%，远小于 SSD SST 节省。

每轮初始 clone 的 SSD SST 均为 21.645MiB、SSD 总量 22.098MiB、双盘总量 1066.348MiB。M copy 采样的 SSD 总量最高 22.098MiB，benchmark 采样最高 26.148MiB；这是采样峰值，可能漏掉扫描之间的瞬时峰。部署必须给初始全 SSD clone 留空间，不能只按迁移后的占用预留。固定段 SSD 总量含日志等文件，两个 run 的基线中位均为 26.148MiB，闭库后为 22.152MiB；不同时间口径不混用。

上表迁移是固定测量段相对前一 phase_end 的差分；预热以来的累计迁移和饱和段差分另列。conservative90 的预热累计/固定差分/饱和差分分别为 uniform r1 `11/12/4`、r2 `9/14/5`，switch r1 `17/22/5`、r2 `21/30/6`。它的固定段 copied_bytes 为 uniform 8.483/9.096MiB、switch 11.112/14.547MiB。预热包含初始保护，copied_bytes 也包含保护复制，不等于 promotion/demotion 的字节和。所有 36 轮正式固定段 over_budget 增量均为 0，末值未保护字节、保护队列字节和迁移错误均为 0；M 有 5 轮固定段结束时迁移队列仍有 1–2 个任务，其余为 0，不能将它与保护队列字节混为一谈，也不能把预热超预算累计计入正式段。保守参数减少后续迁移，不能据此推出尾延迟更低。

M 固定段设备级 HDD 读速率：基线 uniform 约 0.548MiB/s、switch 0.534–0.535MiB/s，分层分别为 0.951–1.346MiB/s 和 0.691–1.071MiB/s。S conservative90 为 uniform 0.898/0.903MiB/s、switch 0.750/0.812MiB/s。进程 CPU 约占单核 0.55–0.75%，没有 CPU 饱和证据。设备计数可能包含其它进程、blob 读取、校验及持久化，不将其全部归因于 SST 迁移，也不据此单独断言瓶颈。

### 缓存条件与适用范围

**原定严格条件 `SST 工作集 >= 可用文件 cache × 1.5` 未证实。** SST 逻辑工作集为 21.620MiB，若以 cgroup 全部 file cache 作代理，应要求 cache 不超过 14.414MiB；M 固定段 2198 个样本为 20.109–24.109MiB，严格代理比值仅约 0.90–1.08，达不到 1.5。S 固定段 628 个样本为 20.149–21.379MiB，也没有满足该代理条件。file cache 还包含 blob 和其它文件，不能全算作可用 SST cache，但本次没有足够归因证据重建严格可用量。36 轮 `cache_dominated=False` 也不能证明这项 1.5 条件。

M 的 224 次 mincore 完整采样中 SST 驻留为 39.59–63.78%（中位 51.83%），S 的 64 次为 38.92–57.59%；采样覆盖完整逻辑工作集，且有实际设备读取，说明 48MiB 组内 SST 没有充分驻留。M 总计费实测 47.590–48.000MiB、RSS 16.465–16.844MiB、kmem 9.219–12.680MiB；计费可能重叠，不能相加推导剩余 cache。该证据支持本轮实际受限缓存场景，但不替代未证实的严格 1.5 gate。两个 run 保持数据规模、cgroup 和缓存配置相同。

每种负载每策略只有两次重复，范围不是置信区间。switch 的切换时刻由原始事件计算；切前及切后 0–15/15–30/30–60 秒各段只合并完整落入区间的窗口直方图，跨界窗口排除并记录，未平均窗口 P99。具体边界、实际覆盖时长、排除窗口、桶计数和真实切换时刻均保存在 JSON，不虚构收敛时间。mixed 与 RPO/故障恢复均未测，读性能结果不外推写入负载、SSD 丢盘恢复、更大工作集或严格 1.5 缓存条件。

## 最终选择与条件候选

按用户完成结果审阅后的决定，**当前采用 `default80`，作为启用 SST 分层时的项目推荐默认 profile**。名义预算为固定基准全 SSD SST 逻辑字节的 80%，reserve 保持 10%，有效策略预算约为该基准的 72%。它不是 SSD 硬盘总容量的 80%，也不是热数据比例，库不会随变化的全库大小实时重算。生产 API 使用绝对 `ssd_capacity_bytes`，调用方须固定基准、计算预算并显式启用 Adaptive；库本身的默认 mode 仍是 Disabled。配置示例与重点参数见 [SST 分层文档](../../docs/components/metabypass/sst-tiering.md)。

`disabled_ssd` 保留为同期全 SSD 对照。没有一种分层策略在两个负载的两次重复中同时维持接近同期全 SSD 的吞吐与尾延迟；当前采用 default80 不代表它已被证明为稳定甜点。

**若明确允许约 9% 饱和吞吐损失，并接受约 25ms 固定率响应 P99 目标，可选择 default65 作为空间候选。** 它在 M 四轮响应 P99 为 16.384–24.576ms，SST 节省 42.07–44.41%，SSD 总节省 34.81–35.56%，饱和吞吐下降 8.95–9.12%。这是当前两次重复的实测条件候选，并非未给出 SLO 下的最优结论或长期尾延迟保证。

当前采用的 `default80` 在本轮 SST 节省 28.05%、SSD 总节省约 23.20%，吞吐下降 3.85–6.63%；但 uniform 第二次响应 P99 达 131.072ms，该尾延迟代价仍需纳入使用场景的判断。`default90` 的 uniform 首次吞吐下降 2.25%，第二次却为 9.17% 且响应 P99 为 114.688ms，不能按首重复选优。

独立补测 **conservative90 没有建立更稳定的高预算甜点**：uniform 吞吐下降 8.97/4.99%、响应 P99 131.072/24.576ms；switch 下降 5.88/4.22%、响应 P99 20.480/49.152ms。其 SST 节省 uniform 24.29/22.87%、switch 20.54/28.05%；相对 default65 节省更少却仍有响应峰，相对 default80 也没有同时改善所有负载的实测证据。conservative50/65 同样说明少迁移不等于低尾延迟。

## 重现命令、哈希与测试记录

两次实际完整启动 argv 分别在 [v1 launch.json](deployment/launch.json) 和 [v2 launch2.json](deployment/launch2.json)，包含实际参数与路径；v2 独立使用 tools2、control2、SSD/HDD root2，v1 归档没有被替换。v2 在剩余总预算内实际采用 `--deadline-seconds 1918 --strategies disabled_ssd conservative90 --workloads uniform switch --repeats 2`，其它策略参数/负载时长不变。每轮 effective_configuration 保留实际 budget、reserve 和最终 argv flags。conservative90 的实际名义 budget 为 20,403,622B，有效 budget 为 18,363,260B（约 81%）；参数为 demote_rounds=8、min_residency_ms=30000、replacement_margin=1.0，reserve_percent=10，其余默认。

下载原始文件主 284/284、补测 84/84 的远近 SHA 全部一致，清单见 [主远端](raw-node/main-control-remote-manifest.jsonl)/[本地](raw-node/main-control-local-manifest.jsonl)、[补测远端](raw-node/supplement-control-remote-manifest.jsonl)/[本地](raw-node/supplement-control-local-manifest.jsonl)。汇总实际读入的 290 个文件另逐个保存并复核 SHA，见 JSON 的 input_sha256 和 README 证据文件。

| 文件 | SHA256 |
|---|---|
| [deployment/tools.tar](deployment/tools.tar) | `ea1c369b0741cc1f9036208f6bed7e20889c4d09e17de812e8fde0806c063862` |
| [deployment/tools-v2.tar](deployment/tools-v2.tar) | `62b64b0202582c1dc09006df60ade60282b00da25d80907cabdcf92e7d0cb186` |
| [analysis/summarize_tradeoff.py](analysis/summarize_tradeoff.py) | `194a385f372fed02479c5a4fbe56d18038556fc25bb28991c42f506e6bc73b50` |
| [analysis/plot_tradeoff.py](analysis/plot_tradeoff.py) | `1630543853033deabb29a295b03d45318b59197dcfaaae85a0e491ddc24c360c` |
| [analysis/combined/plot_presentation.py](analysis/combined/plot_presentation.py) | `bd0c368760ac381314ce6d22f590a46a9251c0160d844e7622bfc1bcdeda4bfe` |
| [主 runner](raw-node/node-control-79d1244c-460e-4f4c-b06f-5f8d8fcbe9e1/runner.json) | `958087b403fa486c9ef933d2571c5dbe70935e19203754ec25d8508b1f6d24cb` |
| [补测 runner](raw-node/node-control-c770a180-6a52-43c6-aa14-4599bb4ae5fc/runner.json) | `01f46a52b91e4926128db29a68ae5cd4a413dd195c3e4c3636fa7d3b4012bacb` |

源码包文件清单与 SHA 在 [v1 manifest](deployment/manifest.json) 和 [v2 manifest](deployment/manifest-v2.json)。补测 v2 驱动 24 项针对性单测在 [本端](unit/local/run-20261004T161359Z/results.json) 及 [远端 Python 3.8](unit/node-v2/run-20261004T172008Z/results.json) 均首轮通过；冻结报告工具最终 [20 项单测](unit/local/summary-run-20261004T161729Z/results.json) 首轮通过，均为 8 workers、每用例 60 秒，无重试、源码起止哈希不变。简洁图仅为报告展示加工，36 个点逐一对应 combined JSON，同 run 基线配对核验通过，没有重新运行生产负载。

从仓库根目录重现 combined。下载目录有双层 node-control UUID，使用严格 UUID 索引指向各内层真实目录，防止扁平同名文件混入另一 run；符号链接只组织读取路径，不改原始字节或冻结源码：

```bash
mkdir -p reports/sst-tradeoff-20261004/analysis/combined/control-index
python3 - <<'PY_INDEX'
from pathlib import Path
import os
base = Path('reports/sst-tradeoff-20261004').resolve()
for uid in ('79d1244c-460e-4f4c-b06f-5f8d8fcbe9e1',
            'c770a180-6a52-43c6-aa14-4599bb4ae5fc'):
    name = 'node-control-' + uid
    target = base / 'raw-node' / name / name
    link = base / 'analysis/combined/control-index' / name
    assert target.is_dir()
    if link.is_symlink():
        assert link.resolve() == target
    else:
        link.symlink_to(os.path.relpath(target, link.parent), target_is_directory=True)
PY_INDEX
python3 reports/sst-tradeoff-20261004/analysis/summarize_tradeoff.py \
  reports/sst-tradeoff-20261004/raw-node/node-control-79d1244c-460e-4f4c-b06f-5f8d8fcbe9e1/runner.json \
  reports/sst-tradeoff-20261004/raw-node/node-control-c770a180-6a52-43c6-aa14-4599bb4ae5fc/runner.json \
  --control-dir reports/sst-tradeoff-20261004/analysis/combined/control-index \
  --workloads uniform switch \
  --output-dir reports/sst-tradeoff-20261004/analysis/combined
python3 reports/sst-tradeoff-20261004/analysis/combined/plot_presentation.py \
  --summary reports/sst-tradeoff-20261004/analysis/combined/tradeoff-summary.json \
  --output-dir reports/sst-tradeoff-20261004/analysis/combined
```
