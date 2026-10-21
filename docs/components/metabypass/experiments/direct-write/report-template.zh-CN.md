# DirectWrite 对比报告（待实测）

本文件为待填报告模板，实际执行状态和结论见各独立输出目录报告。
本文不含正式性能结果，不应作为性能结论引用。

填写二进制 SHA256、原版引擎完整提交、原始驱动来源/摘要、新版源导出摘要、Release 编译命令、
CPU affinity、内存 cgroup、设备、文件系统、空闲容量、后台负载、运行命令和原始日志路径。

当前批准执行参数为三臂统一 `--write-bytes 8589934592 --seconds 180 --verify-samples 4096`：8 GiB 数据集，
覆盖/混合各 180 秒，插入一次性写完、不定时；原 CLI 的 4 GiB/30 秒及 full（verify-samples=0）默认不变。
先单独记录新独立采样 harness 的六项资格检查（overwrite false/true、mixed false/true、insert false/true，
均 round 1、4t、16 GiB ample staging），六项全部通过后才能在新输出目录完整运行 162 样本。
资格检查状态为 `qualification_completed`，属于诊断且禁止总体排名；不与正式样本合并。
旧全量版 `qualify180.py`/`formal180.py` wrapper 已作废；新源码冻结、两版新构建与完整 162 项采样 smoke
通过后才做资格检查。新采样 wrapper 的路径和命令须单独验收与归档，不能复用旧全量入口。附资格检查 plan/结果/harness SHA256，另列此前 `direct-formal-01` 和 8 GiB/30 秒
`direct-qualification-01` 因前台发布点不足停止的证据，以及用户取消旧全量资格检查02的记录，不改写或覆盖历史失败。

矩阵为三轮 × 三负载 × 两线程数 × 三 staging × 三臂，共 162 样本，
每臂 54 样本；报告 `arms` 与 `matrix_scope`，核对 18 个场景的 36 项比较均三轮齐备。
仅 formal completed 且预期样本全部有效时报告总体排名；缺失、失败、额外、重复或错配时禁止排名。

| 场景/线程/staging | 原版1s | 新版false5s | 新版true5s |
|---|---|---|---|
| 三轮完整数量与机制覆盖 | 待填 | 待填 | 待填 |
| 前台 ops/s；P50/P95/P99 | 待填 | 待填 | 待填 |
| 完整生命周期；Open | 待填 | 待填 | 待填 |
| WAL/mirror WAL/metadata 字节 | 待填 | 待填 | 待填 |
| flush/compaction 次数与字节 | 待填 | 待填 | 待填 |
| checkpoint capture/flush/persist/publish | 待填 | 待填 | 待填 |
| staging 峰值/spill/等待；queue 峰值/等待 | 待填 | 待填 | 待填 |
| GC pending/reclaimed/耗时 | 待填 | 待填 | 待填 |
| 全进程 CPU/RSS、设备读写字节 | 待填 | 待填 | 待填 |

每个场景分别报告三轮绝对值中位数及 min/max、同轮 candidate/reference 比值，
不混合不同场景，不平均直方图百分位，不把三轮筛查写成统计显著性。
仅报告 `newtrue5s/newfalse5s` 同间隔下可用 DirectWrite 配置组合的收益，
以及 `newtrue5s/original1s` 真实原版至新版的总体变化；后者包含版本与检查点间隔差异，
不能单独归因为间隔变化。
`newtrue5s/newfalse5s` 中，前者使用 DirectWrite 必需的
`allow_concurrent_memtable_write=false`，后者使用 `allow_concurrent_memtable_write=true`；
必须同时列出这些配置差异，不能把结果归因为只改变 `enable_blob_direct_write`
一个布尔开关的纯因果效果。
列出缺失/失败/不完整样本及保留现场；不以重跑覆盖原失败。

恢复验证分别报告已发布点 Restore、最终 SyncBackup 覆盖、append-reopen、同步写功能回归。
本轮 verify/Restore/append 校验只检查 min(4096, keys) 个唯一 key，无全 iterator/keycount 扫描；
报告每 action 的抽样协议、派生 seed、实际 keylist、实际数量及 live/deleted/unwritten 分类数，
不能把抽样通过写成全库完整性或实际 keycount 已验证。保留 pristine 原 driver、derived 适配 patch、
共同 helper 及两版新二进制的 SHA256，原版引擎仍为23e242cba。
同步写仅验证语义，不作为本轮优化目标；不宣称支持 active Blob WAL 的本地 crash 重开。
后台 GC 未收敛时列出最后 pending 数值；CPU 和设备 I/O 的计量边界见 README。
附 `results.json`、`matrix.json`、`direct-write-summary.json`、所有 OPTIONS 及源/构建证据。
