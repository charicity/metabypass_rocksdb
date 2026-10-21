# DirectWrite 三臂比较

本轮使用独立目录和 `metabypass-direct-write-v1` 比较协议，复用
[`native-checkpoint`](../native-checkpoint/driver-contract.md) 的 Release 驱动、
所有权标记、进程身份、cgroup、affinity、telemetry、OPTIONS 归档、校验和 HDD Restore。
本文件为实验协议，实际执行状态和结论见各独立输出目录报告。
下文保留实验协议和命令，不表示正式矩阵已执行完成。
脚本不 SSH，不改系统配置，不覆盖历史 `results/`。

三臂为 `original1s`、`newfalse5s`、`newtrue5s`。
前者必须使用引擎 HEAD `23e242cba` 独立构建 Release 二进制。
原始实验驱动必须保留 pristine 副本及 SHA256；本轮从它派生仅增加同等抽样校验的兼容驱动，
与新版共用 `verification_sample.h`，分别归档适配 patch、helper、派生 driver 与二进制摘要。
原始 driver SHA256 为 `db27d4277de2dac804ef1ae1aa14fd14236378a79400575d682fe2e2226b744b`，
派生 driver 不能继续使用该摘要冒充原文件。
该驱动曾被 gitignore 排除，不能假定 `git archive` 包含它。应从保存的原始源导出取回驱动，
保留驱动 SHA256、引擎完整提交和源导出身份、编译命令及 CMakeCache。
其余两臂使用同一个新版二进制，检查点间隔均为 5000 ms；原版使用原有的 1000 ms 间隔。
`newtrue5s/newfalse5s` 比较同间隔下可用 DirectWrite 配置组合的收益：
`newtrue5s` 使用 DirectWrite 必需的 `allow_concurrent_memtable_write=false`，
`newfalse5s` 对照使用 `allow_concurrent_memtable_write=true`。
因此该比较同时包含写入模式及 memtable 并发配置变化，不能归因为只改变
`enable_blob_direct_write` 一个布尔开关的纯因果效果。
`newtrue5s/original1s` 比较版本与检查点间隔的总体变化，不能单独归因于间隔变化。
镜像间隔始终为 1000 ms。
原版臂不传仅新版支持的引擎配置参数；派生原版驱动与新版均显式接收本轮抽样参数。
历史 native controller 缺省命令仍不传新增参数。

正式与 smoke 矩阵均为 162 样本：三轮 × 插入/覆盖/90% Get、5% Put、5% Delete 混合 × 1/4 线程 ×
无 staging/充足 staging/64 KiB 饱和 staging × 三臂，每臂 54 样本。
充足 staging 在正式实验为 16 GiB，在 smoke 为 16 MiB。
smoke 的所有臂和负载统一使用 8192 keys/ops、1 KiB 值（8 MiB 数据集），不采用 timed 测量。
旧 smoke 的 512 ops 在 mixed 的 5% Put 下不足以覆盖饱和 staging：
`direct-smoke-01` 的原版单线程样本实际仅 25 Put，峰值 25790 B，低于 64 KiB，
因此未发生 spill；prepare 虽曾 spill，但 run 为新进程，计数重置，不能借用 prepare 的覆盖。
该次失败停止并保留。8192 规模在固定三轮 seed、1/4 线程下至少留下 348 个最终新值，
仅 value 已达 356352 B；仍须由各 run 的实际 spill 计数证明机制覆盖。
纠正后须在新的独占控制目录及输出目录完整重跑 162 样本，不复用或覆盖旧结果。
脚本默认仍为 4 GiB 数据集、覆盖/混合 30 秒；该默认不因 smoke 修正或本轮执行参数调整而改动。
各轮轮换三臂次序，配对 seed 相同；共 18 个场景、36 项配对比较。
写入均为 `sync=false`；SST 冷热策略关闭，Blob 整文件 GC 保持开启。
本轮用户已批准正式比较三臂统一使用 8 GiB 数据集、1 KiB 值，覆盖和混合测量 180 秒，
通过显式 `--write-bytes 8589934592 --seconds 180 --verify-samples 4096` 指定。插入一次性写完 8 GiB，
不用 timed insert，避免 key 绕回后变成覆盖；覆盖和混合先准备同等数据。
正式实验中，每个新版本前台必须至少 15 秒且在前台内实际发布至少两个检查点，
否则保留现场、标记 incomplete，扩大 `--write-bytes` 或调查检查点阻塞后用新输出目录重跑。
原版没有前台边界快照，只保留时长门槛及最终原始 recovery_points，不冒称其前台点数已被独立计数。
充足 staging 必须实际占用 `peak_staging_bytes>0` 且 `staging_spill_bytes==0`；
饱和 staging 必须实际 `staging_spill_bytes>0`。仅 formal 且状态为 completed、
预期 162 个样本全部有效且 36 项比较均三轮齐备时允许总体排名；缺失、失败、额外、
重复或参数错配的样本均不能进入总体排名。`results.json` 与汇总中的 `arms` 记录三臂，
新增 `matrix_scope` 记录轮次、负载、线程、staging、样本数量及实际 `--write-bytes`、
`--seconds`，使独立汇总仍按该次完整矩阵校验；采样时另记录 `verify_samples` 和 `verification_protocol`，
按该策略重建预期样本，策略错配不能排名。重复配对轮次直接拒绝汇总；
其他不匹配样本保留在 `omitted` 中，缺失比较标记为不完整。

本轮 verify、Restore 后校验与 append-reopen 均显式采用 `--verify-samples 4096`，
实际数量为 `min(4096, keys)`。驱动 `--verify-samples=0` 仍保留原全量语义，未指定时保持历史命令兼容。
抽样唯一 key 必含 0、末 key、各写入线程分区首尾及分区内存在的 live/deleted 代表；
独立 xorshift64 PRNG 使用 `seed XOR 0xd1b54a32d192ed03`（零值回退到该常量），补齐去重后排序。
只扫描内存 sidecar 选择类别，不做 DB 全库 Get 或全 iterator/keycount 扫描；小数据库即使全 key
入样也不执行 iterator。预算不足以容纳强制集合时拒绝，抽样与 `verify-acks` 同用时拒绝。
被选 key 保留存在/删除、长度、key、版本和 checksum 断言；append 后强制检查 key 0。
每次校验归档 `verification` 事件的 `verification_protocol=metabypass-key-sample-v1`、mode、
population、requested、actual、derived_seed、排序 keys、live/deleted/unwritten 数量及
`full_keycount_checked=false`；`done.verified_keys` 为实际检查数。传输 protocol 仍为
`metabypass-native-perf-v1`。抽样通过不证明未选 key、全库完整性或实际 keycount；
历史全量/ACK/resume 验证语义不放宽。三臂使用同一策略与配对 seed，实际列表逐 action 保留。

生命周期包括前台、最终 Flush、SyncBackup、5 秒后台 GC 观察和 Close；Open 单列，
状态序列化、独立 verify、HDD Restore、append-reopen 位于该生命周期之外。
本轮每个样本均执行抽样 verify，再隔离原 SSD index/staging，仅从 HDD Restore、抽样验证并 append-reopen。
成功后归档日志、OPTIONS、空间和遥测并清理本任务独占数据库容器；失败保留现场。
尾部观察长度固定，不能把 pending GC 非零写成已收敛。
WAL/mirror/checkpoint/staging/GC 等累计 stats 的最后快照位于 Close 之前，不含 Close 收尾字节；
lifecycle、全进程 CPU/RSS 则包含 Close。Close 后只输出 phase 和 resources，不补发 stats。

构建新版驱动可复用原 CMake，无需本轮不参与比较的 upstream 构建：

```bash
cmake -S "$SOURCE/docs/components/metabypass/experiments/native-checkpoint" \
  -B "$CONTROL/current-build" -DROCKSDB_SOURCE="$SOURCE" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=/usr/bin/clang-14 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-14 \
  -DPORTABLE=ON -DUSE_RTTI=OFF -DMB_UPSTREAM=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build "$CONTROL/current-build" --target mb_native -j48
```

仅识别为 OOM 的构建失败允许依次降为 32/16/8，不清理成功对象。
`$SOURCE`、`$CONTROL` 必须指向本任务源导出和新的独占控制目录；不要在运行性能时并发编译。
新版 `build.json` 最少为以下结构（摘要必须来自实际文件）：

```json
{"protocol":"metabypass-native-perf-v1","source":{"export_manifest":"保存路径与摘要"},
 "binaries":{"current":{"path":"/absolute/current-build/mb_native","sha256":"实际摘要"}}}
```

原版 `original.json` 最少为下列结构，并附上原始与派生 driver 的来源/摘要、适配 patch 和 helper、
原版引擎未变及新构建证据：

```json
{"source_head":"23e242cba的完整40位提交","path":"/absolute/original-build/mb_native",
 "sha256":"实际摘要","driver_source_sha256":"本轮派生driver摘要","build_type":"Release"}
```

先由测试代理执行 Python 合约与局部功能 smoke。`$TOOLS` 为实验目录部署副本，
smoke 使用真实 SSD/HDD 父目录，以覆盖跨设备 HDD Restore；父目录须预先建立且属于当前 uid，
输出路径须从未存在。smoke 不强制正式前台时长和点数门槛：

```bash
python3 -m unittest discover -s "$TOOLS/direct-write" -p 'test_*.py'
python3 -m unittest discover -s "$TOOLS/native-checkpoint" -p 'test_*.py'
python3 "$TOOLS/direct-write/compare.py" plan --profile smoke --cpus 0,1,2,3 --verify-samples 4096
python3 "$TOOLS/direct-write/compare.py" run --profile smoke --no-time-limits --cpus 0,1,2,3 --verify-samples 4096 \
  --ssd-parent /home/lj/ssd/metabypass_test --hdd-parent /home/lj/hdd/metabypass_test \
  --build-manifest "$CONTROL/build.json" --original-manifest "$CONTROL/original.json" \
  --output "$CONTROL/direct-smoke-sample4096-01"
```

2026-10-21 当前执行方案：两版新驱动与控制脚本完成独立检查后，导出新源码快照并在新独占控制目录
以 Clang 14、Release、`-j48` 分别构建，再运行完整 162 项采样 smoke、独立六项 8 GiB/180 秒
采样资格检查；全部通过后才启动新输出目录的完整正式矩阵。
此前 `direct-formal-01` 在 13 个有效样本后，`newfalse5s` 的 insert/4t/ample 前台约 40 秒
仅发布 1 个检查点，因原门槛失败停止；随后 `direct-qualification-01` 的 8 GiB/30 秒
首项 overwrite/4t/ample/newfalse5s 前台内发布 0 个点，也已停止。保留失败目录、日志及结果，
不能把后续重跑写成原样本通过，不能混合不同规模、时长或验证策略的样本。
用户随后取消仍使用全量校验的 `direct-qualification-02`，要求改为抽样；旧现场与取消记录也保留。
此前全量版 `qualify180.py`、`formal180.py` 已作废，不能用于本轮采样实验。

新的独立采样资格 harness（由执行方单独归档路径与 SHA256）使用新冻结源中的 Experiment/sample，
依次检查 overwrite false/true、mixed false/true、insert false/true 六项，均为 round 1、4 线程、
16 GiB ample staging；覆盖/混合 180 秒，插入不定时。原有机制门槛、抽样 verify、HDD Restore、
append-reopen、清理及 1800 秒进展停滞 watchdog 不变。任一失败即停止保留现场。
资格检查成功状态为 `qualification_completed`，仍为诊断，不允许总体排名；其中 `matrix.json`
仅为 162 项参考，实际六项见 `qualification-plan.json`。仅六项全部有效通过后，
才可在另一个从未存在的输出目录完整执行三臂 162 样本。

以下命令仅由获准的执行方在 `lj@node-ssd` 运行，复用现有空闲 8 GiB cgroup，
默认 SSD/HDD 父路径为 `/home/lj/{ssd,hdd}/metabypass_test`：

```bash
python3 "$QUALIFY_SAMPLE" "$CONTROL" direct-qualification-sample4096-01
```

`$QUALIFY_SAMPLE` 指向新准备并独立验收的采样 harness，固定使用 4096 抽样策略；
其实际路径与参数须以归档命令为准。它不是旧 `qualify180.py`，不得复用旧全量 wrapper。
启动时核对新冻结源及两版新二进制的摘要，不修改运行中的 engine、driver 或 compare.py。
先核对资格检查的六项状态及 `qualification_completed`，全部通过后才运行下列正式命令：

```bash
python3 "$TOOLS/direct-write/compare.py" preflight --profile formal --cpus 0,1,2,3 \
  --write-bytes 8589934592 --seconds 180 --verify-samples 4096
python3 "$TOOLS/direct-write/compare.py" plan --profile formal --cpus 0,1,2,3 \
  --write-bytes 8589934592 --seconds 180 --verify-samples 4096 > "$CONTROL/direct-plan-8g180s-sample4096.json"
python3 "$TOOLS/direct-write/compare.py" run --profile formal --no-time-limits --cpus 0,1,2,3 \
  --write-bytes 8589934592 --seconds 180 --verify-samples 4096 \
  --build-manifest "$CONTROL/build.json" --original-manifest "$CONTROL/original.json" \
  --output "$CONTROL/direct-formal-sample4096-01"
python3 "$TOOLS/direct-write/compare.py" summarize --cpus 0,1,2,3 \
  --write-bytes 8589934592 --seconds 180 --verify-samples 4096 --output "$CONTROL/direct-formal-sample4096-01"
```

`--no-time-limits` 取消总/阶段时限，保留原 controller 的进展停滞 watchdog、磁盘余量、
目录归属和 cgroup 独占约束。162 样本包含准备、Open、抽样校验及 HDD Restore，
预计需要多小时；此前 4 GiB/30 秒方案的 12–24 小时粗估不适用于本轮 8 GiB/180 秒方案，
须用资格检查和首轮实际耗时重新估计。
smoke 不设置正式前台时间/点数门槛，不发布设备性能结论。

每样本原始输出包含 WAL/mirror bytes、flush/compaction、延迟直方图、CPU/RSS、
设备 I/O、页缓存驻留、staging/queue wait、迁移和 GC，以及 checkpoint capture/flush/persist/publish 时间。
统计是累计快照，汇总取 Close 前最后值；前台点数用边界差值。
`checkpoint_flush_micros` 含原生 flush 和文件枚举，`checkpoint_publish_micros` 含 persist/校验，
这些时间存在包含关系，不能相加。设备字节覆盖整个进程及其他设备用户，不能称为引擎写放大；
当前没有单独的前台 CPU、逐阶段设备字节或 perf 硬件计数器。报告按
[模板](report-template.zh-CN.md) 明确这些限制。
