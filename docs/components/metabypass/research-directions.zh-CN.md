# Metabypass：相关论文、设计补强与创新定位

分析日期：2026-09-26。代码基线：`758fbd140`。本文是论文与源码的静态分析，没有重新运行测试或性能实验；建议中的机制不表示已经实现。

## 1. 已确认的研究边界

用户明确保留两个前提：SSD 保存索引，HDD 保存主体数据；SSD 可能永久丢失全部内容。正常路径采用尽力一致性，避免每次普通写入等待索引备份完成。

用户进一步确认：**故障后恢复到已发布的完整恢复点，允许丢失之后的写入。** 因而，本研究不以追回恢复点之后残留在 HDD 上的更新为目标。

推荐将这一合同写成：正常读写遵守支持范围内的 RocksDB 语义；普通写完成与跨 SSD 故障的恢复保证分开；发布恢复点时，其原生索引状态和 HDD 数据依赖必须共同可恢复。允许回退的边界是整个合法恢复状态，不能随意漏掉某个已受保护的 key、删除或半个 WriteBatch。

这不要求把所有写入改为同步复制，也不要求引入一致性协议或在线热副本。当前 `sync=true` 和 `SyncBackup()` 是应用主动要求更强保证的入口，应与主要研究负载中的 `sync=false` 分开测量。源码中的 `Options::best_efforts_recovery` 则是另一项 RocksDB 功能，当前包装器明确拒绝它，不能把二者混称。

`paper_ref/` 当前包含解读报告和横向比较文档，并非论文 PDF。本文以这些材料筛选相关工作，再用可访问的作者、会议或研究机构原文核对关键机制。新颖性判断限于这些对照，未完成全领域查新。

## 2. 哪些论文最值得对照

下表区分论文已有机制和对本项目的迁移判断。“不能直接照搬”表示前提不同，不表示原论文有错误。

| 论文 | 与本项目有关的已有机制 | 可借鉴之处与边界 |
|---|---|---|
| [SMORE，MSST 2017 扩展版](https://arxiv.org/pdf/1705.09701) | flash 工作索引、SMR 数据、磁盘索引快照及布局记录；明确覆盖 flash 设备失效。扩展版建立一致索引副本时短暂停止修改。 | 最接近的介质与故障模型先例。不能把“SSD 索引丢失后由 HDD 恢复”本身称为首创。其数据流记录补齐方式不必成为我们的尾部追回机制。 |
| [CPR，SIGMOD 2019](https://www.microsoft.com/en-us/research/wp-content/uploads/2019/01/cpr-sigmod19.pdf)；[DPR，SIGMOD 2021](https://www.microsoft.com/en-us/research/wp-content/uploads/2021/06/dpr-sigmod2021.pdf) | 区分操作完成与异步提交，以合法前缀描述恢复保证；DPR 进一步处理跨分片依赖。 | 借鉴明确的完成、提交、恢复边界。当前单 CF 有序写无需照搬分布式依赖协议，也不能只用“尽力”代替恢复语义。 |
| [Aceso，SOSP 2024](https://pfzuo.github.io/images/sosp24-Aceso.pdf) | 异步差分索引检查点；Slot Version 判断更新先后，Index Version 缩小扫描集合。 | 借鉴把正确性版本与跳过无关工作的摘要分开。其 RDMA/内存恢复和尾部补齐不是本项目的现成磁盘协议；对会被 compaction 重排的 SST，字节级 XOR 差分未必有效。 |
| [Haystack，OSDI 2010](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf)；[WiscKey，FAST 2016](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) | 异步索引、自描述数据、KV 分离及数据生命周期管理。WiscKey 的尾扫入口保存在 LSM 中。 | 可重建索引与 KV 分离都有先例。幸存 value 含 key，也不自动意味着删除、提交顺序和批次边界可恢复。 |
| [DataLinks，SIGMOD 2002](https://research.ibm.com/publications/coordinating-backuprecovery-and-data-consistency-between-database-and-file-systems) | 协调数据库备份恢复与数据库外部文件的一致性。 | 借鉴共同恢复状态的思路。我们的具体扩展是让已发布 LSM 恢复点参与 blob 的保留和退休判断。 |
| [DEPART，FAST 2022](https://www.usenix.org/system/files/fast22-zhang-qiang.pdf) | 在线主副本和冗余副本使用不同的数据组织，降低冗余副本的排序维护成本。 | 恢复表示不必支付全部在线查询优化成本。它保留完整 KV 冗余，不能据此宣称只保护 metadata 已经解决相同问题。 |
| [Instant Restore，2017 作者版](https://arxiv.org/pdf/1702.08042) | 利用可定位的备份与归档日志，按需恢复段并在后台完成其余恢复。 | 借鉴区分首次可服务时间与全部完成时间。它依赖幸存日志及备份；把任务拆成小段并不能自动证明局部索引完整。 |
| [SILK，ATC 2019](https://www.usenix.org/system/files/atc19-balmau.pdf)；[ADOC，FAST 2023](https://www.usenix.org/system/files/fast23-yu.pdf) | 分别从 I/O 干扰和组件数据流积压出发，调节后台任务资源。 | 借鉴反馈控制，但调度目标需加入恢复点滞后和历史数据占用。SILK 报告的实验关闭了 commit logging，不能直接套用其性能倍数。 |
| [All File Systems Are Not Created Equal，OSDI 2014](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai)；[CrashMonkey/B3，OSDI 2018](https://www.usenix.org/conference/osdi18/presentation/mohan) | 分别揭示应用对持久化顺序的隐含依赖，并以有界操作序列测试文件系统崩溃一致性。 | 借鉴围绕持久发布、重命名、删除和重试的故障矩阵。进程 SIGKILL 与设备掉电应分别验证。 |

建议重点阅读顺序为 SMORE、CPR/DPR、WiscKey/DataLinks、Aceso，然后按要解决的性能问题阅读 DEPART、SILK/ADOC 和 Instant Restore。纠删码、远端内存和多节点修复论文可作边界参照，暂不需要把它们的系统架构纳入主线。

## 3. 当前实现已经具备的基础

| 当前事实 | 代码或文档证据 | 对研究定位的影响 |
|---|---|---|
| 主配置直接写 HDD blob，索引文件异步镜像到 HDD；SSD staging 为可选模式 | [direct-storage.md](/home/ceph-lj/metabypass_rocksdb/docs/components/metabypass/direct-storage.md:1) | 主实验应围绕直接写 HDD，不能用 SSD 暂存的前台收益代替核心方案收益。 |
| 普通写不等待完整恢复点；同步写在主写完成后等待发布 | [MetaBypassDB::Write](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/metabypass_db.cc:368) | 与已确认的尽力一致性目标相容。备份屏障失败可能发生在主写已经可见之后，错误不等于回滚。 |
| 镜像按事件前缀捕获候选点，独立验证线程解析原生依赖、同步 blob 后发布 | [Capture/Publish](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/backup.cc:407) | 已经有完整恢复点的实现基础，并非仅仅复制若干文件。 |
| 发布包含完整 MANIFEST 编辑组、完整 WAL 记录、SST 和 blob 依赖及校验；保留最新与前一恢复点 | [Publish](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/backup.cc:445)；[Inspect](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/native_files.cc:149) | 应保留这些安全条件，优化其成本和可验证性。 |
| 工作镜像不是授权恢复输入，离线恢复只选已发布点 | [RestoreFiles](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/backup.cc:826) | 恢复边界清楚；不用把未完成镜像或 blob 尾部拼成新状态。 |
| blob GC 被禁用，blob 删除被忽略 | [选项配置](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/metabypass_db.cc:224)；[DeleteFile](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/separated_storage.cc:71) | 以永久保留换取简单恢复。长期空间成本尚未解决。 |
| 已有文件身份、增量校验、候选固定、错误唤醒、恢复重试与故障注入测试 | [pipeline-validation.md](/home/ceph-lj/metabypass_rocksdb/docs/components/metabypass/pipeline-validation.md:1)；[metabypass_test.cc](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/metabypass_test.cc:328) | 这些是现有成果，不能作为“尚未添加的功能”重新建议。 |

HDD 上实际上保存了完整的原生索引恢复副本。准确的低成本主张是“不复制 blob payload，不再运行一套独立服务查询和执行 compaction 的在线数据库”，而非“不保留完整索引副本”。镜像仍会复制主库产生的 compaction 输出。

## 4. 优先完善的六个问题

### 4.1 把恢复合同从实现约定提升为可检查的不变量

当前文件队列的 `accepted_ / applied_ / published_` 是文件操作进度，不是用户 WriteBatch 的逻辑序号。解析完整 WAL 记录、验证 CRC 和找到所有 blob，是重要条件；论文中还应解释它们与合法逻辑恢复状态之间的对应关系。

建议定义如下不变量，并为每条配置可重复的反例测试：

1. 每个已发布点对应支持范围内的一个合法 RocksDB 状态，WriteBatch 要么完整出现，要么完整不出现；并发历史以引擎规定的顺序解释，不能简单按客户端回包时间排序。
2. 已发布点引用的 SST、WAL、MANIFEST、blob 前缀及必要的身份信息都位于 SSD 故障域之外。
3. 恢复不吸收选定恢复点之外的 blob 记录；物理存在不等于逻辑可见。
4. 任一仍支持的恢复点，不会因为后续 compaction、GC 或恢复重试而失去依据。
5. `SyncBackup()` 成功所承诺的写入，在其覆盖的恢复点及后续合法恢复点中受到保护。校验失败必须显式报告，不能以更旧点悄悄抵消已经给出的保证。

需要特别区分删除的两种情况。假设恢复点保存 `k=A`，随后一个未受保护的 Delete 成功返回：故障后回到 `k=A` 是允许的回退。如果 Delete 已进入选定恢复点，恢复后又出现 `A`，才是错误复活。这一区分应直接进入测试 oracle。

可以增加只在后台发布时持久化的恢复点说明，记录数据库身份、恢复世代、可解释的逻辑边界和构建统计，并与 inventory 绑定。不要把“扫描到的最大 sequence”未经证明就当成无缺口的前缀证明。应用若需要查询进度，可暴露恢复 token；这些能力不必增加每次普通写的 HDD 同步。

### 4.2 优先消除隐藏的 HDD 校验读取放大

这是当前实现中最具体、也最容易被小数据测试掩盖的问题。

`ValidateTable()` 遍历新 SST，并逐条调用 `References::PutBlobIndexCF()`；后者读取 blob 中的 key 和完整 value 校验 CRC。现有缓存可复用未变化的 SST，但新 compaction 输出是新 SST，仍会再次检查其引用的 value，即使那些 value 早已由 WAL 或旧 SST 校验过。见 [引用检查](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/separated_storage.cc:105)、[SST 校验](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/separated_storage.cc:312)。

此外，`RestoreFiles()` 对 inventory 的 blob 前缀计算摘要；随后 `PrepareRecovery()` 先整体扫描验证，再逐文件扫描准备封口，正常 `Open()` 还会调用恢复准备。因此，当前恢复路径有多次读取相关 blob 的可能，实际数量取决于文件状态和调用阶段。源码证据见 [inventory 摘要](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/backup.cc:904)、[恢复准备](/home/ceph-lj/metabypass_rocksdb/utilities/metabypass/separated_storage.cc:447)。

**不能将“无需扫描所有 value 来重建索引”写成“恢复不需读取大量 value”。** 当前依赖还保守包含 MANIFEST 中的历史 blob additions，不仅是最新用户值。读取放大属于静态可见的工作量，真实 HDD 上的严重程度仍需测量。

建议分三步推进：

- 先增加完整 I/O 账本，分别统计 WAL 引用检查、新 SST 引用检查、blob 前缀验证、恢复校验的读字节和调用次数。现有 `validated_blob_bytes` 明确排除了引用检查，不能当作总读量。
- 将“已验证的不可变 blob 记录”作为可复用单元，让同一记录被 WAL、flush SST 和多个 compaction SST 引用时复用证据。缓存键至少绑定数据库/文件身份、位置、长度和被验证的 key/记录信息；必须处理世代变化、恢复封口和未来 GC。不能只按文件号缓存，也不能把 CRC 相等当作持久完成的证明。
- 在当前离线、独占恢复范围内复用扫描得到的边界、记录数和封口状态，研究合并重复读取；仍然先验证全部必需输入，再执行会改变数据的准备工作。持久化验证摘要与后台介质巡检是后续优化，不应通过直接删掉校验制造性能优势。

这个方向比增加队列线程更贴近主线：它减少保护相同恢复点所需的工作，不改变允许丢失的后缀。

### 4.3 让恢复点参与 GC，而不是永久保留数据

最小反例是：恢复点 `C0` 引用 `k=A@a`；主库已更新为 `k=B@b`，但新恢复点尚未发布；如果 GC 只查当前索引而回收地址 `a`，SSD 丢失后 `C0` 就无法恢复。即使 `B` 的 payload 幸存，也不能据此擅自改变恢复点。

这正是从 [DataLinks 的跨系统恢复协调](https://research.ibm.com/publications/coordinating-backuprecovery-and-data-consistency-between-database-and-file-systems) 和 [WiscKey 的 GC 持久化顺序](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) 向本项目迁移时需要解决的额外责任。空间占用问题也应参考 [HashKV 对更新密集负载的研究](https://www.usenix.org/conference/atc18/presentation/chan)，但其分组布局本身不能替代恢复点保留规则。

建议先实现保守、可验证的文件级或 segment 级回收。保留根应覆盖当前在线版本、仍使用旧位置的读者、最新及前一已发布点、正在构建的候选点，以及尚需兑现的 WAL/写入依赖。只有所有保留根都不再需要的数据，才可回收。

这不能通过直接打开 RocksDB 的 blob GC 选项完成。当前 `Inspect/Dependencies` 保守累积 MANIFEST 中的 blob additions，需一并定义历史依赖退出规则；验证缓存的身份与生命周期也要适配回收，否则空间或内存仍可能随历史增长。

如果 GC 搬迁仍被旧点引用的记录，可先保留旧位置；更进一步才考虑让旧点通过持久、受保护的间接映射访问新位置。后者会引入格式和寻址成本，不应一开始就做复杂。退出一个恢复点的保留集合需要先持久化，再删除其独占依赖。

这里还有活性问题：如果磁盘满了，发布新点需要空间，回收旧数据又必须等待新点，系统会互相等待。需要为新恢复点、GC 输出和发布元数据保留应急空间，并明确不足时的准入行为。永久保留规避了回收正确性，却不能支撑长期低成本结论。

### 4.4 用发布滞后和保留空间调度后台工作

`interval_ms=1000` 只是触发条件，不等于最多丢失一秒数据。队列可以已经排空，验证仍未结束；也可能候选复制阻塞镜像，导致队列积压。`last_point_lag_micros` 是最后一个点的统计，不是持续观测的 RPO 上界。

建议监控当前最老未受保护写入的年龄、未覆盖写入量、候选构建阶段、需要完成的校验 I/O，以及为了旧恢复点保留的字节。发布不前进时，即便排队字节很少，也应能识别保护进度停滞。

可借鉴 [SILK](https://www.usenix.org/conference/atc19/presentation/balmau) 的任务干扰控制与 [ADOC](https://www.usenix.org/system/files/fast23-yu.pdf) 的数据流调节，设计根据前台延迟、发布滞后和剩余空间反馈的调度器。针对 HDD，除了 MB/s 还要控制 IOPS、队列深度和单次不可抢占 I/O 的长度。前台保护与后台最低进展必须同时考虑。

当前队列依赖全局事件前缀，不能简单让 WAL 任意越过 MANIFEST/删除等操作。仓库自己的 [调度实验](/home/ceph-lj/metabypass_rocksdb/docs/components/metabypass/scheduling-ab.md:1) 已表明固定 WAL 预留额度和优先级未稳定优于分锁方案；应先定位瓶颈，再决定调度机制。

对于持续超过设备承载能力的负载，有限内存、有限空间、不背压和有界滞后不能同时维持。保留尽力一致性意味着可以报告更大滞后并按既定规则背压，不意味着需要给所有普通写增加同步屏障，也不意味着可以悄悄宣称固定 RPO。

### 4.5 故障验证要跨完整生命周期

现有 SIGKILL、目录删除、校验失败和恢复重试测试很有价值。下一步应借鉴 [B3/CrashMonkey](https://www.usenix.org/conference/osdi18/presentation/mohan) 的有界序列思想，增加以下组合：

| 场景 | 必须检查的结果 |
|---|---|
| Put、覆盖、Delete、跨 key WriteBatch 与 flush/compaction 交错，随机失去 SSD | 恢复等价于选定已发布点；受保护批次不撕裂，删除不错误复活。 |
| blob 或索引文件同步、候选目录同步、LATEST 替换及父目录同步之间发生故障 | 只能选择完整合法点，不能引用未持久候选。 |
| GC 复制、映射发布、旧点退休和旧数据删除之间发生故障 | 每个仍支持的点都有有效引用；数据责任交接没有空隙。 |
| SSD 丢失之后，恢复准备期间再次中断，再次 Restore | 重试仍可完成，已发布源保持可用，文件号不与幸存孤儿文件冲突。 |
| 慢 HDD、验证停顿、ENOSPC/I/O error 与并发写 | 有效点不被错误替换；等待者和错误状态有明确行为。 |

进程退出后 OS 缓存仍可能存在，所以 SIGKILL 不能替代掉电测试。模拟全 SSD 丢失时，也应确保恢复进程无法借助旧文件描述符、幸存旧目录或其他 SSD 路径读到原索引。测试 oracle 可以独立保存操作历史，但只能在恢复结束后用于检查，不能成为恢复输入。

新测试还需覆盖故障发生在最后一次 `SyncBackup()` 之前的异步运行期。只在完整同步和正常 Close 后删索引，主要证明已经同步状态可恢复，不能刻画真实异步损失窗口。

### 4.6 先降低离线恢复工作量，再考虑提前开放服务

当前 Restore 是独占、离线恢复。最快的第一步是减少重复校验和不必要复制，不必立即加入在线写入、范围恢复、后台重建的整套并发协议。

如果实测 RTO 仍是瓶颈，可以借鉴 [Instant Restore](https://arxiv.org/pdf/1702.08042)，从 HDD 的已发布完整点启动受控读取，按需恢复索引单元，并把其余恢复放到后台。该方向仍选择同一个旧恢复点，不包含尾部追回。

需要分别解决：根目录及索引依赖如何先可用；记录在返回前如何验证；未知范围如何区分“尚未恢复”与 `NotFound`；范围扫描怎样保证覆盖完整。若之后支持恢复期间写入，还要处理新写与旧点的覆盖顺序，以及第二次失败。不能把“跳过启动全量校验”当作零代价加速：错误发现时机和对外承诺将随之变化。

## 5. 值得形成的创新与不应独立主张的创新

### 5.1 已有实现可以支撑的项目特色

最有价值的现有组合是：**在原生 RocksDB 文件语义上，异步捕获、验证并发布一个能够独立承受整个 SSD 索引故障域丢失的恢复点；恢复主体 value 留在 HDD，普通写不逐次等待备份。**

这一组合的具体技术内容包括：用完整 MANIFEST 组、WAL 批次及 SST 引用构造恢复依赖；候选固定后与镜像推进并发；不为发布强制 memtable flush；保留原生恢复格式，不重放自定义的持久文件操作日志；恢复时处理未登记 blob、封口和文件号高水位。

它具备清楚的系统特色，但“已有原型”不等于“已经证明学术独创性”。当前恢复格式、生命周期限制和 I/O 成本都必须纳入描述。可选 SSD staging 不作为核心创新的必要条件。

### 5.2 最推荐继续投入的三个贡献方向

| 方向 | 真正需要解决的新问题 | 当前状态 | 必须拿出的证据 |
|---|---|---|---|
| 原生 LSM 恢复点的低成本依赖验证 | 在不逐写同步的前提下，让验证成本跟新数据和真实依赖变化相关，避免每次 compaction 重读旧 value | 已有文件级增量基础；跨 WAL/SST 的记录级复用和恢复重复扫描优化尚未完成 | 合法恢复点论证；验证 I/O 放大；真实 HDD 上的前台延迟和发布滞后；破坏身份/长度等反例。 |
| 已发布恢复点约束的 GC | 同时让当前状态、旧恢复点和候选点可用，并在有限空间下完成保留责任交接 | 尚未实现；现在永久保留 | 覆盖/删除长期稳态；GC 与发布各边界故障；空间占用随更新趋稳；恢复不失去旧地址。 |
| 由恢复进度与空间共同驱动的后台调度 | 在前台延迟、可恢复写入滞后和历史数据占用之间做可观测取舍 | 有界队列与统计已具备，联合控制尚未实现 | 突发及持续负载；每个时间窗口的 p99、发布年龄、队列和空间曲线；与固定周期/静态限速公平比较。 |

其中前两项最贴近当前代码，也更适合先形成论文主线。第三项应建立在完整成本观测上；如果实验没有显著、稳定收益，就保留为工程机制，不勉强包装成独立贡献。

可采用这样的研究问题表述：

> 在允许故障后回退到已发布恢复点的 SSD/HDD LSM 中，如何将索引保护移出普通写入的同步路径，同时以较低的验证 I/O 和有界的数据保留成本，持续提供可验证的整 SSD 丢失恢复能力？

这里“有界的数据保留成本”是下一阶段目标，不是当前实现已经满足的性质。也不应在未给出资源条件和控制策略之前承诺有界 RPO/RTO。

### 5.3 可借鉴但暂不宜变成主线的机制

DEPART 启发了另一条路线：备份保存偏向恢复的逻辑索引基线与增量，不必完整追随主库 SST 的物理重排。这可能减少镜像 compaction 字节，但会增加恢复构建工作，并放弃部分直接使用原生索引的便利。只有测出镜像 SST 写入确实是主要瓶颈时，才值得作为替代设计实验；它不是当前实现已经具备的能力。

Aceso 的版本划分、SMORE 的段摘要也可用于标识依赖、降低验证或目录扫描成本。因为本项目已经选择恢复到发布点，所以没有必要为了模仿其恢复流程，给所有 HDD value 加上用于追回尾部的操作类型、序号和批次提交日志。未来若改变恢复目标，才需要重新评估这些信息的成本。

以下概念都有明确先例，不宜单独声称原创：KV 分离、SSD/HDD 混合、异步 checkpoint、索引可重建、允许回退的一致恢复、流水线、多队列、差异化保护和按需恢复。独特性需要落在上表的具体问题、机制、正确性边界与实测结果上。

## 6. 让性能与创新结论成立的实验设计

### 基线与公平性

- 相同 Blob Direct Write 和 HDD 布局，但关闭备份：用于估计保护服务的额外成本，不能当作同等可靠性的竞争方案。
- 在 HDD 上同步保护足够恢复的索引日志/元数据，并允许合理批提交：作为更强同步保护的性能对照。不要故意每写一条就构造整个恢复点，再把其高成本当作所有强保护方案的必要代价。
- 简单、正确的周期性完整索引备份：固定必要 WAL 与 blob 依赖，作为同样允许回退的对照。比较时看实际发布滞后，不只匹配定时器参数。
- 当前 Metabypass、记录验证复用版本、恢复点感知 GC 版本、联合调度版本：逐项消融，不同时改变故障模型、队列上限和恢复保证。

相关论文首先是机制与新颖性对照。SMORE 的大对象/SMR 负载、Aceso 的 RDMA 平台、DEPART 的完整副本配置与本项目不同，不应直接拿论文峰值数字横比吞吐。

### 负载、设备和运行时间

主实验使用真正独立的 SSD 和 HDD，保持 `staging_dir` 为空，记录文件系统、内存预算、设备缓存与同步配置。预填充超过主要缓存容量的数据集，再测稳态覆盖、删除、混合读写、范围扫描与 compaction；改变 value 大小、更新热点、队列大小和发布间隔。冷、热缓存结果分别报告。

HDD 上的扫描和小随机 value 读取尤其重要：WiscKey 利用的是 SSD 的并行性，不能假定同样的读取方法在 HDD 上免费成立。可实验按 blob/offset 合并读取和受限预取，但必须观察点查延迟、内存使用和吞吐之间的取舍。

运行时间应长到能观察多轮 compaction、发布与 GC，而不是在后台债务出现前结束。GC 尚未实现的版本只能报告随时间增长的空间曲线，不能给出稳态空间优势。

### 结果至少包括以下维度

| 维度 | 需要的观测 |
|---|---|
| 正常路径 | 吞吐、包含排队的请求延迟、每个时间窗口的 p99/p99.9、CPU 与内存。 |
| 完成保护的成本 | 前台时间、最终 SyncBackup、Close 分开；总时间逐轮计算。 |
| 尽力一致性的实际代价 | 持续的发布年龄、未保护写入量，随机故障后的实际回退范围；不能用关闭后的单个 lag 样本替代。 |
| I/O | 分设备读写、引用校验、候选复制、fsync 次数、镜像 compaction 字节、GC 搬迁；应用逻辑 I/O 与物理设备 I/O 分开。 |
| 空间 | 在线索引、工作镜像、两份保留点、候选点、失效 blob、GC 临时空间；硬链接按物理占用和逻辑大小分别报告。 |
| 恢复 | 检测/替换设备时间与算法时间分开；Restore、Open、首次正确服务及恢复后性能分别测量。 |
| 正确性 | 覆盖/删除/批次、原生文件切换、发布、GC、二次中断；恢复 oracle 不参与恢复算法。 |

推荐用“前台延迟—发布滞后—保留空间”的曲线展示取舍。单个吞吐倍数会隐藏尽力一致性究竟付出了多少回退与空间代价。

现有 [分通道基线报告](/home/ceph-lj/metabypass_rocksdb/docs/components/metabypass/channel-baseline-comparison.md:1) 与 [分层报告](/home/ceph-lj/metabypass_rocksdb/docs/components/metabypass/tiered-storage-report.zh-CN.md:1) 已明确记录虚拟机、目录实验或延迟注入的限制。它们是原型验证证据，尚不能代替上述真实双设备、长期稳态评估。

## 7. 推荐的实施顺序

1. 固化本文的恢复合同，补齐逻辑边界与发布状态的说明；增加完整 I/O、保护进度和空间指标。
2. 降低新 SST 重复 value 校验与恢复重复扫描，保留现有安全条件，完成真实 SSD/HDD 的第一轮长期基线。
3. 实现保守的恢复点感知 GC，优先证明回收正确性和空间可持续性，再优化布局与调度。
4. 根据实测瓶颈，选择联合调度或提前服务恢复中的一个深化；不同时引入尾部追回、在线恢复、逻辑备份格式与复杂 GC。

这一路线保留“SSD 可以全部丢失”和“尽力一致性加速正常路径”。需要补强的是恢复依据的可解释性、保护服务的实际 I/O 成本，以及长期运行的空间与进度，而不是把普通写重新改成逐次同步保护。
