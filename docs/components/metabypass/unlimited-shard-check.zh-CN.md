# 7 个失败分片的无时限复查

2026-09-20。取消原来的 `timeout 60`，保留原测试二进制、分片编号和分片总数，
在同一磁盘上的全新 `/tmp` 目录运行，最多并行 4 个进程。此次未使用分片或进程
执行时限，也未使用 tmpfs 替代主复查。并发负载比原来的全量检查低，因此不能
把耗时变化全部归因于移除了超时限制。

[原始日志、耗时及诊断对照](experiments/unlimited-shard-check.json)。

| 原失败分片（编号从 0 开始） | 此次结果 | 耗时 |
| --- | --- | ---: |
| db_bloom_filter_test，3/12 | 8 个用例通过，1 个断言失败 | 9.87 秒 |
| db_bloom_filter_test，4/12 | 8 个用例通过，1 个断言失败 | 12.30 秒 |
| prefetch_test，8/11 | 首个用例断言失败，随后 SIGSEGV；后续用例未执行 | 0.62 秒 |
| compaction_service_test，15/46 | 1/1 通过 | 42.51 秒 |
| external_sst_file_test，253/286 | 1/1 通过 | 36.88 秒 |
| column_family_test，4/13 | 10/10 通过 | 37.87 秒 |
| column_family_test，6/13 | 9/9 通过 | 36.87 秒 |

结论：原来的 4 个超时分片均能正常结束，此次未观察到死锁。另 3 个失败分片与
时间上限无关。它们是测试预期对环境的依赖，以及一个失败路径上的资源生命周期问题。
没有据此宣称整个仓库已经通过全量检查。

## 两个 BloomFilter 断言

失败用例为 `MutatingRibbonFilterPolicy` 和 `MutableFilterPolicy`。
测试用 `BLOCK_CACHE_FILTER_BYTES_INSERT / key_count` 推算过滤器 bits/key，
要求 Ribbon 配置接近 `7 +/- 0.3`。本环境链接 tcmalloc，缓存内存计费包含分配器
尺寸取整和对象开销；同时默认的 `optimize_filters_for_memory=true` 会利用分配器
取整后的空间扩展过滤器。

仅修改临时诊断副本，打印 SST 属性和缓存统计，得到：

| 对照（8,000 个 key 的 Ribbon 阶段） | SST filter_size | 缓存统计字节 | 统计 bits/key |
| --- | ---: | ---: | ---: |
| 默认内存优化开启 | 8,181 | 8,248 | 8.248 |
| 仅关闭过滤器内存优化 | 6,965 | 8,248 | 8.248 |

因此，简单关闭该选项仍不能让这两个缓存计费断言通过。逻辑过滤器大小与缓存内存
计费不是同一个量。两个测试中的数据读取检查通过，失败的是固定空间预期。
修复方向是区分策略切换、过滤器实际大小和缓存计费的验证，避免用固定的逻辑
bits/key 预测分配器相关的缓存占用；不应仅把阈值改成这台机器的 8.248。

## Prefetch 断言与后续崩溃

失败用例为 `PrefetchTest/PrefetchTest.Basic/0`，参数是“不支持原生 Prefetch、
不开启 direct I/O”。

1. 本机磁盘对应的 `max_sectors_kb` 为 1280。Linux 的
   `PosixFileSystem::OptimizeForCompactionTableRead` 会将 2 MiB 的配置限制为
   1,310,720 字节，再结合当前 block 和对齐形成读请求。诊断观测到 1,314,816
   和 1,318,912 字节的请求。
2. 测试仍使用未经调整的 `Options().compaction_readahead_size`（2 MiB）计算
   下界 1,887,436.8 字节，所以断言失败。原二进制的同一用例在 tmpfs 上通过，
   支持这是测试未考虑磁盘系统上限，而非预读请求无故缩小。
3. `ASSERT_GE` 提前退出测试函数，跳过末尾 `Close()`。局部 CompositeEnv 已销毁，
   fixture 中 DB 随后才在 `DBTestBase` 析构时关闭，继续使用失效 Env。此次堆栈
   明确经过 `DBImpl::CloseHelper` 和 `DBTestBase` 析构。
4. 临时诊断副本增加退出清理，确保先关闭 DB、后销毁 Env。原预读断言仍失败，
   但退出码由 SIGSEGV 变为普通测试失败（1），验证了资源销毁顺序的问题。

修复方向：按文件系统调整后的预读大小验证统计；另外用 RAII 保证所有提前退出
路径都先关闭 DB，再释放 Env。不能只修断言，让失败路径的生命周期缺陷继续隐藏。

## 复现与范围

从仓库根目录运行，例如：

```sh
mkdir -p /tmp/mb-unlimited-example
TEST_TMPDIR=/tmp/mb-unlimited-example GTEST_TOTAL_SHARDS=13 GTEST_SHARD_INDEX=4 ./column_family_test
```

其他分片按表替换二进制、总数和编号，使用独立目录。命令不加 `timeout`。
本轮只检查原普通构建的 7 个失败分片，没有重新执行全部测试或 Status 全量检查。
诊断改动仅存在于 `/tmp/mb-seven-unlimited` 的临时源码和可执行文件中，链接当前
仓库的库；仓库测试和生产代码没有因本轮检查而修改。上一次身份恢复修复仍保留。

引用的本地聊天已读取；其最近内容是推理课程实验验证，与本次 RocksDB 测试无关，
未作为测试结论的依据。
