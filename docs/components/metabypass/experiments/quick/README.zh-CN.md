# Metabypass 快速性能测试

协议 `metabypass-quick-v2` 使用同一当前源码和同一 Release 驱动。两组都用
相同的内联 RocksDB 参数和独立的空主库。`current` 通过 MetaBypass 写入并创建
备份，`baseline` 调用普通 RocksDB。两组均开启 WAL、关闭压缩，使用单写线程、
1 KiB 值及 `sync=false`。历史数据见[版本对照](../../version-comparison.md)和[通道基线对照](../../channel-baseline-comparison.md)。

## 构建和运行

```bash
python3 docs/components/metabypass/experiments/quick/build.py \
  --out /tmp/metabypass-quick-v2-build
python3 -m unittest discover -s docs/components/metabypass/experiments/quick -v
python3 docs/components/metabypass/experiments/quick/run.py \
  --build /tmp/metabypass-quick-v2-build/build.json \
  --data-parent /tmp --output /tmp/metabypass-quick-v2-smoke \
  --profile smoke
```

构建目录在仓库外，`--jobs` 可指定并发。runner 校验 v2 manifest、源码、
驱动和二进制哈希。输出目录必须尚不存在，数据父目录须位于本地磁盘文件系统并
具有至少 2 GiB 可用空间。默认固定到进程获准使用的前八个 CPU，可用 `--cpus`
指定。

| 场景 | 备份路径延迟 | 参数 |
|---|---:|---|
| `control` | 0 | WAL 和元数据队列 56 MiB、8 MiB，镜像批量阈值 256 KiB |
| `slow` | 1 ms 每次操作 | 与 control 相同 |
| `pressure` | 1 ms 每次操作 | 128 字节 key，256 KiB write buffer 和目标 SST，L0 触发值 2，层级基数 1 MiB，四个后台任务，队列 1792 KiB、256 KiB，批量阈值 64 KiB |

延迟只作用于备份路径的 writable `Append`、`Sync`、`Fsync` 及命名
`SyncFile`。记录调用数和实际累计等待。主库 I/O 按本地磁盘原有速度执行。
镜像间隔为 1 秒，周期检查点间隔为 30 秒。

- `smoke` 每组每场景进行一次 32 写预热和一次 64 写测量，共 6 个正式小样本，
  预算 5 分钟。
- `quick` 各组各场景 3 次，共 18 个正式样本，预算 30 分钟。
- `standard` 各组各场景 5 次，共 30 个正式样本，预算 60 分钟。
- `backpressure` 单独运行一次 current。1 KiB 值、128 字节 key、16,384 次写，
  使用默认 SST 参数和 2 MiB 队列，注入 1 ms 备份 I/O 延迟。要求实际观测前台
  和总队列等待、有效的队列峰值、备份延迟以及至少一个恢复点。

正式两组档位先各写 2,000 条进行校准，按较慢一组的写入生命周期为场景冻结共同
次数。范围从 2,000 到 50,000，control 可达 200,000。`--counts-from` 只能
读取完整、同设置的 v2 quick 或 standard 结果。各轮轮换执行顺序，单进程默认
超时 180 秒。

写入后记录 OPTIONS 和按 inode 去重的磁盘占用。current 删除本轮自己创建的
主库后，从备份进行 Restore 和 Open。baseline 直接重开本轮自己创建的主库。
两组都执行逐值核验、追加记录、关闭和重开核对。失败现场及原始输出保留。
成功样本默认清理，用 `--keep-data` 保留。

`results.json` 保存构建身份、环境、写入数及原始结果。`report.md` 报告正式
有效样本的前台吞吐、P99、生命周期、恢复、CPU、RSS 和磁盘占用。生命周期累计
前台 Put、current 的备份屏障和 Close。背压独立档位报告队列与恢复数据。
