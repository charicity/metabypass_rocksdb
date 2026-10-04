# Agent Instructions

This repository's authoritative agent instructions live in `CLAUDE.md`.

Read and follow [`CLAUDE.md`](./CLAUDE.md) in full before making changes or
reviewing code in this checkout.

If there is any ambiguity between this file and `CLAUDE.md`, `CLAUDE.md` takes
precedence.

## Project Background and Goals

This repository is a customized RocksDB fork for research, based on the RocksDB
v11.8.1 release. The goal is to improve and test the project's functionality.

The project aims to migrate the original custom Metabypass functionality built
on Ceph into this repository, complete and refine the implementation, and then
test and validate its functionality.

## node-ssd 基本信息与权限约束

### 基本信息

- SSH 别名：`node-ssd`；hostname：`8001`；登录用户：`lj`。
- SSD：`/home/lj/ssd`，设备 `/dev/sdb`，ext4，约 447 GiB。
- HDD：`/home/lj/hdd`，分区 `/dev/sdc1`，ext4，约 7.3 TiB。
- 实验目录：两处挂载下的 `metabypass_test/`。
- 已有 cgroup v1：`/sys/fs/cgroup/memory/mbgc-smallmem`，
  内存限制 8 GiB，`memory.swappiness=0`，无 memsw 限额接口。
- 系统 swap：`/swap.img`，约 8 GiB。

### 权限约束

- 使用 `lj` 账户及其现有权限执行操作。
- 未经用户明确授权，不使用 sudo，不修改全局 swap、缓存、
  调度器、挂载或系统配置。
- 仅修改、清理本任务所属且归属明确的实验文件，终止本任务
  启动的进程；不得干扰其他实验。
- 不覆盖既有实验目录；复用目录或 cgroup 前确认归属及占用情况。
