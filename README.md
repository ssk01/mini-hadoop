# mini-hadoop

Apache Nutch 0.7 (2005) 中 NDFS + MapReduce 模块的 C++ 复现。Hadoop 的前身在 C++20 中重生。

## 快速开始

```bash
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build -j8
ctest --test-dir build                  # 57 tests
./build/src/mini-hadoop version
```

## 运行集群

```bash
# 终端 1: 启动 NameNode
./build/src/mini-hadoop namenode 9000

# 终端 2: 启动 DataNode
./build/src/mini-hadoop datanode 127.0.0.1 9000 9010

# 终端 3: 使用 NDFS
./build/src/mini-hadoop dfs put /etc/hosts /hosts.txt
./build/src/mini-hadoop dfs ls /
./build/src/mini-hadoop dfs get /hosts.txt /tmp/hosts

# 运行 WordCount
echo -e "hello world\nhello mapreduce" > /tmp/input.txt
./build/src/mini-hadoop wordcount /tmp/input.txt /tmp/output.txt
cat /tmp/output.txt
```

## 模块

| 模块 | 文件 | 职责 |
|------|------|------|
| `io/` | buffer, writable, writable_types, md5 | 大端序列化 + 类型系统 + MD5 |
| `ipc/` | server, client | TCP RPC 框架 (acceptor + worker pool) |
| `ndfs/` | 8 files | 分布式文件系统: NameNode, DataNode, Client, FsImage |
| `mapred/` | 6 files | MapReduce: JobTracker, TaskTracker, Shuffle, API |

详见 [ARCHITECTURE.md](ARCHITECTURE.md)

## 特性

- **分布式文件系统**: 多 block、pipeline 复制、FsImage 持久化、NameNode 重启恢复
- **MapReduce**: 单进程 + 分布式两种模式、RPC 调度、Shuffle
- **正确性**: 全部 MD5 逐字节校验，100MB 级别压力测试
- **零外部运行时依赖**: header-only C++20，仅构建时依赖 spdlog/toml++/GTest

## 测试

```
57 tests, ~50s, 0 failures

  I/O 层:          25 tests (buffer, writable, md5)
  IPC:              3 tests (server/client round-trip)
  NDFS 单元:       15 tests (inode, namesystem, block storage)
  NDFS 集成:        5 tests (full cluster, pipeline, persistence, 10MB, 100MB)
  MapReduce:        3 tests (WordCount, 单进程, 分布式 JT+2 TT)
  全链路:           2 tests (NDFS→MR→NDFS, 多块大文件)
  压力:             4 tests (10MB I/O, 10 块边界, 200KB MR, 100MB MD5)
```

## 背景

2005 年，Doug Cutting 和 Mike Cafarella 在 Apache Nutch 项目中开发了 NDFS 和 MapReduce 来支撑搜索引擎的海量数据处理。2006 年这两个模块被拆分出来，成为 Apache Hadoop。本项目基于 Nutch 0.7（最后包含 NDFS 原型的版本）用现代 C++ 重写。

## 许可

Apache 2.0 — 与原 Nutch/Hadoop 保持一致。
