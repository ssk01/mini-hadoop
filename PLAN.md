# mini-hadoop: Java → C++ 重写方案

## 基准版本

**Nutch 0.7** (2005-08-17, Apache Archive) — Hadoop 的真正起点

Hadoop 脱胎于 Nutch。Nutch 0.7 中的 `ndfs` 和 `mapReduce` 模块是最早的分布式文件系统 + MapReduce 实现。2006 年 2 月这些模块从 Nutch 拆分出来，成为独立的 Hadoop 项目。

| 指标 | Nutch 0.7 (选) | hadoop-0.10.1 (对比) |
|------|---------------|---------------------|
| 核心 Java 文件 | **114** | 269 |
| 总行数 | **19,464** | 56,043 |
| 实际代码行(去注释) | **11,451** | 34,271 |
| 模块数 | 6 | 11 |
| 构建 | Ant | Ant + Ivy |
| 发布时间 | 2005-08 | 2007-01 |

**预计 C++ 重写规模**: ~7,000-9,000 行 (Java 代码逻辑通常比 C++ 冗长约 1.3-1.5x)

### 核心模块分布

| 模块 | 文件数 | 代码行 | 职责 |
|------|--------|--------|------|
| **ndfs** | 14 | 3,569 | 分布式文件系统 (NameNode + DataNode 同文件) |
| **mapReduce** | 49 | 2,577 | MapReduce 调度与执行 |
| **io** | 23 | 1,981 | Writable 序列化, SequenceFile, MapFile |
| **ipc** | 3 | 722 | 轻量 RPC (Client, Server, RPC) |
| **fs** | 9 | 851 | 文件系统抽象 (LocalFS, NDFS, S3) |
| **util** | 16 | 1,751 | 配置、日志、工具类 |

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    CLI (main.cpp)                         │
│        namenode | datanode | jobtracker | tasktracker     │
│        dfs -put/-get/-ls | jar wordcount.so              │
├──────────────────────┬──────────────────────────────────┤
│     NDFS Client      │       MapReduce API               │
│   (NDFSClient.java)  │  (JobClient, Mapper, Reducer)     │
├──────────────────────┴──────────────────────────────────┤
│          IPC/RPC Layer (Writable serialization)           │
│          Client.java / Server.java / RPC.java             │
├──────────┬──────────┬──────────┬─────────────────────────┤
│ NameNode │ DataNode │JobTracker│     TaskTracker         │
├──────────┴──────────┴──────────┴─────────────────────────┤
│    FileSystem Abstraction (LocalFS, NDFS, ChecksumFS)    │
├──────────────────────────────────────────────────────────┤
│  I/O Layer (Writable, SequenceFile, MapFile, Compression) │
├──────────────────────────────────────────────────────────┤
│  Utilities (Config, Logging, Networking)                  │
└──────────────────────────────────────────────────────────┘
```

### 核心组件交互流

```
NDFS 写入流程:
  Client ──RPC──> NameNode (分配 blockId + DataNode 列表)
    │                 │
    │                 └── 内存 INodeTree + Block→DN 映射
    │                      (无 FsImage/EditLog, 重启靠 BlockReport 重建)
    │
    └──直连──> DataNode (写入 block 数据, pipeline 复制到副本)

MapReduce 执行流程:
  Client ──RPC──> JobTracker (提交 job, 计算 splits)
                      │
                      ├── RPC (心跳/pollForTask) ──> TaskTracker
                      │
  TaskTracker ──RPC──> JobTracker (报告进度/完成)
      │
      └── fork 子进程 ──> MapTask / ReduceTask
                              │
                              └── RPC (MapOutputProtocol) ──> 拉取 Map 输出 (shuffle)
```

## 技术选型

| 领域 | Java 原实现 | C++ 替代 | 理由 |
|------|------------|---------|------|
| 序列化 | Writable (自研) | **Protobuf** / 自研二进制 | Protobuf 有 schema evolution，或自研以保持轻量 |
| RPC | Java 动态代理 + DataOutputStream | **gRPC** / 自研基于 TCP | gRPC 成熟但重，自研更贴近原版精神 |
| HTTP 服务 | Jetty | **libmicrohttpd** / **cpp-httplib** | 内嵌 HTTP 用于 TaskTracker Web UI, shuffle |
| 构建 | Ant + Ivy | **CMake** | 跨平台标准 |
| 配置 | XML (SAX 解析) | **toml11** / **libconfig** | 轻量配置格式 |
| 日志 | Log4j | **spdlog** | header-only, 高性能 |
| 网络 | java.nio.SocketChannel | **Boost.Asio** / **libuv** / raw epoll | 异步 I/O |
| 线程 | java.lang.Thread | **std::thread** / **ThreadPool** | C++11 标准库 |
| 压缩 | JNI → zlib/lzo | 直接链接 **zlib** / **lz4** / **snappy** | 去掉 JNI 开销 |
| 测试 | JUnit | **GoogleTest** + **GoogleMock** | 业界标准 |

**推荐方案: 自研轻量 RPC + Protobuf（或 flatbuffers），Asio 网络，CMake 构建。**

原因：gRPC 引入过多依赖（http2, protobuf 大而全），与原版「简单高效自研」的理念不符。Protobuf 仅用于消息定义，RPC 层自己写（类似原版 RPC.java 的 400 行核心逻辑）。

## 模块重写规划

### Phase 0: 基础设施 (预计 1 周)

```
目标: 可编译的空壳，跑通 CI/测试框架
```

| 模块 | 文件数 | 说明 |
|------|--------|------|
| CMake 构建 | - | 项目骨架，依赖管理 |
| 日志 (spdlog) | - | 引入 header-only 日志库 |
| 配置解析 | ~3 | 替代 conf/Configuration.java, 支持 key-value |
| 单元测试框架 | - | GoogleTest 集成 |

**产出**: `mini-hadoop` 可编译，`mini-hadoop --version` 可输出版本号。

### Phase 1: I/O 层 + 序列化 (预计 1.5 周)

```
目标: 实现 Writable 等价物 + 文件格式，这是整个系统的数据基础
```

| 原 Java 类 | C++ 对应 | 说明 |
|-----------|---------|------|
| `Writable` | `Writable` 基类 | `virtual void Read(Buffer&)` / `Write(Buffer&)` |
| `IntWritable`, `LongWritable`, `Text`, `BytesWritable`, etc. | 对应模板/类 | 基础类型序列化 |
| `WritableComparable` | `ComparableWritable<T>` | 带比较的 Writable |
| `DataInputBuffer`, `DataOutputBuffer` | `ByteBuffer` / `BufferWriter` | 内存 buffer 读写 |
| `SequenceFile` | `SequenceFile` | block-compressed 顺序文件格式 |
| `MapFile` | `MapFile` | 带索引的排序 SequenceFile |
| `MD5Hash` | `MD5Hash` | OpenSSL MD5 |
| 压缩 (ZlibCompressor, LzoCompressor) | 直接链接 `zlib`, `lz4` | 去掉 JNI 层 |

**核心设计决策**：Writable 序列化是 Hadoop 整个 RPC 和数据流的基础。所有 RPC 参数、block 元数据、MapReduce 中间数据都用它。需要零拷贝设计。

### Phase 2: IPC/RPC 框架 (预计 3 天)

Nutch 0.7 的 IPC 极简：**3 个文件, 722 行代码**。只有 Client/Server/RPC 三个类，不支持动态代理——所有 RPC 都是手写 switch-case。

| 原 Java 类 | 行数 | C++ 对应 |
|-----------|------|---------|
| `Server` | ~400 | `TcpServer` | Reactor 模式, 多 Handler 线程 |
| `Client` | ~300 | `TcpClient` | 连接池, 同步/异步调用 |
| `RPC` | ~200 | `RpcChannel` | 编解码, callId 管理 |

**注意**: Nutch 0.7 的 RPC 没有 Protocol 接口抽象。NameNode RPC 就是硬编码的 `switch(p.op) { case OP_OPEN: ... case OP_CLOSE: ... }`。这反而更简单——C++ 重写时直接保持这种风格。

### Phase 3: NDFS (分布式文件系统) (预计 1.5 周)

**这是最关键的模块。** Nutch 0.7 的 NDFS 只有 **14 个文件, 3,569 行代码**，且 NameNode 和 DataNode 是 NDFS.java 的两个内部类。

#### 架构 (极简版)

```
NDFS.java (933 行)
├── NameNode (内部类 extends IPC Server)
│   ├── FSNamesystem (1211 行) — INode 树 + Block 映射
│   │   ├── FSDirectory — 文件/目录元数据
│   │   └── Block → [DatanodeInfo] 映射
│   ├── 收到 BlockReport → 更新 Block 位置
│   └── 为 Client 分配 DataNode 列表
│
├── DataNode (内部类 extends IPC Client)
│   ├── FSDataset (408 行) — 本地 block 文件存储
│   ├── DataXceiveServer — TCP 接收/发送 block 数据
│   ├── 心跳 + BlockReport → NameNode
│   └── Block 复制 (pipeline 写入)
│
└── NDFSClient (1147 行) — 用户端读写 API
```

#### 重写映射

| 原类 | 行数 | C++ | 说明 |
|------|------|-----|------|
| `NDFS.NameNode` | ~300 | `NameNode` | extends IPC Server |
| `FSNamesystem` | 1211 | `FsNamesystem` | INode 树, Block→DN 映射 |
| `FSDirectory` | ~500 | `InodeTree` | 文件和目录的层次结构 |
| `NDFS.DataNode` | ~400 | `DataNode` | extends IPC Client |
| `FSDataset` | 408 | `BlockStorage` | 本地 block 文件管理 |
| `NDFSClient` | 1147 | `DfsClient` | readBlock/writeBlock/readFile |
| `Block` | ~50 | `BlockId` | blockId + length |
| `DatanodeInfo` | ~80 | `DataNodeInfo` | host/port/capacity |

**不需要的** (Nutch 0.7 没有): FsImage, EditLog, SafeMode, Lease 管理。这些是后来 HDFS 加入的。NDFS 0.7 重启 NameNode 就丢数据——DataNode 自己持有 block 数据，NameNode 靠 BlockReport 恢复。

### Phase 4: MapReduce 框架 (预计 2 周)

**Nutch 0.7 MapReduce: 49 文件, 2,577 行代码。** 远比 hadoop-0.10.1 的 70 文件/15K 行精简。

#### 核心类

| 原类 | 行数 | C++ | 职责 |
|------|------|-----|------|
| `JobTracker` | 910 | `JobTracker` | Job 提交、调度、状态跟踪 |
| `TaskTracker` | 504 | `TaskTracker` | 执行 Map/Reduce Task |
| `JobClient` | ~200 | `JobClient` | CLI 提交 job |
| `JobConf` | 203 | `JobConfig` | Job 配置 |
| `MapTask` | 141 | `MapTask` | Map 阶段执行 |
| `ReduceTask` | 218 | `ReduceTask` | Reduce + shuffle |
| `TaskRunner` | ~200 | `TaskRunner` | fork 子进程 |
| `CombiningCollector` | ~50 | `Combiner` | Map 端预聚合 |

**MapReduce API** (用户接口):
| 原接口 | C++ | 说明 |
|--------|-----|------|
| `Mapper` | `Mapper` 虚基类 | `void map(K, V, OutputCollector&)` |
| `Reducer` | `Reducer` 虚基类 | `void reduce(K, Iterator<V>, OutputCollector&)` |
| `Partitioner` | `Partitioner` | `int partition(K, int numPartitions)` |
| `InputFormat` / `RecordReader` | 同上 | 支持 TextInputFormat, SequenceFileInputFormat |
| `OutputFormat` / `RecordWriter` | 同上 | TextOutputFormat, SequenceFileOutputFormat |

### Phase 5: CLI + 集成测试 (预计 1 周)

```
mini-hadoop namenode    # 启动 NameNode
mini-hadoop datanode    # 启动 DataNode
mini-hadoop jobtracker  # 启动 JobTracker
mini-hadoop tasktracker # 启动 TaskTracker
mini-hadoop dfs -put localfile /remote/path
mini-hadoop dfs -get /remote/path localfile
mini-hadoop dfs -ls /
mini-hadoop jar wordcount.so input/ output/  # 跑 WordCount
```

## 目录结构规划

```
mini-hadoop/
├── CMakeLists.txt              # 顶层构建
├── README.md
├── config/
│   └── mini-hadoop.toml        # 默认配置 (替代 hadoop-default.xml)
├── src/
│   ├── CMakeLists.txt
│   ├── main.cpp                # CLI 入口
│   ├── common/                 # 通用工具
│   │   ├── config.h/cc         # 配置解析
│   │   ├── logging.h/cc        # 日志 (spdlog wrapper)
│   │   ├── status.h/cc         # Status/Error 类型
│   │   └── net_util.h/cc       # 网络工具
│   ├── io/                     # I/O + 序列化
│   │   ├── writable.h/cc
│   │   ├── writable_types.h/cc # IntWritable, Text, etc.
│   │   ├── buffer.h/cc         # DataInput/Output buffer
│   │   ├── sequence_file.h/cc
│   │   ├── map_file.h/cc
│   │   ├── md5.h/cc
│   │   └── compress/           # zlib, lz4 wrapper
│   ├── ipc/                    # RPC 框架
│   │   ├── server.h/cc
│   │   ├── client.h/cc
│   │   └── protocol.h
│   ├── fs/                     # 文件系统抽象
│   │   ├── filesystem.h/cc
│   │   ├── local_fs.h/cc
│   │   ├── checksum_fs.h/cc
│   │   ├── path.h
│   │   ├── file_status.h
│   │   └── fs_shell.h/cc
│   ├── ndfs/                   # 分布式文件系统 (Nutch DFS)
│   │   ├── name_node.h/cc      # NameNode: 元数据管理, RPC 服务
│   │   ├── data_node.h/cc      # DataNode: block 存储 + 心跳
│   │   ├── fs_namesystem.h/cc  # INode 树 + Block 映射
│   │   ├── inode_tree.h/cc     # 文件/目录层次结构
│   │   ├── block_storage.h/cc  # 本地 block 文件读写
│   │   ├── dfs_client.h/cc     # 用户端 read/write/list API
│   │   ├── block.h             # BlockId 类型
│   │   └── datanode_info.h     # DataNode 元信息
│   └── mapred/                 # MapReduce
│       ├── job_tracker.h/cc    # Job 调度 + 状态管理
│       ├── task_tracker.h/cc   # Task 执行
│       ├── map_task.h/cc       # Map 阶段
│       ├── reduce_task.h/cc    # Reduce + shuffle
│       ├── task_runner.h/cc    # fork 子进程
│       ├── api/
│       │   ├── mapper.h        # Mapper 虚基类
│       │   ├── reducer.h       # Reducer 虚基类
│       │   ├── partitioner.h   # Partitioner
│       │   ├── input_format.h  # InputFormat/RecordReader
│       │   └── output_format.h # OutputFormat/RecordWriter
│       └── job_config.h/cc     # JobConf 替代
├── examples/
│   ├── wordcount.cpp
│   ├── grep.cpp
│   └── pi_estimator.cpp
├── tests/
│   ├── CMakeLists.txt
│   ├── io/
│   ├── ipc/
│   ├── dfs/
│   └── mapred/
└── docker/                     # 开发/测试环境
    └── Dockerfile
```

## 总工作量估算

| Phase | 内容 | 预估人天 |
|-------|------|---------|
| Phase 0 | 基础设施 (CMake, 日志, 配置, 测试框架) | 3 |
| Phase 1 | I/O 层 + 序列化 (23 文件 → ~15 C++ 文件) | 4 |
| Phase 2 | IPC/RPC 框架 (3 文件 → ~3 C++ 文件) | 3 |
| Phase 3 | NDFS (14 文件 → ~8 C++ 文件) | 7 |
| Phase 4 | MapReduce (49 文件 → ~15 C++ 文件) | 8 |
| Phase 5 | CLI + 端到端集成 | 5 |
| **合计** | | **~30 人天 (约 6 周)** |

## 关键技术风险

1. **NDFS 无持久化元数据**: Nutch 0.7 没有 FsImage/EditLog。NameNode 重启后靠 DataNode BlockReport 重建 Block 映射，但目录结构会丢失。C++ 版可先保持此行为（最简单），后续再加持久化。
2. **RPC 协议硬编码**: 没有 IDL/Protobuf，所有消息格式手写 Writable 序列化。好处是不引入额外依赖，坏处是改协议要改多处代码。
3. **并发模型**: Nutch 0.7 使用 Java `synchronized`，C++ 需 `std::mutex`。NameNode 所有操作串行化执行（单线程 RPC Handler），简化并发。

## 相比 Nutch 0.7 原版的简化

| 保留 | 去除 |
|------|------|
| NDFS 核心: NameNode + DataNode + Client | Nutch 爬虫/搜索相关代码 (analysis, fetcher, indexer...) |
| MapReduce: JobTracker + TaskTracker | Record I/O 代码生成 (21 个生成文件) |
| IPC: Client/Server/RPC | FTP FileSystem |
| IO: Writable, SequenceFile, MapFile, 压缩 | S3 FileSystem (可选后续加) |
| FS: LocalFS, ChecksumFS | Web UI (Jetty → 可选) |
| 配置: NutchConf → TOML | 分布式缓存 (DistributedCache) |

## 验证策略

每个 Phase 内嵌测试——先写测试，再写实现。不做跨版本对比（Nutch 0.7 无法在现代 JDK 上编译运行），只做自洽验证。

| 层级 | 方法 | 示例 |
|------|------|------|
| 单元测试 | GTest 独立验证，不依赖网络 | Writable round-trip, SequenceFile 读写, INode 树增删查, BlockStorage CRC |
| 集成测试 | 同进程内启动多组件，回环网络 | NameNode + DataNode + Client 单机 DFS put/get, checksum 一致 |
| 端到端 | fork 真实子进程，完整链路 | WordCount 已知输入 → diff 预期输出, 多文件输入, 故障恢复 |

每完成一个模块跑 `ctest`，全部绿灯才进下一 Phase。

---

*Phase 0 开始。*
