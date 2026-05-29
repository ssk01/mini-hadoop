# mini-hadoop 架构文档

## 项目概述

mini-hadoop 是 Apache Nutch 0.7 (2005) 中 NDFS + MapReduce 模块的 C++ 复现。采用 header-only 设计，零外部运行时依赖（仅构建时依赖 CMake/spdlog/toml++/GTest/OpenSSL）。

**代码量**: ~3,300 行 C++ (header-only) | **测试**: 57 个, 50 秒全通过

## 模块架构

```
src/
├── main.cpp              # CLI 入口
├── common/error.h        # Status 错误类型
├── io/                   # I/O 层
│   ├── buffer.h          # 大端序列化 InputBuffer/OutputBuffer
│   ├── writable.h        # Writable/WritableComparable 接口
│   ├── writable_types.h  # Int/Long/Float/Boolean/UTF8/Bytes/Null Writable
│   ├── md5.h             # MD5Hash (OpenSSL/CommonCrypto)
│   └── sequence_file.h   # SequenceFile (有 bug, 暂未启用)
├── ipc/                  # RPC 框架
│   ├── server.h          # TCP RpcServer (acceptor + worker pool)
│   └── client.h          # TCP RpcClient (连接 + 同步调用)
├── ndfs/                 # 分布式文件系统
│   ├── block.h           # Block 结构体 (blockId, numBytes, genStamp)
│   ├── datanode_info.h   # DataNode 元信息 (host, port, capacity, remaining)
│   ├── inode_tree.h      # INode 树 (dir/file 层次结构)
│   ├── fs_namesystem.h   # NameNode 核心 (INode树 + Block→DN 映射 + 心跳)
│   ├── name_node.h       # NameNode RPC 服务 (11 handlers)
│   ├── data_node.h       # DataNode (心跳线程 + block RPC server + pipeline)
│   ├── block_storage.h   # 本地 block 文件 I/O (store/read/remove/report)
│   ├── dfs_client.h      # 客户端 (writeFile/readFile/list/mkdirs/delete)
│   └── fsimage.h         # FsImage 持久化 (save/load, 纯文本格式)
├── mapred/               # MapReduce 框架
│   ├── api/
│   │   ├── mapper.h      # Mapper/Reducer/Partitioner/OutputCollector 接口
│   │   ├── input_format.h # TextInputFormat/TextOutputFormat/RecordReader/Writer
│   │   └── ndfs_io.h      # NDFS 后端 InputFormat/OutputFormat
│   ├── job_tracker.h      # 单进程 JobTracker (WordCount 用)
│   ├── dist_job_tracker.h # 分布式 JobTracker (RPC-based)
│   ├── task_tracker.h     # TaskTracker (poll-for-task + 线程执行 + shuffle)
│   └── examples/wordcount.h # WordCount Mapper/Reducer
└── fs/                    # (预留) FileSystem 抽象层
```

## 各组件详解

### 1. I/O 层 (`src/io/`)

**buffer.h** — 序列化基础设施
- `OutputBuffer`: 大端写入 int/long/float/double/string/bytes
- `InputBuffer`: 大端读取，带越界检查
- 用途: 所有 RPC 消息编解码、Writable 序列化

**writable.h / writable_types.h** — 类型系统
- `Writable` 基类: `Write(OutputBuffer&)` / `ReadFields(InputBuffer&)`
- 基础类型: `IntWritable`, `LongWritable`, `UTF8`, `BytesWritable` 等
- `WritableComparable`: 带排序能力的 Writable

**md5.h** — MD5 哈希
- `MD5Hash::Compute(data)` → 128-bit hash
- macOS 用 CommonCrypto, Linux 用 OpenSSL

### 2. IPC/RPC 框架 (`src/ipc/`)

**server.h — RpcServer**
```
监听端口 → accept() → 入队 → worker 线程池处理
协议: [4B total_len][4B method_id][Writable params...]
响应: [4B total_len][Writable result...]
```
- 注册 handler: `RegisterHandler(method_id, callback)`
- Worker 线程数可配置，默认 4
- `HandleConnection`: 循环读请求 → 分发 → 写响应

**client.h — RpcClient**
- `Connect(host, port)` → 建立 TCP 连接
- `Call(method_id, request, response)` → 同步调用
- `CallRaw(method_id, data, response)` → 原始字节调用（NDFS 内部使用）

### 3. NDFS 分布式文件系统 (`src/ndfs/`)

#### 3.1 架构总览

```
Client (dfs_client.h)
  │
  ├── RPC ──→ NameNode (name_node.h)
  │              └── FsNamesystem (fs_namesystem.h)
  │                    ├── InodeTree (inode_tree.h)   文件/目录树
  │                    ├── Block→[DataNodeInfo]       块位置映射
  │                    └── DataNodeInfo 注册表        节点管理
  │
  └── RPC ──→ DataNode (data_node.h)
                 ├── BlockStorage (block_storage.h)  本地文件 I/O
                 └── Pipeline 转发                   DN→DN 复制
```

#### 3.2 各组件职责

**inode_tree.h — INode 树**
- 树形结构: `INode{type(DIR|FILE), name, blocks[], children[]}`
- 操作: `GetNode(path)`, `Mkdirs(path)`, `CreateFile(path)`, `Delete(path)`, `ListDir(path)`
- 无持久化，纯内存

**fs_namesystem.h — NameNode 核心逻辑**
- **Client 操作**:
  - `StartFile(path, overwrite)` → 分配第一个 Block + 选择 DataNode 列表
  - `AddBlock(path)` → 追加新 Block
  - `GetBlocks(path)` → 返回所有 Block 及其位置
  - `Delete(path)`, `Mkdirs(path)`, `GetListing(path)`
- **DataNode 操作**:
  - `Heartbeat(name, host, port, capacity, remaining)` → 注册/更新 DN 状态
  - `BlockReport(name, blocks)` → DN 上报持有的 Block，重建 block→DN 映射
  - `BlockReceived(name, block, del_hint)` → DN 确认收到 Block
- 并发安全: 所有操作持 `std::mutex` 锁
- 默认配置: block_size=64MB, replication=3

**name_node.h — NameNode RPC 服务**
- 11 个 RPC handler，绑定到 `NdfsOp` 枚举
- 反序列化请求 → 调用 FsNamesystem → 序列化响应
- RPC 协议: 每个 handler 手写编解码（类似 Nutch 0.7 的 switch-case 风格）
- 启动时从 FsImage 加载命名空间，停止时保存

**block_storage.h — 本地 Block 存储**
- 磁盘格式: `{data_dir}/{block_id}.blk`（纯二进制文件）
- `StoreBlock(block, data, len)` → 写入文件
- `ReadBlock(block_id, data)` → 读取文件
- `RemoveBlock(block_id)` → 删除文件
- `GetBlockReport()` → 扫描目录，返回所有 Block 列表
- `GetCapacity()/GetRemaining()` → 通过 std::filesystem::space 获取磁盘空间

**data_node.h — DataNode 进程**
- 内含两个服务器:
  - **Block RPC Server** (用户指定端口): 处理 block 读写请求
    - op 0: 写入 block + pipeline 转发到下游 DN
    - op 1: 读取 block 并返回数据
  - **NameNode RPC Client**: 连接 NameNode 发送心跳
- **Pipeline 复制**: op 0 handler 在本地存储后，解析下游 DN 列表，连接下一个 DN 转发数据
- **心跳线程**: 每 3 秒发送心跳 + BlockReport
- 启动时先注册心跳，再启动心跳线程

**dfs_client.h — 客户端 API**
- `WriteFile(path, data, len, overwrite)`: 
  1. 按 block_size 切分数据
  2. 对每个 block: StartFile/AddBlock RPC → 获取 DN 列表
  3. 通过 pipeline 写入第一个 DN（由 DN 转发到下游）
  4. BlockReceived RPC 通知 NameNode
- `ReadFile(path, data)`:
  1. GetBlocks RPC → 获取 block 列表 + DN 位置
  2. 逐 block 连接任意 DN 读取
  3. 拼接返回完整数据
- `List(path)`, `Mkdirs(path)`, `Delete(path)`: 直接 RPC 调用

**fsimage.h — FsImage 持久化**
- `Save(tree, path)`: 序列化 INode 树到文本文件
  ```
  DIR /data
  FILE /data/a.txt blocks=123456,789012
  DIR /data/sub
  ```
- `Load(tree, path)`: 从文本文件恢复 INode 树
- block→DN 映射由 DataNode BlockReport 重建，不持久化

#### 3.3 数据流

**写入流程 (pipeline)**:
```
Client → RPC(StartFile) → NameNode → 返回 [Block, [DN1, DN2, DN3]]
Client → RPC(op=0) → DN1 → 存储本地 → RPC(op=0) → DN2 → 存储本地
Client → RPC(BlockReceived) → NameNode → 更新 Block→DN 映射
```

**读取流程**:
```
Client → RPC(GetBlocks) → NameNode → 返回 [Block1→[DN1,DN2], Block2→[DN1]]
Client → RPC(op=1) → DN1 → 返回 Block1 数据
Client → RPC(op=1) → DN1 → 返回 Block2 数据  (回退连接其他 DN)
```

### 4. MapReduce 框架 (`src/mapred/`)

#### 4.1 两种模式

**单进程模式 (job_tracker.h)**:
- 用于简单测试和 WordCount
- 在同一进程内串行执行 Map → Shuffle/Sort → Reduce
- 无容错、无调度

**分布式模式 (dist_job_tracker.h + task_tracker.h)**:
- JobTracker: RPC 服务器 (port 可配)
  - op 0: SubmitJob (input, output, numMaps, numReduces)
  - op 1: PollForTask (TaskTracker 轮询获取任务)
  - op 2: TaskCompleted (TaskTracker 报告任务完成)
- TaskTracker: 连接 JobTracker，轮询任务
  - MapTask: 在新线程中读取输入 → 运行 Mapper → 分区输出到内存
  - ReduceTask: 通过 Shuffle RPC 拉取 Map 输出 → Merge Sort → Reduce → 输出
  - Shuffle Server (op 0): 提供 MapOutput 数据给 ReduceTask

#### 4.2 Shuffle 机制

```
ReduceTask → RPC(op=0) → TaskTracker's Shuffle Server
  Request:  [task_id][partition]
  Response: [count][key1][val1][key2][val2]...
```

Map 输出存于 TaskTracker 内存 (`map_outputs_`)，Reduce 通过 RPC 拉取。

#### 4.3 API (`src/mapred/api/`)

**mapper.h** — 用户编程接口:
```cpp
template<K1,V1,K2,V2> class Mapper { virtual void Map(K1,V1,OutputCollector<K2,V2>&) = 0; };
template<K2,V2,K3,V3> class Reducer { virtual void Reduce(K2,vector<V2>,OutputCollector<K3,V3>&) = 0; };
template<K> class Partitioner { virtual int GetPartition(K,int) = 0; };
```

**input_format.h** — 数据 I/O:
- `TextRecordReader`: 按行读取文本文件，key=偏移量, value=行内容
- `TextRecordWriter`: key<TAB>value 格式输出
- `NdfsLineRecordReader`: 从 NDFS 读取文件
- `NdfsRecordWriter`: 写入 NDFS

### 5. CLI (`src/main.cpp`)

```
mini-hadoop version                          # 显示版本
mini-hadoop namenode [port]                  # 启动 NameNode
mini-hadoop datanode [host] [port] [bs_port] # 启动 DataNode
mini-hadoop dfs put <local> <remote>         # 上传文件到 NDFS
mini-hadoop dfs get <remote> <local>         # 从 NDFS 下载文件
mini-hadoop dfs ls [path]                    # 列出目录
mini-hadoop dfs rm <path>                    # 删除文件
mini-hadoop dfs mkdir <path>                 # 创建目录
mini-hadoop wordcount <input> <output>       # 运行 WordCount
```

## 关键技术决策

| 决策 | 原因 |
|------|------|
| Header-only | 零编译依赖，模板内联优化 |
| 自研 RPC 协议 | 避免 gRPC/protobuf 重依赖，贴近原版 Nutch 精神 |
| 手写序列化 | Writable 格式与 Nutch 0.7 兼容 |
| 单线程 NameNode | 简化并发，Nutch 0.7 也是如此 |
| BlockReport 重建映射 | 避免持久化 Block→DN 映射（Nutch 0.7 方式） |
| FsImage 纯文本 | 可读、可调试，性能非瓶颈 |
| Pipeline 客户端→DN₁→DN₂ | 减少 Client 上行带宽 |

## 待完善

- SequenceFile 读写格式 bug
- DataXceiveServer 独立 block 传输（当前混在 RPC 中）
- MapReduce 分布式模式下 NDFS 端到端 I/O
- SafeMode / Lease 管理
- 多 DataNode 副本不足自动 re-replication
- Combiner (Map 端预聚合)
- 推测执行 (speculative execution) — WIP, 来自 hadoop-0.10.1

## 构建与测试

```bash
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j8
ctest --test-dir build                  # 57 tests, ~50s
./build/src/mini-hadoop version
```

运行 NDFS 集群:
```bash
./build/src/mini-hadoop namenode 9000 &
./build/src/mini-hadoop datanode 127.0.0.1 9000 9010 &
./build/src/mini-hadoop dfs put /etc/hosts /hosts.txt
./build/src/mini-hadoop dfs ls /
./build/src/mini-hadoop wordcount input.txt output.txt
```
