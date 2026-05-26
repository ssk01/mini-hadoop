# Plato.md - 项目约定与规范

### 基准版本选择
基于 **Nutch 0.7** (2005-08-17) 而非 hadoop-0.10.1。Nutch 0.7 是 Hadoop 的真正起点，核心代码仅 ~11,500 行（vs. hadoop-0.10.1 的 ~34,000 行）。源文件位于 Apache Archive: `/dist/lucene/nutch/nutch-0.7.tar.gz`。(2026-05-26)

### 重写策略
- 先做 NDFS（分布式文件系统），后做 MapReduce。NDFS 只需 14 个 Java 文件（~3,500 行代码）。
- NameNode 和 DataNode 可先作为一个进程的不同模块，再加 RPC 拆成独立进程。
- 不追求与原版协议兼容，只保留架构思想和核心功能。
- 不引入 protobuf/gRPC 等重型依赖，保持极简依赖树。
(2026-05-26)

<!-- 以下继续记录 -->
