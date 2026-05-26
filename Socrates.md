# Socrates.md - Q&A 知识库

### Q: 最早的 Hadoop 版本在哪里？有多大？
Apache Archive 中最早的 Hadoop 独立发布版是 hadoop-0.10.1 (2007-01)，269 个 Java 核心文件，~34,000 行有效代码。

但 Hadoop 的真正起源是 **Nutch 0.7 (2005-08-17)** 中的 `ndfs` 和 `mapReduce` 模块——Hadoop 的前身是 Nutch 的分布式文件系统和 MapReduce，2006 年 2 月才从 Nutch 拆分独立。Nutch 0.7 的核心仅 114 个文件、11,451 行有效代码，是 hadoop-0.10.1 的 1/3。

在 Nutch 0.7 中，NameNode 和 DataNode 都是 NDFS.java 的内部类，分别继承 IPC Server 和 IPC Client。整个分布式文件系统核心只在一个 933 行的文件中，没有 FsImage/EditLog 持久化（NameNode 重启靠 DataNode BlockReport 恢复）。

Nutch 0.7 在 `https://archive.apache.org/dist/lucene/nutch/nutch-0.7.tar.gz`，其 NDFS 源码路径为 `src/java/org/apache/nutch/ndfs/`。(2026-05-26 12:30)

<!-- 以下继续记录 -->
