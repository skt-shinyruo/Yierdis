# Project Introduction

本文面向第一次阅读 Yierdis 的读者，回答三个问题：这个项目为什么存在、应该用什么预期理解它、为什么它比普通内存 KV 练习更值得读。

## 一句话定位

Yierdis 是一个 Java 25 + Netty + JDK FFM 实现的 Redis-style single-node in-memory KV server。它面向 Redis 风格的数据结构服务器做学习和工程实验：对外走 TCP + RESP，内部把协议、执行器、命令层、DB 和 native-memory runtime 拆开。

RESP2 是当前默认 wire target，`HELLO 3` 可以协商基础 RESP3 replies。它可以用 Redis 客户端做基础连接和命令烟测，但它不是 Redis drop-in replacement，也不承诺完整 Redis 命令语义、运维能力或生态兼容。

## 为什么做这个项目

普通内存 KV 练习通常很快会停在 `Map<String, byte[]>`、`SET`、`GET` 和一个简单 TCP 壳子。Yierdis 选择更接近 Redis 风格系统的路线，是为了把几个更有学习价值的问题放到同一个可运行代码库里：

- 协议字节如何变成命令请求，再回到 RESP reply。
- I/O 线程为什么不直接访问 DB，命令执行为什么需要 owner thread 语义。
- string、list、hash、set、zset、HLL、bitmap 等数据族为什么需要不同内部编码。
- TTL、maxmemory、近似淘汰、事务、背压和观测如何作为运行时护栏一起工作。
- JDK FFM 如何从语言特性变成 native-memory-backed storage path，而不只是一个 API 演示。

## 它当前已经覆盖什么

当前实现重点覆盖 Redis-style data families、TTL、maxmemory、approximate eviction、minimal transactions、backpressure、observability，以及 native-memory-backed paths。

更具体地说，读代码时可以预期看到这些已经落地的主线：

- Redis 风格数据族：string、list、hash、set、zset、HLL 和 bitmap 风格能力。
- key 生命周期：TTL、惰性过期、轻量 maintenance 和 keyspace 入口。
- 内存约束：maxmemory、近似淘汰策略和 memory accounting。
- 执行保护：最小事务队列、有界 executor queue、queued-bytes 预算和连接级 backpressure。
- 可观测性：`INFO`、`INFO yierdis`、`STATS`、`MEMORY STATS`、`MEMORY USAGE`、`OBJECT ENCODING` 等观察入口。
- native memory：默认路径使用 JDK FFM 承载 keyspace、expires 和部分值结构。

## 它当前还没有什么

Yierdis 当前没有 AOF/RDB、replication/cluster、Lua、ACL/TLS、PubSub，也没有 full Redis ecosystem compatibility。

这些缺口意味着你不应该把它当成“可以替换 Redis 的 Java 版本”。它更适合被当成一个边界清楚的 Redis 风格系统样本：先把单机内存、协议、命令、DB、执行器和 native memory 的核心路径讲清楚，再把持久化、复制、集群、安全、订阅和完整生态兼容留给后续演进。

## 为什么不是普通 Map 服务

Yierdis 的价值不在于“能存 key/value”，而在于它把一个小型数据结构服务器拆成了可追踪的层：

- 协议层只关心 RESP request/reply，不成为命令语义的真相源。
- 执行层把网络请求适配为统一执行请求，并通过 executor 保持 DB owner thread 访问。
- 命令层通过能力接口访问 DB，而不是直接依赖具体内存实现。
- DB 层维护 keyspace、expires、数据族操作、内存账本和 key lifecycle。
- memory runtime 负责 native handle、region/span/access model 和 maxmemory 相关约束。

这让它比“一个 Map 加几个命令”更接近数据库内核阅读材料：你会看到模块边界、对象生命周期、运行时预算、协议适配和数据结构编码之间的协作。

## 读代码前先建立的心智模型

先把 Yierdis 看成一条单机请求流水线，而不是一个大类：

```text
Redis client
  -> Netty / RESP
  -> execution adapter
  -> command executor
  -> command processor
  -> DB capability interfaces
  -> in-memory DB + FFM runtime
  -> ReplyWriter / RESP write-back
```

这条线有三个关键约束：

- RESP2/RESP3 是 wire boundary，命令语义在 command 和 DB 层。
- Netty I/O 负责收发和背压，DB 修改收敛到 command executor。
- native memory 不是旁路优化，而是当前默认数据路径的一部分。

## 接下来读什么

先读 [`project-overview.md`](./project-overview.md) 建立模块、运行时边界和首批源码入口；再读 [`request-execution-flow.md`](./request-execution-flow.md) 顺着一次请求从 RESP 字节走到 DB 再写回。
