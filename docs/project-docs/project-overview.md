# 项目总览

本文从代码和运行时边界出发，说明 Yierdis 当前是什么、有哪些模块、一次请求会经过哪些层，以及读源码时先打开哪些文件。

## 当前定位

Yierdis 当前是 Java 25 + Netty + JDK FFM 实现的 Redis-style 单机内存 KV server。它对外暴露 Redis RESP TCP 协议，RESP2 是默认 wire target，`HELLO 3` 可以协商基础 RESP3 replies；对内把网络、协议、执行、命令、DB、memory runtime 和启动装配拆成独立模块。

读源码时最重要的定位是：它不是 Redis drop-in replacement，而是一个刻意限定在单机内存边界内的 Redis 风格系统实现。代码重点不是“兼容所有 Redis 行为”，而是展示一次请求如何穿过 RESP/Netty、执行器、命令处理器、DB 能力接口和 native-memory-backed 数据结构。

它也不是普通 `Map` 服务。普通 `Map` 只能解释 key/value 存取，解释不了 RESP wire format、连接级 session、事务 replay、TTL 和 maxmemory 的写路径约束、reply writer 语义、owner thread、backpressure、native handle lifetime 和 introspection。读代码时应该把它看成一个边界清楚的系统样本：网络、协议、执行、命令、DB、memory runtime 和启动组装各有自己的职责。

## 能力边界

当前已经覆盖的能力包括 Redis 风格数据族、TTL、maxmemory、approximate eviction、minimal transactions、backpressure、observability 和 native-memory-backed paths。

当前没有覆盖的能力包括 AOF/RDB、replication/cluster、Lua、ACL/TLS、PubSub 和 full Redis ecosystem compatibility。看到客户端兼容、协议协商或 Redis 风格命令时，都要把它理解为“当前子集”，而不是完整 Redis 兼容承诺。

## 技术栈和运行时特征

技术栈主线很短：

- Java 25：语言版本和 `java.lang.foreign` FFM API 的运行前提。
- Netty：TCP server、channel pipeline、I/O 线程和 write-back。
- RESP：请求解码、reply 编码和 RESP2/基础 RESP3 wire model。
- Maven multi-module：用模块边界隔离 common、memory、networking、server、command、DB、CLI、benchmark 和 tests。

运行时主线也很明确：

- Netty I/O 线程负责收包、解码、提交和写回，不直接修改 DB。
- `CommandExecutor` 负责排队、背压预算和 owner-thread 命令执行。
- engine 和 command processor 负责把统一执行请求路由到命令实现。
- DB 层通过能力接口暴露读写语义，内存实现持有 keyspace、expires、数据族和内存账本。
- JDK FFM runtime 支撑默认 native-memory path，并参与 maxmemory 相关约束。

读源码前先建立三条心智模型：

- 请求不是“方法调用”，而是一段从 RESP bytes 到 `ExecutionRequest`、executor、command handler、DB、`ReplyWriter` 再回到 RESP bytes 的链路。
- DB 不是一张大表，而是 keyspace、entry metadata、value roots、TTL index、memory ledger 和 native handles 共同维护的生命周期边界。
- native memory 不是旁路优化，而是当前默认数据路径的一部分；但它不等于零拷贝，copy 边界需要按接口 ownership 和 lifetime 判断。

## 模块总览

| 模块区域 | 主要职责 |
| --- | --- |
| `yierdis-common/yierdis-common-bytes` | 共享 byte/key 工具、字节视图和底层数据转换。 |
| `yierdis-memory/yierdis-memory-api` | memory 抽象、handle 和访问边界。 |
| `yierdis-memory/yierdis-memory-ffm` | JDK FFM allocator/runtime、native segment 管理和 stable handle 支撑。 |
| `yierdis-networking/yierdis-networking-resp` | RESP request/reply model、`RespCommandRequest`、`RespExecutionAdapter`、`RespReplyWriter`。 |
| `yierdis-networking/yierdis-networking-netty` | Netty decoder、channel handler、protocol error 和 TCP write-back。 |
| `yierdis-server/yierdis-server-api` | `ExecutionRequest`、`ByteArrayExecutionRequest`、`ReplyWriter` 等执行层公共契约。 |
| `yierdis-server/yierdis-server-core` | engine、execution context 和 server-side command dispatch glue。 |
| `yierdis-server/yierdis-server-executor` | `CommandExecutor`、队列、背压和执行线程模型。 |
| `yierdis-server/yierdis-server-runtime` | `YierdisInstance`、多 DB 装配、runtime config、maxmemory governor 和 maintenance。 |
| `yierdis-server/yierdis-server-main` | `main()`、CLI 参数、server bootstrap、Netty pipeline 装配。 |
| `yierdis-command/yierdis-command-api` | 命令接口、metadata、参数和结果契约。 |
| `yierdis-command/yierdis-command-core` | command registry、command processor、分发和通用校验。 |
| `yierdis-command/yierdis-command-builtin` | Redis 风格内置命令实现。 |
| `yierdis-db/yierdis-db-api` | DB 能力接口、reads/writes view 和数据层契约。 |
| `yierdis-db/yierdis-db-memory` | 单机内存 DB、数据族 ops、TTL、maxmemory、native-backed keyspace/value paths。 |
| `yierdis-cli` | 项目自带 RESP 客户端入口。 |
| `yierdis-benchmark` | 基准压测入口和请求生成。 |
| `yierdis-tests/yierdis-integration-tests`、`yierdis-tests/yierdis-architecture-tests`、`yierdis-db/yierdis-db-testkit` | 端到端行为、架构边界和 DB 级测试支撑。 |

更完整的模块依赖方向看 [`module-architecture.md`](./module-architecture.md)。

## 请求主链概览

一次 RESP 请求的主链可以先按这些术语记：

```text
Netty inbound bytes
  -> RespCommandRequest
  -> RespExecutionAdapter
  -> ByteArrayExecutionRequest
  -> ExecutionRequest
  -> CommandExecutor
  -> engine
  -> command processor
  -> DB
  -> ReplyWriter
  -> RespReplyWriter
  -> Netty write-back
```

这条链的边界含义是：

- `RespCommandRequest` 表示协议层解出的命令请求。
- `RespExecutionAdapter` 把 RESP 请求转成执行层能理解的请求。
- `ByteArrayExecutionRequest` 是以字节参数承载命令的执行请求实现。
- `ExecutionRequest` 是 server/command 层之间的统一请求契约。
- `CommandExecutor` 把请求从 I/O 线程切到执行线程，并施加队列和背压约束。
- engine 持有执行上下文和命令入口，command processor 做命令解析、校验和分发。
- DB 通过 API 暴露读写能力，具体内存实现完成 key 和 value 操作。
- `ReplyWriter` 是命令层写 reply 的抽象，`RespReplyWriter` 把抽象 reply 编成 RESP，最后由 Netty write-back 发回客户端。

逐行追请求时看 [`request-execution-flow.md`](./request-execution-flow.md)。

## 数据和内存主线

数据层不要先想成一个大 `Map`。更准确的模型是：DB owner 负责 keyspace、expires、数据族 ops、memory ledger 和 lifecycle，而 native memory runtime 负责 backing storage、stable handle 和资源边界。

主线可以这样拆：

- `YierdisInstance` 决定逻辑 DB 数量、FFM runtime scope 和 maxmemory scope。
- `YierdisDb` 是单个 DB 的状态 owner 和统一入口。
- keyspace 和 expires 记录 key 到 entry/expire metadata 的映射。
- string、list、hash、set、zset、HLL、bitmap 相关 ops 分别处理数据族语义和内部编码。
- memory API/FFM 层提供 stable native handle，避免 DB 层直接保存可移动的 physical address。
- maxmemory 和 approximate eviction 通过账本、协调器和策略把内存预算反馈到写路径。

DB 内部读 [`db-internals.md`](./db-internals.md)，FFM runtime 和 native-memory-backed 路径读 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## 最先打开的源码文件

第一次读源码可以先打开这些入口，建立从启动到请求再到 DB 的最短路径：

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`

## 接下来读什么

读模块边界和依赖方向看 [`module-architecture.md`](./module-architecture.md)；跟一次请求看 [`request-execution-flow.md`](./request-execution-flow.md)；深入 DB 读 [`db-internals.md`](./db-internals.md)；理解 native-memory runtime 读 [`native-memory-runtime.md`](./native-memory-runtime.md)。
