# 项目总览

本文从代码和运行时边界回答五个问题：Yierdis 现在是什么、它不是 `Map` 也不是完整 Redis、有哪些模块、一次请求经过哪些层、读源码先打开哪些文件。

## 当前定位

Yierdis 当前是 Java 25 + Netty + JDK FFM 实现的 Redis-style 单机内存 KV server。它对外暴露 Redis RESP TCP 协议，RESP2 是默认 wire target，`HELLO 3` 可以协商基础 RESP3 replies；对内把网络、协议、执行、命令、DB、memory runtime 和启动装配拆成独立模块。

读源码时最该记住的定位是：它刻意限定在单机内存边界内，是 Redis 风格系统实现，并不充当 Redis drop-in replacement。代码重点落在展示一次请求如何穿过 RESP/Netty、执行器、命令分发、DB 能力接口和 native-memory-backed 数据结构，而非兼容所有 Redis 行为。

## 为什么不能当成 `Map`

把它当成普通 `Map` 服务不够，是因为 `Map` 只覆盖“按 key 存取 value”这一件事。下面这些关注点各自有独立的代码归属，任何一条都不是 `map.get/put` 能表达的；顺着这张表读源码，就是把“为什么不是 Map”逐条拆开。

| 关注点 | `Map` 解释不了的差异 | 代码归属 |
| --- | --- | --- |
| wire format | 请求/回复是 RESP frame，不是对象调用 | `RespRequestDecoder`、`RespReplyWriter`（`yierdis-networking-resp`） |
| 连接级 session | DB 选择、client name、RESP 版本、事务队列按连接隔离 | `EngineSession` |
| 事务 replay | 排队保存的是 retained request，`EXEC` 时重新 prepare | `TransactionState`、`PreparedExec` |
| TTL | 每个 key 带唯一 `expireAtMillis` deadline，过期清理独立于存取 | `EntryRecord.expireAtMillis`、`YierdisDbKeyLifecycle` |
| maxmemory 写路径约束 | 写之前要按预算预留、记账、可能驱逐 | `YierdisDbMutationExecutor`、memory ledger |
| 语义回复与资源所有权 | 回复不只是字节，可能持有 streaming source / native pin | `RedisReply`、`PreparedCommand` |
| owner thread | DB 只被单一执行线程访问，跨线程访问 fail-fast | `CommandExecutor`、`SerialOwnerExecutor` |
| backpressure | 队列、字节预算、连接 pending 联合限流 | `ExecutorBacklogBudget`、`ExecutorBackpressureController` |
| native handle lifetime | 保存的是 stable handle，不是可移动的 physical address | `memory.foreign` 的 stable handle |
| introspection | `INFO`/`STATS`/`COMMAND` 暴露运行时视图 | `ServerCommandModule`、`NettyServerInfoProvider` |

读代码时应把它看成边界清楚的系统样本：网络、协议、执行、命令、DB、memory runtime 和启动组装各有自己的职责。

## 能力边界

当前已经覆盖的能力包括 Redis 风格数据族、TTL、maxmemory、approximate eviction、minimal transactions、backpressure、observability 和 native-memory-backed paths。

当前没有覆盖的能力包括 AOF/RDB、replication/cluster、Lua、ACL/TLS、PubSub 和 full Redis ecosystem compatibility。遇到客户端兼容、协议协商或 Redis 风格命令，都应理解为“当前子集”，不构成完整 Redis 兼容承诺。

## 技术栈和运行时特征

技术栈主线很短：

- Java 25：语言版本和 `java.lang.foreign` FFM API 的运行前提；`pom.xml` 里 `maven.compiler.release` 设为 25。
- Netty 4.2.18.Final：TCP server、channel pipeline、I/O 线程和 write-back。
- RESP：请求解码、reply 编码和 RESP2/基础 RESP3 wire model。
- Maven multi-module：九个 leaf module 隔离 common、RESP、server API、server、command、DB、CLI、benchmark 和 tests。

运行时主线也很明确：

- Netty I/O 线程收包、解码、提交和写回，不直接修改 DB。
- `CommandExecutor` 承担排队、背压预算和 owner-thread 命令执行。
- `CommandDispatcher` 做请求检查、registry 查找、事务策略和命令分发；`CommandSpec` handler 只解析 `CommandArgs`，返回的 `Function<CommandSession, PreparedCommand>` 再应用于连接 session。
- `PreparedCommand` 暴露回复预留形状，在预留后完成校验和执行，并返回 `CommandResult`；执行器随后通过 `RedisReplyRenderer` 集中渲染语义回复。
- DB 层通过能力接口暴露读写语义，内存实现持有 keyspace、expires、数据族和内存账本。
- JDK FFM runtime 支撑默认 native-memory path，并参与 maxmemory 相关约束。

读源码前先建立三条心智模型：

- 请求是一段链路，并非一次“方法调用”：从 RESP bytes 到 `ExecutionRequest`、`CommandExecutor`、`CommandDispatcher`、command handler、DB、`CommandResult`、`RedisReplyRenderer`，再回到 RESP bytes。
- DB 的生命周期边界由 keyspace、带 TTL deadline 的 entry metadata、value roots、memory ledger 和 native handles 共同维护，而非单张大表。
- native memory 是当前默认数据路径的一部分，不是旁路优化；它也不等于零拷贝，copy 边界要按接口 ownership 和 lifetime 判断。

## 模块总览

| 模块区域 | 主要职责 |
| --- | --- |
| `yierdis-common` | 共享 bytes、memory 和 command 小型值类型与基础契约。 |
| `yierdis-networking-resp` | RESP wire model、客户端 codec、`RespReplyWriter` 和 inline command parsing。 |
| `yierdis-server/yierdis-server-api` | `ExecutionRequest`、`PreparedCommand`、`CommandResult`、语义 `RedisReply`、`RedisReplyRenderer` 和渲染端口 `RedisReplyWriter` 等执行层公共契约。 |
| `yierdis-server/yierdis-server` | 进程入口、embedded runtime、executor、Netty transport、连接状态和最终组装。 |
| `yierdis-command` | 命令契约、registry/dispatcher、事务和 Redis 风格内建命令。 |
| `yierdis-db` | storage API、内存 DB、TTL/maxmemory、FFM backend 和 stable native handle。 |
| `yierdis-cli` | 项目自带 RESP 客户端入口。 |
| `yierdis-benchmark` | 基准压测入口和请求生成。 |
| `yierdis-tests` | 跨模块行为和架构测试；DB 级 helper 归属 `yierdis-db/src/test/java`。 |

更完整的模块依赖方向看 [`module-architecture.md`](./module-architecture.md)。

## 跑起来的最短路径

构建和启动命令都以本仓库的 `README.md` 为准，不是示意。要求 JDK 25 + Maven 3.x。

只构建 server 和 CLI 并打出可执行 fat jar：

```bash
mvn -q -pl yierdis-server/yierdis-server,yierdis-cli -am -DskipTests package
```

启动 server（`java -jar .../yierdis-server-0.1.0-SNAPSHOT.jar --config /path/to/yierdis.conf`，配置文件中 `maxmemoryBytes` 必须显式给出，`0` 表示承认不限制内存；仓库根目录自带一份含全部配置键的 `yierdis.conf` 模板）：

```bash
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar
```

用 `redis-cli` 或项目自带 CLI 验证：

```bash
redis-cli -p 6378 PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar --port 6378 SET a 1
```

启动时 `ServerConfig.fromArgs(...)` 只认 `--config`（缺省读 `./yierdis.conf`），`YierdisServerFileConfig` 应用键值并校验，`YierdisServerBootstrap.start(...)` 完成组装。默认端口 `6378`、`databases=16`、`ioThreads=1`、`maxmemoryBytes=0`。

## 请求主链概览

一次 RESP 请求进入执行器后，命令主链固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

主链外侧是 `Netty inbound bytes -> RespRequestDecoder -> ByteArrayExecutionRequest`，渲染后则经 `RedisReplyWriter / RespReplyWriter -> Netty write-back` 回到客户端。这些边界的含义是：

- `RespRequestDecoder` 在分配前执行 ingress admission，并直接构造执行请求。
- `ByteArrayExecutionRequest` 是网络主链和 heap 输入共用的实现。decoder 用 `takeOwnership(...)` 移交不可变 argv 与 memory lease，`retain()` 共享 argv 并增加 lease 引用，`copyOf(...)` 才创建独立快照。
- `ExecutionRequest` 是 server/command 层之间的统一请求契约。
- `CommandExecutor` 把请求从 I/O 线程切到执行线程，施加队列与背压约束。
- `CommandDispatcher` 完成命令名、null、arity 和事务策略检查；普通命令依次解析 `CommandArgs` 并按 `CommandSession` 准备为 `PreparedCommand`。
- 事务中的 queueable 命令只调用 handler 解析做 preflight，不提前执行 session/DB 准备；排队动作在回复预留成功后保留请求。`EXEC` 通过 dispatcher replay 重新准备子命令，同时关闭子 `PreparedCommand` 和 retained request。
- 执行器按 `PreparedCommand.reservationShape()` 预留容量，校验仍有效后直接传入 `CommandSession` 执行。准备和执行阶段通过 DB API 完成真实读写。
- 命令返回 `CommandResult`，其中 `RedisReply` 描述语义回复；bulk、byte sequence 和 byte map 可以持有语义流式 source/emitter，相关 owner 保持到 renderer 消费完成后才关闭。
- `QUIT` 不接触 writer，而是通过 `CommandResult.closeAfterReply(...)` 携带关闭意图；执行器在结果渲染并发布后关闭连接。
- `RedisReplyWriter` 只是 `RedisReplyRenderer` 面向 RESP 编码器的输出端口。`RespReplyWriter` 按 `ReplyPlan` 在 prepare/预留时刻捕获的 RESP 版本编码（同一版本同时决定容量预留与写出字节），最后由 Netty write-back 发回客户端。
- `EngineSession` 只拥有每条连接的 DB 选择、client name、RESP 版本和事务队列等 session 状态，不参与命令解析、分发、执行或渲染。

逐行追请求时看 [`request-execution-flow.md`](./request-execution-flow.md)。

## 数据和内存主线

数据层不要先想成一个大 `Map`。更准确的模型是：keyspace、expires、数据族 ops、memory ledger 和 lifecycle 由 DB owner 掌管，backing storage、stable handle 和资源边界则由 native memory runtime 处理。

主线可以这样拆：

- `YierdisInstance` 决定逻辑 DB 数量和 maxmemory scope；每个 DB backend 拥有自己的 FFM runtime。
- `YierdisDb` 是单个 DB 的状态 owner 和统一入口。
- keyspace 把 key 映射到 entry，`EntryRecord.expireAtMillis` 保存唯一 TTL deadline。
- string、list、hash、set、zset、HLL 分别由 `StringOps`、`ListOps`、`HashOps`、`SetOps`、`ZSetOps`、`HllOps` 处理。bitmap 的 `setBit`、`getBit`、`bitcount` 在 `StringOps` 上。
- memory API/FFM 层提供 stable native handle，避免 DB 层直接保存可移动的 physical address。
- maxmemory 和 approximate eviction 通过账本、协调器和策略把内存预算反馈到写路径。

DB 内部读 [`db-internals.md`](./db-internals.md)，FFM runtime 和 native-memory-backed 路径读 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## 最先打开的源码文件

第一次读源码先打开这 12 个入口，建立从启动到请求再到 DB 的最短路径。每个文件一句话职责：

| 文件 | 职责 |
| --- | --- |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServer.java` | `main`：解析参数、注册 shutdown hook、阻塞在 `awaitClose()`。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java` | composition root：建 `YierdisInstance`、建 dispatcher/executor、装 Netty groups 并按序关闭。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java` | 每连接 pipeline 装配与连接态绑定。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java` | RESP array/inline 解码，分配前做 ingress admission。 |
| `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java` | heap-backed 不可变 `ExecutionRequest` 实现与 argv/lease ownership。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java` | 提交准入、owner-thread 队列、drain、优雅关闭。 |
| `yierdis-command/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java` | 命令名归一、查表、arity、事务策略、parse 与 prepare。 |
| `yierdis-command/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java` | 命令名到 `CommandSpec` 的映射，注册后 seal。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java` | 每连接 session 状态 owner（DB index、client name、RESP 版本、事务队列）。 |
| `yierdis-command/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java` | string/bitmap 命令注册与 SET/GET 等 handler 实现。 |
| `yierdis-db/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java` | 单个 DB 的状态 owner 和统一入口。 |
| `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java` | 可嵌入、Netty-free 的 instance API：装配多 DB、路由与资源生命周期。 |

## 接下来读什么

读模块边界和依赖方向看 [`module-architecture.md`](./module-architecture.md)；跟一次请求看 [`request-execution-flow.md`](./request-execution-flow.md)；看 Netty 适配与有界写回看 [`netty-adapter-design.md`](./netty-adapter-design.md)；深入 DB 读 [`db-internals.md`](./db-internals.md)；理解 native-memory runtime 读 [`native-memory-runtime.md`](./native-memory-runtime.md)。