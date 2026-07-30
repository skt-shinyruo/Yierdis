# 核心源码索引

本文是源码定位索引，不再重复完整架构解释。每个条目说明职责、入口、边界和应该继续阅读的专题文档。

## 怎么使用这份索引

先用本页判断类属于哪条车道，再打开专题文档读完整链路：

- 请求主链：[`request-execution-flow.md`](./request-execution-flow.md)
- 模块边界：[`module-architecture.md`](./module-architecture.md)
- 命令和数据模型：[`commands-and-data-model.md`](./commands-and-data-model.md)
- 命令解析与分发：[`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)
- 事务与 replay：[`transaction-and-replay.md`](./transaction-and-replay.md)
- DB 内核：[`db-internals.md`](./db-internals.md)
- TTL / 过期：[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)
- maxmemory / 淘汰：[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)
- executor：[`executor-and-backpressure.md`](./executor-and-backpressure.md)
- 生产 hardening 操作和验收：[`production-hardening-operations.md`](./production-hardening-operations.md)
- 代理和变更事件：[`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)
- native memory：[`native-memory-runtime.md`](./native-memory-runtime.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`offheap-copy-behavior.md`](./offheap-copy-behavior.md)
- 测试入口：[`testing-and-debugging.md`](./testing-and-debugging.md)
- 维护覆盖矩阵：[`code-logic-coverage.md`](./code-logic-coverage.md)

## Server 启动与组装

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`YierdisServer`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java) | 进程入口，解析 CLI，检查 FFM，可预期错误转退出码 | `main(String[] args)` | [`main-path-walkthrough.md`](./main-path-walkthrough.md) |
| [`ServerConfig`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java) | 把 CLI 参数变成 bootstrap config | `fromArgs(String[])` | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisServerRuntimeConfig`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java) | server 领域配置校验，并直接生成 executor config | record constructor, `executorConfig()` | [`configuration-and-operations.md`](./configuration-and-operations.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`YierdisServerBootstrap`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java) | composition root，选择默认 `YierdisDbEngineFactory` / memory runtime，并组装 `YierdisInstance`、`CommandDispatcher`、`CommandExecutor` 和 Netty pipeline | `start(...)`, `startInternal()`, `close()` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`YierdisServerChannelInitializer`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java) | 组装每条连接的 Netty handler 链 | `initChannel(...)` | [`protocol-reference.md`](./protocol-reference.md) |
| [`NettyServerInfoProvider`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java) | 为 INFO / STATS / health 一次采集请求级公共统计快照并按 section 输出 | `prepareInfo(...)`, `prepareStats(...)`, `serverStatsSnapshot(...)` | [`configuration-and-operations.md`](./configuration-and-operations.md), [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| `OutboundMemoryBudget` / `ConnectionReplySequencer` / `BoundedChunkedReplySink` | 全局、连接和单回复容量账户；receive-order slot；固定容量 chunk 写回 | reserve/ready/drain methods | [`production-hardening-operations.md`](./production-hardening-operations.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`NettyExecutionRequestIngress`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionRequestIngress.java) | I/O 边界上的 reply slot 对齐、executor admission、容量等待和异常 fallback | `channelRead(...)`, `submitOrDefer(...)`, `exceptionCaught(...)` | [`request-execution-flow.md`](./request-execution-flow.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) |

边界：`yierdis-server-main` 可以接触 Netty、runtime、connection session、executor 和 server-only command；不应该承载普通 Redis 命令语义。

## Runtime 和多 DB

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`YierdisInstance`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java) | 多 DB 生命周期、owner thread 绑定、资源关闭；strict `create(config)` 要求已注入 `DbEngineFactory` 或 `EngineFactoryBinding` | `create(...)`, `engine(int)`, `engines()`, `bindToCurrentThread()`, `close()` | [`db-internals.md`](./db-internals.md) |
| [`YierdisInstanceConfig`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java) | runtime 配置对象，承载外部注入的 DB factory 和 factory-owned resource | builder methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisInstanceRuntimeAccess`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java) | executor 线程绑定和运行期访问面 | `bindToCurrentThread()` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`YierdisInstanceMaintenance`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceMaintenance.java) | maintenance tick，驱动过期清理、defrag 和 maxmemory maintenance | `maintenanceTick()` | [`configuration-and-operations.md`](./configuration-and-operations.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) |
| [`YierdisGlobalMaxmemoryGovernor`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernor.java) | global scope 下跨 DB 协调 cleanup、victim 选择和淘汰 | `prepareWrite(...)`, `enforceMaintenance()`, victim selection helpers | [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisInstanceObservability`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java) | INFO / STATS 使用的实例观测快照 | snapshot methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`CommitStream`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/CommitStream.java) | 固定容量 DB commit ring，异步向 runtime sink 交付已提交记录 | `reserve(...)`, `publish(...)`, `shutdownGracefully(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md), [`production-hardening-operations.md`](./production-hardening-operations.md) |

边界：runtime 暴露 `DbEngine` 能力视图，不把 `YierdisDb` 具体类泄漏给 command/server。

## Executor 和背压

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`CommandExecutor`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java) | owner thread 执行器，管理 admission、drain、统计、关闭 | `start()`, `tryAcquire(...)`, `onAdmissionAvailable(...)`, `close()` | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorSubmitter`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java) | 请求入队、bytes/backlog budget、连接背压 | submit methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorDrainLoop`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java) | GLOBAL / FAIR 调度和 drain budget | drain methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorExecutionSupport`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java) | 在 owner thread 驱动 prepare、reply reserve、validate、execute，并把 `CommandResult` 交给 `RedisReplyRenderer` | execute methods | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`ExecutionConnectionContext`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java) | 单连接执行状态、背压恢复和关闭保护 | queue / drain methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`NettyExecutionConnection`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java) | Channel 级 connection root，绑定 `Channel`、`EngineSession` 和 `ExecutionConnectionContext` | `getOrCreate(...)`, `markClosing()`, `connectionId()` | [`request-execution-flow.md`](./request-execution-flow.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) |

边界：Netty I/O 线程只提交请求，不访问 DB；DB 访问发生在 executor owner thread。

## Session 和事务

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`EngineSession`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java) | 仅作为连接级 session state owner，持有 DB index、RESP version、transaction queue 和 client metadata，不承担命令转发 | session accessors, `DefaultTransactionState` | [`transaction-and-replay.md`](./transaction-and-replay.md) |
| [`CommandSession`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSession.java) | 聚合命令边界需要的 DB index、client metadata、transaction、stats 和 protocol 能力 | inherited capability methods | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`TransactionState`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java) | `MULTI/EXEC/DISCARD` 队列状态、abort 和 replay contract | `begin()`, `discard()`, `tryEnqueue(...)`, `drain()` | [`transaction-and-replay.md`](./transaction-and-replay.md) |
| [`ExecutionRecord`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRecord.java) | dbIndex + command record view 的 change-event API 载体；支持 copied 与 callback-scoped borrowed 两种入口 | constructor, `borrowed(...)`, accessors | [`transaction-and-replay.md`](./transaction-and-replay.md), [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md), [`glossary.md`](./glossary.md) |

边界：事务排队前由 `CommandDispatcher` 只运行 `CommandSpec.handler().parse(CommandArgs)` 做 preflight，
不会调用 `CommandInvocation.prepare(session)`；`EXEC` 由 `TransactionCommands` 通过同一 dispatcher replay，
并拥有队列中 retained request 和 child `PreparedCommand` 的释放责任。

## Command 注册、解析和分发

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`CommandSpec`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java) | 把 `CommandSyntax` 和 session-free `CommandHandler` 组成唯一注册契约 | record accessors | [`commands-and-data-model.md`](./commands-and-data-model.md), [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) |
| [`CommandRegistry`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java) | command name 到 `CommandSpec` 的封闭 JDK map 注册表 | `register(...)`, `seal()`, `specByUpperName(...)`, `upperNamesSorted()` | [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) |
| [`CommandArgs`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArgs.java) | 唯一 argv、ASCII option 和整数读取面，直接包装 `ExecutionRequest` | `argc()`, `bytes(...)`, `is(...)`, `longAt(...)` | [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md), [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) |
| [`CommandInvocation`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandInvocation.java) | parse 后、session-aware prepare 前的命令调用 | `prepare(session)` | [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) |
| [`CommandDispatcher`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java) | empty/unknown command、arity、handler parse、事务 preflight/排队、replay 和预期异常翻译的 command-kernel 入口 | `prepare(session, request)`, `prepareReplay(...)` | [`request-execution-flow.md`](./request-execution-flow.md), [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md), [`transaction-and-replay.md`](./transaction-and-replay.md) |
| [`DefaultCommandModules`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java) | transport-neutral 默认命令模块集合 | `create(...)` | [`module-architecture.md`](./module-architecture.md) |
| [`CommandSupport`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java) | DB routing、server info / slow-command provider 和 prepared mutation 共享逻辑 | helper methods | [`commands-and-data-model.md`](./commands-and-data-model.md), [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) |
| [`YierdisDbRouter`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/YierdisDbRouter.java) | 根据当前 session DB index 选择命令本次访问的 `DbEngine` | `dbFor(...)`, `databases()` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`ServerInfoProvider`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java) | command 层 Netty-free 的 INFO / STATS / MEMORY STATS 观测代理 | `info(...)`, `stats(...)`, `memoryStats(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |

最终主线固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

边界：handler parse 不访问 DB、provider 或 session；command 层返回 Redis 回复语义，不直接操作 internal
root/value，也不调用 `RedisReplyWriter`。

## 代理和 DB 提交事件

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`MutationContext`](../../yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/MutationContext.java) | 命令边界内借用的 mutation record，关闭后释放 argv 引用 | `of(...)`, `retainCommandRecord()`, `close()` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`DbCommitPublisher`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitPublisher.java) | DB API 的有界提交发布端口；可见性前预留、提交后发布 | `reserve(...)`, `publish(...)`, `failAfterCommit(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`DbCommitEvent`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitEvent.java) | 已提交记录的 callback-scoped DB 视图 | event accessors | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`YierdisChangeEvent`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java) | runtime sink 接收的 borrowed event，包含 sequence、kind、record 和提交元数据 | `borrowed(...)`, `close()` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |

边界：当前 change event 是最小可重放事件契约，不是完整 AOF、复制协议或持久化保证。

## DB API 和 DB 内核

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`DbEngine`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngine.java) | DB 能力聚合接口 | `reads()`, `writes()`, `expiration()`, `memory()`, `lifecycle()` | [`db-internals.md`](./db-internals.md) |
| [`YierdisDbEngineFactory`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java) | 创建 memory DB engine，连接 FFM、keyspace、TTL、ledger | factory methods | [`native-memory-runtime.md`](./native-memory-runtime.md) |
| [`YierdisDb`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java) | 单 DB 内核和 capability view 实现 | ops accessors / lifecycle methods | [`db-internals.md`](./db-internals.md) |
| [`YierdisDbKeyLifecycle`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java) | 协调 key directory、entry table、payload root、TTL metadata 和 synthetic delete | `liveEntryRecord(...)`, `computeWithHandleResult(...)`, `removeIfExpired(...)`, `removeEntry(...)` | [`db-internals.md`](./db-internals.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) |
| [`EntryMutationEntries`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/EntryMutationEntries.java) | collection/string/HLL 写路径共享的 current entry 查询、新 entry staging 和失败清理 | `current(...)`, `stage(...)`, `upsert(...)`, `abortStaged(...)` | [`db-internals.md`](./db-internals.md) |
| [`PreparedEntryMutation`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedEntryMutation.java) | entry insert/replace/delete/no-op 的提交、TTL 顺序、abort 和 superseded value 释放 | `unchanged(...)`, `insert(...)`, `replace(...)`, `delete(...)`, lifecycle hooks | [`db-internals.md`](./db-internals.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) |
| [`YierdisDbMutationExecutor`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java) | prepared mutation、memory reservation、rollback/no-op accounting 和 DB commit publication | `execute(plan)` | [`db-internals.md`](./db-internals.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`YierdisDbExpirationSupport`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java) | 按 sample/time budget 做 expire cleanup | `cleanupExpired(...)` | [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) |
| [`YierdisDbMaxmemorySupport`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java) | 单 DB maxmemory participant，负责 victim 选择、evict 和 synthetic `EVICTED` delete | `evictUntilUnder(...)`, `sampleCandidate(...)`, `scanBestCandidate(...)`, `evict(...)` | [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) |
| [`YierdisDbMemoryReporter`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java) | `MEMORY` / `INFO memory` 数据来源 | memory methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisDbIntrospection`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java) | `OBJECT ENCODING` 和 snapshot 读取 native metadata | introspection methods | [`db-internals.md`](./db-internals.md) |

边界：DB API 是 command 层的依赖边界；internal 包里的 entry/keyspace/value/ledger 不应被 command 层直接引用。

## RESP 和回包

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`ExecutionRequest`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRequest.java) | command 层看到的 argv bytes 视图 | `argc()`, `len(int)`, `copyToByteArray(...)`, `toByteArray(int)`, `readOnlyByteArray(int)` | [`protocol-reference.md`](./protocol-reference.md) |
| [`ByteArrayExecutionRequest`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java) | heap byte[] backed request 实现，常用于测试和适配 | constructors / factory methods | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) |
| [`PreparedCommand`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java) | 预留前准备完成、可校验并执行一次的资源 owner | `reservationShape()`, `validateBeforeExecute()`, `execute(...)`, `close()` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`CommandResult`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandResult.java) | 命令执行结果，携带 transport-neutral `RedisReply` 和 result-based `closeAfterReply` | `reply(...)`, `closeAfterReply(...)` | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`RedisReply`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReply.java) | 回复语义树；bulk / byte sequence / byte map 可持有延迟 emitter，在 owner 存活期内流式消费 | sealed reply variants, `shape()` | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`RedisReplyRenderer`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java) | executor 的集中语义回复 renderer | `render(reply, writer)` | [`request-execution-flow.md`](./request-execution-flow.md), [`protocol-reference.md`](./protocol-reference.md) |
| [`RedisReplyWriter`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java) | `RedisReplyRenderer` 面向 RESP 编码器的写出端口；命令层不使用 | reply writer methods | [`protocol-reference.md`](./protocol-reference.md) |
| [`RespReplyWriter`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java) | 将 `RedisReplyWriter` 调用编码成 RESP2/RESP3 bytes | reply methods | [`protocol-reference.md`](./protocol-reference.md) |
| [`RespRequestDecoder`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java) | Netty 入站 RESP decoder | `decode(...)` | [`protocol-reference.md`](./protocol-reference.md) |

边界：命令 handler 只构造回复语义；executor 在关闭 `PreparedCommand` 前集中渲染，协议层只处理 wire
format。`QUIT` 通过 `CommandResult.closeAfterReply(...)` 传递关闭意图，不依赖 writer 的隐藏状态。

## Bytes 和 native memory

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| `BytesView` / `BytesSlice` / `BytesSink` | 跨 heap/off-heap 的 bytes 读取和流式写出抽象 | view/slice/sink methods | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md), [`netty-adapter-design.md`](./netty-adapter-design.md) |
| [`NativeHandle`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java) | native object stable handle 的 ABI 编码 | encode/decode methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`YierdisNativeObjectTable`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java) | object metadata、generation、pin、quarantine | allocate/free/resolve methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`YierdisStableNativeAllocator`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java) | stable allocator、realloc、epoch、active defrag | allocate/realloc/defrag methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`EntryHandle`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java) / [`ValueHandle`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java) | DB 层对 native handle 的类型化包装 | factory/accessor methods | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |

边界：native allocator 解决稳定地址和生命周期；DB handle wrapper 解决 DB domain/kind 语义。

## CLI 和 benchmark

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`InlineCommandParser`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/InlineCommandParser.java) | 共享 inline 命令解析 | parse methods | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| `YierdisClient` | RESP client、请求发送、回包读取 | connect / execute methods | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`YierdisBench`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java) | RESP 根命令与 storage 子命令的薄 launcher | `main(...)`, `commandLine()` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`RedisBenchmarkOptions`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptions.java) | endpoint、workload 和输出 CLI options | option fields / `toConfig(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`StorageBenchmarkRunner`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/storage/StorageBenchmarkRunner.java) | 单 owner DB SET 吞吐与 heap/native footprint 测量 | `run(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`RedisBenchmarkCatalog`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalog.java) | canonical 21-row catalog、selection 和 support declarations | `allCases()`, `select(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`NioBenchmarkRunner`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunner.java) | 单 `Selector` 的 non-blocking load runner | `execute(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`BenchmarkOutputRenderer`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRenderer.java) | human、quiet、Redis-style CSV rendering | `render(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |

边界：CLI 和 RESP benchmark 是外部使用者，不应该绕过 RESP；显式 storage benchmark 是隔离的进程内 DB 诊断入口，其结果不代表端到端 request path。

## 测试和架构护栏

| 测试 | 保护什么 | 继续阅读 |
| --- | --- | --- |
| `ArchitectureDependencyRuleTest` | Maven/module 依赖方向 | [`module-architecture.md`](./module-architecture.md) |
| `RespBoundaryGuardTest` | RESP DTO 不越过协议边界 | [`protocol-reference.md`](./protocol-reference.md) |
| `YierdisDbArchitectureGuardTest` | DB internal 边界和 owner thread 假设 | [`db-internals.md`](./db-internals.md) |
| `ArchitectureBoundaryTest` | 生产 hardening 文档、回复写回 owner 和模块边界 | [`production-hardening-operations.md`](./production-hardening-operations.md) |

## 边界清单

- Netty I/O 线程不访问 DB。
- `ExecutionRequest` 是 command 层输入，RESP DTO 不进入 command 层。
- command 层通过 `DbEngine` / `DbReads` / `DbWrites` 访问 DB，不依赖 `YierdisDb` internal。
- handler 返回 `CommandResult`；只有 `RedisReplyRenderer` 通过 `RedisReplyWriter` 写 RESP 语义，handler 不拼 RESP bytes。
- DB 写路径统一经过 mutation executor 和 memory ledger。
- TTL、key lifecycle、maxmemory 记账要和真实变更一起提交或回滚。
- native handle 必须经过 domain/kind/generation 校验，不能把 raw long 当普通指针传递。
- 新 command、option、DB API、native/internal 结构必须同步补真实测试，并更新受影响的专题文档。
