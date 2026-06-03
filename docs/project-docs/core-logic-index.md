# 核心源码索引

本文是源码定位索引，不再重复完整架构解释。每个条目说明职责、入口、边界和应该继续阅读的专题文档。

## 怎么使用这份索引

先用本页判断类属于哪条车道，再打开专题文档读完整链路：

- 请求主链：[`request-execution-flow.md`](./request-execution-flow.md)
- 模块边界：[`module-architecture.md`](./module-architecture.md)
- 命令和数据模型：[`commands-and-data-model.md`](./commands-and-data-model.md)
- DB 内核：[`db-internals.md`](./db-internals.md)
- executor：[`executor-and-backpressure.md`](./executor-and-backpressure.md)
- 代理和变更事件：[`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)
- native memory：[`native-memory-runtime.md`](./native-memory-runtime.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`offheap-copy-behavior.md`](./offheap-copy-behavior.md)
- 测试入口：[`testing-and-debugging.md`](./testing-and-debugging.md)
- 维护覆盖矩阵：[`code-logic-coverage.md`](./code-logic-coverage.md)

## Server 启动与组装

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`YierdisServer`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java) | 进程入口，解析 CLI，检查 FFM，可预期错误转退出码 | `main(String[] args)` | [`main-path-walkthrough.md`](./main-path-walkthrough.md) |
| [`ServerConfig`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java) | 把 CLI 参数变成 bootstrap config | `fromArgs(String[])` | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`CommandExecutorConfigs`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java) | runtime config 到 executor config 的映射 | `from(...)` | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`YierdisServerBootstrap`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java) | composition root，选择默认 `YierdisDbEngineFactory` / memory runtime，并组装 `YierdisInstance`、`DefaultYierdisEngine`、`CommandExecutor`、Netty pipeline | `start(...)`, `startInternal()`, `close()` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`YierdisServerChannelInitializer`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java) | 组装每条连接的 Netty handler 链 | `initChannel(...)` | [`protocol-reference.md`](./protocol-reference.md) |

边界：`yierdis-server-main` 可以接触 Netty、runtime、engine、executor 和 server-only command；不应该承载普通 Redis 命令语义。

## Runtime 和多 DB

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`YierdisInstance`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java) | 多 DB 生命周期、owner thread 绑定、资源关闭；strict `create(config)` 要求已注入 `DbEngineFactory` | `create(...)`, `createWithDefaults(...)`, `engine(int)`, `engines()`, `bindToCurrentThread()`, `close()` | [`db-internals.md`](./db-internals.md) |
| [`YierdisInstanceConfig`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java) | runtime 配置对象，承载外部注入的 DB factory 和 factory-owned resource | builder methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisInstanceRuntimeAccess`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java) | executor 线程绑定和运行期访问面 | `bindToCurrentThread()` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`YierdisInstanceMaintenance`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceMaintenance.java) | maintenance tick，驱动过期清理和 defrag | tick methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisInstanceObservability`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java) | INFO / STATS 使用的实例观测快照 | snapshot methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |

边界：runtime 暴露 `DbEngine` 能力视图，不把 `YierdisDb` 具体类泄漏给 command/server。

## Executor 和背压

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`CommandExecutor`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java) | owner thread 执行器，管理提交、drain、统计、关闭 | `start()`, `submit(...)`, `close()` | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorSubmitter`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java) | 请求入队、bytes/backlog budget、连接背压 | submit methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorDrainLoop`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java) | GLOBAL / FAIR 调度和 drain budget | drain methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| [`CommandExecutorExecutionSupport`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java) | 执行前后上下文、reply writer、IO 写回 | execute methods | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`ExecutionConnectionContext`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java) | 单连接执行状态、背压恢复和关闭保护 | queue / drain methods | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |

边界：Netty I/O 线程只提交请求，不访问 DB；DB 访问发生在 executor owner thread。

## Engine、session 和事务

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`DefaultYierdisEngine`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java) | 将 `ExecutionRequest` 交给 command processor，维护 engine/session 边界 | `execute(...)` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`CommandSessionCapabilities`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java) | 把通用 `Session` 收窄成命令需要的 DB index、client metadata、transaction、stats 和 protocol 能力 | `from(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`ServerSession`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java) | 连接状态：DB index、RESP version、transaction state、client metadata | session accessors | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| `TransactionState` | `MULTI/EXEC/DISCARD` 队列状态和 abort 状态 | queue/replay methods | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| `ExecutionRecord` | 记录可 replay 的命令快照，也用于 change event | constructor / accessors | [`glossary.md`](./glossary.md) |

边界：事务排队前要复用 `CommandSpec` 校验；真正执行时仍走同一 command handler。

## Command 注册、解析和分发

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`CommandSpec`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java) | 命令元数据、arity、handler、MULTI 限制 | factory / accessor methods | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`CommandRegistry`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java) | command name 到 `CommandSpec` 的注册表 | `register(...)`, `spec(...)`, `upperNamesSorted()` | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`YierdisFastCommandProcessor`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java) | empty/unknown command、命令查表、解析执行主流程；事务入队、异常翻译和 change event gate 委托给 command-kernel 小组件 | `execute(...)` | [`request-execution-flow.md`](./request-execution-flow.md) |
| [`DefaultCommandModules`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java) | transport-neutral 默认命令模块集合 | `create(...)` | [`module-architecture.md`](./module-architecture.md) |
| [`CommandSupport`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java) | 参数读取、DB routing、常用 reply/error helper | helper methods | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`YierdisDbRouter`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/YierdisDbRouter.java) | 根据当前 session DB index 选择命令本次访问的 `DbEngine` | `dbFor(...)`, `databases()` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`ServerInfoProvider`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java) | command 层 Netty-free 的 INFO / STATS / MEMORY STATS 观测代理 | `info(...)`, `stats(...)`, `memoryStats(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |

边界：command 层描述 Redis 语义和回包，不直接操作 internal root/value。

## 代理、桥接和变更事件

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`CommandChangeEmitter`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandChangeEmitter.java) | 包住 command handler 执行，清理/读取 mutation outcome，只在真实变化后通知 observer | `execute(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`CommandChangeObserver`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandChangeObserver.java) | command-core 的变更观察接口，不依赖 runtime sink 或 DB change scope | `onCommandChange(...)`, `observeExecution(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`RuntimeChangeSinkCommandChangeObserver`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RuntimeChangeSinkCommandChangeObserver.java) | server-main adapter，把 runtime `YierdisChangeSink` 接到 command observer 和 DB change scope | `fromSink(...)`, `observeExecution(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`YierdisChangeEventBridge`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEventBridge.java) | 把 DB lifecycle 的 `DbChange` 转成 runtime `YierdisChangeEvent` | `forSink(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |
| [`DbChangeContext`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChangeContext.java) | owner-thread scoped DB synthetic change emitter，用于 expire / eviction 等内部删除 | `open(...)`, `emit(...)` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) |

边界：当前 change event 是最小可重放事件契约，不是完整 AOF、复制协议或持久化保证。

## DB API 和 DB 内核

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`DbEngine`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngine.java) | DB 能力聚合接口 | `reads()`, `writes()`, `expiration()`, `memory()`, `lifecycle()` | [`db-internals.md`](./db-internals.md) |
| [`YierdisDbEngineFactory`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java) | 创建 memory DB engine，连接 FFM、keyspace、TTL、ledger | factory methods | [`native-memory-runtime.md`](./native-memory-runtime.md) |
| [`YierdisDb`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java) | 单 DB 内核和 capability view 实现 | ops accessors / lifecycle methods | [`db-internals.md`](./db-internals.md) |
| [`YierdisDbMutationExecutor`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java) | mutation plan、memory reservation、rollback/no-op accounting | mutation methods | [`db-internals.md`](./db-internals.md) |
| [`YierdisDbMemoryReporter`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java) | `MEMORY` / `INFO memory` 数据来源 | memory methods | [`configuration-and-operations.md`](./configuration-and-operations.md) |
| [`YierdisDbIntrospection`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java) | `OBJECT ENCODING` 和 snapshot 读取 native metadata | introspection methods | [`db-internals.md`](./db-internals.md) |

边界：DB API 是 command 层的依赖边界；internal 包里的 entry/keyspace/value/ledger 不应被 command 层直接引用。

## RESP 和回包

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`ExecutionRequest`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRequest.java) | command 层看到的 argv bytes 视图 | `argc()`, `len(int)`, `copyToByteArray(...)`, `toByteArray(int)`, `readOnlyByteArray(int)` | [`protocol-reference.md`](./protocol-reference.md) |
| [`ByteArrayExecutionRequest`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java) | heap byte[] backed request 实现，常用于测试和适配 | constructors / factory methods | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) |
| [`RedisReplyWriter`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java) / [`ReplyWriter`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java) | command 层唯一 Redis reply 语义接口；`ReplyWriter` 是兼容别名 | `simpleString`, `bulkString`, `integer`, `arrayHeader`, `mapHeader`, `error` | [`commands-and-data-model.md`](./commands-and-data-model.md) |
| [`RespReplyWriter`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java) | 将 `ReplyWriter` 调用编码成 RESP2/RESP3 bytes | reply methods | [`protocol-reference.md`](./protocol-reference.md) |
| [`RespRequestDecoder`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java) | Netty 入站 RESP decoder | `decode(...)` | [`protocol-reference.md`](./protocol-reference.md) |

边界：协议层只处理 wire format；命令 handler 不拼 RESP bytes。

## Bytes 和 native memory

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| `BytesView` / `BytesSlice` | 跨 heap/off-heap 的 bytes 读取抽象 | view/slice methods | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) |
| [`NativeHandle`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java) | native object stable handle 的 ABI 编码 | encode/decode methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`YierdisNativeObjectTable`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java) | object metadata、generation、pin、quarantine | allocate/free/resolve methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`YierdisStableNativeAllocator`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java) | stable allocator、realloc、epoch、active defrag | allocate/realloc/defrag methods | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| [`EntryHandle`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java) / [`ValueHandle`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java) | DB 层对 native handle 的类型化包装 | factory/accessor methods | [`native-memory-runtime.md`](./native-memory-runtime.md) |

边界：native allocator 解决稳定地址和生命周期；DB handle wrapper 解决 DB domain/kind 语义。

## CLI 和 benchmark

| 类/模块 | 职责 | 关键入口 | 继续阅读 |
| --- | --- | --- | --- |
| [`InlineCommandParser`](../../yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java) | CLI inline 命令解析 | parse methods | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| `YierdisClient` | RESP client、请求发送、回包读取 | connect / execute methods | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`YierdisBench`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java) | benchmark 入口、server 启动/复用、workload 执行 | `main(...)` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |
| [`YierdisBenchArgs`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java) | benchmark 参数解析 | parse methods | [`client-and-bench-internals.md`](./client-and-bench-internals.md) |

边界：CLI/bench 是外部使用者，不应该绕过 RESP 或直接调用 DB。

## 测试和架构护栏

| 测试 | 保护什么 | 继续阅读 |
| --- | --- | --- |
| `ArchitectureDependencyRuleTest` | Maven/module 依赖方向 | [`module-architecture.md`](./module-architecture.md) |
| `RespBoundaryGuardTest` | RESP DTO 不越过协议边界 | [`protocol-reference.md`](./protocol-reference.md) |
| `YierdisDbArchitectureGuardTest` | DB internal 边界和 owner thread 假设 | [`db-internals.md`](./db-internals.md) |

## 边界清单

- Netty I/O 线程不访问 DB。
- `ExecutionRequest` 是 command 层输入，RESP DTO 不进入 command 层。
- command 层通过 `DbEngine` / `DbReads` / `DbWrites` 访问 DB，不依赖 `YierdisDb` internal。
- 回包统一走 `ReplyWriter`，handler 不拼 RESP bytes。
- DB 写路径统一经过 mutation executor 和 memory ledger。
- TTL、key lifecycle、maxmemory 记账要和真实变更一起提交或回滚。
- native handle 必须经过 domain/kind/generation 校验，不能把 raw long 当普通指针传递。
- 新 command、option、DB API、native/internal 结构必须同步补真实测试，并更新受影响的专题文档。
