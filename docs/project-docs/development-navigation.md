# 开发导航

本文按常见改动类型回答一个实际问题：我要改某类需求时，应该先打开哪些文件，沿哪条链继续追。

先把两份导航放在手边：测试选择看 [`testing-and-debugging.md`](./testing-and-debugging.md)，源码职责看 [`core-logic-index.md`](./core-logic-index.md)。

## 工作规则

1. 先定改动边界。协议、command、DB、executor、runtime、native memory 不要混在一次小改里处理。
2. 先找最近的测试，再改实现。命令语义优先看 integration tests，DB 语义优先看 direct ops tests，native memory 优先看 internal contract tests。
3. 命令层不要绕过 `DbEngine` / `DbReads` / `DbWrites` 直接依赖 `YierdisDb`。
4. RESP DTO 不进入 command 层；进入 command 层前必须变成 `ExecutionRequest`。
5. 写路径必须经过 mutation executor、memory ledger、TTL/key lifecycle，不要直接改 root/value 结构。
6. 新命令、新 DB API、新 native/internal 结构要同步补真实测试，并更新受影响的专题文档。

命令主线固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

## 改协议

先打开：

- [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)
- [`RetainedRespExecutionRequest.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java)
- [`InboundMemoryBudget.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundMemoryBudget.java)
- [`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java)
- [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)

继续追：

- 请求边界看 [`protocol-reference.md`](./protocol-reference.md) 和 [`request-execution-flow.md`](./request-execution-flow.md)。
- bytes 零拷贝和 materialize 边界看 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。
- Netty 适配层设计和 fast-path 看 [`netty-adapter-design.md`](./netty-adapter-design.md)。
- 如果是 `HELLO 2/3` 或回包类型差异，继续看 `RespReplyWriterFactory`、`CommandSession` 的协议版本方法、作为连接 session owner 的 `EngineSession` 和 `RespHandshakeIntegrationTest`。

测试优先级：

- `RespRequestDecoderTest`
- `RespIngressAdmissionTest`
- `RespReplyWriterTest`
- `RespProtocolIntegrationTest`
- `RespProtocolErrorIntegrationTest`
- `RespHandshakeIntegrationTest`

## 新增或修改命令

先打开：

- [`CommandSpec.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java)
- [`CommandSyntax.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSyntax.java)
- [`CommandArgs.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArgs.java)
- [`CommandInvocation.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandInvocation.java)
- [`PreparedCommand.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java)
- [`CommandRegistry.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java)
- [`CommandDispatcher.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java)
- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)
- 对应家族的 `*Commands.java`
- 需要 server 观测或握手状态时，再看 [`ServerCommandModule.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)

继续追：

- 命令设计和数据模型看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- 主请求链看 [`request-execution-flow.md`](./request-execution-flow.md)。
- handler 的 `parse(CommandArgs)` 必须保持 session-free，不得访问 DB router 或 server provider；DB/session 工作放在 `CommandInvocation.prepare(session)` 或 `PreparedCommand.execute(context)`。
- 新增 option/subcommand 时要补对应成功路径和错误路径测试；每个新注册名还必须给 parse-isolation fixture，server-only 命令要补 server-main 组装或协议集成测试。

测试优先级：

- 对应家族测试，例如 `StringCommandTest`、`ListCommandTest`、`HashCommandTest`、`SetCommandTest`、`ZSetCommandTest`、`HllCommandTest`
- `CommandErrorTest`
- `CommandVariantCoverageTest`
- `CommandRegistryGuardTest`
- `CommandDispatcherTest`
- 默认命令跑 `CommandParseIsolationTest`，server-only 命令再跑 `ServerCommandParseIsolationTest`、`YierdisServerBootstrapCommandWiringTest` 和相关协议集成测试

## 改 transaction / replay

先打开：

- [`CommandDispatcher.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java)
- [`TransactionCommands.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java)
- [`TransactionState.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java)
- [`EngineSession.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java)

可排队命令在 `MULTI` 中只运行 handler parse 做 preflight；`QUEUED` action 在回复容量预留后才调用
`TransactionState.tryEnqueue(request)`，由 transaction state 取得 retained request 所有权，不会提前运行
invocation prepare。`EXEC` 由 `TransactionCommands` drain 队列，通过同一 dispatcher 的 replay 入口依次
prepare/execute；drain 后的 retained request 和 child `PreparedCommand` 都归 `PreparedExec` 所有并由它关闭。
`EngineSession` 只实现连接级事务状态，不执行 replay。

测试优先级：

- `CommandDispatcherTest`
- `TransactionCommandTest`
- `TransactionQueueCleanupTest`
- `ReplyPreflightCommandTest`

## 改 string / bitmap / HLL

先打开：

- string/bitmap command：对应 `StringCommands`
- HLL command：对应 `HllCommands`
- API：`StringReadOps`、`StringWriteOps`、`HllReadOps`、`HllWriteOps`
- DB 实现：`YierdisStringOps`、HLL 相关 ops
- 共享 entry staging：[`EntryMutationEntries.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/EntryMutationEntries.java)
- entry 提交生命周期：[`PreparedEntryMutation.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedEntryMutation.java)
- 内部结构：`StringRoot`、`YierdisHyperLogLog`

继续追：

- string、bitmap、HLL 的关系看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- bytes view、slice 和 fast path 看 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。
- native/off-heap 存储边界看 [`native-memory-runtime.md`](./native-memory-runtime.md) 和 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。

测试优先级：

- `StringCommandTest`
- `BitmapCommandTest`
- `HllCommandTest`
- `StringDirectOpsTest`
- `NativeStorageRegressionTest`
- `StringRootTest`
- `YierdisHyperLogLogTest`

## 改 list / hash / set / zset

先打开：

- 对应 command 家族：`ListCommands`、`HashCommands`、`SetCommands`、`ZSetCommands`
- API：`ListReadOps` / `ListWriteOps` 等同名 family ops
- DB 实现：对应 `Yierdis*Ops`
- 共享 entry staging：[`EntryMutationEntries.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/EntryMutationEntries.java)
- entry 提交生命周期：[`PreparedEntryMutation.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedEntryMutation.java)
- root/value：`ListRoot`、`HashRoot`、`SetRoot`、`ZSetRoot`、`ListValue`、`HashValue`、`SetValue`、`ZSetValue`

继续追：

- 逻辑类型、内部编码、wrong-type 语义看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- DB object graph 和 lifecycle 看 [`db-internals.md`](./db-internals.md)。
- native value 存储和 copy 行为看 [`native-memory-runtime.md`](./native-memory-runtime.md)。

测试优先级：

- `ListCommandTest`
- `HashCommandTest`
- `SetCommandTest`
- `ZSetCommandTest`
- `CollectionDirectOpsTest`
- `ListRootTest`
- `CollectionRootTest`
- `ListValueTest`
- `HashValueTest`
- `SetValueTest`
- `ZSetValueTest`

## 改 keyspace / TTL / maxmemory

先打开：

- `KeyspaceReadOps`、`KeyspaceWriteOps`
- `TtlReadOps`、`TtlWriteOps`
- `MemoryOps`
- `YierdisTtlOps`
- `YierdisDbKeyLifecycle`
- `YierdisDbExpirationSupport`
- `YierdisDbRuntimeInternals.reclaimExpired(...)`
- `YierdisDbMutationExecutor`
- `YierdisDbMemoryLedger`
- `YierdisDbMaxmemorySupport`
- `YierdisGlobalMaxmemoryGovernor`
- `YierdisInstanceRuntimeAccess`

继续追：

- key lifecycle 总览看 [`db-internals.md`](./db-internals.md)。
- TTL 命令写路径、lazy expire、cleanup budget 看 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。
- maxmemory reservation、policy 和 global governor 看 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。
- 运行配置和线上语义看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- native keyspace 和唯一 entry deadline 看 [`native-memory-runtime.md`](./native-memory-runtime.md)。

测试优先级：

- `TtlLifecycleDirectOpsTest`
- `ActiveExpirationTest`
- `ExpireSemanticsTest`
- `TtlMaxmemoryTest`
- `MaxmemoryEvictionTest`
- `MutationExecutorReservationTest`
- `YierdisGlobalMaxmemoryGovernorTest`
- `GlobalMaxmemoryLruAcrossDbsTest`
- `MemoryStatsAccountingConsistencyTest`

## 改 native memory

先打开：

- [`NativeHandle.java`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java)
- [`YierdisNativeObjectTable.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java)
- [`YierdisFfmStableMemoryBackend.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmStableMemoryBackend.java)
- `EntryHandle`、`ValueHandle`、`KeyHandle`
- `NativeKeyDirectory`
- `NativeBytesSlice`、`NativeByteStore`、`NativeByteMap`、`NativeListpack`
- 测试侧 reachable graph：[`YierdisDbNativeHandleGraph.java`](../../yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraph.java)

继续追：

- JDK FFM 基础看 [`ffm-primer.md`](./ffm-primer.md)。
- 当前生产 native-memory 路线看 [`native-memory-runtime.md`](./native-memory-runtime.md)。
- handle、object table、pin、quarantine、active defrag 看 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

测试优先级：

- `NativeHandleTest`
- `YierdisNativeObjectTableTest`
- `YierdisFfmStableMemoryBackendTest`
- `YierdisFfmStableMemoryBackendOwnershipTest`
- `EntryHandleContractTest`
- `ValueHandleContractTest`
- `KeyHandleContractTest`
- `YierdisDbNativeHandleGraphTest`
- `NativeStorageRegressionTest`

## 改 executor / backpressure

先打开：

- [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)
- [`CommandExecutorSubmitter.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java)
- [`CommandExecutorDrainLoop.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java)
- [`CommandExecutorExecutionSupport.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java)
- [`RedisReplyRenderer.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java)
- [`YierdisServerRuntimeConfig.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java)

继续追：

- 执行线程、队列、调度、公平性和背压看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。
- 请求主链和 owner thread 看 [`request-execution-flow.md`](./request-execution-flow.md)。
- execution support 必须按 prepare -> reserve -> validate -> execute -> render 排序，并在 renderer 消费完语义流 source 后再关闭 `PreparedCommand`。
- `QUIT` 等关闭意图来自 `CommandResult.closeAfterReply`，由 executor 标记 reply slot，不从 writer 的隐藏状态推导。
- runtime 配置入口看 [`configuration-and-operations.md`](./configuration-and-operations.md)。

测试优先级：

- `CommandExecutorTest`
- `CommandExecutorBackpressureTest`
- `CommandExecutorFairSchedulingTest`
- `ExecutionConnectionContextTest`
- `RedisReplyRendererTest`
- `YierdisServerBootstrapCommandWiringTest`
- `NettyExecutionAdapterIntegrationTest`

## 改 INFO / STATS / observability

先打开：

- [`ServerCommandModule.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)
- [`NettyServerInfoProvider.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java)
- [`YierdisInstanceObservability.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java)
- [`YierdisDbMemoryReporter.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java)
- `CommandExecutor` stats accessors

`NettyServerInfoProvider.serverStatsSnapshot(...)` 是 INFO / STATS / health 的公共采样边界。新增公共统计时先进入这份请求级快照；memory/keyspace 这类 DB 聚合保持按 section 读取，避免 health 路径无条件扫描实例数据。

继续追：

- 可观测命令和配置含义看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- memory / object introspection 看 [`db-internals.md`](./db-internals.md)。
- server 组装边界看 [`module-architecture.md`](./module-architecture.md)。

测试优先级：

- `YierdisServerBootstrapCommandWiringTest`
- `MemoryStatsCommandTest`
- `YierdisDbMemoryReporterTest`
- `YierdisDbIntrospectionTest`

## 改代理 / DB 提交事件 / AOF replication 起点

先打开：

- [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)
- [`CommandSession.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSession.java)
- [`CommandExecutionContext.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandExecutionContext.java)
- [`YierdisDbRouter.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/YierdisDbRouter.java)
- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)
- [`ServerInfoProvider.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java)
- [`MutationContext.java`](../../yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/MutationContext.java)
- [`DbCommitPublisher.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitPublisher.java)
- [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)
- [`CommitStream.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/CommitStream.java)
- [`YierdisChangeEvent.java`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java)

继续追：

- 请求主链和 command/session 边界看 [`request-execution-flow.md`](./request-execution-flow.md)。
- 命令 record scope 和 DB commit publication 的顺序看 [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)。
- expire / eviction synthetic delete 的 DB lifecycle 看 [`db-internals.md`](./db-internals.md)。

测试优先级：

- `CommandDispatcherTest`
- `CommandPipelineArchitectureTest`
- `CommandParseIsolationTest`
- `DbCommitPublisherTest`
- `CommitStreamTest`
- `CommitStreamShutdownTest`
- `YierdisChangeSinkTest`
- `ActiveExpirationTest`
- `YierdisDbConstructionTest`
- `YierdisServerBootstrapCommandWiringTest`
- `MemoryStatsCommandTest`

## 推荐最小工作流

1. 在 [`core-logic-index.md`](./core-logic-index.md) 找到目标类的职责和边界。
2. 先补或调整最窄测试，再改实现。
3. 跑目标家族测试。
4. 如果改动改变了架构边界、协议语义或 native-memory 当前事实，同步更新对应专题文档。
5. 跑 `git diff --check -- <changed files>`，确认文档和代码没有 whitespace 问题。

## 新人先收藏的文件

- [`project-overview.md`](./project-overview.md)
- [`module-architecture.md`](./module-architecture.md)
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)
- [`request-execution-flow.md`](./request-execution-flow.md)
- [`commands-and-data-model.md`](./commands-and-data-model.md)
- [`db-internals.md`](./db-internals.md)
- [`executor-and-backpressure.md`](./executor-and-backpressure.md)
- [`native-memory-runtime.md`](./native-memory-runtime.md)
- [`testing-and-debugging.md`](./testing-and-debugging.md)
- [`glossary.md`](./glossary.md)
