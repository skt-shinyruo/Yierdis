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
- 如果是 `HELLO 2/3` 或回包类型差异，继续看 `RespReplyWriterFactory`、`ProtocolNegotiationSession`、`EngineSession` 和 `RespHandshakeIntegrationTest`。

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
- [`CommandRegistry.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java)
- [`YierdisFastCommandProcessor.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)
- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)
- 对应家族的 `*Commands.java`
- 需要 server 观测或握手状态时，再看 [`ServerCommandModule.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)

继续追：

- 命令设计和数据模型看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- 主请求链看 [`request-execution-flow.md`](./request-execution-flow.md)。
- 新增 option/subcommand 时要补对应成功路径和错误路径测试；server-only 命令还要补 server-main 组装或协议集成测试。

测试优先级：

- 对应家族测试，例如 `StringCommandTest`、`ListCommandTest`、`HashCommandTest`、`SetCommandTest`、`ZSetCommandTest`、`HllCommandTest`
- `CommandErrorTest`
- `CommandVariantCoverageTest`
- `CommandRegistryGuardTest`
- server-only 命令再跑 `YierdisServerBootstrapCommandWiringTest` 和相关协议集成测试

## 改 string / bitmap / HLL

先打开：

- string/bitmap command：对应 `StringCommands`
- HLL command：对应 `HllCommands`
- API：`StringReadOps`、`StringWriteOps`、`HllReadOps`、`HllWriteOps`
- DB 实现：`YierdisStringOps`、HLL 相关 ops
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
- `YierdisExpireIndex`、`YierdisFfmExpireIndex`
- `YierdisDbMutationExecutor`
- `YierdisDbMemoryLedger`
- `YierdisDbMaxmemorySupport`
- `YierdisGlobalMaxmemoryGovernor`
- `YierdisInstanceRuntimeAccess`、`YierdisInstanceMaintenance`

继续追：

- key lifecycle 总览看 [`db-internals.md`](./db-internals.md)。
- TTL 命令写路径、lazy expire、cleanup budget 看 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。
- maxmemory reservation、policy 和 global governor 看 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。
- 运行配置和线上语义看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- native keyspace 和 expires 看 [`native-memory-runtime.md`](./native-memory-runtime.md)。

测试优先级：

- `TtlLifecycleDirectOpsTest`
- `ExpireIndexTest`
- `ExpireSemanticsTest`
- `ExpireIndexContractTest`
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
- [`YierdisStableNativeAllocator.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java)
- `EntryHandle`、`ValueHandle`、`KeyHandle`
- `NativeKeyDirectory`
- `NativeBytesSlice`、`NativeByteStore`、`NativeByteMap`、`NativeListpack`
- `YierdisDbNativeHandleGraph`

继续追：

- JDK FFM 基础看 [`ffm-primer.md`](./ffm-primer.md)。
- 当前生产 native-memory 路线看 [`native-memory-runtime.md`](./native-memory-runtime.md)。
- handle、object table、pin、quarantine、active defrag 看 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

测试优先级：

- `NativeHandleTest`
- `YierdisNativeObjectTableTest`
- `YierdisStableNativeAllocatorTest`
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
- [`CommandExecutorConfigs.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java)

继续追：

- 执行线程、队列、调度、公平性和背压看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。
- 请求主链和 owner thread 看 [`request-execution-flow.md`](./request-execution-flow.md)。
- runtime 配置入口看 [`configuration-and-operations.md`](./configuration-and-operations.md)。

测试优先级：

- `CommandExecutorTest`
- `CommandExecutorBackpressureTest`
- `CommandExecutorFairSchedulingTest`
- `ExecutionConnectionContextTest`
- `YierdisServerBootstrapCommandWiringTest`
- `NettyExecutionAdapterIntegrationTest`

## 改 INFO / STATS / observability

先打开：

- [`ServerCommandModule.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)
- [`NettyServerInfoProvider.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java)
- [`YierdisInstanceObservability.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java)
- [`YierdisDbMemoryReporter.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java)
- `CommandExecutor` stats accessors

继续追：

- 可观测命令和配置含义看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- memory / object introspection 看 [`db-internals.md`](./db-internals.md)。
- server 组装边界看 [`module-architecture.md`](./module-architecture.md)。

测试优先级：

- `YierdisServerBootstrapCommandWiringTest`
- `MemoryStatsCommandTest`
- `YierdisDbMemoryReporterTest`
- `YierdisDbIntrospectionTest`

## 改代理 / 变更事件 / AOF replication 起点

先打开：

- [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)
- [`CommandSessionCapabilities.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java)
- [`YierdisDbRouter.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/YierdisDbRouter.java)
- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)
- [`ServerInfoProvider.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java)
- [`CommandChangeEmitter.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandChangeEmitter.java)
- [`RuntimeChangeSinkCommandChangeObserver.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RuntimeChangeSinkCommandChangeObserver.java)
- [`YierdisChangeEventBridge.java`](../../yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEventBridge.java)
- [`DbChangeContext.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChangeContext.java)

继续追：

- 请求主链和 engine/session 边界看 [`request-execution-flow.md`](./request-execution-flow.md)。
- 命令如何记录 mutation outcome 看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- expire / eviction synthetic delete 的 DB 生命周期看 [`db-internals.md`](./db-internals.md)。

测试优先级：

- `YierdisFastCommandProcessorPolicyTest`
- `YierdisFastCommandProcessorArchitectureTest`
- `RuntimeChangeSinkCommandChangeObserverTest`
- `YierdisChangeSinkTest`
- `ExpireIndexTest`
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
