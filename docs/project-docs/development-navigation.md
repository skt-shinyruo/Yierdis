# 开发导航

本页按**常见改动类型**回答一个实际问题：要改某类需求时，先打开哪些文件、沿着哪条链继续追、该跑哪些测试、以及这一步最容易踩什么边界。每条改动对应四个小节：**要动的类 → 要跑的测试 → 容易踩的边界 → 继续追的专题文档**。

常备两份配套：测试分层与排障顺序看 [`testing-and-debugging.md`](./testing-and-debugging.md)，模块职责与依赖方向看 [`module-architecture.md`](./module-architecture.md)。

## 工作规则

1. 先定改动边界。协议、command、DB、executor、runtime、native memory 不要混在一次小改里处理。
2. 先找最近的测试，再改实现。命令语义优先看 integration tests（`yierdis-tests`），DB 语义优先看 direct ops tests（`yierdis-db`），native memory 优先看 internal contract tests（`yierdis-db`）。
3. 命令层用 `DbEngine` 的 typed ops 访问数据库，不要直接依赖 `YierdisDb`。
4. RESP DTO 不进入 command 层；进入 command 层前必须变成 `ExecutionRequest`。
5. 写路径必须经过 mutation executor、memory ledger、TTL/key lifecycle，不要直接改 root/value 结构。
6. 新命令、新 DB API、新 native/internal 结构要同步补真实测试，并更新受影响的专题文档。

命令主线固定为（所有命令改动都落在这条链上）：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> returned Function.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

## 找到并跑对测试

测试散落在多个 Maven 模块，**测试类所在模块决定 `-pl`**：

| 测试类别 | 所在模块 | 典型类 |
| --- | --- | --- |
| 协议编解码单元 | `yierdis-networking-resp` | `RespReplyWriterTest`, `RespProtocolLimitsTest` |
| server 侧协议/组装 | `yierdis-server/yierdis-server` | `RespRequestDecoderTest`, `YierdisServerBootstrapCommandWiringTest` |
| executor / 回复渲染 | `yierdis-server/yierdis-server`、`yierdis-server/yierdis-server-api` | `CommandExecutorTest`, `RedisReplyRendererTest` |
| command kernel/API | `yierdis-command` | `CommandDispatcherTest`, `CommandRegistryTest` |
| 命令家族与跨模块集成 | `yierdis-tests` | `StringCommandTest`, `MaxmemoryEvictionTest` |
| DB direct ops / internal contract | `yierdis-db` | `StringDirectOpsTest`, `StringRootTest` |
| CLI / benchmark | `yierdis-cli`、`yierdis-benchmark` | `YierdisClientTest`, `NioBenchmarkRunnerTest` |

统一用本地 JDK 25。focused 测试的标准形态是「限定模块 + 自动带上依赖 + 只跑指定类 + 找不到就跳过」：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl <module> -am -Dtest=ClassA,ClassB -Dsurefire.failIfNoSpecifiedTests=false test
```

`-Dtest=ClassA,ClassB` 会在 reactor 内每个被构建的模块里按类名匹配，`-am` 保证即使目标类只存在于依赖模块里也会被执行；`-Dsurefire.failIfNoSpecifiedTests=false` 让「模块里没有这个类」不报错。因此一条命令可以同时跑多个模块的类，不必拆开。若不确定某个测试类属于哪个模块，用 Glob 在 `**/src/test/java/**/<类名>.java` 核实后再决定 `-pl`。

## 改协议

**要动的类**

- [`RespRequestDecoder.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)
- [`ByteArrayExecutionRequest.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java)
- [`InboundMemoryBudget.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundMemoryBudget.java)
- [`RespReplyWriter.java`](../../yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java)
- [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)
- 协议上限常量在 `yierdis-networking-resp` 的 `RespProtocolLimits`；inline 语法在 `InlineCommandParser`

**要跑的测试**

```bash
# 1. 最窄的单元层：解码、准入、回包编码
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server,yierdis-networking-resp -am \
  -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest,RespReplyWriterTest,RespReplySizerTest,RespProtocolLimitsTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# 2. server 侧协议集成：握手、错误关闭、生命周期
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server -am \
  -Dtest=RespProtocolIntegrationTest,RespProtocolErrorIntegrationTest,RespHandshakeIntegrationTest,RespIngressLifecycleIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

fuzz / 压力形态的回归在 `yierdis-tests`：`RespIngressFuzzTest`、`RespIngressPressureTest`。

**容易踩的边界**

- 请求侧 `ByteArrayExecutionRequest` 的 admission lease：`takeOwnership(...)` 移交 argv + lease，`retain()` 共享 argv 并增引用，`copyOf(...)` 才做独立快照。lease 必须在**最后一个消费者**处释放，早放会 use-after-free，晚放会泄漏准入预算。
- `HELLO 2/3` 或回包类型差异：协议版本是连接级状态，涉及 `CommandSession` 的版本方法、连接 owner `EngineSession` 与 `RespReplyWriter`。回包 wire 版本在 prepare 时就要确定，不能等 execute 才切换（见 `PreparedCommand.replyProtocolVersion()`）。
- 硬上限（`--protocolMaxBulkBytes` / `--protocolMaxArgs` / `--protocolMaxLineBytes` / `--protocolMaxCommandBytes`）在 parser 内生效，必须在请求到达 executor 之前拒绝，不能靠回包侧兜底。

**继续追**：[`protocol-reference.md`](./protocol-reference.md)、[`request-execution-flow.md`](./request-execution-flow.md)、[`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)、[`netty-adapter-design.md`](./netty-adapter-design.md)。

## 新增或修改命令

**要动的类**

- [`CommandSpec.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/CommandSpec.java)
- [`CommandSyntax.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/CommandSyntax.java)
- [`CommandArgs.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/CommandArgs.java)
- [`CommandHandler.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/CommandHandler.java)
- [`PreparedCommand.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java)
- [`CommandRegistry.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java)、[`CommandDispatcher.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java)
- [`DefaultCommandModules.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java)（默认命令注册的唯一来源）
- [`CommandSupport.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)，以及对应家族的 `StringCommands` / `ListCommands` / `HashCommands` / `SetCommands` / `ZSetCommands` / `HllCommands` / `KeyCommands`
- server-only 命令（`HELLO`/`INFO`/`STATS` 等）还要看 [`ServerCommandModule.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)

**要跑的测试**

```bash
# 命令家族 + 错误路径（覆盖成功/失败/arity/wrong-type）
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=StringCommandTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest,BitmapCommandTest,CommandErrorTest,DefaultCommandRegistrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# kernel/注册/parse 隔离
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-command,yierdis-tests,yierdis-server/yierdis-server -am \
  -Dtest=CommandDispatcherTest,CommandRegistryTest,CommandSpecTest,CommandSyntaxTest,CommandContractTest,CommandParseIsolationTest,ServerCommandParseIsolationTest,YierdisServerBootstrapCommandWiringTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- handler 的 `parse(CommandArgs)` 必须保持 **session-free**：不得访问 DB router 或 server provider。真正读 session 的动作放在返回的 `Function<CommandSession, PreparedCommand>.apply(session)`，执行期工作放在 `PreparedCommand.execute(session)`。
- 新增 option/subcommand 要同时补成功路径和错误路径测试；每个新注册名还要有 parse-isolation fixture——`CommandParseIsolationTest` 保证默认命令的 fixture 名称集合与 `DefaultCommandModules` 的注册完全相等，并用会抛异常的 router/provider 验证 parse 不访问服务；server-only 命令由 `ServerCommandParseIsolationTest` 对 `HELLO`/`INFO`/`STATS` 做同样检查。
- 若命令暴露新回包形状，`CommandSpec.syntax()` 的 arity/key metadata 与 `PreparedCommand.reservationShape()` 都要跟着改，否则预留容量与实际写出会不一致。

**继续追**：[`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)、[`commands-and-data-model.md`](./commands-and-data-model.md)、[`request-execution-flow.md`](./request-execution-flow.md)。

## 改 transaction / replay

**要动的类**

- [`CommandDispatcher.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java)
- [`TransactionCommands.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java)
- [`TransactionState.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java)
- [`EngineSession.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java)

**要跑的测试**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=CommandDispatcherTest,EngineSessionTest,TransactionCommandTest,TransactionQueueCleanupTest,TransactionQueueLimitTest,ReplyPreflightCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- 排队语义：可排队命令在 `MULTI` 中只做 handler parse preflight；`QUEUED` action 要等回复容量预留完成，才调用 `TransactionState.tryEnqueue(request)`。入队**不**运行 handler 返回的 prepare function，owner 由 transaction state 取得。
- `EXEC` 由 `TransactionCommands` drain 队列，走同一 dispatcher 的 replay 入口依次 prepare/execute；drain 后的 retained request 和 child `PreparedCommand` 都归 `PreparedExec` 所有，由它关闭。
- `EngineSession` 只管连接级事务状态，不执行 replay。
- 队列也有上限（`--transactionQueueMaxCommands` / `--transactionQueueMaxBytes`），新增语义时确认超限路径仍回确定的错误而不是丢弃。

**继续追**：[`transaction-and-replay.md`](./transaction-and-replay.md)、[`request-execution-flow.md`](./request-execution-flow.md)。

## 改 string / bitmap / HLL

**要动的类**

- command：`StringCommands`、`HllCommands`
- API：`StringOps`、`HllOps`
- DB 实现：`YierdisStringOps`、`YierdisHllOps`
- 共享 entry staging 与 ownership：[`YierdisDbKeyLifecycle.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)
- entry 提交生命周期：[`PreparedEntryMutation.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/PreparedEntryMutation.java)
- 内部结构：`StringRoot`、`YierdisHyperLogLog`

**要跑的测试**

```bash
# command 家族
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am -Dtest=StringCommandTest,BitmapCommandTest,HllCommandTest,EmptyBulkStringCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# DB 层：direct ops + root/value
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-db -am \
  -Dtest=StringDirectOpsTest,StringRootTest,YierdisHyperLogLogTest,NativeStorageRegressionTest,OffHeapStringStorageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- HLL 是带特定 header/payload 的 **string 语义值**，没有独立 `ValueType`；不要为它新增 value type，也不要读旧的私有 `HLL1` payload。
- string 的 encoding 会在 raw / integer-like 之间切换，`OBJECT ENCODING` 观察的就是这一层；改写入路径要同步检查 `YierdisDbObjectEncodingTest`。
- 写入必须经 `YierdisDbKeyLifecycle` staging + `PreparedEntryMutation` 提交，不要直接改 `StringRoot`。

**继续追**：[`commands-and-data-model.md`](./commands-and-data-model.md)、[`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)、[`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。

## 改 list / hash / set / zset

**要动的类**

- command 家族：`ListCommands`、`HashCommands`、`SetCommands`、`ZSetCommands`
- API：`ListOps`、`HashOps`、`SetOps`、`ZSetOps`
- DB 实现：`YierdisListOps`、`YierdisHashOps`、`YierdisSetOps`、`YierdisZSetOps`
- 共享 entry staging 与 ownership：[`YierdisDbKeyLifecycle.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)，entry 提交：[`PreparedEntryMutation.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/PreparedEntryMutation.java)
- root/value：`ListRoot`、`HashRoot`、`SetRoot`、`ZSetRoot` 与 `ListValue`、`HashValue`、`SetValue`、`ZSetValue`

**要跑的测试**

```bash
# command 家族 + SCAN 系列
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,CollectionScanCommandTest,ScanCursorContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# DB 层：direct ops + root/value
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-db -am \
  -Dtest=CollectionDirectOpsTest,ListRootTest,CollectionRootTest,ListValueTest,HashValueTest,SetValueTest,ZSetValueTest,NativeCollectionReadStreamingTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- 逻辑类型（`TYPE`）与内部编码（`OBJECT ENCODING`，如 packed hash / intset / skiplist）是两个层面；wrong-type 语义由 DB 层抛 `WrongTypeException`，命令层只做映射。
- 大集合的读写走 collection scan window / 语义流式 source（`ByteSequenceSource`、`ByteMapSource`、`PoppedValueSequence`），不要在中途 materialize 成无界堆数组。
- 集合的 root/value 变更同样要走 key lifecycle + mutation executor，避免半写入。

**继续追**：[`commands-and-data-model.md`](./commands-and-data-model.md)、[`db-internals.md`](./db-internals.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)。

## 改 keyspace / TTL / maxmemory

**要动的类**

- `KeyspaceOps`、`TtlOps`；DB 实现 `YierdisKeyspaceOps`、`YierdisTtlOps`
- `DbEngine.memoryUsage(...)`、`memoryStats()`、`objectEncoding(...)`
- lifecycle/expiration：[`YierdisDbKeyLifecycle.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)、`YierdisDbExpirationSupport`、`ExpiresIndex`
- 写入与账本：`YierdisDbMutationExecutor`（含 `MutationPlan`、`reserveNormalPlan`）、`PreparedBatchMutation`、`YierdisDbKernel`、`YierdisDbMemoryLedger`
- maxmemory：`YierdisDbMaxmemorySupport`、`YierdisGlobalMaxmemoryGovernor`、`YierdisInstanceRuntimeAccess`
- 主动过期清理：`YierdisDbKernel.reclaimExpired(...)` 由 runtime maintenance 调度

**要跑的测试**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-db,yierdis-tests,yierdis-server/yierdis-server -am \
  -Dtest=TtlLifecycleDirectOpsTest,ActiveExpirationTest,ExpireSemanticsTest,TtlMaxmemoryTest,MaxmemoryEvictionTest,MaxmemoryDoubleReplyRegressionTest,MutationExecutorReservationTest,YierdisGlobalMaxmemoryGovernorTest,GlobalMaxmemoryLruAcrossDbsTest,MemoryStatsAccountingConsistencyTest,MemoryStatsCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- TTL 的唯一状态是 `EntryRecord.expireAtMillis`；主动清理消费派生的按过期时间排序的 `ExpiresIndex`，并用派生 `expireCount` 快速判断是否有工作——不要扫描 key directory，也不要另建第二份 key→deadline 状态。
- 写路径必须「估算 → 预留 → 必要时驱逐 → commit，失败 rollback」，admission 比较的是**不含 reservation 的物理快照**；别把报告用的 `effective_*` 当成 admission 输入。
- maxmemory 与 hard reply 上限是两套独立预算：一次成功的删除只降低 DB 使用量，已存在的 reply source 仍占着 outbound 容量直到 slot 终态清理。

**继续追**：[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)、[`db-internals.md`](./db-internals.md)、[`configuration-and-operations.md`](./configuration-and-operations.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)。

## 改 native memory

**要动的类**

- [`NativeHandle.java`](../../yierdis-db/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java)、`StableMemoryBackend`、`NativeEpochScope`、`NativeAllocationScope`
- [`YierdisNativeObjectTable.java`](../../yierdis-db/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java)
- [`YierdisFfmStableMemoryBackend.java`](../../yierdis-db/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmStableMemoryBackend.java)
- DB 侧句柄：`EntryHandle`、`ValueHandle`、`KeyHandle`；keyspace 目录 `NativeKeyDirectory`
- 内部结构：`NativeBytesSlice`、`NativeByteStore`、`NativeByteMap`、`NativeListpack`、`NativeCollectionScanWindow`
- 测试侧 reachable graph：[`YierdisDbNativeHandleGraph.java`](../../yierdis-db/src/test/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraph.java)

**要跑的测试**

```bash
# 最窄：allocator / handle / object table
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-db -am \
  -Dtest=NativeHandleTest,YierdisNativeObjectTableTest,YierdisFfmStableMemoryBackendTest,YierdisFfmStableMemoryBackendOwnershipTest,NativeAllocationScopeTest,StableMemoryBackendContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# DB native path：句柄契约 + 回归 + 泄漏
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-db,yierdis-tests -am \
  -Dtest=EntryHandleContractTest,ValueHandleContractTest,KeyHandleContractTest,YierdisDbNativeHandleGraphTest,NativeStorageRegressionTest,OffHeapLeakRegressionTest,FfmDbEpochLifecycleIntegrationTest,FfmDefragMaintenanceIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- `NativeHandle` 是 `(allocatorId, localRaw)` 的 paired identity，不是裸地址；`localRaw` 只能由所属 backend 解释，FFM 私有 codec 才在其中编码 domain/kind/slot/generation。
- 移动对象（realloc / defrag）必须保持完整 handle 稳定；pin 期间 free 会进入 quarantine，解除保护后才能回收。所有 view、scope、epoch、显式 pin 都必须在 backend 关闭前结束。
- 每次成功 `pin` 必须由同一 owner 配对 `unpin`；漏配对会让 slot 永远无法复用。

**继续追**：[`native-memory-runtime.md`](./native-memory-runtime.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`ffm-primer.md`](./ffm-primer.md)。

## 改 executor / backpressure

**要动的类**

- [`CommandExecutor.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)
- [`CommandExecutorSubmitter.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java)
- [`CommandExecutorDrainLoop.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java)
- [`CommandExecutorExecutionSupport.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java)
- [`RedisReplyRenderer.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java)
- [`YierdisServerRuntimeConfig.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java)

**要跑的测试**

```bash
# executor 单元
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server,yierdis-server/yierdis-server-api -am \
  -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest,ExecutionConnectionContextTest,ExecutorTaskQueueTest,ExecutorBacklogBudgetTest,ReplyCapacityBlockedSchedulingTest,RedisReplyRendererTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# server 组装 + Netty 适配
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server -am \
  -Dtest=YierdisServerBootstrapCommandWiringTest,NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest,ReplySourceThreadAffinityIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- execution support 必须按 `prepare -> reserve -> validate -> execute -> render` 排序，并在 renderer 消费完语义流 source 之后再关闭 `PreparedCommand`；提前关闭会让 bulk/sequence/map source 悬空。
- `QUIT` 等关闭意图来自 `CommandResult.closeAfterReply`，由 executor 标记 reply slot，**不要**从 writer 的隐藏状态推导。
- 背压涉及队列容量（`--executorQueueCapacity`/`--executorQueueMaxBytes`）与字节水位（`--backpressureBytesHigh`/`--backpressureBytesLow`）两套口径；进入背压后必须能在水位回落后恢复。

**继续追**：[`executor-and-backpressure.md`](./executor-and-backpressure.md)、[`request-execution-flow.md`](./request-execution-flow.md)、[`configuration-and-operations.md`](./configuration-and-operations.md)。

## 改 INFO / STATS / observability

**要动的类**

- [`ServerCommandModule.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)
- [`NettyServerInfoProvider.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java)
- [`YierdisInstanceObservability.java`](../../yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java)
- [`YierdisDbMemoryReporter.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java)
- `CommandExecutor` 的 stats accessors

`NettyServerInfoProvider` 里每个字段只声明一次（名称 + 取值 + 分组 + 生效渲染面），由 map / text 两个渲染器按声明顺序过滤输出。`NettyServerInfoProvider.serverStatsSnapshot(...)` 是 INFO / STATS / health 的公共采样边界：新增公共统计先并入这份请求级快照；memory/keyspace 这类 DB 聚合仍按 section 读取，避免 health 路径无条件扫描实例数据。

**要跑的测试**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server,yierdis-db,yierdis-tests -am \
  -Dtest=YierdisServerBootstrapCommandWiringTest,MemoryStatsCommandTest,YierdisDbMemoryReporterTest,YierdisDbObjectEncodingTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- INFO text 的 `# Stats` 段给字段统一加 `yierdis_` 前缀；`STATS` 的 map 渲染面用未加前缀的字段名。同名统计不要在两处各写一份取值逻辑。
- health 路径要轻量：不要在 `serverStatsSnapshot(...)` 里做全实例扫描。
- `MEMORY STATS` 是 explainable estimate，`ledger_used_bytes` 绑定 `heapDataBytesEstimate`，`ledger_reserved_bytes` 才是 ledger `reservedBytes`——别混用两个口径。

**继续追**：[`configuration-and-operations.md`](./configuration-and-operations.md)、[`db-internals.md`](./db-internals.md)、[`module-architecture.md`](./module-architecture.md)、[`production-hardening-operations.md`](./production-hardening-operations.md)。

## 改 session / DB 路由 / 观测代理起点

**要动的类**

- [`CommandSession.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSession.java)
- [`YierdisDbRouter.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/YierdisDbRouter.java)
- [`CommandSupport.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)
- [`ServerInfoProvider.java`](../../yierdis-command/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java)
- [`YierdisDbMutationExecutor.java`](../../yierdis-db/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

**要跑的测试**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests,yierdis-command,yierdis-server/yierdis-server -am \
  -Dtest=CommandDispatcherTest,CommandParseIsolationTest,ActiveExpirationTest,YierdisDbConstructionTest,DbEngineReadWriteBoundaryTest,YierdisServerBootstrapCommandWiringTest,MemoryStatsCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- `ServerInfoProvider.memoryStats(...)` 返回 `null` 表示「回退到当前 DB 的 `memoryStats()`」；global scope 才会返回 instance 聚合视角。这是 `MEMORY STATS` 的回退语义，新增观测字段时别破坏它。
- command 层只依赖 `DbEngine` 与 `ServerInfoProvider`，不得反向依赖 `yierdis-server` 或 `YierdisDb` internal——这类约束由 `CommandParseIsolationTest` 和 `DbEngineReadWriteBoundaryTest` 守着。

**继续追**：[`proxy-logic.md`](./proxy-logic.md)、[`request-execution-flow.md`](./request-execution-flow.md)、[`db-internals.md`](./db-internals.md)。

## 改 CLI / benchmark

**要动的类**

- CLI：`yierdis-cli` 的 `YierdisCli`、`YierdisCliArgs`、`YierdisClient`；inline 语法在 `yierdis-networking-resp` 的 `InlineCommandParser`、`RespClientCodec`
- RESP benchmark：`RedisBenchmarkOptions`、`BenchmarkConfig`、`RedisBenchmarkCatalog`、`RedisBenchmarkCommandTemplate`、`NioBenchmarkRunner`、`LatencyRecorder`、`BenchmarkOutputRenderer`
- storage benchmark：`StorageBenchmarkOptions`、`StorageBenchmarkConfig`、`StorageBenchmarkRunner`、`StorageMemorySnapshot`、`StorageBenchmarkRenderer`

**要跑的测试**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-cli,yierdis-benchmark -am \
  -Dtest=YierdisClientTest,RedisBenchmarkOptionsTest,RedisBenchmarkCatalogTest,RedisBenchmarkCommandTemplateTest,NioBenchmarkRunnerTest,BenchmarkOutputRendererTest,BenchScriptContractTest,StorageBenchmarkConfigTest,StorageBenchmarkRunnerTest,StorageBenchmarkRendererTest,StorageBenchScriptContractTest,ProcessRssReaderTest,YierdisBenchEntrypointTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

**容易踩的边界**

- CSV 契约是硬约束：RESP CSV 前八列必须与官方 Redis-style CSV 对齐，storage CSV 是另一套 29 列 header，两者不要混用或按列位置硬对齐。
- 变更 catalog/title 会破坏 canonical title 对照，必须同步 `client-and-bench-internals.md` 与 `RedisBenchmarkCatalogCoverageGuardTest`。
- benchmark 只连接已启动的 Yierdis，不启动 Redis，也没有 AUTH 开关。

**继续追**：[`client-and-bench-internals.md`](./client-and-bench-internals.md)、[`production-hardening-operations.md`](./production-hardening-operations.md)。

## 推荐最小工作流

1. 在本页和 [`module-architecture.md`](./module-architecture.md) 找到目标类的职责和边界。
2. 先补或调整最窄测试，再改实现（先见测试红，再改绿）。
3. 跑目标家族测试，再按需要跑跨模块集成。
4. 如果改动改变了架构边界、协议语义或 native-memory 当前事实，同步更新对应专题文档。
5. 跑 `git diff --check -- <changed files>`，确认文档和代码没有 whitespace 问题。

## 新人先收藏的文件

- [`project-overview.md`](./project-overview.md)
- [`module-architecture.md`](./module-architecture.md)
- [`request-execution-flow.md`](./request-execution-flow.md)
- [`commands-and-data-model.md`](./commands-and-data-model.md)
- [`db-internals.md`](./db-internals.md)
- [`executor-and-backpressure.md`](./executor-and-backpressure.md)
- [`native-memory-runtime.md`](./native-memory-runtime.md)
- [`testing-and-debugging.md`](./testing-and-debugging.md)
- [`glossary.md`](./glossary.md)