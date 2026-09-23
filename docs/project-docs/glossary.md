# 术语表

收录 Yierdis 文档和源码里反复出现的术语。每条给出**准确定义**与**在源码里的对应位置**（真实类/字段/方法），方便边读文档边对照代码。

## Request / Reply Path

### `ExecutionRequest`

命令执行层看到的 argv bytes 视图。它提供 `argc()`、参数长度、null 判断和复制读取能力，但不暴露 RESP DTO。

源码位置：`yierdis-server/yierdis-server-api/.../execution/api/ExecutionRequest.java`（默认 `retain()` 与 `retainedBytes()`）。

### `ByteArrayExecutionRequest`

以 heap `byte[][]` 保存参数的 `ExecutionRequest` 实现。网络 decoder 用 `takeOwnership(byte[][], RequestMemoryLease)` 移交不可变 argv 与 admission lease，`retain()` 共享 argv 并增加 lease 引用，`copyOf(...)` 才创建独立快照。

源码位置：`yierdis-server/yierdis-server-api/.../execution/api/ByteArrayExecutionRequest.java`（工厂 `takeOwnership` / `copyOf`，方法 `retain()` / `retainedBytes()`）。详见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

### `RequestMemoryLease`

一次请求占用的准入额度 token。`takeOwnership` 时移交，`retain()` 增引用，释放必须由**最后一个消费者**执行，否则准入预算泄漏或提前失效。

源码位置：`.../execution/api/RequestMemoryLease.java`（实现 `ReferenceCountedRequestMemoryLease`）。

### `RedisReply`

命令结果中的协议无关语义回复，是一个 sealed interface。标量和普通 aggregate 直接保存值；bulk（`BulkString`）、byte sequence / byte map（`ByteAggregate`）保存长度、`retainedSourceBytes` 与 `Consumer<ReplySink>` emitter，由 owner 保持资源有效直到渲染完成。

源码位置：`.../execution/api/RedisReply.java`。详见 [`commands-and-data-model.md`](./commands-and-data-model.md)。

### `CommandResult`

一次命令执行的完整结果，record 形态为 `CommandResult(RedisReply reply, boolean closeAfterReply)`。`QUIT` 通过静态工厂 `CommandResult.closeAfterReply(reply)` 表达「回复发布后关闭」，无需直接操作 writer 或连接。

源码位置：`.../execution/api/CommandResult.java`。

### `RedisReplyRenderer`

执行器中唯一把 `RedisReply` 展开为 RESP-facing 写操作的组件。递归渲染 aggregate，并消费 bulk、byte sequence、byte map 的语义流式 emitter。

源码位置：`.../execution/api/RedisReplyRenderer.java`。

### `RedisReplyWriter`

`RedisReplyRenderer` 面向 RESP 编码实现的输出端口，不是命令实现 API。命令返回 `CommandResult`，不调用 `simpleString`、`bulkString`、`arrayHeader` 等 writer 方法。

源码位置：`.../execution/api/RedisReplyWriter.java`。

### `RespReplyWriter`

`RedisReplyWriter` 的 RESP 实现，把语义回包编码成 RESP2 或基础 RESP3 bytes。

源码位置：`yierdis-networking-resp/.../protocol/resp/RespReplyWriter.java`。详见 [`protocol-reference.md`](./protocol-reference.md)。

## Command Layer

命令执行主链固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

### `CommandArgs`

`ExecutionRequest` 的统一命令参数视图，集中提供 argv bytes、ASCII option 比较和整数解析。handler 的解析阶段只依赖该类型，不访问 session、DB router 或 server provider。

源码位置：`yierdis-command/.../command/api/CommandArgs.java`。

### `CommandSpec`

命令最终注册单元，由 `CommandSyntax` 和 `CommandHandler` 组成。syntax 保存名称、arity、key metadata、`TransactionPolicy` 和 `ReplyAdmissionRequirement`；handler 的 `parse(CommandArgs)` 返回 `Function<CommandSession, PreparedCommand>`。

源码位置：`yierdis-command/.../command/api/CommandSpec.java`、`CommandSyntax.java`、`CommandHandler.java`。

### `CommandRegistry`

命令名到 `CommandSpec` 的注册表。composition 阶段注册，sealed 后由 `CommandDispatcher` 只读查找。默认命令的注册来源是 `DefaultCommandModules`。

源码位置：`yierdis-command/.../command/kernel/CommandRegistry.java`。详见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

### `CommandDispatcher`

执行请求进入命令契约的统一入口。它检查命令名、null、arity 和事务策略，调用 `CommandSpec.handler().parse(CommandArgs)`，再对返回的函数调用 `apply(session)` 得到 `PreparedCommand`。

事务 active 时，queueable 命令只运行 handler parse 做 preflight，不提前 prepare；容量预留成功后事务队列 retain 原 `ExecutionRequest`。`EXEC` replay 通过同一 dispatcher 的准备入口（`prepareExecReplay(...)`）准备子 `PreparedCommand`，并关闭子命令和 retained requests。

源码位置：`yierdis-command/.../command/kernel/CommandDispatcher.java`。

### `Function<CommandSession, PreparedCommand>`

handler 解析成功后返回的准备函数。它在 `apply(CommandSession)` 时读取连接 session，并把命令准备成容量预留前可持有资源的 `PreparedCommand`。

源码位置：`CommandHandler.parse(CommandArgs)` 的返回类型（`yierdis-command/.../command/api/CommandHandler.java`）。

### `PreparedCommand`

容量预留前完成读取和准备、预留后执行一次的工作单元。它提供 `reservationShape()`、`validateBeforeExecute()`、`execute(CommandSession)` 与 `replyProtocolVersion()`；若校验结果为 stale，执行器关闭并重新准备。其回复引用的资源必须保留到 `RedisReplyRenderer` 消费完成。

源码位置：`yierdis-server/yierdis-server-api/.../execution/api/PreparedCommand.java`。

### command variant

同一个 command 的 option、subcommand 或重要语义分支，例如 `SET / NX`、`SCAN / MATCH`、`MEMORY / STATS`。这些分支应由对应命令家族测试覆盖，测试选择看 [`testing-and-debugging.md`](./testing-and-debugging.md)。

## Session / Runtime

### `EngineSession`

每条连接的 `CommandSession` owner，持有 DB index、client name、RESP version、connection stats view 和事务队列。名称中的 Engine 只是保留的包/类型名；它不拥有命令解析、分发、执行或回复渲染。

源码位置：`yierdis-server/yierdis-server/.../execution/engine/EngineSession.java`（内部事务状态 `EngineSession.DefaultTransactionState`）。

### `DbEngine`

DB 的能力聚合接口，直接提供 `strings()`、`hashes()`、`lists()`、`sets()`、`zsets()`、`hll()`、`keyspace()`、`ttl()`、`memoryUsage(...)`、`memoryStats()`、`objectEncoding(...)` 和 `flushDb()`。主动过期清理交给 runtime maintenance 调度。command 层依赖它，不依赖 `YierdisDb` internal。

源码位置：`yierdis-db/.../storage/api/DbEngine.java`（runtime 扩展接口 `RuntimeDbEngine` 追加 `bindToCurrentThread`、`runMaintenance`、`reconcileAccounting`、`shutdown`）。详见 [`db-internals.md`](./db-internals.md)。

### `YierdisDb`

单个 in-memory DB 的具体实现，拥有 keyspace、TTL、root/value、memory ledger 和 mutation executor。它属于 DB internal，不是 command 层 API。

源码位置：`yierdis-db/.../storage/memory/YierdisDb.java`（配套 `YierdisDbKernel`、`YierdisDbStorage`、`YierdisDbMemoryReporter`）。

### `YierdisInstance`

runtime 中的多 DB 容器，掌管 DB 生命周期、owner thread 绑定、resources 和 close 顺序。

源码位置：`yierdis-server/yierdis-server/.../runtime/embedded/YierdisInstance.java`（资源 `YierdisInstanceResources`，实例内访问 `YierdisInstanceRuntimeAccess`）。详见 [`request-execution-flow.md`](./request-execution-flow.md)。

### owner thread

唯一允许访问 DB 的命令执行线程。Netty I/O 线程只提交请求，真正读写 DB 发生在 owner thread 上。DB 未绑定或从非 owner 线程访问都会抛异常。

源码位置：`yierdis-db/.../storage/memory/DbThreadGuard.java`（`bindToCurrentThread()`、`checkDbAccess()`、`checkCurrentThread()`，实现 `MemoryOwner`），被 `YierdisDb` 用于线程约束。详见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### maintenance tick

runtime 周期任务入口，用于驱动过期清理、hash 表 rehash、native defrag 等后台维护动作。它必须尊重 owner thread 和配置预算。

源码位置：`RuntimeDbEngine.runMaintenance()` 与 `YierdisDbKernel.reclaimExpired(...)`；预算参数来自 `--cleanupIntervalMillis`、`--expireCleanupTimeLimitMillis`、`--nativeDefrag*`。详见 [`configuration-and-operations.md`](./configuration-and-operations.md)。

## DB / Keyspace / TTL

### keyspace

DB 内 key 到 entry handle/record 的索引。生产实现是 `YierdisDbStorage` 创建的 `NativeKeyDirectory`。

源码位置：`yierdis-db/.../storage/memory/internal/keyspace/NativeKeyDirectory.java`。

### TTL deadline

`EntryRecord.expireAtMillis` 中保存的绝对过期时间。它是当前唯一 TTL 状态；主动清理消费按过期时间排序的派生 `ExpiresIndex`（不再扫描 key directory），并用派生 `expireCount` 快速判断是否有 TTL 工作。

源码位置：`yierdis-db/.../storage/memory/internal/entry/EntryRecord.java`（`expireAtMillis`）、`.../internal/keyspace/ExpiresIndex.java`。

### maxmemory

运行时内存上限和驱逐策略的统称。写路径会在 mutation 前估算、预留、必要时驱逐，失败则 rollback，避免半写入。

源码位置：`YierdisDbMaxmemorySupport`、`YierdisGlobalMaxmemoryGovernor`、`internal/ledger/YierdisDbMutationExecutor.java`。

### reservation

一次写入在 mutation 前按上界预留的内存额度 token。reserve 成功才提交，异常路径靠 `commit()` / rollback 收敛不变量。

源码位置：`yierdis-db/.../storage/memory/internal/ledger/MemoryReservation.java`（`reservedBytes()`），由 `YierdisDbMutationExecutor` 的 `reserveNormalPlan(...)` 产出。请求侧的对应概念是 `PreparedCommand.reservationShape()`。

### retained bytes

对象当前持有、仍需计入生命周期或 maxmemory 的字节数。它不一定等同于本次写入的参数大小，因为 native spare capacity、root metadata 和 heap topology 都可能参与计算。

请求侧的 `ExecutionRequest.retainedBytes()` 是 heap request footprint 估算，口径由 `HeapRequestFootprint` 定义：请求对象、外层 argv 数组与引用槽位、每个非空参数的数组头和按 8 对齐的 payload 都计入，并非只按参数 payload 长度求和。RESP array path、inline path 和 `ByteArrayExecutionRequest` 各工厂方法共用这一口径，executor queued bytes、连接 pending bytes 和事务 queue bytes 直接消费该值。

源码位置：`yierdis-common/.../common/memory/HeapRequestFootprint.java`、`ExecutionRequest.retainedBytes()`。

## Data Model

### value type

用户可见的逻辑类型，例如 string、list、hash、set、zset。`TYPE` 命令返回的是这类语义。

源码位置：`yierdis-db/.../storage/api/ValueType.java`。

### value encoding

内部编码，例如 raw string、integer-like string、packed hash、intset、skiplist。`OBJECT ENCODING` 关注的是这个层面。

源码位置：`yierdis-db/.../storage/memory/internal/value/ValueEncoding.java`、`YierdisEncodingThresholds.java`。

### HLL string

在 Yierdis 中，HyperLogLog 是带特定 header/payload 的 string 语义值，没有独立的 `ValueType`。相关命令是 `PFADD`、`PFCOUNT`、`PFMERGE`。payload 使用 Redis 兼容格式（`HYLL` magic header、sparse/dense 编码、MurmurHash64A、tau/sigma 估计器），相同 member 得到与 Redis 相同的计数；不读取旧的 Yierdis 私有 `HLL1` payload。

源码位置：`yierdis-db/.../storage/memory/internal/value/YierdisHyperLogLog.java`。

### root / value

root 是 entry record 指向的 family root，例如 `StringRoot`、`ListRoot`；value 是集合内部编码对象，例如 `ListValue`、`HashValue`、`SetValue`、`ZSetValue`。

源码位置：`yierdis-db/.../storage/memory/internal/entry/`（`StringRoot`、`ListRoot`、`HashRoot`、`SetRoot`、`ZSetRoot`）、`.../internal/value/`（`ListValue`、`HashValue`、`SetValue`、`ZSetValue`）。详见 [`db-internals.md`](./db-internals.md)。

## Executor / Backpressure

### backpressure

当 executor backlog、连接队列或输出缓冲超过预算时，系统暂停或限制继续接收请求的机制。用来保护 owner thread 和内存预算。

源码位置：`yierdis-server/yierdis-server/.../execution/executor/CommandExecutor.java` 及 `CommandExecutorSubmitter`。详见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### backlog budget

executor 用来限制待执行请求数量或字节数的预算。提交路径会根据预算决定接受、拒绝或触发背压。

源码位置：`CommandExecutorConfig`（队列容量 `--executorQueueCapacity`、字节上限 `--executorQueueMaxBytes`、水位 `--backpressureBytesHigh/Low`）。

### drain loop

owner thread 上从队列取任务并执行的循环。它受 drain budget 和 scheduling policy 控制。

源码位置：`yierdis-server/yierdis-server/.../execution/executor/CommandExecutorDrainLoop.java`（预算来自 `--executorMaxDrain`、`--executorDrainMillis`）。

### scheduling policy

executor 在多连接之间选择任务的策略，目前是 `GLOBAL` 和 `FAIR`。`FAIR` 会轮转可运行连接，避免某个卡在自身 reply 容量上的连接挡住其它有容量的连接；`GLOBAL` 保持全局 FIFO 头。

源码位置：`yierdis-server/yierdis-server/.../execution/executor/SchedulingPolicy.java`。

## Bytes

### `BytesView`

带长度的随机访问只读接口，要求 `length()`、`getByte(index)` 和 `getBytes(...)`。接口注释要求实现视为短生命周期对象，不得存入 DB。

源码位置：`yierdis-common/.../bytes/BytesView.java`。

### `BytesSlice`

继承 `BytesView`，增加 `writeTo(BytesSink)`。`NativeBytesSlice` 用 offset 和 length 描述 native handle 上的一段；接口本身没有 offset。

源码位置：`yierdis-common/.../bytes/BytesSlice.java`、`yierdis-db/.../internal/value/NativeBytesSlice.java`。

### `BytesSink`

写入端口，只承诺接收 bytes，不承担 source ownership。协议编码器和 reply writer 用它做流式写出。

源码位置：`yierdis-common/.../bytes/BytesSink.java`。

### materialize

把 view/slice 复制成新的 heap byte[]。有些协议回包、测试断言或跨生命周期保存必须 materialize，但 hot path 会尽量避免。

## FFM / Native Memory

### FFM

JDK 25 `java.lang.foreign` API。Yierdis 的 region 分配、`YierdisFfmRegion` 读写和 entry record layout 见 [`ffm-primer.md`](./ffm-primer.md)。

### stable handle

指向 stable-memory backend 中稳定对象的句柄。对象可以在 memory 内移动（realloc、defrag），但只要 handle 有效，其 `(allocatorId, localRaw)` 身份保持不变。

源码位置：`yierdis-db/.../memory/api/NativeHandle.java`。

### `EntryHandle`

DB entry 的稳定句柄包装。它只要求内部 `NativeHandle` 非 null，不检查 `NativeObjectKind`。`ENTRY_RECORD` 由 `EntryTable` 分配和读写时使用。

源码位置：`yierdis-db/.../storage/memory/internal/entry/EntryHandle.java`。

### `ValueHandle`

DB value/root 的稳定句柄包装，包含 null sentinel 约定。entry record 常用它指向具体 value/root。

源码位置：`yierdis-db/.../storage/memory/internal/entry/ValueHandle.java`。key 侧对应 `KeyHandle`（`.../storage/api/KeyHandle.java`、`.../internal/key/AllocatorKeyHandle.java`）。

### `NativeHandle`

stable-memory backend 的 `(allocatorId, localRaw)` paired identity。`localRaw` 只能由所属 backend 解释；FFM 私有 codec 才在其中编码 domain、kind、slot/generation。它不是裸地址。

源码位置：`yierdis-db/.../memory/api/NativeHandle.java`。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

### object table

native allocator 中记录对象 metadata、generation、pin 状态和 quarantine 状态的表。承担 stale handle 和 wrong kind/domain 检查。

源码位置：`yierdis-db/.../memory/foreign/YierdisNativeObjectTable.java`（元数据 record `YierdisNativeObjectMeta`，字段 `allocEpoch` / `freeEpoch` / pinCount / generation）。

### stable memory backend

提供稳定 handle、resolve view、realloc、epoch、pin/quarantine 和 active defrag 的 owner-bound backend。region ownership 是 FFM 实现的内部职责；对象移动时完整 handle 保持稳定。

源码位置：`yierdis-db/.../memory/api/StableMemoryBackend.java`（实现 `YierdisFfmStableMemoryBackend`）。

### epoch

后端回收屏障里的一个活动周期。`beginEpoch()` 打开一个 `NativeEpochScope`；scope 关闭前，该 epoch 保护的退役存储不得回收，关闭只撤销屏障、不保证立即回收。它用来避免在遍历期间把仍在使用的存储复用了。

源码位置：`yierdis-db/.../memory/api/NativeEpochScope.java`、`YierdisFfmStableMemoryBackend.beginEpoch()` / `canReclaim(...)`。

### allocation scope

跟踪同一后端在单次活动分配范围内新建对象的 scope。`promote()` 提交对象并把后续释放责任交给调用方；`abort()`（及默认 `close()`）释放仍被跟踪的对象，因此成功路径必须显式 `promote()`。

源码位置：`yierdis-db/.../memory/api/NativeAllocationScope.java`、`StableMemoryBackend.beginAllocationScope()`。

### pin

临时固定 native 对象，保证 view 使用期间对象不会被释放或移动。pin 期间 free 会进入 quarantine。每次成功 `pin` 必须由同一 owner 配对 `unpin`。

源码位置：`StableMemoryBackend.pin(NativeHandle)` / `unpin(NativeHandle)`、`YierdisNativeObjectTable` 的 pinCount 列。

### quarantine

已请求释放但因为 pin 或 epoch 仍不能复用的对象/slot 暂存区。解除保护后才能通过 `releaseQuarantined(...)` 真正回收。

源码位置：`YierdisNativeObjectTable`（状态 `STATE_FREED_QUARANTINED`，方法 `free(..., forceQuarantine)`、`releaseQuarantined(...)`、`unpin(..., releaseQuarantinedOnZero)`）。

### active defrag

在预算内移动可移动 native 对象、减少碎片并更新 object table metadata 的维护动作。预算由 `--nativeDefragMaxMoveBytes`、`--nativeDefragMaxObjects`、`--nativeDefragTimeLimitMillis` 控制，在 maintenance tick 内运行。

源码位置：`StableMemoryBackend.defragCycle(...)`、`YierdisFfmStableMemoryBackend` 里的 `YierdisNativeBlock` 移动路径。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## Memory Accounting

### `YierdisMemoryStats`

DB 级内存摘要的只读视图，是 `MEMORY STATS` 与 `INFO memory` 的数据源。区分 heap 估算与 ledger 预留：`ledger_used_bytes` 绑定 `heapDataBytesEstimate`，`ledger_reserved_bytes` 绑 ledger `reservedBytes`。

源码位置：`yierdis-db/.../storage/api/YierdisMemoryStats.java`，由 `YierdisDbMemoryReporter.memoryStats()` 产出。

### `MemoryUsageSnapshot`

一次物理内存快照，`effectiveBytesForMaxmemory()` 是快照本身的物理上界。admission 实际比较的是**不含 reservation 的物理快照**，报告里的 `effective_used_bytes_for_maxmemory` 才在其上再加 ledger `reservedBytes`——两者不要混用。

源码位置：`yierdis-common/.../common/memory/MemoryUsageSnapshot.java`。详见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## Testing

### architecture guard

检查少量无法由编译器直接表达的 API 形状与可见性约束，例如 DB 只有一个公共工厂形状、实现类型保持 package-private、parse 阶段不访问服务。模块依赖方向由 Maven 模块图和 Java 编译器约束。

源码位置/测试：`yierdis-tests/.../integration/command/CommandParseIsolationTest.java`、`yierdis-server/.../app/server/ServerCommandParseIsolationTest.java`、`yierdis-tests/.../storage/memory/YierdisDbArchitectureGuardTest.java`、`yierdis-server/.../runtime/embedded/DbEngineReadWriteBoundaryTest.java`。