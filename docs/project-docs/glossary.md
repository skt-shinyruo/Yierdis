# 术语表

本文解释 Yierdis 文档和源码里反复出现的术语。每个术语都尽量指向最相关的专题文档。

## Request / Reply Path

### `ExecutionRequest`

命令执行层看到的 argv bytes 视图。它提供 `argc()`、参数长度、null 判断和复制读取能力，但不暴露 RESP DTO。详见 [`protocol-reference.md`](./protocol-reference.md)。

### `ByteArrayExecutionRequest`

以 heap `byte[]` 保存参数的 `ExecutionRequest` 实现。测试、显式 copy、change-event 公开快照和部分适配路径会使用它；事务 replay 保留原请求实现的 retained view。详见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

### `ExecutionRecord`

`dbIndex` + `CommandRecordView` 的 change-event 记录。公开构造入口会归一化 `dbIndex` 并复制 `ExecutionRequest`；runtime 的 `borrowed(...)` 入口交付 callback-scoped view。事务 replay 不使用该类型。详见 [`transaction-and-replay.md`](./transaction-and-replay.md) 和 [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)。

### `RedisReply`

命令结果中的协议无关语义回复。标量和普通 aggregate 直接保存值；bulk、byte sequence 和 byte map 保存长度、retained source bytes 与 emitter，由 owner 保持资源有效直到渲染完成。详见 [`commands-and-data-model.md`](./commands-and-data-model.md)。

### `CommandResult`

一次命令执行的完整结果，由 `RedisReply` 和 `closeAfterReply` 组成。`QUIT` 通过 `CommandResult.closeAfterReply(...)` 表达“回复发布后关闭”，而不是直接操作 writer 或连接。

### `RedisReplyRenderer`

执行器中唯一把 `RedisReply` 展开为 RESP-facing 写操作的组件。它递归渲染 aggregate，并消费 bulk、byte sequence、byte map 的语义流式 emitter。

### `RedisReplyWriter`

`RedisReplyRenderer` 面向 RESP 编码实现的输出端口，不是命令实现 API。命令返回 `CommandResult`，不调用 `simpleString`、`bulkString`、`arrayHeader` 等 writer 方法。

### `RespReplyWriter`

`RedisReplyWriter` 的 RESP 实现，负责把语义回包编码成 RESP2 或基础 RESP3 bytes。详见 [`protocol-reference.md`](./protocol-reference.md)。

## Command Layer

命令执行主链固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

### `CommandArgs`

`ExecutionRequest` 的统一命令参数视图，集中提供 argv bytes、ASCII option 比较和整数解析。handler 的解析阶段只依赖该类型，不访问 session、DB router 或 server provider。

### `CommandSpec`

命令最终注册单元，由 `CommandSyntax` 和 `CommandHandler` 组成。syntax 保存名称、arity、key metadata 和 `TransactionPolicy`；handler 的 `parse(CommandArgs)` 返回 `CommandInvocation`。

### `CommandRegistry`

命令名到 `CommandSpec` 的注册表。composition 阶段注册，sealed 后由 `CommandDispatcher` 只读查找。详见 [`core-logic-index.md`](./core-logic-index.md)。

### `CommandDispatcher`

执行请求到命令契约的统一入口。它检查命令名、null、arity 和事务策略，调用 `CommandSpec.handler().parse(CommandArgs)`，再调用 `CommandInvocation.prepare(session)` 返回 `PreparedCommand`。

事务 active 时，queueable 命令只运行 handler parse 做 preflight，不提前 prepare；容量预留成功后事务队列 retain 原 `ExecutionRequest`。`EXEC` replay 通过同一 dispatcher 准备子 `PreparedCommand`，并负责关闭子命令和 retained requests。

### `CommandInvocation`

解析成功后的命令调用描述。它通过 `prepare(CommandSession)` 读取连接 session，并把命令准备成容量预留前可持有资源的 `PreparedCommand`。

### `PreparedCommand`

容量预留前完成读取和准备、预留后执行一次的工作单元。它提供 `reservationShape()`、`validateBeforeExecute()` 和 `execute(CommandExecutionContext)`；若校验结果为 stale，执行器关闭并重新准备。其回复引用的资源必须保留到 `RedisReplyRenderer` 消费完成。

### `CommandExecutionContext`

回复容量预留成功后创建的一次命令执行作用域，只包含 `CommandSession` 和请求级 `MutationContext`；关闭时释放 mutation record。回复不通过该上下文写出，而由 `PreparedCommand.execute(...)` 返回 `CommandResult`。

### command variant

同一个 command 的 option、subcommand 或重要语义分支，例如 `SET / NX`、`SCAN / MATCH`、`MEMORY / STATS`。这些分支应由对应命令家族测试覆盖，测试选择看 [`testing-and-debugging.md`](./testing-and-debugging.md)。

## Session / Runtime

### `EngineSession`

每条连接的 `CommandSession` owner，持有 DB index、client metadata、认证状态、RESP version、connection stats view 和事务队列。名称中的 Engine 只是保留的包/类型名；它不拥有命令解析、分发、执行或回复渲染。

### `DbEngine`

DB 的能力聚合接口，提供 `reads()`、`writes()`、`memory()`、`lifecycle()`。TTL 查询与修改分别属于 typed read/write ops，主动过期清理由 runtime maintenance 调度。command 层依赖它，而不是依赖 `YierdisDb` internal。详见 [`db-internals.md`](./db-internals.md)。

### `YierdisDb`

单个 in-memory DB 的具体实现，拥有 keyspace、TTL、root/value、memory ledger 和 mutation executor。它属于 DB internal，不是 command 层 API。

### `YierdisInstance`

runtime 中的多 DB 容器，负责 DB 生命周期、owner thread 绑定、resources 和 close 顺序。详见 [`request-execution-flow.md`](./request-execution-flow.md)。

### owner thread

唯一允许访问 DB 的命令执行线程。Netty I/O 线程只提交请求，真正读写 DB 发生在 owner thread 上。详见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### maintenance tick

runtime 周期任务入口，用于驱动过期清理、defrag 等后台维护动作。它必须尊重 owner thread 和配置预算。详见 [`configuration-and-operations.md`](./configuration-and-operations.md)。

## DB / Keyspace / TTL

### keyspace

DB 内 key 到 entry handle/record 的索引结构。heap 路径和 FFM 路径分别有不同实现，但对上层暴露同一类查找、scan、删除语义。

### TTL deadline

`EntryRecord.expireAtMillis` 中保存的绝对过期时间。它是当前唯一 TTL 状态；主动清理直接扫描 key directory，并用派生 `expireCount` 快速判断是否有 TTL 工作。

### maxmemory

运行时内存上限和驱逐策略的统称。写路径会在 mutation 前估算、预留、必要时驱逐，失败时要 rollback，避免半写入。

### retained bytes

对象当前持有、仍需计入生命周期或 maxmemory 的字节数。它不一定等同于本次写入的参数大小，因为 native spare capacity、root metadata 和 heap topology 都可能参与计算。

## Data Model

### value type

用户可见的逻辑类型，例如 string、list、hash、set、zset。`TYPE` 命令返回的是这类语义。

### value encoding

内部编码，例如 raw string、integer-like string、packed hash、intset、skiplist。`OBJECT ENCODING` 关注的是这个层面。

### HLL string

HyperLogLog 在 Yierdis 中不是独立 `ValueType`，而是带特定 header/payload 的 string 语义值。相关命令是 `PFADD`、`PFCOUNT`、`PFMERGE`。

### root / value

root 是 entry record 指向的 family root，例如 `StringRoot`、`ListRoot`；value 是集合内部编码对象，例如 `ListValue`、`HashValue`、`SetValue`、`ZSetValue`。详见 [`db-internals.md`](./db-internals.md)。

## Executor / Backpressure

### backpressure

当 executor backlog、连接队列或输出缓冲超过预算时，系统暂停或限制继续接收请求的机制。目的是保护 owner thread 和内存预算。详见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### backlog budget

executor 用来限制待执行请求数量或字节数的预算。提交路径会根据预算决定接受、拒绝或触发背压。

### drain loop

owner thread 上从队列取任务并执行的循环。它受 drain budget 和 scheduling policy 控制。

### scheduling policy

executor 在多连接之间选择任务的策略，目前文档中常见的是 `GLOBAL` 和 `FAIR`。

## Bytes

### `BytesView`

只读 bytes 视图，可以来自 heap byte[] 或 off-heap/native memory。它避免在读路径上过早 materialize。

### `BytesSlice`

带 offset/length 的 bytes 片段，常用于写路径把参数或 native slice 传给 DB。

### `BytesSink`

写入端口，只承诺接收 bytes，不承担 source ownership。协议编码器和 reply writer 用它做流式写出。

### materialize

把 view/slice 复制成新的 heap byte[]。有些协议回包、测试断言或跨生命周期保存必须 materialize，但 hot path 会尽量避免。

## FFM / Native Memory

### FFM

JDK 25 `java.lang.foreign` API。Yierdis 的 native-memory runtime、blob store、keyspace、allocator 都建立在这个基础上。入门看 [`ffm-primer.md`](./ffm-primer.md)。

### `EntryHandle`

DB entry 的稳定句柄包装。它保留完整 `NativeHandle`，并约束 `ENTRY_RECORD` 语义，避免误用其他 object kind。

### `ValueHandle`

DB value/root 的稳定句柄包装，包含 null sentinel 约定。它常用于 entry record 指向具体 value/root。

### `NativeHandle`

stable-memory backend 的 `(allocatorId, localRaw)` paired identity。`localRaw` 只能由所属 backend 解释；FFM 私有 codec 才在其中编码 domain、kind、slot/generation。它不是裸地址。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

### object table

native allocator 中记录对象 metadata、generation、pin 状态和 quarantine 状态的表。它负责 stale handle 和 wrong kind/domain 检查。

### stable memory backend

提供稳定 handle、resolve view、realloc、epoch、pin/quarantine、region 和 active defrag 的 owner-bound backend。生产实现是 `YierdisFfmStableMemoryBackend`；对象移动时完整 handle 保持稳定。

### pin

临时固定 native 对象，保证 view 使用期间对象不会被释放或移动。pin 期间 free 会进入 quarantine。

### quarantine

已请求释放但因为 pin 或 epoch 仍不能复用的对象/slot 暂存区。解除保护后才能真正回收。

### active defrag

在预算内移动可移动 native 对象、减少碎片并更新 object table metadata 的维护动作。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## Testing

### architecture guard

保护模块依赖方向和边界的测试，例如 RESP DTO 不能进入 command 层、command 层不能依赖 DB internal。
