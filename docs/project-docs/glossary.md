# 术语表

本文解释 Yierdis 文档和源码里反复出现的术语。每个术语都尽量指向最相关的专题文档。

## Request / Reply Path

### `ExecutionRequest`

命令执行层看到的 argv bytes 视图。它提供 `argc()`、参数长度、null 判断和复制读取能力，但不暴露 RESP DTO。详见 [`protocol-reference.md`](./protocol-reference.md)。

### `ByteArrayExecutionRequest`

以 heap `byte[]` 保存参数的 `ExecutionRequest` 实现。测试、事务 replay 和部分适配路径会使用它。详见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

### `ExecutionRecord`

`dbIndex` + `ExecutionRequest` 的不可变 replay / change-event 快照。构造时会归一化 `dbIndex` 并复制请求，避免把 mutable request 引用带进事务队列或 change sink。详见 [`transaction-and-replay.md`](./transaction-and-replay.md) 和 [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)。

### `RedisReplyWriter`

命令层唯一的 Redis reply 语义出口。handler 只调用 `simpleString`、`bulkString`、`integer`、`arrayHeader`、`mapHeader`、`error` 等 Redis reply 语义方法，不拼 RESP bytes。详见 [`commands-and-data-model.md`](./commands-and-data-model.md)。

### `RespReplyWriter`

`RedisReplyWriter` 的 RESP 实现，负责把语义回包编码成 RESP2 或基础 RESP3 bytes。详见 [`protocol-reference.md`](./protocol-reference.md)。

## Command Layer

### `CommandSpec`

命令定义单元，包含名称、arity、key metadata、handler 和 `MULTI` 限制。新增命令时它是注册表里的核心对象。详见 [`commands-and-data-model.md`](./commands-and-data-model.md)。

### `CommandRegistry`

命令名到 `CommandSpec` 的注册表。`YierdisFastCommandProcessor` 通过它做 unknown command 判断和分发。详见 [`core-logic-index.md`](./core-logic-index.md)。

### `CommandContext`

单次命令执行的上下文，携带 `RedisReplyWriter`、`ServerSession`、当前 DB 路由和 mutation outcome。它把 handler 和执行环境连接起来。

### command variant

同一个 command 的 option、subcommand 或重要语义分支，例如 `SET / NX`、`SCAN / MATCH`、`MEMORY / STATS`。这些分支应由对应命令家族测试覆盖，测试选择看 [`testing-and-debugging.md`](./testing-and-debugging.md)。

## Engine / Runtime

### `DbEngine`

DB 的能力聚合接口，提供 `reads()`、`writes()`、`expiration()`、`memory()`、`lifecycle()`。command 层依赖它，而不是依赖 `YierdisDb` internal。详见 [`db-internals.md`](./db-internals.md)。

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

### expire index

key 到过期时间的索引。它和 keyspace 共享 key/handle 生命周期，负责 TTL 查询、过期清理和相关内存记账。

### maxmemory

运行时内存上限和驱逐策略的统称。写路径会在 mutation 前估算、预留、必要时驱逐，失败时要 rollback，避免半写入。

### retained bytes

对象当前持有、仍需计入生命周期或 maxmemory 的字节数。它不一定等同于本次写入的参数大小，因为 native spare capacity、root metadata、TTL metadata 都可能参与计算。

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

### `DirectBytesSink`

`BytesSink` 的 direct-aware 扩展，暴露 writer cursor 和 memory address，但不会改变 source ownership。

### materialize

把 view/slice 复制成新的 heap byte[]。有些协议回包、测试断言或跨生命周期保存必须 materialize，但 hot path 会尽量避免。

## FFM / Native Memory

### FFM

JDK 25 `java.lang.foreign` API。Yierdis 的 native-memory runtime、blob store、keyspace、allocator 都建立在这个基础上。入门看 [`ffm-primer.md`](./ffm-primer.md)。

### `EntryHandle`

DB entry 的稳定句柄包装。它把 native raw handle 放进 DB entry domain/kind 语义里，避免误用其他 native handle。

### `ValueHandle`

DB value/root 的稳定句柄包装，包含 null sentinel 约定。它常用于 entry record 指向具体 value/root。

### `NativeHandle`

native allocator 的 packed handle，包含 domain、kind、slot/generation 等字段。它不是裸地址。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

### object table

native allocator 中记录对象 metadata、generation、pin 状态和 quarantine 状态的表。它负责 stale handle 和 wrong kind/domain 检查。

### stable native allocator

提供稳定 handle、resolve view、realloc、epoch、pin/quarantine 和 active defrag 的 allocator。对象移动时 handle 保持稳定。

### pin

临时固定 native 对象，保证 view 使用期间对象不会被释放或移动。pin 期间 free 会进入 quarantine。

### quarantine

已请求释放但因为 pin 或 epoch 仍不能复用的对象/slot 暂存区。解除保护后才能真正回收。

### active defrag

在预算内移动可移动 native 对象、减少碎片并更新 object table metadata 的维护动作。详见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## Testing

### architecture guard

保护模块依赖方向和边界的测试，例如 RESP DTO 不能进入 command 层、command 层不能依赖 DB internal。
