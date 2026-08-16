# Bytes 抽象与有界流式路径

本文解释 Yierdis 为什么有一套独立于 Netty 和 DB 的 bytes 抽象，以及这些抽象如何减少无意义复制。

Yierdis 的 bytes 抽象不是为了把 `byte[]` 包一层对象，而是为了让 protocol、execution、DB、native value 和 Netty write-back 共享一套 Netty-free contract；能流式写出时不强制完整结果 materialization，必须 materialize 时又把复制边界写清楚。

## 为什么不是直接传 byte[]

直接传 `byte[]` 有两个问题。

第一，所有上层都会默认“我拥有这段数组”。这会让协议 decode、transaction replay、DB lookup、DB persistence 和 tests 混在同一种形状里，最后只能靠防御性 copy 保安全。

第二，`byte[]` 表达不了 native source 的随机读取和流式写出能力。即使下游可以按 slice 或按 sink 分块处理，上游也已经把数据收缩成完整 heap array。

Yierdis 选择中立 bytes 层：

- lookup API 用短生命周期只读 view 表达输入；当前 DB lifecycle 边界仍会 materialize heap key copy。
- 写入值用 slice，把“读取”和“写出”能力同时交给 DB。
- reply 用 sink，把协议编码和 Netty `ByteBuf` 解耦。
- native value 可以通过同一组中立接口流式写出，但复制策略仍由具体实现和 ownership 边界决定。

## 核心接口

`BytesSource` 是最小随机访问只读接口，只要求 `getByte(index)` 和 `getBytes(index, dst, dstOff, len)`。它不转移底层数据所有权，也不承诺线程安全、连续内存布局或对象生命周期。

`BytesView` 在 `BytesSource` 上增加 `length()`，主要用于 key 等请求级 lookup 输入。接口注释明确要求它是短生命周期对象，不应被直接存入 DB。

`BytesSlice` 继承 `BytesView`，再增加 `writeTo(BytesSink out)`。它既能被随机读取，也能把自己流式写给 sink，是 string value、bulk reply 和 off-heap slice 的关键形状。

`BytesSink` 是最小写接口，只承诺 `writeBytes(byte[], off, len)`。实现必须在方法返回前消费指定范围，且不得保留或修改传入数组；协议编码器、reply writer 和测试 sink 都可以依赖它。

## `ExecutionRequest` 视图族怎么选

网络生产路径由 `RespRequestDecoder` 直接构造 `RetainedRespExecutionRequest`：它拥有已经 materialize 完成的 heap `byte[]` argv 和一份脱离 Netty 对象的 request-memory lease。`retain()` 只增加 lease 引用，不复制 argv；请求跨过 decoder 生命周期后仍保有稳定 argv 与 admission 元数据，同时不会在协议边界做第二次逐参数复制。

`ByteArrayExecutionRequest` 是 heap 输入、`copyOf(...)` snapshot 和 `fromUtf8(...)` 测试构造使用的实现。它自己负责 retained bytes 的饱和计数，不会因为 `int` 回绕变成负数。

`wrapReadOnly(...)` 只适合调用方已经拥有 argv、并且能持续遵守只读约定的场景。`readOnlyByteArray(...)` 是 heap-backed immutable request 的快速读路径，不等于把内部数组的所有权暴露给外部。`fromUtf8(...)` 只是测试、CLI 和固定输入构造的便利函数。

## `BytesView` 与 `BytesSlice` 的 ownership

`BytesView` / `BytesSlice` 是短生命周期只读输入；跨命令、跨线程、跨队列或持久保存前必须取得独立所有权。
`BytesSink.writeBytes(...)` 在返回前同步消费输入范围，不保留传入数组，也不表示 source ownership 转移。

## 协议层如何使用 bytes

RESP decode 后直接得到 `RetainedRespExecutionRequest`。它保存 `byte[][] argv`、payload retained bytes 和 reference-counted request-memory lease；网络层不再经过协议 DTO 或 adapter。decoder 在 bulk/inline 命令完整前就对 argv、payload 和 request 固定开销完成 admission，因此 heap materialization 是有意的 ownership snapshot：请求跨过 Netty decoder 生命周期后，需要稳定 argv 和 admission 计数供 executor 排队、budget 和 transaction 逻辑使用。

reply 编码方向相反。`RespReplyWriter.bulkString(BytesSlice)` 先写 RESP bulk header，再同步调用 `BytesSlice.writeTo(out)` 把内容写入 `BytesSink`，最后写 CRLF。生产路径中的 sink 通过 reply reservation 把输出限制在有界 `ByteBuf` chunk 内。

## DB lookup 和写路径如何使用 bytes

DB API 的很多 read ops 接受 `BytesView`，例如 `StringReadOps`、`TtlReadOps`、`KeyspaceReadOps`、`MemoryOps`。这让 command/DB contract 保持 Netty-free；当前 lookup 的 ownership copy 边界见下文。

当前实现里，`YierdisDbKeyLifecycle` 在 `BytesView` 进入 key directory 前会调用 `YierdisDb.toByteArray(keyView)` materialize 一个 heap `byte[]`。这是因为 `NativeKeyDirectory` 的 lookup API 当前是 `byte[]` based，例如 `get(byte[])`、`getKeyHandle(byte[])` 和 `compute(byte[], ...)`。这份 heap copy 是今天的 ownership/lifetime 边界，不应该写成“lookup 已经避免 heap key 生成”。

新 key 持久化会把 key bytes 存成 allocator-backed `KEY_BYTES`。`SCAN` discovery 只保留 cursor、目录元数据和 epoch，输出时重放同一段目录并把 key 暴露为 native-backed slice；snapshot、`RANDOMKEY`、显式 `byte[]` 和 introspection API 才会为了独立 ownership 或诊断生成 heap copy。这些复制点和 lifecycle 边界 copy 一样，都是有意的 lifetime/ownership 边界。

写路径中，`StringWriteOps.set(...)`、`append(...)` 和 HLL 内部逻辑接收 `BytesSlice`。这让 command 层把 value 作为 slice 交给 DB，由 `StringRoot` 或对应 type root 写入 allocator-backed `STRING_BYTES` 或 collection native payload handles。slice 的重点是延后复制决策，而不是承诺零拷贝持久化。

集合读路径大量使用 `BulkStringSink`。这个 storage API 中的协议无关输出端口支持 `bulkString(byte[])`、`bulkString(byte[], off, len)`、`bulkString(BytesSlice)`、`bulkStringLongAscii(long)` 和 null bulk；`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 等可以逐项 emit 到 sink，避免先组装完整 `List<byte[]>` 再交给协议层。

## 语义回复和 Netty 写回

命令层把 storage source 包装成 `RedisReply.BulkString`、`ByteSequence` 或 `ByteMap`，并把 source owner 挂在 `PreparedCommand` 上。executor 先按 reply shape 完成 slot reservation，执行得到 `CommandResult` 后才创建 `RedisReplyWriter`；`RedisReplyRenderer` 随后同步运行 payload emitter。当前 Netty sink 按固定上限分配 `ByteBuf` chunk，renderer 返回后 prepared owner 才会关闭。

写回路径大致是：

```text
CommandResult / RedisReply
  -> RedisReplyRenderer
  -> RedisReplyWriterFactory / RedisReplyWriter
  -> RespReplyWriter
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> bounded ByteBuf chunks
  -> ConnectionReplySequencer
  -> channel.write(...)
```

`RedisReply` 的 payload emitter 和 `BulkStringSink.bulkString(BytesSlice)` 是关键入口。heap `byte[]` 仍然可用，但不是唯一形状；native string、collection range 和 computed ASCII number 都可以在中央 renderer 调用期间流式写出。reply reservation、source ownership、Netty ownership 和顺序写回的细节见 [`netty-adapter-design.md`](./netty-adapter-design.md)。

## 流式路径和 materialization fallback

流式路径主要出现在这些地方：

- API 边界使用 `BytesView`，让 command/DB contract 不依赖 Netty；当前 DB lifecycle lookup 仍会 materialize heap `byte[]`。
- `BytesSlice.writeTo(BytesSink)` 可以流式写出 value，避免 whole-result materialization，但具体实现仍可能使用有界 heap scratch copy。
- `NativeBytesSlice` 在同步写出期间 pin allocator handle，写完后 unpin，避免为了 `LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 这类流式读先 materialize `List<byte[]>`。
- `SCAN` window 保留 cursor、目录 generation/capacity、epoch 和匹配计数；length/emit 阶段重放相同物理 slot 范围，并把匹配 key 包装为 native-backed slice。
- `ReplyReservationSink` / `BoundedChunkedReplySink` 在分配前取得额度，并把编码结果限制在有界 `ByteBuf` chunk 内。
- `BulkStringSink` 让 collection range 边遍历边输出。

fallback 也同样重要。以下 heap materialization 是有意的：

- protocol snapshots：`RetainedRespExecutionRequest` / `ExecutionRequest` 需要稳定 argv 跨过 decoder 生命周期和 executor queue。
- DB lifecycle lookup：当前 `YierdisDbKeyLifecycle` 用 `YierdisDb.toByteArray(...)` 把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`。
- transaction replay：事务队列通过 `ExecutionRequest.retain()` 取得独立所有权；生产网络实现共享不可变 argv 和 reference-counted request-memory lease，默认接口实现才使用 heap copy。
- explicit materialization：snapshot、`RANDOMKEY`、显式 `byte[]` API 和 `MEMORY` / object 类 introspection 需要构造独立返回值或诊断对象，不能把 native view 泄漏给调用方。
- tests：测试经常用 heap arrays 和 recording sinks 断言内容，这是可读性和确定性的取舍。
- ownership-returning DB APIs：要求 owned `byte[]` 或集合快照的显式 API 会复制；命令 `GET`、`HGET`、pop 和 `SET ... GET` 则持有 retained native-backed view/slice，直到同步 reply rendering 完成后释放。
- unavoidable fallback paths：JSON/base64/escape、短生命周期输入持久化、需要排序/聚合或独立所有权的结果，都可能必须复制。

判断一条路径是否合理，不是看它有没有复制，而是看复制是否发生在 ownership、lifetime 或格式转换真正需要的位置。

## 和 native memory 的关系

bytes 抽象不是 native allocator。它只描述“如何读一段 bytes”和“如何把一段 bytes 写给 sink”。native object 的 lifetime、stable handle、pin、epoch、quarantine、`realloc` 和 active defrag 仍属于 allocator 文档，见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

当前 allocator-backed facts 要分清：

- `NativeKeyDirectory` 持久化 key bytes 为 allocator-backed `KEY_BYTES`。
- `StringRoot` 持久化 string payload 为 allocator-backed `STRING_BYTES`。
- entry metadata、collection root records 和 collection internal bytes 都是 allocator-backed objects。
- list/hash/set/zset 的 streaming reply path 使用 native-backed `BytesSlice` value；当前实现可通过有界 heap scratch 和 reply chunk 完成写出，不需要完整结果 materialization。

因此 `BytesView` / `BytesSlice` 可以帮助 native 和 heap 路径共享 API，但它们本身不保证数据 off-heap，也不保证零拷贝。它们保证的是短生命周期 view、流式写出和 adapter 边界清晰。
