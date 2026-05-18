# Bytes And Fast Paths

本文解释 Yierdis 为什么有一套独立于 Netty 和 DB 的 bytes 抽象，以及这些抽象如何减少无意义复制。

## 先记住一句话

Yierdis 的 bytes 抽象不是为了把 `byte[]` 包一层对象，而是为了让 protocol、execution、DB、off-heap value 和 Netty write-back 共享一套 Netty-free contract；能流式写出时不强制 heap copy，必须 materialize 时又把复制边界写清楚。

## 为什么不是直接传 byte[]

直接传 `byte[]` 有两个问题。

第一，所有上层都会默认“我拥有这段数组”。这会让协议 decode、transaction replay、DB lookup、DB persistence 和 tests 混在同一种形状里，最后只能靠防御性 copy 保安全。

第二，`byte[]` 表达不了 direct/off-heap source 或 Netty output buffer 的能力。即使下游可以按地址、按 slice 或按 sink 流式处理，上游也已经把数据收缩成 heap array。

Yierdis 选择中立 bytes 层：

- lookup API 用短生命周期只读 view 表达输入；当前 DB lifecycle 边界仍会 materialize heap key copy。
- 写入值用 slice，把“读取”和“写出”能力同时交给 DB。
- reply 用 sink，把协议编码和 Netty `ByteBuf` 解耦。
- direct/off-heap 优化只在实现支持时启用，不污染 API 默认语义。

## 核心接口

`BytesSource` 是最小随机访问只读接口，只要求 `getByte(index)` 和 `getBytes(index, dst, dstOff, len)`。它还可选暴露 `hasMemoryAddress()` / `memoryAddress()`，用于实现知道自己有稳定地址时提供 fast path。

`BytesView` 在 `BytesSource` 上增加 `length()`，主要用于 key 等请求级 lookup 输入。接口注释明确要求它是短生命周期对象，不应被直接存入 DB。

`BytesSlice` 继承 `BytesView`，再增加 `writeTo(BytesSink out)`。它既能被随机读取，也能把自己流式写给 sink，是 string value、bulk reply 和 off-heap slice 的关键形状。

`BytesSink` 是最小写接口，只承诺 `writeBytes(byte[], off, len)`。协议编码器、reply writer 和测试 sink 都可以依赖它。

`DirectBytesSink` 扩展 `BytesSink`，暴露 `ensureWritable(len)`、`writerIndex()`、`writerIndex(int)`、`hasMemoryAddress()` 和 `memoryAddress()`。只有需要 direct/off-heap fast path 的实现才依赖这些增强能力。

`NettyByteBufSink` 位于 Netty adapter 层，把 `ByteBuf` 包成 `DirectBytesSink`，并保留 `unwrap()`。上层仍看到 bytes contract；当下游确实是 direct `ByteBuf` 时，slice 实现可以利用 writer index 或 memory address。

`BulkStringSink` 是 storage API 里的协议无关 bulk string 输出端口，支持 `bulkString(byte[])`、`bulkString(byte[], off, len)`、`bulkString(BytesSlice)`、`bulkStringLongAscii(long)` 和 null bulk。list/hash/set/zset range 这类 DB 读路径用它向 reply 层流式发元素。

## 协议层如何使用 bytes

RESP decode 后的协议对象是 `RespCommandRequest`。它内部保存 `byte[][] argv` 和 `retainedBytes`，提供两种构造：

- `copyOf(List<byte[]>)`：复制输入，适合外部 list ownership 不明确的路径。
- `wrapReadOnly(byte[][], retainedBytes)`：包装已经 owned 的 argv，调用方之后必须按只读约定处理。

Netty decoder 在 bulk/inline 命令完整后构造 `RespCommandRequest`，`RespCommandAdapter` 再把它转成 execution 层的 `ExecutionRequest`。这里的 heap materialization 是有意的 protocol adaptation snapshot：请求跨过 Netty decoder 生命周期后，需要一份稳定 argv 和 retained bytes，供 executor 排队、budget 和 transaction 逻辑使用。

reply 编码方向相反。`RespReplyWriter.bulkString(BytesSlice)` 先写 RESP bulk header，再调用 `BytesSlice.writeTo(out)` 把内容写入 `BytesSink`。当 `out` 是 `NettyByteBufSink` / `DirectBytesSink` 时，底层可以减少中间 `byte[]`。

## DB lookup 和写路径如何使用 bytes

DB API 的很多 read ops 接受 `BytesView`，例如 `StringReadOps`、`TtlReadOps`、`KeyspaceReadOps`、`MemoryOps`。这让 command/DB contract 保持 Netty-free，也给未来 direct lookup 优化留出接口空间。

当前实现里，`YierdisDbKeyLifecycle` 在 `BytesView` 进入 key directory 前会调用 `YierdisDb.toByteArray(keyView)` materialize 一个 heap `byte[]`。这是因为 `NativeKeyDirectory` 的 lookup API 当前是 `byte[]` based，例如 `get(byte[])`、`getKeyHandle(byte[])` 和 `compute(byte[], ...)`。这份 heap copy 是今天的 ownership/lifetime 边界，不应该写成“lookup 已经避免 heap key 生成”。

新 key 持久化会把 key bytes 存成 allocator-backed `KEY_BYTES`；SCAN、snapshot 和显式 introspection 仍会为了输出或诊断生成 heap copy。这些复制点和 lifecycle 边界 copy 一样，都是有意的 lifetime/ownership 边界。

写路径中，`StringWriteOps.set(...)`、`append(...)` 和 HLL 内部逻辑接收 `BytesSlice`。这让 command 层把 value 作为 slice 交给 DB，由 `StringRoot` 或对应 type root 决定写入 allocator-backed `STRING_BYTES`、collection payload、adapter-owned bytes 或必要 fallback。slice 的重点是延后复制决策，而不是承诺零拷贝持久化。

集合读路径大量使用 `BulkStringSink`。`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 等可以逐项 emit 到 sink，避免先组装完整 `List<byte[]>` 再交给协议层。

## ReplyWriter 和 Netty 写回

execution 层使用 `ReplyWriterFactory` 为每条命令创建 `ReplyWriter`。Netty adapter 分配 `ByteBuf`，再用 `NettyByteBufSink` 包装给 RESP writer。

写回路径大致是：

```text
CommandExecutorDrainLoop
  -> ioAdapter.allocateOutput(...)
  -> ReplyWriterFactory
  -> RespReplyWriter
  -> BytesSink / DirectBytesSink
  -> NettyByteBufSink
  -> ByteBuf
  -> channel.write(...)
```

`ReplyWriter.bulkString(BytesSlice)` 和 `BulkStringSink.bulkString(BytesSlice)` 是关键入口。heap `byte[]` 仍然可用，但不是唯一形状；off-heap string、collection range 和 computed ASCII number 都可以按更合适的方式写出。

## fast path 和 fallback

fast path 主要出现在这些地方：

- API 边界使用 `BytesView`，让 command/DB contract 不依赖 Netty，并为后续 direct lookup 优化保留形状；当前 DB lifecycle lookup 仍会 materialize heap `byte[]`。
- `BytesSlice.writeTo(BytesSink)` 可以把 value 直接流式写出。
- `DirectBytesSink` 暴露 writer cursor 和 memory address，支持 direct/off-heap aware 写入。
- `NettyByteBufSink.unwrap()` 允许 adapter 边界在必要时使用 `ByteBuf` 能力。
- `BulkStringSink` 让 collection range 边遍历边输出。

fallback 也同样重要。以下 heap materialization 是有意的：

- protocol adaptation snapshots：`RespCommandRequest` / `ExecutionRequest` 需要稳定 argv 跨过 decoder 生命周期和 executor queue。
- DB lifecycle lookup：当前 `YierdisDbKeyLifecycle` 用 `YierdisDb.toByteArray(...)` 把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`。
- transaction replay：事务队列需要保存可重放的 `ByteArrayExecutionRequest` / `ExecutionRecord`，不能引用短生命周期 frame。
- explicit introspection：`SCAN`、snapshot、`MEMORY` / object 类输出需要构造返回值或诊断对象，不能把 native view 泄漏给调用方。
- tests：测试经常用 heap arrays 和 recording sinks 断言内容，这是可读性和确定性的取舍。
- unavoidable fallback paths：JSON/base64/escape、legacy collection internals、adapter-owned payload、短生命周期输入持久化、需要排序/聚合的结果，都可能必须复制。

判断一条路径是否合理，不是看它有没有复制，而是看复制是否发生在 ownership、lifetime 或格式转换真正需要的位置。

## 和 native memory 的关系

bytes 抽象不是 native allocator。它只描述“如何读一段 bytes”和“如何把一段 bytes 写给 sink”。native object 的 lifetime、stable handle、pin、epoch、quarantine、`realloc` 和 active defrag 仍属于 allocator 文档，见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

当前 allocator-backed facts 要分清：

- `NativeKeyDirectory` 持久化 key bytes 为 allocator-backed `KEY_BYTES`。
- `StringRoot` 持久化 string payload 为 allocator-backed `STRING_BYTES`。
- entry metadata、collection root records 和部分 list quicklist metadata 是 allocator-backed objects。
- collection payload internals 仍可能由 adapter 或 legacy FFM structures 拥有。

因此 `BytesView` / `BytesSlice` 可以帮助 native 和 heap 路径共享 API，但它们本身不保证数据 off-heap，也不保证零拷贝。它们保证的是短生命周期 view、流式写出和 adapter 边界清晰。

## 推荐源码和测试

推荐源码顺序：

1. `BytesSource`
2. `BytesView`
3. `BytesSlice`
4. `BytesSink`
5. `DirectBytesSink`
6. `NettyByteBufSink`
7. `RespCommandRequest`
8. `RespCommandAdapter`
9. `RespReplyWriter`
10. `BulkStringSink`
11. `YierdisDbKeyLifecycle`
12. `StringRoot` 和 collection type roots

推荐测试：

- `RespReplyWriterTest`：`BytesSlice` reply 写回和 chunked/fast-path 行为。
- `RespRequestDecoderTest`：`RespCommandRequest` decode、retained bytes 和 adapter 边界。
- `ExecutionRequestContractTest`：heap snapshot request contract。
- `OffHeapContractsSmokeTest`：off-heap slice 如何挂在中立 bytes API 上。
- `OffHeapBytesViewTtlRegressionTest`：`BytesView` 在 TTL lookup 路径中的意义。
- `OffHeapCollectionReadStreamingTest`：collection 通过 `BulkStringSink` 流式输出。
- `NativeKeyDirectoryTest`：native key bytes、`KeyHandle` 和 `EntryHandle` 的 lookup/storage contract。
