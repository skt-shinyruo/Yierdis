# Off-Heap Copy 边界

本文解释 heap、`ByteBuf`、FFM native memory 之间什么时候发生 copy，什么时候只是 view 或 handle。结论不是“用了 off-heap 就零拷贝”，而是：当前写回路径通过有界复制避免完整结果 materialization，并不提供生产零拷贝能力。

Yierdis 的 native memory 主要降低稳态 heap 占用、改善 GC 压力并让部分 read/write-back 可以流式处理；只要接口边界要求 `byte[]`、`List<byte[]>`、`String`、snapshot 或长期 ownership，copy 仍然会发生。

相关背景见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) 和 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## Heap -> off-heap

常见写入链路从 heap 开始。

RESP decode 后，`RetainedRespExecutionRequest` / `ExecutionRequest` 保存稳定 argv snapshot，通常是 heap `byte[]`，并持有脱离 Netty 对象的 memory lease。这是协议帧跨过 Netty decoder lifecycle 和 executor queue 的 ownership 边界。

DB lookup 当前也会 materialize heap key。虽然 DB read/write API 接受 `BytesView`，`YierdisDbKeyLifecycle` 进入 `NativeKeyDirectory` 前会把 key view 转成 heap `byte[]`，因为 `NativeKeyDirectory` 当前 lookup/compute API 是 byte-array based。

新 key 持久化时，`NativeKeyDirectory` 会分配 allocator-backed `KEY_BYTES` object，并把 heap key bytes 写入 native object。这里是 heap -> off-heap copy。

string 写入时，`StringRoot` 会把输入 `BytesSlice` / `byte[]` 内容写入 allocator-backed `STRING_BYTES` object。当前实现可能通过 heap scratch buffer 分块读，再写入 native object；不要把 `BytesSlice` 自动理解成 address-to-address copy。

collection 写入也类似：root record 和 payload internals 都是 allocator-backed native objects。输入仍通常来自 heap argv，因此写入会把 field、member、score 或 list entry bytes 复制到 type-specific native handles。

## Off-heap -> heap

只要 API 语义要求 heap shape，就必须复制回 heap。

常见场景：

- 返回 `byte[]` 的 storage API。
- `RANDOMKEY`、snapshot、introspection 和测试断言。
- `MEMORY` / `OBJECT` 类命令需要构造诊断输出。
- 返回 `List<byte[]>` 的 collection read API，例如非流式聚合结果。

当前 string `GET` 路径可以通过 `BytesSlice` / `BulkStringSink` 包装成流式 `RedisReply`，再由中央 renderer 写入协议端口。native slice 只在同步 `writeTo` 期间 pin allocator handle；它不是可被长期持有的 allocator view。

命令 `GET`、`HGET`、pop 和 `SET ... GET` 使用 retained native-backed view/slice；`SCAN` 则保留 cursor、目录元数据和 epoch，在输出阶段重放目录并生成 native-backed key slice。它们都不要求先把完整结果 materialize 到 heap。

keyspace 也是同理：key bytes 持久化为 `KEY_BYTES` native object，但只要外部接口要 `byte[]`，就会通过 allocator resolve view 读取并复制出来。

## Off-heap -> bounded streaming output

有界流式写出的意义是避免完整结果 heap materialization，但它要求上下游接口都表达“我可以按 slice 或 sink 工作”。

典型形状：

```text
NativeBytesSlice
  -> writeTo(BytesSink)
  -> reusable bounded heap scratch
  -> ReplyReservationSink / BoundedChunkedReplySink
  -> bounded ByteBuf chunks
```

`RespReplyWriter.bulkString(BytesSlice)` 会先写 RESP header，再让 slice 同步写入 sink，最后写 CRLF。`NativeBytesSlice` 当前使用可复用的 8 KiB heap scratch 分块读取 native bytes；`BoundedChunkedReplySink` 在 allocator 调用前把预留额度转换为 allocated credit，再写入固定上限的 `ByteBuf` chunk。

collection read path 的 `BulkStringSink` 也服务这个目标。`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 这类命令可以边遍历边 emit bulk string，而不是必须先构造完整 `List<byte[]>`。

这条路径避免完整 heap 结果，但当前仍执行有界复制：

- source slice 可能来自 request heap bytes，或来自同步 pin/unpin 的 native handle view。
- native slice 先复制到有界 heap scratch，sink 再把数组范围同步复制到 reply chunk。
- 某些格式转换、排序、聚合、escape 或 base64 边界仍需要中间 buffer。

reply reservation、chunk ownership 和顺序写回见 [`netty-adapter-design.md`](./netty-adapter-design.md)。

## 同侧复制也存在

copy 不只发生在 heap/off-heap 之间。

heap -> heap：

- 协议适配、transaction replay、测试 recording sink、字符串编码转换都可能创建新的 heap array。

off-heap -> off-heap：

- `StringRoot` append/growth 调用 allocator `realloc(..., PRESERVE_PREFIX)` 时，如果容量不足，allocator 会分配新 native block 并复制旧 prefix。
- active defrag 移动 `KEY_BYTES`、`ENTRY_RECORD`、`STRING_BYTES`、collection root records、`LIST_NODE` metadata record 或 collection internal byte handles 时，会复制 old block 到 target block，再通过 object table 发布新 location。
- `ByteBuf` 写回也可能发生 buffer copy，而不是 view 共享。

view / handle 不是 copy：

- FFM 的 `MemorySegment.asSlice(...)` 是子视图，不复制底层 memory；Yierdis backend 当前不额外暴露 span wrapper。
- `NativeHandle` 是 stable identity，不复制对象内容。
- `NativeObjectView` 是 resolved bounded view；打开 view 会 pin，对象内容没有因为 resolve 自动复制。

## 常见误读

误读一：off-heap 等于零拷贝。

实际是当前生产写回明确经过 bounded heap scratch 和 bounded reply chunk。很多边界有意复制，以获得稳定 ownership 或避免泄漏 native view。

误读二：`BytesView` lookup 已经避免 heap key。

当前 DB lifecycle 仍会把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`。新 key 的持久化 key bytes 是 `KEY_BYTES` native object，但 lookup 边界 copy 仍存在。

误读三：stable handle 是可直接读写的地址。

`NativeHandle` 不是 physical address。必须通过 allocator resolve，拿到短生命周期 `NativeObjectView` 后读写。这个约束让 `realloc`、quarantine 和 active defrag 可以成立。

误读四：collection nativeized 等于完全零拷贝。

当前 collection root records、list quicklist metadata records 和 hash/set/zset/list payload internals 都是 allocator-backed native handles。但 RESP decode、snapshot、聚合返回和诊断输出仍可能需要 copy。

误读五：同侧就不会复制。

off-heap 扩容、`ByteBuf` write-back、active defrag、heap snapshot 和 retry buffer 都可能在同一侧发生 copy。
