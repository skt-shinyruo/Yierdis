# Off-Heap Copy 边界

本文解释 heap、direct buffer、FFM native memory 之间什么时候发生 copy，什么时候只是 view 或 handle。结论不是“用了 off-heap 就零拷贝”，而是：zero-copy 是被选择出来的目标，不是所有路径的默认属性。

Yierdis 的 native memory 主要降低稳态 heap 占用、改善 GC 压力并给部分 read/write-back 留出 fast path；只要接口边界要求 `byte[]`、`List<byte[]>`、`String`、snapshot 或长期 ownership，copy 仍然会发生。

相关背景见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) 和 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## Heap -> off-heap

常见写入链路从 heap 开始。

RESP decode 后，`RespCommandRequest` / `ExecutionRequest` 保存稳定 argv snapshot，通常是 heap `byte[]`。这是协议帧跨过 Netty decoder lifecycle 和 executor queue 的 ownership 边界。

DB lookup 当前也会 materialize heap key。虽然 DB read/write API 接受 `BytesView`，`YierdisDbKeyLifecycle` 进入 `NativeKeyDirectory` 前会把 key view 转成 heap `byte[]`，因为 `NativeKeyDirectory` 当前 lookup/compute API 是 byte-array based。

新 key 持久化时，`NativeKeyDirectory` 会分配 allocator-backed `KEY_BYTES` object，并把 heap key bytes 写入 native object。这里是 heap -> off-heap copy。

string 写入时，`StringRoot` 会把输入 `BytesSlice` / `byte[]` 内容写入 allocator-backed `STRING_BYTES` object。即使 source 是带 memory address 的 slice，当前 `StringRoot` 也可能通过 heap scratch buffer 分块读，再写入 native object；不要把 `BytesSlice` 自动理解成 address-to-address copy。

collection 写入也类似：root record 可能是 allocator-backed native object，但 payload bytes 是否复制到 native、adapter-owned bytes 或 legacy FFM structure，要看具体 type root 和 value implementation。

## Off-heap -> heap

只要 API 语义要求 heap shape，就必须复制回 heap。

常见场景：

- 返回 `byte[]` 的 storage API。
- `SET ... GET` 这类需要返回旧值的命令。
- `RANDOMKEY`、`SCAN`、snapshot、introspection 和测试断言。
- `MEMORY` / `OBJECT` 类命令需要构造诊断输出。
- 返回 `List<byte[]>` 的 collection read API，例如非流式聚合结果。

当前 string `GET` 路径虽然可以通过 `BytesSlice` / `BulkStringSink` 接到 reply writer，但 `StringRoot.slice(...)` 不应被理解成长期暴露 allocator view。它可能先从 native `STRING_BYTES` 复制成 heap-backed slice，再交给上层写出。

keyspace 也是同理：key bytes 持久化为 `KEY_BYTES` native object，但只要外部接口要 `byte[]`，就会通过 allocator resolve view 读取并复制出来。

## Off-heap -> off-heap / direct -> direct

off-heap fast path 的意义是避免不必要的中间 heap materialization，但它要求上下游接口都表达“我可以按 slice 或 sink 工作”。

典型形状：

```text
BytesSlice
  -> writeTo(BytesSink)
  -> DirectBytesSink / NettyByteBufSink
  -> direct ByteBuf
```

`RespReplyWriter.bulkString(BytesSlice)` 会先写 RESP header，再让 slice 写入 sink。如果 sink 是 direct-aware implementation，底层可以选择 native/direct-aware copy 或减少临时 heap buffer。

collection read path 的 `BulkStringSink` 也服务这个目标。`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 这类命令可以边遍历边 emit bulk string，而不是必须先构造完整 `List<byte[]>`。

但这仍不是普遍零拷贝承诺：

- source slice 可能本身已经是 heap-backed copy。
- sink 可能只是普通 `BytesSink`，只能接收 `byte[]`。
- 某些格式转换、排序、聚合、escape、base64 或 legacy adapter 边界仍需要中间 buffer。

## 同侧复制也存在

copy 不只发生在 heap/off-heap 之间。

heap -> heap：

- 协议适配、transaction replay、测试 recording sink、字符串编码转换都可能创建新的 heap array。

off-heap -> off-heap：

- `StringRoot` append/growth 调用 allocator `realloc(..., PRESERVE_PREFIX)` 时，如果容量不足，allocator 会分配新 native block 并复制旧 prefix。
- active defrag 移动 `KEY_BYTES`、`ENTRY_RECORD`、`STRING_BYTES`、collection root records 或 `LIST_QUICKLIST_NODE` metadata record 时，会复制 old block 到 target block，再通过 object table 发布新 location。
- direct buffer 写回也可能是 direct -> direct copy，而不是 view 共享。

view / handle 不是 copy：

- `YierdisFfmSpan.slice(...)` 和 `MemorySegment.asSlice(...)` 是子视图，不复制底层 memory。
- `NativeHandle` 是 stable identity，不复制对象内容。
- `NativeObjectView` 是 resolved bounded view；打开 view 会 pin，对象内容没有因为 resolve 自动复制。

## 常见误读

误读一：off-heap 等于零拷贝。

实际是 zero-copy 只在接口、lifetime、ownership 和 sink 能力同时允许时才成立。很多边界有意复制，以获得稳定 ownership 或避免泄漏 native view。

误读二：`BytesView` lookup 已经避免 heap key。

当前 DB lifecycle 仍会把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`。新 key 的持久化 key bytes 是 `KEY_BYTES` native object，但 lookup 边界 copy 仍存在。

误读三：stable handle 是可直接读写的地址。

`NativeHandle` 不是 physical address。必须通过 allocator resolve，拿到短生命周期 `NativeObjectView` 后读写。这个约束让 `realloc`、quarantine 和 active defrag 可以成立。

误读四：collection root nativeized 等于所有 collection payload 都 nativeized。

当前 collection root records 是 allocator-backed，list quicklist metadata records 也进入 allocator；但 payload bytes、adapter-owned structures 和 legacy FFM internals 仍是分阶段迁移边界。

误读五：同侧就不会复制。

off-heap 扩容、active defrag、direct write-back、heap snapshot 和 retry buffer 都可能在同一侧发生 copy。
