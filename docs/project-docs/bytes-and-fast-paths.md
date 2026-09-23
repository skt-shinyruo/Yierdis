# Bytes 抽象与有界流式路径

Yierdis 的 bytes 抽象独立于 Netty 和 DB，目的是把复制放在真正需要的地方，而不是让每一层都套一层防御性 copy。

这套抽象不只是把 `byte[]` 包一层对象：protocol、execution、DB、native value 和 Netty write-back 由此共享一套 Netty-free contract；能流式写出时不强制完整结果 materialization，必须 materialize 时则把复制边界写清楚。

## 为什么不是直接传 byte[]

直接传 `byte[]` 有两个问题。

第一，所有上层都会默认“我拥有这段数组”，于是协议 decode、transaction replay、DB lookup、DB persistence 和 tests 混进同一种形状，最后只能靠防御性 copy 保安全。

第二，`byte[]` 表达不了 native source 的随机读取和流式写出能力。即使下游可以按 slice 或按 sink 分块处理，上游也已经把数据收缩成完整 heap array。

Yierdis 选择中立 bytes 层：

- lookup API 用短生命周期只读 view 表达输入；当前 DB lifecycle 边界仍会 materialize heap key copy。
- 写入值用 slice，把“读取”和“写出”能力同时交给 DB。
- reply 用 sink，把协议编码和 Netty `ByteBuf` 解耦。
- native value 可以通过同一组中立接口流式写出，但复制策略仍由具体实现和 ownership 边界决定。

## 三个核心接口

`BytesView` 是带长度的随机访问只读接口（`yier.bubu.redis.bytes` 包，Netty-free）：

```java
int length();
byte getByte(int index);
default void getBytes(int index, byte[] dst, int dstOff, int len)  // 逐字节循环，检查 null/负长度/越界
```

接口注释要求把实现视为短生命周期对象，不得被存入 DB。默认 `getBytes` 会做越界检查后循环调用 `getByte`；native 实现会覆写它做整段读取。这个默认实现的真实成本（逐字节虚调用，100 B 上实测约是 `System.arraycopy` 的 3 倍，而带分配的 `Arrays.copyOf` 才是差一个数量级的那一个）以及“它不进内核”的判定与实测数据见 [`copy-cost-and-kernel-boundary.md`](./copy-cost-and-kernel-boundary.md)。

`BytesSlice extends BytesView`，只多一个方法 `void writeTo(BytesSink out)`：既能随机读取，也能把自己流式写给 sink，是 string value、bulk reply 和 off-heap slice 的关键形状。

`BytesSink` 是最小的写接口，只承诺 `void writeBytes(byte[] src, int srcIndex, int len)`。实现必须在方法返回前消费指定范围，且不得保留或修改传入数组；协议编码器、reply writer 和测试 sink 都可以依赖它。

## `ByteArrayExecutionRequest` 的构造路径

`ByteArrayExecutionRequest` 是不可变的 heap-backed `ExecutionRequest`，内部就是 `byte[][] argv` + `int retainedBytes` + `boolean exposeReadOnlyBacking` + `RequestMemoryLease lease`。五个工厂方法的差别全在“复制不复制”和“读路径暴露不暴露 backing”上：

| 工厂 | 是否 clone 参数 | `exposeReadOnlyBacking` | 用途 |
| --- | --- | --- | --- |
| `copyOf(List<byte[]>)` | 是（逐参数 `arg.clone()`） | `false` | heap 输入的独立 snapshot |
| `copyOf(ExecutionRequest)` | 是（新建并 `copyToByteArray`） | `false` | 从任意 request 复制（也是默认 `retain()`） |
| `wrapReadOnly(byte[][])` | clone 外层数组，不 clone 元素 | `true` | 调用方已拥有 argv 且能持续遵守只读约定 |
| `takeOwnership(byte[][], lease)` | 否 | `true` | 网络生产路径 |
| `fromUtf8(String, List<String>)` | 是（`getBytes(UTF_8)`） | `true` | 测试、CLI、固定文本输入 |

网络生产路径中，`RespRequestDecoder` 调用 `takeOwnership(...)`，把已 materialize 的 heap `byte[][] argv` 和脱离 Netty 对象的 request-memory lease 一并移交给请求——这里不做任何逐参数复制。

`readOnlyByteArray(index)` 的语义取决于 `exposeReadOnlyBacking`：

```java
byte[] arg = argv[index];
return exposeReadOnlyBacking ? arg : arg.clone();
```

所以它只在 `takeOwnership` / `wrapReadOnly` / `fromUtf8` 构造的请求上是零拷贝热点路径；`copyOf` 构造的请求每次调用都会 clone。默认接口实现（`ExecutionRequest.readOnlyByteArray`）本身就是 `toByteArray(index)`，会复制。`toByteArray(index)` 无条件 clone（`arg.clone()`），`CommandArgs.bytes(...)` 走的就是 `readOnlyByteArray`。

`retain()` 共享同一个 `argv`（不复制数据），只做 `lease.retain()`；`close()` 做 `lease.close()`。retained bytes 一律由 `HeapRequestFootprint` 按 heap footprint 估算：

- `REQUEST_FIXED_BYTES = 32`（请求对象本身）；
- `OUTER_ARGV_BYTES = 16`（外层 `byte[][]` 数组头）+ 每槽位 `REFERENCE_BYTES = 8`；
- 每个非空参数 `ARRAY_HEADER_BYTES = 16` + `align8(length)` 的 payload。

累加全程饱和（`addSaturating`），`estimateRetainedBytes` 把 long 夹到 `Integer.MAX_VALUE`，不会因为 `int` 回绕变成负数。decoder 的准入计费、连接 pending bytes 和事务 queue bytes 消费同一个口径。

## 零拷贝边界在哪里

把“不复制”说清楚，比笼统说“用了 bytes 抽象”有用：

**argv 不复制的前提是拿走了所有权。** 只有 `takeOwnership` 这一条路径对参数零拷贝，代价是请求必须通过 `close()` 归还 lease，且调用方不能再改那批 `byte[][]`。`copyOf` 两条路径都复制，换来的是调用方可以随意复用/释放自己的数组。

**key 一定会复制。** `YierdisDbKeyLifecycle` 在把 `BytesView` 交给 key directory 之前调用 `YierdisDb.toByteArray(keyView)`，后者 `new byte[length]` 再 `view.getBytes(...)` 填满：

```java
static byte[] toByteArray(BytesView view) {
    int length = view.length();
    if (length < 0) { return null; }
    byte[] bytes = new byte[length];
    view.getBytes(0, bytes, 0, length);
    return bytes;
}
```

原因是 `NativeKeyDirectory` 的 lookup API 当前是 `byte[]` based：`get(byte[])`、`getKeyHandle(byte[])`、`stageInsert(byte[] keyBytes)`。源码注释明确这是有意的暂缓项——“allocation profile 证明该 copy 是热点时，再为 `NativeKeyDirectory` 增加 direct lookup”。所以不能写成“lookup 已经避免 heap key 生成”。

**插入 key 会再复制一次。** `stageInsert` 进来第一件事是 `Arrays.copyOf(keyBytes, keyBytes.length)`，得到一个与调用方数组解耦的稳定副本，再用它算 hash 并 `allocateKey(...)` 写进 allocator-backed `KEY_BYTES`。这次复制是为了 staged insert 期间调用方可以释放自己的数组。

**native 数据写出时按块复制。** `NativeBytesSlice.writeTo` 用一个 8 KiB 的 ThreadLocal scratch（`COPY_CHUNK_BYTES = 8 * 1024`、`TL_COPY_BUF`）分块把 native view 拷进 heap，再喂给 `BytesSink`。native 与 sink 之间没有直接内存映射，写出必然经过这段 scratch。slice 是否用 `retainedPin` 决定 `readView()` 走 `allocator.resolvePinned` 还是 `allocator.resolve`。

**collection range 在 prepare 时物化成快照。** 见下文。

## 协议层如何使用 bytes

RESP decode 后直接得到 `ByteArrayExecutionRequest`，其中保存 `byte[][] argv`、`HeapRequestFootprint` 口径的 retained bytes 和 reference-counted request-memory lease；网络层不再经过协议 DTO 或 adapter。decoder 在 bulk/inline 命令完整前就对 argv、payload 和 request 固定开销完成 admission，因此 heap materialization 是有意的 ownership snapshot：请求跨过 Netty decoder 生命周期后，需要稳定 argv 和 admission 计数供 executor 排队、budget 和 transaction 逻辑使用。

reply 编码方向相反。`RespReplyWriter.bulkString(BytesSlice)` 先写 RESP bulk header，再同步调用 `BytesSlice.writeTo(out)` 把内容写入 `BytesSink`，最后写 CRLF。生产路径中的 sink 用 reply reservation 把输出限制在有界 `ByteBuf` chunk 内。

## DB lookup 和写路径如何使用 bytes

DB API 的很多读方法接受 `BytesView`，例如 `StringOps`、`TtlOps`、`KeyspaceOps` 与 `DbEngine` 上的 memory/object-encoding 方法。command/DB contract 由此保持 Netty-free。

写路径中，`StringOps.set(...)`、`append(...)` 和 HLL 内部逻辑接收 `BytesSlice`。command 层把 value 作为 slice 交给 DB，由 `StringRoot` 或对应 type root 写入 allocator-backed `STRING_BYTES` 或 collection native payload handles。slice 的重点是延后复制决策，而不是承诺零拷贝持久化。

`GET` 是 retained native view 的典型：`StringRoot.retainedValueWithCloseHook(...)` 先 `allocator.pin(nativeHandle)`，再构造 `ByteValue.owned(NativeBytesSlice.retained(...), payloadLength, retainedBytes, () -> allocator.unpin(nativeHandle))`。close hook 负责 unpin；如果构造失败，`finally` 里同步 unpin。

## 哈希与比较走哪条路径

`NativeKeyDirectory` 的 key 相等性判断不依赖“取出 key 再比较”，而是把 stored key 留在 native：

```java
private int hash(byte[] keyBytes) {
    return SipHash24.foldToInt(SipHash24.hash(hashSeed, keyBytes));
}

private int hash(NativeHandle keyHandle) {
    try (NativeObjectView view = allocator.resolve(keyHandle, NativeAccessMode.READ_ONLY)) {
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, view));
    }
}

private boolean equalsBytes(NativeHandle handle, byte[] keyBytes) {
    try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
        return view.size() == keyBytes.length
                && view.contentEquals(0, keyBytes, 0, keyBytes.length);
    }
}
```

要点：

- hash 算法是 SipHash-2-4，`HashSeed` 是 `record HashSeed(long key0, long key1)`，由 `SecureRandom` 在启动时 `random()` 生成一次，因此 timer/DoS 攻击面比固定 hash 小。
- `SipHash24` 有三个重载：`hash(HashSeed, byte[])`、`hash(HashSeed, BytesView)`、`hash(HashSeed, NativeObjectView)`，lookup 用 heap `byte[]`，rehash 迁移用 native view，二者不需要互相转换。
- 查找比较是“native view 对 heap bytes”（`equalsBytes`），不需要把 stored key materialize 成 `byte[]`。
- 迁移时用 `probeStoredKey(NativeHandle, hash)`，比较的是 handle 身份，连内容都不用读。
- `allocateKey(byte[] keyBytes)` 分配 `KEY_BYTES` 并把字节写进去；写入失败时 `finally` 里 `allocator.free(handle)`，不留悬空 handle。

## 集合遍历与语义回复

集合内部遍历使用协议无关的 `ByteValueSink`：

```java
void value(byte[] data);
void value(byte[] data, int offset, int length);
void value(BytesSlice slice);
void longAscii(long value);
void nullValue();
```

`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE*` 在 prepare 时通过 `ByteSequenceSources.copiedFrom` / `ByteMapSources.copiedFrom` 把选中元素拷进独立 source。`copiedFrom` 内部用 `CapturedByteItems.capture(emitter)` 只遍历 live native 结构一次，之后 emit/length 只回放这份快照；renderer 因此不会在写协议时再遍历 live native 结构（避免遍历期间结构被并发改动）。

`SMEMBERS` 的 intset 分支是例外：`SetValue.members()` / `membersInto(...)` 直接 `Long.toString(...)` 生成 ASCII 元素，因为 `SET_INTSET` 本来就在 heap 上。

## 语义回复和 Netty 写回

命令层把 storage source 包装成 `RedisReply.BulkString` 或 `ByteAggregate`（kind 为 SEQUENCE/SET/MAP），并把 source owner 挂在 `PreparedCommand` 上。executor 先按 reply shape 完成 slot reservation，执行得到 `CommandResult` 后才创建 `RedisReplyWriter`；`RedisReplyRenderer` 随后同步运行 payload emitter。当前 Netty sink 按固定上限分配 `ByteBuf` chunk，renderer 返回后 prepared owner 才会关闭。

写回路径大致是：

```text
CommandResult / RedisReply
  -> RedisReplyRenderer
  -> RedisReplyWriter (RespReplyWriter)
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> bounded ByteBuf chunks
  -> ConnectionReplySequencer
  -> channel.write(...)
```

`RedisReply` 的 payload emitter 和 `ByteValueSink.value(BytesSlice)` 是关键入口。heap `byte[]` 仍然可用，但不是唯一形状。`GET` / `HGET` / pop 可以在中央 renderer 调用期间通过已 pin 的 native slice 写出；collection range 回放的是 prepare 时拷好的快照。reply reservation、source ownership、Netty ownership 和顺序写回的细节见 [`netty-adapter-design.md`](./netty-adapter-design.md)。

## 流式路径和 materialization fallback

流式路径主要出现在这些地方：

- API 边界使用 `BytesView`，让 command/DB contract 不依赖 Netty；当前 DB lifecycle lookup 仍会 materialize heap `byte[]`。
- `BytesSlice.writeTo(BytesSink)` 可以流式写出 value，避免 whole-result materialization，但 native 实现仍使用有界 heap scratch copy。
- `GET` 的 `NativeBytesSlice` 在 `StringRoot.retainedValueWithCloseHook` 取出时 pin，`CommandExecutorExecutionSupport` 同步渲染结束后 `closePrepared` 才 unpin；`writeTo` 使用这份已有 pin（retained slice 走 `resolvePinned`），不会在写出期间单独 pin/unpin。
- `SCAN` window 保留 cursor、目录 generation/capacity、epoch 和匹配计数；length/emit 阶段重放相同物理 slot 范围，并把匹配 key 包装为 native-backed slice。
- `ReplyReservationSink` / `BoundedChunkedReplySink` 在分配前取得额度，并把编码结果限制在有界 `ByteBuf` chunk 内。

fallback 同样重要，以下 heap materialization 是有意的：

- protocol snapshots：`ByteArrayExecutionRequest` 需要稳定 argv 跨过 decoder 生命周期和 executor queue。
- DB lifecycle lookup：当前 `YierdisDbKeyLifecycle` 用 `YierdisDb.toByteArray(...)` 把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`；`stageInsert` 还会再 `Arrays.copyOf` 一次。
- transaction replay：事务队列通过 `ExecutionRequest.retain()` 取得独立所有权；生产网络实现共享不可变 argv 和 reference-counted request-memory lease，默认接口实现才使用 heap copy。
- explicit materialization：snapshot、`RANDOMKEY`、显式 `byte[]` API 和 `MEMORY` / object 类 introspection 需要构造独立返回值或诊断对象，不能把 native view 泄漏给调用方。
- collection range：`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE*` 在 prepare 时经 `copiedFrom` 拷成独立 source，renderer 只回放快照。
- tests：测试经常用 heap arrays 和 recording sinks 断言内容，这是可读性和确定性的取舍。
- ownership-returning DB APIs：要求 owned `byte[]` 或集合快照的显式 API（`NativeByteStore.toByteArray(...)`、`SetValue.members()` 等）会复制；命令 `GET`、`HGET`、pop 和 `SET ... GET` 则持有 retained native-backed view/slice，直到同步 reply rendering 完成后释放。
- unavoidable fallback paths：JSON/base64/escape、短生命周期输入持久化、需要排序/聚合或独立所有权的结果，都可能必须复制。

判断一条路径是否合理，不是看它有没有复制，而是看复制是否发生在 ownership、lifetime 或格式转换真正需要的位置。

## 和 native memory 的关系

bytes 抽象不是 native allocator。它只描述“如何读一段 bytes”和“如何把一段 bytes 写给 sink”。native object 的 lifetime、stable handle、pin、epoch、quarantine、`realloc` 和 active defrag 仍属于 allocator 文档，见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

当前 allocator-backed facts 要分清：

- `NativeKeyDirectory` 持久化 key bytes 为 allocator-backed `KEY_BYTES`。
- `StringRoot` 持久化 string payload 为 allocator-backed `STRING_BYTES`。
- entry metadata、collection root records 和 collection internal bytes 都是 allocator-backed objects。
- hash/list/zset 的 packed 编码是 allocator-backed `NativeListpack`，dictionary 编码是 `NativeByteMap`；只有 `SET_INTSET` 是纯 heap 的 `short[]`/`int[]`/`long[]`。
- list/hash/set/zset 的 `LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE*` 在 prepare 时经 `ByteSequenceSources.copiedFrom` 或 `ByteMapSources.copiedFrom` 拷成堆快照，renderer 只回放这份快照。

`BytesView` / `BytesSlice` 让 native 和 heap 路径共用同一套 API，但它们本身不保证数据 off-heap，也不保证零拷贝；真正保证的是短生命周期 view、流式写出和清晰的 adapter 边界。