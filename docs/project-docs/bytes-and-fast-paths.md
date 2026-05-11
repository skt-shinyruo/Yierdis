# Bytes And Fast Paths

本文解释 Yierdis 里一组很容易被忽略、但实际上非常核心的基础抽象：

- `BytesSource`
- `BytesView`
- `BytesSlice`
- `BytesSink`
- `DirectBytesSink`

如果你第一次看到这些接口，最容易误解成“只是为了包装 `byte[]`”。实际上它们存在的价值远不止这一点。

## 先记住一句话

这套 bytes 抽象的目标不是“把 Java bytes 面向对象化”，而是：

- 让 protocol、DB、off-heap 和 Netty 写出路径共享一套 Netty-free 接口
- 在需要的时候又能保留 direct/off-heap fast-path

## 为什么需要单独一层 bytes 抽象

如果没有这层抽象，代码会很快滑向两种坏结果：

第一种：

- protocol 和 DB 层都直接依赖 Netty `ByteBuf`

第二种：

- 所有路径都退化成 `byte[]`，为了安全和兼容不断复制

Yierdis 选择的是中间路线：

- 上层尽量依赖中立接口
- 需要优化时，再由实现暴露更低层能力

## 五个核心接口分别做什么

### `BytesSource`

最底层的随机访问只读接口。

它只承诺两件事：

- `getByte(index)`
- `getBytes(index, dst, dstOff, len)`

并且可选地支持：

- `hasMemoryAddress()`
- `memoryAddress()`

也就是说，它不仅能表示 heap bytes，也能表示拥有稳定内存地址的 off-heap/source。

### `BytesView`

在 `BytesSource` 之上补了：

- `length()`

它适合表示：

- key lookup 输入
- 短生命周期的只读字节视图

源码注释已经写得很明确：

- 它主要用于请求级 lookup 输入
- 不应该被直接存进 DB

### `BytesSlice`

它是在 `BytesView` 基础上再加：

- `writeTo(BytesSink out)`

这让一段 bytes 不只可以“被读”，还可以“被流式写出”。

这正是 server reply fast-path 需要的能力。

### `BytesSink`

最小写入接口：

- `writeBytes(byte[], off, len)`

它是最通用的输出口，协议编码器和写回路径都可以依赖它，而不必知道底层是不是 Netty。

### `DirectBytesSink`

这是 `BytesSink` 的增强版本，额外暴露：

- `ensureWritable(len)`
- `writerIndex()`
- `writerIndex(int)`
- `hasMemoryAddress()`
- `memoryAddress()`

这一步的意义是：

- 普通路径仍然只需要 `BytesSink`
- 真正想走 direct/off-heap fast-path 的路径，可以检测并利用更强能力

## 这套抽象在哪些地方最重要

### 1. DB 读路径里的 key lookup

很多 DB 接口不是收 `byte[]`，而是收 `BytesView`，例如：

- `StringReadOps`
- `TtlReadOps`
- `MemoryOps`
- `KeyspaceReadOps`

这意味着：

- lookup 不要求调用方一定先拷成新的 `byte[]`
- key 可以以 view 的形式被读取和比较

`YierdisDbKeyLifecycle`、`YierdisKeyspaceOps`、`YierdisTtlOps` 这一类代码都大量依赖 `BytesView`。

### 2. string 写路径

`StringWriteOps.set(...)` 和 `append(...)` 依赖的是：

- `BytesSlice`

这说明写入值时，系统不要求调用方必须先把内容变成一个持久化 heap 副本。

更准确地说：

- command 层把值作为 slice 交进来
- DB 内核再决定是否复制、复制到哪里、是否走 off-heap

### 3. reply 写回路径

`ReplyWriter.bulkString(BytesSlice slice)` 是这套设计的另一个关键点。

这让 server 在写回 bulk string 时，可以：

- 不必先把所有数据组装成一个新的 heap `byte[]`
- 而是尽量按 slice 流式写出

### 4. protocol codec

`RespReplyWriter.bulkString(BytesSlice slice)` 会利用这套抽象做：

- RESP bulk string 长度 header
- `BytesSlice.writeTo(out)` 流式写出
- 通过 `DirectBytesSink` / `NettyByteBufSink` 保持 fast-path

所以 bytes 抽象不仅存在于 DB，也直接参与协议编码。

## `NettyByteBufSink` 为什么关键

`NettyByteBufSink` 是 bytes 抽象接上 Netty 的桥。

它把 `ByteBuf` 包成一个 `DirectBytesSink`，让上层仍然只看到：

- `BytesSink`
- `DirectBytesSink`

而不是直接看到 Netty。

这样做有两个结果：

1. `ReplyWriterFactory` 和协议编码器可以保持 Netty-free 接口风格
2. 当下游真的是 `ByteBuf` 时，又能通过 `hasMemoryAddress()/memoryAddress()` 等能力走 fast-path

这也是为什么 server-app 的 `NettyExecutionIoAdapter` 会把 `ByteBuf` 包成：

- `new NettyByteBufSink(out)`

## fast-path 到底体现在哪里

这套设计里最值得注意的不是“接口名”，而是它让哪些路径有机会避免额外复制。

### 情况 1：只是 lookup

如果只是拿 key 做读取、TTL、memory introspection：

- `BytesView` 就足够了
- 可以避免无意义的中间复制

### 情况 2：reply bulk string 无需 escape

如果 reply 的内容是合法 UTF-8，且不需要 JSON escape：

- `BytesSlice.writeTo(out)` 可以直接写给 sink

这时协议层不必先拼一个完整临时字符串副本。

### 情况 3：下游是 direct / off-heap sink

如果 sink 还是 `DirectBytesSink`，并且底层有稳定内存地址：

- slice 实现就有机会进一步走 address-aware fast-path

### 情况 4：必须退化

并不是所有路径都能零拷贝。

例如：

- 需要 JSON escape
- 需要 base64
- 需要把短生命周期输入持久化进 DB

这些情况下仍然会复制，但复制发生的位置和时机被控制得更清楚。

## 这套抽象和 off-heap 文档的关系

本文解释的是：

- 接口边界和 fast-path 结构

而真正讨论“哪些路径发生复制、哪些路径 stay off-heap”的文档仍然是：

- [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)

可以这样理解两篇的关系：

- 本文解释“为什么有 `BytesView/BytesSlice/BytesSink`”
- `offheap-copy-behavior.md` 解释“具体哪条路径会拷贝”

## 在源码里该先看哪里

推荐顺序：

1. `BytesSource`
2. `BytesView`
3. `BytesSlice`
4. `BytesSink`
5. `DirectBytesSink`
6. `NettyByteBufSink`

然后再看这几个消费方：

- `YierdisDbKeyLifecycle`
- `YierdisStringOps`
- `RespReplyWriter`
- `NettyExecutionIoAdapter`

## 最值得看的测试

- `RespReplyWriterTest`
  看 `BytesSlice` 在 reply 写回里的 chunked/fast-path 行为
- `OffHeapContractsSmokeTest`
  看 `OffHeapSlice` 如何建立在中立 `BytesSlice` 之上
- `OffHeapBytesViewTtlRegressionTest`
  看 `BytesView` 在 TTL 路径里的实际意义
- `ByteArrayKeyspaceTest`
  看 `BytesView` lookup 如何参与 keyspace 比较

## 一句话总结

Yierdis 的 bytes 抽象层本质上是在做一件事：

- 上层逻辑尽量不依赖 Netty
- 下层实现又不放弃 direct/off-heap fast-path

它看起来小，但其实正好卡在 protocol、DB、reply 写回和 off-heap 之间，是整套系统非常关键的基础设施。
