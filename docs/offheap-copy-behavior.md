# Off-Heap Copy Behavior

本文说明 Yierdis 在启用 off-heap 后端时，哪些场景会触发 heap / off-heap 之间的拷贝，以及哪些路径可以保持在堆外或 direct buffer 之间流转。

一个实用判断是：

- 如果接口边界还是 `byte[]`、`List<byte[]>`、`String`，大概率会回到 heap。
- 如果接口边界是 `KeyHandle`、`BytesSlice`、`OffHeapSlice`、`BulkStringSink`，就有机会一直待在堆外，或者只做 native / direct-buffer 之间的拷贝。

## Heap -> Off-Heap

- 当前 server 的常见写入路径里，请求参数先在 heap。协议适配层会把请求里的命令和参数转成 heap `byte[]`。
  代表路径：`yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- `SET`、`APPEND` 等字符串写入命令，在启用 off-heap 后端时，会把这些 heap `byte[]` 或 `BytesSlice` 复制到 native memory。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- 如果启用了 off-heap keyspace，第一次插入 key 时，也会把 key 从 heap `byte[]` 复制到 native memory。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/YierdisUnsafeOffHeapKeyspace.java`
- 但如果 source 本身就是带 memory address 的 `BytesSlice`，则可以直接走 address-to-address copy，不必先落成 heap 临时数组。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`

需要注意的是，当前 server 的网络入口通常不是这种“source 已经带 native 地址”的场景，所以线上常见写入仍然主要是 `heap -> off-heap`。

## Off-Heap -> Heap

- 任何要求返回 `byte[]` 的 API，都会把 off-heap value 拷回 heap。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- `SET ... GET` 这类“返回旧值”的路径，也会先把旧值 materialize 成 heap `byte[]`。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- off-heap key 只要走 `randomKey()`、`forEach()` 这种 `byte[]` 语义接口，就会复制回 heap。
  代表路径：
  - `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/YierdisUnsafeOffHeapKeyspace.java`
  - `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/YierdisUnsafeOffHeapExpireIndex.java`
- 集合类里，凡是返回 `List<byte[]>` 的读取接口，也会把结果 materialize 到 heap。例如 `HGETALL` 的“返回 pairs 列表”路径。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- 即使上游拿到的是 `OffHeapSlice`，如果最终写出的 sink 不是 direct / Netty fast-path，也会退化成“分块拷到 heap scratch buffer 再写出”。
  代表路径：
  - `yierdis-memory/netty/src/main/java/yier/bubu/redis/db/memory/netty/YierdisNettyOffHeapAllocator.java`
  - `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/YierdisUnsafeOffHeapString.java`

## Off-Heap -> Off-Heap / Direct -> Direct

- `GET` 这类字符串读路径，如果 value 在 off-heap，当前实现可以直接返回 `BulkStringValue.slice(...)`，不必先变成 heap `byte[]`。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- reply 写回链路支持 `BytesSlice` 直通；如果下游是 direct `ByteBuf` 或带内存地址的 sink，可以直接把 bytes 拷到目标 native memory。
  代表路径：
  - `yierdis-server/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`
  - `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java`
  - `yierdis-memory/netty/src/main/java/yier/bubu/redis/db/memory/netty/YierdisNettyOffHeapAllocator.java`
  - `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/YierdisUnsafeOffHeapString.java`
- 集合类也有同样分界。例如 `HGETALL` 的流式写回路径，可以把 off-heap field/value 直接按 `BulkStringSink` 输出，而不必先拼成 `List<byte[]>`。
  代表路径：`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`

## 同侧复制也不少

- `heap -> heap`：例如协议适配阶段，`String.getBytes(...)` 会新建 heap `byte[]`。
- `off-heap -> off-heap`：例如字符串扩容时，会新分配 native block，再把旧内容搬过去。
- 追加写入时，如果现有 off-heap buffer 容量足够，只是继续往后写；如果容量不够，就会触发重分配和 native-to-native 搬迁。

## 结论

off-heap 并不自动等于“完全零拷贝”。

它的主要价值是：

- 降低稳态 heap 占用
- 降低 GC 压力
- 让部分读写路径可以绕开 `byte[]` materialization

但是否真的避免拷回 heap，最终取决于接口边界是否仍然要求 `byte[]`、`List<byte[]>`、`String` 这类 heap 语义。
