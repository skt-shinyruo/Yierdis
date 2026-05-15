# Off-Heap Copy Behavior

本文说明 Yierdis 当前统一使用 JDK 25 FFM native-memory 路径时，哪些场景会触发 heap / off-heap 之间的拷贝，以及哪些路径可以保持在堆外或 direct buffer 之间流转。

一个实用判断是：

- 如果接口边界还是 `byte[]`、`List<byte[]>`、`String`，大概率会回到 heap。
- 如果接口边界是 `KeyHandle`、`BytesSlice`、`OffHeapSlice`、`BulkStringSink`，就有机会一直待在堆外，或者只做 native / direct-buffer 之间的拷贝。

## Heap -> Off-Heap

- 当前 server 的常见写入路径里，请求参数先在 heap。RESP 适配层会把请求里的命令和参数转成 heap `byte[]`。
  代表路径：`yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- `SET`、`APPEND` 等字符串写入命令，会把这些 heap `byte[]` 或 `BytesSlice` 复制到 native memory。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- 当前 keyspace 使用 FFM 存储时，第一次插入 key 也会把 key 从 heap `byte[]` 复制到 native memory。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- 如果 source 本身是带 memory address 的 `BytesSlice`，`StringRoot.setBytes(...)` 当前仍会通过 heap scratch buffer 分块读取 `BytesSlice`，再写入 native `STRING_BYTES`；`StringRoot` 内没有直接 address-to-address copy 路径。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`

需要注意的是，当前 server 的网络入口通常是 heap 参数；即使上游传入 `BytesSlice`，`StringRoot` 当前也按 scratch buffer 分块写入 native `STRING_BYTES`。

## Off-Heap -> Heap

- 任何要求返回 `byte[]` 的 API，都会把 off-heap value 拷回 heap。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `SET ... GET` 这类“返回旧值”的路径，也会先把旧值 materialize 成 heap `byte[]`。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `GET` 这类字符串读路径当前返回 `BulkStringValue.slice(...)`，但 `StringRoot.slice(...)` 会先把 native `STRING_BYTES` copy 成 heap-backed `OffHeapSlice`，不暴露 allocator view。
  代表路径：
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- off-heap key 只要走 `randomKey()`、`forEach()` 这种 `byte[]` 语义接口，就会复制回 heap。
  代表路径：
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- 集合类里，凡是返回 `List<byte[]>` 的读取接口，也会把结果 materialize 到 heap。例如 `HGETALL` 的“返回 pairs 列表”路径。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- 对于真正 native-ref-backed 的 `OffHeapSlice`（不是当前 string `GET` 路径），如果最终写出的 sink 不是 direct / Netty fast-path，也会退化成“分块拷到 heap scratch buffer 再写出”。
  代表路径：
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmBytesRefSlice.java`

## Off-Heap -> Off-Heap / Direct -> Direct

- reply 写回链路支持 `BytesSlice` / `OffHeapSlice` 抽象直通；下游可以通过 `DirectBytesSink` / `NettyByteBufSink` 保持协议层抽象不依赖 Netty。当前 string `OffHeapSlice` 是 heap-backed copy，不暴露 allocator view；后续如果 slice 实现识别 direct sink，可进一步做 native-to-direct copy。
  代表路径：
  - `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`
  - `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- 集合类也有同样分界。例如 `HGETALL` 的流式写回路径，可以把 off-heap field/value 直接按 `BulkStringSink` 输出，而不必先拼成 `List<byte[]>`。
  代表路径：`yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`

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
