# FFM 在 Yierdis 中的用法

Yierdis 使用 JDK 25 `java.lang.foreign`。`YierdisFfmMemoryRuntime.allocateRegion(...)` 用 `Arena.ofShared()` 分配 region：region 可能在 bootstrap 期间创建，再由 DB owner thread 释放。分配失败时关闭已创建的 arena，并抛出 `NativeCapacityExceededException`。`YierdisFfmRegion.close()` 关闭 arena，再调用 `YierdisFfmMemoryRuntime.onRegionClosed(...)` 扣减 live region 与 used bytes。

`YierdisFfmRegion` 在 `ensureOpen()` 和范围检查之后访问 segment：

- `getByte` / `setByte` 使用 `ValueLayout.JAVA_BYTE`
- `getInt` / `setInt` 使用 `ValueLayout.JAVA_INT_UNALIGNED`
- `getLong` / `setLong` 使用 `ValueLayout.JAVA_LONG_UNALIGNED`
- `getBytes` / `setBytes` / `copyTo` 使用 `MemorySegment.copy(...)`

`YierdisFfmStableMemoryBackend.resolve(...)` 返回 `NativeObjectView`。视图关闭后不能再访问内容；调用方用完必须 `close()`。

`EntryTable` 按固定 offset 把 entry record 写入 native memory。`NativeStorageLayout.ENTRY_RECORD_BYTES` 为 72。handle 由 `writeHandle` 按 little-endian long 写入 `allocatorId` 和 `localRaw`：

```text
0   key handle allocatorId
8   key handle localRaw
16  value handle allocatorId
24  value handle localRaw
32  key hash
36  type
40  encoding
44  flags
48  expireAtMillis
56  version
64  lruOrLfu
```
