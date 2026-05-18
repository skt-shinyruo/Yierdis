# Native Memory Runtime

本文解释 Yierdis 如何把 JDK FFM 接入运行时：runtime、region、span、access、DB scope、maxmemory 和仍然 heap-backed 的边界。

## 当前结论

Yierdis 当前 native-memory runtime 建在 JDK 25 `java.lang.foreign` 上。最底层不是直接把 `MemorySegment` 暴露给 DB hot path，而是先封装成：

- `YierdisFfmMemoryRuntime`
- `YierdisFfmRegion`
- `YierdisFfmSpan`
- `YierdisFfmAccess`

再往上由 stable allocator 和 DB storage graph 使用。生产 DB 路径主要保存 stable `NativeHandle`，而不是保存 `MemorySegment`、native physical address 或 packed address。

一个容易出错的当前事实：`YierdisFfmMemoryRuntime.allocateRegion(...)` 使用 `Arena.ofShared()`，不是 confined arena。原因是 region 可能在 bootstrap 阶段创建，再由 DB owner thread release；arena 必须允许跨线程 close。

## 启动和组装

instance 启动时，`YierdisInstance.create(...)` 会创建 instance-level `YierdisFfmMemoryRuntime("instance")`。随后按 maxmemory scope 选择 DB factory 组装方式：

- global scope：默认 `YierdisDbEngineFactory(memoryRuntime, nativeDefragOptions)`，多个 DB 共享 instance-level runtime，global governor 额外把 shared off-heap usage source 纳入预算。
- per-db scope：默认 `YierdisDbEngineFactory(nativeDefragOptions)`，每个 DB storage components 在没有外部 runtime 时创建 DB-owned `YierdisFfmMemoryRuntime("db")`。

`YierdisDbStorageComponents.create(...)` 负责把 runtime 装配成 DB 内部结构：

```text
YierdisFfmMemoryRuntime
  -> YierdisForeignOffHeapAllocator
  -> YierdisStableNativeAllocator
  -> EntryTable
  -> NativeKeyDirectory
  -> StringRoot / ListRoot / HashRoot / SetRoot / ZSetRoot
  -> YierdisFfmExpireIndex
```

DB resources 会记录哪些对象由当前 DB 拥有。关闭 DB 时，只关闭自己 owns 的 runtime / off-heap allocator / native allocator，避免 global scope 下一个 DB 误关 shared runtime。

## runtime / region / span / access

`YierdisFfmMemoryRuntime` 是 region accounting 和 lifecycle 入口。它维护：

- runtime name。
- `usedBytes`。
- live region set。
- closed 状态。

`allocateRegion(owner, bytes)` 会：

1. 校验 bytes 和 closed 状态。
2. 创建 `Arena.ofShared()`。
3. 从 arena 分配 `MemorySegment`。
4. 包成 `YierdisFfmRegion`。
5. 加入 live region set，并增加 `usedBytes`。

`close()` 不主动释放 live regions，而是先把 runtime 标记 closed，再检查 live region set。如果还有 region 没有关闭，会抛 native memory leak 异常。

`YierdisFfmRegion` 是一块 arena-backed memory 的 owner。它保存 runtime、owner label、arena、segment 和 size。`span(offset, length)` 先确认 region open，再检查范围，最后返回 `new YierdisFfmSpan(segment.asSlice(offset, length))`。`close()` 会关闭 arena，并通知 runtime 扣减 region accounting。

`YierdisFfmSpan` 是轻量 segment view。它只保存 `MemorySegment`，提供 `size()` 和 `slice(offset, length)`。span 不是 owner；region 或 arena 关闭后，span 背后的 segment 也不再可访问。

`YierdisFfmAccess` 是统一访问工具。它提供 `getByte`、`setByte`、`getInt`、`setInt`、`getLong`、`setLong`、`getBytes`、`setBytes` 和 `asByteBuffer`。所有方法都按 span size 做边界检查，再使用 JDK FFM `ValueLayout` 读写。

## global scope 和 per-db scope

runtime ownership 和 maxmemory scope 是相关但不同的概念。

global maxmemory scope 下，instance 层有一个 shared `YierdisFfmMemoryRuntime`。每个 DB 仍然有自己的 keyspace、entry table、type roots、ledger 和 allocator实例，但底层 FFM region accounting 来自 shared runtime。global maxmemory governor 汇总各 DB participant usage，并通过 shared off-heap usage source 避免把 shared native memory 在每个 DB 上重复计算。

per-db maxmemory scope 下，每个 DB 使用自己的 runtime / allocator resources。per-DB ledger reserve、evict 和 memory stats 都以当前 DB 的预算为边界。

无论哪种 scope，`YierdisDb` 都不是线程安全 map。DB 访问仍由 owner thread guard 约束；`Arena.ofShared()` 只说明 FFM region 可以跨线程关闭，不表示 DB storage graph 可以跨线程并发读写。

## keyspace、expires 和 value roots

FFM-backed storage paths 当前集中在这些结构：

- `NativeKeyDirectory`：key -> `EntryHandle` 目录。key bytes 存为 allocator-backed `KEY_BYTES` object；directory 的 open-addressing arrays 仍是 heap arrays。
- `EntryTable`：`EntryHandle` -> `ENTRY_RECORD`。entry metadata 是 allocator-backed native object，按固定 offset 存 key identity、value handle、type、encoding、TTL、version 和 LRU/LFU 字段。
- `YierdisFfmExpireIndex`：TTL index 使用 FFM/native 结构参与 DB lifecycle；TTL authoritative 字段仍要和 `EntryRecord.expireAtMillis` 保持一致。
- `StringRoot`：string payload 使用 allocator-backed `STRING_BYTES`。
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot`：collection root records 使用 allocator-backed `LIST_NODE`、`HASH_NODE`、`SET_NODE`、`ZSET_NODE`，root record 再映射到 adapter-owned payload implementation。
- list quicklist metadata records 使用 allocator-backed `LIST_QUICKLIST_NODE`；payload bytes 以及 hash/set/zset internals 仍有迁移边界。

这些结构由 `YierdisDbKeyLifecycle` 统一串起来。插入、替换、删除 key 时，lifecycle 负责同步 key directory、entry table、expire index、type root release 和 memory ledger delta。

## maxmemory 和 memory stats

Yierdis 把 native-memory usage 纳入 maxmemory 预算入口。写路径通过 `YierdisDbMutationExecutor` 先 reserve upper bound，再执行 mutation，最后按 actual delta commit 或 rollback。global scope 下，`YierdisGlobalMaxmemoryGovernor` 汇总 DB participant usage 和 shared off-heap usage source；per-db scope 下，DB ledger 按自己的 limit 做 cleanup、evict 或 reject。

需要注意两点：

- 这是 Yierdis 当前 runtime/accounting 口径，不要过度声称和 Redis 内部 maxmemory 精确等价。
- `MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph。它会区分 ledger used/reserved、allocator stats、off-heap usage、是否纳入 maxmemory、heap estimate 和 native structure estimate。

## 仍然会 materialize 到 heap 的地方

native memory 不等于所有路径零拷贝。

当前仍会 materialize 到 heap 的常见边界：

- RESP decode 和 execution request snapshot 使用 heap `byte[]`。
- `YierdisDbKeyLifecycle` 当前把 `BytesView` lookup 输入转成 heap `byte[]` 后进入 `NativeKeyDirectory`。
- `SCAN`、snapshot、`RANDOMKEY`、introspection 和显式返回 `byte[]` / `List<byte[]>` 的 API 会复制出 heap bytes。
- 当前 string `GET` 路径返回的 slice 仍可能先复制成 heap-backed slice，而不是暴露 allocator view。
- collection payload internals、adapter-owned bytes、排序/聚合结果和 legacy structures 可能有自己的 copy 边界。

copy 细节见 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 和 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## 和 stable allocator 的关系

runtime 层只负责 region、span、access 和 accounting；它不提供 stable object identity。

stable identity 来自 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) 描述的 allocator 层：

```text
DB graph
  stores NativeHandle raw values

YierdisStableNativeAllocator
  resolves handle through YierdisNativeObjectTable

YierdisNativePageAllocator
  owns actual FFM blocks allocated from runtime regions
```

因此：

- DB hot path 不保存 `MemorySegment` 或 physical address。
- `NativeHandle` 不是 raw address。
- `realloc` 可以移动 native block，同时保持 handle 不变。
- active defrag 可以移动 allocator-backed objects，并通过 object table 发布新 location。
- pin、epoch 和 quarantine 防止 view 或扫描期间过早回收 memory。

## 推荐源码和测试

推荐源码：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSpan.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmAccess.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`

推荐测试：

- `YierdisFfmMemoryRuntimeTest`
- `YierdisForeignOffHeapAllocatorTest`
- `OffHeapLeakRegressionTest`
- `YierdisInstanceTest`
- `MaxmemoryScopeTest`
- `MemoryStatsAccountingConsistencyTest`
- `NativeStorageRegressionTest`
