# Native Memory 运行时

本文解释 Yierdis 如何把 JDK FFM 接入运行时：runtime、region、span、DB scope、maxmemory 和必须 materialize 到 heap 的边界。

## 当前结论

Yierdis 当前 native-memory runtime 建在 JDK 25 `java.lang.foreign` 上。最底层不是直接把 `MemorySegment` 暴露给 DB hot path，而是先封装成：

- `YierdisFfmMemoryRuntime`
- `YierdisFfmRegion`
- `YierdisFfmSpan`

再往上由 stable allocator 和 DB storage graph 使用。生产 DB 路径主要保存 stable `NativeHandle`，而不是保存 `MemorySegment`、native physical address 或 packed address。

一个容易出错的当前事实：`YierdisFfmMemoryRuntime.allocateRegion(...)` 使用 `Arena.ofShared()`，不是 confined arena。原因是 region 可能在 bootstrap 阶段创建，再由 DB owner thread release；arena 必须允许跨线程 close。

## 启动和组装

生产启动路径的默认 DB backend 由 `YierdisServerBootstrap` 组装，并通过 `YierdisInstanceConfig` 注入 `YierdisInstance.create(config)`。runtime 的 strict create 入口要求 `engineFactory` 非空，不再在生产入口里偷偷选择默认 DB backend。

- global scope：`YierdisServerBootstrap` 创建 instance-level `YierdisFfmMemoryRuntime("instance")` 和 `YierdisDbEngineFactory(memoryRuntime, nativeDefragOptions)`，多个 DB 共享 instance-level runtime。global governor 汇总 DB participant 的 owned physical snapshots；这个 shared runtime 作为 factory-owned resource 交给 instance 关闭。
- per-db scope：`YierdisServerBootstrap` 创建 `YierdisDbEngineFactory(nativeDefragOptions)`，每个 DB storage components 在没有外部 runtime 时创建 DB-owned `YierdisFfmMemoryRuntime("db")`。

embedded/test 调用方与生产 bootstrap 一样，必须在 `YierdisInstanceConfig` 中显式提供 `DbEngineFactory` 或 `EngineFactoryBinding`；`YierdisInstance.create(config)` 不包含默认 DB backend 的回退逻辑。

`YierdisDbStorageComponents.create(...)` 负责把 runtime 装配成 DB 内部结构：

```text
YierdisFfmMemoryRuntime
  -> YierdisStableNativeAllocator
     -> EntryTable
     -> NativeKeyDirectory
     -> StringRoot / ListRoot / HashRoot / SetRoot / ZSetRoot
     -> YierdisFfmExpireIndex
```

DB resources 会记录哪些对象由当前 DB 拥有。关闭 DB 时，只关闭自己 owns 的 runtime / native allocator，避免 global scope 下一个 DB 误关 shared runtime。

## runtime / region / span

`YierdisFfmMemoryRuntime` 是 region accounting 和 lifecycle 入口。它维护：

- runtime name。
- `usedBytes`。
- `liveRegionCount`。
- closed 状态。

`allocateRegion(owner, bytes)` 会：

1. 校验 bytes 和 closed 状态。
2. 创建 `Arena.ofShared()`。
3. 从 arena 分配 `MemorySegment`。
4. 包成 `YierdisFfmRegion`。
5. 原子递增 `liveRegionCount` 和 `usedBytes`。

`close()` 不主动释放 regions，而是先把 runtime 标记 closed，再检查原子 region counter。如果还有 region 没有关闭，会抛 native memory leak 异常。

`YierdisFfmRegion` 是一块 arena-backed memory 的 owner。它保存 runtime、owner label、arena、segment 和 size。byte/int/long/bulk copy 等生产访问方法都先确认 region open、检查范围，再直接用 `ValueLayout` 或 `MemorySegment.copy(...)` 访问。`span(offset, length)` 返回受同一 arena 生命周期约束的子视图；`close()` 会关闭 arena，并通知 runtime 扣减 region accounting。

`YierdisFfmSpan` 是轻量 segment view。它只保存 `MemorySegment`，提供 `size()` 和 `slice(offset, length)`。span 不是 owner；region 或 arena 关闭后，span 背后的 segment 也不再可访问。

## global scope 和 per-db scope

runtime ownership 和 maxmemory scope 是相关但不同的概念。

global maxmemory scope 下，instance 层有一个 shared `YierdisFfmMemoryRuntime`。每个 DB 仍然有自己的 keyspace、entry table、type roots、ledger 和 allocator实例，但底层 FFM region accounting 来自 shared runtime。global maxmemory governor 汇总各 DB participant 的 owned `MemoryUsageSnapshot`；runtime counter 负责生命周期 leak detection，不是一个额外的 maxmemory 账本。

per-db maxmemory scope 下，每个 DB 使用自己的 runtime / allocator resources。per-DB ledger reserve、evict 和 memory stats 都以当前 DB 的预算为边界。

无论哪种 scope，`YierdisDb` 都不是线程安全 map。DB 访问仍由 owner thread guard 约束；`Arena.ofShared()` 只说明 FFM region 可以跨线程关闭，不表示 DB storage graph 可以跨线程并发读写。

## keyspace、expires 和 value roots

FFM-backed storage paths 当前集中在这些结构：

- `NativeKeyDirectory`：key -> `EntryHandle` 目录。key bytes 存为 allocator-backed `KEY_BYTES` object；directory 的 open-addressing arrays 仍是 heap arrays。
- `EntryTable`：`EntryHandle` -> `ENTRY_RECORD`。entry metadata 是 allocator-backed native object，按固定 offset 存 key identity、value handle、type、encoding、TTL、legacy `version` 和 LRU/LFU 字段。`version` 当前保存 entry accounting estimate，不是 mutation version；stale prepared mutation 由 source generation、raw handle/value 等条件识别。
- `YierdisFfmExpireIndex`：TTL index 只接收 native-backed `KeyHandle`，并以 native key identity 参与 DB lifecycle；TTL authoritative 字段仍要和 `EntryRecord.expireAtMillis` 保持一致。
- `StringRoot`：string payload 使用 allocator-backed `STRING_BYTES`。
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot`：collection root records 使用 allocator-backed `LIST_ROOT`、`HASH_ROOT`、`SET_ROOT`、`ZSET_ROOT`。
- collection internal bytes 使用 allocator-backed objects：list payload 为 `LISTPACK_BYTES`，hash field/value 为 `HASH_FIELD_BYTES` / `HASH_VALUE_BYTES`，set members 为 `SET_MEMBER_BYTES`，zset members 为 `ZSET_MEMBER_BYTES`。
- list quicklist node metadata 使用 allocator-backed `LIST_NODE`。

`EntryHandle` 和 `ValueHandle` 是这些结构上的 typed stable-handle wrapper。`NativeKeyDirectory` 持有的是 `EntryHandle` raw value，`EntryTable` 持有的是 `EntryHandle -> ENTRY_RECORD` 的稳定映射，而 `ENTRY_RECORD` 内再保存 `ValueHandle` raw value。它们都不是 physical address，也不应该被当成可以长期缓存的 `MemorySegment` 视图。

这些结构由 `YierdisDbKeyLifecycle` 统一串起来。插入、替换、删除 key 时，lifecycle 负责同步 key directory、entry table、expire index、type root release 和 memory ledger delta。

## maxmemory 和 memory stats

Yierdis 把 native-memory usage 纳入 maxmemory 预算入口。写路径通过 `YierdisDbMutationExecutor` reserve upper bound 并 prepare；commit 后依次 promote allocation、settle logical ledger、publish commit stream、release superseded resources，再按提示尝试 trim。global scope 下，`YierdisGlobalMaxmemoryGovernor` 汇总 DB participant 的 owned physical snapshots；per-db scope 下，DB 使用自己的 snapshot 做 cleanup、trim、evict 或 reject。

需要注意两点：

- 这是 Yierdis 当前 runtime/accounting 口径，不要过度声称和 Redis 内部 maxmemory 精确等价。
- enforcement snapshot 的公式是 `heap estimate + native metadata committed + native data committed`。ledger `usedBytes` 是 mutation delta 的逻辑账本；shared runtime counter 只用于 region lifecycle/leak 诊断，不能再叠加到 participant snapshots。
- `MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph。`native reclaimable` 只表示 allocator 的候选回收量，不能从 committed enforcement footprint 中预先扣除。

## 仍然会 materialize 到 heap 的地方

native memory 不等于所有路径零拷贝。

当前仍会 materialize 到 heap 的常见边界：

- RESP decode 会把 argv materialize 成 heap `byte[]`；executor 与事务 retain 共享这份不可变 argv，显式 `ByteArrayExecutionRequest.copyOf(...)` 才再次复制。
- `YierdisDbKeyLifecycle` 当前把 `BytesView` lookup 输入转成 heap `byte[]` 后进入 `NativeKeyDirectory`。
- `SCAN`、snapshot、`RANDOMKEY`、introspection 和显式返回 `byte[]` / `List<byte[]>` 的 API 会复制出 heap bytes。
- `GET` / `HGET` / pop / snapshot 这类所有权返回 API 会复制出 heap bytes。
- collection range/member streaming 路径通过 native-backed `BytesSlice` 输出；排序、聚合或命令语义要求拥有结果时仍可能复制。

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

## Operations Cross-Check

native runtime 的 committed/reserved usage 参与 DB 和 maxmemory 诊断，但不会解除请求或回复的独立硬容量限制。运行时出现 native allocation failure 时，按 mutation 的 preflight/commit 边界判断是否可安全返回 OOM，还是必须走 result-unknown close；操作流程见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
