# FFM Usage

本文是 Yierdis native-memory 文档的入口页。JDK FFM 基础、Yierdis runtime 接入、stable allocator 和 copy 行为已经拆到独立文档中。

## 该先读哪一篇

- 第一次接触 JDK FFM：先读 [`ffm-primer.md`](./ffm-primer.md)。它只讲 `Arena`、`MemorySegment`、`ValueLayout`、slice、lifetime 和 native function 边界。
- 想理解 Yierdis runtime 如何装配 native memory：读 [`native-memory-runtime.md`](./native-memory-runtime.md)。
- 想理解 production allocator、stable handle、object table、pin/epoch/quarantine 和 active defrag：读 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。
- 想判断某条路径是否会 heap copy、native copy 或 direct-buffer copy：读 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。
- 想理解 DB 对象图、TTL、maxmemory、entry/value lifecycle：读 [`db-internals.md`](./db-internals.md)。
- 想理解 `BytesView`、`BytesSlice`、`BulkStringSink` 和 Netty-free fast path：读 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## Yierdis 里的 native-memory 层次

当前 native-memory 路径可以按四层看：

```text
JDK 25 java.lang.foreign
  -> YierdisFfmMemoryRuntime / YierdisFfmRegion / YierdisFfmSpan / YierdisFfmAccess
  -> YierdisStableNativeAllocator / YierdisNativeObjectTable / YierdisNativePageAllocator
  -> NativeKeyDirectory / EntryTable / StringRoot / collection roots / expire index
  -> DB reads/writes, maxmemory, memory stats, reply write-back
```

第一层是 FFM 原始能力：arena、segment、layout、slice 和 lifetime。

第二层是 Yierdis runtime wrapper：`YierdisFfmMemoryRuntime` 负责 region accounting 和 leak check；`YierdisFfmRegion` 拥有一个 arena-backed segment；`YierdisFfmSpan` 是 bounded segment view；`YierdisFfmAccess` 统一 byte/int/long 和 byte array 访问。

第三层是 production stable allocator：DB 层保存 `NativeHandle` raw value，而不是保存 physical address。allocator 通过 object table 把 handle 解析到当前 block，并在 `realloc`、free、quarantine 和 active defrag 中保持 handle 稳定。

第四层是 DB storage graph：`NativeKeyDirectory` 保存 key bytes 和 `EntryHandle` 映射，`EntryTable` 保存 entry metadata，type roots 保存 string payload 或 collection root records，TTL、maxmemory 和 memory reporter 在同一个生命周期里观察这些结构。

## 当前生产路径摘要

当前生产路径的事实是：

- 使用 JDK 25 `java.lang.foreign`。
- instance 启动会创建 `YierdisFfmMemoryRuntime("instance")`；global maxmemory scope 下 DB factory 共享这个 runtime。
- per-DB scope 下默认 DB factory 会让每个 DB 创建和拥有自己的 runtime / allocator resources。
- `YierdisFfmMemoryRuntime.allocateRegion(...)` 当前使用 `Arena.ofShared()`，因为 region 可能在 bootstrap 线程创建，在 DB owner thread 释放。
- `NativeKeyDirectory` 持久化 key bytes 为 allocator-backed `KEY_BYTES` object；directory table 数组仍在 heap。
- `EntryTable` 持久化 entry metadata 为 allocator-backed `ENTRY_RECORD` object。
- `StringRoot` 持久化 string payload 为 allocator-backed `STRING_BYTES` object。
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 使用 allocator-backed collection root records：`LIST_NODE`、`HASH_NODE`、`SET_NODE`、`ZSET_NODE`。
- list quicklist metadata records 使用 `LIST_QUICKLIST_NODE`，但 list payload bytes 和 hash/set/zset internals 仍有 adapter-owned 或 legacy FFM-owned 边界。
- maxmemory 把 native-memory usage 纳入预算入口，但实现是 Yierdis 当前 runtime/accounting 口径，不要直接等同 Redis 的精确内部口径。

## 不要误读的边界

不要把 off-heap 误读成“完全零拷贝”。请求 decode、DB lookup、显式 introspection、snapshot、SCAN、集合聚合结果和某些 reply fallback 仍会 materialize heap bytes。copy 细节见 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。

不要把 stable handle 误读成 native address。`NativeHandle` 是 slot/generation/domain/kind ABI；physical packed address、page id、offset、region 和 span 都是 allocator 私有实现。长期缓存 address 会破坏 `realloc` 和 active defrag。

不要把 collection root record 误读成“所有集合内部节点和 payload 都 nativeized”。root record 是 stable allocator object；部分 list quicklist metadata 也进入 allocator；其余 collection payload internals 仍按各自 adapter 或 legacy FFM structure 管理。

不要把 `Arena.ofShared()` 误读成 DB 结构本身线程安全。DB 仍然由 owner thread 约束，shared arena 只是解决 region 创建/释放可能跨线程的问题。

## 文档索引

- [`ffm-primer.md`](./ffm-primer.md)：最小 JDK FFM 入门。
- [`native-memory-runtime.md`](./native-memory-runtime.md)：Yierdis runtime、region、span、access、DB scope、maxmemory 和 heap-backed 边界。
- [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)：stable allocator、handle ABI、object table、pin/epoch/quarantine、active defrag。
- [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)：heap/direct/native memory 的 copy、view 和 handle 边界。
- [`db-internals.md`](./db-internals.md)：DB keyspace、entry、value、TTL、maxmemory 和 owner-thread 语义。
- [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)：bytes abstraction、sink、reply fast path 和 fallback。
