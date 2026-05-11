# FFM Native Storage Core Design

> **Status:** Approved design, not yet implemented.

## Goal

把 `yierdis-db-memory` 的主存储模型推进到方案 3：canonical storage state 直接落在 FFM 管理的 native layout 里，而不是落在 `YierdisObject` 这类 Java 对象壳里。

目标不是“把所有 Java 对象都删光”，而是把热路径上的 key/value/entry/索引/类型根节点都搬到 off-heap 记录和 slab 里，让 Java 只负责调度、协议、命令执行和生命周期编排。

## Non-Goals

- 不改 RESP / 命令协议。
- 不引入 AOF、RDB、replication、cluster 作为这次重构的一部分。
- 不改变 `DbReads` / `DbWrites` 的外部调用方式。
- 不尝试一次性把整个项目的所有 heap 对象清空。
- 不把 `YierdisObject` 立即删除；它会先退化成兼容适配层。

## Current State

现在的内核已经有一些正确的底座，但 canonical data model 仍然是半对象化的：

- `YierdisDb` 目前持有 `YierdisFfmKeyspace<YierdisObject>` 和 `YierdisFfmExpireIndex`。
- `YierdisObject` 仍然是 `type + encoding + payload` 的统一包装层，payload 可能是 `byte[]`、`OffHeapBuf` 或复合值对象。
- `ListValue`、`HashValue`、`SetValue`、`ZSetValue`、`YierdisHyperLogLog` 已经有 FFM 背景，但主路径还是通过 Java 价值对象来组织。
- `YierdisDbIntrospection.snapshot(...)` 和 `YierdisSnapshot` 已经提供了按 cursor 扫描当前状态的形状。
- `YierdisFastCommandProcessor` 已经能在写成功后发 `YierdisChangeEvent`，说明 replay-style 接缝已经存在。

这意味着这次重构不需要从零搭架子，真正要做的是把“对象壳 + 混合 payload”换成“native entry + native root + handle”。

## Target Architecture

### 1. Shared FFM storage substrate

FFM 仍然是底层内存 API，但不再按 key/value 分配零散 region。

新的 storage core 应该使用一个内部 slab/page allocator：

- 先申请少量大块 native slabs。
- 在 slab 里切 page / block / size class。
- 业务层只持有 64-bit handle。

这里的重点是：`OffHeapAllocator` 继续作为通用 buffer API 保留，但真正的 storage core 不能依赖“每个 blob 一个 region”这种粗粒度分配模型。

### 2. Canonical entry model

canonical metadata 由固定布局的 `EntryRecord` 承担，而不是由 `YierdisObject` 承担。

建议的 entry 字段：

```text
EntryRecord {
  uint64 keyHandle
  uint64 keyHash
  uint8  type
  uint8  encoding
  uint16 flags
  int64  expireAtMillis
  uint64 version
  uint64 lruOrLfu
  uint64 valueRootHandle
  uint64 auxRootHandle
}
```

语义上：

- `keyHandle` 指向 off-heap key bytes。
- `valueRootHandle` 指向具体类型的根节点。
- `auxRootHandle` 留给变长辅助结构、扫描辅助结构或 future expansion。
- `version` 仍然用于 WATCH / CAS / snapshot consistency。
- `expireAtMillis` 成为 entry 的第一性字段，而不是 value 内部属性。

`EntryTable` 是这些记录的唯一 canonical 宿主。

### 3. Key directory

`KeyDirectory` 负责把 key bytes 映射到 entry slot。

它不再是“Java 对象 map”，而是 native table：

- bucket array / open addressing / chaining 都可以，但实现必须可渐进 rehash。
- key bytes 本身也要走 native handle，不在热路径反复 materialize 成 heap `byte[]`。
- 查找、更新、删除都应围绕 `KeyHandle` 或 key bytes 的 native ref 进行。

这一步的结果是：keyspace 不再把 `YierdisObject` 当 value 存，只保存 entry slot 的定位信息。

### 4. Type roots

每种 Redis 类型都拥有自己的 native root layout，统一通过 `TypeRoot` 语义访问。

建议的 root 家族：

- `StringRoot`
- `HashRoot`
- `ListRoot`
- `SetRoot`
- `ZSetRoot`
- `HllRoot`

它们的共同点：

- 都是 native layout。
- 都通过 handle 间接访问。
- 都可以单独编码、单独释放、单独快照。
- 都不要求外层持有一个 Java 级统一 value 对象。

它们的不同点：

- String 需要支持 INT / RAW / EMBSTR-like / CHUNKED。
- List 需要 quicklist 风格的多节点布局。
- Hash / ZSet / Set 需要 listpack 与 hashtable / intset / skiplist 等复合编码。
- HLL 需要 sparse / dense 双编码。

### 5. Expiration and eviction

TTL 和 eviction 元数据都应该向 entry 收敛。

设计原则：

- `expireAtMillis` 是 entry 的真相来源。
- `ExpireIndex` 只负责加速 cleanup，不负责保存额外语义。
- eviction 元数据优先放在 entry 里，避免又造一层 heap wrapper。
- `unlinkEntry(...)` 仍然是统一释放入口，负责同时拆掉 key、value root、expire 记录和 eviction 记录。

### 6. Snapshot and introspection

`YierdisSnapshot`、`YierdisDbIntrospection`、`MEMORY USAGE`、`OBJECT ENCODING` 这类读路径，后续都应该直接从 entry table 和 type root 读取。

这意味着：

- 不需要先还原 `YierdisObject` 再回答内省问题。
- snapshot 可以直接产出 entry-level 视图。
- `objectEncoding` 变成 native metadata 的派生值，而不是对象壳上的字段回读。

## Read and Write Semantics

### Read path

读路径应当收敛为：

```text
key bytes
  -> key handle
  -> entry slot
  -> type root
  -> typed reader / encoder
```

读路径上必须保持：

- 惰性过期删除
- 类型错误语义
- 只读路径不额外分配 heap wrapper
- 流式读优先返回 off-heap slice 或零拷贝视图

### Write path

写路径应当收敛为：

```text
estimate upper bound
  -> reserve memory
  -> mutate native root
  -> update entry metadata
  -> commit delta
  -> release replaced handles
```

必须保留的语义：

- wrong type 仍然报 Redis 风格错误。
- maxmemory 仍然由 mutation reservation 和 eviction 协作控制。
- 原地增长优先；不够时再分配新块并切换 handle。
- 失败时要么不改变可见状态，要么完整回滚。

## Compatibility Layer

`YierdisObject` 不会马上消失，但它不再是 canonical storage。

它的新角色是：

- 兼容旧路径的适配器。
- 供少量 introspection / migration code 使用的翻译层。
- 作为迁移期间的可读性和测试桥梁。

设计约束是：

- keyspace 里不再存 `YierdisObject` 作为主值。
- 类型实现不再依赖 `YierdisObject` 作为数据宿主。
- 任何新 hot path 都不能把 `YierdisObject` 当主存储形式。

## Module Boundaries

这次重构的主要落点应该仍然集中在存储和内存模块：

- `yierdis-db/yierdis-db-memory`
- `yierdis-memory/yierdis-memory-ffm`

其中：

- memory 模块负责更底层的 slab / handle / layout 原语。
- db-memory 模块负责 `EntryTable`、`KeyDirectory`、`TypeRoot` 和各类型引擎。
- `yierdis-db-api` 只在确实需要暴露新 handle contract 时才改。

server、command、networking 层不应该因为这次重构而改协议或命令面。

## Migration Order

为了避免一次性翻掉整个内核，迁移顺序应当是：

1. 先把 key directory 和 entry table 立起来，让 keyspace 不再以 `YierdisObject` 为主值。
2. 先迁 string，因为它最容易验证 handle 生命周期、zero-copy read 和 release。
3. 再迁 list / hll，因为它们已经有比较明确的编码边界。
4. 再迁 hash / set / zset，把复合编码统一到 native root 体系。
5. 最后收紧 `YierdisObject`，让它只做兼容，不再影响主路径。

这个顺序的目的不是“按模块好看”，而是尽早把最关键的 heap 对象图砍薄，同时保留可验证的回退面。

## Risks

- 双模型并存太久会让 bug 面积变大。缓解方式是尽快让 string 先走完完整链路，再逐类型切换。
- handle 生命周期错误会导致泄漏或悬挂引用。缓解方式是把 release 统一收口到 `unlinkEntry(...)` 和单一替换路径。
- snapshot / scan 很容易在 rehash 或迁移期间出错。缓解方式是把它们绑定到 entry table 的稳定 cursor 语义上。
- 继续保留旧对象壳会诱发回退。缓解方式是明确规定：`YierdisObject` 只能做适配，不能做 canonical storage。

## Success Criteria

这次设计真正落地后，至少要满足这些条件：

- `YierdisDb` 不再把 `YierdisObject` 作为 keyspace 的主 value。
- live data 的 canonical owner 变成 `EntryTable` + type-specific native roots。
- key / value / TTL / eviction 都能通过统一的 handle 生命周期正确释放。
- string 路径能完全证明 new model 的正确性，然后再推广到复合类型。
- 现有 off-heap 和 streaming 相关测试继续通过，并新增针对 entry/handle/lifetime 的回归测试。

## Summary

这不是“把 Java 对象换个名字”，而是把 DB 内核改成真正的 native storage core：

```text
FFM slabs
+ 64-bit handles
+ EntryTable
+ KeyDirectory
+ TypeRoot per data type
+ unified unlink / expire / eviction
+ adapter-only YierdisObject
```

它比现在更纯，也比全量 native 重写更适合分阶段落地。
