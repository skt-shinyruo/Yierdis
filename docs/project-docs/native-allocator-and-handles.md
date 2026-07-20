# Native Allocator 与 Handles

本文解释 production stable native allocator、handle ABI、object table、pin/epoch/quarantine、active defrag，以及 DB handle 语义。它接在 [`native-memory-runtime.md`](./native-memory-runtime.md) 后面：runtime 管 region 和 FFM lifetime，allocator 管稳定对象身份和可移动 native block。

## 为什么需要 stable handle

DB 层不能把 native physical address 当作长期引用保存。physical address 可能因为 `realloc`、active defrag、page reuse 或 release 失效；旧地址一旦被继续使用，就可能读到悬空 memory 或另一个对象。

Yierdis 用 64-bit `NativeHandle` 作为跨层 ABI：

```text
DB / key directory / entry table / type roots
  store NativeHandle raw values

YierdisStableNativeAllocator
  decode domain/kind/slot/generation
  resolve through YierdisNativeObjectTable

YierdisNativePageAllocator
  manage actual FFM-backed physical blocks
```

所以 stable handles are not raw physical addresses。handle 是稳定 identity；physical packed address、page id、page offset、region 和 span 都是 allocator 私有细节。DB hot path 只能在一次有界操作里 resolve handle，不能缓存 resolved address 或长期持有 `NativeObjectView`。

## NativeHandle 位布局

`NativeHandle` 是一个 64-bit raw value。`0` 表示 null handle；非零 handle 不能使用 reserved domain。

```text
bits 63..60  domain      4 bits
bits 59..56  kind        4 bits
bits 55..16  slotId     40 bits
bits 15..4   generation 12 bits
bits 3..0    flags       4 bits
```

字段含义：

- `domain`：大类边界，来自 `NativeHandleDomain`。
- `kind`：domain 内对象种类，来自 `NativeObjectKind.code()`。
- `slotId`：object table slot，不是地址。
- `generation`：slot 复用代数，降低 stale handle 命中新对象的风险。
- `flags`：预留给 allocator 内部或未来 ABI。

当前重要 kind 包括：

- `KEY_BYTES`
- `ENTRY_RECORD`
- `STRING_BYTES`
- `LIST_ROOT`
- `HASH_ROOT`
- `SET_ROOT`
- `ZSET_ROOT`
- `LIST_NODE`
- `LISTPACK_BYTES`
- `HASH_FIELD_BYTES`
- `HASH_VALUE_BYTES`
- `SET_MEMBER_BYTES`
- `ZSET_MEMBER_BYTES`
- `INDEX_NODE`
- `METADATA_RECORD`

`NativeHandle.of(...)` 会检查 domain/kind 是否匹配，以及 slot/generation/flags 是否在位宽范围内。`EntryHandle` 只允许包装 `ENTRY_RECORD`；`ValueHandle` 可以包装 string 或 collection root 相关 native handle，但调用边界必须校验 domain/kind。

## DB hot path 只保存 stable handle

DB hot path 保存的是 stable handle raw value，不是 physical address，也不是 `NativeObjectView`。`EntryHandle` 和 `ValueHandle` 只是 typed wrapper：前者约束 `ENTRY_RECORD` 语义，后者约束 string / collection root 语义。调用方只应该在一次有界操作里 `resolve(...)`，拿到短生命周期 view 后立刻关闭，不能把 resolved address 缓存到 DB graph 里。

## domain / kind / generation 校验失败意味着什么

`NativeHandle.of(...)`、`resolve(...)` 和 wrapper factory 会把 domain / kind / generation 当作 ABI 和生命周期护栏，而不是调试辅助。校验失败意味着 stale handle、wrong kind/domain 或 slot 复用错误，应该 fail fast。`pin`、`quarantine` 和 active defrag 之所以能成立，依赖的就是这组校验不会被绕过。

## Object table

`YierdisNativeObjectTable` 是 handle 到对象 metadata 的权威表。当前每个 slot 固定为 `36 bytes`：page offset、size、page id、两个 epoch 和 pin count 使用独立字段；generation、domain、kind、flags、page class、state 打包在一个 32-bit word 中。capacity 不在 slot 内重复保存，而是按 page id、offset 和 page class 从 small-page size class 或 span descriptor 派生；allocate、realloc location update 和 defrag publish 都会校验调用方声明的 capacity 与 descriptor 一致。owner shard id 属于整张 object table，也不在每个 slot 重复保存。

```text
offset  0: page offset       (int32)
offset  4: logical size      (int32)
offset  8: page id           (int32)
offset 12: packed metadata   (int32)
offset 16: alloc epoch       (int64)
offset 24: free epoch        (int64)
offset 32: pin count         (int32)
```

每个 slot 的逻辑信息包括：

- page-local physical offset
- logical size
- 由 page/span descriptor 派生的 capacity
- segment id
- page class
- generation
- domain
- kind code
- flags
- pin count
- object table 继承的 owner shard id
- alloc epoch
- free epoch
- state

slot 初始 generation 为 `1`。释放 slot 时 generation 递增；generation 达到 12-bit 最大值后，slot 会 retired，不再回到 free list，避免 wrap 后旧 handle 命中新对象。

对象状态包括：

```text
FREE
ALLOCATED
PINNED
MOVING
FREED_QUARANTINED
CORRUPT
```

常见约束：

- `resolve` 只接受 live handle。
- null handle、unknown slot、generation mismatch 会触发 stale handle 语义。
- domain/kind mismatch 是错误，不能把 entry handle 当 string handle 用。
- moving 状态只属于 allocator move/defrag 协议。
- quarantined slot 在 pin 或 epoch 未安全前不能回收。

## Page 和 size class

`YierdisNativePageAllocator` 管真实 FFM-backed block。它向 `YierdisFfmMemoryRuntime` 申请 region，并把 region 切成 page 或 span。

page 大小固定为 64 KiB。小对象按 size class 分配，当前档位包括：

```text
16, 24, 32, 48, 64, 96, 128, 192,
256, 384, 512, 768, 1024, 1536, 2048,
3072, 4096, 6144, 8192, 12288, 16384,
24576, 32768
```

请求大小不超过 32768 bytes 时走 small page；每个 small page 只服务一个 size class。更大的对象走 span：

- `MEDIUM_SPAN`：请求大小不超过 1 MiB。
- `LARGE_SPAN`：请求大小超过 1 MiB。

packed address 当前是 allocator 私有格式：

```text
packedAddress = (pageId << 32) | unsigned(pageOffset)
```

外部代码不能解析、保存或比较它来表达对象 identity。

## Stable allocator API

生产实现是 `YierdisStableNativeAllocator`，实现 `NativeAllocator`。

核心 API：

- `allocate(kind, size)`：分配 native object，返回 stable `NativeHandle`。
- `resolve(handle, mode)`：返回短生命周期 `NativeObjectView`。
- `realloc(handle, newSize, policy)`：调整对象大小，handle 不变。
- `free(handle)`：释放对象，必要时进入 quarantine。
- `pin(handle)` / `unpin(handle)`：显式 pin。
- `beginEpoch(kind)`：打开 command、scan、snapshot 或 defrag epoch。
- `defragOne(handle, maxMoveBytes)`：按预算尝试移动一个对象。
- `defragCycle(options)`：按对象数、bytes、时间预算运行 active defrag。
- `stats()`：导出 allocator stats。

`NativeObjectView` 是 resolve 后的短生命周期访问视图。resolve 会 pin 对象；`view.close()` 会 unpin。view 读写方法按 logical size 检查 offset，`READ_ONLY` mode 禁止写。

调用规则：

- resolve 后必须 close view。
- 不允许缓存 view 背后的 physical address。
- pinned 对象不能被 moving realloc 或 active defrag 移动。
- double free、stale handle、kind/domain mismatch 都应在 allocator 层 fail fast。

## realloc 语义

`realloc` 输入 stable handle，成功后仍返回同一个 identity。它改变的是 object table 中的 logical size、capacity 和 physical location。

主要路径：

1. `newSize <= capacity`：只更新 logical size，计为 in-place realloc。
2. `newSize > capacity` 且 policy 是 `NO_MOVE`：失败，不改变原对象。
3. `newSize > capacity` 且 policy 允许移动：分配新 block，复制旧 size 前缀，发布新 location，旧 block 进入 epoch-safe release。

失败回滚规则：

- 新 block 分配后，如果复制或 metadata 更新失败，新 block 会释放。
- object table 发布前失败，handle 仍指向旧 block。
- 发布后才切换 allocator allocation state。
- 旧 block 不会过早释放；epoch 不安全时进入 retained moved blocks。

这让 `realloc` 对 DB 来说像一次小事务：成功后 handle 解析到新位置，失败后旧对象仍可访问。

## pin / epoch / quarantine

allocator 用两层机制避免过早回收：

- `pinCount`：对象级保护，来自 resolved view 或显式 pin。
- `NativeEpochScope`：批量操作保护，当前 epoch kind 包括 command、scan、snapshot 和 defrag。

free 路径：

```text
free(handle)
  if pinCount > 0 or active epoch may still observe it
    mark FREED_QUARANTINED
    retain physical block / slot
  else
    release physical block and slot
```

move 路径：

```text
publish new location
retire old block
if epoch safe
  release old block
else
  keep retained moved block
```

epoch reclaim 规则是：只要存在可能观察 retired memory 的 active epoch，相关 quarantined memory 就不能释放。epoch scope 关闭时，allocator 会尝试 reclaim eligible quarantine。

这套机制覆盖两类风险：active view 还在读写时不能释放；scan/snapshot/maintenance 这类批量边界还没结束时不能回收 retired block。

## active defrag

active defrag 的目标是移动 allocator-backed live object，减少碎片或释放旧 block，同时保持 DB graph 中的 handle 不变。

可以移动的对象包括：

- `KEY_BYTES`
- `ENTRY_RECORD`
- `STRING_BYTES`
- collection root records：`LIST_ROOT`、`HASH_ROOT`、`SET_ROOT`、`ZSET_ROOT`
- collection internal records and bytes：`LIST_NODE`、`LISTPACK_BYTES`、`HASH_FIELD_BYTES`、`HASH_VALUE_BYTES`、`SET_MEMBER_BYTES`、`ZSET_MEMBER_BYTES`

DB native handle graph traversal must enumerate keys, entries, string values, collection roots, and collection internal handles. Defrag validation uses the same stable-handle model: handles stay stable while allocator-backed blocks may move behind the object table.

单对象移动协议：

```text
beginMove(handle)
  require ALLOCATED and pinCount == 0
  mark MOVING

allocate target block
copy old bytes to target
validate source metadata and target

publishMoved(...)
  update object table location/capacity/page class
  mark ALLOCATED

retire old block through epoch-safe reclaim
```

如果 publish 前失败，allocator abort move、释放 target，handle 继续指向旧 block。`defragCycle` 由 `NativeDefragOptions` 控制 `maxMoveBytes`、`maxObjects` 和 `timeBudgetNanos`，报告 moved、skipped pinned、skipped budget、failed moves 等统计。

## DB memory handle 迁移

DB memory graph 当前按 stable handle 组织：

```text
NativeKeyDirectory
  key bytes: KEY_BYTES
  maps to EntryHandle

EntryTable
  EntryHandle -> ENTRY_RECORD

EntryRecord
  stores ValueHandle raw

Type roots
  STRING_BYTES or collection root records
```

`NativeKeyDirectory` 用 allocator 分配 `KEY_BYTES` 并写入 key bytes。目录 table、hashes、states 仍是 heap arrays；key bytes object 才是 native allocator 对象。

`EntryTable` 用 `ENTRY_RECORD` 保存 entry metadata。entry record layout 是固定 56 bytes：

```text
0   key handle identity      8 bytes
8   value handle raw         8 bytes
16  key hash                 4 bytes
20  value type ordinal       4 bytes
24  value encoding ordinal   4 bytes
28  flags                    4 bytes
32  expireAtMillis           8 bytes
40  version                  8 bytes
48  LRU/LFU                  8 bytes
```

layout 中的 `version` 是为兼容既有格式保留的字段名，当前值是 entry accounting estimate。它不是 mutation version 或乐观锁序号；prepared mutation 的 stale-plan 校验依赖 source adapter/table、generation、size、raw handle 和旧 value 等显式条件。该 estimate 也不保存 collection 的完整 heap topology，后者由各 adapter 的 `heapBytes()` / staged growth 单独计量。

`StringRoot` 使用 `STRING_BYTES`。`set` 分配新 object，`append` / growth 通过 `realloc(..., PRESERVE_PREFIX)` 保持 handle 稳定，读写时短暂 resolve `NativeObjectView`。

collection root records 使用：

- list：`LIST_ROOT`
- hash：`HASH_ROOT`
- set：`SET_ROOT`
- zset：`ZSET_ROOT`

`NativeCollectionRootTable` 为 root record 分配 8 bytes 并写入自己的 handle raw value。Java adapter registry 是按 allocator slot id 直接寻址的 segmented `Object[][]`，不是 `Map<Long, adapter>`：每段 4096 个槽位，`segmentIndex = (slotId - 1) / 4096`，槽内 `AdapterSlot` 再比较完整 raw handle，防止 slot 复用后的旧 generation 命中新 adapter。目录使用 `Object[][]` 和 `int[]` live counts，按需扩展；空段会释放，其 directory、slot 和 adapter heap bytes 都进入 root table 计量。

root identity stable 不代表 payload physical block 或 Java topology 不变。现有 List staged mutation、existing ZSet `ZADD` 和 `HASH_HT` HSET delta 保持 `EntryRecord.valueHandle()` 不变，在原 adapter 内发布新 block/topology；packed Hash 和当前 Set 的 replacement 路径仍可能发布新 root。无论哪条路径，DB graph 都不能缓存 adapter 地址或 resolved native address。

collection 使用“native payload + heap topology”的混合布局：

- `NativeListpack` 把全部变长条目编码到单个连续 `LISTPACK_BYTES` block，heap 只保留 `int[] entryOffsets`。普通扩容通过 `realloc` 保持 block handle；detached build 一次申请最终大小，避免 prepare 后再次 native allocation。
- `NativeByteMap` 的 table topology 是 `byte[] states`、`int[] hashes`、`long[] keyRawHandles`，value slots 按用途选择 `long[]`、`Object[]` 或 constant。rehash replacement 复用已有 key raw handles，只替换 heap arrays。
- Set intset 使用 `short[]` / `int[]` / `long[]`；ZSet skiplist 的排序 topology 使用 Java nodes、`Node[] forward` 和 `int[] span`。因此“primitive heap topology”表示索引和紧凑 metadata 优先放在 primitive/raw-handle arrays 中，不表示所有算法节点都已消除 Java object reference。
- List quicklist metadata 使用 `LIST_NODE`，packed payload 使用 `LISTPACK_BYTES`；hash、set、zset 的独立 bytes 使用各自 object kind。只有 allocator-owned handles 进入 native handle graph 和 active defrag traversal，heap topology 由 adapter 自己计量和验证。

ZSet skiplist 的 member payload 是 canonical ownership 的典型边界。每个 member 只分配一个 `ZSET_MEMBER_BYTES`，skiplist node 持有它，`byMember` 使用 borrowed-key `NativeByteMap` 索引同一 raw handle。borrowed map 的 remove/clear/close 只移除索引槽位，不 free member；`memberStore` 是唯一 owner。Hash/Set 的 owning-key map 则在新增 key 时分配 payload，并在 remove/clear/close 或 prepared abort 时释放。

## Prepared mutation 与计费边界

增长型 collection 写入先 reserve upper bound，再打开 `NativeAllocationScope` 并进入 prepare。prepare 负责 native payload、root、replacement table、listpack offset、skiplist node/path 等所有可能失败的分配，并保存 source adapter/table、generation、size、raw handle 和旧 value 条件。commit 前重新校验这些条件；需要 commit stream 时也在此时预留 publication capacity。已 staged 的 commit 只发布数组引用、槽位或节点链接，不再申请 native payload。

prepare 后的峰值 reservation 按 `NativeAllocationGrowth.effectiveBytes() + stagedNonNativeGrowthBytes()` reconcile。前者来自 allocation scope 实测的 allocator heap metadata、native metadata 和 data committed growth，后者覆盖 adapter segment、primitive arrays、Java node/path 和其他非 native topology。成功路径固定为 commit、allocation promote、logical ledger settle、commit-stream publish、release superseded，最后在启用 maxmemory 且 mutation 给出提示时尝试 trim empty pages。先发布新 graph 再释放旧 payload，保证 stable root、entry 和 borrowed index 在任一可见状态都只指向 live object。

`actualDeltaBytes` 是发布并清理旧资源后的稳态逻辑增量，不是物理 snapshot。commit 开始前失败时，prepared resource、scope、stream reservation 和 ledger reservation 可以各自 abort/rollback；commit 开始后则必须走 post-commit settle/result-unknown，不能回收已经可能被可见 graph 引用的新 allocation。`shouldTrimNativePagesAfterCommit()` 只表示 superseded release 可能产生空页；是否真的减少 committed bytes 要结合 `MemoryReclaimResult` 并重新采样 owned `MemoryUsageSnapshot`。

## metrics

`NativeAllocatorStats` 暴露 allocator 观测面，包括：

- logical used bytes
- reserved bytes
- committed bytes
- free bytes
- internal fragmentation bytes
- external fragmentation bytes
- small / medium / large free bytes
- live small pages
- live medium / large span pages
- live objects
- pinned objects
- quarantined objects
- quarantine bytes
- stale handle detections
- double free detections
- realloc in-place / moved count
- defrag moved bytes
- defrag skipped pinned objects
- defrag reclaimed pages
- object kind counts
- allocation latency histogram

几个口径要分清：

- `logicalUsedBytes`：对象请求的 logical size 总和。
- `reservedBytes`：live block 和 retained moved block 的 capacity 总和。
- `committedBytes`：page allocator 当前向 runtime 申请的 native bytes。
- `internalFragmentationBytes`：reserved 与 logical 的差。
- `externalFragmentationBytes`：page allocator 可复用空闲。
- `quarantineBytes`：freed quarantined object 和 moved old block 仍占用的 bytes。

## Production Boundary

allocator 的 native usage 是 maxmemory 和 DB mutation ledger 的输入，不是 RESP reply capacity 的替代指标。生产排查必须同时看 native/maxmemory、ingress、commit-stream 和 outbound reply 账户；这些账户的关闭与验收口径见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
