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
- `LIST_NODE`
- `HASH_NODE`
- `SET_NODE`
- `ZSET_NODE`
- `LIST_QUICKLIST_NODE`
- `INDEX_NODE`
- `METADATA_RECORD`

`NativeHandle.of(...)` 会检查 domain/kind 是否匹配，以及 slot/generation/flags 是否在位宽范围内。`EntryHandle` 只允许包装 `ENTRY_RECORD`；`ValueHandle` 可以包装 string 或 collection root 相关 native handle，但调用边界必须校验 domain/kind。

## DB hot path 只保存 stable handle

DB hot path 保存的是 stable handle raw value，不是 physical address，也不是 `NativeObjectView`。`EntryHandle` 和 `ValueHandle` 只是 typed wrapper：前者约束 `ENTRY_RECORD` 语义，后者约束 string / collection root 语义。调用方只应该在一次有界操作里 `resolve(...)`，拿到短生命周期 view 后立刻关闭，不能把 resolved address 缓存到 DB graph 里。

## domain / kind / generation 校验失败意味着什么

`NativeHandle.of(...)`、`resolve(...)` 和 wrapper factory 会把 domain / kind / generation 当作 ABI 和生命周期护栏，而不是调试辅助。校验失败意味着 stale handle、wrong kind/domain 或 slot 复用错误，应该 fail fast。`pin`、`quarantine` 和 active defrag 之所以能成立，依赖的就是这组校验不会被绕过。

## Object table

`YierdisNativeObjectTable` 是 handle 到对象 metadata 的权威表。每个 slot 记录：

- physical packed address
- logical size
- capacity
- segment id
- page class
- generation
- domain
- kind code
- flags
- pin count
- owner shard id
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
- collection root records：`LIST_NODE`、`HASH_NODE`、`SET_NODE`、`ZSET_NODE`
- list quicklist metadata records：`LIST_QUICKLIST_NODE`

不能过度声称的边界：

- active defrag 不移动 adapter-owned payload bytes。
- active defrag 不整理仍由 legacy FFM structures 拥有的 hash/set/zset internals。
- collection root record 可移动，不等于所有 collection internal nodes/payload bytes 都已经 fully nativeized。

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

`StringRoot` 使用 `STRING_BYTES`。`set` 分配新 object，`append` / growth 通过 `realloc(..., PRESERVE_PREFIX)` 保持 handle 稳定，读写时短暂 resolve `NativeObjectView`。

collection root records 使用：

- list：`LIST_NODE`
- hash：`HASH_NODE`
- set：`SET_NODE`
- zset：`ZSET_NODE`

`NativeCollectionRootTable` 为 root record 分配 8 bytes，写入自己的 handle raw value，并用 `Map<Long, adapter>` 把 stable native root identity 映射到当前 Java adapter。root record 进入 allocator stats 和 defrag；adapter-owned payload bytes 仍按各类型实现管理。

list quicklist metadata records 使用 `LIST_QUICKLIST_NODE` 进入 allocator。这里仍要区分 metadata record 和 payload bytes：metadata nativeized 不代表 list entry payload 或所有 collection internals 都已经 nativeized。

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
