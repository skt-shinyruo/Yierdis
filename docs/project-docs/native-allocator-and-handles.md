# Native Allocator And Handles

本文记录当前生产 native allocator、stable handle、object table、DB memory handle 迁移和 active defrag 的实现语义。它补充 [`ffm-usage.md`](./ffm-usage.md)：`ffm-usage.md` 负责解释 JDK FFM 和项目整体 native memory 路径，本文专门解释 production allocator 这层。

## 为什么需要 stable handle

DB 层不能把 native 物理地址当作长期引用保存。物理地址一旦被 `realloc`、碎片整理或 page allocator 复用改变，旧地址就可能变成悬空引用，甚至误读到另一个对象。

当前设计把跨层引用统一收口成 64-bit `NativeHandle`：

```text
DB / type root / entry metadata
  保存 stable NativeHandle raw value

YierdisStableNativeAllocator
  用 handle.slotId() 查 object table
  再解析到当前 physical block

YierdisNativePageAllocator
  管理真实 FFM page / span / offset
```

因此 handle 是跨层 ABI，物理 page id、page offset、region、span 和 packed address 都是 allocator 私有实现细节。DB hot path 只能在一次有界操作内 resolve handle，不能缓存 native address。

## `NativeHandle` 位布局

`NativeHandle` 是一个 64-bit raw value。`0` 是 null handle，非零 handle 不能使用 reserved domain。

```text
bits 63..60  domain      4 bits
bits 59..56  kind        4 bits
bits 55..16  slotId     40 bits
bits 15..4   generation 12 bits
bits 3..0    flags       4 bits
```

字段语义：

- `domain`：大类边界，来自 `NativeHandleDomain`。
- `kind`：domain 内对象种类，来自 `NativeObjectKind.code()`。
- `slotId`：object table slot，最大 40-bit 编号空间。
- `generation`：slot 复用代数，防止旧 handle 命中新对象。
- `flags`：预留给 allocator 内部或未来 ABI 扩展。

当前 domain 包括：

- `STORAGE_OBJECT`
- `ENTRY_OBJECT`
- `KEY_BYTES`
- `TYPE_ROOT`
- `INDEX_NODE`
- `ALLOCATOR_METADATA`

当前 object kind 包括：

- `GENERIC`
- `STRING_BYTES`
- `ENTRY_RECORD`
- `KEY_BYTES`
- `LIST_NODE`
- `HASH_NODE`
- `SET_NODE`
- `ZSET_NODE`
- `INDEX_NODE`
- `METADATA_RECORD`

`NativeHandle.of(...)` 会校验 domain/kind 是否匹配、slot/generation/flags 是否在位宽范围内。`EntryHandle` 只允许包装 `ENTRY_RECORD` handle；`ValueHandle` 可以包装不同 value/root 相关 domain，但调用方需要在边界处校验 domain。

注意：当前只有 `EntryHandle` 一定是 object-table-backed allocator handle。`ValueHandle` 采用同一套 bit layout 做 type-root-owned typed identity；除非对应 root 明确把对象交给 `NativeAllocator` 分配，否则不能把任意 `ValueHandle` 拿去 `NativeAllocator.resolve(...)`。

## Object table

`YierdisNativeObjectTable` 是 handle 到对象 metadata 的权威表。每个 slot 使用 72 bytes metadata，记录：

- physical packed address
- logical size
- allocation capacity
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

slot 初始 generation 是 `1`。释放 slot 时 generation 递增；如果 generation 达到 12-bit 最大值，slot 被 retired，不再进入 free list。这样可以降低 generation wrap 后 ABA 命中的风险。

### 状态机

object table 当前状态包括：

```text
FREE
ALLOCATED
PINNED
MOVING
FREED_QUARANTINED
CORRUPT
```

正常路径：

```text
FREE -> ALLOCATED
ALLOCATED -> PINNED
PINNED -> ALLOCATED
ALLOCATED -> MOVING
MOVING -> ALLOCATED
ALLOCATED -> FREED_QUARANTINED
PINNED -> FREED_QUARANTINED
FREED_QUARANTINED -> FREE
ALLOCATED -> FREE
```

关键约束：

- `resolve` 只能接受 live handle，默认不能访问 quarantined slot。
- stale slot、generation mismatch、null handle、unknown slot 会抛 `StaleNativeHandleException`。
- domain/kind mismatch 会抛 native memory 异常，防止把 entry handle 当成 string 或 index handle 使用。
- moving 状态只能由 defrag move 协议进入；发布成功后回到 allocated，失败时 abort 回到 allocated。
- quarantined slot 在 pin 或 epoch 未安全前不能回收到 free list。

## Page / size-class allocator

真实内存由 `YierdisNativePageAllocator` 管理。它把 FFM region 抽象成 `YierdisNativeBlock`，block 记录：

- owning allocator
- backing region
- region offset
- requested bytes
- capacity
- page id
- page offset
- page count
- page class
- small size class

page 大小固定为 64 KiB。

小对象使用 size class，共 23 档：

```text
16, 24, 32, 48, 64, 96, 128, 192,
256, 384, 512, 768, 1024, 1536, 2048,
3072, 4096, 6144, 8192, 12288, 16384,
24576, 32768
```

请求大小不超过 32768 bytes 时走 small page。每个 small page 只服务一个 size class，free offsets 进入页内 free list。

超过 32768 bytes 时走 span：

- `MEDIUM_SPAN`：请求大小不超过 1 MiB。
- `LARGE_SPAN`：请求大小超过 1 MiB。

span 按 64 KiB page 数向上取整分配。释放 span 会关闭对应 FFM region 并回收 committed/used accounting；small page 释放 block 时只把 offset 放回页内 free list。

当前 packed address 是 allocator 私有格式：

```text
packedAddress = (pageId << 32) | unsigned(pageOffset)
```

外部代码不能解析或持久化这个值。

## Stable allocator API

`NativeAllocator` 是 allocator API 边界，当前生产实现是 `YierdisStableNativeAllocator`。

核心方法：

- `allocate(kind, size)`：分配对象并返回 stable `NativeHandle`。
- `realloc(handle, newSize, policy)`：调整对象逻辑大小。
- `free(handle)`：释放对象；必要时进入 quarantine。
- `pin(handle)` / `unpin(handle)`：显式 pin 对象。
- `beginEpoch(kind)`：打开 command、scan、snapshot 或 defrag epoch。
- `resolve(handle, mode)`：返回有界 `NativeObjectView`。
- `defragOne(handle, maxMoveBytes)`：尝试移动单个对象。
- `defragCycle(options)`：按预算执行一轮 active defrag。
- `stats()`：返回 allocator 统计。

`NativeObjectView` 是短生命周期视图。`resolve` 会 pin 对象；`view.close()` 会 unpin。读写通过 `getByte`、`setByte`、`getBytes`、`setBytes` 进行，所有 offset 都按对象 logical size 做边界检查。`READ_ONLY` 视图禁止写入。

调用约束：

- resolve 后必须关闭 view。
- 不允许缓存 view 背后的 physical address。
- pinned 对象不能被 moving realloc 或 defrag 移动。
- stale handle、double free、kind/domain mismatch 都应该在 allocator 层被发现。

## `realloc` 语义

`realloc` 的输入是 stable handle，不会返回新 handle；成功后同一个 handle 解析到新的 size/capacity/location。

路径分三类：

1. 新大小小于等于当前 capacity：只更新 object table size，计为 in-place realloc。
2. 新大小超过当前 capacity 且 policy 是 `NO_MOVE`：抛异常，不改变原对象。
3. 新大小超过当前 capacity 且 policy 允许移动：分配新 block，复制旧 size 前缀，发布新 location，旧 block 进入 epoch-safe 释放路径。

失败回滚规则：

- 新 block 分配后，如果复制或 metadata 更新失败，新 block 会关闭。
- object table 发布前失败时，handle 仍指向旧 block。
- 发布后才切换 `allocation.current`。
- 旧 block 不会过早释放；如果 epoch 不安全，进入 retained moved blocks。

这使 `realloc` 接近小事务：要么完整成功，要么保持原对象可访问。

## Pin / epoch / quarantine

allocator 用两层保护解决“正在读的对象不能被释放”：

- `pinCount`：单个对象级别，主要来自 resolved view。
- `NativeEpochScope`：批量读或维护操作级别，分 command、scan、snapshot、defrag。

释放对象时：

```text
free(handle)
  -> 如果 pinCount > 0 或存在阻止回收的 active epoch
       object table 标记 FREED_QUARANTINED
       allocation 标记 quarantined
  -> 否则立即 release slot 和 physical block
```

移动对象时：

```text
old block retired
  -> 如果 epoch 安全，立即关闭 old block
  -> 否则进入 retained moved blocks
```

epoch reclaim 规则是：只要存在 epoch 小于等于 retired epoch，就不能回收这批 retired memory。epoch scope 关闭时 allocator 会尝试 `reclaimEligibleQuarantine()`。

这样可以同时避免两类错误：

- 太早释放：读者还持有 view 或 epoch 时访问悬空 memory。
- 太晚释放：已安全的 quarantined object / moved block 长期占用 native memory。

## Active defrag

active defrag 的基本目标是移动 live object，释放旧 block 或让旧 block 在 epoch 安全后释放。DB 层只保存 stable handle，所以对象移动不需要全量扫描 DB 引用并重写地址。

单对象移动协议：

```text
beginMove(handle)
  要求对象是 ALLOCATED 且 pinCount == 0
  object table 状态改成 MOVING

allocate target block
copyPrefix(old, target, size)
validate(handle, sourceMeta, target)

publishMoved(handle, size, targetCapacity, targetAddress, targetPageClass)
  object table 改成新 address/capacity/page class
  状态回到 ALLOCATED

old block 进入 epoch-safe reclaim
```

失败协议：

```text
如果 publish 前失败：
  abortMove(handle)
  target block close
  handle 继续指向 old block

如果 validation 失败：
  同样 abort，旧对象保持可读写
```

`defragOne` 会检查 `maxMoveBytes` 和 pin count。`defragCycle` 使用 `NativeDefragOptions` 控制：

- `maxMoveBytes`
- `maxObjects`
- `timeBudgetNanos`

报告字段包括 scanned objects、moved objects、moved bytes、skipped pinned、skipped budget、failed moves 和预算停止原因。

## DB memory 层迁移

### `EntryHandle`

`EntryHandle` 现在只是 `NativeHandle` raw value 的类型包装。它要求 handle 必须是：

```text
domain = ENTRY_OBJECT
kind   = ENTRY_RECORD
```

`EntryTable` 用 `NativeAllocator.allocate(ENTRY_RECORD, 56)` 分配 entry metadata。entry record 的 native layout 是：

```text
0   key handle identity   8 bytes
8   value handle raw      8 bytes
16  key hash              4 bytes
20  value type ordinal    4 bytes
24  value encoding ordinal 4 bytes
28  flags                 4 bytes
32  expireAtMillis        8 bytes
40  version               8 bytes
48  LRU/LFU               8 bytes
```

读写 entry 时，`EntryTable` resolve `ENTRY_RECORD` handle，得到 `NativeObjectView`，按固定 offset 读写字段。释放 entry 时调用 allocator free。

### `ValueHandle`

`ValueHandle` 也是 `NativeHandle` raw value 包装，但承载的是 value/root 侧引用：

- string 使用 `STRING_BYTES` kind。
- list 使用 `LIST_NODE` kind。
- hash 使用 `HASH_NODE` kind。
- set 使用 `SET_NODE` kind。
- zset 使用 `ZSET_NODE` kind。

当前 type roots 仍各自拥有 payload adapter 或 off-heap buffer 管理结构；`ValueHandle` 负责给 `EntryRecord` 一个稳定、带 domain/kind 的引用形状。`StringRoot` 的 payload bytes 仍经 `OffHeapAllocator` 管理，集合 roots 仍包装 `ListValue` / `HashValue` / `SetValue` / `ZSetValue`。

因此，当前 `ValueHandle.slotId()` 是对应 root 的局部 identity，不等同于 `YierdisNativeObjectTable` slot。后续如果把某类 payload 也迁入 stable allocator，需要在该 root 内明确完成 allocate / resolve / free 协议，并补齐对应测试。

### key graph

主图现在可以理解为：

```text
NativeKeyDirectory
  key bytes -> EntryHandle(raw NativeHandle)

EntryTable
  EntryHandle -> native ENTRY_RECORD

EntryRecord
  ValueType + ValueEncoding + ValueHandle(raw NativeHandle) + TTL/LRU/accounting

TypeRoot
  ValueHandle -> payload adapter / off-heap bytes
```

关键变化是：entry metadata 进入 production stable allocator，DB 层不保存 entry slot 的物理地址。

## Metrics

`NativeAllocatorStats` 暴露以下观测面：

- logical used bytes
- reserved bytes
- committed bytes
- free bytes
- internal fragmentation bytes
- external fragmentation bytes
- small / medium / large free bytes
- live small pages
- live medium span pages
- live large span pages
- free pages
- live objects
- pinned objects
- quarantined objects
- quarantine bytes
- stale handle detections
- double free detections
- realloc in-place count
- realloc moved count
- defrag moved bytes
- defrag skipped pinned objects
- defrag reclaimed pages
- object kind counts
- allocation latency histogram

几个口径需要区分：

- `logicalUsedBytes`：对象请求的逻辑 size 总和。
- `reservedBytes`：当前 live block 和 retained moved block 的 capacity 总和。
- `committedBytes`：page allocator 当前实际向 runtime 申请的 native bytes。
- `internalFragmentationBytes`：reserved 与 logical 的差。
- `externalFragmentationBytes`：page allocator free bytes 扣除 retained moved block bytes 后的可复用空闲。
- `quarantineBytes`：freed quarantined object 和 moved old block 仍占用的 bytes。

## 测试覆盖

allocator 相关测试分布在三层：

- `yierdis-memory-api`
  - `NativeHandleTest`
  - `NativeAllocatorContractTest`
- `yierdis-memory-ffm`
  - `YierdisNativeObjectTableTest`
  - `YierdisNativePageAllocatorTest`
  - `YierdisStableNativeAllocatorTest`
- `yierdis-db-memory`
  - `EntryHandleContractTest`
  - `ValueHandleContractTest`
  - `EntryTableContractTest`
  - `NativeStorageRegressionTest`
  - `YierdisDbMemoryReporterTest`
  - `NativeKeyDirectoryTest`

改 allocator API、object table、pin/quarantine、realloc、defrag 或 DB handle 迁移时，至少跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-architecture-tests -am test
```
