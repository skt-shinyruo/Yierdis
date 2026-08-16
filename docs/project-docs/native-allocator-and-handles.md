# Stable Memory Backend 与 Handles

本文解释 stable handle、object table、page allocation、pin/epoch/quarantine、realloc 和 active defrag。FFM runtime/region ownership 见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## 为什么需要 stable handle

DB 不能把 native physical address 当作长期引用。realloc、defrag、page reuse 或 release 都会让旧 location 失效。

生产实现 `YierdisFfmStableMemoryBackend` 通过 object table 把稳定 identity 与当前物理 block 分开：

```text
DB graph
  stores NativeHandle / typed wrappers

YierdisFfmStableMemoryBackend
  validates backend identity and resolves localRaw

YierdisNativeObjectTable
  maps stable slot/generation to current page location

YierdisNativePageAllocator
  owns FFM-backed pages and spans
```

调用方只在有界操作内 resolve handle，使用短生命周期 `NativeObjectView`，随后 close。physical page、offset、capacity 和 segment 都是 backend 私有状态。

## 公共 handle 与私有 localRaw

公共 API 是：

```java
public record NativeHandle(long allocatorId, long localRaw)
```

- `allocatorId` 标识创建 handle 的 backend；进程内不复用。
- `localRaw` 是只允许所属 backend 解释的不透明值。
- 只有两个分量都为 `0` 时才是 `NativeHandle.NULL`。
- realloc 和 defrag 不改变两个分量；free 后该 identity 失效。

FFM backend 私有的 `YierdisLocalHandleCodec` 才把 `localRaw` 编成 64 bit：

```text
bits 63..60  domain       4 bits
bits 59..56  kind         4 bits
bits 55..16  slotId      40 bits
bits 15..4   generation  12 bits
bits 3..0    flags        4 bits
```

DB/API 调用方不得复制这套 codec，也不得把 `localRaw` 当作完整 handle。`EntryHandle`、`ValueHandle` 和 `KeyHandle` 必须保留 paired identity；跨 backend、wrong kind/domain、unknown slot 和 generation mismatch 都 fail fast。

## Object table

`YierdisNativeObjectTable` 是 `localRaw` 到当前 location metadata 的权威表。每个 FFM segment 含 4,096 个 36-byte slots；slot 保存 page offset、logical size、page id、packed state、allocation/free epoch 和 pin count。capacity 从 page/span descriptor 派生，不在 slot 中重复维护。

table 使用一个按实际长度增长的 segment array。分配先扫描现有 segments 的可复用 slot，再在需要时追加 segment。generation 在 slot 复用时递增；12-bit generation 用尽的 slot 被永久 retired，避免 wrap 后旧 handle 命中新对象。

主要状态转换包括 allocated、pinned、moving、freed-quarantined、free 和 corrupt。object table 负责：

- 校验 allocator/domain/kind/slot/generation；
- resolve 当前 page location；
- pin/unpin 与 quarantined release；
- realloc/defrag 的 begin move、publish location 和 abort move；
- cursor scan、统计和 scope rollback。

## Pages、spans 与 registry

`YierdisNativePageAllocator` 使用一个直接按 page id 索引的 `Object[]` registry。slot 保存 `SmallPage` 或 `SpanAllocation`，不再维护第二套 native page directory。

page 大小为 64 KiB。请求不超过 32,768 bytes 时进入单一 size-class small page；档位为：

```text
16, 24, 32, 48, 64, 96, 128, 192,
256, 384, 512, 768, 1024, 1536, 2048,
3072, 4096, 6144, 8192, 12288, 16384,
24576, 32768
```

更大对象按连续 region allocation 进入：

- `MEDIUM_SPAN`：不超过 1 MiB；
- `LARGE_SPAN`：超过 1 MiB。

block 对外只暴露 backend 所需的 capacity、page identity/class 和 byte access。requested size、page count、size class 等 test-only 镜像不保留在 block 中；真实信息由 registry descriptor 和 object table 决定。

## StableMemoryBackend API

`YierdisFfmStableMemoryBackend` 是生产 `StableMemoryBackend` 实现，负责：

- owner binding 与 shutdown；
- `allocate` / `reallocate` / `free`；
- `resolve` / `resolvePinned`；
- `pin` / `unpin`；
- command/scan/snapshot/defrag epochs；
- allocation scopes；
- single-object 与 cycle defrag；
- regions、growth estimate、trim、usage 和 stats。

每个普通操作先校验 owner 与 backend open。`resolve(...)` 返回拥有自身 retain 的 view，close 时释放；`resolvePinned(...)` 只允许 read-only，并借用调用方已持有的 pin。

FFM `NativeObjectView` 在 copy/comparison/typed default 前完成 lifecycle、writability 和完整范围检查。重叠 copy 保持 memmove 方向；无效 multi-byte/copy write 在改动任何 byte 前失败。

## Realloc

`reallocate(handle, newSize, policy)` 保持完整 `NativeHandle` 不变：

1. 新 logical size 可放进现有 capacity 时，只更新 object metadata。
2. `NO_MOVE` 且容量不足时失败，旧对象不变。
3. 允许移动时分配 target block，用一次 block-to-block native copy 保留旧 prefix。
4. object table 发布新 location 后，旧 block 立即释放或进入 retired list。

复制、metadata 校验或 publication 前的任何失败都会关闭未发布 target，并 abort move；handle 继续解析到旧 block。publication 后不会假装旧状态仍可回滚。

## Pin、epoch 与 quarantine

pin 保护当前 object，epoch 保护一个批量观察窗口：

- live view 或显式 `pin(...)` 增加 object pin count；
- command/scan/snapshot/defrag epoch 记录可能仍观察旧 location 的范围；
- free 一个仍 pinned 的 object 时，slot/block 进入 quarantine；
- move 发布后的旧 block 在所有相关 active epochs 结束前进入 retired list。

最后一个 pin 或 epoch 关闭时，backend 尝试回收 eligible quarantine/retired blocks。stable handle 仍能表示逻辑 identity，但 freed/quarantined handle 不能作为新的普通 resolve 入口。

## Allocation scope

prepare 阶段的 `NativeAllocationScope` 跟踪 allocation encounter order，并记录 object-table baseline segment count：

- allocation 自动加入 scope；
- explicit free 从 scope 移除；
- `promote()` 把存活 handles 转交给 committed graph；
- `abort()` 或未 promoted 的 close 逆序释放剩余 handles；
- abort 后关闭新增且为空的 tail segments，并截断 segment array；
- terminal operations 幂等。

scope 的 `growth()` 保留从进入以来的峰值，包括 transient growth。实现不承诺 scope open 无复制、abort 无分配或其他 allocation-free 形状；验收只依赖 ownership、rollback 和保守 accounting。

## Active defrag

active defrag 选择未 pinned 的 live object，分配 target、复制 bytes、运行 validator，再发布新 location。handle、kind、logical size 和 DB graph identity不变。

```text
beginMove(handle)
  -> allocate target
  -> copy current logical bytes
  -> validate source/target
  -> publishMoved(...)
  -> retire old block through epoch reclaim
```

validator 或 copy 在 publication 前失败时执行 `abortMove()` 并释放 target。`defragCycle(options)` 受 move bytes、object count 和 time budget 约束；pin rejection、budget skip、failed move 和 reclaimed pages 进入 stats。

## DB native layouts

DB graph 保存完整 paired handles：

- `ENTRY_RECORD` 为 72 bytes；key/value handle 各 16 bytes，其后是 hash、type、encoding、flags、deadline、version 和 LRU/LFU。
- collection root record 为 16 bytes，保存 root 自己的完整 handle identity。
- `EntryRecord.version` 是递增 mutation version，用于 prepared/stale candidate 校验，不是 accounting estimate。
- key、entry、string、collection root/node/payload 使用各自 `NativeObjectKind`。

Java adapter/topology 可以位于 heap，但必须单独计量，并由唯一 owner 释放。ZSet borrowed member index 等结构只借用 canonical member handle，不能重复 free payload。realloc/defrag traversal 只移动 backend-owned native objects，不改变 adapter 对 stable handles 的引用。

## Mutation 与 accounting

增长型 mutation 先 reserve upper bound，再打开 allocation scope 并 prepare。prepare 后用 scope 实测 `NativeAllocationGrowth.effectiveBytes()` 加 staged non-native growth reconcile reservation。成功顺序是 commit、scope promote、logical ledger settle、release superseded、optional trim。

`NativeAllocatorStats` 区分：

- logical used bytes；
- reserved block capacity；
- committed page/region bytes；
- internal/external fragmentation；
- live/pinned/quarantined objects；
- realloc/defrag counters；
- object-kind counts 与 allocation histogram。

runtime/allocator usage 是 DB maxmemory snapshot 的组成部分，不替代 ingress 或 outbound reply 的独立容量账户。详细生产排查边界见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
