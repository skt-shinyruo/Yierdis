# Stable Memory Backend 与 Handles

本文覆盖 stable handle、object table、page allocation、pin/epoch/quarantine、realloc 和 active defrag。FFM runtime/region ownership 见 [`native-memory-runtime.md`](./native-memory-runtime.md)；这层对外的最终统计口径见 [`production-hardening-operations.md`](./production-hardening-operations.md)。

建议按"对象是什么 → 谁调用谁 → 不这样做会出什么问题"的顺序读：§1–§2 是合同，§3–§4 是对象结构，§5–§9 每一节都对应一类真实的失效场景（use-after-free、地址漂移、回滚不彻底、ABA、搬迁收益为零）。

## 1. 这一层为什么存在

DB graph 里存的是 key 字节、field、member、listpack 块这类数据，生命周期跨越无数次 mutation。如果直接把 FFM `MemorySegment` 当引用存进 graph，会同时撞上三个问题：

1. **地址不能长期持有**：`reallocate` 换块、defrag 搬迁、page 被复用之后，旧地址会指向别人的数据。
2. **释放不能立即生效**：读路径可能正处于一次有界 resolve 中（`NativeObjectView`、cursor scan、上层 callback），立刻 `free` 就是 use-after-free。
3. **失败不能就地回滚**：一次 mutation 可能已经分配了十几个对象才失败，需要一个物理层面的"撤销到某一点"能力，而不是只把账本改回去。

生产实现 `YierdisFfmStableMemoryBackend` 因此切成三层，每层只解决一个问题：

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

调用方只在有界操作内 resolve handle，用完即 close 短生命周期的 `NativeObjectView`。physical page、offset、capacity 和 segment 都是 backend 私有状态。

**为什么不把 location 规则交给各 DB 自己维护**：那样每个 DB 都要复制一遍搬迁/失效逻辑，且没法给 defrag 提供统一安全网。把规则收进 object table 的 generation 字段，代价是所有位置变化都要多一次 table 写入——换来的是"删/搬/复用"的语义只有一处。

## 2. 公共 handle 与私有 localRaw

公共 API 是：

```java
public record NativeHandle(long allocatorId, long localRaw)
```

- `allocatorId` 标识创建 handle 的 backend，由 `StableMemoryBackendIds` 进程内单调发放、永不复用。
- `localRaw` 是只允许所属 backend 解释的不透明值。
- 只有两个分量都为 `0` 时才是 `NativeHandle.NULL`。
- `realloc` 和 defrag 不改变两个分量；`free` 之后该 identity 失效（generation 递增，见 §3）。

FFM backend 私有的 `YierdisLocalHandleCodec` 才把 `localRaw` 编成 64 bit：

```text
bits 63..60  domain       4 bits
bits 59..56  kind         4 bits
bits 55..16  slotId      40 bits
bits 15..4   generation  12 bits
bits 3..0    flags        4 bits
```

对应的位移常量是 `DOMAIN_SHIFT = 60`、`KIND_SHIFT = 56`、`SLOT_SHIFT = 16`、`GENERATION_SHIFT = 4`，掩码为 `SLOT_MASK = (1L << 40) - 1L`、`GENERATION_MASK = (1L << 12) - 1L`、`FOUR_BIT_MASK = 0x0fL`。

为什么这样切：

- `slotId` 是全局槽位索引，40 位可覆盖约 1.1×10¹² 个槽，位宽不是本层的实际瓶颈；
- `generation` 与 `kind`/`domain` 放高位，是为了让"跨 backend、错 kind、错 domain、未知 slot、generation 不匹配"这五类错误的判定可以纯位运算完成；
- `flags` 4 位目前恒为 0（所有写入路径都传 0，且没有任何校验），保留位只在 `localHandleFor` 里原样回填——这是预留而非在用字段。

DB/API 调用方不得复制这套 codec，也不得把 `localRaw` 当作完整 handle。`EntryHandle`、`ValueHandle` 和 `KeyHandle` 必须保留 paired identity。

## 3. Object table：稳定身份的权威表

`YierdisNativeObjectTable` 是 `localRaw` 到当前 location metadata 的权威表。每个 FFM segment 含 `SLOTS_PER_SEGMENT = 4_096` 个槽，每槽 `META_BYTES = 36`：

| 偏移 | 常量 | 内容 | 宽度 |
|---|---|---|---|
| 0 | `PAGE_OFFSET_OFFSET` | 页内/span 内偏移 | 4 B |
| 4 | `SIZE_OFFSET` | logical size | 4 B |
| 8 | `PAGE_ID_OFFSET` | 所属 page id | 4 B |
| 12 | `PACKED_METADATA_OFFSET` | 打包字段（见下） | 4 B |
| 16 | `ALLOC_EPOCH_OFFSET` | 分配时的 epoch | 8 B |
| 24 | `FREE_EPOCH_OFFSET` | 释放时的 epoch | 8 B |
| 32 | `PIN_COUNT_OFFSET` | pin 计数 | 4 B |

**capacity 不在槽里**：它由 `capacityResolver.resolveCapacity(...)` 从 page/span descriptor 反查。理由是最小档对象只有 16 B，再塞 4 B capacity 是纯浪费，而 descriptor 本来就是权威值。

打包的 32-bit word（`PACKED_METADATA_OFFSET`）按位切分，只用了低 30 位：

```text
state      [2:0]    STATE_MASK = 0x07
pageClass  [5:3]    PAGE_CLASS_MASK = 0x07
flags      [9:6]    FLAGS_MASK = 0x0f
kind       [13:10]  KIND_MASK = 0x0f
domain     [17:14]  DOMAIN_MASK = 0x0f
generation [29:18]  GENERATION_MASK = 0x0fff
```

`pageClass` 与 generation 同时放进槽里而不是只放在 handle 里，是因为读取一个 handle 时 backend 必须能独立判定"这个槽现在是否还属于这个 handle"——如果只信 handle 自带的 generation，就无法发现 object table 已经把它复用掉了。

状态常量与语义：

| 常量 | 值 | 语义 |
|---|---|---|
| `STATE_FREE` | 0 | 槽空闲 |
| `STATE_ALLOCATED` | 1 | 正常存活 |
| `STATE_PINNED` | 2 | 存活且 pin 计数非零相关 |
| `STATE_MOVING` | 3 | 正在搬迁（`beginMove` 之后、`publishMoved` 之前） |
| `STATE_FREED_QUARANTINED` | 4 | 已 free 但被 pin/epoch 拖住，延迟归还 |
| `STATE_CORRUPT` | 5 | 定义但从未写入或匹配（见 §10） |

**generation 与 ABA 防范**：`INITIAL_GENERATION = 1`，槽位每次被释放时递增；涨到 `MAX_GENERATION = 0x0fff` 后该槽**永久 retire**，不再回 free stack。

- 为什么必须 retire：12 位 generation 会回绕，回绕后的旧 handle 会恰好命中复用该槽的新对象，这就是 ABA。retire 是把"无法再安全复用"这件事变成物理事实。
- 代价：单个槽位寿命约 4095 次复用（generation 从 1 递增到 0x0fff）。达到这个量级需要长期高频 churn，正常压测很难触发，但一旦触发，object table 会持续向新 segment 增长，只能靠进程重启回收。
- 段内的实现：`YierdisNativeObjectSegment` 用 `int[] freeStack = new int[SLOTS_PER_SEGMENT]` 保存空闲偏移，`freeCount` 计栈深，`long[] retiredBitmap = new long[RETIRED_WORDS]`（`4096 / Long.SIZE = 64` 个字）标记 retired 槽。`releaseOffset(offset)` 对 retired 槽直接抛 `IllegalStateException("retired slot cannot be released")`，栈满抛 `"segment free stack overflow"`。

**slot 分配策略**：分配时先扫现有 segments 的 free stack，不够再追加 segment；table 用一个按实际长度增长的 segment array 承载。`AUTOMATIC_MAX_SLOTS = Integer.MAX_VALUE` 是默认上限。

object table 承担的职责：

- 校验 allocator/domain/kind/slot/generation；
- resolve 当前 page location；
- pin/unpin 与 quarantined release；
- realloc/defrag 的 `beginMove`、`publishMoved`、`abortMove`；
- cursor 遍历（`firstOccupiedSlot` / `nextOccupiedSlot` / `occupiedMeta`）、统计与 scope rollback。

## 4. Pages、spans 与 size class

`YierdisNativePageAllocator` 用 `NavigableMap<Integer, PageAllocation> pagesById`（`TreeMap`）按 page id 保存 `SmallPage` 或 `SpanAllocation`，另有 `NavigableSet<Integer> reusablePageIds`（`TreeSet`）保存已脱离 registry 待复用的 id。没有第二套 native page directory。

page 固定 `PAGE_BYTES = 64 * 1024`。请求不超过 `MAX_SMALL_BYTES = B32768.bytes = 32_768` 时按单一 size class 进 small page，共 23 档（`YierdisNativeSizeClass` 的 `B16`…`B32768`）：

```text
16, 24, 32, 48, 64, 96, 128, 192,
256, 384, 512, 768, 1024, 1536, 2048,
3072, 4096, 6144, 8192, 12288, 16384,
24576, 32768
```

档位是 4/3 与 3/2 交替的几何级数：越小的对象档差越细（16→24 只差 8 B），这是为了让 tiny 对象的内部碎片可控。

**small page 内的 free-offset 栈**（`SmallPage`）：

- `int[] freeOffsets = new int[blockCount]`，`blockCount = PAGE_BYTES / sizeClass.bytes()`；
- 构造时把每个块偏移一次性压栈（`freeOffsets[freeCount++] = i * sizeClass.bytes()`）；
- 分配 = `freeOffsets[--freeCount]`，释放 = `pushFreeOffset(offset)`，另有 `liveBlocks` 计存活块数。

之所以能这么简单，是因为**同页同档位，所有块等长**——任意空槽都能装下任何请求。代价是内部碎片：一个 17 KB 的值落在 24576 档，每块浪费约 7 KB。替代方案是变长块 + 分裂/合并，那会让 free 变成 O(空闲块数) 且需要处理拼接；本层选择"固定档位"是有意的取舍。

**span**：请求超过 32 KB 时走连续 region allocation，页数由 `pagesFor(bytes)` 向上取整（`((long) bytes + PAGE_BYTES - 1) / PAGE_BYTES`）：

- `MEDIUM_SPAN`：不超过 `MEDIUM_MAX_BYTES = 1024 * 1024`（1 MiB）；
- `LARGE_SPAN`：超过 1 MiB。

span 没有页内 free slot 概念，`summarizePages()` 里它的 committed 与 used 都等于 `capacity`。

**warm page 保留规则**：一个 small page 被释放到 `liveBlocks == 0` 时不立刻关页，而是 `findWarmPage(sizeClass, page)` 找同档位的另一个空页：

- 没找到 → 保留，作为该档位的 warm page（每档位至多一个）；
- 找到 → 关掉其中一个。但 `createdInActiveScope(page) && !createdInActiveScope(warmPage)` 时**关新页**，代码注释写明原因是"abort 只回收 scope 新建页，临时 warm page 不能淘汰命令前的基线页"。

这条规则固定了两个不变量：每 size class 至多 1 个 warm empty page（`trimEmptyPages` 回收其余），以及 abort 之前就存在的基线页不会被 scope 内的临时对象挤掉。

**page id 与复用**：`claimPageId()` 先 `reusablePageIds.pollFirst()`，没有才递增 `nextPageId`；`nextPageId` 到 `Integer.MAX_VALUE` 后置 -1，再用尽抛 `NativeCapacityExceededException("native page id space exhausted")`。id 复用本身不承担 ABA 防护——代码注释写得很直白：*"复用集合只接收已脱离 registry 的 ID；旧句柄仍由 object-table generation 判为 stale"*。也就是说，**page id 复用是安全的，前提是 object table 的 generation 机制没有被绕过**。

**统计口径**（`summarizePages()`）：small page 记 `committed += PAGE_BYTES`、`used += liveBlocks * sizeClass.bytes()`、`smallFreeBytes += freeCount * sizeClass.bytes()`；span 记 `committed = used = capacity`，并把 `span.pageCount` 累加进 `liveMediumSpanPages` / `liveLargeSpanPages`。注意这两项是**页数**，span 描述符数在 `liveSpanDescriptors`（见 §10）。

block（`YierdisNativeBlock`）对外只暴露 backend 所需的 capacity、page identity/class 和 byte access。requested size、page count、size class 等 test-only 镜像不留在 block 中；真实信息由 registry descriptor 和 object table 决定。

## 5. 释放安全：pin / epoch / quarantine / retired

这一节回答的问题是：**一个 block 什么时候才能真正归还给操作系统/复用**。

三种保护粒度：

- **pin**：object 粒度。`pin(handle)` 增加槽位 `PIN_COUNT`；`resolve(handle, mode)` 返回的 `NativeObjectView` 自己持有一次 retain，close 时释放；`resolvePinned(...)` 只允许 read-only，借用调用方已持有的 pin。
- **epoch**：批量观察窗口。`beginEpoch()` 返回 `NativeEpochScope` 并登记进 `activeEpochScopes`。SCAN 的 discovery→replay 就是典型用户：整个窗口内允许观察旧位置。
- **quarantine / retired**：延迟归还的容器。被 free 但仍被 pin 或仍有活跃 epoch 可能观察旧位置的对象进入 `FREED_QUARANTINED`；搬迁后发布新位置的旧 block 进入 `retiredBlocks`。

free 路径的判定（`freeLocal`）：

```java
boolean delayRelease = meta.pinCount() > 0 || !canReclaim(freeEpoch);
objectTable.free(localRaw, freeEpoch, delayRelease);
if (delayRelease) {
    reclaimEligibleQuarantine();   // 顺带尝试回收别的可回收对象
    ...
    return;
}
releaseAllocation(meta);           // 立即归还 block 与 ledger
```

搬迁路径的判定（`reserveMovedBlock`）：`canReclaim(retiredEpoch)` 为真时直接 `block.close()`，只把容量差记入 `reservedBytes`；为假时整块容量记入 `reservedBytes` 并 append 到 `retiredBlocks`。

`canReclaim` 的完整语义：

```java
private boolean canReclaim(long retiredEpoch) {
    if (retiredEpoch <= 0L) {
        return activeEpochScopes.isEmpty();
    }
    // 后启动的 scope 不会引用退役前的位置，因此不应阻塞这次回收。
    return activeEpochScopes.stream().noneMatch(scope -> scope.epoch <= retiredEpoch);
}
```

为什么"后启动的 scope 不阻塞回收"是正确的：一个 scope 只能观察到它**开启时仍然存在**的位置。退役点之后才开的 scope 不可能拿到退役前的地址，所以不构成风险。

这条判断写错的两个方向都有具体后果：

- 写成 `scope.epoch >= retiredEpoch`（方向反了）→ 仍在读旧位置的 view 被回收 → use-after-free；
- 写成"任何 active scope 都阻塞"→ 一个长生命周期 epoch 会让所有回收永久卡住，`reservedBytes` 单调上涨，表现为"对象都删了内存不降"。

回收触发点：`reclaimEligibleQuarantine()` 内部依次 `reclaimEligibleFreedObjects()`（按 `meta.freeEpoch()` 判）与 `reclaimEligibleMovedBlocks()`；`defragCycle` 结束时也会调一次。最后一个 pin/epoch 关闭时同样会尝试回收。stable handle 仍能表示逻辑 identity，但 freed/quarantined handle 不能作为新的普通 resolve 入口。

## 6. Allocation scope：mutation 回滚的物理基础

`NativeAllocationScope` 是"一次 mutation 的物理事务边界"。`beginAllocationScope()` 会做三件事：

1. 取一次 `memoryUsage()` baseline，供 `growth()` 计算增量；
2. 在 page allocator 上打 `AllocationScopeCheckpoint`，记录 `creationSequence`、`nextPageId` 和 `reusablePageIds` 快照；
3. 在 object table 上打 checkpoint，记录 segment 数 baseline。

同一时刻只允许一个 scope：重复 `beginAllocationScope()` 抛 `IllegalStateException("native allocation scope is already active")`。

生命周期：

- allocation 自动加入 scope（`scope.track`）；
- explicit free 从 scope 移除（`untrackScopedHandle`）；
- `promote()` 把存活 handles 转交给 committed graph，并释放两处 checkpoint 引用；
- `abort()` 或未 promoted 的 close 逆序释放剩余 handles，然后依次 `pageAllocator.restoreAllocationScope(checkpoint)` → `objectTable.restoreAllocationScopeCheckpoint(checkpoint)`；
- terminal operations 幂等。

`restoreAllocationScope` 的动作顺序是有约束的，不能调换：

```java
// 1) 校验并关闭 scope 新建的页
for (PageAllocation allocation : pagesById.values()) {
    if (allocation.creationSequence < checkpoint.creationSequence) continue;
    if (!(allocation instanceof SmallPage page) || page.liveBlocks != 0) {
        throw new IllegalStateException("allocation scope left a live native page");
    }
    createdEmptyPages.add(page);
}
for (SmallPage page : createdEmptyPages) closeSmallPage(page);
// 2) 之后才能恢复 id 复用集合与计数器
reusablePageIds.clear();
reusablePageIds.addAll(checkpoint.reusablePageIds);
nextPageId = checkpoint.nextPageId;
nextCreationSequence = checkpoint.creationSequence;
```

代码注释给出了理由：*"scope 新页已全部消失，此时恢复快照才不会让可复用 ID 指向仍存活的描述符。"* 如果先恢复 id 集合再关页，`claimPageId` 可能在同一个 scope 内把仍被引用的 id 发出去，造成两个描述符共用 id，随后 `pagesById` 的所有者校验（`"page id is already live"` / `"page id owner mismatch"`）会开始随机报错。"allocation scope left a live native page" 则说明有对象漏 free——这是 abort 路径上最值得关注的一条失败信息。

**为什么 abort 必须做物理回滚**：新建页、新建 segment、`nextPageId`/`nextCreationSequence` 的推进都发生在 prepare 阶段。只回滚 ledger 会让物理状态永久偏移，`trimEmptyPages`、`stats()` 与后续 ID 复用都会漂移。

**`growth()` 保留峰值**（包括 transient growth），不是最终值：一次 prepare 可能先分配后释放（例如 packed→HT 升级），只有峰值才是 admission 需要覆盖的量。

**明确不承诺的事**：scope open 无复制、abort 无分配、其它 allocation-free 形状。验收只看 ownership、rollback 和保守 accounting。相关预估值由 `estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount)` 与 `estimateAdditionalGrowth(int... requestedBytes)` 提供。

## 7. Realloc

`reallocate(handle, newSize, policy)` 保持完整 `NativeHandle` 不变，`reallocateLocal` 有三条路径：

1. **pinned 直接拒绝**：`meta.pinCount() > 0` → `NativeMemoryException("native object is pinned")`。
2. **容量够，原位更新**：`newSize <= meta.capacity()` → 只调 `objectTable.updateLocation(...)`（pageId/pageOffset/pageClass 原样保留），`logicalUsedBytes` 加上差值，`reallocInPlaceCount++`。
3. **容量不足，搬迁**：`pageAllocator.moveSource(meta)` → 分配 target → `previous.copyTo(next, oldSize)` 保留旧 prefix → `objectTable.updateLocation(...)` 发布新位置 → `reserveMovedBlock(previous, next.capacity(), nextEpoch())` → `reallocMovedCount++`；若此时有 active scope，额外 `recordGrowth()`。

失败边界很清楚：

- 复制、metadata 校验或 publication 之前的任何失败，都会关闭未发布的 target 并让 handle 继续解析到旧 block——**旧数据必须完好**；
- publication 之后不再假装旧状态仍可回滚。

`NativeReallocPolicy` 当前只有 `PRESERVE_PREFIX` 一个值，实现也没有按 policy 分支（行为上仍是保留 prefix）。要加"不保留 prefix"或"归零"策略，分支点就在这里。

## 8. Active defrag

`defragCycle(NativeDefragOptions options)` 从 `objectTable.firstOccupiedSlot()` 起沿 `nextOccupiedSlot` 遍历：

- 跳过 `STATE_FREED_QUARANTINED`；
- 跳过既非 `STATE_ALLOCATED` 也非 `STATE_PINNED` 的槽（`STATE_MOVING` 因此不会被二次搬迁）；
- 对象的 `pinCount() > 0` 时计 `skippedPinnedObjects` 并跳过；
- 受 `maxObjects`、`timeBudgetNanos`、`maxMoveBytes` 三重预算约束，结束时调 `reclaimEligibleQuarantine()`。

预算检查顺序是 **object → time → pinned → byte**，`skippedBudgetObjects` 只在 byte 预算停止时自增（object/time 预算停止时为 0），这是统计口径问题而非行为问题。

单次搬迁（`moveLiveObject`）的完整序列：

```text
beginMove(handle)
  -> pageAllocator.moveSource(meta)      // 拿旧 block
  -> pageAllocator.allocate(...)         // 分配 target
  -> previous.copyTo(target, size)       // 复制 logical bytes
  -> objectTable.publishMoved(...)       // 发布新 location（此后不可回滚）
  -> reserveMovedBlock(previous, targetCapacity, nextEpoch())
  -> retire 旧 block（或立即 close）
```

handle、kind、logical size 和 DB graph identity 全程不变——**defrag 的安全性正建立在"file 身份与物理位置彻底分离"之上**：搬迁只改 object table 里的 pageId/pageOffset/capacity，DB graph 里的 `NativeHandle` 一个字节都没动。

安全性的第二半在 `reserveMovedBlock`：新位置发布后，旧 block 只有在 `canReclaim(nextEpoch())` 为真时才能立刻 close，否则必须留在 `retiredBlocks` 里等 epoch 关窗。这条链把"搬迁"和"并发观察"隔开了。

`publishMoved` 之前抛异常会执行 `abortMove()` 并 `close()` target，对象回到旧位置；统计上记 `failedMoves`。

**`defragReclaimedPages` 的口径**：`moveLiveObject` 里按 `retiredBytes / PAGE_BYTES` 累加（`retiredBytes = previous.capacity()`），统计的是**退役 block 覆盖的页数**，不是后端真正回收的页数——真正的回收发生在 quarantine/epoch 允许之后。另外 `trimEmptyPages(...)` 也把本次回收量累加进同一个 `defragReclaimedPages` 计数器，所以这个字段同时混入了两种来源。需要"真实回收页数"时不要用这个字段。

另一个已知边界：`defragCycle` **没有"搬迁是否有收益"的启发式**，对每个合格对象都会分配新 block 并复制。也就是说当内存已经紧凑时，defrag 仍然是纯粹的复制开销，只能靠 budget 限制规模。

## 9. 改动时必须守住的不变量

改这层任何代码前，先确认以下不变量没有被破坏：

1. **handle 的 `(allocatorId, localRaw)` 在 realloc/defrag 期间绝不改变**；只有 free 会通过 generation 递增使其失效。
2. **任何位置变化都必须经过 object table**。绕过它直接改 page/offset，会让 generation 校验失去意义，同时 page id 复用立刻变成 ABA 风险。
3. **槽位复用的前提是 generation 未耗尽**；retired 槽不可释放、不可复用。
4. **回收判定只能走 `canReclaim`**（pin 判定走 `pinCount()`），不要新增"直接 close"的旁路。
5. **abort 必须同时回滚 page allocator 与 object table**，且 `restoreAllocationScope` 里"先关页、后恢复 id 集合"的顺序不可调换。
6. **`capacity` 的唯一来源是 descriptor**；slot 里的 4 B capacity 是不存在的字段，不要"补上"。
7. **block 不携带 test-only 镜像**（requested size、page count 等）；需要时从 registry/table 取。

## 10. 已知命名与口径偏差

以下都在源码中确认过，不影响已文档化的不变量，但读 stats 时容易误判：

| 观察 | 位置 | 说明 |
|---|---|---|
| `freePages` 复用 `emptySmallPages` | `YierdisNativePageAllocator.stats()` | 两个字段填的是同一个值（构造 `YierdisNativePageAllocatorStats` 时把 `summary.emptySmallPages()` 传了两次） |
| `mediumFreeBytes` / `largeFreeBytes` 恒 0 | 同上 | span 没有页内空闲块，这两个字段硬编码为 `0L` |
| `liveMediumSpanPages` / `liveLargeSpanPages` 是**页数** | 同上 | 累加的是 `span.pageCount`；span 描述符数在 `liveSpanDescriptors` |
| `defragReclaimedPages` 名不符实 | `moveLiveObject` 与 `trimEmptyPages` | 一个记退役 block 覆盖页数，另一个记 trim 回收量，混在同一字段 |
| `skippedBudgetObjects` 偏窄 | `defragCycle` | 只在 byte 预算停止时自增 |
| `STATE_CORRUPT` 未使用 | `YierdisNativeObjectTable` | 定义但从未写入或匹配 |
| handle `flags` 恒 0 | `YierdisLocalHandleCodec` / object table | 会被解码并由 `localHandleFor` 回填，但所有写入路径都传 0，也没有校验 |
| `doubleFreeDetections` 语义偏宽 | `requireLiveMetaForFree` | 捕获 `StaleNativeHandleException` 即自增，实为"free 时命中 stale 句柄"，包含非 double-free 场景 |
| `NativeReallocPolicy` 单值 | `reallocateLocal` | 只有 `PRESERVE_PREFIX`，实现未按 policy 分支 |

## 11. 与 DB 层的交界

DB graph 保存完整 paired handles：

- `ENTRY_RECORD` 为 72 bytes；key/value handle 各 16 bytes，其后是 hash、type、encoding、flags、deadline、version 和 LRU/LFU。
- collection root record 为 16 bytes，保存 root 自己的完整 handle identity。
- `EntryRecord.version` 是递增 mutation version，用于 prepared/stale candidate 校验，不是 accounting estimate。
- key、entry、string、collection root/node/payload 各有自己的 `NativeObjectKind`；4 个 handle domain 为 `STORAGE_OBJECT`、`ENTRY_OBJECT`、`KEY_BYTES`、`TYPE_ROOT`。

Java adapter/topology 可以放在 heap，但必须单独计量，并由唯一 owner 释放。ZSet borrowed member index 等结构只借用 canonical member handle，不能重复 free payload。realloc/defrag traversal 只移动 backend-owned native objects，不改变 adapter 对 stable handles 的引用。

**mutation 与 accounting 的连接方式**：增长型 mutation 先 reserve upper bound，再 `beginAllocationScope()` 并 prepare；prepare 后用 scope 实测的 `NativeAllocationGrowth.effectiveBytes()` 加上 staged non-native growth 调 `ledger.reconcile(...)`。成功顺序是 commit、scope promote、logical ledger settle、release superseded、optional trim。也就是说 **§6 的 scope 是 ledger 之所以能"先预留、后收窄"的物理依据**——没有 scope 的实测峰值，reservation 只能一直按最坏上界挂着。

`NativeAllocatorStats` 的字段分组：logical used bytes、reserved block capacity、committed page/region bytes、internal/external fragmentation、live/pinned/quarantined objects、realloc/defrag counters、object-kind counts。runtime/allocator usage 是 DB maxmemory snapshot 的组成部分，不替代 ingress 或 outbound reply 的独立容量账户。