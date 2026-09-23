# Native Memory 运行时

Yierdis 把 JDK 25 FFM 接入 DB，涉及 backend、runtime、region、stable handle、maxmemory，以及必须 materialize 到 heap 的边界。本文讲这些对象谁拥有谁、生命周期怎么走、native 计数如何与 maxmemory 耦合。FFM API 本身的角色见 [`ffm-primer.md`](./ffm-primer.md)。

## 当前结构

生产路径只公开 `YierdisFfmStableMemoryBackend`（`StableMemoryBackend` 的唯一实现）。内部由这些对象组合：

| 对象 | 职责 |
|---|---|
| `YierdisFfmMemoryRuntime` | region accounting、`Arena` 创建、leak detection |
| `YierdisFfmRegion` | 拥有一个 shared arena 及其 `MemorySegment`，按 offset 做 byte/int/long/bulk 访问 |
| `YierdisNativePageAllocator` | 管理 small pages 与 medium/large spans，`PAGE_BYTES = 64 * 1024` |
| `YierdisNativeObjectTable` | 把 stable handle 解析为当前物理 location，每 segment `SLOTS_PER_SEGMENT = 4096` 个 slot、每 slot `META_BYTES = 36` |
| `YierdisFfmStableMemoryBackend` | 组合 allocate / resolve / pin / epoch / realloc / defrag / trim / close |

backend 还持有 `allocatorId`（来自 `StableMemoryBackendIds.nextId()`）——它保证 `NativeHandle` 不会被误用到另一个 backend。

DB graph 保存 `NativeHandle` 或 typed wrapper，不保存 `MemorySegment`、physical address 或 packed location。region/block access 都是 backend 内部实现细节。typed wrapper 有 `EntryHandle`（entry record）、`ValueHandle`（value root）、`AllocatorKeyHandle`（key bytes），都只是 stable identity 加访问方法，不是长期有效的 segment view。

## Runtime 的创建、绑定与关闭

`YierdisFfmMemoryRuntime` 是 backend 内部的 region 账本，字段是 `name`、`usedBytes`（`AtomicLong`）、`liveRegionCount`（`AtomicLong`）、`closed`（volatile）。

创建 region：`allocateRegion(owner, bytes)`

1. 校验 `bytes > 0`、runtime 未关闭、`owner != null`；
2. `Arena.ofShared()` 创建 arena，再 `arena.allocate(bytes)`；
3. 分配抛 `OutOfMemoryError` 时关闭刚创建的 arena，再抛 `NativeCapacityExceededException`；
4. 成功则建 `YierdisFfmRegion`，并 `liveRegionCount++`、`usedBytes += bytes`。

为什么用 `Arena.ofShared()` 而不是 `ofConfined()`：region 可能在 bootstrap 阶段（构造期、尚未绑定 owner 线程）分配，而释放发生在 DB owner thread，甚至 shutdown 时由另一个线程触发。confined arena 只允许创建线程 close，这里必须允许跨线程 close。这**不**表示 DB storage graph 可以跨线程并发访问——那种并发由 `DbThreadGuard` 在别处拒绝。

绑定线程：backend 构造时拿到一个 `MemoryOwner`（生产路径是 `DbThreadGuard`）。`YierdisDb.bindToCurrentThread()` → `backend.bindToCurrentThread()` → `owner.bindToCurrentThread()`。`MemoryOwner` 的约束是：普通访问必须来自已绑定的 owner thread；未绑定的实例允许关闭，绑定后拒绝跨线程关闭。所以 `Arena.ofShared` 的跨线程能力只用于关闭路径。

runtime 关闭（`close()`）：

- 置 `closed = true`；
- 检查 `liveRegionCount`，非 0 说明有 region 未释放，抛 `IllegalStateException("native memory leak in " + name + ": " + N + " live regions")`。

runtime **不替调用方释放仍然存活的 region**：它只做 leak 报告。region 释放是 `YierdisFfmRegion.close()` 的职责（见下）。

region 关闭（`YierdisFfmRegion.close()`）：

- 幂等（`closed` 保护）；
- `arena.close()`；
- `runtime.onRegionClosed(size)` 扣减 `liveRegionCount` / `usedBytes`，计数下溢抛 `IllegalStateException`；
- 两步各自的异常都会保留，第二次作为 suppressed 附加。

`onRegionClosed` 用 `AtomicLong` 是因为 region 的创建与关闭可能在构造期/owner/shutdown 等不同阶段发生，但同一时刻的调用仍是串行的。

## 每个 DB 一个独立 backend 的含义

`YierdisInstance.create(...)` 用 `YierdisFfmStableMemoryBackend::new` 创建内部 `YierdisDbEngineFactory`。每次 `YierdisDbEngineFactory.create(config)`：

1. `new DbThreadGuard()` 作为该 DB 的 owner；
2. `backendFactory.create("db-" + config.dbIndex(), nativeSlotCapacity, owner)` → 一个独立 backend；
3. backend 构造自己的 `YierdisFfmMemoryRuntime`、`YierdisNativePageAllocator`、`YierdisNativeObjectTable`；
4. `YierdisDb.create(config, backend, owner, hashSeed)`。

```text
YierdisInstance
  -> YierdisDbEngineFactory
     -> StableMemoryBackendFactory.create("db-N", nativeSlotCapacity, DbThreadGuard)
        -> YierdisFfmStableMemoryBackend
           -> YierdisFfmMemoryRuntime
              -> YierdisNativePageAllocator / YierdisNativeObjectTable
```

"每个 DB 独立 backend"意味着：DB 之间**不共享** region、page、object table 或 runtime 计数，也没有共享的 native 内存池。每个 DB 的 `usedBytes`、`liveRegionCount`、allocator stats 只描述它自己，这是 governor 能直接相加各 DB snapshot 的前提。

所有权链：`YierdisDb` 构造一开始就把 backend 所有权转交 `YierdisDbStorage.create(...)`（`backendOwnershipTransferred = true`）。storage graph（`EntryTable` / `NativeKeyDirectory` / 各 `*Root`）组合失败与正常 shutdown 都走同一释放顺序——`YierdisDbKeyLifecycle.OwnedResources.close()` 先 `clearData()`，再依次 close entryTable、keyDirectory、string/list/hash/set/zset roots，**最后**才 close backend。任何一步的异常都累积成 suppressed exception，不会中断后续清理。

global / per-db maxmemory scope 只改变预算协调方式，不改变 FFM 所有权：

- per-db scope 由当前 DB 的 ledger 和 physical snapshot 做 admission、cleanup、trim 和 eviction（预算由实例级 `maxmemoryBytes` 在各 DB 间均分，见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)）；
- global scope 由 instance governor 汇总多个 DB participant 的独占 snapshots；
- global scope 仍保留 per-DB backend/runtime ownership，也不把 runtime counter 叠加进 participant snapshots。

## Region 与访问

`YierdisFfmRegion` 保存 runtime、arena、segment、size。所有访问先 `ensureOpen()`（`closed` 标志 + `arena.scope().isAlive()`），再 `checkRange(offset, length)`，然后访问 segment：

- `getByte` / `setByte` 用 `ValueLayout.JAVA_BYTE`；
- `getInt` / `setInt` 用 `ValueLayout.JAVA_INT_UNALIGNED`；
- `getLong` / `setLong` 用 `ValueLayout.JAVA_LONG_UNALIGNED`；
- `getBytes` / `setBytes` / `copyTo` 用 `MemorySegment.copy(...)`。

用 `*_UNALIGNED` 是因为 record 布局不保证每个 int/long 落在自然对齐边界上（`ENTRY_RECORD` 里 32 位字段紧跟在 long 之后）。

page allocator 把 region 切成块：`requestedBytes <= YierdisNativeSizeClass.MAX_SMALL_BYTES` 走 small page（`PAGE_BYTES` 一页切成固定 size class 块），否则走 span（`pagesFor(bytes)` 页，`<= MEDIUM_MAX_BYTES = 1 MiB` 记为 `MEDIUM_SPAN`，更大为 `LARGE_SPAN`）。空 small page 可以被 `trimEmptyPages` 回收成一个 `MemoryReclaimResult`。

## Stable handle 与物理块

stable identity 来自 object table，并非 FFM address：

```text
DB graph
  stores stable NativeHandle values

YierdisFfmStableMemoryBackend
  resolves handle through YierdisNativeObjectTable

YierdisNativePageAllocator
  owns FFM blocks allocated from runtime regions
```

`NativeHandle = (allocatorId, localRaw)`。`localRaw` 由 `YierdisLocalHandleCodec.encode(domain, kind, slotId, generation, flags)` 打包；`requireOwned` 校验 `allocatorId` 匹配、`localRaw` 合法。因此 realloc 和 active defrag 可以分配新 block、复制内容并发布新 location，同时保持 handle 不变：

- `reallocate(handle, newSize, policy)`：`newSize <= capacity` 时原地改 size（`reallocInPlaceCount++`）；否则分配新 block、`previous.copyTo(next, oldSize)`、`objectTable.updateLocation(...)`、把旧 block 按 epoch 退役（`reallocMovedCount++`）。pinned 对象拒绝 realloc。
- `defragCycle(options)`：遍历 occupied slot，跳过 pinned / quota 状态，按 `maxObjects` / `timeBudgetNanos` / `maxMoveBytes` 预算移动 live 对象，`moveLiveObject` 复制到 target block 后 `objectTable.publishMoved(...)` 发布新 location。
- 复制在 publication 前失败，就 `objectTable.abortMove` / 关闭新 block，旧 location 继续有效。
- publication 后，旧 block 按 epoch 状态立即释放或进入 retired list（`reserveMovedBlock`）。

`NativeObjectView`（唯一实现是 backend 内部的 `StableObjectView`）提供 byte/bulk/copy/comparison/typed access。它委托默认实现前仍会完整检查 lifecycle、writability 和范围，因此无效的 multi-byte/copy 写入不会留下部分修改，read-only/closed 异常优先级也保持稳定。block-to-block realloc/defrag 使用一次直接 native copy。

## pin / view / epoch / quarantine / allocation scope

这五者职责不同，容易混为一谈：

- **pin / view**：`resolve` 会 pin 住对象，view 持有该 pin 直到 `close()`；`resolvePinned` 接受一个已存在的 pin（只读）。作用：防止仍被观察的 object 过早释放。
- **epoch（`NativeEpochScope`）**：`beginEpoch()` 记录一个 epoch；当某个 scope 可能看到过旧位置时，退役 block 不能立即释放。`canReclaim(retiredEpoch)` 只在没有任何仍活动的 scope 的 epoch ≤ retiredEpoch 时才允许回收——更晚启动的 scope 不可能引用退役前的位置，所以不阻塞回收。
- **quarantine**：已逻辑 free 但 `pinCount > 0` 的对象进入 `STATE_FREED_QUARANTINED`，等最后一个 pin 释放（`unpinLocal`）或 scope 关闭时由 `reclaimEligibleQuarantine()` 真正释放。
- **allocation scope（`NativeAllocationScope`）**：`beginAllocationScope()` 记录 baseline 与 table/page checkpoint；prepare 阶段新分配的 handle 被 `track`；commit 后 `promote()` 提交并清空；abort 时反向 `freeLocal` 每个 handle 并 `restoreAllocationScope` 回收 scope 内新建的空 page。它同时记录 `growth()`（heap / nativeMetadata / nativeData 的峰值增量），供执行器两阶段预算收窄 reservation。

epoch 与 defrag 的关系：defrag 移动对象后，旧 block 进入 retired list，随后 `nextEpoch()` 标记；只有所有相关 epoch scope 关闭后 `reclaimEligibleMovedBlocks` 才真正 close 旧 block 并从 `reservedBytes` 扣减。

## DB storage graph

FFM-backed storage 主要包括：

- `NativeKeyDirectory`：key 到 `EntryHandle`；key bytes 是 `KEY_BYTES` object；
- `EntryTable`：`EntryHandle` 到固定布局的 `ENTRY_RECORD`（`NativeStorageLayout.ENTRY_RECORD_BYTES = 72`）；
- `EntryRecord.expireAtMillis`：唯一 TTL deadline（位于 offset 48）；
- string、list、hash、set、zset 的 root、node 和 payload objects（`NativeObjectKind` 中 `STRING_BYTES` / `LISTPACK_BYTES` / `HASH_FIELD_BYTES` / `SET_MEMBER_BYTES` / `ZSET_MEMBER_BYTES` / `SCORE_BYTES` / `*_ROOT` / `LIST_NODE` / `*_TABLE` / `ZSET_NODE` 等）；
- object table 与 page allocator 的 native metadata。

`EntryHandle`、`ValueHandle` 和 `KeyHandle` 是 stable-handle wrapper，不等于 physical address，也不能当作长期有效的 segment view。`YierdisDbKeyLifecycle` 发布、替换和释放 directory entry、entry record、value root 与 derived accounting（`expireCount`、expires 索引）。

## Maxmemory 与 memory stats 的耦合点

写路径由 `YierdisDbMutationExecutor` reserve upper bound，再用 allocation scope 实测 prepare peak。commit 后依次 promote、settle logical ledger、release superseded resources，再按提示尝试 trim。

enforcement snapshot 固定为：

```text
heap estimated
  + native metadata committed
  + native data committed
```

耦合点要分清层次：

- `YierdisFfmStableMemoryBackend.memoryUsage()` 产出上面的 snapshot：`heapEstimatedBytes`（object table + page allocator + retired block list + active allocation scope 的堆估算）、`nativeMetadataCommittedBytes`（object table 的 segment metadata region）、`nativeDataCommittedBytes`（page allocator 的 committed pages）、`nativeDataLiveBytes`（page 实际用掉的字节）、`nativeReclaimableBytes`（空 small page 数 × `PAGE_BYTES`）。
- `YierdisDbMemoryReporter` 把 backend snapshot 与 `componentRetainedHeapBytes`（keyDirectory/list/hash/set/zset 的堆结构）合并成 owned snapshot，`usedBytesForMaxmemory()` 就是它的 `effectiveBytesForMaxmemory()`。
- runtime `usedBytes` / `liveRegionCount` 用于 FFM lifecycle 诊断（`INFO memory` 的 `yierdis_native_live_regions`），**不是**额外 maxmemory 账本。
- `nativeReclaimableBytes` 也只是候选量；只有 `trimEmptyPages` 返回实际 reclaimed bytes 并**重新采样**后，才能影响 admission 判断。

## Heap materialization 边界

native memory 不等于所有路径零复制。当前仍会 materialize 到 heap 的常见边界包括：

- RESP decode 把 argv materialize 成 heap `byte[]`；
- key lookup 把 `BytesView` 转为 owned key bytes（`YierdisDb.toByteArray`）；
- snapshot、`RANDOMKEY`、introspection 和显式返回 `byte[]` / `List<byte[]>` 的 API；
- 排序、聚合或协议组装需要脱离 native view 生命周期时复制；
- `LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE*` 在 prepare 时用 `ByteSequenceSources/ByteMapSources.copiedFrom(...)` 把选中元素拷进独立 source。

`SCAN`、命令 `GET`/`HGET` 和 pop 会持有 pin/epoch/handle，通过 native-backed `BytesSlice`（如 `NativeBytesSlice`）或等价的 retained view 有界写出；这些路径不会先 materialize 整批 payload。`GET` 在 `StringRoot.retainedValue` 时 `allocator.pin`，`CommandExecutorExecutionSupport` 同步渲染结束后于 `finally` 调用 `closePrepared`，由 `ByteValue` 的 close hook `unpin`。调用方不能让 callback-scoped view 逃逸。详细边界见 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 和 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## Operations Cross-Check

native committed/reserved usage 参与 DB 和 maxmemory 诊断，但不会替代 ingress 或 outbound reply 的独立容量限制。native allocation failure 仍按 mutation 的 commit 前/后边界决定返回 OOM 或 result-unknown；操作流程见 [`production-hardening-operations.md`](./production-hardening-operations.md)。