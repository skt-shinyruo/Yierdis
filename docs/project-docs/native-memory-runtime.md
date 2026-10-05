# Native Memory 运行时

Yierdis 把 JDK 25 FFM 接入 DB，涉及 backend、runtime、region、stable handle、maxmemory，以及必须 materialize 到 heap 的边界。本文系统阐述**为什么选用堆外存储**、**堆外内存管理与 C Redis (jemalloc) 的对照**，以及**运行时核心对象的拓扑与生命周期**。FFM API 本身的基础语法见本文 §三（Region 与 FFM 原语访问机制），底层分配器与句柄实现见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## 一、为什么选用堆外存储：JVM 约束与工程推导

用 Java 实现类似 Redis 的高性能内存数据库，JVM 在网络 I/O 与 JIT 编译执行上并不拖累吞吐（热路径性能接近 C，NIO/epoll 无瓶颈，虚拟线程亦能简化并发）。真正的问题集中在 JVM 堆内存模型本身：

### 1. 四大致命内存痛点

1. **GC 停顿 → 尾延迟毛刺（最致命）**：
   Redis 核心指标是亚毫秒级 P99。C 版 Redis 无 GC 停顿（主要停顿仅来自 fork RDB）；而在 JVM 中，即使是 G1，大堆 Full GC 也会带来数百毫秒 STW，常规 Young GC 也有几十毫秒。若将数据全放在堆内，GC 扫描成本随数据量线性膨胀。只有采用 **小堆 + 大堆外** 并结合低延迟收集器（ZGC / Shenandoah），才能保证数据量增加时不扩大 GC 扫描面，将停顿压制在毫秒甚至亚毫秒内。
2. **对象头开销 → 内存严重膨胀**：
   每个 Java 对象带有 12~16 字节对象头（取决于压缩指针开启与否）加 8 字节对齐填充。一个普通的 `Entry + String key + byte[] value` 小 KV，堆内实际占用常是裸数据的 3~5 倍。C 版 Redis 使用 listpack、intset 等紧凑编码把内存利用率推到极致，堆内对象模型天然无法与之竞争。
3. **堆内存不可精确计量 → 淘汰与配额难以实施**：
   `maxmemory` 与 LRU/LFU 驱逐依赖精确的字节级物理记账。对复杂的堆内对象图做精确记账几乎不可行，且堆内数据越多，GC 扫描代价越沉重。
4. **大堆惩罚（CompressedOops 失效）**：
   当堆大小超过 32GB 时，JVM 将丢失指针压缩（CompressedOops），引用指针从 4 字节膨胀到 8 字节，进一步恶化内存放大问题。

### 2. 堆外存储方案与工程对策

针对上述痛点，Yierdis 确立了**“小堆（元数据/协议） + FFM 堆外（数据内核）”**的架构路线：

| 约束与风险 | 核心工程对策 | 对应实现 |
|---|---|---|
| **消除 GC 停顿** | 将所有键值记录移入 FFM 堆外；堆内仅留短生命周期对象与协议缓冲区 | 本文 §三, 本文 |
| **消除对象头开销** | 采用紧凑连续字节布局（如 72 字节紧凑 `ENTRY_RECORD`）与基于页面的 Slab 分配器 | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| **精确物理限额** | 堆外物理页自主记账，精确维护 Committed / Live / Reclaimable 字节 | [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) |
| **确定析构防泄漏** | 摒弃不确定的 `DirectByteBuffer` Cleaner，基于 FFM `Arena.close()` 与 Stable Handle 显式引用计数 | 本文 |

---

## 二、堆外内存管理职责分层与 C Redis 对照

选定堆外存储后，内存管理各层能力不再由操作系统或成熟库（如 jemalloc）免费提供，需要由 Yierdis 显式自建：

```text
层级职责        C Redis（jemalloc 生态）               Yierdis 堆外（FFM 生态）
策略层          maxmemory 淘汰 · TTL 主动过期           自记账 maxmemory · 采样淘汰 · 预算轮询
生命周期层      robj 引用计数 · lazy free 后台线程       NativeHandle 引用计数 · Arena Scope · 确定析构
分配器层        jemalloc (size class + arena + tcache)  自建 Slab/SizeClass on Arena（核心分水岭）
OS/底层         mmap · fork COW · MADV                  fork不可用 · 手动页回收 (madvise) · 内存映射
```

### 1. 分配与释放的核心设计要点
- **分配元数据放堆内**：Free list 等轻量元数据存放在堆内 `long[]`，便于保留 `jmap` 与 Heap Dump 的可观测性。
- **Stable Handle 替代裸物理指针**：上层绝不持有 64 位物理内存地址，统一分发 `NativeHandle`（`id = 索引 + 代次`）。代次机制彻底防止悬垂引用与 Java 版 ABA 问题；同时使得内存重分配（Realloc）和内存碎片整理（Defrag）只用改句柄映射表，无需上层业务图感知。
- **单 Owner 线程模型消灭竞争**：DB 存储引擎严格绑定单个 Owner 线程运行，天然消除了多线程分配竞争，无需复杂沉重的 Thread Cache (tcache) 层。

### 2. 内存碎片三层防御治理
1. **变长转 Size Class 化**：将变长字节规整到标准规格档位（Small Page Slab），以可控的内部碎片（~10-15%）杜绝不可控的外部碎片。
2. **间接句柄支持零停顿搬迁（Active Defrag）**：碎片整理只需在后台复制内存块并更新句柄表中的物理地址映射，上层 Handle 引用完全不变。
3. **空闲页主动归还 OS**：后台定期扫描全空的小页，通过 `trimEmptyPages` 释放内存，保证系统物理 RSS 真实回落。

---

## 三、当前运行时架构与核心对象

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

## Region 与 FFM 原语访问机制

### 1. FFM 核心原语在 Yierdis 中的角色

FFM（`java.lang.foreign`）在 Yierdis 中仅用于**堆外内存管理**（不调用 C 函数，不使用 `Linker` / `SymbolLookup`），集中在 `YierdisFfmMemoryRuntime` 与 `YierdisFfmRegion` 两个核心类中：

| FFM 原语 | 在 Yierdis 中的角色与约束 |
|---|---|
| `Arena` | 生命周期作用域。Yierdis 统一采用 `Arena.ofShared()`（因为 Region 可能在启动线程创建、而在 DB Owner 线程或优雅停机线程关闭，跨线程关闭要求 shared scope；单线程约束由项目自身的 `DbThreadGuard` 保证）。分配失败抛 `OOM` 时显式 `arena.close()` 避免堆外泄漏。 |
| `MemorySegment` | 有界物理内存切片视图。`ensureOpen()`（结合 `arena.scope().isAlive()`）做双重生命周期防护。 |
| `ValueLayout` | 单个字段读写布局。统一使用 `*_UNALIGNED`（`JAVA_INT_UNALIGNED` / `JAVA_LONG_UNALIGNED`），因为内存记录中的 32 位/64 位字段紧凑排列，不保证落在 8 字节自然对齐边界上。 |
| `MemorySegment.copy(...)` | 内存块搬运的唯一正规途径：支持 region ↔ heap array、region ↔ region（Allocator 扩容与 Defrag 搬迁）。 |

### 2. 为什么采用手写固定 Offset 而不是 StructLayout

`EntryTable` 将键元数据紧凑压缩为 72 字节的 `ENTRY_RECORD`（9 个 8 字节槽位）：

```text
0   key handle allocatorId (8B)
8   key handle localRaw (8B)
16  value handle allocatorId (8B)
24  value handle localRaw (8B)
32  key hash (4B)
36  type (4B)
40  encoding (4B)
44  flags (4B)
48  expireAtMillis (8B, 唯一 TTL deadline)
56  version (8B)
64  lruOrLfu (8B)
```

Yierdis 放弃了 FFM 的 `StructLayout` + `VarHandle`，选择直接手写常量 offset 并调用 `getIntLittleEndian` / `getLongLittleEndian`：
- **收益**：字段固定且极简，直接 offset 计算消除了复杂布局对象的构造与动态校验开销，JIT 极易内联为直接 CPU load/store。

`YierdisFfmRegion` 保存 runtime、arena、segment、size。page allocator 把 region 切成块：`requestedBytes <= YierdisNativeSizeClass.MAX_SMALL_BYTES` 走 small page（`PAGE_BYTES = 64 KiB` 一页切成固定 size class 块），否则走 span（`pagesFor(bytes)` 页，`<= MEDIUM_MAX_BYTES = 1 MiB` 记为 `MEDIUM_SPAN`，更大为 `LARGE_SPAN`）。空 small page 可以被 `trimEmptyPages` 回收成一个 `MemoryReclaimResult`。

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
- **quarantine**：已逻辑 free 但 `pinCount > 0` 或仍有 epoch 挡住复用的对象进入 `STATE_FREED_QUARANTINED`，并记入 quarantine 槽位集合。`reclaimEligibleFreedObjects()` 只遍历这个集合，仍要等 `pinCount == 0` 且 `canReclaim(freeEpoch)` 才真正释放。最后一个 pin 释放（`unpinLocal`）或 scope 关闭时都会触发这次回收。
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

native committed/reserved usage 参与 DB 和 maxmemory 诊断，但不会替代 ingress 或 outbound reply 的独立容量限制。native allocation failure 仍按 mutation 的 commit 前/后边界决定返回 OOM 或 result-unknown；操作流程见 [`configuration-and-operations.md`](./configuration-and-operations.md#生产环境加固与验收操作)。