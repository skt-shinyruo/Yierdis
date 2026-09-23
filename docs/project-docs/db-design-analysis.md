# DB 设计分析

Yierdis 的 DB 层**为什么这样设计**，要从分层意图、层与层之间的契约、与 Redis C 实现的对照、刻意偏离，以及设计与取舍中值得质疑的地方说起。

与其它文档的分工：

- [`db-internals.md`](./db-internals.md)：DB 内部结构的**机制与组合参考**（对象是什么、谁调用谁）。
- [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)：各专题的完整机制。
- [`db-behavior-gaps.md`](./db-behavior-gaps.md)：运行期行为缺口、可疑观察项与运维注意事项。

## 一句话概括

Yierdis 的 DB 不是 `Map<byte[], Object>`，而是一个**受单一 owner thread 约束的 native 内存所有权内核**：

- 所有跨层引用都是 `(allocatorId, localRaw)` 的**稳定句柄**而非地址；realloc/defrag 只改物理位置，句柄身份不变。
- **一切变更**（含 lazy expire、active expire、eviction、flush、rehash 扩容）都必须走同一个 `execute(MutationPlan)`，先过内存账本再改 graph。
- 失败以 **commit 开始**为界显式收敛：commit 前 abort + 回滚，commit 后 degrade + result-unknown，不静默修正。

## 分层结构

```text
YierdisInstance (server/runtime, 多 DB + 可选全局 governor)
  └─ YierdisDb  ── 组合根：kernel / ledger / lifecycle / ops / maintenance
       ├─ YierdisDbKernel        唯一的 mutation 入口 + owner 检查
       ├─ YierdisDbKeyLifecycle  key/entry/value 的所有权记录
       └─ YierdisDbStorage       storage graph 的单一 ownership
            ├─ EntryTable         native ENTRY_RECORD 表
            ├─ NativeKeyDirectory keyspace 主索引
            └─ *Root (String/List/Hash/Set/ZSet)  type root 注册表
                 └─ StableMemoryBackend (FFM)  size class / page / handle
```

对外只有 `DbEngine`：8 个 typed ops（`StringOps`/`HashOps`/`ListOps`/`SetOps`/`ZSetOps`/`HllOps`/`KeyspaceOps`/`TtlOps`）加 `memoryUsage`、`memoryStats`、`objectEncoding`、`flushDb`、`health`。`RuntimeDbEngine` 追加 `bindToCurrentThread`、`runMaintenance`、`reconcileAccounting`、`shutdown`。

关键边界：**command 层只依赖 `DbEngine`；ops 拿不到 backend/table/directory/roots**，所有 graph 变更只能经 kernel 的 mutation 工厂构造。handle、`EntryRecord`、backend、ledger 都不进入公开接口。

## L0 native 分配器：稳定句柄 + 页式分配

生产实现是 `YierdisFfmStableMemoryBackend`（`StableMemoryBackend` 的其它实现只在 test 源码中）。完整机制见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

**句柄身份**

- `NativeHandle(long allocatorId, long localRaw)`；只有两段都为 `0` 才是 `NULL`。`allocatorId` 由 `StableMemoryBackendIds` 进程内单调发放、永不复用。
- FFM 私有的 `localRaw` 编码：`domain[63:60] | kind[59:56] | slotId[55:16] | generation[15:4] | flags[3:0]`。
- **realloc / defrag 不改变句柄身份**（只改 slot 内 location）；free 使 generation+1，旧句柄随之 stale；12-bit generation 耗尽后 slot **永久 retire**，不再回 free stack，杜绝 ABA。

**分配**

- **23 个 size class**：`16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768`，最小 16 B、最大 32 KB。
- **页**固定 64 KiB。请求 ≤32 KB 走单一 size-class small page（页内 free-offset 栈 + `liveBlocks`）；更大走 span：≤1 MiB 为 `MEDIUM_SPAN`，否则 `LARGE_SPAN`。每个 size class 至多保留 1 个 warm empty page，其余由 `trimEmptyPages` 回收。
- **object table** 每槽 36 B 元数据，generation/domain/kind/flags/pageClass/state **打包进一个 32-bit word**；`capacity` **不存槽位**，由 page/span descriptor 反查，避免为每个 tiny object 重复 4 B。每 segment 4096 槽。

**事务与回收基础**

- `beginAllocationScope()` 同时给 page allocator 与 object table 打 checkpoint；`abort()` 按逆序 free 已跟踪句柄、关闭 scope 新建的空页、截断新建 segment、回滚 page id 复用集合与 sequence。这是 mutation 失败可回滚的物理基础。
- **pin + epoch + quarantine**：被 free 但仍被 pin、或仍有活跃 epoch scope 可能观察旧位置的对象进入 `FREED_QUARANTINED`；搬迁后的旧 block 进入 retired list。`canReclaim(epoch)` 只让 epoch ≤ 退役点的 scope 阻塞回收，后启动的 scope 不阻塞。
- **defrag** 按 slotId 升序，跳过 pinned/quarantine，受 `maxObjects`/`maxMoveBytes`/`timeBudgetNanos` 三重预算约束；搬迁后经 retired-block + epoch 保证安全。

## L1 身份与存储图

- `EntryTable` 把每条记录编码成**固定 72 字节** `ENTRY_RECORD`：

  | 偏移 | 字段 | 宽度 |
  |---|---|---|
  | 0 / 16 | keyHandle / valueHandle 的 allocatorId+localRaw | 各 16 B |
  | 32 | keyHash | 4 B |
  | 36 / 40 / 44 | type / encoding / flags（ordinal） | 各 4 B |
  | 48 | expireAtMillis（-1 = 无 TTL） | 8 B |
  | 56 | version | 8 B |
  | 64 | lruOrLfu | 8 B |

  句柄在 DB 边界**显式保存两个 long**，不退化成 `localRaw`。

- **type root 是 16 字节自引用锚**（`COLLECTION_ROOT_RECORD_BYTES = 16`）：native 侧只存自己的句柄（用于 stale 校验与计账）。真正的聚合对象 `HashValue`/`ListValue`/`SetValue`/`ZSetValue` 是 **JVM 对象**，由 heap 上的 `HashMap<NativeHandle, AdapterSlot<T>>`（`NativeCollectionRootTable`）持有。
- 因此是**双份账**：payload 字节（key、field、member、listpack 块、quicklist node 记录）在 native；结构对象与哈希拓扑数组在 heap，两套都要估。
- 4 个 handle domain：`STORAGE_OBJECT`、`ENTRY_OBJECT`、`KEY_BYTES`、`TYPE_ROOT`。

> 容易误读的一点：`NativeKeyDirectory` **只把 key 字节存成 allocator-backed `KEY_BYTES`**；槽位数组（`byte[] states`、`int[] hashes`、`NativeHandle[] keyHandles`、`NativeHandle[] entryHandles`）在 heap，所以 `NativeKeyDirectory.nativeBytes()` 恒为 `0L`。"keyspace 在 native" 指的是 key 字节，不是 hash slot。

## L2 keyspace 索引：开放寻址 + 共享拓扑

`NativeKeyDirectory` 与 `NativeByteMap`（HASH/SET 的 HT 编码）**都不自己实现哈希逻辑**，委托同一个 `OpenAddressingTopology`。

- 4 种槽状态：`EMPTY`、`FILLED`、`TOMBSTONE`、`MIGRATED_SCAN_SHADOW`。**shadow 是为 SCAN 正确性专设的**：迁移后 old 表保留 key 用于定位与重放。
- 容量策略（`HashCapacityPolicy`）：`filled > capacity - capacity/4` → `GROW`；`tombstones > max(size, capacity/8)` → `COMPACT`；`capacity > 16 && size < capacity/8` → `SHRINK`。容量必须是 2 的幂，范围 `[16, 1<<30]`。
- **线性探测**；插入复用第一个 tombstone；remove 只置墓碑。
- **双表增量 rehash**：写路径每次顺带推进 `WRITE_REHASH_BUDGET = 2` 个 slot；维护节拍另有 64 slot 预算。若 rehash 期间插入会顶穿合并占用，目录 API 会**同步坍缩**成一张装得下全部存活 entry 的 standalone 表，显式防止 "no insertion slot"。
- 哈希是 **SipHash24，种子来自 `SecureRandom`**（`HashSeed`），每个 `YierdisDbEngineFactory` 一份。
- 删除有两条路：按 key 字节探测；或**按 entry 持有的 key 句柄 + dict hash 反查槽位**（`removeEntry`，O(probe)）。位置在删除时现查，所以双表状态下也不会指向 stale slot。
- **SCAN 游标**（`ScanCursorV2`）：`position[31:0] | phase[33:32] | generation[62:34]`（29 位 generation）。generation 不匹配或 phase 非法时从 active 表重启（允许重复，绝不因客户端乱填 cursor 抛错）。

## L3 key 生命周期与 staging

`YierdisDbKeyLifecycle` 是 directory + entry table + 各 root 的**唯一所有权记录**。

- 新 key 必须走 opaque `StagedEntry`：`EntryTable.reserve()` + `NativeKeyDirectory.stageInsert()`；abort/未发布时 `close()` 幂等释放两者；发布后 token 被消费。
- **`expireCount` 是派生计数**，随 publish/replace/release 对 `expireAtMillis >= 0` 的变化 ±1，不是独立索引；下溢会抛 `IllegalStateException`。
- **`ExpiresIndex`** 是 owner 线程独占的 `PriorityQueue`，只在 **deadline 真的变化**时写新项；touch / `KEEPTTL` 复用旧项；改 TTL / `PERSIST` 不主动清旧项，由消费方惰性判 stale。索引规模可超过存活 TTL key 数，且**不计入 ledger，也不计入物理 committed footprint**（见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)）。
- `version` 只在语义变化（新 record、TTL 或 flags 变化）时递增，**纯 access-clock touch 保留原 version**；`touchRecord` 在 LRU 下会写回新 record，且要求当前 record 与预期一致才落盘。

## L4 mutation 执行器与内存账本（最核心）

`YierdisDbKernel` 是 package-private 深模块：读路径在 owner 检查后直接执行，**一切变更必须经 `execute(MutationPlan)`**。

精确顺序（`YierdisDbMutationExecutor.executePrepared`）：

```text
admissionMode == RECLAMATION ? ledger.beginReclamation() : ledger.reserve(upperBound)
  -> backend.beginAllocationScope()
  -> plan.prepare()                     // 可失败：native 分配、拓扑替换、source 校验
  -> 用实测 peak 收窄 reservation：ledger.reconcile(...)
  -> 不变式：usedBytes + actualDelta 不溢出/不下溢
  ═══ commitStarted = true ═══
  -> prepared.commit() -> allocations.promote() -> ledger.commit(actualDelta)
  -> prepared.releaseSuperseded() -> 可选 trimEmptyPages
```

**失败边界是整套设计的支点**：

- **commit 前失败**：abort prepared → abort allocation scope → rollback ledger，旧 graph 保持可见；`NativeMemoryException`/`IllegalStateException` 还额外标记 **degraded**。
- **commit 后失败**：**不再宣称"没发生"**，best-effort promote/settle/release，标记 degraded，抛 `PostCommitMutationException`（result-unknown）。
- `RECLAMATION` 模式带硬不变式：upper bound 必须为 0、不得产生正增长、不得提交正 delta。
- `reserveNormalPlan` 有**重读准入循环**：被拒 → 重算 upper bound → 变小就用更小值重试；变大则 rollback 后按更大值重来。

**两套账刻意分开**：

- `MemoryLedger` 是 admission 的 SSOT：`usedBytes`（按 `actualDeltaBytes` 的逻辑账）+ `reservedBytes`（已准入未 settle 的预算窗口）；`effectiveUsedBytes = used + reserved`。
- 物理用量另由 `MemoryUsageSnapshot` 重算。per-db 与 global 的唯一区别是预算判定交给 coordinator；本地仍保留 reservation 以对账。

## L5 value 编码

阈值是内部常量、不可配置（`YierdisEncodingThresholds`）：hash 512 项/64 B、zset 128 项/64 B、set intset 512、list 8 KB；string embstr 44 B、最大 512 MB。

| 类型 | 编码 | 实现要点 |
|---|---|---|
| STRING | `STRING_INT` / `STRING_EMBSTR` / `STRING_RAW` | int 要求字节与 `Long.toString` canonical 形式逐字节相等；**三者物理表示相同**（一个 native `STRING_BYTES` blob），int 只影响 encoding 标签与 entry 元数据估算 |
| SET | `SET_INTSET` / `SET_HT` | intset 是 **heap 的 `short[]`/`int[]`/`long[]` 有序数组 + 二分**，16→32→64 可跳级；超过阈值或遇非整数成员转 `NativeByteMap` 常量 value（成员在 native、value 槽零开销） |
| HASH | `HASH_PACKED` / `HASH_HT` | packed 升级用 **COW 整体替换**；HT 用 **原位 delta**（`NativeByteMap.PreparedMutation`，source 显式记 `ACTIVE`/`OLD`/`ABSENT` + 非负 slot） |
| LIST | `LIST_PACKED` / `LIST_QUICKLIST` | quicklist = `ArrayDeque` 装固定 **80 B** native node 记录（ownerRoot/prev/next/payloadRef/entryCount/encodedBytes/flags/reserved），每 node 一个 listpack 块；单向升级，无降级 |
| ZSET | `ZSET_PACKED` / `ZSET_SKIPLIST` | packed 的 member 在 native `NativeListpack`、score 在 heap `double[]`；skiplist `MAX_LEVEL=32`，member 存 native 句柄，比较走无符号 `compareLex`；`canonicalScore` 把 `-0.0` 归一为 `+0.0`；单向升级 |
| HLL | **不是独立类型** | 以 `STRING`/`STRING_RAW` 存储，但**字节级 Redis 兼容**：`HYLL` header（16 B：magic + encoding 0=dense/1=sparse + 3 B 保留）、dense 6-bit LSB 打包共 12304 B、sparse `ZERO`/`XZERO`/`VAL` 游程、MurmurHash64A(seed `0xadc83b19`)、Ertl tau/sigma 估计器。sparse→dense 晋升与 Redis 一致：**寄存器值 > 32 或 sparse 长度超过 3000 B**；dense 永不降级 |

`NativeByteMap` 的 value 布局有 `OBJECT_REFERENCES`/`NATIVE_HANDLES`/`CONSTANT` 三种：SET 用 `CONSTANT` 让成员集合的 value 槽零开销；ZSET 的 byMember 索引用 `borrowedKeys`（`ownsKeys=false`，只借用 canonical member handle）。

### 刻意偏离 Redis 的地方

| 维度 | Redis | Yierdis |
|---|---|---|
| string `int`/`embstr` | 真的换物理表示（内联整数 / 共享分配） | **只换 encoding 标签，字节仍是 raw native blob**，不省内存 |
| intset | `intset` 紧凑内存布局 | **heap 的 `short[]`/`int[]`/`long[]`**，不受 off-heap 口径影响 |
| listpack 二进制格式 | Redis 自有 listpack 格式 | **自定义 varint(len+1)+payload 块格式**；HLL 是唯一字节级兼容的编码 |
| ZSET listpack | `member,score` 交替编码 | native listpack **只存 member**，score 在 heap 的 `double[]` |
| hash packed 阈值 | Redis 7.x 默认 128 项 | **512 项**（对齐旧 `hash-max-ziplist-entries`） |
| 淘汰策略 | 8 种（含 volatile-*、LFU） | 只有 `noeviction` / `allkeys-random` / `allkeys-lru` |
| 过期 | 主动采样 + 惰性 | 到期时间优先队列 + 惰性校验，不做 keyspace 采样 |
| 并发 | 单线程 event loop | 单 owner thread + 显式线程守卫 |
| 内存管理 | jemalloc/libc 直接 free | pin + epoch + quarantine 后延迟回收 |

## L6 TTL 与过期

- `expireAtMillis` 是唯一 deadline；时钟是 `System.currentTimeMillis()`（`nanoTime` 只用于时间预算）。没有可注入时钟；测试通过显式参数传时间。
- **惰性过期**由 `YierdisDbKernel.liveEntryRecord` 在读路径拦截，命中即走完整 reclamation mutation 并结算 ledger，对调用方隐藏。
- **主动过期不扫 keyspace**，只消费 expires 索引队首：单次最多回收 **20** 个（stale 丢弃不占名额）+ 时间预算；**回收成功才出队，删除失败保留队首下轮重试**；每个候选先做三道惰性校验（key 仍在目录、entry 仍持有同一 key identity、deadline 与索引项一致），stale 即丢弃。
- TTL 命令通过 prepared entry replace/delete 实现：设 deadline 复用原 entry handle，`PERSIST` 改为 `-1`，已过期输入直接准备删除。**条件判定（NX/XX/GT/LT）在删除分支之前求值**，与 Redis `expireGenericCommand` 顺序一致。
- `TTL` 秒值按 `(剩余毫秒 + 500) / 1000` 四舍五入，与 Redis 对齐。

细节见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## L7 maxmemory 与淘汰

- 两条协调路径：`PER_DB` 把总预算均分给各 DB（余数逐个 +1）；`GLOBAL` 由 `YierdisGlobalMaxmemoryGovernor` 汇总所有 participant 的物理快照，跨 DB 选 victim，并提供**全局单调 LRU clock**，使 LRU 在多 DB 间可比。
- `usedBytesForMaxmemory = heapEstimated + nativeMetadataCommitted + nativeDataCommitted`；`effectiveUsedBytesForMaxmemory` 再叠加 `reservedBytes`（报告值；admission 比较的是不含在途 reservation 的值）。
- 策略只有 3 种。**过期 key 优先于 live key 被选为 victim**（过期候选以 `lruClock = 0` 上报，live key 访问时钟恒 ≥ 1），`evict(...)` 对它先走 expiration reclamation——但 `noeviction` 永不选 victim，过期占用只能等维护节拍或读路径惰性过期，增长写入仍按 OOM 拒绝。
- LRU 采样默认 5；样本数 ≥ key 数时退化为全量扫描。
- 淘汰循环双上限：`max(64, keyCount*2)` 次尝试 + 时间窗口（默认 5 ms）；global governor 另加 stalled 计数，连续无进展即退出。

细节见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## L8 线程归属、epoch 与健康

- `DbThreadGuard` 是 `OPEN → CLOSING → CLOSED` 状态机 + owner thread 绑定：未绑定 / 跨线程 / CLOSING / CLOSED 一律 fail-fast；一次绑定同时锁住 DB 访问与 native 内存。
- Netty I/O 线程**只提交**；真正的 DB 执行在 `SerialOwnerExecutor` 单线程上，维护命令也投到同一个 owner executor。
- **SCAN / KEYS 一致性靠 epoch + discovery/replay**：`KeyWindow` 在 epoch 内记录 cursor、目录 generation/capacity、glob、过期时间，`emitTo` 时按同一物理范围重放，必须得到相同 count、无多余匹配、结束游标一致，否则抛 `IllegalStateException`。
- **degraded 不自动恢复**：写入被 `MISCONF DB is in a degraded state; writes are disabled` 拒绝；`reconcileAccounting()` 是唯一显式恢复入口，刻意绕过 mutation executor（degraded 会拒写），把逻辑账本对齐到物理重算值，成功才清除 degraded。持续性记账 bug 会反复以事故暴露，而不是被静默抹平。
- 其运行期副作用是：`requireWritable` 在读取 `AdmissionMode` **之前**执行，因此 degraded 时连 reclamation 类 mutation 也被拒——**过期回收、`DEL`、`FLUSHDB`、读路径惰性回收都会失败**。详见 [`db-behavior-gaps.md`](./db-behavior-gaps.md)。

## L9 维护与自省

维护节拍严格顺序（`YierdisDbDataMaintenance.runMaintenance`）：

```text
reclaimDetachedEntries(≤64)
  -> health.requireWritable()
  -> drainExpiredWithinBudget()      // 循环到无到期候选或时间预算耗尽
  -> rehashMaintenance(64 slots)
  -> enforceMaxmemory()
```

之后才轮到全局 governor 的 `enforceGlobalMaxmemoryMaintenance`。degraded 时除 detached 回收外的整个 tick 被跳过。

- `FLUSHDB ASYNC` 只在 mutation commit 边界**发布空目录**，旧目录进入 detached 队列，由后续 owner maintenance 分批释放（`ASYNC_FLUSH_RECLAIM_MAX_ENTRIES = 64`）。
- `MEMORY USAGE` 是可解释估算（key 长度 + entry metadata + value root 估算），不是 JVM instrumentation。

## 多 DB 与 server 装配

- `YierdisInstance.create` 按 `databases` 创建独立的 `YierdisDb`，每个 DB 有自己的 backend/runtime、keyspace、entry table、roots 和 ledger。
- `PER_DB` scope 下按 `maxmemoryBytes / databases` 分份额、余数分给前几个 DB；`GLOBAL` scope 下每个 DB 记录完整全局额度并挂同一个 governor。
- 一个 instance 的 shutdown 按 DB 逐个收敛，失败聚合为 suppressed，原始失败保持 primary。

## 与 Redis C 实现的对照

| 维度 | Redis | Yierdis |
|---|---|---|
| 键/值引用 | `robj*` 指针 | `(allocatorId, localRaw)` 稳定句柄 |
| 字典 | `dict`（链式 + 增量 rehash） | 开放寻址 + 墓碑 + 增量 rehash，keyspace 与 HT 编码共享拓扑 |
| 内存管理 | jemalloc/libc | FFM 自建 size class + page/span allocator |
| 回收安全 | 直接 free，无 GC | pin + epoch + quarantine 保证 view 安全，再延迟回收 |
| maxmemory | 全局单一 | per-db 或 global coordinator 两种 scope |
| 过期 | 主动采样 + 惰性 | 到期时间优先队列 + 惰性校验 |
| 并发 | 单线程 event loop | 单 owner thread + 显式线程守卫 |

## 设计取舍与代价

1. **prepared mutation 复杂度成本很高**：每个 family 都要实现 unchanged/insert/replace/delete/upsert/callback/batch 变体 + source 校验 + abort + superseded 释放；`ListValue`、`ZSetValue`、`NativeByteMap` 都是千行级类。
2. **两套账允许静默漂移**，只在 `reconcileAccounting()` 时发现；native reclaimable bytes 也不能预先从 committed footprint 扣。
3. **内存估算大量硬编码常量**（entry overhead 64 B、SET 32 B、ZSET 96 B、各类对象 16/48/64/72/80 B…），与真实 JVM 开销漂移时账本不会报错。
4. **expires 索引无界增长**：stale 项只在消费到队首且已到期时才丢弃，高频改 TTL 会持续堆积；且这部分堆不进任何内存账。
5. **主动过期可能被单个候选卡住**：队首候选删除失败即返回，后续候选等下一轮。
6. **degraded 是重状态**：连回收都停且不可自愈，恢复入口没有接入 server/command 路径。
7. **`noeviction` 下不回收过期占用**：这是有意的策略边界（见 TTL/maxmemory 专题），但会造成"明明有可回收过期 key 仍 OOM"的运维困惑。
8. **观测字段命名有历史包袱**：`ledger_used_bytes` 实为堆估算，与 ledger 逻辑 `usedBytes` 无关。

## 可疑与冗余实现观察

以下各点均读自源码，不影响正确性，但值得清理或确认：

- `YierdisNativePageAllocator.stats()`：`freePages` 复用 `emptySmallPages`；`mediumFreeBytes`/`largeFreeBytes` 恒 0；`liveMediumSpanPages`/`liveLargeSpanPages` 实际计的是**页数**而非 span 描述符数。
- defrag 的 `skippedBudgetObjects` 只在 byte 预算停止时自增，object/time 预算停止时为 0。
- `state == STATE_CORRUPT` 定义但本层从未写入或匹配；handle 的 4-bit `flags` 由 `readMeta` 解出、经 `localHandleFor` 原样回填，但**所有写入路径都传 0**，也没有任何校验，实际恒为 0。
- `NativeAllocatorStats.defragReclaimedPages` 在 `moveLiveObject` 里按 `retiredBytes / PAGE_BYTES` 记账，统计的是**退役 block 覆盖的页数**，不是后端真正回收的页数（真实回收发生在 quarantine/epoch 允许之后）。
- `ZSkipList.P = 0.25` 声明但未使用；`levelFor` 用 `mix64(scoreBits ^ memberHash) & 0x3` 决定层数——概率等价于 P，但**层数由 (score, member) 确定性推导**，使 prepared insert/delete 无需携带随机状态。
- `NativeReallocPolicy` 只有 `PRESERVE_PREFIX`，实现未按 policy 分支（行为上仍保留 prefix）。
- `doubleFreeDetections` 实为"free 时命中 stale 句柄"，包含非 double-free 场景。
- `defragCycle` 没有"移动是否有收益"的启发式，对每个合格对象都会重新分配并复制。
- `SetValue.LONG_MIN_VALUE_BYTES` 为死常量；quicklist node 的 `payloadRef`（offset 48）恒为 `NULL` 且无读取者；`ZSkipList.P` 声明但 `levelFor` 用 `(state & 0x3) == 0` 等价实现；`NativeObjectKind.SCORE_BYTES` 声明但全仓无使用。
- 单 DB 淘汰的 `maxAttempts = keyCount * 2` 无 int 溢出保护（global governor 有）。
- `INFO memory` 的 `yierdis_maxmemory_per_db_bytes` 用整数除法，与实际"余数 +1"分配可能差 1 字节。

完整的运行期缺口、影响与状态见 [`db-behavior-gaps.md`](./db-behavior-gaps.md)。

## 验证状态

本文主要基于源码，但并非每一行都经过同等强度的核对。为便于后续维护，这里明确来源：

**已逐条回源核对（本次审计）**

- L0 全部：`YierdisLocalHandleCodec`（位段布局）、`YierdisNativeObjectSegment`（4096 槽/段、retire 位图）、`YierdisNativeObjectTable`（36 B 元数据、打包位段、capacity 由 descriptor 反查、generation 耗尽 retire）、`YierdisNativePageAllocator`（64 KiB 页、`MEDIUM_MAX_BYTES=1 MiB`、free-offset 栈、warm page、trim、page id 复用）、`YierdisFfmStableMemoryBackend`（scope checkpoint/abort/promote/growth、pin/quarantine/epoch、`canReclaim`、defrag 循环与预算、`stats()` 映射）。
- L5 全部：`SetValue`、`HashValue`、`ListValue`、`ZSetValue`、`ZSkipList`、`NativeByteMap`、`NativeListpack`、`YierdisHyperLogLog`、`YierdisEncodingThresholds`、`ValueEncoding`。
- 关键行为事实：`DbThreadGuard`、`YierdisDbHealth`、`YierdisDbMutationExecutor` 中 `requireWritable` 早于 `admissionMode`、`EntryTable`、`NativeKeyDirectory`、`OpenAddressingTopology`、`HashCapacityPolicy`、`ExpiresIndex`、`reconcileAccounting` 的全仓调用点、`INFO`/`MEMORY STATS` 字段映射。

**仍只做了抽查或依赖既有专题文档/子代理审计**

以下文件的**完整方法级细节**没有逐行重读（此前读取中段曾被工具截断），相关结论来自既有专题文档与并行审计，引用时应保留怀疑：

- `YierdisDbKeyLifecycle` 中段（`reconcileDerivedEntryState`、publish/replace/delete、detach 与 `expireCount` 维护）；
- `YierdisDbMutationExecutor` 中段（`postCommitFailure`、`requireReclamationInvariants`、`requireLedgerDeltaInvariant`、`reserveNormalPlan` 重试循环）；
- `YierdisDbMemoryLedger`、`PreparedEntryMutation`、`AbstractPreparedMutation` 的完整状态机细节；
- `YierdisDbMaxmemorySupport`、`YierdisGlobalMaxmemoryGovernor`、`YierdisDbDataMaintenance`、`YierdisTtlOps`、`YierdisKeyspaceOps`、`YierdisStringOps` 的中段方法；
- `YierdisInstance` 的多 DB 装配细节。

L6–L9 的**机制框架**由本人读源码确认，但其中精确常量与分支顺序如需用作改动依据，建议先按 `db-behavior-gaps.md` 的方式再回源一次。

## 修改导航

- DB 组装：`YierdisDbEngineFactory`、`YierdisDb`、`YierdisDbStorage`。
- 内部执行入口：`YierdisDbKernel`、`YierdisDbMutationExecutor.MutationPlan`；实现细节：`YierdisDbMemoryContext`、`YierdisDbKeyLifecycle`。
- 身份与存储图：`EntryTable`、`NativeStorageLayout`、`NativeCollectionRootTable`、各 type root。
- keyspace 索引：`NativeKeyDirectory`、`OpenAddressingTopology`、`HashCapacityPolicy`、`ExpiresIndex`、`ScanCursorV2`。
- mutation 与预算：`PreparedDbMutation`、`PreparedEntryMutation`、`PreparedBatchMutation`、`YierdisDbMemoryLedger`、`MemoryLedger`。
- 编码：`ValueEncoding`、`YierdisEncodingThresholds`、`HashValue`/`ListValue`/`SetValue`/`ZSetValue`、`NativeListpack`、`NativeByteMap`、`YierdisHyperLogLog`。
- TTL / maxmemory：`YierdisTtlOps`、`YierdisDbExpirationSupport`、`YierdisDbMaxmemorySupport`、`MaxmemoryPolicy`、`YierdisGlobalMaxmemoryGovernor`。
- 运行与维护：`YierdisDbRuntimeState`、`DbThreadGuard`、`YierdisDbDataMaintenance`、`YierdisDbHealth`、`YierdisDbMemoryReporter`。
- native 分配器：`YierdisFfmStableMemoryBackend` 与 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。
