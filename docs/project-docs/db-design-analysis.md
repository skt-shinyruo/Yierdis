# DB 设计分析

Yierdis 的 DB 层**为什么这样设计**，要从分层意图、层间契约、与 Redis C 实现的对照、刻意偏离，以及设计取舍中值得质疑之处说起。

与其它文档的分工：

- [`db-internals.md`](./db-internals.md)：DB 内部结构的**机制与组合参考**（对象是什么、谁调用谁、改哪里要动什么）。
- [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)：各专题的完整机制。
- [`db-behavior-gaps.md`](./db-behavior-gaps.md)：运行期行为缺口、可疑观察项与运维注意事项。

本文的写法是：**每条设计选择都给出"为什么不选另一条路"以及这条路的代价**。只给结论不给不选它的理由，等于没论证。

## 一句话概括

Yierdis 的 DB 是**受单一 owner thread 约束的 native 内存所有权内核**，而非 `Map<byte[], Object>`：

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

这个边界的代价是"读要绕、写要包"：读路径必须自己重复 owner 检查（不进 executor），写路径每个 family 都要写 prepared mutation 的多个变体。换来的收益是唯一写入口——maxmemory、回滚、owner 约束都只需要在一个地方成立。

## 核心取舍一：为什么用稳定句柄而不是地址

Redis 用 `robj*` 直接就是地址，而且**这不是问题**——因为 Redis 的 rehash 迁移的是指针数组，对象地址从不改变；没有 defrag 时 `robj*` 就是稳定身份。

Yierdis 的处境不同：native 对象没有中间指针层。如果一个 128 B 的 listpack 块被 defrag 搬到新页，所有指向它的引用都必须被改写：`EntryRecord.valueHandle`、collection root record、quicklist node 的 `payloadRef`、skiplist 的 member 引用……这些引用分散在多种 native 结构里，还可能是 heap 上的 adapter 持有的 `NativeHandle`。**遍历改写既慢又必然漏。**

于是选择把"位置"从引用中抽出来：

```text
DB graph 存 NativeHandle(allocatorId, localRaw)
                              └─ 私有 codec：domain | kind | slotId | generation | flags
object table 槽位存当前 pageId / pageOffset / size / capacity(=descriptor 反查)
```

代价（都是真实开销，不是理论）：

- 每次访问多一次 object table 解析，且要过 generation 校验；
- handle 是 16 字节（两个 long），地址是 8 字节；
- object table 每个对象固定 36 B 元数据——一个 16 B 的 key 字节要背 36 B 记账元数据，比 2× 还多。

不这样选的后果：defrag 从"可选优化"变成"必须停写世界并全图改写"，任何一处漏改就是静默的数据错乱；而且无法实现下面的 epoch 语义（旧位置在被观察期间不能失效）。

## 核心取舍二：为什么 mutation 必须走单一 executor

三个要求必须同时成立，而它们只能在同一个地方实现：

1. **先记账后改图**——增长型写入需要在改动前拿到一个可拒绝的准入决定；
2. **失败可物理回滚**——prepare 阶段可能已经分配了十几个 native 对象；
3. **owner 唯一**——没有第二个人能并发改 graph。

如果允许 ops 自己改 graph，第 1 条会退化成"先改，发现超了再补偿删除"。而补偿删除本身是一次写入，它可能失败、可能触发 maxmemory 判定、可能又需要补偿——一致性证明直接崩掉。第 2 条则完全不可能：改动分散在多个 ops 里，没有统一的"撤销点"。

代价：

- 每个 family 都要实现 unchanged / insert / replace / delete / upsert / callback / batch 变体 + source 校验 + abort + superseded 释放，`ListValue`、`ZSetValue`、`NativeByteMap` 因此都是千行级类；
- 读路径必须绕开 executor，导致 owner 检查在两个地方各写一遍（读在 `liveEntryRecord` 前，写在 mutation 创建/状态检查/提交入口）；
- 回收类操作（expire/evict/flush）也要包成 mutation，即使它们的 upper bound 是 0；
- 批量语义需要额外的 `PreparedBatchMutation` 组合层。

不这样选的后果：未记账的增长会绕过 admission，maxmemory 变成"事后统计"而不是"事前拒绝"；一次中途失败会留下半个 graph。

## 核心取舍三：为什么 commit 前后语义要分界

`prepared.commit()` 之后，物理上已经发生不可回滚的事情：object table 里的 location 已发布、旧 block 已进入 retired list、目录槽位已替换。此时**没有回到旧状态的路**，任何"假装回滚"都是在制造错误的真相。

因此设计上把失败分成两类：

| | commit 之前 | commit 之后 |
|---|---|---|
| 语义 | 未发生 | 已发生，但客户端不知结果 |
| 动作 | abort prepared → abort allocation scope → rollback ledger | best-effort promote/settle/release → `recordInvariantFailure` → degraded |
| 对外 | 原异常 | `PostCommitMutationException`（result-unknown） |
| 是否 degrade | `NativeMemoryException`/`IllegalStateException` 会降级；容量失败（OOM）不降级 | 一定降级 |

代价：

- 系统多出一个"结果未知"状态，命令层必须能表达它；
- degraded 是重状态：写入被 `MISCONF DB is in a degraded state; writes are disabled` 拒绝，**连回收类 mutation 一起拒**（`requireWritable` 在读取 `AdmissionMode` 之前执行），且唯一恢复入口 `reconcileAccounting()` 刻意绕过 executor，在 stock server 上还没有触发路径（见 [`db-behavior-gaps.md`](./db-behavior-gaps.md) A1）。

不这样选的后果：commit 后失败若对外报"没发生"，客户端会重试，而重试建立在错误前提上（比如 `INCR` 会被执行两次）；内部若假装回滚，账本会与物理实际不符，且这个偏差会被静默带入后续 admission。

## 核心取舍四：为什么是双份账

两套账各自的口径：

- **逻辑账**（`YierdisDbMemoryLedger`）：`usedBytes` 按 committed mutation 的 `actualDeltaBytes` 累加，`reservedBytes` 是在途预算；`effectiveUsedBytes() = used + reserved`。O(1) 更新。
- **物理账**（`MemoryUsageSnapshot` 重算）：`heap estimated + native metadata committed + native data committed`。O(对象数)。

为什么不能合一：逻辑 delta 无法精确推出物理 footprint——size class 会把 17 KB 上取整到 24 KB、page 粒度是 64 KiB、退役 block 仍占着 `reservedBytes`、object table 还有 descriptor heap 与 registry heap。反过来，物理重算不可能每次写都跑。

代价：

- 两套账允许**静默漂移**——不会自动触发 invariant failure，只在 `reconcileAccounting()` 时暴露；
- native reclaimable bytes 不能预先从 committed footprint 扣除（回收是否真的发生要等 trim/epoch）；
- 报告口径容易出现"名字骗人"的字段（`ledger_used_bytes` 实为堆估算）。

不这样选的后果：只用逻辑账，上取整与碎片会让实际内存先于预期撑爆，且无法解释"为什么 1 GB 数据占了 1.4 GB"；只用物理账，每次写都要遍历对象做一次 O(n) 重算，单线程下不可接受。

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

**取舍**：size class 换来 O(1) 分配与零外部碎片（同页等长块，任意空槽可用），代价是内部碎片（17 KB 落在 24 KB 档）。替代方案是变长块 + 分裂/合并，但那会让 free 变成 O(空闲块数) 并引入拼接逻辑——在一个已经要在 owner 单线程上跑所有 mutation 的系统里不划算。

**事务与回收基础**

- `beginAllocationScope()` 同时给 page allocator 与 object table 打 checkpoint；`abort()` 按逆序 free 已跟踪句柄、关闭 scope 新建的空页、截断新建 segment、回滚 page id 复用集合与 `nextPageId`/`nextCreationSequence`。这是 mutation 失败可回滚的物理基础。
- **pin + epoch + quarantine**：被 free 但仍被 pin、或仍有活跃 epoch scope 可能观察旧位置的对象进入 `FREED_QUARANTINED`；搬迁后的旧 block 进入 retired list。`canReclaim(epoch)` 只让 `epoch <= 退役点` 的 scope 阻塞回收，后启动的 scope 不阻塞（论证见分配器专题 §5）。
- **defrag** 按 slotId 升序（`firstOccupiedSlot`/`nextOccupiedSlot`），跳过 quarantined 与非 allocated/pinned 状态，pin 计数非零也跳过；受 `maxObjects`/`maxMoveBytes`/`timeBudgetNanos` 三重预算约束；搬迁后经 retired-block + epoch 保证安全。

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

  句柄在 DB 边界**显式保存两个 long**，不退化成 `localRaw`。固定宽度是为了让 entry 表可以整体用 native 连续内存承载，读写都是定长偏移寻址；代价是 type/encoding/flags 各占 4 B 打包后仍有余量（都为枚举 ordinal）。

- **type root 是 16 字节自引用锚**（`COLLECTION_ROOT_RECORD_BYTES = Long.BYTES * 2 = 16`）：native 侧只存自己的句柄（用于 stale 校验与计账）。真正的聚合对象 `HashValue`/`ListValue`/`SetValue`/`ZSetValue` 是 **JVM 对象**，由 heap 上的 `HashMap<NativeHandle, AdapterSlot<T>>`（`NativeCollectionRootTable`）持有。
- 因此是**双份账**：payload 字节（key、field、member、listpack 块、quicklist node 记录）在 native；结构对象与哈希拓扑数组在 heap，两套都要估。
- 4 个 handle domain：`STORAGE_OBJECT`、`ENTRY_OBJECT`、`KEY_BYTES`、`TYPE_ROOT`。

> 容易误读的一点：`NativeKeyDirectory` **只把 key 字节存成 allocator-backed `KEY_BYTES`**；槽位数组（`byte[] states`、`int[] hashes`、`NativeHandle[] keyHandles`、`NativeHandle[] entryHandles`）在 heap，所以 `NativeKeyDirectory.nativeBytes()` 恒为 `0L`。"keyspace 在 native" 指的是 key 字节，不是 hash slot。

## L2 keyspace 索引：开放寻址 + 共享拓扑

`NativeKeyDirectory` 与 `NativeByteMap`（HASH/SET 的 HT 编码）**都不自己实现哈希逻辑**，委托同一个 `OpenAddressingTopology`。

- 4 种槽状态：`EMPTY`、`FILLED`、`TOMBSTONE`、`MIGRATED_SCAN_SHADOW`。**shadow 是为 SCAN 正确性专设的**：迁移后 old 表保留 key 用于定位与重放。
- 容量策略（`HashCapacityPolicy`）：`filled > capacity - capacity/4` → `GROW`；`tombstones > max(size, capacity/8)` → `COMPACT`；`capacity > 16 && size < capacity/8` → `SHRINK`。容量必须是 2 的幂，范围 `[16, 1<<30]`。
- **线性探测**；插入复用第一个 tombstone；remove 只置墓碑。
- **双表增量 rehash**：写路径每次顺带推进 `WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE)`（2 个 slot）；维护节拍另有 64 slot 预算。若 rehash 期间插入会顶穿合并占用，目录 API 会**同步坍缩**成一张装得下全部存活 entry 的 standalone 表，显式防止 "no insertion slot"。
- 哈希是 **SipHash24，种子来自 `SecureRandom`**（`HashSeed`），每个 `YierdisDbEngineFactory` 一份。
- 删除有两条路：按 key 字节探测；或**按 entry 持有的 key 句柄 + dict hash 反查槽位**（`removeEntry(keyHandle, keyHash, expectedHandle)`，O(probe)）。位置在删除时现查，所以双表状态下也不会指向 stale slot。
- **SCAN 游标**（`ScanCursorV2`）：`position[31:0] | phase[33:32] | generation[62:34]`（29 位 generation）。generation 不匹配或 phase 非法时从 active 表重启（允许重复，绝不因客户端乱填 cursor 抛错）。

**为什么不用链式哈希（Redis `dict` 的做法）**：链式实现上的增量 rehash 更简单（把桶里的节点逐个搬走即可），但每个 entry 需要额外指针，且 tombstone 问题变成"空桶/单链表"的复合状态机。开放寻址把状态收敛成一个 byte，让 keyspace 与 HASH/SET 的 HT 编码能共享同一份拓扑实现——代价是 tombstone 必须显式压缩，而且 SCAN 必须专门为 old 表加一个 shadow 状态。

## L3 key 生命周期与 staging

`YierdisDbKeyLifecycle` 是 directory + entry table + 各 root 的**唯一所有权记录**。

- 新 key 必须走 opaque `StagedEntry`：`EntryTable.reserve()` + `NativeKeyDirectory.stageInsert()`；abort/未发布时 `close()` 幂等释放两者；发布后 token 被消费。
- **`expireCount` 是派生计数**，随 publish/replace/release 对 `expireAtMillis >= 0` 的变化 ±1，不是独立索引；下溢会抛 `IllegalStateException("derived expire count underflow")`。
- **`ExpiresIndex`** 是 owner 线程独占的 `PriorityQueue`，只在 **deadline 真的变化**时写新项；touch / `KEEPTTL` 复用旧项；改 TTL / `PERSIST` 不主动清旧项，由消费方惰性判 stale。索引规模可超过存活 TTL key 数，且**不计入 ledger，也不计入物理 committed footprint**（见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)）。
- `version` 只在语义变化（新 record、TTL 或 flags 变化）时递增，**纯 access-clock touch 保留原 version**；`touchRecord` 在 LRU 下会写回新 record，且要求当前 record 与预期一致才落盘。

**为什么用 stage token 而不是"先插目录再补 entry"**：分两步插会让中间态对其他读可见（一个指向不存在 entry 的 key）。token 把"预留 + 发布"做成显式两阶段，abort 时只需关一个对象；代价是每个 family 的新增路径都要处理这个 token 的生命周期。

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

- **commit 前失败**：abort prepared → abort allocation scope → rollback ledger，旧 graph 保持可见；`NativeMemoryException`/`IllegalStateException` 还额外标记 **degraded**；容量失败（`MemoryLedgerOutOfMemoryException`/`NativeCapacityExceededException`）转成 OOM 错误但**不** degrade。
- **commit 后失败**：**不再宣称"没发生"**，best-effort promote/settle/release，标记 degraded，抛 `PostCommitMutationException`（result-unknown）。
- `RECLAMATION` 模式带硬不变式：upper bound 必须为 0、不得产生正增长、不得提交正 delta（`requireReclamationInvariants`）。
- `reserveNormalPlan` 有**重读准入循环**：被拒 → 重算 upper bound → 变小就用更小值重试；变大则 rollback 后按更大值重来。

**两套账刻意分开**：

- `MemoryLedger` 是 admission 的 SSOT：`usedBytes`（按 `actualDeltaBytes` 的逻辑账）+ `reservedBytes`（已准入未 settle 的预算窗口）；`effectiveUsedBytes = used + reserved`。
- 物理用量另由 `MemoryUsageSnapshot` 重算；per-db 与 global 的唯一区别是预算判定交给 coordinator，本地仍保留 reservation 以对账。

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

**取舍**：编码单向升级、无降级，换来的是"编码只前进，状态空间有限"；代价是一次大删除后不会回到紧凑编码。同理 SET 的 `SET_INTSET` 放在 heap 是因为它本来就是排序数组 + 二分，放进 native 只会多一层解析，且不受 off-heap 口径影响。

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
- **主动过期不扫 keyspace**，只消费 expires 索引队首：单次最多回收 **20** 个（`CLEANUP_MAX_CANDIDATES`，stale 丢弃不占名额）+ 时间预算；**回收成功才出队，删除失败保留队首下轮重试**；每个候选先做三道惰性校验（key 仍在目录、entry 仍持有同一 key identity、deadline 与索引项一致），stale 即丢弃。
- TTL 命令通过 prepared entry replace/delete 实现：设 deadline 复用原 entry handle，`PERSIST` 改为 `-1`，已过期输入直接准备删除。**条件判定（NX/XX/GT/LT）在删除分支之前求值**，与 Redis `expireGenericCommand` 顺序一致。
- `TTL` 秒值按 `(剩余毫秒 + 500) / 1000` 四舍五入，与 Redis 对齐。

细节见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

**为什么是优先队列而不是 Redis 的采样 dict**：优先队列给出"按到期时间精确有序"的保证，队首即最早到期者，不需要随机采样撞运气；代价是 stale 项会堆积（改 TTL 时不清旧项），且队列本身不进任何内存账。

## L7 maxmemory 与淘汰

- 两条协调路径：`PER_DB` 把总预算均分给各 DB（`maxmemoryBytes / databases` 的余数逐个 +1）；`GLOBAL` 由 `YierdisGlobalMaxmemoryGovernor` 汇总所有 participant 的物理快照，跨 DB 选 victim，并提供**全局单调 LRU clock**，使 LRU 在多 DB 间可比。
- `usedBytesForMaxmemory = heapEstimated + nativeMetadataCommitted + nativeDataCommitted`；`effectiveUsedBytesForMaxmemory` 再叠加 `reservedBytes`（报告值；admission 比较的是不含在途 reservation 的值）。
- 策略只有 3 种。**过期 key 优先于 live key 被选为 victim**（过期候选以 `lruClock = 0` 上报，live key 访问时钟恒 ≥ 1），`evict(...)` 对它先走 expiration reclamation——但 `noeviction` 永不选 victim，过期占用只能等维护节拍或读路径惰性过期，增长写入仍按 OOM 拒绝。
- LRU 采样默认 5（`YierdisInstanceConfig.maxmemorySamples = 5`）；样本数 ≥ key 数时退化为全量扫描。
- 淘汰循环双上限：`max(64, keyCount*2)` 次尝试 + 时间窗口；global governor 另加 stalled 计数（`max(1, totalKeys)`），连续无进展即退出。

细节见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

**为什么 PER_DB 不等于 GLOBAL**：PER_DB 让每个 DB 的 admission 判定完全不依赖其他 DB，代价是热点 DB 会先撞墙而其他 DB 有空闲额度；GLOBAL 让额度统一，代价是每次 admission 都要经过 coordinator 汇总并跨 DB 挑 victim（更长的判定路径 + 全局 LRU clock 的额外状态）。

## L8 线程归属、epoch 与健康

- `DbThreadGuard` 是 `OPEN → CLOSING → CLOSED` 状态机 + owner thread 绑定：未绑定 / 跨线程 / CLOSING / CLOSED 一律 fail-fast；一次绑定同时锁住 DB 访问与 native 内存。
- Netty I/O 线程**只提交**；真正的 DB 执行在 `SerialOwnerExecutor` 单线程上，维护命令也投到同一个 owner executor。
- **SCAN / KEYS 一致性靠 epoch + discovery/replay**：`KeyWindow` 在 epoch 内记录 cursor、目录 generation/capacity、glob、过期时间，`emitTo` 时按同一物理范围重放，必须得到相同 count、无多余匹配、结束游标一致，否则抛 `IllegalStateException`。
- **degraded 不自动恢复**：写入被 `MISCONF DB is in a degraded state; writes are disabled` 拒绝；`reconcileAccounting()` 是唯一显式恢复入口，刻意绕过 mutation executor（degraded 会拒写），把逻辑账本对齐到物理重算值，成功才清除 degraded。持续性记账 bug 会反复以事故暴露，不会被静默抹平。
- 其运行期副作用是：`requireWritable` 在读取 `AdmissionMode` **之前**执行，因此 degraded 时连 reclamation 类 mutation 也被拒——**过期回收、`DEL`、`FLUSHDB`、读路径惰性回收都会失败**。详见 [`db-behavior-gaps.md`](./db-behavior-gaps.md)。

**为什么 owner 可以是 Netty I/O 线程之外的一个线程**：FFM 的 `Arena.ofShared()` 允许 region 跨线程关闭，但**这不解除 DB 的 thread confinement**——把 region 当共享对象来关闭是 runtime 层的权限，不是 graph 的权限。二者的边界见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## L9 维护与自省

维护节拍严格顺序（`YierdisDbDataMaintenance.runMaintenance`）：

```text
checkThread
  -> reclaimDetachedEntries(≤ ASYNC_FLUSH_RECLAIM_MAX_ENTRIES = 64)
  -> health.requireWritable()
  -> drainExpiredWithinBudget()      // 循环到无到期候选或时间预算耗尽
  -> rehashMaintenance(MAINTENANCE_REHASH_MAX_INSPECTED_SLOTS = 64)
  -> enforceMaxmemory()
```

之后才轮到全局 governor 的 `enforceGlobalMaxmemoryMaintenance`。degraded 时除 detached 回收外的整个 tick 被跳过。

- `FLUSHDB ASYNC` 只在 mutation commit 边界**发布空目录**，旧目录进入 detached 队列，由后续 owner maintenance 分批释放（`ASYNC_FLUSH_RECLAIM_MAX_ENTRIES = 64`）。
- `MEMORY USAGE` 是可解释估算（key 长度 + entry metadata + value root 估算），不是 JVM instrumentation。

**为什么把 requireWritable 放在第 3 步**：degraded 时仍要允许"释放已经 detached 的旧目录"，否则一次 `FLUSHDB ASYNC` 之后的 degraded 会永远漏着这批内存。反过来，把 requireWritable 放在最前面就会让这条唯一的清理路径也被掐掉。

## 多 DB 与 server 装配

- `YierdisInstance.create` 按 `databases` 创建独立的 `YierdisDb`，每个 DB 有自己的 backend/runtime、keyspace、entry table、roots 和 ledger。
- `PER_DB` scope 下按 `maxmemoryBytes / databases` 分份额、余数分给前几个 DB（每个 +1）；`GLOBAL` scope 下每个 DB 记录完整全局额度并挂同一个 governor。
- 一个 instance 的 shutdown 按 DB 逐个收敛，失败聚合为 suppressed，原始失败保持 primary。

## 与 Redis C 实现的逐点对照

| 维度 | Redis C | Yierdis | 代价 / 收益 |
|---|---|---|---|
| 键值引用 | `robj*`（地址 + `refcount`） | `(allocatorId, localRaw)` 稳定句柄 | 多一次 table 解析与 generation 校验、每对象 36 B 元数据；换来 defrag 无需改写全图引用 |
| 对象共享 | refcount + 共享整数对象 | 无引用计数，靠唯一所有权 + generation | 失去共享收益；换来不需要处理循环引用与 refcount 下溢 |
| 字典 | `dict`（链式 + 两个 hash table 增量 rehash） | 开放寻址 + 墓碑 + shadow 表 | 链式 rehash 更简单、每桶搬运即完成；Yierdis 需要 tombstone 压缩，但拓扑可与 HASH/SET HT 编码共享 |
| 过期 | `expires` dict + 主动采样（随机 20/轮）+ 惰性 | `PriorityQueue` 队首 + 三道惰性校验 | 有序且精确；代价是 stale 项堆积且不进内存账 |
| 内存管理 | jemalloc/libc，直接 `free` | FFM size class + page/span allocator | 无外部碎片、分配 O(1)；代价是内部碎片 + 延迟回收的额外状态 |
| 回收安全 | 直接 free，无并发读（单线程） | pin + epoch + quarantine，再延迟回收 | 支持有界 view 与 SCAN replay；代价是 retired/quarantine 的内存不是立即归还 |
| maxmemory | 全局单一 + 8 种策略 | per-db 或 global coordinator 两种 scope + 3 种策略 | 策略更少、更易解释；代价是缺少 volatile-* 与 LFU 语义 |
| 失败处理 | 单线程内完成，无"回滚"概念 | prepare/commit/abort + result-unknown | 能表达"已发生但结果未知"；代价是命令层要处理第三态与 degraded |
| 并发 | 单线程 event loop | 单 owner thread + `DbThreadGuard` | 显式 fail-fast 而非依赖约定；代价是跨线程访问直接抛异常 |
| 数据结构实现 | 手写 SDS/intset/ziplist/quicklist/skiplist | 部分复用 native（listpack 自定义块格式、HT topology 共享） | HLL 字节级兼容、其余格式自定义；代价是二进制不互通 |

## 设计取舍与代价

1. **prepared mutation 的复杂度代价很高**：每个 family 都要实现 unchanged/insert/replace/delete/upsert/callback/batch 变体 + source 校验 + abort + superseded 释放；`ListValue`、`ZSetValue`、`NativeByteMap` 都是千行级类。替代方案是"先改再补偿"，但那需要补偿本身也可回滚，证明不收敛。
2. **两套账允许静默漂移**，只在 `reconcileAccounting()` 时发现；native reclaimable bytes 也不能预先从 committed footprint 扣。替代方案是每次写做物理重算，代价 O(n)。
3. **内存估算大量硬编码常量**（entry overhead 64 B、SET 32 B、ZSET 96 B、各类对象 16/48/64/72/80 B…），与真实 JVM 开销漂移时账本不会报错。替代方案是用 JOL/Instrumentation 实测，但那要求在生产路径引入 instrumentation 依赖。
4. **expires 索引无界增长**：stale 项只在消费到队首且已到期时才丢弃，高频改 TTL 会持续堆积；且这部分堆不进任何内存账。替代方案是改 TTL 时主动删旧项，但那需要索引支持 O(log n) 定位随机项（`PriorityQueue` 不支持）。
5. **主动过期可能被单个候选卡住**：队首候选删除失败即返回，后续候选等下一轮。替代方案是失败时跳过队首继续尝试，代价是丢失"队首必须最先处理"的语义。
6. **degraded 是重状态**：连回收都停且不可自愈，恢复入口没有接入 server/command 路径。替代方案是让 degraded 只拒增长型写入（放开 RECLAMATION），但那样记账 bug 会被回收路径继续放大。
7. **`noeviction` 下不回收过期占用**：这是有意的策略边界（见 TTL/maxmemory 专题），但会造成"明明有可回收过期 key 仍 OOM"的运维困惑。
8. **观测字段命名有历史包袱**：`ledger_used_bytes` 实为堆估算，与 ledger 逻辑 `usedBytes` 无关（后者全仓只有 `prepareFlushDb` 与 `reconcileAccounting` 使用）。

## 可疑与冗余实现观察

以下各点均读自源码，不影响正确性，但值得清理或确认：

- `YierdisNativePageAllocator.stats()`：`freePages` 复用 `emptySmallPages`；`mediumFreeBytes`/`largeFreeBytes` 恒 0；`liveMediumSpanPages`/`liveLargeSpanPages` 实际计的是**页数**（累加 `span.pageCount`）而非 span 描述符数（后者是 `liveSpanDescriptors`）。
- `NativeAllocatorStats.defragReclaimedPages` 混用两种口径：`moveLiveObject` 里按 `retiredBytes / PAGE_BYTES` 记**退役 block 覆盖页数**（不是真正回收的页数），`trimEmptyPages(...)` 又把本次 trim 回收量累加到同一字段。
- defrag 的 `skippedBudgetObjects` 只在 byte 预算停止时自增，object/time 预算停止时为 0。
- `state == STATE_CORRUPT` 定义但本层从未写入或匹配；handle 的 4-bit `flags` 由 `readMeta` 解出、经 `localHandleFor` 原样回填，但**所有写入路径都传 0**，也没有任何校验，实际恒为 0。
- `doubleFreeDetections` 实为"free 时命中 stale 句柄"（`requireLiveMetaForFree` 捕获 `StaleNativeHandleException` 即自增），包含非 double-free 场景。
- `ZSkipList.P = 0.25` 声明但未使用；`levelFor(score, member)` 以 `state = mix64(Double.doubleToLongBits(score) ^ memberStore.hashBytes(member))` 起步，从 `lvl = 1` 开始只要低 2 位为 0（`(state & 0x3L) == 0L`）就升一层并 `state = mix64(state + 0x9E3779B97F4A7C15L)`，直到 `MAX_LEVEL = 32`——每层继续概率 1/4，与声明的 P 等价，但**层数完全由 (score, member) 确定性推导**，所以 prepared insert/delete 不必携带随机状态、重放同一插入必得同一拓扑。
- `NativeReallocPolicy` 只有 `PRESERVE_PREFIX`，实现未按 policy 分支（行为上仍保留 prefix）。
- `defragCycle` 没有"移动是否有收益"的启发式，对每个合格对象都会重新分配并复制。
- `SetValue.LONG_MIN_VALUE_BYTES` 为死常量；quicklist node 的 `payloadRef`（offset 48）恒为 `NULL` 且无读取者；`NativeObjectKind.SCORE_BYTES` 声明但全仓无使用。
- 单 DB 淘汰的 `maxAttempts = Math.max(64, keyCount * 2)` 无 int 溢出保护（global governor 的同类计算有）。
- `INFO memory` 的 `yierdis_maxmemory_per_db_bytes` 用整数除法，与实际"余数 +1"分配可能差 1 字节。

完整的运行期缺口、影响与状态见 [`db-behavior-gaps.md`](./db-behavior-gaps.md)。

## 验证状态

本文主要基于源码。为便于后续维护，这里明确哪些结论已逐条回源、哪些仍是推断：

**已逐条回源核对（本次审计）**

- **L0 全部**：`YierdisLocalHandleCodec`（五位段布局与全部 shift/mask 常量）、`YierdisNativeObjectSegment`（`SLOTS_PER_SEGMENT = 4096`、`freeStack`、`RETIRED_WORDS = 64`、`retire`/`isRetired`/`releaseOffset`）、`YierdisNativeObjectTable`（`META_BYTES = 36` 与七个字段偏移、packed word 的 state/pageClass/flags/kind/domain/generation 位段、`INITIAL_GENERATION = 1`、`MAX_GENERATION = 0x0fff`、六个状态常量、capacity 不在槽位）、`YierdisNativePageAllocator`（`PAGE_BYTES = 64 * 1024`、`MEDIUM_MAX_BYTES = 1 MiB`、`freeOffsets`/`freeCount`/`liveBlocks`、`findWarmPage` 与 `createdInActiveScope`、`claimPageId`/`nextPageId` 溢出、`summarizePages` 字段口径、`restoreAllocationScope` 的顺序与异常消息）、`YierdisFfmStableMemoryBackend`（`canReclaim` 实现、`freeLocal` 的 `delayRelease` 判定、`reserveMovedBlock`、`reallocateLocal` 三条分支、`defragCycle` 遍历与预算顺序、`moveLiveObject` 的 `defragReclaimedPages` 记账、`trimEmptyPages` 对同一字段的累加、`stats()` 字段映射）。
- **L5 全部**：`SetValue`、`HashValue`、`ListValue`、`ZSetValue`、`ZSkipList`、`NativeByteMap`、`NativeListpack`、`YierdisHyperLogLog`、`YierdisEncodingThresholds`、`ValueEncoding`。
- **关键行为事实**：`DbThreadGuard`、`YierdisDbHealth`（`MISCONF_DEGRADED` 文案与 `recordInvariantFailure`/`recordReconciliation` 语义）、`YierdisDbMutationExecutor`（`requireWritable` 早于 `admissionMode`、`reserveNormalPlan` 重读循环、`requireReclamationInvariants`、三处失败收口方法、容量失败不 degrade 的例外）、`EntryTable`、`NativeKeyDirectory`（`nativeBytes()` 恒 0、`WRITE_REHASH_BUDGET`、`removeEntry` 反查、`collapsedReplacement`）、`OpenAddressingTopology`（4 状态）、`HashCapacityPolicy`（三条阈值）、`ExpiresIndex`、`ScanCursorV2` 位段、`YierdisDbKeyLifecycle`（`StagedEntry`、`expireCount` 下溢、`touchRecord` 条件、close 顺序）、`YierdisDbDataMaintenance`（维护顺序与两个预算常量）、`YierdisDbExpirationSupport`（`CLEANUP_MAX_CANDIDATES = 20`）、`YierdisDbMaxmemorySupport`（`maxAttempts` 表达式、过期候选 `lruClock = 0`）、`YierdisDbMemoryEstimator` 与 `DbMemoryConstants` 常量、`YierdisInstance.create` 的 PER_DB/GLOBAL 装配、`NettyServerInfoProvider` 与 `KeyCommands` 的字段映射、`reconcileAccounting` 的全仓调用点。

**仍只做了抽查或依赖既有专题文档**

以下内容的**完整方法级细节**未逐行重读，相关结论来自既有专题文档与并行审计，引用时应保留怀疑：

- `YierdisDbKeyLifecycle` 的 publish/replace/delete 与 detach 全流程；
- `PreparedEntryMutation`、`AbstractPreparedMutation`、`PreparedBatchMutation` 的完整状态机；
- `YierdisTtlOps`、`YierdisKeyspaceOps`、`YierdisStringOps` 的中段方法；
- `YierdisGlobalMaxmemoryGovernor` 的 victim 挑选细节（`nextLruClock` 与 stalled 计数已核对）。

L6–L9 的**机制框架**由源码确认，但其中精确常量与分支顺序如需用作改动依据，建议先按 [`db-behavior-gaps.md`](./db-behavior-gaps.md) 的方式再回源一次。

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