# DB 内部结构

单个 `YierdisDb` 并非一张并发 `Map<byte[], Object>`；它受 owner thread 约束，是掌管 key、entry、value、TTL、mutation、maxmemory 与生命周期的状态 owner。

设计意图、层间契约、与 Redis C 实现的对照以及已知取舍见 [`db-design-analysis.md`](./db-design-analysis.md)；运行期行为缺口与可疑观察见 [`db-behavior-gaps.md`](./db-behavior-gaps.md)。

本文按"这一层是什么 → 谁调用谁 → 改的时候不能破什么"三段式组织：§1–§5 是结构与所有权，§6–§7 是读/写两条完整调用序列，§8–§11 是各子系统的接入点，§12 是改动前的自检清单。

## 1. 这一层在做什么

`YierdisDb` 的职责可以用一句话界定：**它是 graph 的唯一 mutator，也是内存账本的唯一写入者**。由此派生出三条硬约束，几乎所有设计都是它们的推论：

1. **所有 graph 变更都走 `YierdisDbKernel.execute(MutationPlan)`**——包括 lazy expire、active expire、eviction、`FLUSHDB`、rehash 扩容。没有第二条写路径。
2. **写之前先过量本账**：增长型写入先 reserve upper bound，prepare 后按实测收窄，commit 后才结算。
3. **失败以 `prepared.commit()` 开始为界**：之前回滚，之后 degrade + result-unknown。

## 2. 组合边界

command 层只依赖 `DbEngine`，后者直接暴露合并读写后的 `StringOps`、`HashOps`、`ListOps`、`SetOps`、`ZSetOps`、`HllOps`、`KeyspaceOps` 和 `TtlOps`，以及 memory/lifecycle 方法。runtime 使用 `YierdisDbEngineFactory` 创建 `YierdisDb`。

```text
YierdisInstance
  -> YierdisDbEngineFactory.create(DbEngineConfig)
     -> StableMemoryBackendFactory.create("db-N", ...)
     -> YierdisDb.create(...)
        -> YierdisDbStorage
        -> YierdisDb
```

`DbEngineConfig` 是 DB 配置的唯一输入（`dbIndex` / `maxmemoryBytes` / `maxmemoryPolicy` / `maxmemorySamples` / `evictionTimeLimitMillis` / `expireCleanupTimeLimitMillis` / `defrag`）。`YierdisDbEngineFactory` 持有一个 `HashSeed.random()` 播种的哈希种子，同一次装配中的所有 DB 共用一个 factory、各自独立 backend。

`YierdisDb` 在私有构造器内按顺序组装 ledger → mutation executor → DB kernel → memory context → expiration support → 各 family ops → memory reporter → maxmemory support → maintenance。构造失败的清理路径由一个"backend ownership 是否已转移"的标志决定：未转移则关闭 backend，已转移则交给 storage 的 ownership 路径。

`YierdisDbStorage` 只记录 `hashTableMaintenanceRegistry` 与 `keyLifecycle`，并在 `create` 里按固定顺序构造：

```text
hashTableMaintenanceRegistry
  -> EntryTable
  -> NativeKeyDirectory
  -> StringRoot -> ListRoot -> HashRoot -> SetRoot -> ZSetRoot
  -> KeyLifecycle
```

中途失败走 `closePartiallyConstructed(...)`，已建成的部分按同一 ownership 路径回收。storage 一创建就接管 backend。构造失败与正常 shutdown 都沿 key lifecycle 的同一 ownership 路径清理：原始失败保持为 primary，清理失败附加为 suppressed。

`YierdisDbRuntimeState` 保存 `dbIndex`、线程守卫、`lruEnabled`、`NativeDefragOptions`、maxmemory 协调器、`MaxmemoryParticipant` 和本地 LRU clock（`nextLruClock()` 在没有 coordinator 时本地自增）。`defragMaintenance` 把 options 交给调用方，不在这里保存 defrag 报告，也不持有 storage backend。

global/per-db maxmemory 只改变预算协调方式：每个 DB 始终有独立的 stable-memory backend/runtime、keyspace、entry table、roots 和 ledger。

## 3. Storage graph 与所有权

核心对象图：

```text
NativeKeyDirectory
  key bytes -> EntryHandle

EntryTable
  EntryHandle -> ENTRY_RECORD

EntryRecord
  keyHandle + valueHandle + type + encoding + flags
  + expireAtMillis + version + lruOrLfu

Type roots
  ValueHandle -> string/list/hash/set/zset payload
```

**所有权归属表**（每层的"谁负责释放谁"）：

| 层 | 拥有什么 | 不拥有什么 |
|---|---|---|
| `NativeKeyDirectory` | allocator-backed `KEY_BYTES`（key 字节本体） | 槽位数组在 heap；不释放 entry 或任何 payload；不理解 value 类型 |
| `EntryTable` | `ENTRY_RECORD` 这块 native 内存 | 不解释 handle 指向的对象，不释放 payload |
| type roots / adapters | `ValueHandle` 指向的 string bytes / root record / node / payload | 不拥有 key 与 entry |
| `YierdisDbKeyLifecycle` | directory + entry table + 各 root 的**派生状态与删除顺序** | 不拥有 backend 本身 |
| `YierdisFfmStableMemoryBackend` | page/span/segment 与 object table | 不知道任何 DB 语义 |

`NativeKeyDirectory` 保存 allocator-backed `KEY_BYTES`，把 key 映射到 `EntryHandle`，并实现 lookup、insert/remove、random candidate、cursor scan 和 table maintenance。按 entry 删除（`removeEntry`）用 entry record 持有的 key handle 与 dict hash 反向探测槽位，O(probe) 定位，不做全表扫描；位置在删除时现查，rehash 两表状态下也不会指向 stale slot。

目录的槽位数组（`byte[] states`、`int[] hashes`、key/entry handle 引用）在 heap，只有 key 字节本体是 allocator-backed；所以 `NativeKeyDirectory.nativeBytes()` 恒为 `0L`，"keyspace 在 native" 指的是 key 字节而非 hash slot。删除除了按 key 字节探测，还有"按 entry 持有的 key 句柄 + dict hash 反查"这条路（`removeEntry(keyHandle, keyHash, expectedHandle)`）。

`OpenAddressingTopology` 集中表达 slot state、linear probing、tombstone 复用和 active/old 增量 rehash，不持有 key/value 数组、native handle 或任何 payload ownership。`NativeByteMap` 与 `NativeKeyDirectory` 都把生产 topology 委托给该核心，只保留 payload arrays 与 ownership/lifecycle logic。槽状态有四种：`EMPTY` / `FILLED` / `TOMBSTONE` / `MIGRATED_SCAN_SHADOW`——最后一种是给 SCAN 正确性专设的：迁移后 old 表仍保留 key 用于定位与重放。

写路径的 rehash 预算是 `WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE)`，即每次写顺带推进 2 个 slot；维护节拍另有 64 slot 预算。容量策略由 `HashCapacityPolicy` 决定：`filled > capacity - capacity/4` 则 grow；`tombstones > max(size, capacity / 8)` 则 compact；`capacity > 16 && size < capacity / 8` 则 shrink；容量恒为 2 的幂，范围 `[16, 1 << 30]`。

rehash 期间如果新插入会顶穿合并占用，目录 API 会同步坍缩成一张装得下全部存活 entry 的 standalone 表（`collapsedReplacement`），显式避免"no insertion slot"。

`EntryTable` 把每个 `EntryRecord` 编码进 72-byte `ENTRY_RECORD`。key/value handle 各占 16 bytes，显式保存 `allocatorId` 与 `localRaw`；其后字段依次为 keyHash（4 B）、type（4 B）、encoding（4 B）、flags（4 B）、expireAtMillis（8 B，`-1` 表示无 TTL）、version（8 B）、lruOrLfu（8 B）。字段偏移常量与 `NativeStorageLayout.ENTRY_RECORD_BYTES = 72` 同源。

公共 `NativeHandle` 是 `(allocatorId, localRaw)` 的 paired stable identity，既非 physical address，也不是一个全局 packed long。只有 FFM backend 私有的 `localRaw` 由 `YierdisLocalHandleCodec` 编码 slot/generation/kind/domain；DB 边界不能丢掉 `allocatorId`。

`EntryRecord.version` 在语义 mutation（新 record、TTL 或 flags 变化）时递增；纯 access-clock touch 保留原 version（`touchRecord` 只有在 `nextClock != record.lruOrLfu()` 且当前 record 与预期一致时才 replaceEntry，从而保留 version）。prepared mutation 和 active expiration 用它与 handle、deadline、source state 一起识别 stale candidate；entry accounting 由 `YierdisDbMemoryEstimator` 计算（`entryMetadataBytes` = `ENTRY_OVERHEAD_BYTES_ESTIMATE(64)` + `STRING_INT` 时额外 8 B），不存放在 version 字段中。

`ValueHandle` 同样包装完整 `NativeHandle`。string 指向 `STRING_BYTES`；collection 指向对应 root record，再由 root/adapter 持有 packed block、node 或 hash topology。DB graph 只保存 stable identity，realloc/defrag 的 page、offset 和 location 发布属于 memory backend。**注意 `StringRoot.encoding()` 恒返回 `STRING_RAW`**——`STRING_INT` 只是 entry 上的标签。

adapter 内部并非全部 off-heap：`SET_INTSET` 是 heap 的 `short[]`/`int[]`/`long[]`，ZSET `packed` 的 score 在 heap `double[]`（native listpack 只存 member）。这些 heap 部分由 adapter 的 `heapEstimatedBytes()` 计入 `componentRetainedHeapBytes()`。

## 4. Key lifecycle

directory、entry table、type roots 和派生状态的所有权都归 `YierdisDbKeyLifecycle`。主要职责是：

- 解析 `KeyHandle` / `EntryHandle` / `EntryRecord`；
- 为新 key staged insert 分配 key 与 entry；
- 发布、替换和释放 entry；
- 按 value type 释放 payload/root；
- 更新 TTL 派生计数、等待物理删除计数和 LRU/LFU clock；
- 提供 bounded key-directory scan。

新 key 只能通过 opaque `StagedEntry` token 预留 entry 与 native key（`EntryTable.reserve()` + `NativeKeyDirectory.stageInsert()`）。abort 或未发布时关闭 token 会幂等释放两者（`StagedInsert.close()` 幂等）；发布后 token 被消费，prepared mutation 只调用 lifecycle 的 publish/replace/delete 语义，不再持有 directory staging 类型。

ops 不直接组合 directory 与 entry table，也不能从 lifecycle 取出 backend、table、directory 或 roots。各 family root 只在 DB 组合时注入对应 family ops。删除必须让 directory entry、entry record、value/root 和 key allocation 一起收敛；替换则必须在 source identity 仍匹配时才发布。需要验证 raw graph 的底层测试把反射夹具留在 `src/test`，生产代码不提供 inspection view。

`EntryRecord.expireAtMillis` 是唯一 TTL deadline。`expireCount` 只是随 entry publish/replace/release 更新的派生计数，不是独立索引；`reconcileDerivedEntryState` 里一旦发现下溢就抛 `IllegalStateException("derived expire count underflow")`——注意这个异常发生在 commit 阶段，会被升级成 degraded（见 [`db-behavior-gaps.md`](./db-behavior-gaps.md) 的 A2）。

`ExpiresIndex` 是 owner 线程独占的 `PriorityQueue`，按 `(expireAtMillis, sequence)` 排序，无同步、允许 stale 项、不计任何内存账。lifecycle 只在 deadline 真的变化且 keyHandle 非 null 时才 `add`。

**close 顺序**（`OwnedResources.close`）：`clearDataFailure` → `entryTable` → `keyDirectory` → `stringRoot` → `listRoot` → `hashRoot` → `setRoot` → `zsetRoot` → `stableMemoryBackend`。先释放指向 payload 的索引，最后才关 backend——反过来的话，directory 里的 handle 会指向已关闭的 backend。

## 5. Runtime kernel 与 facade

`YierdisDbKernel` 是 package-private 深模块。普通读取由 concrete ops 在 owner 检查后直接执行，mutation 通过 `execute(MutationPlan)` 进入同一个 executor。concrete ops、memory context、key lifecycle、entry mutation、TTL driver、active-expiration driver、memory reporter 和 maxmemory participant 都保持 package-private，handle、entry record、backend 与 ledger 不进入公开 DB interface。

`YierdisDbMutationExecutor.MutationPlan` 直接声明 upper bound（`upperBoundBytes()`）、`AdmissionMode`（默认 `NORMAL`）和无参 `prepare()`，并返回 `PreparedDbMutation`。family 通过 kernel 的 unchanged/insert/replace/delete/upsert/callback/batch 方法构造 prepared mutation；批量组合由 `PreparedBatchMutation` 提交、释放或中止子 mutation。`YierdisDbKernel.reclaim(...)` 固定用 `upperBound = 0` + `AdmissionMode.RECLAMATION`。`YierdisDbMemoryContext` 继续封装 allocation 估算、epoch、native slice、allocator stats 和 page trim，但不是可见扩展点。

`ListValue.PreparedMutation` 按操作变体分为 unchanged、packed replacement、packed-to-quicklist、quicklist push 和 quicklist pop。每个变体只保存自己拥有的 replacement、node 或 superseded 资源，并分别实现 source 校验、提交、superseded 释放和放弃清理；公共生命周期只做状态保护与变体调度，避免通过 nullable 资源组合推断操作类型。

`NativeByteMap.PreparedMutation` 把每个 source 显式记录为 active、old 或 absent，并为 present source 单独保存非负 slot index；inspect、validate、commit 和 abort cleanup 共用该位置模型，不再用整数符号区分 table。

`CommandSupport.commandDb(session)` 直接返回路由选中的 `DbEngine`；命令通过 typed ops 或 `memoryUsage(...)`、`memoryStats()`、`objectEncoding(...)`、`flushDb()` 调用。prepared set/pop 使用无参数 `commit()`；lazy expiry、active expiry 和 eviction 也进入同一个 mutation executor。

## 6. 读路径

常规读路径的完整序列：

```text
DbEngine.strings()/hashes()/lists()/sets()/zsets()/hll()/keyspace()/ttl()
  -> Yierdis*Ops（参数检查）
  -> YierdisDbKernel.checkOwner()
  -> YierdisDbKernel.liveEntryRecord(key)
       -> keyDirectory.lookup(key)            // 未命中 -> null，读路径结束
       -> entryTable.read(entryHandle)
       -> expireAtMillis 判定
            live      -> 直接返回
            expired   -> reclaimExpired(...)  // 完整 mutation：删 graph + 结算 ledger
       -> 返回 live EntryRecord
  -> type/encoding check                    // 类型不符 -> 按命令语义返回空/错误
  -> EntryRecord.valueHandle()
  -> type root read
       string -> 直接读 STRING_BYTES
       collection -> root adapter 读 packed / quicklist / HT topology
  -> 复制或返回有界 view
```

要点：

- `liveEntryRecord(...)` 比较 `expireAtMillis`。live record 正常返回；过期 record 触发 `reclaimExpired(...)` 并对调用方隐藏。reclamation 是完整 mutation，会删除 graph 并结算 ledger。
- 只有成功取得 live record 的 LRU 路径才 touch clock。
- 普通查询的参数检查、live-entry 解析和结果视图构造都在同一次调用里完成，且位于 owner 检查之后。
- prepared mutation 会跨越一次调用的生命周期，所以在创建、状态检查与提交入口使用 `YierdisDbKernel.checkOwner()`；scan/result view 则按各自契约持有 epoch 或结果资源。实际变更仍只能通过 `MutationPlan` 进入 executor。
- 需要拥有结果的 API 会复制 bytes；callback-scoped streaming 可以使用短生命周期 native view。
- `SCAN` 的 `KeyWindow` 先在 bounded epoch 内 discovery，再按同一 cursor/window 同步 replay 到 sink；window close 后 epoch 才释放，**不能让 slice 或 view 逃逸**。cursor 编码为 `position[31:0] | phase[33:32] | generation[62:34]`（`ScanCursorV2`：`POSITION_BITS = 32`、`PHASE_BITS = 2`、`GENERATION_BITS = 29`）；generation 不匹配或 phase 非法时从 active 表重启，允许重复但绝不因客户端乱填 cursor 抛错。

## 7. 写路径

增长型写入不能先改 graph 再检查 maxmemory。标准路径：

```text
estimate upper bound
  -> YierdisDbKernel.execute(MutationPlan)
     -> YierdisDbMutationExecutor.execute(plan)
        -> threadChecker.run()
        -> health.requireWritable()                  // 早于 admissionMode
        -> reservation
             RECLAMATION -> ledger.beginReclamation()
             NORMAL      -> reserveNormalPlan(upperBound)   // 含重读准入循环
        -> stableMemoryBackend.beginAllocationScope()
        -> plan.prepare()                            // 可失败：native 分配、拓扑替换、source 校验
        -> ledger.reconcile(measured peak)           // 实测 native growth + staged non-native growth
        -> requireLedgerDeltaInvariant(...)
        -> commitStarted = true
        -> prepared.commit()
        -> allocationScope.promote()
        -> ledger.commit(actualDelta)
        -> prepared.releaseSuperseded()
        -> optional trimEmptyPages
```

可能失败的 native allocation、replacement topology 和 source validation 都发生在 prepare。family 通过 kernel 工厂产生 `PreparedEntryMutation` 或其他 `PreparedDbMutation`，用 unchanged/insert/replace/delete/upsert/callback/batch 表达转换，并按 value representation 附加 abort、before-publish 或 superseded-release hook。

`reserveNormalPlan` 的重读准入循环是必要的：admission 被拒后重算 upper bound，变小就用更小值重试，变大则 rollback 后按更大值重来。没有这个循环，第一次估算偏差就会变成一次假 OOM。

失败以 `prepared.commit()` 开始为界（三处收口方法：`abortBeforeCommit` / `settleAfterCommit` / `postCommitFailure`）：

- **commit 前失败**：`abortBeforeCommit` 依次 abort prepared resources、abort allocation scope、rollback ledger reservation；旧 graph 保持可见。其中容量类失败（`MemoryLedgerOutOfMemoryException` / `NativeCapacityExceededException`）直接转成 `YierdisCommandException(MaxmemoryErrors.OOM_ERR)`，**不**标记 degraded。
- **commit 后失败**：不再宣称 mutation 未发生。`settleAfterCommit` best-effort promote/settle/release，`postCommitFailure` 调 `recordInvariantFailure(...)` 标记 DB degraded，并抛 `PostCommitMutationException`（result-unknown）。
- 是否 degrade 由 `isDegradingInvariantFailure` 判定：`NativeMemoryException` 与 `IllegalStateException` 会 degrade，容量失败不会。

upper bound 覆盖新 key/entry/root、native payload、allocator metadata、allocation-scope bookkeeping、heap topology 和编码升级。prepare 后用实测 native growth 加 staged non-native growth 收窄 reservation；`actualDeltaBytes` 只表示提交后的逻辑增量。是否真正回收 committed page 必须以 trim result 和重新采样的 physical snapshot 为准。

## 8. TTL 与主动清理

`YierdisTtlOps` 用 prepared entry replacement/delete 实现 TTL 命令。设置 deadline 复用原 entry handle；`PERSIST` 把 deadline 改为 `-1`；已经到期的输入直接准备删除。条件判定（NX/XX/GT/LT）在删除分支之前求值。

`YierdisDbExpirationSupport.cleanupExpired(...)` 只消费 `ExpiresIndex` 队首，不扫描 keyspace slot：

- `CLEANUP_MAX_CANDIDATES = 20`，单次最多回收 20 个过期 key，并受时间预算限制；
- stale 候选丢弃**不占名额**；
- 每个候选先按 key identity 和真实 `expireAtMillis` 做惰性校验（key 仍在目录、entry 仍持有同一 key identity、deadline 与索引项一致），再走 `YierdisDbKernel.reclaimExpired(...)`；
- **只有回收成功才 `dropExpiresIndexHead()`**，删除失败则保留队首下轮重试。

详细的 retry、rehash dedup 和 commit failure 语义见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## 9. Maxmemory 与 ledger

`YierdisDbMemoryLedger` 维护：

- `usedBytes`：按 committed mutation `actualDeltaBytes` 更新的逻辑账本；
- `reservedBytes`：admission 已通过但尚未 settle 的预算窗口；
- `effectiveUsedBytes() = usedBytes + reservedBytes`。

`reserve` 在挂了 coordinator 时会先 `coordinator.prepareWrite(...)`；`reconcile` 发现超额抛 `IllegalStateException`；`realignUsage(...)` 用于对账时把逻辑账本拉回物理值。

enforcement 使用本 DB 独占的 `MemoryUsageSnapshot`：

```text
heap estimated
  + native metadata committed
  + native data committed
```

写 admission 不内联跑 expires 索引清理。per-db scope 的本地 enforce 按 `maxmemoryBytes - estimatedExtraBytes` trim/resample/evict。global scope 把跨 DB 预算交给 `YierdisGlobalMaxmemoryGovernor`，由后者汇总 snapshots、挑选 victim，并提供全局单调 LRU clock。维护节拍里，每个 DB 的 `runMaintenance()` 仍会先排空到期 key，再做本地 enforce；global governor 的 maintenance 在 DB 循环之后。各 DB backend runtime counter 只用于 lifecycle 诊断，不作为第二套 global usage source。

`noeviction` 不选 victim；`allkeys-random` 随机取候选；`allkeys-lru` 比较 `EntryRecord.lruOrLfu()`。candidate selection 不跳过过期 key（抽到或扫描到即作为最优候选，过期候选以 `lruClock = 0` 上报，live key 的访问时钟恒 ≥ 1），过期候选先走 expiration reclamation，真正 victim 通过 `YierdisDbKernel.evict(...)` 删除。

ledger 逻辑账本与 admission 的物理重算是两套账。`ledger.usedBytes` 与物理用量之间的漂移是静默的，不会自动触发 invariant failure。degraded 来自 `YierdisDbHealth.recordInvariantFailure(...)`（只保留首个失败）和 commit 开始后的失败，写入被 `MISCONF_DEGRADED = "MISCONF DB is in a degraded state; writes are disabled"` 拒绝。

`RuntimeDbEngine.reconcileAccounting()` 是唯一的显式恢复入口：在 owner thread 上重算物理用量、用 `realignUsage` 把逻辑账本对齐到物理值、清除 degraded 并恢复写入；每次尝试与结果（成功/失败/修正量）记入 `DbHealthSnapshot.lastReconciliation`（`recordReconciliation` 成功时才清 degraded）。快照的失败字段只描述当前未恢复的 episode，对账成功后随之关闭，下一场事故重新入账。恢复不会自动发生，持续性记账 bug 仍以事故形式暴露。

更完整的 admission、OOM 和 result-unknown 边界见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 10. Memory 与 introspection

`YierdisDb` 直接读取 entry encoding，`YierdisDbMemoryReporter` 则聚合 key lifecycle、ledger 和 native allocator 状态。`MEMORY USAGE` / `MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph。

主要口径包括：

- owned physical snapshot 与 backend allocator stats；
- `ledger_used_bytes` 是 `heapDataBytesEstimate`，不是 ledger 逻辑 `usedBytes`；`ledger_reserved_bytes` 才是 ledger `reservedBytes`。这条绑定在 `KeyCommands` 里就是 `memoryStat("ledger_used_bytes", YierdisMemoryStats::heapDataBytesEstimate)`；
- type root estimates 与 `componentRetainedHeapBytes`；
- key count 和 derived expire count；
- `usedBytesForMaxmemory = heap + native metadata committed + native data committed`；
- `effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + reservedBytes`。

server 侧的 `used_memory` 是 `heapDataBytesEstimate + offHeapUsedBytes`；`yierdis_ledger_used_bytes` 同样绑到 `heapDataBytesEstimate`；`yierdis_maxmemory_per_db_bytes = maxmemoryBytes / max(1, databases)`（整数除法，与实际"余数 +1"的分配可能差 1 字节）。

native reclaimable bytes 只是候选量，不能预先从 committed footprint 扣除。显式 introspection 需要返回 owned bytes/result object 时会 materialize heap copy。

## 11. Owner 与 shutdown

`DbThreadGuard` 强制单 owner 语义：未绑定访问、foreign-thread 访问/关闭、CLOSING 和 CLOSED 状态都 fail fast；同 owner 重复 bind/close 保持既定契约。状态机是 `OPEN → CLOSING → CLOSED`。

Netty I/O 线程只提交请求，command executor owner thread 执行 DB mutation 和 maintenance。`Arena.ofShared()` 只允许 FFM region 在不同线程关闭，不解除 DB thread confinement。

shutdown 的固定顺序：`resetUsage` → `reclaimAllDetachedEntries` → `kernel.close()`，随后 key lifecycle 按 §4 的 close 顺序清空 graph 并关闭 backend。lifecycle 保证 close-once、固定依赖顺序和失败聚合；构造失败也沿同一 ownership 路径清理已创建的 graph（`closePartiallyConstructed`）。

## 12. 改动时最容易踩的边界

按"改了哪里 → 必须同时确认什么"整理：

| 想改的东西 | 首要入口 | 不能破的东西 |
|---|---|---|
| 新增/修改一条写命令 | 对应 `Yierdis*Ops` + kernel 的 mutation 工厂 | 不能绕过 `execute(MutationPlan)`；不能先改 graph 再记账；upper bound 要覆盖新增的 native + heap 增长 |
| 改 `ENTRY_RECORD` 布局 | `EntryTable` + `NativeStorageLayout` | 72 B 与字段偏移是同源常量，改一处就必须同步另一处；`version` 语义不能被挪作他用 |
| 改 keyspace 索引 | `NativeKeyDirectory` + `OpenAddressingTopology` + `HashCapacityPolicy` | 探测/墓碑/shadow 语义共享给 HASH/SET 的 HT 编码；`removeEntry` 依赖 entry 里的 key handle 反查 |
| 改删除/替换语义 | `YierdisDbKeyLifecycle` | 必须让 directory entry、entry record、value/root、key allocation **一起收敛**；替换要校验 source identity 仍匹配；`expireCount` 是派生值不是索引 |
| 改 TTL | `YierdisTtlOps` + `YierdisDbExpirationSupport` | `expireAtMillis` 是唯一 deadline；只有 deadline 真变化才写 `ExpiresIndex`；回收成功才出队 |
| 改 maxmemory | `YierdisDbMaxmemorySupport` / `YierdisGlobalMaxmemoryGovernor` / ledger | `effectiveUsedBytes` 是报告口径，admission 比较的是不含在途 reservation 的值；per-db 与 global 只差预算判定 |
| 改 native 对象布局 | `YierdisNativePageAllocator` / `YierdisNativeObjectTable` | 见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) §9 的不变量清单 |

通用红线：

- command 不依赖具体 `YierdisDb`；ops 不绕过 mutation executor 做增长型写入；
- 删除不绕过 lifecycle；DB 不缓存 physical location 或长生命周期 `NativeObjectView`；
- 任何"把失败当成已回滚"的假设在 commit 开始后都不成立——那里只能 degrade + result-unknown。

## 13. 修改导航

- DB 组装：`YierdisDbEngineFactory`、`YierdisDb`、`YierdisDbStorage`。
- 内部执行入口：`YierdisDbKernel`、`YierdisDbMutationExecutor.MutationPlan`；实现细节：`YierdisDbMemoryContext`、`YierdisDbKeyLifecycle`。
- key/entry/value 生命周期：`YierdisDbKeyLifecycle`、`EntryTable`、type roots。
- mutation 与预算：`MutationPlan`、`YierdisDbKernel`、`PreparedDbMutation`、`PreparedBatchMutation`、`YierdisDbMutationExecutor`、`YierdisDbMemoryLedger`。
- TTL：`YierdisTtlOps`、`YierdisDbExpirationSupport`、`YierdisDbKernel.reclaimExpired(...)`。
- maxmemory：`YierdisDbMaxmemorySupport`、`YierdisGlobalMaxmemoryGovernor`。
- 维护与自省：`YierdisDbDataMaintenance`、`YierdisDbHealth`、`YierdisDbMemoryReporter`。
- memory backend：`YierdisFfmStableMemoryBackend` 与 [`native-memory-runtime.md`](./native-memory-runtime.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。