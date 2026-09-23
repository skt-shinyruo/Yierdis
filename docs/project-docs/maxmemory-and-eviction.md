# Maxmemory 与淘汰

ledger reservation、per-DB / global scope、eviction policy 和 OOM 路径都遵循同一条原则：先证明本次写入有空间，再让 mutation 执行。cleanup 和 eviction 只把 usage 拉回目标线，不替代 mutation commit。

## 两套账：逻辑账本与物理快照

maxmemory 相关的一切数字都来自两个不同来源，混用它们是最常见的误读。

**逻辑账本 `YierdisDbMemoryLedger`** 维护两个数：

- `usedBytes`：按已提交 mutation 的 `actualDeltaBytes` 增减的逻辑账本，**不是** allocator / JVM 的实时物理占用；
- `reservedBytes`：预算已通过、但 mutation 还没 commit/rollback 的那段窗口。

`usedBytes` 的用途很窄：`prepareFlushDb` 用它算 flush 的 committed delta，`reconcileAccounting` 用它对比物理重算值。它不出现在任何对外字段里。

**物理快照 `MemoryUsageSnapshot`** 由每个 DB 报告的 owned snapshot 构成，`effectiveBytesForMaxmemory()` 固定为：

```text
usedBytesForMaxmemory
  = heapEstimatedBytes
  + nativeMetadataCommittedBytes
  + nativeDataCommittedBytes
```

entry 中的 TTL 字段和 collection topology 已进入 owned snapshot，不能再按带 TTL 的 key 数量重复加一遍。`nativeDataLiveBytes` 和 `nativeReclaimableBytes` 是诊断维度，不从 committed footprint 中扣除。`MemoryUsageSnapshot.addSaturating` 是全仓 main 源唯一的饱和加法：任一操作数为负或求和溢出都收敛到 `Long.MAX_VALUE`，绝不回绕。

`MEMORY STATS` 的字段名与两套账的对应关系（`YierdisDbMemoryReporter.memoryStats()` → `YierdisMemoryStats`，命令层映射见 `KeyCommands.MEMORY_STATS_FIELDS`）：

| `MEMORY STATS` 字段 | `YierdisMemoryStats` 字段 | 含义 |
|---|---|---|
| `maxmemory_bytes` | `maxmemoryBytes` | 本 DB 的预算上限 |
| `used_bytes_for_maxmemory` | `usedBytesForMaxmemory` | 上面的物理快照（= `effectiveBytesForMaxmemory()`） |
| `effective_used_bytes_for_maxmemory` | `effectiveUsedBytesForMaxmemory` | 物理快照 + ledger `reservedBytes` |
| `ledger_used_bytes` | `heapDataBytesEstimate` | **堆估算**，不是 ledger 逻辑 `usedBytes`（历史命名包袱） |
| `ledger_reserved_bytes` | `reservedBytes` | ledger `reservedBytes` |
| `offheap_used_bytes` | `offHeapUsedBytes` | native metadata committed + native data committed |

注意两个"effective"别混淆：`MemoryUsageSnapshot.effectiveBytesForMaxmemory()` 是物理快照本身；`MEMORY STATS` 的 `effective_used_bytes_for_maxmemory` 是在它之上再加 ledger `reservedBytes`。**admission 实际比较的是不含 reservation 的物理快照**，`effective` 只是报告值。

enforcement 不把 ledger `usedBytes`、native counter 和 TTL estimate 再拼成一套数字：每个 DB 直接报告 owned snapshot，采样口径固定为上面的四项之和。

## `YierdisDbMutationExecutor` 为什么先 reserve 再 prepare

标准写路径：

```text
estimate upper bound
  -> YierdisDbKernel.execute(MutationPlan)
  -> YierdisDbMutationExecutor.execute(plan)
     -> ledger.reserve(upperBound)              // 或 beginReclamation()
     -> stableMemoryBackend.beginAllocationScope()
     -> plan.prepare()
     -> ledger.reconcile(preparedPeak)          // 或 reclamation invariants
     -> prepared.commit()
     -> allocationScope.promote()
     -> ledger.commit(actualDelta)
     -> prepared.releaseSuperseded()
     -> optional native page trim
```

这样做是因为 DB mutation 经常需要"先分配、后知道实际变化量"：

- 新 key 可能新增 key bytes、entry record 和 value payload；
- TTL deadline 更新会产生 mutation-scope bookkeeping，但没有独立的 TTL allocation；
- collection 或 string 可能触发编码升级；
- 覆盖写可能最终是 shrink、no-op 或负 delta。

三段数字各司其职：

- `upperBoundBytes()` 解决"能不能先让这次写动起来"；
- prepare 后实测的 native growth 加 staged heap topology（`prepared.stagedNonNativeGrowthBytes()`）用来 `reconcile` 收窄 reservation；`reconcile` 要求 `requiredBytes <= reserved`，超出即抛 invariant failure；
- `actualDeltaBytes()` 给出"最后到底长了多少 / 缩了多少"，由 `ledger.commit` 落账。

`reserveNormalPlan` 还有个重试环：`reserve` 抛 OOM 时再算一次 refined upper bound，若确实变小就重试（部分 mutation 的 upper bound 依赖 prepare 之前的估算，读取真实 key 后可能下降）。因此在拒绝之前，执行器已经尽量给了重新估算的机会。

提交后的顺序固定为 allocation promote、ledger settle、release superseded，最后才按 `PreparedDbMutation.shouldTrimNativePagesAfterCommit()`（默认 `actualDeltaBytes() < 0`）尝试 trim；`ledger.maxmemoryEnabled()` 为 false 时连 trim 都不做。

### reservation 先于 mutation，回滚不污染下一次

`MutationExecutorReservationTest` 覆盖两个关键点：

- 预算不过关时，`prepare()` 根本不会执行；
- commit 前 prepare / 校验失败时，prepared resources、allocation scope 和 ledger reservation 都会 abort / rollback，不会污染下一次写入。

### commit 之后的失败：degraded 与 result-unknown

`prepared.commit()` 开始之后，就再没有"确认未生效"这种安全回滚前提。此后的异常触发 post-commit settle：executor best-effort promote allocation、settle ledger、release superseded resources，DB 转入 degraded，调用方收到 result-unknown（`PostCommitMutationException`）。

- capacity 类异常（`MemoryLedgerOutOfMemoryException` / `NativeCapacityExceededException`）在 commit 前被拦下 → 稳定映射成 Redis 风格 OOM；commit 后被包成 invariant failure，不会伪装成"确定未执行"。
- 其他 `RuntimeException` / `Error` 若属于 `NativeMemoryException` 或 `IllegalStateException`（`isDegradingInvariantFailure`），commit 前也会 `health.recordInvariantFailure` 后转 degraded。

degraded 不是终态：`RuntimeDbEngine.reconcileAccounting()` 在 owner thread 上重算物理用量、把 ledger 漂移修正入账（`YierdisDbMemoryLedger.realignUsage`）并清除 degraded，尝试与结果记入 `DbHealthSnapshot.lastReconciliation`。恢复只能显式触发，maintenance tick 不会自动对账。

## per-DB scope 的判断顺序

没有全局 coordinator 时，本地 maxmemory 语义以 `YierdisDbMemoryLedger.enforceLocalLimit(estimatedExtraBytes)` 的判断顺序为准：

1. `limitBytes <= 0` → 直接返回（maxmemory 关闭）。
2. `estimatedExtraBytes > limitBytes` → 直接 OOM。
3. 目标线 `limit = max(0, limitBytes - estimatedExtraBytes)`；owned physical snapshot 不超过 `limit` 就通过。
4. 超过则调用 `YierdisDbMaxmemorySupport.evictUntilUnder(limit)`。该入口**先 trim empty native pages，再重新采样 snapshot**。
5. 淘汰结束后再次采样：仍超限且 `estimatedExtraBytes > 0` → OOM；`estimatedExtraBytes == 0`（维护型 / 缩容型）放行。
6. 只有通过这些检查后，`reserve` 才增加 `reservedBytes`；`estimatedExtraBytes == 0` 返回 `NoopReservation`。

`enforceLocalMaintenance()` 就是 `enforceLocalLimit(0)`，所以纯维护路径复用同一套判断口径，不另起一套。

这解释了几个容易混淆的现象：

- 覆盖写如果最终缩小 value，可以在"已经顶到 maxmemory"时成功（`estimatedExtraBytes == 0`，淘汰后仍超限也放行）。
- `usedBytesForMaxmemory()` 是 owned physical snapshot 的投影；ledger `usedBytes` 只负责 mutation delta 对账，不能替代拒写采样。

## per-DB 与 global scope 的差异

配置入口是 `YierdisInstanceConfig.MaxmemoryScope`（`PER_DB` / `GLOBAL`，默认 `PER_DB`），在 `YierdisInstance.create(...)` 里被翻译成两种截然不同的预算布局：

- **PER_DB**：实例级 `maxmemoryBytes` 被**均分**给各 DB——`perDbMaxmemory = maxmemoryBytes / databases`，余数（`remainder` 个字节）逐个 +1 分给前面的 DB。每个 DB 拿到的是自己那份 `dbMax`，`YierdisDbMemoryLedger` 用本地 ledger + 本地物理快照做 admission、cleanup、trim 和 eviction。不创建 governor。
- **GLOBAL**：每个 DB 的本地 `maxmemoryBytes` 仍是**整份**实例预算（`dbMax = config.maxmemoryBytes()`），但本地 ledger 不自行计算跨 DB 预算，改为委托 `YierdisGlobalMaxmemoryGovernor.prepareWrite(participant, estimatedExtraBytes)`。governor 由 instance 在 `maxmemoryBytes > 0` 时创建并 `db.attachMaxmemoryCoordinator(governor)`。

两种 scope 都不改变 FFM 所有权（详见 [`native-memory-runtime.md`](./native-memory-runtime.md)）。global scope 仍保留 per-DB backend / runtime ownership，也不把 runtime counter 叠加进 participant snapshots。

## global scope 与 governor 协调

`YierdisGlobalMaxmemoryGovernor.prepareWrite(requester, estimatedExtraBytes)` 是 `synchronized` 的，主线如下：

1. `maxmemoryBytes <= 0` → 直接返回。
2. 按 budget 轮转调用所有 participant 的 `trimMemory(...)`：`trimAllParticipants` 维护 `nextTrimParticipantIndex`，每次 tick 从不同的 participant 起，避免总是先喂同一个 DB。
3. `estimatedExtraBytes > maxmemoryBytes` → OOM。
4. 计算目标线 `limit = max(0, maxmemoryBytes - estimatedExtraBytes)`。
5. 汇总所有 participant 最新的 owned physical snapshots（`globalUsedBytesForMaxmemory()`，饱和相加）；总量不超过 `limit` 时直接通过。
6. `noeviction` 在 trim / resnapshot 后仍超限时：`extra > 0` → OOM；`extra == 0` → 放行（不增长的维护路径可以继续）。
7. 需要淘汰时跨 participant 挑 victim（`pickVictim`）；每次释放后继续 trim 并汇总新 snapshots，直到全局 usage 压回目标线，或在时间 / 尝试 / stalled 预算内停止。
8. 淘汰后再 trim 一次；仍超限且 `extra > 0` → OOM。

governor 的收敛边界由多个预算共同限制：`maxAttempts = max(64, totalKeys * 2)`、时间预算（`evictionTimeLimitNanos`）、以及"连续多少轮释放后总量不下降"的 `maxStalledAttempts = max(1, totalKeys)`。任一触发即停止淘汰，把最终判定交回调用方。

两个跨 DB 约束：

- participant 是每个 DB 暴露出来的 `YierdisDb`（实现 `MaxmemoryParticipant`）；governor 只能通过 SPI（`memoryUsage` / `trimMemory` / `keyCountEstimate` / `sampleCandidate` / `scanBestCandidate` / `evict`）观察和驱动，不直接越过 DB API。
- governor 只相加每个 DB 独占的 `MemoryUsageSnapshot`；各 backend runtime 的 counter 不进入全局 enforcement，它只用于对应 backend 的 region lifecycle 和 native leak 诊断。

maintenance 时的顺序由 `YierdisInstanceRuntimeAccess.maintenanceTick()` 固定：

- 每个 DB 先跑 `runMaintenance()`：回收 detached entry、在时间预算内排空 expires 索引、推进 rehash，然后 `enforceMaxmemory()`（`ledger.enforceLocalMaintenance()`）。global scope 下每个 DB 的本地 `maxmemoryBytes` 仍是整份全局预算，这一步照常执行。
- `defrag` 打开时，每个 DB 在 `runMaintenance()` 之后再跑 `defragMaintenance()`。
- DB 循环结束后调用 `YierdisInstanceResources.enforceGlobalMaxmemoryMaintenance()`。只有 global scope 创建了 governor 时它才会 `enforceMaintenance()`（就是 `prepareWrite(null, 0)`）；per-db scope 下这次调用是空操作。

`GlobalMaxmemoryLruAcrossDbsTest` 覆盖了一个核心语义：DB1 的写入可以在 global scope 下淘汰 DB0 里真正的全局 LRU key。

## eviction policy 的真实分支

**当前只实现了三种策略**：`MaxmemoryPolicy` 枚举只有 `NOEVICTION`、`ALLKEYS_RANDOM`、`ALLKEYS_LRU`。`MaxmemoryPolicy.parse` 接受 `noeviction` / `allkeys-random` / `allkeys-lru`（trim、lower-case、`_`→`-` 归一化），任何其他名字（包括 Redis 的 `volatile-*`、`allkeys-lfu`、`volatile-ttl`）都会抛 `IllegalArgumentException`。所以没有"八种策略的分支"——只有下面三条真实路径。

| 策略 | `pickEvictionKey`（DB 内） | governor `pickVictim`（跨 DB） | 是否选 victim |
|---|---|---|---|
| `noeviction` | 直接返回 `null` | `null` | 否（只 trim，压力仍超限则拒绝增长型写入） |
| `allkeys-random` | `keyLifecycle.randomKeyHandle()` | `sampleAnyCandidate`：随机 participant 起，向它要 candidate | 是 |
| `allkeys-lru` | 见下（样本或全扫） | 见下（样本或 deterministic scan） | 是 |

`allkeys-lru` 的单 DB 选择（`pickEvictionKey`）：

- `samples = max(1, maxmemorySamples)`（默认 5）；
- 若 `samples >= keyCount` 退化为 `pickFullScanVictim`——完整扫描，避免随机抽样在小 keyspace 上错过最旧 key；
- 否则随机抽 `samples` 次，按 `EntryRecord.lruOrLfu()` 取最小；
- 抽样中命中过期 key 直接返回它（见下文）。

`allkeys-lru` 的 governor 选择（`pickVictim`）：

- 若 `samples >= totalKeys`，先试 `scanBestCandidate`（每个 participant 的 `scanBestCandidate` 做全扫），减少测试和小 keyspace 下的随机抖动；
- 否则采样 `samples` 次，取 `lruClock` 最小。

`GlobalMaxmemoryLruAcrossDbsTest` 与 `YierdisGlobalMaxmemoryGovernorTest` 覆盖了 deterministic LRU scan 和采样路径。

## 候选选择为什么不过滤过期 key

这是"淘汰"和"过期"两条删除路径的交汇点。candidate selection **不把过期 key 当作不可回收而跳过**，而是把它上报为**最优候选**：

- `YierdisDbMaxmemorySupport.sampleCandidate`：抽到过期 key（`keyLifecycle.isKeyExpired`）时返回 `new MaxmemoryCandidate(owner, keyHandle, 0L)`——`lruClock = 0`。
- `pickEvictionKey` 的 LRU 采样：命中过期 key 直接返回。
- `pickFullScanVictim`：整表扫描时优先记录第一个过期 key，没有过期 key 才退化为最小 LRU clock 的 live key。

`lruClock = 0` 是关键：live key 的访问时钟恒 `>= 1`，所以过期候选在 LRU 比较中永远排在所有 live key 之前。governor 的 `evictCandidate` → `participant.evict(...)` → `YierdisDbMaxmemorySupport.evict(...)` 对这个候选先调 `kernel.reclaimExpired(key, record, nowMillis)`，成功则返回；只有 live key 才走 `kernel.evict(...)` 真正的 victim 淘汰。

如果反过来"跳过过期 key"，只剩过期条目的 keyspace 会找不到 victim，把本可成功的写入无故卡进 OOM。因此这条规则是正确性要求，不是优化（`writeAdmissionUnderAllkeysLruReclaimsExpiredKeysInsteadOfOom` 等）。

`noeviction` 是唯一例外：它永不选 victim，所以过期占用在写路径上不能被回收，只能等维护节拍或读路径惰性过期；增长型写入仍被拒绝（`writeAdmissionUnderNoevictionStillRejectsWhenOnlyExpiredOccupancyRemains`）。

## enforce 的 trim / resample 顺序

`evictUntilUnderChecked(limitBytes)` 是"把 usage 压回目标线"的唯一实现（本地与 governor 都把最终判定放在外面），它**从不抛异常**，只做 best-effort，顺序固定为：

```text
limit = max(0, requested)
trimEmptyNativePages()                        // ① 先回收空 native page
if used <= limit: return                      // ② 重新采样
loop (attempts < max(64, keyCount*2) && now < deadline):
    victim = pickEvictionKey(now)             // ③ 选候选
    if victim == null: break                  //    noeviction 或 keyspace 空
    if reclaimExpired(victim, record, now):   // ④ 过期候选先走 expiration reclamation
        trimEmptyNativePages()
        if used <= limit: return
        continue
    if evict(victim, record): trimEmptyNativePages()   // ⑤ live victim 走真正淘汰
trimEmptyNativePages()                        // ⑥ 收尾 trim
return
```

要点：

- 每次释放后都 `trimEmptyNativePages()` 再继续，所以"淘汰一个 → 回收可能变空的 page"是交替进行的。
- 时间预算 `evictionTimeLimitNanos` 与尝试次数上限同时生效；维护任务在调用线程内执行，必须限制淘汰循环，避免一次写入拖垮 event loop。
- 真正 eviction 时，`YierdisDbMaxmemorySupport.evict` 调用 `YierdisDbKernel.evict(...)`；reclamation plan 在 prepare 阶段复制稳定 key bytes，commit 时移除 directory entry 并释放完整 entry/value/key graph，随后结算 ledger。

`PreparedDbMutation.shouldTrimNativePagesAfterCommit()` 和 snapshot 的 `nativeReclaimableBytes` 都只是回收候选提示，不代表相应字节已经离开 committed footprint。`trimMemory(...)` 返回的 `MemoryReclaimResult` 记录本次检查了什么（`inspectedUnits`）、实际回收了多少（`reclaimedUnits` / `reclaimedBytes`）、为什么停下（`StopReason.COMPLETE / INSPECTION_LIMIT / BYTE_LIMIT / TIME_LIMIT`）；admission 仍要在 trim 后**重新采样** owned snapshot，不能拿 reclaimable estimate 或一次 trim hint 就推断"已经低于 maxmemory"。

## 仍然无法写入时的错误路径

即使已经跑过 trim / eviction，本次写入仍可能失败：

- trim 没有释放出足够空间；
- eviction policy 在预算内找不到可删的 victim；
- participant owned snapshots 的全局总量仍高于目标线；
- prepare 阶段命中了 native allocator capacity limit。

这些失败路径的约束是：

- commit 开始前被 admission / capacity 拒绝的增长型写入必须返回稳定 OOM 文案（`MaxmemoryErrors.OOM_ERR`）；
- commit 开始前不能把半成品 mutation 留在 DB 内部，reservation 必须 rollback；
- commit 开始后的失败必须走 post-commit settle / result-unknown，不能宣称 mutation 一定未发生。

主动过期和 random / LRU eviction candidates 用的都是 directory 中的 native-backed key handles。删除前复制稳定 key bytes 只服务于本次 reclamation plan，不代表 DB 内部还留着 heap keyspace。

`prepareWrite(0)` / `enforceLocalMaintenance()` 是 maintenance-only enforcement 的关键特例：`noeviction` 下它不会因为"当前已经超限"而挡掉不增长的维护操作。

## 相关测试

- `MutationExecutorReservationTest`：reservation 先于 mutation，异常回滚后不污染下一次写入。
- `MaxmemoryEvictionTest`：`noeviction`、`allkeys-random`、`allkeys-lru`、collection growth 与拒写不变式。
- `TtlMaxmemoryTest`：TTL mutation 的保守 reservation、OOM 和失败原子性。
- `YierdisGlobalMaxmemoryGovernorTest`：全局 trim / eviction / OOM 路径、deterministic LRU scan 和时间预算分支。
- `GlobalMaxmemoryLruAcrossDbsTest`：global scope 下跨 DB 的真实 LRU 淘汰。
- `MaxmemoryPhysicalProgressTest`：物理进度而非逻辑 delta 驱动淘汰收敛。
- `MemoryStatsAccountingConsistencyTest`、`MaxmemoryScopeTest`：观测口径与 enforcement 口径保持一致，global / per-db scope 的统计差异可解释。

## Independent Capacity Domains

maxmemory protects DB growth and native-backed values. It does not replace ingress admission or hard outbound reply limits. A successful deletion/eviction can lower DB usage while an existing reply source still owns outbound capacity until its slot reaches a terminal cleanup state. Use [`production-hardening-operations.md`](./production-hardening-operations.md) when correlating `MEMORY STATS` with `INFO stats` during pressure or shutdown.