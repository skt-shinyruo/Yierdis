# Maxmemory 与淘汰

ledger reservation、per-DB / global scope、eviction policy 和 OOM 路径，都围绕同一条原则展开：先证明本次写入有空间，再让 mutation 执行；cleanup 和 eviction 只把 usage 拉回目标线，不替代 mutation commit。

## `usedBytes`、`reservedBytes` 和 effective usage

`YierdisDbMemoryLedger` 维护两份数字：

- `usedBytes`：按已提交 mutation 的 `actualDeltaBytes` 增减的逻辑账本，不是 allocator/JVM 的实时物理占用。
- `reservedBytes`：预算已通过，但 mutation 还没 commit/rollback 的那段窗口。

`YierdisDbMemoryReporter.memoryStats()` 暴露的相关字段是：

- `ledger_used_bytes`：`heapDataBytesEstimate`，不是 ledger 逻辑 `usedBytes`
- `ledger_reserved_bytes`：ledger `reservedBytes`
- `effective_used_bytes_for_maxmemory`：物理 `usedBytesForMaxmemory` 加 `reservedBytes`

enforcement 不把 ledger `usedBytes`、native counter 和 TTL estimate 再拼成一套数字。每个 DB 直接报告 owned `MemoryUsageSnapshot`，物理口径固定为：

```text
usedBytesForMaxmemory
  = heapEstimatedBytes
  + nativeMetadataCommittedBytes
  + nativeDataCommittedBytes
```

entry 中的 TTL 字段和 collection topology 已进入 owned snapshot，不能再按带 TTL 的 key 数量重复加一遍。`nativeDataLiveBytes` 和 `nativeReclaimableBytes` 是诊断维度，不从 committed footprint 中扣除。`MEMORY STATS` 的 `used_bytes_for_maxmemory` 就是这份物理快照；`effective_used_bytes_for_maxmemory` 在此之上再加 ledger `reservedBytes`。

## `YierdisDbMutationExecutor` 为什么先 reserve 再 prepare

标准写路径是：

```text
estimate upper bound
  -> YierdisDbKernel.execute(MutationPlan)
  -> YierdisDbMutationExecutor.execute(plan)
     -> ledger.reserve(upperBound)
     -> stableMemoryBackend.beginAllocationScope()
     -> plan.prepare()
     -> ledger.reconcile(preparedPeak)
     -> prepared.commit()
     -> allocationScope.promote()
     -> ledger.commit(actualDelta)
     -> prepared.releaseSuperseded()
     -> optional native page trim
```

之所以要这样，是因为 DB mutation 经常需要“先分配、后知道实际变化量”：

- 新 key 可能新增 key bytes、entry record 和 value payload；
- TTL deadline 更新会产生 mutation-scope bookkeeping，但没有独立的 TTL allocation；
- collection 或 string 可能触发编码升级；
- 覆盖写可能最终是 shrink、no-op 或负 delta。

`upperBoundBytes()` 解决“能不能先让这次写动起来”，prepare 后实测的 native growth 加 staged heap topology 用来收窄 reservation，`actualDeltaBytes()` 给出“最后到底长了多少/缩了多少”。提交后的顺序固定为 allocation promote、ledger settle、release superseded，最后才看提示尝试 trim。

`MutationExecutorReservationTest` 覆盖了两个关键点：

- 预算不过关时，`prepare()` 根本不会执行；
- commit 前 prepare/校验失败时，prepared resources、allocation scope 和 ledger reservation 都会 abort/rollback，不会污染下一次写入。

`prepared.commit()` 开始之后，就再没有“确认未生效”这种安全回滚前提。此后的异常触发 post-commit settle：executor best-effort promote allocation、settle ledger、release superseded resources；DB 转入 degraded，调用方收到 result-unknown，而不是把异常简单映射成一次确定未执行的 OOM。只有 commit 开始前的 capacity rejection 才能安全返回 Redis 风格 OOM。

degraded 不是终态：`RuntimeDbEngine.reconcileAccounting()` 在 owner thread 上重算物理用量、把 ledger 漂移修正入账并清除 degraded，尝试与结果记入 `DbHealthSnapshot.lastReconciliation`。恢复只能显式触发，maintenance tick 不会自动对账。

## per-DB scope 的判断顺序

没有全局 coordinator 时，`YierdisDbMemoryLedger.reserve(...)` 的判断顺序是本地 maxmemory 语义的真相来源：

1. 如果 `estimatedExtraBytes > maxmemoryBytes`，直接 OOM。
2. 计算本次写入前必须压到的目标：`limit = maxmemoryBytes - estimatedExtraBytes`。
3. 如果 owned physical snapshot 超过 `limit`，调用 `YierdisDbMaxmemorySupport.evictUntilUnder(limit)`。该入口先 trim empty native pages，再重新采样 snapshot。
4. 重新采样后仍超限时，`noeviction` 不选 victim；增长型写入 OOM，`estimatedExtraBytes == 0` 返回 noop reservation。其他策略继续淘汰，并在每次释放后 trim/resnapshot。
5. 淘汰结束后再次采样；仍超限且本次写入会增长时，OOM。
6. 只有通过这些检查后，才增加 `reservedBytes`。

写 admission 不内联跑 expires 索引清理（那仍是维护节拍的工作），但 `allkeys-lru`/`allkeys-random` 的 victim 选择不再把过期 key 当成不可回收而跳过：抽样抽到或扫描到过期 key 时，它作为最优候选先走 expiration reclamation。因此「只剩过期条目」的 keyspace 不会再把 admission 卡进 OOM——只要回收过期占用能把 owned physical snapshot 压回目标线，本会 OOM 的写入就会成功。物理占用仍超限，或策略是 `noeviction`（永不选 victim）时，增长型写入才被拒绝。

这解释了几个容易混淆的现象：

- 覆盖写如果最终缩小 value，可以在“已经顶到 maxmemory”时成功。
- 纯 maintenance enforcement 复用 `enforceLocalLimit` 的同一套判断口径，而不是另起一套。
- `usedBytesForMaxmemory()` 是 owned physical snapshot 的投影；ledger `usedBytes` 只负责 mutation delta 对账，不能替代拒写采样。

## global scope 与 governor 协调

global scope 下，本地 ledger 不自己算跨 DB 预算，而是先委托 `YierdisGlobalMaxmemoryGovernor.prepareWrite(estimatedExtraBytes)`。

governor 的主线是：

1. 按 budget 轮转调用所有 participant 的 `trimMemory(...)`。expires 索引清理由各 DB 的维护节拍驱动，`prepareWrite` 不内联触发；但 participant 抽样/扫描上报的 candidate 可以是过期 key，governor 的 `evict(candidate)` 会先走 expiration reclamation 回收它（见下文收敛规则）。
2. 计算本次写入前的目标线 `limit = maxmemoryBytes - estimatedExtraBytes`。
3. 汇总所有 participant 最新的 owned physical snapshots；总量不超过 `limit` 时直接通过。
4. `noeviction` 在 trim/resnapshot 后仍超限时，只允许不增长的维护路径继续。
5. 需要淘汰时，跨 participant 挑 victim；每次释放后继续 trim 并汇总新 snapshots，直到全局 usage 压回目标线，或在时间/尝试预算内停止。

有以下两个跨 DB 约束：

- participant 是每个 DB 暴露出来的 `YierdisDbMaxmemorySupport`，governor 只能通过 SPI 观察和驱动，不直接越过 DB API。
- governor 只相加每个 DB 独占的 `MemoryUsageSnapshot`。各 backend runtime 的 counter 不进入全局 enforcement，它只用于对应 backend 的 region lifecycle 和 native leak 诊断。

maintenance 时的顺序由 `YierdisInstanceRuntimeAccess.maintenanceTick()` 固定：

- 每个 DB 先跑 `runMaintenance()`：回收 detached entry，在时间预算内排空 expires 索引，推进 rehash，然后 `enforceMaxmemory()`（`ledger.enforceLocalMaintenance()`）。global scope 下每个 DB 的本地 `maxmemoryBytes` 仍是整份全局预算，这一步照常执行。
- `defrag` 打开时，每个 DB 在 `runMaintenance()` 之后再跑 `defragMaintenance()`。
- DB 循环结束后调用 `enforceGlobalMaxmemoryMaintenance()`。只有 global scope 创建了 governor 时它才会 `enforceMaintenance()`；per-db scope 下这次调用是空操作。

`GlobalMaxmemoryLruAcrossDbsTest` 覆盖了一个核心语义：DB1 的写入可以在 global scope 下淘汰 DB0 里真正的全局 LRU key。

## `allkeys-random` / `allkeys-lru` / `noeviction`

三种策略的差异不只在“挑谁删”：

- `noeviction`：不挑 victim；压力路径仍会先 trim empty pages 并重新采样，确认 committed footprint 仍超限后才拒绝增长型写入。
- `allkeys-random`：单 DB 视角下用 `randomKeyHandle()` 选 key；global governor 先随机 participant，再向它要 candidate。
- `allkeys-lru`：按 `EntryRecord.lruOrLfu()` 选择最小值。样本数覆盖全部 key 时，单 DB 和 global 两层都会退化成完整扫描，减少测试和小 keyspace 下的随机抖动。

还有两条收敛规则：

- 过期候选优先于 live victim。`allkeys-lru`/`allkeys-random` 的 candidate selection 抽到或扫描到过期 key 时直接把它上报为候选（LRU 比较中按 `lruClock=0` 排在所有 live key 之前，live 访问时钟恒 ≥ 1），`evict(...)` 对它先走 expiration reclamation；只有 live key 才进入真正的 victim 淘汰。
- 真正 eviction 时，`YierdisDbMaxmemorySupport` 调用 `YierdisDbKernel.evict(...)`；reclamation plan 在 prepare 阶段复制稳定 key bytes，commit 时移除 directory entry 并释放完整 entry/value/key graph，随后结算 ledger。

所以“淘汰”和“过期”都会删除 key，但触发原因和测试入口不同。

`PreparedDbMutation.shouldTrimNativePagesAfterCommit()` 和 snapshot 的 `nativeReclaimableBytes` 都只是回收候选提示，不代表相应字节已经离开 committed footprint。`trimMemory(...)` 返回的 `MemoryReclaimResult` 记录本次检查了什么、实际回收了多少、为什么停下；admission 仍要在 trim 后重新采样 owned snapshot，不能拿 reclaimable estimate 或一次 trim hint 就推断“已经低于 maxmemory”。

## 仍然无法写入时的错误路径

即使已经跑过 trim / eviction，本次写入仍可能失败：

- trim 没有释放出足够空间；
- eviction policy 在预算内找不到可删的 victim；
- participant owned snapshots 的全局总量仍高于目标线；
- prepare 阶段命中了 native allocator capacity limit。

这些失败路径的约束是：

- commit 开始前被 admission/capacity 拒绝的增长型写入必须返回稳定 OOM 文案；
- commit 开始前不能把半成品 mutation 留在 DB 内部，reservation 必须 rollback；
- commit 开始后的失败必须走 post-commit settle/result-unknown，不能宣称 mutation 一定未发生。

主动过期和 random/LRU eviction candidates 用的都是 directory 中的 native-backed key handles。删除前复制稳定 key bytes 只服务于本次 reclamation plan，不代表 DB 内部还留着 heap keyspace。

`prepareWrite(0)` 是 maintenance-only enforcement 的关键特例：`noeviction` 下它不会因为“当前已经超限”而挡掉不增长的维护操作。

## 相关测试

- `MutationExecutorReservationTest`：reservation 先于 mutation，异常回滚后不污染下一次写入。
- `MaxmemoryEvictionTest`：`noeviction`、`allkeys-random`、`allkeys-lru`、collection growth 与拒写不变式。
- `TtlMaxmemoryTest`：TTL mutation 的保守 reservation、OOM 和失败原子性。
- `YierdisGlobalMaxmemoryGovernorTest`：全局 trim/eviction/OOM 路径、deterministic LRU scan 和时间预算分支。
- `GlobalMaxmemoryLruAcrossDbsTest`：global scope 下跨 DB 的真实 LRU 淘汰。
- `MemoryStatsAccountingConsistencyTest`、`MaxmemoryScopeTest`：观测口径与 enforcement 口径保持一致，global/per-db scope 的统计差异可解释。

## Independent Capacity Domains

maxmemory protects DB growth and native-backed values. It does not replace ingress admission or hard outbound reply limits. A successful deletion/eviction can lower DB usage while an existing reply source still owns outbound capacity until its slot reaches a terminal cleanup state. Use [`production-hardening-operations.md`](./production-hardening-operations.md) when correlating `MEMORY STATS` with `INFO stats` during pressure or shutdown.
