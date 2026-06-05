# Maxmemory 与淘汰

本文解释 ledger reservation、per-DB / global scope、eviction policy 和 OOM 路径。核心原则是：先证明本次写入有空间，再让 mutation 执行；cleanup 和 eviction 只负责把 usage 拉回目标线，不替代 mutation commit。

## `usedBytes`、`reservedBytes` 和 effective usage

`YierdisDbMemoryLedger` 维护两份数字：

- `usedBytes`：已经提交的真实占用。
- `reservedBytes`：预算已通过、但 mutation 还没 commit/rollback 的窗口。

`YierdisDbMemoryReporter.memoryStats()` 会把它们暴露成：

- `ledger_used_bytes`
- `ledger_reserved_bytes`
- `effective_used_bytes_for_maxmemory`

其中 `effective_used_bytes_for_maxmemory` 不是单纯的 `usedBytes + reservedBytes` 视角，而是“参与 maxmemory 判断的 used 口径”再加 reservation。`usedBytesForMaxmemory()` 本身也不只是 ledger：

- 以 ledger `usedBytes` 为基线；
- 按部署模式决定是否把 native/off-heap usage 算进去；
- 再按 TTL 条目数追加 expire index 的稳定估算。

所以 `MEMORY STATS` 里的 `used_bytes_for_maxmemory` 解释的是 enforcement 口径，而不是“所有内部计数器里挑一个字段直接打印”。

## `YierdisDbMutationExecutor` 为什么先 reserve 再 apply

标准写路径是：

```text
estimate upper bound
  -> YierdisDbMutationExecutor.execute(plan)
     -> ledger.reserve(upperBound)
     -> plan.apply()
     -> ledger.commit(actualDelta)
```

这样做的原因是 DB mutation 经常需要“先分配、后知道实际变化量”：

- 新 key 可能新增 key bytes、entry record 和 value payload；
- TTL 首次写入可能新增 expire metadata；
- collection 或 string 可能触发编码升级；
- 覆盖写可能最终是 shrink、no-op 或负 delta。

`upperBoundBytes()` 解决“能不能先让这次写动起来”，`actualDeltaBytes()` 解决“最后到底长了多少/缩了多少”。`MutationExecutorReservationTest` 覆盖了两个关键点：

- 预算不过关时，`apply()` 根本不会执行；
- `apply()` 自己抛异常时，reservation 会 rollback，不会污染下一次写入。

执行器还负责统一错误映射：

- ledger / coordinator 的 OOM 映射成 Redis 风格 `OOM command not allowed when used memory > 'maxmemory'.`
- `OffHeapOutOfMemoryException` 映射成 `OOM off-heap memory limit exceeded`

## per-DB scope 的判断顺序

没有全局 coordinator 时，`YierdisDbMemoryLedger.reserve(...)` 的判断顺序是本地 maxmemory 语义的真相来源：

1. 先做 `cleanupExpired.run()`，让刚释放的空间参与同一次预算判定。
2. 如果 `estimatedExtraBytes > maxmemoryBytes`，直接 OOM。
3. 计算本次写入前必须压到的目标：`limit = maxmemoryBytes - estimatedExtraBytes`。
4. 如果 `usedBytesForMaxmemory() > limit`：
   - `noeviction`：增长型写入直接拒绝；
   - `estimatedExtraBytes == 0`：返回 noop reservation，让 shrink/no-growth 路径还能执行；
   - 其他策略：调用 `YierdisDbMaxmemorySupport.evictUntilUnder(limit)`。
5. 淘汰后再次检查；仍超限且本次写入会增长时，OOM。
6. 只有通过这些检查后，才增加 `reservedBytes`。

这解释了几个容易混淆的现象：

- 覆盖写如果最终缩小 value，可以在“已经顶到 maxmemory”时成功。
- 纯 maintenance enforcement 会复用 `reserve(0)`，而不是另起一套判断口径。
- `usedBytesForMaxmemory()` 比 ledger `usedBytes` 更贴近真正的拒写条件，因为它把 TTL/native 口径也带进来了。

## global scope 与 governor 协调

global scope 下，本地 ledger 不自己算跨 DB 预算，而是先委托 `YierdisGlobalMaxmemoryGovernor.prepareWrite(estimatedExtraBytes)`。

governor 的主线是：

1. 对所有 participant 执行 `cleanupExpired(nowMillis)`。
2. 计算本次写入前的目标线 `limit = maxmemoryBytes - estimatedExtraBytes`。
3. `globalUsedBytesForMaxmemory() <= limit` 时直接通过。
4. `noeviction` 只允许不增长的维护路径继续。
5. 需要淘汰时，跨 participant 挑 victim，直到全局 usage 压回目标线，或在时间/尝试预算内停止。

这里有两个跨 DB 约束：

- participant 是每个 DB 暴露出来的 `YierdisDbMaxmemorySupport`，governor 只能通过 SPI 观察和驱动，不直接越过 DB API。
- shared usage source 表示实例级共享 native/off-heap usage，只加一次，避免 shared runtime 在每个 DB 上重复计数。

maintenance 时的顺序由 `YierdisInstanceRuntimeAccess.maintenanceTick()` 固定：

- 每个 DB 都先 `cleanupExpired()`，再 `defragMaintenance()`；
- per-DB scope 在每个 DB 内分别 enforce；
- global scope 则在 DB 循环结束后，统一跑一次实例级 maxmemory maintenance。

`GlobalMaxmemoryLruAcrossDbsTest` 覆盖了一个核心语义：DB1 的写入可以在 global scope 下淘汰 DB0 里真正的全局 LRU key。

## `allkeys-random` / `allkeys-lru` / `noeviction`

三种策略的差异不只在“挑谁删”：

- `noeviction`：不挑 victim；增长型写入直接报 OOM。
- `allkeys-random`：单 DB 视角下用 `randomKeyHandle()` 选 key；global governor 先随机 participant，再向它要 candidate。
- `allkeys-lru`：按 `EntryRecord.lruOrLfu()` 选择最小值。样本数覆盖全部 key 时，单 DB 和 global 两层都会退化成完整扫描，减少测试和小 keyspace 下的随机抖动。

还有两条收敛规则：

- cleanup 先于 eviction。候选 key 如果已经过期，会先走 `removeIfExpired(...)`，它算 `EXPIRED`，不是 `EVICTED`。
- 真正 eviction 时，`YierdisDbMaxmemorySupport.removeRecord(...)` 会先复制稳定 key bytes，再移除 TTL index、删除 entry、扣减 used bytes，最后发 synthetic `DEL key`，`kind=EVICTED`。

所以“淘汰”和“过期”都会删除 key，但 change-event kind、触发原因和测试入口不同。

## 仍然无法写入时的错误路径

即使已经跑过 cleanup / eviction，本次写入仍可能失败：

- cleanup 没释放出足够空间；
- eviction policy 在预算内找不到可删的 victim；
- shared/global usage 仍高于目标线；
- `apply()` 过程中命中了 native allocator OOM。

这些失败路径的约束是：

- 增长型写入最终必须返回稳定 OOM 文案；
- 失败前不能把半成品 mutation 留在 DB 内部；
- reservation 必须 rollback，避免后续写入被“幽灵 reserved bytes”污染。

TTL index、random/LRU eviction candidates 和 synthetic delete 都使用 native-backed key handles。删除前复制稳定 key bytes 是为了 change event 和 output ownership，不表示 DB 内部仍有 heap keyspace。

`prepareWrite(0)` 是 maintenance-only enforcement 的关键特例：在 `noeviction` 下它不会因为“当前已经超限”而阻止不增长的维护操作。

## 相关测试

- `MutationExecutorReservationTest`：reservation 先于 mutation，异常回滚后不污染下一次写入。
- `MaxmemoryEvictionTest`：`noeviction`、`allkeys-random`、`allkeys-lru`、collection growth 与拒写不变式。
- `TtlMaxmemoryTest`：TTL metadata 本身也参与 maxmemory enforcement。
- `YierdisGlobalMaxmemoryGovernorTest`：全局 cleanup/eviction/OOM 路径、deterministic LRU scan 和时间预算分支。
- `GlobalMaxmemoryLruAcrossDbsTest`：global scope 下跨 DB 的真实 LRU 淘汰。
- `MemoryStatsAccountingConsistencyTest`、`MaxmemoryScopeTest`：观测口径与 enforcement 口径保持一致，global/per-db scope 的统计差异可解释。
