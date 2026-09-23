# TTL 与过期生命周期

TTL 命令、惰性过期和主动清理共同构成过期生命周期。权威状态只有一份：`EntryRecord.expireAtMillis`。在它之上有两份**派生状态**——精确计数 `expireCount` 和按过期时间排序的 `ExpiresIndex`——两者都由 entry 的发布 / 替换 / 释放同步维护。主动清理只消费索引中已到期的候选，不再扫描 keyspace。

本文覆盖从写入到回收的完整链路：TTL 写命令如何落成新的 `EntryRecord`、覆盖与 `PERSIST` 的语义、读路径的惰性过期、维护节拍里的有界主动清理，以及 eviction 路径如何复用同一条 reclamation。

## 三份状态

| 状态 | 载体 | 角色 | 何时更新 |
|---|---|---|---|
| `EntryRecord.expireAtMillis` | `ENTRY_RECORD` native object，offset 48 | 权威 deadline | entry publish / replace |
| `expireCount` | `YierdisDbKeyLifecycle.expireCount` | 精确派生计数 | `reconcileDerivedEntryState` |
| `ExpiresIndex` | `YierdisDbKeyLifecycle.expiresIndex` | 到期候选索引 | `reconcileDerivedEntryState`（仅 deadline 实际变化时 `add`） |

`expireAtMillis` 的取值约定：

- 负值表示 persistent；
- 非负值表示绝对毫秒时间戳。

`TTL`、`PTTL`、惰性过期、主动清理和 eviction 候选的过期判定都读这个字段。`YierdisDbKeyLifecycle` 里有两个私有谓词界定它：`hasTtl(record)` 即 `expireAtMillis >= 0`；`isExpired(record, now)` 即 `hasTtl && expireAtMillis <= now`。

`expireCount()` 只用于 `MEMORY STATS`、instance observability 和 flush outcome，不参与查找，也不是第二份 key-to-deadline 状态。

`ExpiresIndex` 是 owner 线程独占的堆内优先队列（`java.util.PriorityQueue`，无同步原语），按 `(expireAtMillis, 插入序号 sequence)` 排序，与 key 内容无关。`sequence` 是单调插入序号，只用于打破同一时间戳的次序，避免与 keyHandle 比较耦合。它登记的是截止时间发生变化的 entry：`reconcileDerivedEntryState` 只在 `newHasTtl && (oldRecord == null || oldRecord.expireAtMillis() != newRecord.expireAtMillis())` 时 `add`，因此 insert 和真正的 TTL 改值会登记新项，而 touch、`KEEPTTL` 这类 `expireAtMillis` 不变的替换复用既有索引项。

索引**允许 stale 项滞留**：TTL 被移除、改值或 key 删除重建后旧项不主动清除，消费方必须惰性校验。所以索引规模可以超过存活 TTL key 数。它是纯堆内存派生 bookkeeping，不计入 ledger 逐 mutation 账，也不计入 physical committed footprint——deadline-only mutation 不改变物理记账结论（`deadlineOnlyMutationReusesStoredKeyAndDoesNotChangePhysicalMemoryAccounting`）。

## TTL 写路径

入口是 `YierdisTtlOps`（`TtlOps` 实现）：

| 命令 | 方法 |
|---|---|
| `EXPIRE` / `PEXPIRE` | `expire(BytesView, seconds, condition)` / `pexpire(BytesView, milliseconds, condition)` |
| `EXPIREAT` | `expireAtSeconds(BytesView, unixSeconds, condition)`（`Math.multiplyExact(sec, 1000)` 溢出 → `Long.MAX_VALUE`） |
| `PEXPIREAT` | `expireAtMillis(BytesView, unixMillis, condition)` |
| `PERSIST` | `persist(BytesView)` |
| `TTL` / `PTTL` | `ttlSeconds(BytesView)` / `ttlMillis(BytesView)` |

`SET ... EX/PX/EXAT/PXAT/KEEPTTL` 走 `YierdisStringOps.set(...)`：`ExpireOption.toExpireAtMillis(now)` 解析 deadline（`KEEP_TTL` 无 deadline，保留旧值），最终同样提交新的 `EntryRecord`。

所有写路径都收敛到同一条执行链：

```text
command
  -> YierdisTtlOps / YierdisStringOps
  -> YierdisDbKernel.execute(MutationPlan)
  -> YierdisDbMutationExecutor.execute(plan)
     -> reserve(estimatedExtraBytes) 或 beginReclamation()
     -> beginAllocationScope()
     -> plan.prepare()
     -> reclamation invariants 或 ledger.reconcile(preparedPeak)
     -> prepared.commit()
     -> allocations.promote() -> ledger.commit(actualDelta)
     -> prepared.releaseSuperseded()
     -> optional native page trim
```

这套执行器的两阶段预算、commit 失败边界与 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) 完全共用，下面只讲 TTL 特有的分支。

| 情形 | admission | upperBound | prepare 动作 | outcome |
|---|---|---|---|---|
| 设置 / 更新 TTL（`setExpirePrepared`） | `NORMAL` | `memoryContext.nativeAllocationScopeBookkeepingBytes(0)` | 复用原 `EntryHandle`，`withExpireAtMillis`，`replace(..., releaseReplacedValue=false)` | `TTL_CHANGED` |
| `PERSIST` | `RECLAMATION` | `0` | deadline 改 `-1` | `TTL_CHANGED` |
| 即时过期（`EXPIRE`/`PEXPIRE` 值 ≤ 0，或 `EXAT`/`PXAT` ≤ now） | `RECLAMATION` | `0` | `delete`，`actualDelta = -estimatedBytesForRemoval`，`releaseReplacedValue=true` | `VALUE_CHANGED` |
| key 缺失 / 已过期 / 提交前被改 | 依入口 | — | `kernel.unchanged(Boolean.FALSE)` | unchanged |

关键点：

- 设置或更新 TTL **不新增 TTL 数据结构 allocation**：upper bound 只包含 allocation-scope bookkeeping，deadline 写在既有 entry metadata 里，物理 committed footprint 不变。maxmemory 仍会对 mutation scope 的保守 bookkeeping 做 admission。
- `PERSIST` 使用 reclamation admission：upper bound `0`、不得产生正增长。只有当前确有 TTL（`expireAtMillis >= 0`）才提交，否则返回 unchanged。
- 即时过期不会写入一个"过期 deadline"，而是准备删除当前 entry。
- 条件标志 NX/XX/GT/LT 在删除分支**之前**判定，入口是 `ExpireCondition.allows(currentExpireAtMillis, newExpireAtMillis)`：无 TTL 的键按无限 TTL 参与比较（GT 必然失败、LT 必然成功），NX 与 XX/GT/LT 互斥、GT 与 LT 互斥在构造时即拒绝。条件不满足返回 unchanged，键与旧 TTL 都保留。
- 相对或绝对时间计算溢出时 deadline 饱和到 `Long.MAX_VALUE`（`YierdisTtlOps.safeExpireAtMillis` / `safeAddMillis`，以及 `ExpireOption.safeExpire*`）。

### 提交前竞态与提交失败语义

每个 prepare 都会重新用 `keyLifecycle.copyKeyBytes(handle)` 拿稳定 key bytes、重新解析 `EntryHandle` 与当前 record，当 `current == null || !record.equals(current)` 时返回 `kernel.unchanged(...)`。`EntryRecord` 是含 `version`、`expireAtMillis`、`lruOrLfu` 的记录，等值比较同时约束了这些字段，所以提交前发生过 touch、覆盖或删建的 key 不会被旧操作覆盖。

commit 失败边界（与执行器一致）：

- `commitStarted` 之前的 capacity / admission rejection 返回稳定 OOM；
- `commitStarted` 之后的异常走 post-commit settle，executor best-effort promote / 对账 / 释放，DB 转 degraded，调用方收到 result-unknown。

因为 TTL mutation 自身不产生正增长（`PERSIST` 与删除是 RECLAMATION，set TTL 只改元数据），它触发的拒绝只会来自 admission 的物理占用判定，不会来自 TTL 数据结构的分配。

### 读命令：TTL / PTTL

`YierdisTtlOps.remainingTtlMillis` 决定 `-2 / -1 / 剩余`：

1. `keyLifecycle.keyHandle(keyView)` 为 null → `-2`；
2. `kernel.liveEntryRecord(handle)` 为 null → `-2`；
3. 有 TTL 时先 `keyLifecycle.touchRecord(handle, record)` 写回访问时钟（仅 LRU 开启且时钟前进时真写），再比较 deadline；
4. `expireAtMillis < 0` → `-1`；
5. 剩余 ≤ 0 → `kernel.reclaimExpired(handle, record, now)` 后返回 `-2`；
6. 否则返回剩余毫秒。

两个易错点：

- 回答 `-2` 前**先 reclaim**（惰性过期的同一笔 reclamation），保证回答 `-2` 之后 `GET`/`EXISTS` 观察不到它；仍存在的 key 永远不会得到 `-2`。
- touch 在 LRU 下会写回新 record，所以传给 `reclaimExpired` 的必须是 touched 后的 record；否则 expectedRecord 与 current 的 `lruOrLfu` 不一致会让 reclamation 静默空转。

`ttlSeconds` 按 `(remainingMillis + 500) / 1000` 四舍五入到秒——Redis 兼容；向下取整会让刚设置的 TTL 恒少 1 秒。

## 惰性过期

DB ops 解析到 `EntryRecord` 后比较 `expireAtMillis` 与当前时间：未过期返回 record；已过期调用 `YierdisDbKernel.reclaimExpired(...)` 并对调用方隐藏该 key。相关入口：

- `YierdisDbKernel.liveEntryRecord(keyHandle)`：解析 record，已过期则回收并返回 null；
- `YierdisDbKernel.reclaimExpiredBeforeMutation(keyBytes, nowMillis)`：写命令在覆盖前先回收同 key 的过期残留；
- `YierdisDbKeyLifecycle.liveEntryRecord(...)`：只判 null，不回收（给只读路径用）；它还会顺手清理 keyDirectory 指向已消失 handle 的悬挂映射。

回收本身是一笔完整 mutation（`YierdisDbKernel.reclaim(..., requireExpired=true)`）：

1. prepare 重新读 `entryRecord`，校验 `expectedRecord.equals(current)`（以及 `requireExpired` 时的 `isExpired`）；
2. `copyKeyBytes` 复制稳定 key bytes，`estimatedBytesForRemoval` 算删除 delta；
3. commit 移除 directory entry，并释放 entry 与 key resources；
4. settle ledger，再释放 superseded value，并按提示回收空 native page。

所以惰性过期不是单纯的优化：一次读可能完成物理删除和记账。只有成功取得 live record 的 LRU 路径才会更新访问时钟。

## 有界主动清理

`YierdisDbExpirationSupport.cleanupExpired(nowMillis)`（无参重载传 `0`，内部取 `System.currentTimeMillis()`）只消费 `ExpiresIndex` 队首：

```text
while (队首存在 && 队首.expireAtMillis <= now) {
    校验 = liveRecord(队首)                  // 三道惰性校验
    if (校验 == null) { dropExpiresIndexHead(); consumed++; }        // stale，丢弃
    else if (reclaimExpired(队首 keyHandle, live, now)) {
        dropExpiresIndexHead(); consumed++; reclaimed++;             // 回收成功才移除
    } else { return consumed; }                                       // 保留队首，下次重试
    if (reclaimed >= CLEANUP_MAX_CANDIDATES || timeLimitReached(started)) return consumed;
}
```

- 成本与"已过期候选数 + 队首 stale 项数"成正比，与总 key 数无关。
- 单次调用硬边界：最多回收 `CLEANUP_MAX_CANDIDATES = 20` 个过期 key（stale 项的丢弃不占这个名额）；同时受 `expireCleanupTimeLimitNanos` 限制，但由于时间检查发生在处理完一个队首项之后，每次调用**至少处理一个队首项**。
- 三道惰性校验（`liveRecord`）：① 按索引项持有的 key handle 能解析出 entry（key 已删除时 native 句柄失效，抛 `StaleNativeHandleException`，直接判 stale）；② entry 仍持有同一 key identity（`record.keyHandle().equals(candidate.keyHandle().nativeHandle())`）；③ entry 的真实 `expireAtMillis` 与索引项一致。任一不满足即判 stale。re-SET 改 TTL、`PERSIST`、overwrite、删除重建产生的 stale 项都不会误删新状态。
- 校验通过后仍走 `YierdisDbKernel.reclaimExpired(...)`，它在 commit 前会**再完整校验一次** record（double check）。

retry 与失败边界：

- 回收成功或证实 stale 后才移除队首项；
- 候选仍过期但本次未能删除时保留队首项，下一次调用重试同一候选；
- 回收抛异常时队首项同样保留，异常向上传播。

rehash dedup：`ExpiresIndex` 与 key 目录拓扑解耦。目录 rehash 期间同一 key 可能在旧表与 active 表同时可见（shadow），两者指向同一个 `KEY_BYTES` native handle。消费到重复候选时，第一个删除生效，后续候选的 identity 校验失败被判 stale 丢弃——不会重复删除，也不会泄漏（`cleanupExpiredDeduplicatesRehashShadowCandidates`）。单次候选上限在 rehash 全程保持不变，可跨越表切换反复调用直到排空（`cleanupExpiredDrainsAcrossDirectoryRehashGenerations`）。

## flush 与派生状态

`flushDb` / `flushDbAsync` 的 commit 回调 `commitFlushDb` / `commitFlushDbAsync` 调用 `YierdisDbKeyLifecycle.resetExpirationTracking()`（`expireCount = 0`、`expiresIndex.clear()`）。async 变体只在 commit 边界发布空目录，旧目录在后续 owner maintenance 里释放；退役目录里的 stale 索引项不跟随旧目录延迟回收。

## Maintenance 与 maxmemory

调度链：

```text
Netty worker timer
  -> CommandExecutor.executeMaintenance(Runnable)      // 提交到 owner executor，running 时才跑
  -> YierdisInstanceRuntimeAccess.maintenanceTick()
     -> every DB: YierdisDb.runMaintenance() -> YierdisDbDataMaintenance.runMaintenance()
          reclaimDetachedEntries()        // 每 tick 最多回收 64 个 async-flush 残留 entry
          health.requireWritable()
          drainExpiredWithinBudget()      // 循环单次 cleanupExpired，直到无到期候选或预算耗尽
          rehashMaintenance(HashTableWorkBudget.of(64, maintenanceTimeLimitNanos))
          enforceMaxmemory() -> ledger.enforceLocalMaintenance()
        if defrag enabled: defragMaintenance()
     -> YierdisInstanceResources.enforceGlobalMaxmemoryMaintenance()
        governor present: enforceMaintenance()
        per-db scope: no-op
```

真正的 DB cleanup 只在 owner thread 上执行。`drainExpiredWithinBudget` 用 `hasDueExpiredCandidates`（队首 O(1) 比较）判断"是否还有到期候选"，在时间预算内反复调用单次 cleanup，直到没有到期候选或预算耗尽；短 TTL churn 在节拍之间不会无限积压。`maintenanceTimeLimitNanos` 就是实例配置里的 `expireCleanupTimeLimitMillis`（默认 5ms）。

写 admission（local 与 global 两种 maxmemory 模式）**不内联触发** expires 索引清理：预算判定只看 owned physical snapshot、trim 和 eviction。过期 key 的回收时机因此有三条：维护节拍、读路径惰性过期、eviction candidate selection。

- `allkeys-lru` / `allkeys-random` 抽样或扫描到过期 key 时把它作为最优候选（LRU 比较中 `lruClock = 0`，排在所有 live key 之前），在淘汰路径上先走 expiration reclamation 而不跳过，因此「只剩过期条目」的 keyspace 不会再把写入卡进 OOM。
- `noeviction` 不选 victim，过期占用只能等维护节拍或读路径惰性过期，admission 仍按 OOM 拒绝增长写入（`writeAdmissionUnderNoevictionStillRejectsWhenOnlyExpiredOccupancyRemains`）。

## 维护约束

- 不要在 entry lifecycle 之外直接改写 `expireAtMillis`；`expireCount` 与 expires 索引必须随 entry publish/replace/release 一起更新（唯一入口是 `YierdisDbKeyLifecycle`）。
- expires 索引只在 deadline 实际变化时登记新项；不要为 touch 等 `expireAtMillis` 不变的替换写索引，否则读路径会把索引灌满重复项。
- 索引项的消费必须先做惰性校验（identity + 真实 deadline），不能只按索引项的 deadline 删除。
- 索引维护与消费只发生在 owner 线程；结构本身不带同步原语，不要引入跨线程访问或新锁。
- expiration reclamation 必须使用 `AdmissionMode.RECLAMATION`，upper bound 为 `0`，且不得产生正增长（`YierdisDbMutationExecutor.requireReclamationInvariants`）。

## 相关测试

- `TtlLifecycleDirectOpsTest`：`TTL/PTTL` 的 `-2/-1/>0`、`PERSIST`、即时过期和溢出饱和。
- `ActiveExpirationTest`：索引消费只访问过期候选、单次候选上限（20）、rehash 全程排空、rehash shadow 候选去重、stale 索引项惰性校验（re-SET/PERSIST/overwrite/删除重建）、LRU/RANDOM 写 admission 回收过期占用而 noeviction 仍 OOM，以及 churn 在维护节拍间排空。
- `ExpireSemanticsTest`、`ExpireConditionFlagsTest`：各 value type 的即时过期与后续重建、`TTL/PTTL` 的 `-2` 只在 key 已不在时回答，以及 NX/XX/GT/LT 条件标志。
- `TtlMaxmemoryTest`：TTL mutation 的 maxmemory admission 与失败原子性。
- `PhysicalMemoryAccountingTest`：deadline-only mutation 不改变物理 committed footprint。