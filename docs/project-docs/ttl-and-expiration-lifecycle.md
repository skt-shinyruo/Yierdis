# TTL 与过期生命周期

本文解释 TTL 命令、惰性过期、批量清理和 expire index 一致性。这里要同时盯住三条线：读路径把过期 key 当作不存在，写路径让 TTL metadata 与 entry graph 一起提交，maintenance 只在 owner thread 内做 best-effort 清理。

## TTL 元数据到底存在哪里

TTL 当前不是单点字段，而是两份状态一起维护：

- `YierdisFfmExpireIndex` / `YierdisHeapExpireIndex`：按 `KeyHandle -> expireAtMillis` 建索引，给 `TTL/PTTL`、随机采样 cleanup、maxmemory cleanup-first 使用。
- `EntryRecord.expireAtMillis`：镜像当前 TTL，保证 entry 改写、introspection 和后续 value rewrite 看到同一状态。

`YierdisDbKeyLifecycle.setExpireAtMillis(...)` 先写 expire index，再通过 `replaceEntryExpire(...)` 更新 `EntryRecord` 镜像。`removeExpire(...)` 则先删 index，再把 entry 里的 TTL 设回 `-1`。

这两份状态的职责不同：

- expire index 是查找和 cleanup 的入口；
- `EntryRecord.expireAtMillis` 是 DB object graph 内部的同步镜像；
- `YierdisDbMemoryReporter.usedBytesForMaxmemory()` 还会按 TTL 条目数追加稳定估算，因为 expire index 不靠 value payload ledger 自动记账。

维护时不要只改一边。只改 entry 会让 cleanup/`TTL` 看不到 TTL；只改 index 会让 entry rewrite、memory/introspection 和 lazy expire 看到旧状态。

## TTL 命令写路径

TTL 写命令和 `SET ... EX/PX/EXAT/PXAT/KEEPTTL` 最终都收敛到同一条 DB 写路径：

```text
command
  -> YierdisTtlOps / YierdisStringOps
  -> YierdisDbMutationExecutor.execute(plan)
     -> YierdisDbMemoryLedger.reserve(upperBound)
     -> plan.apply()
        -> keyLifecycle.setExpireAtMillis(...) / removeExpire(...) / removeEntry(...)
     -> ledger.commit(actualDelta)
```

几条关键分支：

- 第一次给 persistent key 增加 TTL 时，`upperBoundBytes()` 会额外预留一份 `ENTRY_OVERHEAD_BYTES_ESTIMATE`，因为 expire index 会新增条目。
- 更新已有 TTL 不再重复预留这份 metadata 开销。
- `PERSIST` 仍走 mutation executor，但 `upperBoundBytes()` 为 `0`，成功时只移除 TTL metadata，`MutationOutcome` 是 `TTL_CHANGED`。
- `EXPIRE 0`、`PEXPIRE 0` 或绝对过期时间已经不晚于 `now` 时，不是“写一个已过期 TTL”，而是直接删除 key。`YierdisTtlOps.deleteImmediately(...)` 会先摘 expire index，再删 entry，并把 `actualDeltaBytes` 记成负数。
- key 缺失或在进入 TTL 命令前已经被 lazy expire 删除时，写命令返回 unchanged；读命令返回 Redis 兼容的 `-2` / `-1` / `>0`。

这也是 `TtlMaxmemoryTest` 的重点：新增 TTL metadata 本身可能触发 `noeviction` OOM，失败时旧值必须保持不变。

## `liveEntryRecord(...)` 的惰性删除语义

`YierdisDbKeyLifecycle.liveEntryRecord(...)` 是 TTL 可见性的核心，不是普通 lookup。读路径大致顺序是：

1. 从 `NativeKeyDirectory` 找到 `EntryHandle`。
2. 从 `EntryTable` 取 `EntryRecord`。
3. 如果 directory 还指向已经消失的 entry，调用 `unlinkEntry(...)` 清理悬挂映射并返回 `null`。
4. 如果 TTL 已过期，调用 `removeIfExpired(...)`：
   - 先复制稳定 key bytes；
   - 估算删除要扣掉的 bytes；
   - 先移除 expire index；
   - 再 `removeEntry(...)` 释放 entry 和 payload；
   - 调整 used bytes；
   - 发 synthetic `DEL key`，`kind=EXPIRED`。
5. 如果删除没有完成，但 `isKeyExpired(...)` 仍判断为过期，继续把 key 隐藏成 `null`。

这意味着 lazy expire 不是纯优化，而是当前实现的真实读语义：

- `GET` / `TYPE` / `TTL` / `MEMORY USAGE` 都通过它观察“活着的 key”；
- 读路径本身可能触发删除、记账和 change event；
- 启用了 LRU 时，只有成功拿到 live record 的读路径才继续 `touchRecord(...)` 更新访问时钟。

## `cleanupExpired(...)` 的扫描和 budget

后台清理由 `YierdisDbExpirationSupport.cleanupExpired(...)` 负责，但它不是全表扫描。

当前策略：

- 每轮最多从 expire index 采样 `20` 个 key。
- 最多循环 `16` 轮。
- 单次调用还受 `expireCleanupTimeLimitNanos` 限制。
- 当本轮过期命中率不高于 `25%` 时提前结束，避免在“几乎没有过期 key”的场景里浪费整轮 budget。

每个样本有三种自愈分支：

- expire index 里有 key，但 `expireAtMillis` 取不到：删脏索引。
- expire index 里有 key，但 `EntryTable` 里没有 record：删脏索引。
- TTL 已过期：走 `removeIfExpired(...)`，按完整 key lifecycle 删除。

maintenance 调度链再包一层：

```text
Netty worker timer
  -> CommandExecutor.executeMaintenance(...)
  -> YierdisInstanceRuntimeAccess.maintenanceTick()
     -> every DB: cleanupExpired() -> defragMaintenance()
     -> per-db scope: enforceMaxmemoryMaintenance()
     -> global scope: instance-level governor maintenance
```

`YierdisServerBootstrap` 用 `cleanupPending` 把重复 tick coalesce 掉，避免 executor 忙时堆出追赶式 cleanup 队列。真正的 DB cleanup 仍只在 owner thread 上执行。

如果配置了 change sink，`YierdisInstanceRuntimeAccess.maintenanceTick()` 会先打开 `DbChangeContext`，这样 cleanup 删除过期 key 时发出的 synthetic `EXPIRED` 事件就能桥接到 runtime change sink。

## `EntryRecord.expireAtMillis` 与 expire index 的双写约束

TTL 相关删除和更新都在维护同一个顺序约束：

- 设置 TTL：先写 expire index，再更新 entry mirror。
- 清除 TTL：先删 expire index，再把 entry mirror 设回 `-1`。
- 删除过期 key：先摘 expire index，再删 entry，避免 cleanup 立刻再次随机命中同一个 key。
- 显式即时过期：`deleteImmediately(...)` 也遵循同样顺序。

这让脏状态可以被自愈，但不应该被主动制造：

- `cleanupExpired(...)` 能清掉 stale expire index entry；
- `liveEntryRecord(...)` 能清掉 stale key-directory -> entry 映射；
- 这些分支是保护网，不是鼓励直接改底层结构的许可。

维护规则很简单：TTL 行为统一经 `YierdisTtlOps`、`YierdisStringOps` 和 `YierdisDbKeyLifecycle`；不要在别的地方直接写 `EntryRecord.expireAtMillis` 或单独操作 expire index。

## 相关测试

- `TtlLifecycleDirectOpsTest`：`TTL/PTTL` 的 `-2/-1/>0` 语义、`PERSIST`、绝对过期和 cleanup 后可见性。
- `ExpireIndexTest`：cleanup 删除无需访问的过期 key、清理 stale expire entry、发 synthetic `EXPIRED` delete、TTL maxmemory accounting。
- `ExpireSemanticsTest`：`EXPIRE 0` 删除 list/hash/set/zset 后，后续写入可以按新 key 重建。
- `TtlMaxmemoryTest`：新增第一条 TTL metadata 时，`noeviction` 下的 OOM 必须发生在 mutation 之前。
- `MemoryStatsAccountingConsistencyTest`：`usedBytesForMaxmemory` 与 `MEMORY STATS` 在有 TTL metadata 时保持一致。
- `OffHeapBytesViewTtlRegressionTest`、`ExpireKeySharingTest`：补充覆盖 off-heap 读视图和 key 生命周期共享状态下的过期行为。
