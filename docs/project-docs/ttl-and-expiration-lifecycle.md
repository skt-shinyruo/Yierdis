# TTL 与过期生命周期

本文解释 TTL 命令、惰性过期和主动过期清理。TTL 的权威状态只有一份：`EntryRecord.expireAtMillis`；在此之上有一个按过期时间排序的派生 expires 索引，主动清理只消费索引中已到期的候选，不再扫描 keyspace。

## TTL 状态

`EntryRecord.expireAtMillis` 是每个 key 的 TTL 权威字段：

- 负值表示 persistent；
- 非负值表示绝对毫秒时间戳；
- `TTL`、`PTTL`、惰性过期、主动清理和 eviction 候选的过期判定都读取这个字段。

`YierdisDbKeyLifecycle.expireCount()` 是随 entry 发布、替换和释放同步更新的精确派生计数，用于 `MEMORY STATS`、instance observability 和 flush outcome。它不是第二份 key-to-deadline 状态，也不参与查找。

`ExpiresIndex` 是第二份派生状态：owner 线程独占的堆内优先队列，按 `(expireAtMillis, 插入序号)` 排序。它只在 deadline 实际变化的 entry 转移（insert、替换）时登记新项；touch、`KEEPTTL` 等 `expireAtMillis` 不变的替换复用既有索引项。TTL 被移除、改值或 key 被删除重建时旧项不主动清除——索引允许 stale 项，消费方必须惰性校验。索引是纯堆内存派生 bookkeeping，不计入 ledger 逐 mutation 账，也不计入物理 committed footprint；deadline-only mutation 不改变物理记账的结论因此保持不变。

## TTL 命令写路径

TTL 写命令和 `SET ... EX/PX/EXAT/PXAT/KEEPTTL` 最终都通过 mutation executor 提交新的 `EntryRecord`：

```text
command
  -> YierdisTtlOps / YierdisStringOps
  -> YierdisDbKernel.execute(MutationPlan)
  -> YierdisDbMutationExecutor.execute(plan)
     -> reserve upper bound
     -> prepare replacement or deletion
     -> commit EntryRecord
     -> settle ledger and release superseded resources
```

关键分支如下：

- 设置或更新 TTL 会复用原 `EntryHandle`，只准备并发布新的 `EntryRecord`；upper bound 只包含 allocation-scope bookkeeping，不存在额外 TTL 数据结构 allocation。
- `PERSIST` 使用 reclamation admission，upper bound 为 `0`，成功时把 deadline 改为 `-1`，结果为 `TTL_CHANGED`。
- `EXPIRE 0`、`PEXPIRE 0` 或已经到期的绝对时间不会写入一个过期 deadline，而是准备删除当前 entry。
- `EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT` 的 `NX/XX/GT/LT` 条件标志在删除分支之前判定：条件不满足时返回 unchanged，键与旧 TTL 都保留；无 TTL 按无限 TTL 参与比较（GT 必然失败、LT 必然成功）。
- key 缺失或在提交前已发生变化时，prepared mutation 返回 unchanged，不覆盖较新的 record。
- 相对或绝对时间计算溢出时 deadline 饱和到 `Long.MAX_VALUE`。
- 读命令保持 Redis 兼容结果：key 不存在或已过期为 `-2`，persistent 为 `-1`，其余返回剩余时间。`TTL`/`PTTL` 回答 `-2` 前会先 reclaim 该 key（惰性过期的同一笔 reclamation），保证 `-2` 之后 `GET`/`EXISTS` 观察不到它；仍存在的 key 永远不会得到 `-2`。

TTL deadline 本身位于既有 entry metadata 中。只改变 deadline 不增加 DB 的物理 committed footprint；maxmemory 仍会对 mutation scope 的保守 bookkeeping 做 admission。

## 惰性过期

DB ops 解析到 `EntryRecord` 后会比较 `expireAtMillis` 与当前时间。未过期时返回 record；已过期时调用 `YierdisDbKernel.reclaimExpired(...)`，并始终对调用方隐藏该 key。

回收仍是一笔完整 mutation：

1. 重新校验 key identity、当前 record 与 deadline。
2. 在 prepare 阶段复制稳定 key bytes，计算删除后的 accounting delta。
3. commit 时移除 directory entry，并释放 entry 与 key resources。
4. 结算 ledger，再释放 superseded value，并按提示回收空 native page。

因此惰性过期不是单纯的优化：读取可能完成物理删除和记账。只有成功取得 live record 的 LRU 路径才会更新访问时钟。

## 有界主动清理

`YierdisDbExpirationSupport.cleanupExpired(...)` 只消费 expires 索引的队首：每次取出最早到期的索引项，直到队首 deadline 晚于当前时间。清理成本与"已过期 key 数（加队首 stale 项数）"成正比，与总 key 数无关。单次调用的硬边界是：

- 最多回收 `20` 个过期 key（stale 项的丢弃不占这个名额）；
- 同时受 `expireCleanupTimeLimitNanos` 限制，但每次调用至少处理一个队首项。

每个候选在回收前做三道惰性校验：按索引项持有的 key handle 解析当前 entry（key 已删除时 native 句柄失效，直接判 stale）、entry 仍持有同一 key identity、entry 的真实 `expireAtMillis` 与索引项一致。任一不满足即丢弃索引项；re-SET 改 TTL、`PERSIST`、overwrite、删除重建产生的 stale 项都不会误删新状态。校验通过后仍走 `YierdisDbKernel.reclaimExpired(...)` 这笔完整 reclamation mutation，提交前它会再校验一次完整 record。

索引项的移除规则与 mutation 失败边界一致：

- 回收成功或证实 stale 后才移除队首项；
- 候选仍过期但本次未能删除时保留队首项，下一次调用重试同一候选；
- 回收抛异常时队首项同样保留，异常向上传播。

flushDb / flushDbAsync 在 commit 时清空整个索引并重置 `expireCount`，退役目录里的 stale 索引项不跟随旧目录延迟回收。

## Maintenance 与 maxmemory

调度链如下：

```text
Netty worker timer
  -> CommandExecutor.executeMaintenance(...)
  -> YierdisInstanceRuntimeAccess.maintenanceTick()
     -> every DB: runMaintenance()
          reclaimDetachedEntries()
          -> drainExpiredWithinBudget()
          -> rehashMaintenance(...)
          -> enforceMaxmemory()
             ledger.enforceLocalMaintenance()
        if defrag enabled: defragMaintenance()
     -> enforceGlobalMaxmemoryMaintenance()
        governor present: enforceMaintenance()
        per-db scope: no-op
```

真正的 DB cleanup 只在 owner thread 上执行。expires 索引让"是否还有到期候选"成为 O(1) 判断，因此维护节拍会在时间预算内循环调用单次 cleanup，直到没有到期候选或预算耗尽；短 TTL churn 在节拍之间不会无限积压。

写 admission（local 与 global 两种 maxmemory 模式）不内联触发 expires 索引清理：预算判定只看 owned physical snapshot、trim 和 eviction。过期 key 的回收时机是维护节拍、读路径惰性过期，以及 eviction candidate selection——`allkeys-lru`/`allkeys-random` 抽样或扫描到过期 key 时把它作为最优候选，在淘汰路径上先走 expiration reclamation，而不是跳过；因此「只剩过期条目」的 keyspace 不会再把写入卡进 OOM。`noeviction` 不选 victim，过期占用只能等维护节拍或读路径惰性过期，admission 仍按 OOM 拒绝增长写入。

## 维护约束

- 不要在 entry lifecycle 之外直接改写 `expireAtMillis`；`expireCount` 与 expires 索引必须随 entry publish/replace/release 一起更新。
- expires 索引只在 deadline 实际变化时登记新项；不要为 touch 等 `expireAtMillis` 不变的替换写索引，否则读路径会把索引灌满重复项。
- 索引项的消费必须先做惰性校验（identity + 真实 deadline），不能只按索引项的 deadline 删除。
- 索引维护与消费只发生在 owner 线程；结构本身不带同步原语，不要引入跨线程访问或新锁。
- expiration reclamation 必须使用 `AdmissionMode.RECLAMATION`，upper bound 为 `0`，且不得产生正增长。

## 相关测试

- `TtlLifecycleDirectOpsTest`：`TTL/PTTL` 的 `-2/-1/>0`、`PERSIST`、即时过期和溢出饱和。
- `ActiveExpirationTest`：索引消费只访问过期候选、单次候选上限、rehash 全程排空、stale 索引项惰性校验（re-SET/PERSIST/overwrite/删除重建）、LRU/RANDOM 写 admission 回收过期占用而 noeviction 仍 OOM，以及 churn 在维护节拍间排空。
- `ExpireSemanticsTest`：各 value type 的即时过期和后续重建、`TTL/PTTL` 的 `-2` 只在 key 已不在时回答。
- `TtlMaxmemoryTest`：TTL mutation 的 maxmemory admission 与失败原子性。
- `PhysicalMemoryAccountingTest`、`ActiveExpirationTest`：deadline-only mutation 不改变物理 committed footprint。
