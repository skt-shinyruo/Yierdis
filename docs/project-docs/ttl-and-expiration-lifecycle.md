# TTL 与过期生命周期

本文解释 TTL 命令、惰性过期和有界主动清理。当前实现只有一份 TTL 状态：`EntryRecord.expireAtMillis`。读写与 maintenance 都围绕同一条 entry graph 工作，不再维护独立的过期索引。

## TTL 状态

`EntryRecord.expireAtMillis` 是每个 key 的 TTL 权威字段：

- 负值表示 persistent；
- 非负值表示绝对毫秒时间戳；
- `TTL`、`PTTL`、惰性过期、主动清理和 maxmemory cleanup 都读取这个字段。

`YierdisDbKeyLifecycle.expireCount()` 是随 entry 发布、替换和释放同步更新的精确派生计数，用于 cleanup fast path、`MEMORY STATS`、instance observability 和 flush outcome。它不是第二份 key-to-deadline 状态，也不参与查找。

## TTL 命令写路径

TTL 写命令和 `SET ... EX/PX/EXAT/PXAT/KEEPTTL` 最终都通过 mutation executor 提交新的 `EntryRecord`：

```text
command
  -> YierdisTtlOps / YierdisStringOps
  -> YierdisDbKernel.execute(MutationUse)
  -> internal YierdisDbMutationExecutor adapter
     -> reserve upper bound
     -> prepare replacement or deletion
     -> commit EntryRecord
     -> settle ledger and release superseded resources
```

关键分支如下：

- 设置或更新 TTL 会复用原 `EntryHandle`，只准备并发布新的 `EntryRecord`；upper bound 只包含 allocation-scope bookkeeping，不存在额外 TTL 数据结构 allocation。
- `PERSIST` 使用 reclamation admission，upper bound 为 `0`，成功时把 deadline 改为 `-1`，结果为 `TTL_CHANGED`。
- `EXPIRE 0`、`PEXPIRE 0` 或已经到期的绝对时间不会写入一个过期 deadline，而是准备删除当前 entry。
- key 缺失或在提交前已发生变化时，prepared mutation 返回 unchanged，不覆盖较新的 record。
- 相对或绝对时间计算溢出时 deadline 饱和到 `Long.MAX_VALUE`。
- 读命令保持 Redis 兼容结果：key 不存在或已过期为 `-2`，persistent 为 `-1`，其余返回剩余时间。

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

`YierdisDbExpirationSupport.cleanupExpired(...)` 直接渐进扫描 `NativeKeyDirectory`，不构建单独的 TTL 数据结构。每次调用的硬边界是：

- 每个 scan chunk 最多检查 `32` 个物理 slot；
- 单次 cleanup 最多检查 `320` 个物理 slot；
- 单次最多收集 `20` 个过期候选；
- 同时受 `expireCleanupTimeLimitNanos` 限制。

cleanup 保存 `ScanCursorV2` 和完整的 key-directory `tableGeneration`。generation 改变时从头开始，避免旧 cursor 在新表拓扑上继续。扫描 callback 只收集候选，退出目录遍历后才执行删除；rehash shadow 可能重复暴露 key，因此候选按完整 native identity 去重。

每个候选在回放前都会重新校验 key bytes、native identity、record/version 和 deadline。TTL 被延后、record 被替换或 key 被重建时，旧候选只会被判为 stale，不会误删新状态。

cursor 的提交规则与 mutation 失败边界一致：

- 候选已删除或已证实 stale 后才保存 next cursor；
- commit 前失败或当前过期 entry 尚未删除时保留 batch start，供下一次重试；
- commit 后失败保留已经完成的删除，并保存 next cursor，避免重复处理同一批次。

`expireCount == 0` 时 cleanup 快速返回并重置 cursor。只要目录拓扑最终稳定，连续调用会遍历完整 keyspace；单次调用始终受 slot、候选和时间预算限制。

## Maintenance 与 maxmemory

调度链如下：

```text
Netty worker timer
  -> CommandExecutor.executeMaintenance(...)
  -> YierdisInstanceRuntimeAccess.maintenanceTick()
     -> every DB: cleanupExpired() -> defragMaintenance()
     -> per-db scope: enforceMaxmemoryMaintenance()
     -> global scope: instance-level governor maintenance
```

真正的 DB cleanup 只在 owner thread 上执行。maxmemory admission 也先调用 cleanup，使刚过期的数据能在同一次预算判断中释放。

## 维护约束

- 不要在 entry lifecycle 之外直接改写 `expireAtMillis`；`expireCount` 必须随 entry publish/replace/release 一起更新。
- discovery callback 内不要删除目录项；候选必须在扫描返回后重新验证并回放。
- 不要只保存 cursor 的编码值；必须同时比较完整 `tableGeneration`。
- expiration reclamation 必须使用 `Admission.RECLAMATION`，upper bound 为 `0`，且不得产生正增长。

## 相关测试

- `TtlLifecycleDirectOpsTest`：`TTL/PTTL` 的 `-2/-1/>0`、`PERSIST`、即时过期和溢出饱和。
- `ActiveExpirationTest`：有界 slot/candidate 扫描、cursor 推进、rehash 去重、stale 候选、generation 重置，以及 commit 前重试和 commit 后失败。
- `ExpireSemanticsTest`：各 value type 的即时过期和后续重建。
- `TtlMaxmemoryTest`：TTL mutation 的 maxmemory admission 与失败原子性。
- `PhysicalMemoryAccountingTest`、`ActiveExpirationTest`：deadline-only mutation 不改变物理 committed footprint。
