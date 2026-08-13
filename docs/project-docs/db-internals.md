# DB 内部结构

本文解释单个 `YierdisDb` 如何组织 key、entry、value、TTL、mutation、maxmemory 和生命周期。它不是一张并发 `Map<byte[], Object>`，而是一个受 owner thread 约束的状态 owner。

## 组合边界

command 层只依赖 `DbEngine` 及其 reads/writes/expiration/memory/lifecycle 能力。runtime 通过 `DbEngineFactory` 创建 `RuntimeDbEngine`，生产实现的唯一公开组合入口是 `YierdisDbEngineFactory`。

```text
YierdisInstance
  -> YierdisDbEngineFactory.create(DbEngineConfig)
     -> StableMemoryBackendFactory.create("db-N", ...)
     -> YierdisDb.create(...)
        -> YierdisDbStorage
        -> YierdisDbOperationViews
        -> YierdisDb
```

`DbEngineConfig` 是 DB 配置的唯一输入。`YierdisDb` 在私有构造器内直接组装 ledger、mutation executor 和 maintenance；`YierdisDbStorage` 只聚合 storage ownership，`YierdisDbOperationViews` 只聚合公开 operation views。构造失败与正常 shutdown 都由同一 storage ownership 路径关闭 backend，原始失败保持为 primary，清理失败附加为 suppressed。

global/per-db maxmemory 只改变预算协调方式。每个 DB 都有独立的 stable-memory backend/runtime、keyspace、entry table、roots 和 ledger。

## Storage graph

核心对象图如下：

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

`NativeKeyDirectory` 保存 allocator-backed `KEY_BYTES`，并把 key 映射到 `EntryHandle`。它负责 lookup、insert/remove、random candidate、cursor scan 和 table maintenance，不理解 value 类型，也不释放 payload。

`EntryTable` 把每个 `EntryRecord` 编码进 72-byte `ENTRY_RECORD`。key/value handle 各占 16 bytes，显式保存 `allocatorId` 与 `localRaw`；其余字段保存 key hash、type、encoding、flags、TTL、version 和 LRU/LFU clock。

公共 `NativeHandle` 是 `(allocatorId, localRaw)` 的 paired stable identity，不是 physical address，也不是一个全局 packed long。只有 FFM backend 私有的 `localRaw` 由 `YierdisLocalHandleCodec` 编码 slot/generation/kind/domain；DB 边界不能丢掉 `allocatorId`。

`EntryRecord.version` 在语义 mutation（新 record、TTL 或 flags 变化）时递增；纯 access-clock touch 保留原 version。prepared mutation 和 active expiration 用它与 handle、deadline、source state 一起识别 stale candidate；entry accounting 由 `YierdisDbMemoryEstimator` 计算，不存放在 version 字段中。

`ValueHandle` 同样包装完整 `NativeHandle`。string 指向 `STRING_BYTES`；collection 指向对应 root record，再由 root/adapter 持有 packed block、node 或 hash topology。DB graph 只保存 stable identity，realloc/defrag 的 page、offset 和 location 发布属于 memory backend。

## Key lifecycle

`YierdisDbKeyLifecycle` 统一拥有 directory、entry table、type roots 和派生状态。主要职责是：

- 解析 `KeyHandle` / `EntryHandle` / `EntryRecord`；
- 为新 key staged insert 分配 key 与 entry；
- 发布、替换和释放 entry；
- 按 value type 释放 payload/root；
- 更新 TTL 派生计数和等待物理删除计数；
- 更新 LRU/LFU clock；
- 提供 bounded key-directory scan。

ops 不直接组合 directory 与 entry table。删除必须让 directory entry、entry record、value/root 和 key allocation 一起收敛；替换必须在 source identity 仍匹配时才发布。

`EntryRecord.expireAtMillis` 是唯一 TTL deadline。`expireCount` 只是随 entry publish/replace/release 更新的派生计数，不是独立索引。

## Runtime internals 与 facade

`YierdisDbRuntimeInternals` 是 concrete ops 共用的窄内部能力：owner check、mutation executor、key lifecycle 和 ledger。它没有第二实现，也不作为公开 SPI；请求级 `MutationContext` 不进入长期 DB graph。

`YierdisDbWrites.withMutationContext(...)` 只创建一个 immutable contextual view。这个 view 同时实现八个 family write interface，复用已经构造好的 family implementation，并把 context 显式传给 mutation executor；它不会重建 internals 或 family modules，也不会临时改写共享字段。`CommandDb` 在请求入口绑定并缓存该 view，因此两个 contextual write/lifecycle views 可以交错使用。

prepared set/pop 的 `commit(context)` 以显式 commit context 为准。lazy expiry、active expiry 和 eviction 则使用 executor 的无用户 context overload，发布 `EXPIRED` / `EVICTED` synthetic `DEL`，不会继承用户 command context。

## 读路径

常规读路径是：

```text
DbReads
  -> Yierdis*Ops
  -> YierdisDbRuntimeInternals.liveEntryRecord(...)
  -> type/encoding check
  -> EntryRecord.valueHandle()
  -> type root read
```

`liveEntryRecord(...)` 比较 `expireAtMillis`。live record 正常返回；过期 record 触发 `reclaimExpired(...)` 并对调用方隐藏。reclamation 是完整 mutation，可能删除 graph、结算 ledger 和发布 `EXPIRED` commit。只有成功取得 live record 的 LRU 路径才 touch clock。

需要拥有结果的 API 会复制 bytes；callback-scoped streaming 可以使用短生命周期 native view。`SCAN` 的 `KeyWindow` 先在 bounded epoch 内 discovery，再按同一 cursor/window 同步 replay 到 sink；window close 后 epoch 才释放，不能让 slice 或 view 逃逸。

## 写路径

增长型写入不能先改 graph 再检查 maxmemory。标准路径如下：

```text
estimate upper bound
  -> YierdisDbMutationExecutor.execute(plan)
     -> ledger.reserve(upperBound)
     -> NativeAllocationScope.begin()
     -> plan.prepare()
     -> ledger.reconcile(measured peak)
     -> commitStream.reserve() when required
     -> prepared.commit()
     -> allocationScope.promote()
     -> ledger.commit(actualDelta)
     -> commitStream.publish()
     -> prepared.releaseSuperseded()
     -> optional page trim
```

prepare 完成可能失败的 native allocation、replacement topology 和 source validation。`PreparedEntryMutation` 用 unchanged/insert/replace/delete 表达 entry 转换，并只在 value representation 需要时附加 abort、before-publish 或 superseded-release hook。

失败边界以 `prepared.commit()` 开始为界：

- commit 前失败会 abort prepared resources 与 allocation scope、释放 stream reservation，并 rollback ledger reservation；旧 graph 保持可见。
- commit 后失败不再宣称 mutation 未发生。executor best-effort promote/settle/release，标记 stream failure 和 DB degraded，并以 result-unknown 结束请求。

upper bound 覆盖新 key/entry/root、native payload、allocator metadata、allocation-scope bookkeeping、heap topology 和编码升级。prepare 后用实测 native growth 加 staged non-native growth 收窄 reservation；`actualDeltaBytes` 只表示提交后的逻辑增量。是否真正回收 committed page 必须以 trim result 和重新采样的 physical snapshot 为准。

## TTL 与主动清理

TTL 命令由 `YierdisTtlOps` 通过 prepared entry replacement/delete 实现。设置 deadline 复用原 entry handle；`PERSIST` 把 deadline 改为 `-1`；已经到期的输入直接准备删除。

`YierdisDbExpirationSupport` 持久化 cursor 与完整 table generation，直接扫描 `NativeKeyDirectory`。单次最多检查 320 个 physical slots、收集 20 个候选，并受时间预算限制。扫描 callback 只发现候选；返回后按 key identity、record version 和 deadline 重新验证再删除。

详细的 retry、rehash dedup 和 synthetic commit 语义见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## Maxmemory 与 ledger

`YierdisDbMemoryLedger` 维护：

- `usedBytes`：按 committed mutation `actualDeltaBytes` 更新的逻辑账本；
- `reservedBytes`：admission 已通过但尚未 settle 的预算窗口。

enforcement 使用当前 DB 独占 `MemoryUsageSnapshot`：

```text
heap estimated
  + native metadata committed
  + native data committed
```

per-db scope 先 cleanup expired，再按 `maxmemoryBytes - estimatedExtraBytes` trim/resample/evict。global scope 把相同 participant 操作交给 `YierdisGlobalMaxmemoryGovernor`，由它跨 DB 汇总 snapshots 和挑选 victim。各 DB backend runtime counter 只用于 lifecycle 诊断，不作为第二套 global usage source。

`noeviction` 不选 victim；`allkeys-random` 随机取候选；`allkeys-lru` 比较 `EntryRecord.lruOrLfu()`。过期候选先按 `EXPIRED` reclaim，真正 victim 通过 `YierdisDbRuntimeInternals.evict(...)` 发布 `EVICTED` synthetic delete。

更完整的 admission、OOM 和 result-unknown 边界见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## Memory 与 introspection

`YierdisDbMemoryReporter` 和 `YierdisDbIntrospection` 通过 runtime internals/lifecycle 读取 live state。`MEMORY USAGE` / `MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph。

主要口径包括：

- owned physical snapshot 与 backend allocator stats；
- ledger logical used/reserved；
- type root estimates；
- key count、derived expire count 和 rehash state；
- `usedBytesForMaxmemory = heap + native metadata committed + native data committed`；
- `effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + reservedBytes`。

native reclaimable bytes 只是候选量，不能预先从 committed footprint 扣除。显式 introspection 需要返回 owned bytes/result object 时会 materialize heap copy。

## Owner 与 shutdown

`DbThreadGuard` 强制单 owner 语义：未绑定访问、foreign-thread 访问/关闭、CLOSING 和 CLOSED 状态都 fail fast；同 owner 重复 bind/close 保持既定契约。

Netty I/O 线程只提交请求，command executor owner thread 执行 DB mutation 和 maintenance。`Arena.ofShared()` 只允许 FFM region 在不同线程关闭，不解除 DB thread confinement。

shutdown 会先重置 ledger、回收 detached entries，再让 storage owner 清空 graph 并关闭 backend。close-once 与失败聚合由 owned resources 保证；构造失败也沿同一 ownership 路径清理已创建的 graph。

## 修改导航

- DB 组装：`YierdisDbEngineFactory`、`YierdisDb`、`YierdisDbStorage`、`YierdisDbOperationViews`。
- key/entry/value 生命周期：`YierdisDbKeyLifecycle`、`EntryTable`、type roots。
- mutation 与预算：`YierdisDbMutationExecutor`、`PreparedEntryMutation`、`YierdisDbMemoryLedger`。
- TTL：`YierdisTtlOps`、`YierdisDbExpirationSupport`、`YierdisDbRuntimeInternals.reclaimExpired(...)`。
- maxmemory：`YierdisDbMaxmemorySupport`、`YierdisGlobalMaxmemoryGovernor`。
- memory backend：`YierdisFfmStableMemoryBackend` 与 [`native-memory-runtime.md`](./native-memory-runtime.md)。

边界不要跨越：command 不依赖具体 `YierdisDb`；ops 不绕过 mutation executor 做增长型写入；删除不绕过 lifecycle；DB 不缓存 physical location 或长生命周期 `NativeObjectView`。
