# DB Internals

本文解释单个 `YierdisDb` 内部如何组织 key、entry、value、TTL、maxmemory、memory ledger 和 introspection。

## 先记住一句话

`YierdisDb` 不是一张简单的 `Map<byte[], Object>`，而是单 DB 的状态 owner：key bytes、entry metadata、value payload、TTL index、memory ledger、maxmemory eviction 和 introspection 都在这里按同一套生命周期协调。

主对象图可以简化成：

```text
YierdisDb
  -> NativeKeyDirectory
  -> EntryTable
  -> StringRoot / ListRoot / HashRoot / SetRoot / ZSetRoot
  -> YierdisFfmExpireIndex
  -> YierdisDbKeyLifecycle
  -> YierdisDbMutationExecutor
  -> YierdisDbMemoryLedger
  -> YierdisDbExpirationSupport
  -> YierdisDbMaxmemorySupport
  -> YierdisDbMemoryReporter / YierdisDbIntrospection
```

如果改 DB 时绕过这些协作者，最常见的问题不是命令返回错，而是 TTL metadata、payload release、ledger accounting 或 owner-thread 语义被悄悄破坏。

## 从 YierdisInstance 到 YierdisDb

生产启动和 embedded 启动都不是让命令层直接创建 `YierdisDb`。runtime 先创建 `YierdisInstance`，再按 DB 数量和 memory scope 装配 `RuntimeDbEngine[]`，每个 runtime DB engine 持有一个 `YierdisDb`。

边界要这样看：

- `YierdisInstance` 管多 DB、shared FFM runtime、global/per-db maxmemory 和 runtime lifecycle。
- `RuntimeDbEngine` 是 runtime 看到的 DB contract，包含 `DbEngine` facade、owner-thread binding、shutdown、maintenance 和 maxmemory participant hook。
- `DbEngine` 是 command 层看到的能力视图，只暴露 `reads()`、`writes()`、`expiration()`、`memory()` 和 `lifecycle()`。

命令实现通常走：

```text
CommandSupport
  -> DbEngine / DbReads / DbWrites
  -> YierdisDbReads / YierdisDbWrites
  -> YierdisStringOps / YierdisHashOps / ...
```

因此 command 层不应该 import `YierdisDb` 具体实现；跨 DB 的 global maxmemory 也通过 `MaxmemoryCoordinator` / `MaxmemoryParticipant` SPI 协调，而不是让 runtime 直接改 DB 内部字段。

## DB storage graph

当前 key/value 状态拆成四层：

```text
NativeKeyDirectory
  key bytes -> EntryHandle(raw NativeHandle)

EntryTable
  EntryHandle -> ENTRY_RECORD

EntryRecord
  keyHandle + ValueType + ValueEncoding + ValueHandle + expireAtMillis + version + LRU/LFU

Type root
  ValueHandle -> payload
```

`NativeKeyDirectory` 是 key 目录。它把 key bytes 保存为 allocator-backed `KEY_BYTES` object，并把 key 映射到 `EntryHandle`。它负责 lookup、insert/remove、random sample、cursor scan 和 native table stats，但不理解 value 类型，也不释放 payload。

`EntryTable` 保存 allocator-backed `ENTRY_RECORD` metadata。`EntryHandle` 包装 `ENTRY_RECORD` kind 的 stable `NativeHandle` raw value；DB 层保存的是稳定 handle，不是 native physical address。读取或替换 entry 时，`EntryTable` 通过 allocator `resolve(..., READ_ONLY/READ_WRITE)` 打开短生命周期 `NativeObjectView`，按固定 offset 读写 `EntryRecord`，然后关闭 view 释放 pin。

`EntryRecord` 是 key 的 metadata，不保存 Java collection 本体。它包含 key handle identity、`ValueHandle`、key hash、`ValueType`、`ValueEncoding`、flags、`expireAtMillis`、version 和 LRU/LFU 槽位。

`ValueHandle` 也是 raw identity。string 的 `ValueHandle` 包装 allocator-backed `STRING_BYTES`；list/hash/set/zset 的 `ValueHandle` 包装对应 allocator-backed root record：`LIST_NODE`、`HASH_NODE`、`SET_NODE`、`ZSET_NODE`。这些 root record 让 DB graph 有统一入口，但不能过度理解成所有 collection internals 都已经完全 allocator-object 化。

type roots 管真实 payload：

- `StringRoot` 创建、读取、修改和释放 string bytes，当前 string payload 是 allocator-backed object。
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 的 allocator-backed root record 提供稳定 root identity 和 validation；`NativeCollectionRootTable` 再用 native handle raw value 映射到 Java adapter，从而通过 `ValueHandle` 找回 payload implementation。
- `ListRoot` 的 quicklist 节点 metadata 有 allocator-backed `LIST_QUICKLIST_NODE` records。
- collection payload bytes、list entry bytes 以及 hash/set/zset 内部结构仍可能由 adapter 或 legacy FFM structures 拥有；active defrag 不移动 adapter-owned payload bytes，也不把任意 collection internal identity 当作 allocator object resolve。

这些事实要和 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) 保持一致：DB hot path 只保存 stable handle，物理 page、offset、packed address、pin、epoch、quarantine、`realloc` 和 defrag 都属于 allocator 语义。

## Key lifecycle

`YierdisDbKeyLifecycle` 是 key 生命周期协调者。它把 `BytesView` lookup、`NativeKeyDirectory`、`EntryTable`、`EntryRecord`、expire index、type roots、LRU clock 和 ledger delta callback 串在一起。

读侧常用方法包括：

- `keyHandle(...)`
- `entryRecord(...)`
- `liveEntryRecord(...)`
- `removeIfExpired(...)`
- `touchRecord(...)`

`liveEntryRecord(...)` 不是普通 get。它会确认 key 仍在目录里，检查 `EntryRecord.expireAtMillis`，必要时通过 lifecycle 惰性删除过期 key，最后才返回 live `EntryRecord`。需要记录访问的读命令再调用 `touchRecord(...)` 更新 LRU/LFU 槽位。

写侧常用方法包括：

- `computeWithHandle(...)`
- `computeIfPresentWithHandle(...)`
- `newRecord(...)`
- `removeEntry(...)`

ops 不直接拼 directory 和 entry table。它们通过 lifecycle 取得 key handle 和旧 record，构造新 record 后交回 lifecycle。lifecycle 负责新 key 分配 `EntryHandle`、替换 `EntryRecord`、删除 directory entry、释放 entry record、释放旧 payload、同步 expire index，并通过 ledger delta callback 修正 accounting。

TTL metadata 也在这里收口：设置或移除 TTL 必须同时更新 `EntryRecord.expireAtMillis` 和 `YierdisFfmExpireIndex`。只改其中一边会让 `TTL`、lazy expiration、cleanup、snapshot 和 memory stats 看到不同状态。

## 读路径

读路径主线是：

```text
command
  -> CommandSupport.dbReads(ctx)
  -> DbReads.<family>()
  -> Yierdis*Ops read method
  -> YierdisDbKeyLifecycle.liveEntryRecord(...)
  -> require type
  -> EntryRecord.valueHandle()
  -> Type root read
```

以 `GET` 为例，命令层拿到 key 参数后，DB API 接受 `BytesView`，`YierdisStringOps` 调用 lifecycle 找 live string record，再通过 `StringRoot` 读 `BytesSlice` 或复制值。

读路径隐含做三件事：

- `checkThread()` 确认当前线程是 DB owner。
- lazy expiration 把已过期 key 当作不存在，并顺手释放 entry、key、TTL metadata 和 payload。
- LRU/LFU policy 启用时更新访问槽位。

不同命令只在后半段分化：`TYPE` 读 `EntryRecord.type()`，`OBJECT ENCODING` 读 `EntryRecord.encoding()`，`MEMORY USAGE` 汇总 entry、key、expire index 和 root estimate，`SCAN` 通过 key lifecycle cursor 遍历 `NativeKeyDirectory` 并在 bounded epoch 内复制要返回的 key bytes。

## 写路径

写路径的核心约束是：不能先改结构，再补做 maxmemory 检查。

标准写路径是：

```text
command
  -> CommandSupport.dbWrites(ctx)
  -> DbWrites.<family>()
  -> Yierdis*Ops write method
  -> estimate upper bound
  -> YierdisDbMutationExecutor.execute(plan)
     -> YierdisDbMemoryLedger.reserve(upperBound)
     -> plan.apply()
        -> YierdisDbKeyLifecycle.computeWithHandle(...)
        -> Type root mutation
        -> EntryRecord replacement / delete
        -> TTL metadata update
     -> ledger.commit(actualDelta)
```

`YierdisDbMutationExecutor` 把 mutation 统一成 `MutationPlan.upperBoundBytes()`、`MutationPlan.apply()` 和 `MutationResult.actualDeltaBytes()`。它先检查 owner thread，再 reserve ledger 预算，执行实际 mutation，成功时按实际 delta commit；ledger OOM、off-heap OOM 或 runtime exception 时 rollback reservation，并把 maxmemory OOM 映射成 Redis 风格错误。

upper bound 覆盖可能增长的新 key bytes、entry record、value payload、expire index entry、编码升级额外结构等。mutation 后再用 actual delta 修正，避免保守估算长期污染 `usedBytes`。

## TTL 和过期清理

TTL 有两条路径：

- 命令路径由 `YierdisTtlOps` 实现 `EXPIRE`、`PEXPIRE`、`EXPIREAT`、`PEXPIREAT`、`PERSIST`、`TTL`、`PTTL`。
- 批量清理由 `YierdisDbExpirationSupport.cleanupExpired(...)` 执行。

TTL 写命令仍走 `YierdisDbMutationExecutor`。首次给 key 增加 TTL 时，expire index entry 的估算成本进入 upper bound；过期时间小于等于当前时间时，TTL ops 直接删除 key。

cleanup 不扫描全部 key，而是从 `YierdisFfmExpireIndex` 采样。它会处理三类情况：entry 不存在时清掉脏索引；entry TTL 与 index 不一致时修正索引；key 已过期时通过 lifecycle 删除完整 key graph。循环会被过期比例、最大轮数和时间预算限制。

## maxmemory 和 memory ledger

`YierdisDbMemoryLedger` 维护 `usedBytes` 和 `reservedBytes`。`reservedBytes` 表示预算已通过但 mutation 还没 commit 的窗口；成功后 reservation 释放，actual delta 进入 `usedBytes`，失败时只撤销 reservation。

per-DB maxmemory scope 下，`reserve(...)` 的顺序是：

1. 有 maxmemory 时先 cleanup expired。
2. 本次 upper bound 大于总 limit 时直接 OOM。
3. 按 `maxmemoryBytes - estimatedExtraBytes` 计算写入前必须压到的目标。
4. 当前 usage 超限时，`noeviction` 直接拒绝增长型写入，`allkeys-random` / `allkeys-lru` 调用 `YierdisDbMaxmemorySupport.evictUntilUnder(...)`。
5. 仍超限则 OOM；通过后创建 reservation。

`YierdisDbMaxmemorySupport` 在 owner thread 内选 victim：random 策略随机采样，LRU 策略按 samples 选择 LRU clock 最小的 key；samples 覆盖所有 key 时可以扫描最佳候选，减少测试不稳定性。删除 victim 仍走 lifecycle，ledger delta callback 会扣减 usage。

global maxmemory scope 下，ledger 把预算准备委托给 instance 级 `YierdisGlobalMaxmemoryGovernor.prepareWrite(...)`。governor 汇总各 DB participant usage 和 shared off-heap usage source，跨 DB cleanup/evict，并避免把 shared FFM usage 在每个 DB 上重复计算。

## memory / object introspection

观测 facade 包括 `MemoryOps`、`DbLifecycleOps` 和 `ExpirationManager`，主要实现是 `YierdisDbMemoryOps`、`YierdisDbMemoryReporter`、`YierdisDbIntrospection`、`YierdisDbLifecycleOps` 和 `YierdisDbExpirationManager`。

`MEMORY USAGE` / `MEMORY STATS` 汇总的是 explainable estimate，不是 JVM instrumentation object graph。来源包括：

- ledger used/reserved bytes
- `NativeAllocatorStats` / allocator off-heap usage
- allocator-backed `ENTRY_RECORD` 和 `KEY_BYTES`，主要通过 allocator stats 体现，而不是作为 `EntryTable` / `NativeKeyDirectory` 的独立重复加项
- expire index/native adapter estimates where applicable
- type root estimated bytes
- key count、expire count、rehash 状态

`YierdisMemoryStats` 里容易混淆的字段：

- `usedBytesForMaxmemory`：参与 maxmemory 判断的 used 口径。
- `reservedBytes`：已 reserve、未 commit 的预算。
- `effectiveUsedBytesForMaxmemory`：`usedBytesForMaxmemory + reservedBytes`。
- `heapDataBytesEstimate`：DB/value 层估算，不是 JVM heap 精确值。
- `offHeapUsedBytes`：allocator usage 加 DB 内部 native structure usage。
- `offHeapIncludedInMaxmemory`：当前 stats 是否把 off-heap 纳入 maxmemory 口径；global scope 下单 DB 通常不重复计 shared off-heap。

`OBJECT ENCODING` 从 `EntryRecord.encoding()` 映射出 Redis 风格名称，例如 `int`、`embstr`、`raw`、`listpack`、`hashtable`、`intset`、`quicklist`、`skiplist`。显式 introspection 会在需要输出 bytes 或构造结果对象时 materialize heap copy，这是有意边界，不应泄漏长期 native view。

## owner thread 约束

`YierdisDb` 不是线程安全并发容器。`DbThreadGuard` 强制单线程 DB 语义：

- `bindToCurrentThread()` 把 DB 绑定到 owner thread。
- `checkThread()` 在未绑定、已关闭或跨线程访问时 fail fast。
- `checkThreadForShutdown()` 防止非 owner thread 关闭已绑定 DB。

生产路径里 DB owner thread 是 command executor 线程。Netty I/O 线程只提交请求，不直接访问 DB；maintenance task 也通过 executor 调回 owner thread 执行。这一约束让 `NativeKeyDirectory`、`EntryTable`、type roots、expire index 和 ledger 可以保持简单，不需要在每个结构内部再实现多线程写锁。

## 改 DB 前先看什么

按修改目标定位源码：

- 改实例、多 DB、runtime 归属或 global maxmemory：看 `YierdisInstance`、`YierdisDbEngineFactory`、`YierdisGlobalMaxmemoryGovernor`。
- 改单 DB 对象图装配：看 `YierdisDb`、`YierdisDbComponentFactory`、`YierdisDbStorageComponents`。
- 改 key 生命周期、删除、TTL metadata、payload release：看 `YierdisDbKeyLifecycle`。
- 改写路径预算、OOM、commit/rollback：看 `YierdisDbMutationExecutor` 和 `YierdisDbMemoryLedger`。
- 改 native key/entry/value layout：看 `NativeKeyDirectory`、`EntryTable`、`EntryRecord`、`EntryHandle`、`ValueHandle` 和 type roots。
- 改过期清理：看 `YierdisDbExpirationSupport` 和 `YierdisFfmExpireIndex`。
- 改 memory/object 命令：看 `YierdisDbMemoryReporter`、`YierdisDbIntrospection` 和 `DbMemoryAccounting`。

边界也要守住：command 层不要依赖 `YierdisDb`；ops 不要绕过 mutation executor 做增长型写入；删除 key 不要绕过 lifecycle；TTL index 和 `EntryRecord.expireAtMillis` 不要只更新一边；DB hot path 不要缓存 allocator physical address 或长生命周期 `NativeObjectView`。

## 推荐测试

- `YierdisInstanceTest`：多 DB、owner thread 和 runtime 行为。
- `GlobalMaxmemoryLruAcrossDbsTest`：global maxmemory 跨 DB 选择 LRU victim。
- `MutationExecutorReservationTest`：reservation rollback、noeviction 拒绝和 no-op mutation。
- `MemoryLedgerContractTest`：ledger reserve/commit/rollback 不变量。
- `ExpireIndexTest`：expire index 和 TTL lifecycle。
- `TtlLifecycleDirectOpsTest`：TTL、flush、memory/object API 的 direct ops 行为。
- `MemoryStatsAccountingConsistencyTest`：memory accounting 字段一致性。
- `NativeStorageRegressionTest`：native entry/key/value graph 回归。
- `NativeKeyDirectoryTest`、`EntryTableContractTest`、`EntryHandleContractTest`、`ValueHandleContractTest`：handle 和 native storage contract。
- `YierdisDbArchitectureGuardTest`：command 层和 DB 实现边界。
