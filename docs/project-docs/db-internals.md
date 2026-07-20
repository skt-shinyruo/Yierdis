# DB 内部结构

本文解释单个 `YierdisDb` 内部如何组织 key、entry、value、TTL、maxmemory、memory ledger 和 introspection。

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
  keyHandle + ValueType + ValueEncoding + ValueHandle + expireAtMillis
  + legacy entry-accounting estimate(version slot) + LRU/LFU

Type root
  ValueHandle -> payload
```

`NativeKeyDirectory` 是 key 目录。它把 key bytes 保存为 allocator-backed `KEY_BYTES` object，并把 key 映射到 `EntryHandle`。它负责 lookup、insert/remove、random sample、cursor scan 和 native table stats，但不理解 value 类型，也不释放 payload。

`EntryTable` 保存 allocator-backed `ENTRY_RECORD` metadata。`EntryHandle` 包装 `ENTRY_RECORD` kind 的 stable `NativeHandle` raw value；DB 层保存的是稳定 handle，不是 native physical address。读取或替换 entry 时，`EntryTable` 通过 allocator `resolve(..., READ_ONLY/READ_WRITE)` 打开短生命周期 `NativeObjectView`，按固定 offset 读写 `EntryRecord`，然后关闭 view 释放 pin。

`EntryRecord` 是 key 的 metadata，不保存 Java collection 本体。它包含 key handle identity、`ValueHandle`、key hash、`ValueType`、`ValueEncoding`、flags、`expireAtMillis`、`version` 和 LRU/LFU 槽位。这里的 `version` 是历史遗留字段名，当前保存 entry accounting estimate，供删除和 `MEMORY USAGE` 等路径复用；它不是 mutation version，也不参与 stale-plan 判断。prepare/commit 的过期计划校验依赖 source adapter/table、generation、size 和 raw handle/value 等条件。

`ValueHandle` 也是 raw identity。string 的 `ValueHandle` 包装 allocator-backed `STRING_BYTES`；list/hash/set/zset 的 `ValueHandle` 包装对应 allocator-backed root record：`LIST_ROOT`、`HASH_ROOT`、`SET_ROOT`、`ZSET_ROOT`。这些 root record 让 DB graph 有统一入口，并通过 native handle graph 暴露 collection internal handles。

type roots 管真实 payload：

- `StringRoot` 创建、读取、修改和释放 string bytes，当前 string payload 是 allocator-backed object。
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 的 allocator-backed root record 提供稳定 root identity 和 validation。`NativeCollectionRootTable` 不使用 `Map<Long, adapter>`；它按 allocator slot id 直接定位分段 `Object[][]`，每段 4096 个槽位，并在槽内保存完整 raw handle 校验 generation 后返回 Java adapter。空段会被回收，目录和 adapter heap bytes 独立计量。
- packed collection 的 payload 不是每个元素一个 native object。`NativeListpack` 把变长编码条目保存在一个连续 `LISTPACK_BYTES` block 中，heap 上只保留 `int[]` offset topology；扩容使用 stable-handle `realloc`，staged replacement 则一次分配最终 block。Hash packed 复用同一布局，ZSet packed 使用连续 member block 加 heap `double[]` score topology。
- hash/set 的 hashtable topology 使用 `byte[]` state、`int[]` hash、`long[]` key raw handle，以及按 value layout 选择的 `long[]`、`Object[]` 或 constant value；这些 heap arrays 不进入 native handle graph。List quicklist 另外保留 allocator-backed `LIST_NODE` metadata，ZSet skiplist 使用 Java node links 和 primitive span arrays。
- ZSet skiplist 每个 member 只分配一个 canonical `ZSET_MEMBER_BYTES`。skiplist node 持有该 handle，`byMember` 是 borrowed-key `NativeByteMap`，只索引同一个 handle，remove/clear/close 都不会释放 canonical member；memberStore 是唯一 owner。Hash/Set 的 owning-key map 则负责释放自己的 field/member handles。

collection root identity 与内部 representation 要分开看。现有 List staged mutation、ZSet existing-key `ZADD`，以及 `HASH_HT` 的 HSET delta 都保持原 `ValueHandle`，只在 adapter 内发布 packed block、topology 或槽位变化；同值写入可以直接成为 no-op。packed Hash 和当前 Set 的 replacement 路径仍可能创建并发布新的 root handle，因此不能把“root record 是 stable handle”误解为所有命令都永远复用同一个 `ValueHandle`。

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

- `computeWithHandleResult(...)`
- `computeIfPresentWithHandleResult(...)`
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

不同命令只在后半段分化：`TYPE` 读 `EntryRecord.type()`，`OBJECT ENCODING` 读 `EntryRecord.encoding()`，`MEMORY USAGE` 汇总 entry、key、expire index 和 root estimate，`SCAN` 通过 key lifecycle cursor 遍历 `NativeKeyDirectory`，但不会在 discovery 阶段复制本页 key bytes。

`YierdisKeyspaceOps.KeyWindow` 把 keyspace 扫描拆成 discovery 和同步 replay 两步。discovery 在 DB owner thread 上开启 bounded `SCAN` epoch，扫描有限数量的物理 slot，只记录起止游标、inspected slot 数、匹配数量、RESP 编码长度、table generation/capacity 和统一的过期判断时刻。窗口接管 epoch 所有权，因此 key 即使在窗口存活期间被删除，其 native allocation 也只会进入 quarantine，不会被立即回收或复用。

命令层完成 reply preflight 后调用一次 `KeyWindow.emitTo(...)`。它按同一起始游标和 slot 范围同步重放扫描，逐个构造指向 allocator-backed `KEY_BYTES` 的 `NativeBytesSlice`；slice 写入 `BulkStringSink` 时才打开短生命周期只读 view/pin，写完立即关闭，而不是把每个 key 复制成长期存活的 Java `byte[]`。replay 还会核对匹配数量、额外匹配和结束游标，防止 discovery 与输出看到不一致的窗口。

`KeyWindow` 自身不逐 key 持有 retained pin；它持有的是 scan epoch。`emitTo(...)` 只保证 sink 在调用期间同步消费 slice，随后命令层把 window 的 close 所有权转交给 `RedisReplyWriter`：同步 writer 当场关闭，网络 writer 则由 reply slot/source cleanup 在回复生命周期结束时关闭。`close()` 最终释放 epoch，使 quarantine 中已不可达的 key allocation 可以回收。这个边界既避免预先复制整页 key，也要求任何新增 writer 保持 `BulkStringSink` 的同步消费和 reply source 的确定性关闭语义。

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
        -> NativeAllocationScope.begin()
        -> plan.prepare()
           -> allocate detached native payload / heap topology
           -> validate source identity and generation
        -> ledger.reconcile(nativeGrowth + stagedNonNativeGrowth)
        -> commitStream.reserve() when publication is required
        -> prepared.commit()
           -> publish adapter state before EntryRecord when required
           -> publish EntryRecord / key directory / TTL metadata
        -> allocationScope.promote()
        -> ledger.commit(actualDelta)
        -> commitStream.publish()
        -> prepared.releaseSuperseded()
        -> optional nativeAllocator.trimEmptyPages()
```

`YierdisDbMutationExecutor` 把新写路径统一成 `MutationPlan.upperBoundBytes()`、`MutationPlan.prepare()` 和 `PreparedDbMutation` 的 `commit()` / `releaseSuperseded()` / `abort()`。prepare 阶段完成会失败的 native allocation、replacement topology 和 planner canonicalization，并记录 source table、generation、entry raw handle/value 等前置条件；staged collection 的 commit 只交换引用、写槽位或重连预分配节点。需要 commit stream 时，executor 在触及 DB 可见状态前预留发布容量，然后固定按 commit、allocation promote、ledger settle、stream publish、release superseded、optional trim 的顺序完成写入。

失败边界以 `prepared.commit()` 开始为界。commit 开始前的失败可以依次 abort prepared resource、abort allocation scope、关闭 commit-stream reservation 并 rollback ledger reservation，旧 graph 保持可见；commit 一旦开始，executor 不再假设 mutation 可回滚，而会 best-effort 完成 allocation promote、ledger settle、superseded release 和 stream failure 标记，将 DB 标记为 degraded，并以 post-commit/result-unknown 结束请求。这个边界避免把“未能 publish stream”误报成“mutation 一定没有发生”。

upper bound 覆盖新 key/entry/root、native payload 的物理增长、allocator metadata 与 allocation-scope bookkeeping、heap topology、expire index 和编码升级结构。prepare 后 executor 用 allocation scope 实测的 `NativeAllocationGrowth.effectiveBytes()` 加 `stagedNonNativeGrowthBytes()` reconcile reservation；这是旧值与 detached replacement 同时存活时的峰值边界。`actualDeltaBytes` 表示发布并清理后的稳态逻辑增量，成功后才进入 ledger `usedBytes`，因此保守峰值不会长期污染 ledger。allocation scope 中的新 native allocation 只有 commit 后才 `promote()`；abort 只回收本次 scope 和 prepared mutation 明确拥有的资源。`shouldTrimNativePagesAfterCommit()` 只是 superseded release 之后的回收尝试提示；是否释放 committed page 要看 `MemoryReclaimResult`，maxmemory 判断仍必须重新采样 owned physical snapshot。

## TTL 和过期清理

TTL 有两条路径：

- 命令路径由 `YierdisTtlOps` 实现 `EXPIRE`、`PEXPIRE`、`EXPIREAT`、`PEXPIREAT`、`PERSIST`、`TTL`、`PTTL`。
- 批量清理由 `YierdisDbExpirationSupport.cleanupExpired(...)` 执行。

TTL 写命令仍走 `YierdisDbMutationExecutor`。首次给 key 增加 TTL 时，expire index entry 的估算成本进入 upper bound；过期时间小于等于当前时间时，TTL ops 直接删除 key。

cleanup 不扫描全部 key，而是从 `YierdisFfmExpireIndex` 采样。它会处理三类情况：entry 不存在时清掉脏索引；entry TTL 与 index 不一致时修正索引；key 已过期时通过 lifecycle 删除完整 key graph。循环会被过期比例、最大轮数和时间预算限制。

## 更细的 TTL 行为

本页只保留 DB storage graph 和 lifecycle 总览。TTL 命令写路径、`liveEntryRecord(...)` 的 lazy expire、`cleanupExpired(...)` 的 sample/budget，以及 `EntryRecord.expireAtMillis` 与 expire index 的双写约束见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## maxmemory 和 memory ledger

`YierdisDbMemoryLedger` 维护逻辑账本 `usedBytes` 和预算窗口 `reservedBytes`。`usedBytes` 按 mutation 的 `actualDeltaBytes` 增减，不代表 allocator 当前实际 committed 的物理字节；`reservedBytes` 表示预算已通过但 mutation 还没 commit 的窗口。maxmemory enforcement 使用当前 DB 独占的 `MemoryUsageSnapshot`，口径固定为 `heap estimate + native metadata committed + native data committed`。

per-DB maxmemory scope 下，`reserve(...)` 的顺序是：

1. 有 maxmemory 时先 cleanup expired。
2. 本次 upper bound 大于总 limit 时直接 OOM。
3. 按 `maxmemoryBytes - estimatedExtraBytes` 计算写入前必须压到的目标。
4. 当前 owned physical snapshot 超限时调用 `YierdisDbMaxmemorySupport.evictUntilUnder(...)`；该入口先尝试 trim empty native pages 并重新采样。
5. 重新采样后仍超限时，`noeviction` 才拒绝增长型写入；`allkeys-random` / `allkeys-lru` 则继续选择 victim，并在释放和 trim 后再次采样。
6. 最终仍超限则 OOM；通过后创建 reservation。

`YierdisDbMaxmemorySupport` 在 owner thread 内选 victim：random 策略随机采样，LRU 策略按 samples 选择 LRU clock 最小的 key；samples 覆盖所有 key 时可以扫描最佳候选，减少测试不稳定性。删除 victim 仍走 lifecycle，ledger delta callback 会扣减 usage。

global maxmemory scope 下，ledger 把预算准备委托给 instance 级 `YierdisGlobalMaxmemoryGovernor.prepareWrite(...)`。governor 汇总各 DB participant 的 owned `MemoryUsageSnapshot`，跨 DB cleanup/trim/evict，并以这些 participant snapshots 作为唯一的 enforcement 输入。shared runtime counter 只用于 runtime lifecycle 和 leak 诊断，不再作为另一份使用量加到全局总数。

## 更细的 maxmemory 行为

ledger reservation、`usedBytes` / `reservedBytes` 口径、per-DB 与 global scope、victim 选择和 OOM 路径见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## memory / object introspection

观测 facade 包括 `MemoryOps`、`DbLifecycleOps` 和 `ExpirationManager`，主要实现是 `YierdisDbMemoryOps`、`YierdisDbMemoryReporter`、`YierdisDbIntrospection`、`YierdisDbLifecycleOps` 和 `YierdisDbExpirationManager`。

`MEMORY USAGE` / `MEMORY STATS` 汇总的是 explainable estimate，不是 JVM instrumentation object graph。来源包括：

- ledger 的逻辑 used/reserved bytes
- owned `MemoryUsageSnapshot` 与 `NativeAllocatorStats` / allocator off-heap usage
- allocator-backed `ENTRY_RECORD` 和 `KEY_BYTES`，主要通过 allocator stats 体现，而不是作为 `EntryTable` / `NativeKeyDirectory` 的独立重复加项
- expire index/native adapter estimates where applicable
- type root estimated bytes
- key count、expire count、rehash 状态

`YierdisMemoryStats` 里容易混淆的字段：

- `usedBytesForMaxmemory`：owned physical snapshot 的 `heap estimate + native metadata committed + native data committed`。
- `reservedBytes`：已 reserve、未 commit 的预算。
- `effectiveUsedBytesForMaxmemory`：`usedBytesForMaxmemory + reservedBytes`。
- `heapDataBytesEstimate`：DB/value 层估算，不是 JVM heap 精确值。
- `offHeapUsedBytes`：allocator usage 加 DB 内部 native structure usage。
- `offHeapIncludedInMaxmemory`：当前 stats 的 maxmemory 口径包含 committed native bytes。global scope 直接汇总每个 DB 的 owned snapshot，不额外叠加 shared runtime counter。

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

## Production Mutation Outcome

reply preflight 是 mutation 之前的一个独立安全边界：可以精确估算的回复先完成 admission，再进入 mutation executor。若 mutation 已经 commit 或失败点无法判断客户端是否已看到结果，执行层不能猜测成功或失败，而是关闭连接作为 result-unknown。该行为与 maxmemory rollback、native allocation accounting、回复所有权和客户端恢复步骤的完整说明在 [`production-hardening-operations.md`](./production-hardening-operations.md)。
