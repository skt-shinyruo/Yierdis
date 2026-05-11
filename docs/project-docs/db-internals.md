# DB Internals

本文专门解释 Yierdis 的 DB 内核是怎么组织起来的。

如果你已经看过：

- [`commands-and-data-model.md`](./commands-and-data-model.md)
- [`request-execution-flow.md`](./request-execution-flow.md)
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)

那么这篇文档的目标就是把这些问题讲透：

- `YierdisDb` 到底拥有哪一批核心状态和协作者
- 一次读操作和写操作分别如何穿过 DB 内核
- TTL、内存记账、maxmemory 和 entry/root value state 是如何协作的

## 先记住一句话

`YierdisDb` 不是一个“大 Map 包装类”，而是一个“单 DB 状态 owner + 一组高密度协作者”的对象图。

如果把它只看成“key -> value 容器”，会看丢掉很多真正关键的逻辑：

- key 生命周期
- 过期索引
- mutation 记账
- maxmemory/淘汰
- value 编码升级
- owner-thread 约束

## 从 instance 到单 DB

在 server 启动或 embedded 启动时，真正创建 DB 的不是命令层，而是 `YierdisInstance`。

大致关系是：

```text
YierdisInstance
  -> RuntimeDbEngine[]
  -> YierdisDb
     -> DbReads / DbWrites / MemoryOps / ExpirationManager / DbLifecycleOps
```

这里最重要的分层是：

- `YierdisInstance`
  负责“多 DB 实例装配、资源 ownership、global/per-db maxmemory 组织”
- `YierdisDb`
  负责“单个 DB 的真实状态和策略”
- `DbEngine`
  负责把 `YierdisDb` 暴露成 command 层能依赖的能力视图

## `YierdisDb` 拥有什么

`YierdisDb` 仍然长期持有单 DB 运行所需的核心对象，但这些对象的创建不再集中在
`YierdisDb` 构造函数里。构造细节由 `YierdisDbConfig`、
`YierdisDbStorageComponents`、`YierdisDbComponents` 和
`YierdisDbComponentFactory` 收敛。

### 1. 主存储、native entry 和过期索引

当前 DB 存储图已经拆成几层：

- `EntryTable`
- `NativeKeyDirectory`
- `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot`
- `expires`

默认实现分别是：

- `EntryTable` 使用 slab allocator 保存 entry 元数据
- `NativeKeyDirectory` 保存 key bytes 并映射到 `EntryHandle`
- 各 `TypeRoot` 用 `ValueHandle` 管理具体 payload
- `YierdisFfmExpireIndex`

这说明：

- `NativeKeyDirectory` 负责 key -> entry handle
- `EntryRecord` 负责 type、encoding、value handle、expire、estimate 和 LRU 元数据
- `TypeRoot` 负责 value handle -> payload
- `expires` 负责 key -> expireAt，并同步回 entry 元数据

### 2. 线程与资源底座

- `threadGuard`
- `memoryRuntime`
- `offHeapAllocator`
- `resources`

这些对象不是业务逻辑，但它们决定了：

- 当前线程是不是 owner thread
- DB 是否能安全访问
- off-heap/native memory 资源由谁持有和关闭

### 3. 记账与策略协作者

- `ledger`
- `mutationExecutor`
- `expirationSupport`
- `maxmemorySupport`
- `keyLifecycle`

这几个对象是 DB 内核最值得理解的部分：

- `YierdisDbMemoryLedger`
  负责 reservation / commit / rollback 和 maxmemory 预算入口
- `YierdisDbMutationExecutor`
  负责把“上界估算 + 真正 mutation + 记账提交/回滚”串成受控写路径
- `YierdisDbExpirationSupport`
  负责清理过期 key
- `YierdisDbMaxmemorySupport`
  负责采样候选、淘汰和 LRU 相关策略
- `YierdisDbKeyLifecycle`
  负责 key handle、live `EntryRecord`、惰性过期删除和 TTL 元数据更新

### 4. 类型化操作对象

- `YierdisStringOps`
- `YierdisHashOps`
- `YierdisListOps`
- `YierdisSetOps`
- `YierdisZSetOps`
- `YierdisHllOps`
- `YierdisTtlOps`
- `YierdisKeyspaceOps`

这批 `*Ops` 才是命令层最终会落到的地方。

### 5. 观测和内省

- `memoryReporter`
- `introspection`

它们负责把内部状态整理成：

- `MEMORY USAGE`
- `MEMORY STATS`
- `OBJECT ENCODING`

这类命令可以消费的视图。

### 6. 面向 command 层的 facade

- `reads`
- `writes`
- `expirationManager`
- `memoryOps`
- `lifecycleOps`

这一步很关键，因为它说明命令层看到的不是 `YierdisDb` 本体，而是经过能力裁剪后的 facade。

### 7. 构造和纯工具类

为了避免 `YierdisDb` 再次变成所有细节的落点，几个非 facade 职责被放在独立类里：

- `YierdisDbConfig`
  负责校验构造参数、保存 `yierdis-db-api` maxmemory policy、计算时间预算和 LRU 开关。
- `YierdisDbStorageComponents`
  负责 FFM runtime、allocator、keyspace、expire index 和 owned resources 的组装结果。
- `YierdisDbComponents`
  负责承载 factory 返回的包内对象图 bundle。
- `YierdisDbComponentFactory`
  负责把 storage、ledger、mutation executor、key lifecycle、ops 和 facade 拼成对象图。
- `YierdisDbMemoryEstimator`
  负责 entry 估算和写入上界估算。
- `YierdisGlobMatcher`
  负责 `KEYS` / `SCAN` 使用的 Redis 风格 byte glob 匹配。

这几个类的共同点是：它们是 DB 包内部实现细节，不扩大 command 层或 runtime 层能看到的 API。

## 可以把 `YierdisDb` 想成下面这张图

```text
YierdisDb
  -> components/config/factory assemble object graph
  -> EntryTable(entry handle -> EntryRecord)
  -> NativeKeyDirectory(key -> entry handle)
  -> TypeRoot(value handle -> payload)
  -> expires(key -> expireAt)
  -> threadGuard
  -> memoryRuntime / offHeapAllocator
  -> ledger
  -> mutationExecutor
  -> expirationSupport
  -> maxmemorySupport
  -> keyLifecycle
  -> string/hash/list/set/zset/hll/ttl/keyspace ops
  -> memoryReporter / introspection
  -> reads / writes / memory / lifecycle facades
```

## 读路径是怎么走的

读路径的核心不是“直接从 Map 里 get”，而是：

1. 先定位 key
2. 检查是否过期
3. 过期就删除
4. 未过期则 touch
5. 再把对象交给具体类型逻辑处理

### `YierdisDbKeyLifecycle` 的角色

这条路径的中心基本都在 `YierdisDbKeyLifecycle`：

- `keyHandle(...)`
- `entryRecord(...)`
- `liveEntryRecord(...)`
- `removeIfExpired(...)`
- `expireAtMillis(...)`

`liveEntryRecord(...)` 的语义很重要：

- 不是简单返回 entry metadata
- 它会先做惰性过期删除
- 然后由调用方通过 `touchRecord(...)` 更新 LRU/last-access
- 最后才返回仍然有效的 `EntryRecord`

读写路径需要 entry 元数据时会先走 `liveEntryRecord(...)` 或
`entryRecord(...)`，再用 `EntryRecord.valueHandle()` 交给对应 `TypeRoot`。

这意味着很多读路径天然就带有：

- lazy expiration
- LRU 触摸

这也是为什么 TTL 逻辑不会只存在于后台 cleanup 里。

### 典型读链

例如 string 读取，逻辑大致是：

```text
command
  -> DbReads.strings()
  -> YierdisStringOps.getStringValue(...)
  -> keyLifecycle.liveEntryRecord(...)
  -> EntryRecord.valueHandle()
  -> StringRoot
```

如果你在追 `TTL`、`TYPE`、`MEMORY USAGE`，最终也会走到类似的
`BytesView -> keyHandle -> EntryRecord -> TypeRoot` 路径。

## 写路径是怎么走的

真正体现 DB 内核设计的，是写路径。

对 Yierdis 来说，写操作通常不是：

- “直接改对象，再顺手记账”

而是：

- “先估算上界，预留预算，再执行 mutation，最后按实际 delta 提交”

### 写路径主线

可以把主线记成：

```text
command
  -> *Ops
  -> executeMutation(plan)
  -> ledger.reserve(upperBound)
  -> keyLifecycle.computeWithHandle(...)
  -> refreshEstimatedBytes(...)
  -> ledger.commit(actualDelta)
```

### `YierdisDbMutationExecutor`

`YierdisDbMutationExecutor` 的职责非常聚焦：

- 检查当前线程必须是 owner thread
- 向 ledger 申请 reservation
- 执行 mutation plan
- commit 实际 delta
- 在异常时 rollback

它把所有 mutation 压成了统一协议：

- `upperBoundBytes()`
- `apply() -> MutationResult<T>`

这让 DB 写路径变成一种“受控事务样式”的内部协议，即便它不是用户可见事务。

### 为什么要先算 upper bound

因为 maxmemory 和 memory ledger 不能等对象真的写进去了再补救。

正确顺序必须是：

1. 先估算这次写入理论上最多会多占多少
2. 再判断预算够不够
3. 再决定是否需要 cleanup/evict
4. 真正执行 mutation
5. 最后用实际 delta 校正

这就是 `MemoryLedger` 的意义。

## `YierdisDbMemoryLedger` 在做什么

`YierdisDbMemoryLedger` 是 DB 内核的预算入口。

它维护的不是一个单纯的 `usedBytes`，而是至少两类量：

- `usedBytes`
- `reservedBytes`

### reserve 阶段

当 mutation executor 调用 `reserve(...)` 时，ledger 会：

1. 先看是否存在全局 `MaxmemoryCoordinator`
2. 如果有，就交给 coordinator 统一判断
3. 如果没有，就在当前 DB 内做：
   - cleanup expired
   - noeviction 判断
   - eviction 判断
4. 如果预算允许，则增加 `reservedBytes`

### commit / rollback 阶段

- `commit(...)` 会结束 reservation，并把 `actualDeltaBytes` 记到 `usedBytes`
- `rollback(...)` 只撤销 reservation，不会把 mutation 当成成功写入

### 为什么 `reservedBytes` 很关键

因为如果没有它，写路径在“估算”和“真实 mutation”之间会有一段无保护窗口。

`reservedBytes` 的意义就是：

- 在 mutation 还没真正完成前，先把预算占住
- 出错再回滚

## TTL 和 expire index 是怎么协作的

TTL 不是只存在 payload 里的一个裸字段，而是被拆成：

- `EntryRecord.expireAtMillis`
- expire index 里的时间元数据

### 为什么要分开

因为：

- 不是所有 key 都有 TTL
- TTL 逻辑需要独立扫描和清理
- `persist`、`expire`、惰性删除、后台 cleanup 都要围绕这份索引工作

### `YierdisDbKeyLifecycle` 管什么

它负责：

- `setExpireAtMillis(...)`
- `removeExpire(...)`
- `expireAtMillis(...)`
- `removeIfExpired(...)`

### `YierdisDbExpirationSupport` 管什么

它负责：

- `cleanupExpired()`
- `cleanupExpired(nowMillis)`

也就是：

- keyLifecycle 负责“单 key 生命周期动作”
- expirationSupport 负责“批量过期清理策略”

## maxmemory 是怎么接进来的

对单个 DB 来说，maxmemory 相关逻辑主要在：

- `YierdisDbMemoryLedger`
- `YierdisDbMaxmemorySupport`

对多 DB 实例来说，还会接到：

- `YierdisInstance`
- `YierdisGlobalMaxmemoryGovernor`

### 两种作用方式

第一种：per-db scope

- `YierdisInstance` 把 `maxmemoryBytes` 按 DB 数量分摊给各 DB
- 默认 `YierdisDbEngineFactory` 会为每个 DB 创建独立 FFM runtime
- 每个 DB 有自己的本地预算
- ledger 本地处理 cleanup/evict/noeviction

第二种：global scope

- 默认 `YierdisDbEngineFactory` 让所有 DB 复用实例级 shared runtime
- instance 级 governor 作为 `MaxmemoryCoordinator`
- 各 DB attach 到同一个全局协调器
- governor 汇总各 DB participant，并把 shared runtime 的 `usedBytes()` 作为共享 usage source 计入一次

所以：

- 单 DB 里看到的是 ledger + support
- 多 DB 里真正的跨库协调来自 instance/runtime 层
- `MEMORY STATS` 在 global scope 下返回实例聚合视角，off-heap shared runtime 不按 DB 重复相加

## native entry/root 在 DB 内核里处于什么位置

命令层通常只关心 string/list/hash 之类的逻辑类型，但 DB 内核现在真正处理的是：

- `EntryRecord`
- `ValueHandle`
- `StringRoot`
- `ListRoot`
- `HashRoot`
- `SetRoot`
- `ZSetRoot`

### `EntryRecord`

它是每个 key 的元数据记录，持有：

- key handle identity
- value handle
- `ValueType`
- `ValueEncoding`
- expireAt
- 估算字节数
- LRU 相关字段

删除、过期清理、maxmemory 淘汰和 memory reporter 都优先从 entry
元数据计算。

### `TypeRoot`

各类型的 payload 由 root 管理：

- `StringRoot`
- `ListRoot`
- `HashRoot`
- `SetRoot`
- `ZSetRoot`

root 负责 `ValueHandle` 的创建、读取、变更、估算和释放。集合类型原来的
`HashValue`、`ListValue`、`SetValue`、`ZSetValue` 是 root 内部的 payload
结构；DB hot path 不再通过单独的兼容对象容器访问 key state。

更细的编码说明请配合：

- [`commands-and-data-model.md`](./commands-and-data-model.md)

## owner thread 为什么是硬约束

`YierdisDb` 不是线程安全并发容器，它明确要求：

- 所有 DB 访问必须在绑定后的 owner thread 上进行

相关入口：

- `threadGuard`
- `bindToCurrentThread()`
- `checkThread()`

这也是为什么：

- Netty I/O 线程不直接访问 DB
- maintenance 也要回到 executor 线程执行

如果你把这个约束拿掉，很多“简单”的方法都会瞬间变成竞态点。

## 对照源码时推荐看的顺序

1. `YierdisInstance`
   看 DB 是如何被实例层装配出来的
2. `YierdisDb`
   看对象图是如何拼起来的
3. `YierdisDbKeyLifecycle`
   看 key handle、live `EntryRecord`、TTL 元数据
4. `YierdisDbMutationExecutor`
   看受控 mutation 协议
5. `YierdisDbMemoryLedger`
   看预算、reservation 和 commit/rollback
6. `YierdisDbMemoryReporter`
   看观测和记账结果怎么被暴露出来

## 最值得看的测试

- `YierdisInstanceTest`
  看多 DB、global/per-db maxmemory、owner thread 约束
- `MutationExecutorReservationTest`
  看 reservation rollback 和 noeviction 拒绝路径
- `ExpireIndexTest`
  看 expire index、lazy delete 和 TTL 记账
- `MemoryStatsAccountingConsistencyTest`
  看记账结果是否一致
- `YierdisDbArchitectureGuardTest`
  看 DB 暴露面被哪些测试约束

## 一句话总结

Yierdis 的 DB 内核不是“Map + 一些命令辅助方法”，而是：

- 用 key lifecycle 管活 key
- 用 mutation executor 管写路径
- 用 memory ledger 管预算
- 用 entry/root 管真实编码和 payload handle

这四层协作起来，才组成了一个真正像数据库内核的单 DB 实现。
