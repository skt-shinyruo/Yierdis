# 变更事件与代理逻辑

本文集中说明几条容易被读漏的代理、委托和桥接逻辑。它同时服务两类读者：

- 读源码的人：快速定位哪些对象只是边界代理，不应该承载业务语义。
- 准备实现 AOF / replication / audit 的人：理解当前最小 change-event contract，不把它误读成完整复制协议。

## 先看结论

已经有专题充分解释的代理层，不在本文重复展开：

| 代理逻辑 | 继续阅读 |
| --- | --- |
| RESP DTO 到 `ExecutionRequest` | [`protocol-reference.md`](./protocol-reference.md), [`request-execution-flow.md`](./request-execution-flow.md) |
| executor 到 Netty I/O adapter | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| command 到 `DbEngine` / typed ops facade | [`commands-and-data-model.md`](./commands-and-data-model.md), [`db-internals.md`](./db-internals.md) |
| DB typed native handle wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| FFM runtime wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md) |

本文补齐的是四条文档里较少展开的代理链：

1. command session capabilities
2. 多 DB routing 和 `CommandDb`
3. server observability provider
4. change event bridge

## Command Session Capabilities

executor 交给 engine 的是通用 `Session`，但命令层不能用一个弱 marker session 静默运行。`DefaultYierdisEngine.execute(...)` 会先把 `Session` 收窄成 `CommandSessionCapabilities`，再构造 `CommandContext`。

```text
CommandExecutorExecutionSupport
  -> CommandExecutionEngine.execute(session, request, writer)
  -> DefaultYierdisEngine.execute(...)
  -> CommandSessionCapabilities.from(session)
  -> CommandContext
  -> YierdisFastCommandProcessor
```

`CommandSessionCapabilities` 要求 session 同时提供这些能力：

- `DbIndexSession`
- `ClientMetadataSession`
- `TransactionSession`
- `ConnectionStatsSession`
- `ProtocolNegotiationSession`

如果缺任一能力，engine 会 fail fast。这个代理层的意义是让命令实现明确依赖连接状态能力，而不是假设所有 `Session` 都天然支持 DB routing、事务、客户端元数据和 RESP 协商。

源码入口：

- `DefaultYierdisEngine.execute(...)`
- `CommandSessionCapabilities.from(...)`
- `CommandContext`
- `EngineSession`

## DB Routing And CommandDb

`SELECT` 不直接切换某个全局 DB 指针。它只修改当前连接 session 里的 DB index；后续每条命令再通过 `YierdisDbRouter` 选择本次命令要操作的 DB。

```text
SELECT <db>
  -> CoreConnectionCommands.select(...)
  -> DbIndexSession.setDbIndex(db)

every command
  -> CommandSupport.commandDb(ctx)
  -> YierdisDbRouter.dbFor(ctx.dbIndexSession())
  -> CommandDb.reset(selected DbEngine)
  -> DbReads / DbWrites / MemoryOps / DbLifecycleOps
```

`CommandDb` 是内置命令面向 DB 的窄代理。它只暴露 `reads()`、`writes()`、`memory()` 和 `lifecycle()`，不暴露 `YierdisDb` internal、entry table、type root 或 allocator handle。

生产路径里的 router 由 `YierdisServerBootstrap.dbRouter(instance)` 创建。embedded 和测试路径也可以注入自己的 router，因此命令模块可以保持 transport-neutral，不需要知道 DB 数组来自 Netty server、embedded runtime 还是 test fixture。

需要注意两点：

- `SELECT` 负责参数校验，DB index 越界时返回错误。
- router 是每次命令执行时解析当前 session 状态，不是命令模块初始化时固定 DB。

源码入口：

- `CoreConnectionCommands.select(...)`
- `YierdisDbRouter`
- `CommandSupport.commandDb(...)`
- `CommandDb`
- `YierdisServerBootstrap.dbRouter(...)`

## Server Observability Provider

`INFO`、`STATS` 和部分 `MEMORY STATS` 口径需要 server runtime、executor 和当前连接统计。但默认命令模块不能直接依赖 Netty 或 server-main，因此这里用 `ServerInfoProvider` 做观测代理。

```text
ServerCommandModule.INFO / STATS
  -> ServerInfoProvider
  -> NettyServerInfoProvider
  -> CommandExecutor.StatsSnapshot
  -> current ConnectionStatsView
  -> YierdisInstanceObservability

MEMORY STATS
  -> CommandSupport.infoProvider()
  -> ServerInfoProvider.memoryStats(ctx)
  -> null ? current DB memory stats : instance/global memory stats
```

`ServerInfoProvider` 位于 command API，生产实现 `NettyServerInfoProvider` 位于 server-main。这个方向很重要：command 层可以请求观测摘要，但不能反向 import Netty channel、server bootstrap 或 executor implementation details。

源码入口：

- `ServerInfoProvider`
- `ServerCommandModule`
- `NettyServerInfoProvider`
- `KeyCommands.memory(...)`
- `YierdisInstanceObservability`

## Change Event Bridge

change event 当前是最小可重放事件契约。它适合作为 AOF / replication / audit 的起点，但不是完整 Redis replication 或持久化协议保证。

生产组装时，`YierdisServerBootstrap` 把 runtime config 里的 `YierdisChangeSink` 包成 command-core 能理解的 `CommandChangeObserver`：

```text
YierdisServerBootstrap
  -> YierdisCommandProcessorOptions.changeObserver(...)
  -> RuntimeChangeSinkCommandChangeObserver.fromSink(config.changeSink())
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
```

用户命令事件和 DB 内部 synthetic 事件走同一个 sink，但触发位置不同。

用户命令路径：

```text
YierdisFastCommandProcessor
  -> CommandChangeEmitter.execute(request, ctx, action)
     -> ctx.clearMutationOutcome()
     -> observer.observeExecution(action)
     -> if ctx.changedAny()
        -> observer.onCommandChange(dbIndex, request)
        -> YierdisChangeEvent(ExecutionRecord(dbIndex, request))
```

这里真正被传递的是 `ExecutionRecord`：它把 `dbIndex` 和已复制的 `ExecutionRequest` 绑成不可变快照，所以 change sink 看到的是可重放事实，而不是原始 mutable request 引用。`YierdisChangeEvent` 只是再包一层 kind / synthetic 标记。

DB 内部 synthetic 路径有两个 owner-thread scope。命令执行期间的 lazy expire / eviction 由 command observer 打开 DB change scope；maintenance tick 则由 runtime access 打开同类 scope。

```text
command execution scope
RuntimeChangeSinkCommandChangeObserver.observeExecution(...)
  -> DbChangeContext.open(YierdisChangeEventBridge.forSink(sink))
  -> DB lifecycle / expire / eviction logic
  -> DbChangeContext.emit(DbChange.syntheticDelete(...))
  -> YierdisChangeEventBridge
  -> YierdisChangeEvent(ExecutionRecord(dbIndex, DEL key), kind, synthetic=true)

maintenance scope
YierdisInstanceRuntimeAccess.maintenanceTick()
  -> DbChangeContext.open(instance.changeListener())
  -> expire cleanup / defrag / maxmemory maintenance
  -> DbChangeContext.emit(DbChange.syntheticDelete(...))
  -> YierdisChangeEventBridge
```

惰性过期、cleanup 和 synthetic `EXPIRED` delete 的细节见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)；victim 选择和 synthetic `EVICTED` delete 的细节见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

`CommandSupport.recordMutation(...)` 是用户命令路径里的关键连接点。命令 handler 通过它把 DB 返回的 `MutationOutcome` 记录到 `CommandContext`。没有真实 value/TTL 变化的命令不应该发用户命令事件。

事件消费失败是 best-effort：bridge 和 observer 都会吞掉 sink 异常，不能让消费端失败影响命令执行、expire cleanup 或 eviction。

源码入口：

- `YierdisCommandProcessorOptions`
- `YierdisFastCommandProcessor`
- `CommandChangeEmitter`
- `CommandChangeObserver`
- `RuntimeChangeSinkCommandChangeObserver`
- `YierdisInstanceRuntimeAccess`
- `YierdisChangeSink`
- `YierdisChangeEventBridge`
- `DbChangeContext`
- `DbChange`

## Event Semantics

| kind | 来源 | synthetic | payload | replay 语义 |
| --- | --- | --- | --- | --- |
| `USER_COMMAND` | 成功执行并记录了真实 mutation outcome 的用户命令 | `false` | 原始 `ExecutionRequest` 快照和 DB index | 表示“这个写命令可被重放”，不携带 DB internal diff |
| `EXPIRED` | lazy expiration 或 maintenance cleanup 删除过期 key | `true` | 规范化 `DEL key` 的 `ExecutionRequest` 快照和 DB index | 表示最终状态等价于删除该 key |
| `EVICTED` | maxmemory eviction 删除 victim key | `true` | 规范化 `DEL key` 的 `ExecutionRequest` 快照和 DB index | 表示最终状态等价于删除该 key |

用户命令事件的判断标准是 `CommandContext.changedAny()`。这意味着读命令、未改变值的条件写入、解析失败、unknown command、事务入队阶段都不应该直接产出用户命令事件。

synthetic 事件不表示客户端真的发送了 `DEL`。它是 DB lifecycle 为了可重放最小记录构造的等价命令。

## change event 不是完整持久化协议

当前 change event contract 只承诺“哪些事实值得被重放”，不承诺“这些事实已经被可靠持久化或复制”。

- `CommandChangeEmitter` 只有在 command handler 记录了真实 mutation outcome 后才发用户命令事件。
- `EXPIRED` / `EVICTED` synthetic delete 只说明最终状态等价于 `DEL key`，不携带 DB internal diff。
- sink 消费失败不会回滚命令、cleanup 或 eviction；当前语义始终是 best-effort。
- maintenance 路径和用户命令路径都能发事件，但它们共用的是最小 replay contract，不是 Redis replication backlog、AOF fsync 或 crash-recovery 日志。

## Non-Guarantees

当前 change event contract 不保证：

- 完整 Redis replication 协议、ACK、offset、PSYNC 或 backlog。
- AOF 持久化、fsync 策略、落盘顺序或崩溃恢复。
- exactly-once 交付、消费失败重试或死信队列。
- 跨线程异步消费安全；sink 被执行路径调用，消费端不能阻塞热路径。
- DB internal diff、entry/value/root 结构变化、native handle 或 allocator address。
- 事件消费失败会回滚命令；当前语义是 best-effort。

如果以后要实现 AOF 或 replication，应在这个最小事件契约之上增加持久化队列、顺序号、消费失败处理和明确的生命周期边界，而不是把当前 sink 当成完整复制层。

## Tests And Guards

改这些代理层时，优先找下面几类测试或架构护栏：

| 主题 | 关注点 | 测试入口 |
| --- | --- | --- |
| command change event | 只有真实 mutation 后发用户命令事件；command-core 不依赖 runtime sink 或 DB change scope | `YierdisFastCommandProcessorPolicyTest`, `YierdisFastCommandProcessorArchitectureTest` |
| runtime change sink bridge | 用户事件和 synthetic DB event 都能转换成 `YierdisChangeEvent`，NOOP sink 不安装 observer | `RuntimeChangeSinkCommandChangeObserverTest`, `YierdisChangeSinkTest` |
| DB synthetic event | expire cleanup / eviction 删除 key 时发 synthetic delete | `ExpireIndexTest`, `YierdisDbConstructionTest`, maxmemory 相关测试 |
| DB routing | `SELECT`、session DB index 和 router 选择一致 | connection command tests, embedded/runtime DB routing tests |
| session capabilities | engine 要求显式 command session capability | engine/session contract tests |
| observability provider | `INFO` / `STATS` / global `MEMORY STATS` 通过 provider 汇总 server/runtime 统计 | `YierdisServerBootstrapCommandWiringTest`, `MemoryStatsCommandTest` |

同时检查架构边界：command-builtin 不应依赖 command-kernel internal；command-core 不应 import server-main、runtime change sink 或 DB change scope；server-main 是把这些接口接起来的 composition root。

## Commit-Stream Operations

runtime change delivery is a bounded in-process commit-stream, not a durable replication log. Its reserved event/byte counters and shutdown timeout are independent of command reply capacity and maxmemory. Production pressure, recovery, and acceptance rules are collected in [`production-hardening-operations.md`](./production-hardening-operations.md); do not promise restart recovery or exactly-once delivery from the current stream.
