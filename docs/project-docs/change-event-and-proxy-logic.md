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

## Command Session Contract

executor 和 dispatcher 之间直接使用 `CommandSession`。这个接口聚合命令所需的连接能力，避免命令层接收一个弱 marker session 后再做运行时收窄。生产环境把 `CommandDispatcher::prepare` 注入 executor 的窄准备端口，不再存在独立 command engine 对象。

```text
CommandExecutorExecutionSupport
  -> CommandDispatcher.prepare(commandSession, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(commandSession)
  -> PreparedCommand
  -> reply capacity reservation
  -> validateBeforeExecute()
  -> CommandExecutionContext.forRequest(commandSession, request)
  -> PreparedCommand.execute(context)
  -> CommandResult -> RedisReplyRenderer
```

`CommandSession` 同时提供这些能力：

- `DbIndexSession`
- `ClientMetadataSession`
- `TransactionSession`
- `ConnectionStatsSession`
- `ProtocolNegotiationSession`

类型边界保证 dispatcher 收到的 session 已具备全部能力。准备阶段只暴露 session；reply capacity 预留成功后，`CommandExecutionContext` 再加入请求级 `MutationContext`，但不暴露 writer。`EngineSession` 只是这些连接状态的生产 owner，不负责命令查找、解析或回包。

源码入口：

- `CommandDispatcher.prepare(...)`
- `CommandSpec`
- `CommandInvocation`
- `CommandSession`
- `CommandExecutionContext`
- `EngineSession`

## DB Routing And CommandDb

`SELECT` 不直接切换某个全局 DB 指针。它只修改当前连接 session 里的 DB index；后续每条命令再通过 `YierdisDbRouter` 选择本次命令要操作的 DB。

```text
SELECT <db>
  -> CoreConnectionCommands.select(...)
  -> DbIndexSession.setDbIndex(db)

every command
  -> CommandSupport.commandDb(ctx)
  -> YierdisDbRouter.dbFor(ctx.session())
  -> new CommandDb(selected DbEngine, optional MutationContext)
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

每次 `INFO`、结构化 `INFO`、`STATS` 或 health 请求都会先创建一份请求级 `ServerStatsSnapshot`。executor、ingress、commit stream、egress、child channels、runtime health 和 uptime 在该回复中只采样一次，所有 writer 共享同一份公共事实；memory 与 keyspace 聚合仍按 section 按需执行，避免轻量 health 请求扫描全部 DB。

源码入口：

- `ServerInfoProvider`
- `ServerCommandModule`
- `NettyServerInfoProvider`
- `KeyCommands.memory(...)`
- `YierdisInstanceObservability`

## DB Commit Stream

change event 是 DB 提交事实的有界、顺序化视图。它适合作为 AOF、replication 或 audit 的后续输入，但当前实现不是完整 Redis replication 或持久化协议。

生产组装时，`YierdisInstance` 只在 `YierdisChangeSink` 不是 `NOOP` 时创建 `CommitStream`，并把它作为 `DbCommitPublisher` 附着到每个 DB。command-core 不持有 sink，也不负责从命令语义推断 DB 是否已提交。

用户命令路径：

```text
CommandDispatcher.prepare(...)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reply capacity reservation
  -> CommandExecutionContext.forRequest(...)
     -> MutationContext.of(request)
  -> PreparedCommand.execute(...) / DbWrites
  -> YierdisDbMutationExecutor
     -> reserve immutable commit record before visibility
     -> storage + ledger commit
     -> DbCommitPublisher.publish(reservation)
  -> CommitStream worker
  -> YierdisChangeSink.onChange(callback-scoped event)
```

`MutationContext` 是命令执行边界内借用的 immutable command record 视图；`CommandExecutionContext` 关闭时会释放它，跨越当前调用栈的 commit record 必须显式 retain。`YierdisDbMutationExecutor` 仅在 prepared mutation 的 outcome 表示真实变化时预留 stream slot；预留失败发生在 storage commit 之前，因此不会把不可发布的写入变成可见状态。发布转换本身不分配、不回调，也不进行新的容量判断。

expire 和 eviction 的路径也经同一个 DB commit boundary。DB lifecycle 为实际删除构造规范化的 `DEL key` record，并分别使用 `EXPIRED` 或 `EVICTED` kind；客户端并没有真的发送这条 `DEL` 命令。

## Event Semantics

| kind | 来源 | synthetic | payload | 交付语义 |
| --- | --- | --- | --- | --- |
| `USER_COMMAND` | 已提交且改变状态的用户 mutation | `false` | 当前命令的 immutable record、DB index、提交时的 memory delta 和时间戳 | 表示 DB 已接受这个可重放命令事实，不携带 internal diff |
| `EXPIRED` | lazy expiration 或 maintenance cleanup 的已提交删除 | `true` | 规范化 `DEL key` record 和 DB index | 表示最终状态等价于删除该 key |
| `EVICTED` | maxmemory eviction 的已提交删除 | `true` | 规范化 `DEL key` record 和 DB index | 表示最终状态等价于删除该 key |

读命令、parse error、unknown command、条件写入的 no-op 和 `MULTI` 的 `QUEUED` 阶段不会获得 reservation，也不会生成事件。只有 `EXEC` replay 中实际提交的 mutation 会走这一流程。

worker 交给 `YierdisChangeSink` 的 `YierdisChangeEvent` 是 callback-scoped borrowed view。sink 不得保留 request/view，也不得在 callback 返回后访问它；event 在 callback 返回时关闭。commit stream 保留内部 immutable record 直到 callback 成功确认，随后释放该记录并推进 sequence。

## Failure And Durability Boundaries

commit stream 是 in-process fixed ring，不是 durable log：它不提供 AOF、fsync、crash recovery、PSYNC、replication backlog、ACK 或 exactly-once delivery。

- stream 容量不足或已失败时，新的需要发布的 mutation 在可见性提交前被拒绝。
- 若 storage 已提交后发布不变量失败，stream 保留该 reservation 并进入 failed state；DB 不能回滚已可见状态，调用方必须把该结果当作 post-commit failure 处理。
- sink callback 抛出异常会使 stream failed，而不是被静默吞掉；后续需要 stream 的 mutation 将被拒绝，直到实例按故障流程停止或替换。
- stream 的 sequence 只说明本进程本轮生命周期内的 delivery ordering，不能用于恢复或跨进程去重。

如果以后要实现 AOF 或 replication，应在这个提交事实边界之外增加 durable queue、offset/ack、重试、恢复和明确的 backpressure policy，而不是把当前 sink 当成完整复制层。

## Tests And Guards

改这些代理层时，优先找下面几类测试或架构护栏：

| 主题 | 关注点 | 测试入口 |
| --- | --- | --- |
| command record scope | executor 为每次实际执行建立并关闭请求级 `MutationContext`，EXEC child 另建自己的作用域 | `CommandExecutorTest`, `CommandDispatcherTest`, `MutationContextTest` |
| DB commit reservation | reservation 在可见性前完成，发布后才递送，post-commit failure 不被取消 | `DbCommitPublisherTest`, DB mutation tests |
| commit stream | ring capacity、顺序、borrowed callback view、sink failure 和 shutdown ownership | `CommitStreamTest`, `CommitStreamShutdownTest`, `CommitStreamIntegrationTest` |
| DB synthetic event | expire cleanup / eviction 删除 key 时只在实际 commit 后发布对应 kind | `ExpireIndexTest`, `YierdisDbConstructionTest`, maxmemory 相关测试 |
| DB routing | `SELECT`、session DB index 和 router 选择一致 | connection command tests, embedded/runtime DB routing tests |
| session capabilities | dispatcher 使用显式 `CommandSession` capability；`EngineSession` 只拥有连接状态 | `CommandDispatcherTest`, `EngineSessionTest` |
| observability provider | `INFO` / `STATS` / global `MEMORY STATS` 通过 provider 汇总 server/runtime 统计 | `YierdisServerBootstrapCommandWiringTest`, `MemoryStatsCommandTest` |

同时检查架构边界：command-builtin 不应依赖 command-kernel internal；command-core 不应 import runtime sink 或 DB commit implementation；DB API 只暴露 publisher port；runtime 持有 stream worker；server-main 是把这些接口接起来的 composition root。

## Commit-Stream Operations

runtime change delivery is a bounded in-process commit-stream, not a durable replication log. Its reserved event/byte counters and shutdown timeout are independent of command reply capacity and maxmemory. Production pressure, recovery, and acceptance rules are collected in [`production-hardening-operations.md`](./production-hardening-operations.md); do not promise restart recovery or exactly-once delivery from the current stream.
