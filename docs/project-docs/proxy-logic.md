# 代理逻辑

本文记录 command session、DB routing 和 server observability 之间仍存在的委托边界。

## 先看结论

已经有专题充分解释的代理层，不在本文重复展开：

| 代理逻辑 | 继续阅读 |
| --- | --- |
| RESP DTO 到 `ExecutionRequest` | [`protocol-reference.md`](./protocol-reference.md), [`request-execution-flow.md`](./request-execution-flow.md) |
| executor 到 Netty I/O adapter | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| command 到 `DbEngine` / typed ops facade | [`commands-and-data-model.md`](./commands-and-data-model.md), [`db-internals.md`](./db-internals.md) |
| DB typed native handle wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| FFM runtime wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md) |

本文补齐的是三条文档里较少展开的代理链：

1. command session capabilities
2. 多 DB routing
3. server observability provider

## Command Session Contract

executor 和 dispatcher 之间直接使用 `CommandSession`。这个接口聚合命令所需的连接能力，避免命令层接收一个弱 marker session 后再做运行时收窄。生产环境把 `CommandDispatcher::prepare` 注入 executor 的窄准备端口，不再存在独立 command engine 对象。

```text
CommandExecutorExecutionSupport
  -> CommandDispatcher.prepare(commandSession, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(commandSession)
  -> PreparedCommand
  -> reply capacity reservation
  -> validateBeforeExecute()
  -> PreparedCommand.execute(commandSession)
  -> CommandResult -> RedisReplyRenderer
```

`CommandSession` 直接提供 DB index、client name、transaction、connection stats 和 RESP protocol 能力。

路由、准备和执行共用同一个 session。`EngineSession` 是这些连接状态的生产 owner，不负责命令查找、解析或回包。

源码入口：

- `CommandDispatcher.prepare(...)`
- `CommandSpec`
- `CommandSession`
- `EngineSession`

## DB Routing

`SELECT` 不直接切换某个全局 DB 指针。它只修改当前连接 session 里的 DB index；后续每条命令再通过 `YierdisDbRouter` 选择本次命令要操作的 DB。

```text
SELECT <db>
  -> CoreConnectionCommands.select(...)
  -> CommandSession.setDbIndex(db)

every command
  -> CommandSupport.commandDb(session)
  -> YierdisDbRouter.dbFor(session)
  -> selected DbEngine
  -> typed ops or direct memory/lifecycle method
```

`CommandSupport.commandDb(session)` 直接返回 `DbEngine`。命令通过 `strings()`、`hashes()`、`lists()`、`sets()`、`zsets()`、`hll()`、`keyspace()` 和 `ttl()` 调用合并后的 typed ops，memory 与 flush 则是 `DbEngine` 的直接方法。

生产路径里的 router 由 `YierdisServerBootstrap.dbRouter(instance)` 创建。embedded 和测试路径也可以注入自己的 router，因此命令模块可以保持 transport-neutral，不需要知道 DB 数组来自 Netty server、embedded runtime 还是 test fixture。

需要注意两点：

- `SELECT` 负责参数校验，DB index 越界时返回错误。
- router 是每次命令执行时解析当前 session 状态，不是命令模块初始化时固定 DB。

源码入口：

- `CoreConnectionCommands.select(...)`
- `YierdisDbRouter`
- `CommandSupport.commandDb(...)`
- `YierdisServerBootstrap.dbRouter(...)`

## Server Observability Provider

`INFO`、`STATS` 和部分 `MEMORY STATS` 口径需要 server runtime、executor 和当前连接统计。但默认命令模块不能直接依赖 Netty 或 `yierdis-server`，因此这里用 `ServerInfoProvider` 做观测代理。

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

`ServerInfoProvider` 位于 `yierdis-command` 的 API 包，生产实现 `NettyServerInfoProvider` 位于 `yierdis-server`。这个方向很重要：command 层可以请求观测摘要，但不能反向 import Netty channel、server bootstrap 或 executor implementation details。

每次 `INFO`、结构化 `INFO`、`STATS` 或 health 请求都会先创建一份请求级 `ServerStatsSnapshot`。executor、ingress、egress、child channels、runtime health 和 uptime 在该回复中只采样一次，所有 writer 共享同一份公共事实；memory 与 keyspace 聚合仍按 section 按需执行，避免轻量 health 请求扫描全部 DB。

源码入口：

- `ServerInfoProvider`
- `ServerCommandModule`
- `NettyServerInfoProvider`
- `KeyCommands.memory(...)`
- `YierdisInstanceObservability`

## Tests And Guards

改这些代理层时，优先找下面几类测试或架构护栏：

| 主题 | 关注点 | 测试入口 |
| --- | --- | --- |
| DB routing | `SELECT`、session DB index 和 router 选择一致 | connection command tests, embedded/runtime DB routing tests |
| session capabilities | dispatcher 使用显式 `CommandSession` capability；`EngineSession` 只拥有连接状态 | `CommandDispatcherTest`, `EngineSessionTest` |
| observability provider | `INFO` / `STATS` / global `MEMORY STATS` 通过 provider 汇总 server/runtime 统计 | `YierdisServerBootstrapCommandWiringTest`, `MemoryStatsCommandTest` |

同时检查架构边界：`command.defaults` 不应依赖 `command.kernel` internal；command 层只通过 `storage.api` capability 访问存储；`yierdis-server` 是把 session、router、provider、runtime 和 transport 接起来的 composition root。
