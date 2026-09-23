# 代理逻辑

command session、DB routing 与 server observability 之间还有三条委托边界，专题文档尚未充分展开。本文先把这三条边界讲清楚，再明确“代理”在 Yierdis 里到底指什么、有哪些能力边界。

## 这里的“代理”是什么

Yierdis 当前没有独立的网络代理进程，也没有转发到其他 Yierdis 实例的中间层：它是单机内存 KV server，不做 replication/cluster，不做请求转发。本文说的“代理逻辑”是**进程内的存取代理（delegation proxy）**——命令层不自己持有 DB 指针、连接状态或 server 统计，而是通过三个小接口向运行时借用能力：

1. command session capabilities（连接状态）
2. 多 DB routing（选择本次命令作用在哪个 DB）
3. server observability provider（INFO/STATS/MEMORY 统计）

这样命令模块可以保持 transport-neutral：它不需要知道 DB 数组来自 Netty server、embedded runtime 还是 test fixture，也不需要 import Netty 或 `yierdis-server`。

已经有专题充分解释的代理层，不再重复展开：

| 代理逻辑 | 继续阅读 |
| --- | --- |
| RESP DTO 到 `ExecutionRequest` | [`protocol-reference.md`](./protocol-reference.md), [`request-execution-flow.md`](./request-execution-flow.md) |
| executor 到 Netty I/O adapter | [`executor-and-backpressure.md`](./executor-and-backpressure.md) |
| command 到 `DbEngine` / typed ops facade | [`commands-and-data-model.md`](./commands-and-data-model.md), [`db-internals.md`](./db-internals.md) |
| DB typed native handle wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
| FFM runtime wrapper | [`native-memory-runtime.md`](./native-memory-runtime.md) |

## 代理转发的真实路径

一条命令从连接到回包，真正的“转发”就是下面这条串联调用。它把命令改写、回复回传、错误处理和超时的来源都暴露得很清楚：

```text
连接建立
  Netty accept -> YierdisServerChannelInitializer.initChannel(ch)
    -> childChannelRegistry.admit(ch)
    -> NettyExecutionConnection.getOrCreate(ch, txMaxCommands, txMaxBytes)   // 建 EngineSession + ExecutionConnectionContext
    -> 装配 RespRequestDecoder / NettyExecutionRequestIngress / reply sequencer

命令上行（Netty worker event loop）
  RespRequestDecoder
    -> RegisteredRespMessage(decodedMessage, replySlot)        // 每个请求先拿到 receive-order reply slot
    -> NettyExecutionRequestIngress.channelRead(...)
    -> CommandExecutor.tryAcquire(connection, retainedBytes)
    -> ExecutorAdmission.publish(request, replySlot)

命令执行（owner engine thread）
  CommandExecutorDrainLoop
    -> CommandDispatcher.prepare(session, request)             // 解析 + arity + 事务策略
    -> PreparedCommand.execute(session)
        -> CommandSupport.commandDb(session)                   // DB routing 委托点
        -> YierdisDbRouter.dbFor(session) -> DbEngine
    -> CommandResult

回包下行（channel event loop）
  CommandExecutorExecutionSupport
    -> RedisReplyRenderer.render(result.reply(), RedisReplyWriter)
    -> reply slot READY
  ConnectionReplySequencer 按收包顺序写出连续 READY 槽位并 flush
```

其中和“代理”直接相关的三个委托点是：

- **命令改写还是透传**：默认透传。命令名、参数不会在转发途中被改写。唯一的“状态改写”是 `SELECT`——它修改 `CommandSession` 上的 DB index，而不是任何全局指针；此外协议层可能前置一条 `SELECT`（benchmark 的 `database != 0` 路径），那是客户端行为，不属于 server 代理。
- **回复回传**：命令层只返回 `CommandResult`，不直接写 socket。渲染与写出由 `CommandExecutorExecutionSupport` + `ConnectionReplySequencer` 在 channel event loop 上按 receive-order slot 完成。
- **错误处理**：分三层来源——协议层（`RespProtocolError`）、调度层（`CommandDispatcher` 的 `abortingError` / `error`）、执行层（executor 的 `ERR internal error` / result-unknown）。
- **超时的来源**：Yierdis 没有“单条命令执行超时”这种机制。能观察到的超时/关闭都来自传输与关闭路径：读空闲（`--client-idle-timeout-millis`）、慢客户端输出缓冲宽限（`--client-output-buffer-over-limit-millis`）、shutdown reply drain（`--replyDrainTimeoutMillis`）。executor 侧的 reply capacity 不是超时，`WAITING` 会挂起并等一次性容量回调后重试。客户端侧的超时是客户端自己的（CLI `--timeoutMillis` 默认 `5000`，见 [`client-and-bench-internals.md`](./client-and-bench-internals.md)）。

## 当前代理能力的边界

明确边界，避免把“进程内委托”误读成“网络代理能力”：

- **没有跨进程转发**：不存在把请求转发到另一个 Yierdis/Redis 的路径。这也意味着没有 load balancing、failover、read/write splitting 或连接池代理。
- **没有命令重写/拦截中间件**：除 `SELECT` 改 session 状态、MULTI 队列对可队列命令做延迟 prepare 外，命令不改写、不合并、不拆分。
- **单 owner 串行执行**：所有 DB 访问仍回到同一条 owner thread（见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)）。routing 只决定“用哪个 DB”，不引入并行度。
- **路由只按 session DB index**：`YierdisDbRouter.dbFor(session)` 在每次命令执行时解析当前 session；命令模块初始化时并不固定 DB，也没有按 key hash 分片的语义。
- **观测代理单向依赖**：command 层可以请求观测摘要，但不能反向 import Netty channel、server bootstrap 或 executor implementation details。

## Command Session Contract

executor 和 dispatcher 之间直接使用 `CommandSession`，由它聚合命令所需的连接能力；命令层不必先接收一个弱 marker session 再做运行时收窄。生产环境把 `CommandDispatcher::prepare` 注入 executor 的窄准备端口，已经没有独立的 command engine 对象。

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

`CommandSession` 是一个窄接口，直接提供命令所需的能力：`dbIndex()` / `setDbIndex(int)`、`clientName()` / `setClientName(String)`、`transaction()`、`connectionStats()`、`respVersion()` / `setRespVersion(int)`。

路由、准备和执行共用同一个 session 实例：连接建立时 `NettyExecutionConnection.getOrCreate(...)` 创建 `EngineSession`，之后 `CommandExecutorExecutionSupport` 用 `connection.session()`、`CommandDispatcher.prepare(session, request)`、router 的 `dbFor(session)`、provider 的 `info/stats/memoryStats(session)` 都拿的是这同一个对象。`EngineSession` 是这些连接状态的生产 owner，不负责命令查找、解析或回包；`create(...)` 和 `discardTransaction()` 的注释都强调连接关闭路径要在 command owner 上归还事务里 retain 的请求。

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
  -> 校验 dbIndex 在 0..databases()-1
  -> CommandSession.setDbIndex(db)

every command
  -> CommandSupport.commandDb(session)
  -> YierdisDbRouter.dbFor(session)
  -> selected DbEngine
  -> typed ops or direct memory/lifecycle method
```

`CommandSupport` 持有一个 `YierdisDbRouter`、一个可空的 `ServerInfoProvider` 和 `SlowCommandLimits`。`CommandSupport.commandDb(session)` 直接返回 `DbEngine`；命令通过 `strings()`、`hashes()`、`lists()`、`sets()`、`zsets()`、`hll()`、`keyspace()` 和 `ttl()` 调用合并后的 typed ops，memory 与 flush 则是 `DbEngine` 的直接方法。

生产路径里的 router 由 `YierdisServerBootstrap.dbRouter(instance)` 创建：它把 `instance.engines()` 固化成 `DbEngine[]`，`dbFor(session)` 取 `session.dbIndex()`，越界时退回索引 0，`databases()` 返回数组长度。embedded 和测试路径也可以注入自己的 router，命令模块因此能保持 transport-neutral。

有两个容易忽略的地方：

- `SELECT` 自己做参数校验：`CoreConnectionCommands.select(...)` 在 dbIndex 越界时返回 `ERR DB index is out of range`；router 的越界兜底（退回 0）是防御性的第二道，正常命令行不会走到。
- router 在每次命令执行时解析当前 session 状态，命令模块初始化时并不固定 DB。

源码入口：

- `CoreConnectionCommands.select(...)`
- `YierdisDbRouter`
- `CommandSupport.commandDb(...)`
- `YierdisServerBootstrap.dbRouter(...)`

## Server Observability Provider

`INFO`、`STATS` 和部分 `MEMORY STATS` 口径需要 server runtime、executor 与当前连接统计，而默认命令模块不能直接依赖 Netty 或 `yierdis-server`，因此这里用 `ServerInfoProvider` 做观测代理。

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

`ServerInfoProvider` 位于 `yierdis-command` 的 API 包，生产实现 `NettyServerInfoProvider` 位于 `yierdis-server`。依赖方向是单向的：command 层可以请求观测摘要，但不能反向 import Netty channel、server bootstrap 或 executor implementation details。

接口只有三个方法：`info(CommandArgs, CommandSession)`、`stats(CommandSession)` 和可选的 `memoryStats(CommandSession)`。`memoryStats` 返回 `null` 表示“用 DB 级默认实现”，这正是 `MEMORY STATS` 的回退语义：`NettyServerInfoProvider.memoryStats(...)` 只在配置为 `global` scope 时返回 instance 聚合视角，否则返回 `null`，`KeyCommands.memory(...)` 再回退到 `support.commandDb(session).memoryStats()`。

每次 `INFO`、结构化 `INFO`、`STATS` 或 health 请求都会先创建一份请求级 `ServerStatsSnapshot`。executor、ingress、egress、child channels、runtime health 和 uptime 在该回复中只采样一次，所有 writer 共享同一份采样结果；memory 与 keyspace 聚合仍按 section 按需执行，避免轻量 health 请求扫描全部 DB。字段清单与渲染面见 [`configuration-and-operations.md`](./configuration-and-operations.md) 的“可观测命令”一节。

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