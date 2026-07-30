# 源码主路径导览

本文按源码阅读顺序串起 Yierdis 的主路径，适合一边打开文件一边跟读。

## 阅读前提

先知道三件事就够了：

1. 这是一个 Netty + RESP 的 server。
2. 命令执行会被提交到 owner thread。
3. DB 写入和协议回包是分层处理的。

如果还不熟，先看 [`project-overview.md`](./project-overview.md) 和 [`request-execution-flow.md`](./request-execution-flow.md)。

## 路线图

```text
YierdisServer
  -> YierdisServerBootstrap
  -> YierdisInstance
  -> YierdisServerChannelInitializer
  -> RespRequestDecoder
  -> RetainedRespExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
  -> RedisReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
```

这条路线的重点不是类多，而是每一层只管自己的职责。

## 1. 进程入口

先看 [`YierdisServer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java)。

它只做三件事：

- 解析启动参数
- 检查 FFM 可用性
- 启动 `YierdisServerBootstrap`

这里不放业务逻辑。`main` 只是启动壳。

## 2. 组装中心

再看 [`YierdisServerBootstrap.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java)。

它负责把实例、dispatcher、executor、Netty group 和 server channel 组起来。读它时重点盯住：

- `YierdisInstance`
- `CommandDispatcher`
- `CommandExecutor`
- `YierdisServerChannelInitializer`

`YierdisServerBootstrap` 是接线中心：它通过 `ServerCommandComposition` 组装并封闭命令注册表，把
`CommandDispatcher::prepare` 接到 executor 的 transport-neutral `CommandExecutionEngine` 边界；这里不实现命令语义。

## 3. 实例和多 DB

看 [`YierdisInstance.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java)。

它不是单个 DB，而是实例级资源 owner。这里能看到：

- 多 DB 创建
- maxmemory scope
- runtime memory runtime
- DB engine 数组导出

你要记住的是：command 层通过 `DbEngine / DbReads / DbWrites` 访问 DB，不直接绑定具体实现。

## 4. 连接和 pipeline

看 [`YierdisServerChannelInitializer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java) 和 [`NettyExecutionConnection.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java)。

这里会建立连接根对象，并把状态分成三块：

- Netty `Channel`
- `EngineSession`，只拥有连接级 DB index、RESP version、事务队列和 client metadata
- `ExecutionConnectionContext`

pipeline 的关键点是 `RespRequestDecoder`。它在完成 ingress admission 后把 RESP bytes 直接变成带 lease 的 `ExecutionRequest`，再交给提交层。

## 5. RESP 解码

继续看 [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java) 和 [`RetainedRespExecutionRequest.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java)。

它们的职责很窄：

- decoder 识别 RESP 帧
- decoder 在分配 argv/payload 前取得 ingress admission，并直接构造 `RetainedRespExecutionRequest`
- `RetainedRespExecutionRequest` 对外类型是 `ExecutionRequest`；其 lease 可在 executor 或事务 replay 的最后一个消费者处释放

这一步之后，协议层就不再继续往里走。

## 6. Executor 提交和 drain

看 [`NettyExecutionRequestIngress.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionRequestIngress.java) 和 [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)。

`NettyExecutionRequestIngress` 只处理 reply slot 与 executor admission，不执行命令。`CommandExecutor` 在
owner thread 上负责：

- 排队
- 背压
- owner thread drain
- 驱动 prepare、回复容量预留、validate、execute 和集中渲染
- 释放 request 生命周期

这里最容易读错的点是：executor 统一编排执行生命周期，但命令查找、参数语义和 DB 操作仍分别属于
dispatcher、handler 和 DB 层。

## 7. Dispatcher 到命令层

看 [`CommandDispatcher.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java)、[`CommandSpec.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java)、[`CommandArgs.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArgs.java) 和 [`PreparedCommand.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java)。

最终命令流固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

`CommandDispatcher` 统一处理空命令、unknown command、arity、事务策略和预期命令错误。handler 的
`parse(CommandArgs)` 只解释 argv，不读取 DB、provider 或 session；得到的 `CommandInvocation` 才在
`prepare(session)` 阶段访问连接和 DB 能力。`CommandParseIsolationTest` 用全部默认命令的 fixture 集合锁住
这条边界，`ServerCommandParseIsolationTest` 同样覆盖 `HELLO`、`INFO` 和 `STATS`。

在 `MULTI` 中，可排队命令只运行 handler parse 做 preflight，不运行 invocation prepare；真正的
`EXEC` 由 `TransactionCommands` 通过同一 `CommandDispatcher.prepareReplay(...)` 重放队列，并负责释放
队列中保留的 request 和每个 child prepared command。

## 8. PING 路径

先看 [`CoreConnectionCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java)。`PING` 在这里注册并实现，属于 transport-neutral 的连接命令。

如果只追主路径，`PING` 的重点是：

- `CommandExecutor` 把请求送到 owner thread
- `CommandDispatcher` 找到 `PING` 的 `CommandSpec`
- handler parse 返回 invocation，invocation prepare 返回 `PreparedCommand`
- executor 在预留和校验后得到包含 `PONG` 的 `CommandResult`
- `RedisReplyRenderer` 把语义回复写到 RESP-facing `RedisReplyWriter`

## 9. SET 路径

`SET` 是最值得读的写路径。顺序建议是：

1. [`StringCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)
2. [`YierdisStringOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)
3. [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

你会看到命令层先解析参数，再把写入交给 DB 层的 mutation executor 和 key lifecycle。这里也能对应 `YierdisDbWrites`、`DbEngine`、`DbReads` 的能力边界。

## 10. 回包写出

最后看 [`CommandResult.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandResult.java)、[`RedisReplyRenderer.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java)、[`RedisReplyWriter.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java)、[`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java) 和 [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)。

这里的核心是：

- 命令层返回 `CommandResult` 和 transport-neutral `RedisReply` 语义
- executor 用 `RedisReplyRenderer` 集中消费回复；bulk、sequence 和 map 可以通过语义 emitter 流式读取仍由 `PreparedCommand` 持有的 source
- `RedisReplyWriter` 只是 renderer 面向 RESP 编码器的端口，`RespReplyWriter` 负责 RESP2/RESP3 编码
- Netty 层做最终 flush

`PreparedCommand` 必须活到 renderer 消费完结果后再关闭。`QUIT` 也不再通过 writer 的隐式控制状态关连接，
而是返回 `CommandResult.closeAfterReply(...)`，由 executor 把关闭标记交给 reply sequencer。

## 容易读错的边界

- `YierdisServerBootstrap` 是组装层，不是命令层
- `NettyExecutionRequestIngress` 只做 admission/publish，不执行命令
- `CommandExecutor` 编排 prepare 到 render，但不实现命令语义
- `EngineSession` 只拥有连接 session 状态，不转发命令执行
- `CommandDispatcher` 处理查表、解析入口和事务策略，不编码 RESP
- `StringCommands` 负责命令语义，不负责 RESP 编码
- `YierdisDbMutationExecutor` 负责写入预算和提交，不负责网络回包

## 继续阅读

建议接着看：

- [`module-architecture.md`](./module-architecture.md)
- [`commands-and-data-model.md`](./commands-and-data-model.md)
- [`db-internals.md`](./db-internals.md)
- [`executor-and-backpressure.md`](./executor-and-backpressure.md)
- [`protocol-reference.md`](./protocol-reference.md)
