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
  -> RespCommandAdapter
  -> RespExecutionAdapter
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> StringCommands
  -> YierdisStringOps
  -> YierdisDbMutationExecutor
  -> ReplyWriter
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

它负责把实例、engine、executor、Netty group 和 server channel 组起来。读它时重点盯住：

- `YierdisInstance`
- `DefaultYierdisEngine`
- `CommandExecutor`
- `YierdisServerChannelInitializer`

`YierdisServerBootstrap` 是接线中心，不是命令语义中心。

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
- `EngineSession`
- `ExecutionConnectionContext`

pipeline 的关键点是 `RespRequestDecoder` 和 `RespCommandAdapter`。它们把 RESP bytes 变成 `ExecutionRequest`，再交给提交层。

## 5. RESP 适配

继续看 [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)、[`RespCommandAdapter.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java) 和 [`RespExecutionAdapter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java)。

它们的职责很窄：

- decoder 识别 RESP 帧
- `RespCommandAdapter.channelRead(...)` 接收 `RespCommandRequest` 后调用 `RespExecutionAdapter.toExecutionRequest(...)`
- `RespExecutionAdapter.toExecutionRequest(...)` 返回的具体对象是 `ByteArrayExecutionRequest`，对外类型是 `ExecutionRequest`

这一步之后，协议层就不再继续往里走。

## 6. Executor 提交和 drain

看 [`YierdisFastCommandHandler.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java) 和 [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)。

`YierdisFastCommandHandler` 只提交，不执行。`CommandExecutor` 负责：

- 排队
- 背压
- owner thread drain
- 释放 request 生命周期

这里最容易读错的点是：executor 不是命令层，它只是调度层。

## 7. Engine 到命令层

看 [`DefaultYierdisEngine.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java) 和 [`YierdisFastCommandProcessor.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)。

`DefaultYierdisEngine` 接住调度合同，把 `Session + ExecutionRequest + ReplyWriter` 送进命令层。

`YierdisFastCommandProcessor` 才是命令分发入口，它会把请求分到具体 `CommandSpec` 和 typed handler。

## 8. PING 路径

先看 [`CoreConnectionCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java)。`PING` 在这里注册并实现，属于 transport-neutral 的连接命令。

如果只追主路径，`PING` 的重点是：

- `CommandExecutor` 把请求送到 owner thread
- `DefaultYierdisEngine` 接管执行
- `YierdisFastCommandProcessor` 找到对应 command implementation
- `ReplyWriter` 写出 `PONG`

## 9. SET 路径

`SET` 是最值得读的写路径。顺序建议是：

1. [`StringCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)
2. [`YierdisStringOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)
3. [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

你会看到命令层先解析参数，再把写入交给 DB 层的 mutation executor 和 key lifecycle。这里也能对应 `YierdisDbWrites`、`DbEngine`、`DbReads` 的能力边界。

## 10. 回包写出

最后看 [`ReplyWriter.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java)、[`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java) 和 [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)。

这里的核心是：

- 命令层写语义
- RESP 层做协议编码
- Netty 层做最终 flush

这三层不要混写。

## 容易读错的边界

- `YierdisServerBootstrap` 是组装层，不是命令层
- `YierdisFastCommandHandler` 只提交，不执行
- `CommandExecutor` 只调度，不解析命令
- `DefaultYierdisEngine` 是执行入口，不是 DB 实现
- `StringCommands` 负责命令语义，不负责 RESP 编码
- `YierdisDbMutationExecutor` 负责写入预算和提交，不负责网络回包

## 继续阅读

建议接着看：

- [`module-architecture.md`](./module-architecture.md)
- [`commands-and-data-model.md`](./commands-and-data-model.md)
- [`db-internals.md`](./db-internals.md)
- [`executor-and-backpressure.md`](./executor-and-backpressure.md)
- [`protocol-reference.md`](./protocol-reference.md)
