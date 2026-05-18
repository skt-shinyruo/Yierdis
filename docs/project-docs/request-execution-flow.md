# Request Execution Flow

本文解释一条客户端请求在 Yierdis 里的运行路径：从 Netty 收到 RESP bytes，到命令进入 owner thread，再到 DB 读写和 RESP 回包写出。

## 一张主链图

![Yierdis request execution flow](./assets/request-execution-flow.svg)

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter
  -> RespExecutionAdapter
  -> ByteArrayExecutionRequest
  -> ExecutionRequest
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandSpec<ExecutionRequest>
  -> command implementation
  -> DbEngine / DbReads / DbWrites
  -> yierdis-db-memory
  -> ReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
  -> transport flush
```

这条链最重要的边界是：

- protocol 只负责 wire shape 和 reply encoding，不负责 DB 语义；
- command 只负责 parsing 和 command semantics，不负责 Netty；
- executor 只负责提交、排队、背压和 owner thread 调度；
- DB 负责 storage behavior，不直接理解 RESP；
- server-main 负责最终组装。

## 启动和连接状态

`YierdisServer.main(...)` 只做启动参数解析、FFM 可用性检查和 `YierdisServerBootstrap.start(...)`。

`YierdisServerBootstrap` 是 composition root。它会按顺序创建：

1. `YierdisInstance`
2. `DefaultYierdisEngine`
3. `CommandExecutor`
4. Netty boss / worker / command group
5. `ServerBootstrap`

真正接收请求之前，owner thread 已经由 `executor.start()` 绑定好。这样 DB 访问线程、命令执行线程和 Netty I/O 线程就不会混在一起。

每个连接由 `YierdisServerChannelInitializer` 初始化出 `NettyExecutionConnection`，它把三类状态拆开：

- `Channel`：真实 transport
- `EngineSession`：DB index、transaction、client state、RESP version
- `ExecutionConnectionContext`：pending、pending bytes、closing、backpressure、调度状态

## Netty pipeline

连接上的 pipeline 负责把网络数据推进到请求模型，再推进到 executor 提交点。

关键节点是：

- `RespRequestDecoder`：从 `ByteBuf` 解析 RESP array 或 inline command，产出 `RespCommandRequest` 或协议错误
- `RespCommandAdapter`：把 `RespCommandRequest` 转成 `ExecutionRequest`
- `YierdisFastCommandHandler`：接收 `ExecutionRequest`，只调用 `CommandExecutor.trySubmit(...)`

这条路径里，I/O 线程不执行命令，只做协议适配和提交。

## RESP 到 ExecutionRequest

`RespExecutionAdapter` 是协议和执行层之间的转换器。它把 `RespCommandRequest` 的 argv 视图复制成 `ByteArrayExecutionRequest`，再以 `ExecutionRequest` 形式交给后续层。

这里的意义有三个：

1. 协议 DTO 停留在 networking 边界内。
2. 命令层只认识 `ExecutionRequest`。
3. 事务 replay 和普通执行共享同一个请求模型。

`ByteArrayExecutionRequest` 是生产路径里的具体实现，但主线语义是 `ExecutionRequest`，不是 RESP DTO。

## 提交到 CommandExecutor

`YierdisFastCommandHandler` 不做命令执行，只把请求交给 `CommandExecutor`。

`CommandExecutor` 负责：

- 检查连接和全局预算
- 记录 pending / pending bytes
- 在高水位时关闭输入
- 将请求排队到 owner thread

如果提交失败，handler 会直接回写 busy/error，而不会让请求进入命令层。

## owner thread 执行

owner thread 是 DB 访问的真实边界。`CommandExecutor` 在这个线程上 drain 队列，然后调用 `DefaultYierdisEngine.execute(...)`。

executor 交给 engine 的只有三样东西：

- `Session`
- `ExecutionRequest`
- `ReplyWriter`

它不创建 `CommandContext`，也不直接知道命令语义。

## Engine 和命令分发

`DefaultYierdisEngine` 是 engine 实现入口。它把外部调度合同转换成命令层可执行的上下文，再委托 `YierdisFastCommandProcessor`。

`YierdisFastCommandProcessor` 负责：

- 通过 `CommandSpec<ExecutionRequest>` 找到命令
- 解析参数
- 调用 command implementation
- 把读写请求导向 `DbEngine / DbReads / DbWrites`

命令层本身不碰 Netty，只通过 DB 能力接口做真实存取。

## PING 最短路径

`PING` 是最短的主链验证：

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter
  -> RespExecutionAdapter
  -> ByteArrayExecutionRequest
  -> ExecutionRequest
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandSpec<ExecutionRequest>
  -> command implementation
  -> ReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
```

它不访问 DB，主要验证协议适配、提交、owner thread 切换和回包链路。

## SET 写路径

`SET` 把命令解析、DB 路由、内存预算和 TTL 一起串起来。

主流程是：

1. `StringCommands.set(...)` 解析 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`
2. `YierdisStringOps` 进入 DB 写实现
3. `YierdisDbMutationExecutor` 负责 reserve / apply / commit / rollback
4. `YierdisDbKeyLifecycle` 维护 keyspace、TTL、旧值释放和访问元数据
5. `DbEngine / DbReads / DbWrites` 最终落到 `yierdis-db-memory`

这说明 `SET` 不只是“改一个值”，它还会触发 memory ledger、TTL 和 entry 记录更新。

## 事务和 replay

事务保存的是 `ExecutionRequest` 快照，不是另一套命令 IR。

入队时，当前请求会被复制成 `ByteArrayExecutionRequest`，这样原始对象在 executor 生命周期结束后仍然能安全 replay。`EXEC` 之后的重放仍然走同一套 `YierdisFastCommandProcessor`、`CommandSpec<ExecutionRequest>`、command implementation 和 DB 访问路径。

## 错误、关闭和背压

错误处理分三类：

- protocol error：在协议层回写
- submit reject：在 handler 层直接返回 busy/error
- runtime error：标记连接 closing，再尽快回写并关闭

背压同时受单连接 pending、pending bytes、全局 queue slot、queued bytes 和 channel writability 影响。`NettyExecutionIoAdapter` 负责把 buffered reply 写回 transport，必要时触发 flush 和 close-after-reply。

## 推荐源码和测试

推荐先看这些文件：

- [`YierdisServer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java)
- [`YierdisServerBootstrap.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java)
- [`YierdisServerChannelInitializer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java)
- [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)
- [`RespCommandAdapter.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java)
- [`RespExecutionAdapter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java)
- [`YierdisFastCommandHandler.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java)
- [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)
- [`DefaultYierdisEngine.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java)
- [`YierdisFastCommandProcessor.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)
- [`StringCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)
- [`YierdisStringOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)
- [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)
- [`ReplyWriter.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java)
- [`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java)
- [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)

配套测试可以从 `RespExecutionAdapterTest`、`RespRequestDecoderTest`、`CommandExecutorTest`、`DefaultYierdisEngineTest`、`CommandProcessorTest` 和 `TransactionCommandTest` 读起。
