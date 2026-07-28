# 请求执行链路

本文解释一条客户端请求在 Yierdis 里的运行路径：从 Netty 收到 RESP bytes，到命令进入 owner thread，再到 DB 读写和 RESP 回包写出。

## 一张主链图

```mermaid
flowchart LR
  client["client RESP bytes"]
  decoder["RespRequestDecoder"]
  registered["RegisteredRespMessage / ReplySlot"]
  ingress["NettyExecutionRequestIngress"]
  executor["CommandExecutor"]
  engine["DefaultYierdisEngine"]
  processor["YierdisFastCommandProcessor"]
  command["CommandDefinition + PreparedCommand"]
  db["DbEngine / DbReads / DbWrites"]
  memory["yierdis-db-memory"]
  reply["RedisReplyWriter"]
  respReply["RespReplyWriter"]
  io["NettyExecutionIoAdapter"]
  flush["transport flush"]

  client --> decoder --> registered --> ingress --> executor
  executor --> engine --> processor --> command --> db --> memory --> reply --> respReply --> io --> flush
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

`NettyExecutionConnection` 是这三类状态的唯一连接 root。`getOrCreate(channel, txMaxCommands, txMaxBytes)` 会把一个 `EngineSession` 和一个 `ExecutionConnectionContext` 绑到同一个 `Channel.attr(...)` 上，重复初始化会复用同一个实例；`markClosing()` 会先把连接标成 closing，再丢弃事务，避免 `QUIT` 或 close-after-reply 之后继续保留 queued request lease。

`YierdisServerBootstrap` 的组装和关闭顺序也在这条链里固定下来：先创建 `YierdisInstance`、engine 和 executor，再创建 Netty groups 和 `ServerBootstrap`；关闭时则反向释放 server、event loop、executor 和 runtime owned resources，避免半初始化状态泄漏。

`YierdisServerChannelInitializer` 的 pipeline 只做连接级装配，不承载命令语义。顺序是 read credit/accounting -> decode -> protocol error reply -> fast command handler。连接级 close 和 protocol error 也在这条 pipeline 上闭环，而不是让 handler 自己猜测 channel 生命周期。

## Netty pipeline

连接上的 pipeline 负责把网络数据推进到请求模型，再推进到 executor 提交点。

关键节点是：

- `RespRequestDecoder`：从 `ByteBuf` 解析 RESP array 或 inline command，执行 bulk/argc/line/command-bytes 四类入口限制，并通过 reply gate 产出带 `ReplySlot` 的 `RegisteredRespMessage`
- `NettyExecutionRequestIngress`：接收其中的 `ExecutionRequest` 或 `RespProtocolError`，保持 reply 顺序，完成 executor admission 或协议错误回包

这条路径里，I/O 线程不执行命令，只做协议适配和提交。

## RESP 到 ExecutionRequest

`RespRequestDecoder` 是协议和执行层之间的直接边界。它在 argv 与 payload 分配前完成 ingress admission，构造不可变的 `RetainedRespExecutionRequest`，并将它作为 `ExecutionRequest` 交给后续层。每个请求持有脱离 Netty 对象的 reference-counted request-memory lease；RESP array 里的 null bulk string 会原样保留为 null argv 元素，命令是否合法仍由后面的 command-kernel 决定。

这里的意义有三个：

1. 命令层只认识 `ExecutionRequest`，不会看到 RESP DTO。
2. 请求 lease 会一直保留到 executor、事务队列或最后一个 retained view 释放。
3. 事务 replay 和普通执行共享同一个请求模型。

`RetainedRespExecutionRequest` 是网络主链的具体实现；`ByteArrayExecutionRequest` 用于 heap 输入、显式 copy 和事务 snapshot。主线语义始终是 `ExecutionRequest`。

## 提交到 CommandExecutor

`NettyExecutionRequestIngress` 不做命令执行，只为请求取得 executor admission 并把 request/reply slot 的所有权一起发布给 `CommandExecutor`。

`CommandExecutor` 负责：

- 检查连接和全局预算
- 记录 pending / pending bytes
- 在高水位时关闭输入
- 将请求排队到 owner thread

如果容量暂时不足，ingress 会保留 pending submission、暂停输入并通过 `onAdmissionAvailable(...)` 等待恢复；请求不会越过预算进入命令层。

## Admission 等待和终止拒绝

`NettyExecutionRequestIngress` 使用两阶段 admission，不负责执行业务命令：先 `executor.tryAcquire(...)` 预留 backlog，再由 `ExecutorAdmission.publish(request, replySlot)` 转移所有权。

更具体地说：

- `Acquired`：publish 后 request 和 reply slot ownership 转给 executor；
- `Unavailable`：queue slot 或 bytes budget 暂时不足，submission 留在连接级 pending queue，恢复容量后重试；
- `REQUEST_TOO_LARGE`：请求永远不可能装入 configured bytes budget，当前 slot 回 `ERR request exceeds executor queue byte limit`；
- closing / not-running / publish invariant failure：清理 ownership 并终止连接，不把无法确认顺序的 busy reply 插入流中；
- `exceptionCaught(...)` 的内部错误使用 terminal reply slot 回 `ERR internal error` 并 close-after-reply。

这就是“Netty I/O 线程只提交、不执行业务命令”的实际落点。更细的提交预算和背压关系见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)；命令进入 processor 之后的查表和 parse 细节见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## owner thread 执行

owner thread 是 DB 访问的真实边界。`CommandExecutor` 在这个线程上 drain 队列，然后调用 `DefaultYierdisEngine.prepare(...)`。

executor 在准备阶段交给 engine 的只有两样东西：

- `CommandSession`
- `ExecutionRequest`

engine 返回带 `ReplyShape` 的 `PreparedCommand`。executor 据此预留 reply capacity，校验 prepared state 仍有效，再创建 `RedisReplyWriter` 和 `CommandExecutionContext` 执行；它不解释具体命令语义。

## Engine 和命令分发

`DefaultYierdisEngine` 是 engine 实现入口。它用 `CommandSession` 和请求构造 `CommandPreparationContext`，再委托 `YierdisFastCommandProcessor`。`CommandSession` 已经聚合命令所需的 DB index、client metadata、transaction、connection stats 和 protocol negotiation 能力；细节见 [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md)。

`YierdisFastCommandProcessor` 负责：

- 通过 `CommandRegistry` 找到 `CommandDefinition<?>`
- 解析参数
- 调用 `CommandPreparer` 生成 `PreparedCommand`
- 让命令准备/执行把读写请求导向 `DbEngine / DbReads / DbWrites`

命令层本身不碰 Netty，只通过 DB 能力接口做真实存取。

如果你在追 `CommandRegistry`、`CommandDefinition`、`ArgReader`、parse error、unknown command 或 transaction policy，直接读 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) 会更准确；本页只保留请求主链视角。

## PING 最短路径

`PING` 是最短的主链验证：

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> RetainedRespExecutionRequest / ExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandDefinition<ArgReader>
  -> PreparedCommand
  -> reply capacity reservation
  -> CommandExecutionContext
  -> RedisReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
```

它不访问 DB，主要验证协议适配、提交、owner thread 切换和回包链路。

## SET 写路径

`SET` 把命令解析、DB 路由、内存预算和 TTL 一起串起来。

主流程是：

1. `StringCommands.parseSet(...)` 解析 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`
2. `StringCommands.set(...)` 通过 `YierdisStringOps` 准备 mutation 和 reply shape
3. executor 预留 reply capacity 后执行 prepared mutation
4. `YierdisDbMutationExecutor` 负责 reserve / prepare / commit / abort
5. `YierdisDbKeyLifecycle` 维护 keyspace、TTL、旧值释放和访问元数据

这说明 `SET` 不只是“改一个值”，它还会触发 memory ledger、TTL 和 entry 记录更新。

## 事务和 replay

事务保存的是独立拥有的 retained `ExecutionRequest`，不是另一套命令 IR。

入队时，`request.retain()` 为事务队列创建独立所有权视图；网络实现共享不可变 argv 和 reference-counted request-memory lease，因此当前 executor owner 关闭后仍能安全 replay。`EXEC` 之后的重放仍然走同一套 `YierdisFastCommandProcessor`、`CommandDefinition.parse(...)`、`PreparedCommand` 和 DB 访问路径。

## replay 仍然走同一条执行链

事务重放不会走另一套“内部命令执行器”。触发 replay 的 `EXEC` 请求仍先经过 `DefaultYierdisEngine`，而队列里的 retained requests 会逐条重新进入同一个 `YierdisFastCommandProcessor` 和原始 command path。

当前实现里，`EXEC` 会把事务队列里的 retained `ExecutionRequest` 按顺序重新喂回同一个 processor；查表、parse、prepare、validation、mutation context 和 reply writer 都复用普通请求的边界。更完整的状态机、abort 和 queue limit 细节见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 错误、关闭和背压

错误处理分三类：

- protocol error：在协议层回写
- submit reject：在 handler 层直接返回 busy/error
- runtime error：标记连接 closing，再尽快回写并关闭

背压同时受单连接 pending、pending bytes、全局 queue slot、queued bytes 和 channel writability 影响。`NettyExecutionIoAdapter` 负责把 buffered reply 写回 transport，必要时触发 flush 和 close-after-reply。
