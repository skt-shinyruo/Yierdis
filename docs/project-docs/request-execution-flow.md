# 请求执行链路

本文解释一条客户端请求在 Yierdis 里的运行路径：从 Netty 收到 RESP bytes，到命令进入 owner thread，再到 DB 读写、语义结果渲染和有序回包。

## 一张主链图

```mermaid
flowchart LR
  client["client RESP bytes"]
  decoder["RespRequestDecoder"]
  request["ExecutionRequest / ReplySlot"]
  ingress["NettyExecutionRequestIngress"]
  executor["CommandExecutor"]
  dispatcher["CommandDispatcher.prepare"]
  spec["CommandSpec.handler.parse(CommandArgs)"]
  prepare["handler result.apply(session)"]
  prepared["PreparedCommand"]
  db["DbEngine / typed ops"]
  result["CommandResult / RedisReply"]
  renderer["RedisReplyRenderer"]
  writer["RedisReplyWriter"]
  io["reply slot / Netty transport"]

  client --> decoder --> request --> ingress --> executor
  executor --> dispatcher --> spec --> prepare --> prepared
  prepared --> db --> result --> renderer --> writer --> io
```

命令执行部分统一为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> returned Function.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

这条链的边界是：

- protocol 负责 wire shape、RESP 版本和编码，不负责 DB 语义；
- command 负责查表、参数解析和命令语义，不接触 Netty 或 reply sink；
- executor 负责提交、排队、owner-thread 调度、回复预留、执行和统一渲染；
- DB 负责 storage behavior，不理解 RESP；
- `yierdis-server` 是 composition root，拥有 `CommandDispatcher` 与 executor 的最终组装；
- `RedisReplyWriter` 只是 `RedisReplyRenderer` 面向 RESP 的输出端口，不是 command handler API。

## 启动和连接状态

`YierdisServer.main(...)` 只做启动参数解析和 `YierdisServerBootstrap.start(...)`。

`YierdisServerBootstrap` 是 composition root。它创建 `YierdisInstance`，通过 `CommandRegistries.dispatcher(...)` 注册默认命令、事务命令和 server-only 命令，再把 `dispatcher::prepare` 交给 `CommandExecutor`。之后才创建 Netty groups 和 `ServerBootstrap`。

真正接收请求之前，owner thread 已由 `executor.start()` 绑定。DB 访问和命令执行留在该线程，Netty I/O 线程只做协议适配与提交。

每个连接由 `YierdisServerChannelInitializer` 初始化出 `NettyExecutionConnection`，其中三类状态彼此独立：

- `Channel`：真实 transport；
- `EngineSession`：具体的每连接 `CommandSession`，只持有 DB index、transaction、client name、RESP version，并借用连接统计视图；
- `ExecutionConnectionContext`：pending、pending bytes、closing、backpressure 暂停原因和统计；GLOBAL/FAIR 调度状态统一由 `ExecutorTaskQueue` 持有。

`EngineSession` 只是连接 session 状态的 owner，不是命令执行引擎，也不拥有 dispatcher、DB 或 reply writer。`NettyExecutionConnection` 才是 `Channel`、session 和 executor connection context 的连接 root。

`NettyExecutionConnection.markClosing()` 会先标记 executor connection context，再把事务清理调度到 command owner；owner 已退出时才同步兜底清理。这样 `QUIT`、协议错误或 transport close 都不会把 retained transaction requests 留在队列里。

## Netty pipeline

连接 pipeline 把网络数据推进到请求模型，再推进到 executor admission：

- `RespRequestDecoder` 解析 RESP array 或 inline command，执行 bulk、argc、line 和 command-bytes 入口限制，并把结果封闭为 `RespDecodedMessage.Request` 或 `RespProtocolError`；reply gate 再将该变体与 `ReplySlot` 绑定为 `RegisteredRespMessage`；
- `NettyExecutionRequestIngress` 穷尽处理其中的两个变体，保持回复顺序，完成 executor admission 或协议错误回包；
- I/O 线程不调用 command handler，也不访问 DB。

`RespRequestDecoder` 在 argv 与 payload 分配前完成 ingress admission，随后通过 `ByteArrayExecutionRequest.takeOwnership(...)` 把不可变 argv 与 reference-counted request-memory lease 一并移交给请求，不做第二次逐参数复制。RESP array 中的 null bulk string 会原样保留，合法性由 `CommandDispatcher` 判断。

`ByteArrayExecutionRequest` 是网络主链和 heap 输入共用的实现；`takeOwnership(...)` 接管 decoder 的 argv 与 lease，`copyOf(...)` 创建独立快照，`fromUtf8(...)` 用于文本构造。普通执行与事务重放都只依赖 `ExecutionRequest`。

## 提交、admission 和背压

`NettyExecutionRequestIngress` 先调用 `executor.tryAcquire(...)` 预留 backlog，再通过 `ExecutorAdmission.publish(request, replySlot)` 转移请求和回复槽所有权。

首次提交和 capacity waiter 唤醒后的重试都经过同一个 admission 分类入口；暂时不可用的 submission 保持在连接 pending deque 的头部，不会被后到请求越过。协议错误也由这个生产 ingress 使用原先注册的 reply slot 排序回写，不存在旁路 protocol handler。

- `Acquired`：publish 后 ownership 转给 executor；
- `Unavailable`：queue slot 或 bytes budget 暂时不足，submission 留在连接 pending queue，暂停输入并等待 `onAdmissionAvailable(...)`；
- `REQUEST_TOO_LARGE`：当前请求永远无法装入 configured bytes budget，当前 slot 返回对应错误；
- closing、not-running 或 publish invariant failure：清理 ownership 并终止连接，不破坏已有 reply 顺序；
- 协议错误和 ingress 内部错误使用已经注册的 reply slot 完成 terminal 回包。

更细的提交预算和背压关系见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## owner thread 上的统一执行

`CommandExecutor` drain 到任务后按以下顺序处理：

1. 调用 `CommandDispatcher.prepare(connection.session(), request)`；
2. 根据 `PreparedCommand.reservationShape()` 生成 reply plan；
3. 让 reply slot 预留 encoded bytes 与 retained source bytes；
4. 若容量暂时不可用，保留同一个 prepared command，暂停该连接输入并等待恢复；
5. 容量成功后调用 `validateBeforeExecute()`；若结果为 `STALE`，关闭旧对象并重新 prepare；
6. 执行一次 `PreparedCommand.execute(session)`；
7. 得到 `CommandResult` 后创建 `RedisReplyWriter`，由 `RedisReplyRenderer` 同步渲染其中的 `RedisReply`；
8. 根据 `CommandResult.closeAfterReply()` 标记连接与 reply slot，最后关闭 prepared command 和 request。

容量预留发生在可见 mutation 前。command handler 不写 reply bytes，executor 也不解释具体 Redis 命令；两者通过 `PreparedCommand`、`ReplyShape` 和 `CommandResult` 连接。

## 命令查表、解析和准备

`CommandDispatcher` 是 command-kernel 的单一入口。它负责空命令与 null argument 检查、命令名 ASCII 大写归一、`CommandRegistry` 查表、arity、transaction policy 和预期命令异常翻译。

查到的 `CommandSpec` 只有两部分：

- `CommandSyntax`：命令名、arity、key spec、transaction policy 和 reply admission requirement；
- `CommandHandler`：`parse(CommandArgs)` 返回 transport-neutral 的 `Function<CommandSession, PreparedCommand>`。

`CommandArgs` 集中提供 argv、ASCII literal 和整数读取。parse 阶段只解释请求参数；dispatcher 随后把 session 传给返回的 function，由它访问 DB、准备 mutation 或取得需要延迟释放的 reply source，并返回 `PreparedCommand`。

更完整的分支顺序见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## PING 最短路径

`PING` 是最短的主链验证：

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> ByteArrayExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> PING CommandSpec.handler().parse(CommandArgs)
  -> returned Function.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult(SimpleString)
  -> RedisReplyRenderer
  -> RedisReplyWriter / ReplySlot
  -> Netty transport
```

无 message 的 `PING` 不访问 DB，主要验证协议适配、提交、owner-thread 切换和统一渲染。

## SET 写路径

`SET` 把参数解析、DB 路由、memory ledger 和 TTL 串在一起：

1. `StringCommands` 的 handler 使用 `CommandArgs` 解析 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`，得到不可变参数并返回 prepare function；
2. prepare function 根据 session 选择 DB，调用 `StringOps` 准备 mutation，并从 preview 构造语义 `RedisReply` 与 reservation shape；
3. executor 预留回复容量并验证 mutation 仍为 current；
4. `PreparedCommand.execute(...)` 提交 prepared mutation，返回 `CommandResult`；
5. executor 统一渲染结果，随后关闭 prepared mutation owner。

可见性提交仍由 `YierdisDbMutationExecutor` 与 `YierdisDbKeyLifecycle` 维护 storage、TTL、memory ledger 和旧值释放；命令层不直接操作 allocator。

## 语义 streamed reply 的所有权

`GET`、`HGETALL`、`LRANGE`、`SMEMBERS` 和 scan 等读命令可能从 DB 获得 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。这些 source 可以持有 native pin，不能在 prepare 返回时提前关闭。

命令层通过 `DbReplies` 把 source 包装为带 payload length、retained source bytes 和 emitter 的 `RedisReply`，再用 `PreparedCommands.owned(...)` 让 `PreparedCommand` 持有 source。executor 先根据 reply shape 做预留；执行返回语义结果后，`RedisReplyRenderer` 同步调用 emitter 写入 `RedisReplyWriter`；只有渲染结束后 executor 才关闭 prepared command，从而在 command owner thread 归还 source。

这里的职责分配是：DB 创建并定义 source 生命周期，prepared command 拥有 source，`RedisReply` 只描述如何同步发射语义 payload，renderer 消费它，`RedisReplyWriter` 只负责 RESP-facing 输出。

## 事务和 replay

事务队列保存的是自己拥有的 retained `ExecutionRequest`，不是另一套命令 IR。`MULTI` 中的 queueable 命令会先经过同一个 registry lookup、arity 校验和 `handler.parse(CommandArgs)`；只有这些 preflight 成功，排队用的 prepared action 才会在 reply reservation 后调用 `TransactionState.tryEnqueue(request)` 并返回 `QUEUED`。此时不会把 session 应用到 handler 返回的 function，也不会访问 DB。

`EXEC` 重放每条 retained request 时调用同一个 `CommandDispatcher.prepareExecReplay(...)`。该入口只跳过再次排队，仍复用查表、arity、handler parse、session-aware prepare、prepared validation、execution 和 DB mutation path。子命令返回的 `RedisReply` 被收集成外层数组，executor 最终只调用一次 `RedisReplyRenderer`。

持有 streamed source 的 child `PreparedCommand` 会一直保留到整个 `EXEC` 聚合回复渲染结束；随后外层 prepared command 按逆序关闭 children 与 drained requests。child 返回的 `ControlError` 会先降为可嵌套的普通 `Error`，因为 control reservation 只适用于顶层回复。详细状态机见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## QUIT、错误和关闭

`QUIT` 的关闭语义属于结果而不是 writer side effect：其 handler 返回 `CommandResult.closeAfterReply(SimpleString("OK"))`。executor 先渲染 `OK`，再依据 result flag 标记连接 closing 并把同一 flag 交给有序 reply slot；Netty reply sequencer 在该回复写完后关闭 transport。`EXEC` 中若子结果请求关闭，外层结果会传播该 flag。

普通 command handler 不直接调用 `RedisReplyWriter`。预期的执行期命令错误可返回顶层 `ControlError`，renderer 会调用 `controlError(...)` 切换到当前槽位的 control reservation；该方法本身不请求关闭连接。executor/ingress 的控制路径直接写入 control error，并通过 reply slot 的 `markReady(true)` 传递关闭语义；普通命令的关闭语义仍由 `CommandResult` 携带。

错误大致分为：

- frame 级 protocol error：由 decoder/ingress 使用 reply slot 回写并关闭；
- admission reject：由 ingress/executor 边界处理；
- command error：parse/prepare 错误由 `CommandDispatcher` 表达为普通 `RedisReply.Error`；回复预留后的可预期执行期错误由 `CommandResult.controlError(...)` 表达为顶层 `RedisReply.ControlError`，`EXEC` 会在聚合前将 child control error 转为普通 `Error`；
- execute 后结果未知或渲染失败：executor 取消 reply ownership 并关闭 transport，不能伪造一个确定的 command error。

背压同时受单连接 pending、pending bytes、全局 queue slot、queued bytes、reply capacity 和 channel writability 影响。`NettyExecutionIoAdapter` 与 reply gate 负责有序 flush 和 close-after-reply。
