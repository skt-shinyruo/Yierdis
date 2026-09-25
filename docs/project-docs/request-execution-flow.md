# 请求执行链路

一条客户端请求在 Yierdis 里会依次经过：Netty 收到 RESP bytes、命令进入 owner thread、DB 读写、语义结果渲染和有序回包。本文按“先看线程怎么切，再逐步追一条请求”的顺序展开。

## 线程模型：谁在哪个线程上跑

读这条链最容易踩的坑是把异步回调当成同步调用。先分清四类线程：

| 线程 | 由谁创建 | 在这条链上做什么 |
| --- | --- | --- |
| boss event loop | `MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())` | accept 新连接。 |
| worker event loop | `MultiThreadIoEventLoopGroup(ioThreads, NioIoHandler.newFactory())`（默认 1） | 收包、解码、ingress 提交、reply 写回与 flush、关闭 transport。 |
| command owner thread | `DefaultEventExecutorGroup(1).next()`，经 `NettySerialOwnerExecutor` 暴露 | prepare、预留、校验、执行、渲染 DB 访问，是 DB 的唯一访问线程。 |
| 维护调度线程 | `workerGroup.next()` 上的 `scheduleWithFixedDelay` | 触发 maintenance tick，实际动作仍回到 owner thread。 |

`CommandExecutor.start()` 里 `executeOwnerTask(bindToCurrentThread).join()` 先把 owner thread 绑定到 instance（`YierdisInstanceRuntimeAccess::bindToCurrentThread`），之后才允许接收请求；DB 访问在别的线程上会 fail-fast。

**线程切换点**（下面“最短路径”里用 ①②③ 标注）：

- ① ingress 在 I/O 线程调用 `executor.tryAcquire(...)`、`ExecutorAdmission.publish(...)`；`publish` 内部 `taskQueue.offer(...)` 后调用 `drainLoop::scheduleDrain`，把 `drainLoop` 提交到 owner thread。
- ② owner thread 执行完命令后调用 `reply.markReady(...)`，`ReplySlot` 转交 `ConnectionReplySequencer.onSlotReady(...)`，后者经 `executeOnEventLoop(...)` 把写回放到 I/O 线程。
- ③ 背压暂停/恢复输入时，`NettyExecutionIoAdapter` 把 `setAutoRead` / `InboundReadCreditHandler` 操作放到 `channel.eventLoop().execute(...)`。
- ④ reply 容量阻塞后注册的容量回调 `task.reply.onCapacityAvailable(...)` 重新把任务提交回 owner thread 再继续 drain。
- ⑤ maintenance 由 I/O 线程的定时器触发，经 `CommandExecutor.executeMaintenance(...)` 落到 owner thread。

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

命令执行部分固定走这条链：

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

- protocol 负责 wire shape、RESP 版本和编码，不管 DB 语义；
- command 查表、解析参数并实现命令语义，不接触 Netty 或 reply sink；
- executor 承担提交、排队、owner-thread 调度、回复预留、执行和统一渲染；
- DB 只管 storage behavior，不理解 RESP；
- `yierdis-server` 是 composition root，拥有 `CommandDispatcher` 与 executor 的最终组装；
- `RedisReplyWriter` 只是 `RedisReplyRenderer` 面向 RESP 的输出端口，不是 command handler API。

## 启动和连接状态

`YierdisServer.main(...)` 解析启动参数，调用 `YierdisServerBootstrap.start(...)`，注册 shutdown hook，然后阻塞在 `server.awaitClose()`。`ServerConfig.fromArgs(...)` 要求 `--maxmemoryBytes` 显式给出，否则在 stderr 打一行原因并以 `exit(2)` 退出。

`YierdisServerBootstrap` 是 composition root，装配顺序是：先建 `YierdisInstance`，再用 `CommandRegistries.dispatcher(...)` 注册命令，然后把 `dispatcher::prepare` 交给 `CommandExecutor`，最后才创建 Netty groups 和 `ServerBootstrap`。`CommandRegistries.dispatcher(...)` 内部先注册 `TransactionCommands`（`MULTI`/`DISCARD`/`EXEC`），再注册传入的 `DefaultCommandModules.create(...)` 模块和 `ServerCommandModule`，最后 `registry.seal()`。

`YierdisServerBootstrap.startInternal()` 里与执行链相关的关键装配：

```text
instance = YierdisInstance.create(config)                     // 多 DB + maxmemory scope
dispatcher = CommandRegistries.dispatcher(DefaultCommandModules.create(...), new ServerCommandModule(...))
commandGroup = new DefaultEventExecutorGroup(1)               // owner thread 池（单线程）
commandOwner = new NettySerialOwnerExecutor(commandGroup.next())
executor = new CommandExecutor(runtimeAccess::bindToCurrentThread,
                               dispatcher::prepare,
                               commandOwner,
                               new RespReplySizer(),
                               RespReplyWriter::new,           // replyWriterFactory
                               new NettyExecutionIoAdapter(),
                               executorConfig)
bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
workerGroup = new MultiThreadIoEventLoopGroup(ioThreads, NioIoHandler.newFactory())
executor.start()                                              // 绑定 owner thread
```

`executor.start()` 在真正接收请求之前就绑定了 owner thread。DB 访问和命令执行留在该线程，Netty I/O 线程只做协议适配与提交。

每个连接由 `YierdisServerChannelInitializer` 初始化出 `NettyExecutionConnection`，其中三类状态彼此独立：

- `Channel`：真实 transport；
- `EngineSession`：具体的每连接 `CommandSession`，只持有 DB index、transaction、client name、RESP version，并借用连接统计视图；
- `ExecutionConnectionContext`：pending、pending bytes、closing、backpressure 暂停原因和统计；GLOBAL/FAIR 调度状态则由 `ExecutorTaskQueue` 持有。

`EngineSession` 只是连接 session 状态的 owner，不是命令执行引擎，也不拥有 dispatcher、DB 或 reply writer。`NettyExecutionConnection` 才是 `Channel`、session 和 executor connection context 的连接 root。

`NettyExecutionConnection.markClosing()` 先标记 executor connection context，再把事务清理调度到 command owner（`ownerTaskExecutor.accept(session::discardTransaction)`）；owner 已退出时才同步兜底清理。这样 `QUIT`、协议错误或 transport close 都不会把 retained transaction requests 留在队列里。server 主动发起的关闭都收敛到 `initiateClose()`：idle timeout 的 `CloseOnReadIdleHandler`、慢客户端宽限期结束的 `WriteBufferBackpressureHandler`、ingress 异常路径，先 `markClosing()` 再 `channel.close()`。reply sequencer/gate 等回复写路径的关闭，以及其余关闭来源，由 `closeFuture` 上的 `markClosing()` 监听兜底。executor 执行前除 `isClosing()` 外还会回看 transport 是否 active；某条关闭路径漏掉 closing 标记时，已入队命令也不会在断开的连接上继续执行。

## Netty pipeline

连接 pipeline 把网络数据推进到请求模型，再推进到 executor admission：

- `RespRequestDecoder` 解析 RESP array 或 inline command，施加 bulk、argc、line 和 command-bytes 入口限制，并把结果封闭为 `RespDecodedMessage.Request` 或 `RespProtocolError`；reply gate 再将该变体与 `ReplySlot` 绑定为 `RegisteredRespMessage`；
- `NettyExecutionRequestIngress` 穷尽处理其中的两个变体，保持回复顺序，完成 executor admission 或协议错误回包；
- I/O 线程不调用 command handler，也不访问 DB。

`RespRequestDecoder` 在 argv 与 payload 分配前完成 ingress admission，随后通过 `ByteArrayExecutionRequest.takeOwnership(...)` 把不可变 argv 与 reference-counted request-memory lease 一并移交给请求，不做第二次逐参数复制。RESP array 中的 null bulk string 会原样保留，合法性由 `CommandDispatcher` 判断。

`ByteArrayExecutionRequest` 是网络主链和 heap 输入共用的实现；`takeOwnership(...)` 接管 decoder 的 argv 与 lease，`copyOf(...)` 创建独立快照，`fromUtf8(...)` 用于文本构造，`retain()` 共享 argv 并增加 lease 引用。普通执行与事务重放都只依赖 `ExecutionRequest`。

## 从 socket 到 reply 的最短路径（PING）

`PING` 是最短的主链验证，无 message 的 `PING` 不访问 DB，主要验证协议适配、提交、owner-thread 切换和统一渲染。按序号追一遍：

```text
[I/O thread]  Netty ByteBuf
  1. RespRequestDecoder.channelRead -> process()                解析 '*1\r\n$4\r\nPING\r\n'
  2. 分配前 requestAdmission -> budget.tryTransferForProgress   ingress admission
  3. buildCompletedRequest -> ByteArrayExecutionRequest.takeOwnership(argv, lease)
  4. decodedMessageGate.tryAdmit -> 预留 control reservation、register ReplySlot
     -> RegisteredRespMessage 交给下一个 handler
  5. NettyExecutionRequestIngress.handleRegistered              pending defer FIFO 检查
  6. executor.tryAcquire(connection, retainedBytes)  -> Acquired
  7. admission.publish(request, slot)  -> offer + scheduleDrain ①
[owner thread] CommandExecutorDrainLoop.drainLoop
  8. executionSupport.execute(task)
  9. commandProcessor.apply(session, request) == dispatcher.prepare(session, request)
 10. spec.handler().parse(CommandArgs) -> session -> PreparedCommands.ready(SimpleString("PONG"))
 11. replySizer.apply(version, reservationShape) -> ReplyPlan
 12. reply.tryReserve(replyPlan) -> RESERVED
 13. prepared.validateBeforeExecute() -> VALID
 14. prepared.execute(session) -> CommandResult.reply(RedisReply.SimpleString("PONG"))
 15. RedisReplyRenderer.render(reply, new RespReplyWriter(plan.protocolVersion, reply.sink))
 16. reply.markReady(false)                                    ②
[I/O thread] ConnectionReplySequencer
 17. readyOnEventLoop -> drainOnEventLoop -> channel.write(chunk) + channel.flush
 18. 写完成 -> slot.finish -> 回收 slot 与 outbound lease
```

同一段链路对应到方法名：

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

## 提交、admission 和背压

`NettyExecutionRequestIngress` 先调用 `executor.tryAcquire(...)` 预留 backlog，再用 `ExecutorAdmission.publish(request, replySlot)` 转移请求和回复槽所有权。

首次提交和 capacity waiter 唤醒后的重试都经过同一个 admission 分类入口；暂时不可用的 submission 留在连接 pending deque（`pendingSubmissions`）头部，后到请求无法越过。协议错误同样走这个生产 ingress，按原先注册的 reply slot 排序回写，没有旁路 protocol handler。

- `Acquired`：publish 后 ownership 转给 executor；
- `Unavailable`：queue slot 或 bytes budget 暂时不足，submission 留在连接 pending queue，暂停输入并等待 `onAdmissionAvailable(...)`；
- `REQUEST_TOO_LARGE`：当前请求永远无法装入 configured bytes budget，当前 slot 写 `ERR request exceeds executor queue byte limit` 并 `markReady(false)`；
- closing、not-running 或 publish invariant failure：清理 ownership 并终止连接，不破坏已有 reply 顺序；
- 协议错误和 ingress 内部错误用已经注册的 reply slot 完成 terminal 回包；若此刻 pending deque 里还有未发布的延迟提交，ingress 取消这些 slot 并直接拆除连接，不刷出更晚的终端错误，也不伪造 `ERR busy` 替身回复。

backlog 预算由 `ExecutorBacklogBudget` 统一记账：`tryReserve(retainedBytes)` 同时检查 queue slot 与 queued bytes，`release(...)` 递减并在锁外唤醒容量等待者。全局背压高低水位由 queueCapacity 派生（高水位约 3/4 queue capacity，低水位为其一半），字节水位同理由 `queueMaxBytes` 派生。

更细的提交预算和背压关系见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## owner thread 上的统一执行

`CommandExecutor` drain 到任务后，`CommandExecutorExecutionSupport.execute(task)` 按以下顺序处理：

1. 若 `context.isClosing()` 或 transport 不 active，记 skipped，关闭 prepared/request/reply 并归还预算，不执行；
2. 调用 `CommandDispatcher.prepare(connection.session(), request)`（仅当 `task.prepared == null`）；
3. 读取 `PreparedCommand.replyProtocolVersion()`（缺省回退 `connection.session().respVersion()`），据此 `replySizer.apply(version, reservationShape)` 生成 reply plan；
4. 让 reply slot `tryReserve(replyPlan)` 预留 encoded bytes 与 retained source bytes；
5. 若返回 `WAITING`，`markInputPausedByReply()` 并暂停该连接输入，保留同一个 prepared command，返回 `REPLY_CAPACITY_BLOCKED`，由 `CommandExecutorDrainLoop.registerBlockedReplyTask` 登记容量回调等待恢复；
6. 容量成功后调用 `validateBeforeExecute()`；若结果为 `STALE`，关闭旧对象并返回 `REPREPARE`，由 drain loop `requeueStaleTask` 放到队头重试；
7. 执行一次 `PreparedCommand.execute(session)`；
8. 得到 `CommandResult` 后创建 `RedisReplyWriter`（`replyWriterFactory.apply(plan.protocolVersion(), reply.sink())`），由 `RedisReplyRenderer.render(...)` 同步渲染其中的 `RedisReply`；
9. 若 `CommandResult.closeAfterReply()` 为真，记 `closeAfterReply` 并 `connection.markClosing()`；最终 `reply.markReady(closeAfterReply)`；
10. 在 finally 中关闭 prepared command 和 request、清 inputPausedByReply、归还 backlog 预算，并按低水位尝试恢复输入。

容量预留发生在可见 mutation 前。command handler 不写 reply bytes，executor 也不解释具体 Redis 命令；两者通过 `PreparedCommand`、`ReplyShape` 和 `CommandResult` 连接。

## 命令查表、解析和准备

`CommandDispatcher` 是 command-kernel 的单一入口。`prepare(...)` 的分支顺序固定：

1. argc ≤ 0、`argv[0]` 为 null 或长度为 0 → `ERR empty command`，并在事务中 `markAborted()`；
2. 命令名按 ASCII 大写归一（非 ASCII 返回 null）；
3. 除 `PING`/`ECHO` 的第 2 个参数外，出现 null argument → `ERR Protocol error: null bulk string`；
4. `registry.specByExactUpperName(nameUpper)` 查表；未命中 → `ERR unknown command '<name>'`（名称含不可打印字符或超长时省略引号内内容）；
5. `spec.syntax().arity().validate(...)`；
6. 事务策略：`DISALLOWED_IN_MULTI` 报错，`QUEUEABLE` 走 `preflightMultiQueue`（只调用 `handler.parse` 做 preflight）并返回 `prepareRetainedRequestEnqueue`；
7. 普通路径：`spec.handler().parse(args)` 得到 `Function<CommandSession, PreparedCommand>`，再 `apply(session)` 得到 `PreparedCommand`；
8. `CommandParseException` → 可中止的错误回复；`WrongTypeException`/`YierdisCommandException` → 普通 `RedisReply.Error`。

查到的 `CommandSpec` 只有两部分：

- `CommandSyntax`：命令名、arity、key spec、transaction policy 和 reply admission requirement；
- `CommandHandler`：`parse(CommandArgs)` 返回 transport-neutral 的 `Function<CommandSession, PreparedCommand>`。

`CommandArgs` 集中提供 argv、ASCII literal（`is(index, "NX")` 大小写折叠）和整数读取（`longAt` 走 Redis `string2ll` 方言，拒绝 `+` 前缀与前导零）。

更完整的分支顺序见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## SET 写路径

`SET` 把参数解析、DB 路由、memory ledger 和 TTL 串在一起：

1. `StringCommands.set(CommandArgs)` 用 `CommandArgs` 解析 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`，得到不可变 `SetArgs` 并返回 prepare function；互斥 flag 才报 `ERR syntax error`，重复书写同一 flag 幂等；
2. prepare function（`prepareSet`）根据 session 经 `support.commandDb(session).strings().prepareSet(...)` 准备 `PreparedMutation`，并从 `mutation.preview()` 构造语义 `RedisReply`（`GET` 返回旧值、`NX/XX` 未命中返回 null，否则 `OK`）与 reservation shape；
3. executor 预留回复容量并 `validateBeforeExecute()` 确认 mutation 仍为 current；
4. `PreparedCommand.execute(...)` 执行 `mutation.commit()` 并返回 `CommandResult`；
5. executor 统一渲染结果，随后在 finally 关闭 prepared command（`mutation` 释放）。

可见性提交仍由 `YierdisDbMutationExecutor` 与 `YierdisDbKeyLifecycle` 维护 storage、TTL、memory ledger 和旧值释放；命令层不直接操作 allocator。

## 语义 streamed reply 的所有权

`GET`、`HGETALL`、`LRANGE`、`SMEMBERS` 和 scan 等读命令可能从 DB 获得 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。这些 source 可以持有 native pin，不能在 prepare 返回时提前关闭。

命令层通过 `DbReplies` 把 source 包装成 `RedisReply`，带上 payload length、retained source bytes 和 emitter，再用 `PreparedCommands.owned(...)` 让 `PreparedCommand` 持有 source。executor 先按 reply shape 做预留；执行返回语义结果后，`RedisReplyRenderer` 同步调用 emitter 写入 `RedisReplyWriter`；渲染结束后 executor 才关闭 prepared command，在 command owner thread 归还 source。

这里的职责分配是：DB 创建并定义 source 生命周期，prepared command 拥有 source，`RedisReply` 只描述如何同步发射语义 payload，renderer 消费它，`RedisReplyWriter` 只负责 RESP-facing 输出。

## 事务和 replay

事务队列保存的是自己拥有的 retained `ExecutionRequest`，不是另一套命令 IR。`MULTI` 中的 queueable 命令会先经过同一个 registry lookup、arity 校验和 `handler.parse(CommandArgs)`；只有这些 preflight 成功，排队用的 prepared action 才会在 reply reservation 后调用 `TransactionState.tryEnqueue(request)` 并返回 `QUEUED`。此时不会把 session 应用到 handler 返回的 function，也不会访问 DB。队列满（命令数或字节数超限）时 `tryEnqueue` 置 `aborted` 并返回 `ERR Transaction queue is full`。

`EXEC` 重放每条 retained request 时调用 `CommandDispatcher.prepareExecReplay(...)`（复用 `prepare(..., false)`，只跳过再次排队）。每条 child 依次 `validateBeforeExecute()`，`STALE` 时关闭并重试 prepare；随后 `execute(session)`。子命令返回的 `RedisReply` 收集成外层数组，`PreparedExec.execute` 只返回一个聚合结果，executor 最终只调用一次 `RedisReplyRenderer`。

持有 streamed source 的 child `PreparedCommand` 会一直保留到整个 `EXEC` 聚合回复渲染结束；随后外层 prepared command 的 `closeOwnedReverse` 按队列索引逆序关闭 children 与 drained requests。child 返回的 `ControlError` 会在聚合前降为可嵌套的普通 `Error`，因为 control reservation 只适用于顶层回复；child 执行开始后抛出的异常会被升级为 `ResultUnknownException`。详细状态机见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## QUIT、错误和关闭

`QUIT` 的关闭语义属于结果而不是 writer side effect：其 handler（`CoreConnectionCommands.quit`）返回 `PreparedCommands.ready(CommandResult.closeAfterReply(RedisReplies.simpleString("OK")))`。executor 先渲染 `OK`，再依据 result flag 标记连接 closing 并 `reply.markReady(true)`；`ConnectionReplySequencer.readyOnEventLoop` 看到 `slot.closeAfterReply()` 后关闭后续 slot 注册并取消排在它之后的 slot，写完这条回复即 `channel.close()`。`EXEC` 中若任一子结果请求关闭，外层 `PreparedExec` 会传播该 flag。

普通 command handler 不直接调用 `RedisReplyWriter`。预期的执行期命令错误可返回顶层 `ControlError`，renderer 会调用 `controlError(...)`（`RespReplyWriter.controlError` 会先 `useControlReservation()`）切换到当前槽位的 control reservation；该方法本身不请求关闭连接。executor/ingress 的控制路径直接写入 control error，并通过 reply slot 的 `markReady(true)` 传递关闭语义；普通命令的关闭语义仍由 `CommandResult` 携带。

错误路径按发生位置分成四类：

- **frame 级 protocol error**：`RespRequestDecoder` 产出 `RespProtocolError`，由 ingress 用已注册 reply slot 回写 `controlError` 并关闭连接（`markReady(true)`）。
- **admission reject**：`REQUEST_TOO_LARGE` 用当前 slot 写 executor queue 错误并 `markReady(false)`（不关连接）；NOT_RUNNING/CONNECTION_CLOSING/publish 失败则回收所有权并拆连接。
- **command error**：parse/prepare 错误由 `CommandDispatcher` 表达为普通 `RedisReply.Error`；回复预留后的可预期执行期错误由 `CommandResult.controlError(...)` 表达为顶层 `RedisReply.ControlError`，`EXEC` 会在聚合前将 child control error 转为普通 `Error`。
- **execute 后结果未知或渲染失败**：`CommandExecutorExecutionSupport` 区分两种情况——已有字节写出时直接 `cancel` + 关 transport；否则用 control reservation 写 `ERR internal error` 并关闭。`ResultUnknownException` 一律走 `closeResultUnknown`（`markResultUnknown` + `cancel` + 关连接），不能伪造一个确定的 command error。

背压同时受单连接 pending、pending bytes、全局 queue slot、queued bytes、reply capacity 和 channel writability 影响。`NettyExecutionIoAdapter` 与 reply gate 负责有序 flush 和 close-after-reply；Netty 侧 `channelWritabilityChanged` 与 `WriteBufferWaterMark` 的表现见 [`netty-adapter-design.md`](./netty-adapter-design.md)。