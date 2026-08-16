# 事务与重放

本文只解释 `MULTI/EXEC/DISCARD`：连接 session 状态、入队 preflight、retained request、重放、semantic streamed reply owner、abort 和清理。

普通执行的 canonical command path 是：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

transaction replay 把 dispatcher 入口替换为 `prepareReplay(...)` 以关闭再次排队，并复用查表、parse、invocation prepare、validation 和 execute；child 不单独 reserve 或 render，外层 `PreparedExec` 统一预留并在聚合后交给 renderer。下面的 queue path 是 transaction active 时的 preflight 分支：它在 handler parse 后暂停，不调用 invocation prepare；`EXEC` 才让 retained request 进入上述 child replay 路径。

## 事务状态属于连接 session

事务不是 command-kernel 的全局结构，也不属于 DB 或 executor。生产实现中，`NettyExecutionConnection` 拥有一个 `EngineSession`；`EngineSession` 只是具体的每连接 `CommandSession` owner，其中的 `DefaultTransactionState` 通过 `TransactionState` 接口暴露给 command layer。

transaction state 保存：

- `active`：是否已经执行 `MULTI`；
- `aborted`：是否因排队前错误或 queue limit 失效；
- `queue`：transaction 自己拥有的 retained `ExecutionRequest`；
- `queuedBytes`：所有 queued request 的 retained bytes；
- `maxQueuedCommands` 与 `maxQueuedBytes`：连接级队列上限。

`EngineSession` 不拥有 `CommandDispatcher`、DB、executor 或 renderer。它只承载跨请求持续存在的连接状态；命令重放仍由 dispatcher 与 executor 完成。

## 为什么保存 retained `ExecutionRequest`

入队保存的不是当前 executor task 最终要关闭的同一个 owner，也不是另一套 transaction IR，而是 `request.retain()` 返回的独立所有权视图。

这样做有两个原因：

1. 当前 task 在返回 `QUEUED` 后会关闭自己的 request owner，transaction queue 必须拥有可独立关闭的 view；
2. `EXEC` 要复用普通命令链。保留 `ExecutionRequest` 就能再次走 lookup、arity、handler parse、invocation prepare 和 prepared execution。

生产网络 request 的 retained view 共享不可变 argv 与 reference-counted request-memory lease；heap request 可以通过自身实现提供稳定副本。transaction state 只依赖 `ExecutionRequest.retain()` 和 `retainedBytes()` 合同。

## `MULTI` 和入队 preflight

`MULTI` 的 handler parse 不访问 session。其 invocation 在 prepare 时检查当前 transaction 是否已 active；正常时返回一个 prepared action。只有 executor 完成 reply reservation 并执行该 action，`tx.begin()` 才清理旧状态、设置 active 并返回 `OK`。

之后 queueable command 的主线是：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
     -> command name / null checks
     -> CommandRegistry lookup
     -> CommandArity.validate
     -> TransactionPolicy.QUEUEABLE
     -> CommandSpec.handler().parse(CommandArgs)
     -> queued PreparedCommand
  -> reserve -> validate -> execute(context)
     -> TransactionState.tryEnqueue(request)
     -> CommandResult(QUEUED or queue-full error)
  -> RedisReplyRenderer
```

这里有四个关键边界：

- preflight 复用普通执行的 registry、arity 和 handler parse；
- parse 只解释 argv，不调用 session、DB router 或任何 provider；
- preflight 成功后不会调用 `CommandInvocation.prepare(session)`，所以不会读取 DB、准备 mutation 或创建 reply source；
- `tryEnqueue` 是 session mutation，必须等 executor 预留回复容量后在 queued prepared action 中发生。

下列前置错误会返回 error，并在该 error action 执行时标记 transaction aborted：

- empty 或 unknown command；
- illegal null bulk argument；
- wrong arity 或 handler parse error；
- `TransactionPolicy.DISALLOWED_IN_MULTI`。

queue 条数或字节超限由 `TransactionState.tryEnqueue(...)` 返回 `ERR Transaction queue is full` 并标记 aborted。`TRANSACTION_CONTROL` 命令不进入 queueable 分支；`MULTI/EXEC/DISCARD` 立即走各自 invocation。

## `EXEC` prepare 的两种策略

`EXEC` invocation 先查看 transaction state：

- 未 active：返回 `ERR EXEC without MULTI`；
- 已 aborted：准备一个 error action，执行时 `discard()`，返回 `EXECABORT Transaction discarded because of previous errors.`；
- active 且未 aborted：创建 transaction-owned `PreparedExec`。

`PreparedExec` 根据 queue size 使用两种准备策略：

- `0` 或 `1` 个 child：在外层 prepare 阶段遍历 queue，调用 `CommandDispatcher.prepareReplay(session, request)`，提前持有 child prepared command；空队列可给出精确空 array shape，单 child 使用 maximum reservation；
- 多个 child：外层先使用 maximum reservation，drain 后在 execute 阶段逐条 prepare。这样后一个 child 能观察前一个 child 的顺序 side effect，而不会在执行前把整个 transaction 的 state-dependent read 固化。

对于提前准备的 child，外层 `validateBeforeExecute()` 会逐个检查；任一 `STALE` 都让 executor 关闭整个外层对象并重新 prepare，此时 queue 尚未 drain。动态策略则为每个当前 child 循环 prepare/validate，直到不再 stale。

## replay 主链

容量预留和 validation 成功后，`PreparedExec.execute(...)` 执行：

1. `tx.drain()` 取出 retained requests，同时重置 active、aborted 和 queue accounting；
2. 对每个 request 取得或动态创建 child `PreparedCommand`；
3. 每个 child 都使用独立的 `CommandExecutionContext.forSession(session)`；
4. child execute 返回 `CommandResult`，外层收集其中的语义 `RedisReply`；
5. child 的顶层 `ControlError` 转成可放入 `EXEC` array 的普通 error；
6. 所有 child reply 聚合为一个 `RedisReply.Aggregate(ARRAY, ...)`；
7. 所有 child 的 `closeAfterReply` flag 做 OR，并放到外层 `CommandResult`；
8. 外层 executor 调用一次 `RedisReplyRenderer` 渲染整个 array。

`CommandDispatcher.prepareReplay(...)` 只关闭“再次排队”的 transaction policy。每个 replay request 仍复用：

- 空命令、null argument 与 name 安全检查；
- 同一个 `CommandRegistry` 与 `CommandSpec`；
- 同一个 `CommandArity` 和 `handler.parse(CommandArgs)`；
- `CommandInvocation.prepare(session)`；
- `PreparedCommand` validation/execution 语义；reply reservation 由外层 `PreparedExec` 统一拥有；
- 相同的 DB mutation path。

所以 `EXEC` 没有第二套命令解释器，也没有 child reply writer。child 产生 semantic results，只有外层 executor 的 renderer 接触 `RedisReplyWriter`。

## streamed child reply 的所有权

child invocation 可能从 DB 取得 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。这类 source 由 child `PreparedCommand` 通过 `PreparedCommands.owned(...)` 持有；其 semantic `RedisReply` 的 emitter 在 source 存活期间有效。

`PreparedExec` 持有所有 child prepared commands，并在 execute 后继续存活。executor 先渲染外层 aggregate；renderer 递归访问 child reply 并同步调用 source emitter。只有渲染成功或 task 进入 terminal cleanup 后，外层 `close()` 才按 queue index 逆序执行：同一索引先关闭 child owner，再关闭 drained request。

因此 native pin 不会在 aggregate render 前释放，也不会被转交给 Netty event loop。`TransactionCommandTest.execKeepsStreamedChildAliveUntilTheAggregateIsRendered` 保护这一所有权边界。

## `QUIT` 在 transaction 中的传播

`QUIT` 是 queueable command。普通执行时它返回 `CommandResult.closeAfterReply(SimpleString("OK"))`；在 transaction replay 中 child result 的 close flag 被 OR 到外层 `EXEC` result。

renderer 先输出完整的 `EXEC` array，executor 再根据外层 result flag 标记 connection closing，并把 close-after-reply 交给有序 reply slot。关闭不是 child handler 对 writer 的 side effect，transaction code 也不直接操作 transport。

## `DISCARD`、abort 和错误路径

`DISCARD` 未处于 transaction 时返回 `ERR DISCARD without MULTI`。active 时，它返回 prepared action；reply reservation 成功并执行后，`tx.discard()` 关闭所有 queued request、清空 accounting，并返回 `OK`。

其他控制错误包括：

- nested `MULTI`：`ERR MULTI calls can not be nested`；
- `EXEC` without `MULTI`：`ERR EXEC without MULTI`；
- aborted `EXEC`：discard 后返回 `EXECABORT Transaction discarded because of previous errors.`。

若 child execution 尚未开始，prepare/ownership failure 可以作为普通 executor failure 清理。若任何 child 已开始执行后发生异常，前面 mutation 是否可见已无法由客户端确认，`PreparedExec` 将 failure 提升为 `ResultUnknownException`；executor 会 mark result unknown、取消 reply 并关闭 transport，不伪造 transaction error array。

外层关闭会尽力回收所有 child 与 drained request，后一个 close failure 作为 suppressed failure 保留，不能中断其余 ownership cleanup。

## queue limits

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 同时限制：

- `maxQueuedCommands`；
- `maxQueuedBytes`。

顺序是：

1. 检查 command count；
2. 用原 request 的 `retainedBytes()` 做一次估算；
3. 调用 `request.retain()` 取得 queue owner；
4. 用 retained view 的 `retainedBytes()` 再做真实检查；
5. 成功后加入 queue 并累加 `queuedBytes`；
6. count 或 estimated-bytes 在 retain 前失败时直接标记 aborted；真实 retained-bytes 检查失败时先关闭临时 retained view，再返回统一的 queue-full error。

上限从 server config 传入每个 `EngineSession`，不是 CLI 或 command module 自己维护的第二套限制。

## connection close 与清理

`NettyExecutionConnection.markClosing()` 在第一次进入 closing 时把 `session.discardTransaction()` 调度到 command owner。这样 queued request 的 retain 通常在 owner thread 归还，避免 transport event loop 与 DB/source 生命周期交叉。

如果 owner 调度已经失败，连接层会同步执行 discard 作为兜底；`DefaultTransactionState` 的同步和幂等清理避免重复释放。server shutdown、`QUIT`、protocol terminal error 和 transport failure 最终都收敛到同一 transaction cleanup。

## 相关测试

- `EngineSessionTest`：begin、retain、count/bytes limit、drain、discard 和 close；
- `CommandDispatcherTest`：transaction preflight、aborting error、queue action、replay validation 与 child cleanup；
- `TransactionCommandTest`：客户端语义、顺序 replay、streamed child owner 和 parse failures；
- `ReplyPreflightCommandTest`：`EXEC` maximum reservation、capacity rejection 和 state-dependent child；
- `TransactionQueueCleanupTest`：connection closing 清空 transaction state；
- `TransactionQueueLimitTest`：server/CLI 视角的 queue limits；
- `CommandParseIsolationTest`、`ServerCommandParseIsolationTest`：排队前 handler parse 不访问运行时 service。
