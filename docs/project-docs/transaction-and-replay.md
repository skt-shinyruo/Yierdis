# 事务与重放

范围限于 `MULTI/EXEC/DISCARD`：连接 session 状态、入队 preflight、retained request、重放、streamed reply owner、abort 和清理。这套实现里没有 `WATCH`/`UNWATCH`，事务不具备乐观并发检测（见文末）。

普通执行的 canonical command path 是：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

transaction replay 把 dispatcher 入口换成 `prepareExecReplay(...)`（即 `prepare(session, request, false)`），只关掉“再次排队”这一个分支，其余查表、parse、准备函数应用、validation 和 execute 全部复用。child 不单独 reserve 或 render：外层 `PreparedExec` 统一预留，聚合后再交给 renderer。

队列路径是 transaction active 时的 preflight 分支，它在 handler parse 后暂停，不应用返回的准备函数；只有 `EXEC` 才会让 retained request 进入上述 child replay 路径。

## 事务状态属于连接 session

事务不是 command-kernel 的全局结构，也不属于 DB 或 executor。生产实现里，`NettyExecutionConnection` 拥有一个 `EngineSession`；`EngineSession` 是每连接 `CommandSession` 的具体 owner，它内部的 `DefaultTransactionState` 通过 `TransactionState` 接口暴露给 command layer。

接口本身很小：

```java
public interface TransactionState {
    boolean active();
    boolean aborted();
    void begin();
    void markAborted();
    String tryEnqueue(ExecutionRequest request);   // 返回 null 表示成功
    int size();
    void forEachQueued(Consumer<? super ExecutionRequest> visitor);
    List<ExecutionRequest> drain();                // 取出并重置状态
    void discard();                                // 关闭并重置
}
```

`DefaultTransactionState` 的字段就是全部事务状态：

- `active`：是否已执行 `MULTI`；
- `aborted`：是否因排队前错误或 queue limit 失效；
- `queue`：transaction 自己拥有的 retained `ExecutionRequest`；
- `queuedBytes`：所有 queued request 的 retained bytes 之和；
- `maxQueuedCommands` 与 `maxQueuedBytes`：连接级队列上限（构造时 `Math.max(0, ...)` 归一化，`0` 表示不限）。

除 `forEachQueued` 外所有方法都 `synchronized`，因为 markClosing 的兜底清理可能不在 command owner 上跑。`begin()` 与 `discard()` 都会先 `closeOwnedRequests()` 清空队列，然后重置 `active`/`aborted`/`queuedBytes`。`EngineSession` 不拥有 `CommandDispatcher`、DB、executor 或 renderer，只承载跨请求持续存在的连接状态。

## 为什么保存 retained `ExecutionRequest`

入队保存的是 `request.retain()` 返回的独立所有权视图，既不是当前 executor task 最终要关闭的那个 owner，也不是另一套 transaction IR。

两个原因：

1. 当前 task 在返回 `QUEUED` 后会关闭自己的 request owner，transaction queue 必须拥有可独立关闭的 view；
2. `EXEC` 要复用普通命令链。保留 `ExecutionRequest` 就能再次走 lookup、arity、handler parse、准备函数应用和 prepared execution。

`ExecutionRequest` 的合同把两件事分开：`retain()` 默认实现是 `ByteArrayExecutionRequest.copyOf(this)`，即 heap 副本；生产网络 request 的实现则共享不可变 argv、只增加 reference-counted request-memory lease 的引用。`retainedBytes()` 是 `HeapRequestFootprint` 口径的 heap footprint 估算——请求对象 + 外层 argv 与引用槽位 + 每参数数组头与 8 对齐 payload，做饱和计数不会回绕成负数。因此“小 payload、多参数”的命令也会按真实驻留成本计入 `maxQueuedBytes`，而不是只按 payload 长度求和。

## `MULTI` 与入队 preflight

`MULTI` 的 handler parse 不访问 session。返回的准备函数在 `apply(session)` 时检查事务是否已 active：已 active 直接返回 `error("ERR MULTI calls can not be nested")`；否则返回一个 `PreparedCommands.action(ReplyShapes.simpleString("OK"), ...)`，只有 executor 完成 reply reservation 并执行该 action，`tx.begin()` 才清理旧状态、设置 active 并返回 `OK`。

`MULTI`/`EXEC`/`DISCARD` 三个命令的 `CommandSyntax` 都是 `CommandArity.exact(1)` + `CommandKeySpec.NONE` + `TransactionPolicy.TRANSACTION_CONTROL`，所以它们永远不会被排队。

之后 queueable command 的主线是：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
     -> command name / null checks
     -> CommandRegistry lookup
     -> CommandArity.validate
     -> TransactionPolicy.QUEUEABLE
     -> CommandSpec.handler().parse(CommandArgs)   // preflight，不 apply
     -> queued PreparedCommand
  -> reserve -> validate -> execute(session)
     -> TransactionState.tryEnqueue(request)
     -> CommandResult(QUEUED or queue-full error)
  -> RedisReplyRenderer
```

关键边界有四个：

- preflight 复用普通执行的 registry、arity 和 handler parse；
- parse 只解释 argv，不调用 session、DB router 或任何 provider；
- preflight 成功后不应用 handler 返回的准备函数，因此不读 DB、不准备 mutation，也不创建 reply source；
- `tryEnqueue` 是 session mutation，必须等 executor 预留回复容量后在 queued prepared action 中发生。

下列前置错误会返回 error，并在该 error action 执行时标记 transaction aborted：

- empty 或 unknown command（`ERR empty command` / `ERR unknown command ...`）；
- illegal null bulk argument（`ERR Protocol error: null bulk string`）；
- wrong arity 或 handler parse error（`CommandParseException`）；
- `TransactionPolicy.DISALLOWED_IN_MULTI`（`ERR <NAME> is not allowed in MULTI`）。

这些错误都用 `abortingError`，`markAborted()` 推迟到错误回复获得容量并进入执行之后，prepare 阶段不碰 session。

queue 条数或字节超限由 `TransactionState.tryEnqueue(...)` 返回 `ERR Transaction queue is full` 并标记 aborted。`TRANSACTION_CONTROL` 命令不进入 queueable 分支，立即应用各自的准备函数。

## queue limits

`DefaultTransactionState.tryEnqueue(request)` 同时限制 `maxQueuedCommands` 和 `maxQueuedBytes`，顺序固定：

1. `request == null` 直接返回 `null`（不排队也不报错，防御性分支）；
2. 检查 command count：`maxQueuedCommands > 0 && queue.size() >= maxQueuedCommands` → 标记 aborted，返回 queue-full；
3. 用**原 request** 的 `retainedBytes()` 做一次估算并查字节上限；
4. 调用 `request.retain()` 取得 queue owner；
5. 用 **retained view** 的 `retainedBytes()` 再做真实检查；
6. 成功则 `queue.add(retained)` 并累加 `queuedBytes`。

字节上限判定是 `maxQueuedBytes > 0 && requestBytes > 0 && (requestBytes > maxQueuedBytes || queuedBytes > maxQueuedBytes - requestBytes)`，用减法规避溢出。第 3 步失败直接标记 aborted；第 5 步失败先 `retained.close()` 归还临时 owner，再返回同一个 queue-full error——两次失败返回的文案相同，但只有第 5 步需要清理。

上限从 server config 传入每个 `EngineSession`，不是 CLI 或 command module 自己维护的第二套限制。

## `EXEC` 的 child preparation 策略

`prepareExec(session)` 先看 transaction state，三分支：

- 未 active：`error("ERR EXEC without MULTI")`；
- 已 aborted：返回 `PreparedCommands.action(ReplyShapes.error(EXEC_ABORT), ...)`，执行时 `tx.discard()` 再返回 `EXECABORT Transaction discarded because of previous errors.`；
- active 且未 aborted：创建 transaction-owned `PreparedExec`。

`EXEC_ABORT` 是一条完整文案常量 `"EXECABORT Transaction discarded because of previous errors."`——首个 token `EXECABORT` 全是大写字母，所以 `ReplyShapes.normalizeError` 判定它已有 Redis 错误前缀，不会再补 `ERR `。

`PreparedExec` 对任意 queue size 用同一套策略：外层 prepare 不创建 child；空 queue 用 `ReplyShapes.array(List.of())` 精确预留，非空 queue 用 `ReplyShapes.maximum()`。reservation shape 只描述外层容量边界，不选择 child preparation 流程。`PreparedExec.validateBeforeExecute()` 恒返回 `VALID`——真正的内容在 `EXEC` 到达时已经确定。

## replay 主链

容量预留和 validation 成功后，`PreparedExec.execute(...)`：

1. `tx.drain()` 取出 retained requests；`drain()` 复制队列后 clear，并重置 `active=false`、`aborted=false`、`queuedBytes=0`；
2. 按队列顺序对每个 request 调用 `prepareCurrentChild(request)`；
3. `prepareCurrentChild` 是无限循环：`dispatcher.prepareExecReplay(session, request)` 得到 child，调 `validateBeforeExecute()`；`STALE` 就 `closeSuppressing(null, child::close)` 后重来，非 STALE 才返回。当前 child 执行完成后才准备下一个，因此前一个 child 的 session 或 DB side effect 对后一个 child 的准备和执行可见；
4. 每个 child 都用 `addOwnedChild(children, child)` 发布到清理列表（发布失败时在**当前栈帧**归还 child owner），然后把同一个 `session` 直接传给 `child.execute(session)`；
5. child execute 返回 `CommandResult`；若 reply 是 `RedisReply.ControlError`，降级为 `RedisReplies.error(controlError.message())` 再放进数组，因为 control error 依赖顶层预留槽位，在 aggregate 里只能当普通 error；
6. 所有 child reply 聚合为一个 `RedisReply.Aggregate(ARRAY, ...)`；
7. 所有 child 的 `closeAfterReply` 做 OR，决定外层 `CommandResult` 是否 close-after-reply；
8. 外层 executor 调用一次 `RedisReplyRenderer` 渲染整个 array。

`prepareExecReplay(...)` 仍复用：空命令、null argument 与 name 安全检查；同一个 `CommandRegistry` 与 `CommandSpec`；同一个 `CommandArity` 和 `handler.parse(CommandArgs)`；handler 返回的准备函数及其 `apply(session)`；`PreparedCommand` validation/execution 语义；相同的 DB mutation path。reply reservation 由外层 `PreparedExec` 统一拥有。

所以 `EXEC` 没有第二套命令解释器，也没有 child reply writer。child 产生 semantic results，只有外层 executor 的 renderer 接触 `RedisReplyWriter`。

## streamed child reply 的所有权

child 准备函数可能从 DB 取得 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。child `PreparedCommand` 用 `PreparedCommands.owned(...)` 持有这类 source；其 semantic `RedisReply` 的 emitter 在 source 存活期间有效。

`PreparedExec` 持有所有 child prepared commands 和 drained requests，并在 execute 后继续存活。executor 先渲染外层 aggregate；renderer 递归访问 child reply 并同步调用 source emitter。只有渲染成功或 task 进入 terminal cleanup 后，外层 `close()` 才按队列索引**逆序**执行：同一索引上先关闭 child owner，再关闭 drained request。`closeOwnedReverse` 用 `closeAccumulating` 把每个 close 失败挂成 suppressed，不让一个 owner 的异常打断其余清理。

因此 native pin 不会在 aggregate render 前释放，也不会转交给 Netty event loop。`TransactionCommandTest.execKeepsStreamedChildAliveUntilTheAggregateIsRendered` 保护这一所有权边界。

## `QUIT` 在 transaction 中的传播

`QUIT` 是 queueable command。普通执行时它返回 `CommandResult.closeAfterReply(SimpleString("OK"))`；在 transaction replay 中 child result 的 close flag 被 OR 到外层 `EXEC` result。

renderer 先输出完整的 `EXEC` array，executor 再根据外层 result flag 调用 `connection.markClosing()`，并把 close-after-reply 交给有序 reply slot。关闭不是 child handler 对 writer 的 side effect，transaction code 也不直接操作 transport。

## `DISCARD`、abort 与错误路径

`DISCARD` 未处于 transaction 时直接返回 `error("ERR DISCARD without MULTI")`；active 时返回 `PreparedCommands.action(ReplyShapes.simpleString("OK"), ...)`，reply reservation 成功并执行后 `tx.discard()` 关闭所有 queued request、清空 accounting，返回 `OK`。

其他控制错误：

- nested `MULTI`：`ERR MULTI calls can not be nested`；
- `EXEC` without `MULTI`：`ERR EXEC without MULTI`；
- aborted `EXEC`：discard 后返回 `EXECABORT Transaction discarded because of previous errors.`。

`WATCH`/`UNWATCH` 在当前实现里完全不存在（全仓库没有注册，也没有 match 到任何源码引用）。这意味着 `EXEC` 没有“被其他连接修改则放弃”的语义：只要队列未被 abort，`EXEC` 一定执行全部 child。要做条件执行只能靠命令自身的 `PreparedMutation` preview/isCurrent 校验，或客户端自己比较。

异常路径按“child 是否已开始执行”分叉。`PreparedExec.execute` 里有一个 `childExecutionStarted` 标志，在第一个 child 被 `addOwnedChild` 发布后置真。catch 分支：

```java
Throwable primary = childExecutionStarted
        ? resultUnknown(failure)      // 包装成 ResultUnknownException
        : failure;
closeOwnedReverse(primary);           // 先逆序归还所有 owner
rethrow(primary);
```

`resultUnknown` 把失败包成 `ResultUnknownException("transaction child result may already be visible", failure)`；executor 识别该标记后 mark result unknown、取消 reply 并关闭 transport，不伪造 transaction error array。若所有 child 都还没开始执行，failure 原样上抛，作为普通 executor failure 清理。

## connection close 与清理

`NettyExecutionConnection.markClosing()` 在第一次进入 closing 时把 `session::discardTransaction` 调度到 command owner：

```java
Runnable discard = session::discardTransaction;
try {
    ownerTaskExecutor.accept(discard);   // 由 command owner 回收队列
} catch (Throwable schedulingFailure) {
    discard.run();                       // owner 已退出，同步兜底
}
return true;
```

`DefaultTransactionState` 的同步和幂等清理保证这两条路径不会重复释放。server shutdown、`QUIT`、protocol terminal error 和 transport failure 最终都收敛到同一 transaction cleanup。`initiateClose()` 内部的顺序是先 `markClosing()` 再 `channel.close()`，不能交换——否则 close 完成到 closing 置位之间，已入队命令仍可能执行。

`closeOwnedRequests()` 逐个 `request.close()`，单个 close 抛出的 `Throwable` 被吞掉，不打断其余归还。

## 相关测试

- `EngineSessionTest`：begin、retain、count/bytes limit、drain、discard 和 close；
- `CommandDispatcherTest`：transaction preflight、aborting error、queue action、replay validation 与 child cleanup；
- `TransactionCommandTest`：客户端语义、顺序 replay、streamed child owner 和 parse failures；
- `ReplyPreflightCommandTest`：`EXEC` maximum reservation、capacity rejection 和 state-dependent child；
- `TransactionQueueCleanupTest`：connection closing 清空 transaction state；
- `TransactionQueueLimitTest`：server/CLI 视角的 queue limits；
- `CommandParseIsolationTest`、`ServerCommandParseIsolationTest`：排队前 handler parse 不访问运行时 service。