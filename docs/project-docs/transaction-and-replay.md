# 事务与重放

本文只解释 `MULTI/EXEC/DISCARD` 相关逻辑：session 状态、请求保留、重放、abort 和验证路径。

## 事务状态属于哪里

事务状态不是 command-kernel 自己持有的全局结构，而是连接级 session capability 状态的一部分。

生产实现里，`EngineSession` 持有一个私有的 `DefaultTransactionState`，并通过 `TransactionSession` / `TransactionState` 接口暴露给命令层。这个状态同时保存：

- `active`：当前是否处于 `MULTI` 中
- `aborted`：事务是否已经因为前置错误而失效
- `queue`：排队且由事务独立拥有的 retained `ExecutionRequest`
- `queuedBytes`：当前队列累计保留字节
- `maxQueuedCommands` / `maxQueuedBytes`：队列上限

这意味着事务是严格的连接态：

- 它跟随 `EngineSession`
- 它跨多条命令持续存在
- 它不属于某个 DB、某个 handler，也不属于 executor 全局

所以文档里看到的“事务”首先应理解成 session 状态机，而不是 command processor 里的临时列表。

## 为什么队列里保存的是 retained `ExecutionRequest`

入队时保存的不是当前 owner 必须关闭的同一个请求对象，也不是另一套内部 IR，而是 `request.retain()` 返回的独立所有权视图。生产网络请求的 retained view 共享不可变 argv 和 reference-counted request-memory lease；`ExecutionRequest` 的默认实现才会退化为 heap copy。

原因有两个：

1. 当前 owner 持有的 `ExecutionRequest` 生命周期跟着本次 submit / prepare 走，命令结束后会被关闭；事务队列必须拥有自己可关闭的一份 retain。
2. replay 需要复用同一条命令执行链。保存稳定的 `ExecutionRequest` 视图，就能让 `EXEC` 后的命令继续走 `DefaultYierdisEngine` 和 `YierdisFastCommandProcessor`，而不是走另一套解释器。

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 还会同时记录 `queuedBytes`，因此 transaction queue 的容量限制不是只看命令条数，也看 retained request 保留的字节数。

## `ExecutionRecord` 属于 change-event 记录

`ExecutionRecord` 把 `dbIndex` 和 `CommandRecordView` 绑成一个 record。公开的 `ExecutionRequest` 构造入口会把负的 `dbIndex` 归零并复制成 `ByteArrayExecutionRequest`；runtime sink 的 `borrowed(...)` 入口则保留 callback-scoped 只读视图，不额外复制。

它是 change-event API 的载体，`YierdisChangeEvent` 直接包着该 record。事务队列本身保存 retained `ExecutionRequest`，不通过 `ExecutionRecord` replay。

## `MULTI` 之后的入队流程

`MULTI` 自身只做一件事：把 `TransactionState.active` 置为 true，并清空之前残留的 queue / aborted 状态。

进入 `MULTI` 之后，普通命令的主线变成：

```text
YierdisFastCommandProcessor.prepare(...)
  -> TransactionQueuePolicy.queueIfNeeded(...)
     -> registry lookup
     -> CommandSyntax.transactionPolicy() check
     -> definition.parse(request)
     -> tx.tryEnqueue(request)
     -> prepared QUEUED reply
```

这里有几个容易误解的点：

- 命令不会在 `QUEUED` 时执行 handler
- 参数解析不会推迟到 `EXEC` 再做，而是在入队前先跑一遍
- queue 中保存的是独立拥有、最终必须关闭的 retained request view

如果前置校验失败，事务会被标记为 aborted：

- unknown command
- `TransactionPolicy.DISALLOWED_IN_MULTI`
- parse error
- queue size / queued bytes 超限

所以事务排队不是“先收进去，之后再统一报错”，而是“入队前先做一遍最小执行资格检查”。

## `EXEC` 的 replay 主链

`EXEC` 自己不解释队列中的命令。它会准备一个 `PreparedExec`，由该对象在执行阶段：

1. 检查当前是否 `active`
2. 检查事务是否 `aborted`
3. `tx.drain()` 取出队列并重置 transaction state
4. 写出 `arrayHeader(queued.size())`
5. 逐条通过同一个 processor replay `ExecutionRequest`

replay 的核心可以简化成：

```text
for (ExecutionRequest queuedRequest : tx.drain()) {
  try (queuedRequest;
       PreparedCommand child = processor.prepareQueued(queuedRequest, preparationContext);
       CommandExecutionContext childContext = CommandExecutionContext.forRequest(session, reply, queuedRequest)) {
    child.execute(childContext);
  }
}
```

这说明 replay 仍然走同一条执行链：

- 同一个 `YierdisFastCommandProcessor`
- 同一个 `CommandRegistry`
- 同一个 `CommandDefinition.parse(...)`
- 同一个 `CommandPreparer` / `PreparedCommand`
- 同一个请求级 `MutationContext`

也正因为如此，wrongtype、业务错误、mutation event、DB side effect 的行为都和非事务执行保持同源；`EXEC` 不是一个特殊的小型解释器。

## `DISCARD`、abort 和错误路径

`DISCARD` 的语义很直接：丢弃 queue，清掉 `active` 和 `aborted`，回 `OK`。

但真正值得文档化的是 abort 路径：

- `MULTI` 嵌套：`ERR MULTI calls can not be nested`
- `DISCARD` 不在事务里：`ERR DISCARD without MULTI`
- `EXEC` 不在事务里：`ERR EXEC without MULTI`
- `MULTI` 中的 unknown command / parse error / disallowed-in-multi：立即回错，并标记 transaction aborted
- aborted 之后执行 `EXEC`：先 `discard()`，再回 `EXECABORT Transaction discarded because of previous errors.`

还有一条容易漏掉的关闭路径：连接进入 closing 状态时，`NettyExecutionConnection` 会丢弃事务状态，避免 retained request 和 ingress lease 长期滞留在队列里。`TransactionQueueCleanupTest` 专门保护这个回归点。

## 队列限制和配置边界

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 同时受两类限制：

- `maxQueuedCommands`
- `maxQueuedBytes`

顺序是：

1. 如果条数已满，直接返回 `ERR Transaction queue is full`
2. 先用 `request.retainedBytes()` 做一次预估检查
3. 再执行 `request.retain()` 取得队列自己的所有权
4. 用 retained view 的 `retainedBytes()` 再做一次真实检查
5. 通过后入队并累加 `queuedBytes`

无论是条数超限还是字节超限，当前实现都复用同一条错误文案：`ERR Transaction queue is full`，并标记 transaction aborted。

这些限制不是 CLI 自己发明的。`EngineSession` 构造时就接收上限参数，server wiring 和测试客户端都复用同一套 `TransactionState` 约束。

## 相关测试

- `EngineSessionTest`：事务队列拥有关系、queue size / bytes 限制、drain / discard 行为
- `YierdisFastCommandProcessorPolicyTest`：`MULTI` 入队、abort、`EXECABORT`、replay 和 change event
- `YierdisServerBootstrapCommandWiringTest`：server wiring 把 transaction queue 限制正确接进连接
- `TransactionQueueCleanupTest`：连接 closing 必须清空事务状态
- `TransactionQueueLimitTest`：CLI / 客户端视角下的队列限制表现

命令查表、parser 复用和 transaction queue policy 的更细分发逻辑见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。
