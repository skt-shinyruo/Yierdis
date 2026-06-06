# 事务与重放

本文只解释 `MULTI/EXEC/DISCARD` 相关逻辑：session 状态、入队快照、重放、abort 和验证路径。

## 事务状态属于哪里

事务状态不是 command-kernel 自己持有的全局结构，而是连接级 session capability 状态的一部分。

生产实现里，`EngineSession` 持有一个私有的 `DefaultTransactionState`，并通过 `TransactionSession` / `TransactionState` 接口暴露给命令层。这个状态同时保存：

- `active`：当前是否处于 `MULTI` 中
- `aborted`：事务是否已经因为前置错误而失效
- `queue`：排队的 `ExecutionRequest` 快照
- `queuedBytes`：当前队列累计保留字节
- `maxQueuedCommands` / `maxQueuedBytes`：队列上限

这意味着事务是严格的连接态：

- 它跟随 `EngineSession`
- 它跨多条命令持续存在
- 它不属于某个 DB、某个 handler，也不属于 executor 全局

所以文档里看到的“事务”首先应理解成 session 状态机，而不是 command processor 里的临时列表。

## 为什么队列里保存的是 `ExecutionRequest` 快照

入队时保存的不是原始请求对象引用，也不是另一套内部 IR，而是 `ByteArrayExecutionRequest.copyOf(request)` 生成的 `ExecutionRequest` 快照。

原因有两个：

1. 原始 `ExecutionRequest` 的生命周期跟着本次 submit / execute 走，命令结束后会被关闭或释放；事务队列不能继续悬挂这份对象。
2. replay 需要复用同一条命令执行链。保存 `ExecutionRequest` 快照，就能让 `EXEC` 后的命令继续走 `DefaultYierdisEngine` 和 `YierdisFastCommandProcessor`，而不是走另一套解释器。

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 还会同时记录 `queuedBytes`，因此 transaction queue 的容量限制不是只看命令条数，也看 snapshot 保留的字节数。

## `ExecutionRecord` 是 replay 和 change-event 的不可变快照

`ExecutionRecord` 把 `dbIndex` 和 `ExecutionRequest` 绑成一个不可变 record。构造时它会把负的 `dbIndex` 归零，并通过 `ByteArrayExecutionRequest.copyOf(request)` 再复制一份稳定快照，所以它不会持有可变 request 引用。

它是 transaction replay 和 change-event 记录的共同载体：`YierdisChangeEvent` 直接包着 `ExecutionRecord`，而 replay / sink / test 都应该读取这个封装后的快照，而不是原始请求对象。

## `MULTI` 之后的入队流程

`MULTI` 自身只做一件事：把 `TransactionState.active` 置为 true，并清空之前残留的 queue / aborted 状态。

进入 `MULTI` 之后，普通命令的主线变成：

```text
YierdisFastCommandProcessor.execute(...)
  -> TransactionQueuePolicy.queueIfNeeded(...)
     -> registry lookup
     -> disallowed-in-multi check
     -> spec.parse(request)
     -> tx.tryEnqueue(request)
     -> QUEUED
```

这里有几个容易误解的点：

- 命令不会在 `QUEUED` 时执行 handler
- 参数解析不会推迟到 `EXEC` 再做，而是在入队前先跑一遍
- queue 中保存的是 snapshot，不是原始 request 指针

如果前置校验失败，事务会被标记为 aborted：

- unknown command
- `disallowedInMultiError`
- parse error
- queue size / queued bytes 超限

所以事务排队不是“先收进去，之后再统一报错”，而是“入队前先做一遍最小执行资格检查”。

## `EXEC` 的 replay 主链

`EXEC` 自己不解释队列中的命令。它只是：

1. 检查当前是否 `active`
2. 检查事务是否 `aborted`
3. `tx.drain()` 取出队列并重置 transaction state
4. 先回一个 `arrayHeader(queued.size())`
5. 逐条 replay `ExecutionRequest`

真正的 replay 核心在 `TransactionCommands.exec(...)`：

```text
for (ExecutionRequest queuedRequest : queued) {
  try (ExecutionRequest replay = queuedRequest) {
    CommandContext replayCtx = new CommandContext(ctx.sessionCapabilities(), out);
    processor.execute(replay, replayCtx);
  }
}
```

这说明 replay 仍然走同一条执行链：

- 同一个 `YierdisFastCommandProcessor`
- 同一个 `CommandRegistry`
- 同一个 `CommandSpec.parse(...)`
- 同一个 command handler
- 同一个 change-event gate

也正因为如此，wrongtype、业务错误、mutation event、DB side effect 的行为都和非事务执行保持同源；`EXEC` 不是一个特殊的小型解释器。

## `DISCARD`、abort 和错误路径

`DISCARD` 的语义很直接：丢弃 queue，清掉 `active` 和 `aborted`，回 `OK`。

但真正值得文档化的是 abort 路径：

- `MULTI` 嵌套：`ERR MULTI calls can not be nested`
- `DISCARD` 不在事务里：`ERR DISCARD without MULTI`
- `EXEC` 不在事务里：`ERR EXEC without MULTI`
- `MULTI` 中的 unknown command / parse error / disallowed-in-multi：立即回错，并标记 transaction aborted
- aborted 之后执行 `EXEC`：先 `discard()`，再回 `EXECABORT Transaction discarded because of previous errors.`

还有一条容易漏掉的关闭路径：连接进入 closing 状态时，`NettyExecutionConnection` 会丢弃事务状态，避免大 request snapshot 长期滞留在队列里。`TransactionQueueCleanupTest` 专门保护这个回归点。

## 队列限制和配置边界

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 同时受两类限制：

- `maxQueuedCommands`
- `maxQueuedBytes`

顺序是：

1. 如果条数已满，直接返回 `ERR Transaction queue is full`
2. 先用 `request.retainedBytes()` 做一次预估检查
3. 再执行 `ByteArrayExecutionRequest.copyOf(request)`
4. 用 snapshot 的 `retainedBytes()` 再做一次真实检查
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
