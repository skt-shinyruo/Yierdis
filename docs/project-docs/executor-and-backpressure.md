# Executor And Backpressure

本文专门解释 Yierdis 的执行器和背压机制。

如果你已经看过：

- [`request-execution-flow.md`](./request-execution-flow.md)
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)
- [`configuration-and-operations.md`](./configuration-and-operations.md)

那么这篇文档会把“请求如何入队、如何被调度、什么时候拒绝、什么时候关闭 `autoRead`、什么时候恢复，以及慢客户端输出缓冲怎么触发传输层背压”这条内部机制讲细。

## 先记住一句话

Yierdis 不是“收到请求就立刻执行”的 server。

它的核心模型是：

- I/O 线程负责收包和提交
- command executor 线程负责串行执行
- backlog budget 和 backpressure 负责防止系统无界积压
- Netty outbound buffer 也会参与背压，防止慢读客户端无限堆积待写数据

## 主角有哪些

这条机制里的核心对象有 9 个：

```text
YierdisServerChannelInitializer.WriteBufferBackpressureHandler
YierdisFastCommandHandler
CommandExecutor
  -> CommandExecutorSubmitter
  -> ExecutorBacklogBudget
  -> ExecutorTaskQueue
  -> CommandExecutorDrainLoop
  -> CommandExecutorExecutionSupport
  -> ExecutorBackpressureController
```

另外还有一个经常被忽略但很关键的连接态根对象：

- `NettyExecutionConnection`

它承载：

- session
- pending / pendingBytes
- closing 标记
- backpressure 统计
- fair scheduling 所需的 queue state

## `CommandExecutor` 是总控台

`CommandExecutor` 自己并不把所有逻辑写死在一个方法里，而是像总控台一样把几个子组件接起来：

- `ExecutorBacklogBudget`
- `ExecutorTaskQueue`
- `ExecutorBackpressureController`
- `CommandExecutorSubmitter`
- `CommandExecutorDrainLoop`
- `CommandExecutorExecutionSupport`

所以理解它时，最好不要把它看成“一个大执行函数”，而是看成：

- 负责装配提交、调度、回包、预算和背压的一层协调器

## 请求是怎么被提交进来的

提交入口在：

- `YierdisFastCommandHandler.channelRead0(...)`

这层只做一件事：

- 把 `ExecutionRequest` 交给 `CommandExecutor.trySubmit(...)`

如果提交失败，handler 立刻回：

- `ERR busy queue_full`
- `ERR busy bytes_budget`
- `ERR busy not_running`
- `ERR busy offer_failed`

也就是说，拒绝策略不是“默默丢请求”，而是 fail-fast。

## `CommandExecutorSubmitter` 真正在做什么

提交逻辑主要在 `CommandExecutorSubmitter.trySubmit(...)`。

这条路径可以记成：

1. 检查 executor 是否仍在运行
2. 读取当前连接的 `pending` 和 `pendingBytes`
3. 必要时先触发连接级 `autoRead` 关闭
4. 尝试向全局 backlog 预留一个 slot
5. 计算 `retainedBytes`
6. 尝试预留 queued-bytes 预算
7. 向 `ExecutorTaskQueue` 投递任务
8. 更新连接统计和背压状态
9. 调度 drain loop

### 为什么要先 reserve 再 offer

顺序不是随便写的。

如果不先 reserve：

- queue slot 和 bytes 预算就无法形成一致约束
- 失败时也很难知道该回滚哪一部分状态

正确顺序必须是：

- 先试图占预算
- 成功后再把任务真正放进队列
- 任一阶段失败都回滚

## backlog budget 负责什么

`ExecutorBacklogBudget` 负责全局预算，不区分连接。

它维护两种硬约束：

- 任务条数
- queued bytes

### 它内部维护哪些状态

- `queuedTasks`
- `queuedBytes`

以及四个由 capacity/maxBytes 推导出来的全局背压水位：

- `globalBackpressureHighWatermark`
- `globalBackpressureLowWatermark`
- `globalBackpressureBytesHighWatermark`
- `globalBackpressureBytesLowWatermark`

### 为什么这里也有高低水位

因为全局背压和单连接背压一样，都需要滞回。

如果只用一个阈值：

- 一旦稍微高于阈值就关闭
- 稍微低于阈值又开启

系统会很容易在边界附近抖动。

## 调度策略：`GLOBAL` 和 `FAIR`

`ExecutorTaskQueue` 是“只管排队、不管语义”的调度组件。

当前有两种策略：

### `GLOBAL`

- 单个全局 FIFO 队列

特点：

- 简单
- 更接近“所有连接共享一个全局 backlog”

### `FAIR`

- 每个连接一个本地队列
- 活跃连接放进 `activeKeys`
- drain 时 round-robin 轮转

特点：

- 避免单连接长期霸占执行器
- 更适合多连接竞争下的公平性

### `NettyExecutionConnection` 在这里为什么重要

因为 fair scheduling 需要每个连接都带一份调度状态。

这份状态不放在 protocol DTO，也不放在 session 本体，而是放在：

- `NettyExecutionConnection.context().queueState()`

这也是 `NettyExecutionConnection` 作为“连接态根对象”的一个关键作用。

## `CommandExecutorDrainLoop` 是真正执行命令的地方

提交成功并不代表命令已经执行。真正执行发生在：

- `CommandExecutorDrainLoop`

### drain tick 做什么

每次 tick 会不断 poll task，并在下面两个预算之一打满时停下：

- `maxDrainCommands`
- `drainTimeLimitNanos`

这意味着：

- drain 不是“把队列一次性跑空”
- 它是 cooperative 的

这样做的目的，是让同一 executor 上的其他任务也有机会被调度，例如：

- maintenance
- 其他排队命令

### `executeOne(...)` 做什么

单个任务执行时，大致顺序是：

1. 检查 channel 是否 active / closing
2. 为这条命令分配 output buffer
3. 创建 `ReplyWriter`
4. 调用 `executionSupport.execute(...)`
5. 把 reply 写入 channel
6. 如果需要 `close-after-reply`，flush 后关连接
7. 最终释放 request 并归还预算

### 为什么最后统一 flush

drain loop 使用了 `NettyReplyFlushBatch` 做 flush coalescing：

- 单条命令只 `write`
- tick 结束时统一 `flushAll()`

这让执行器在高频命令下减少 flush 次数。

## `CommandExecutorExecutionSupport` 为什么存在

这个类的职责是把“执行器视角”和“engine / command 层视角”接起来。

它负责：

- 通过 transport adapter 创建 `ReplyWriter`
- 从 connection 上取 `EngineSession`
- 调用 `CommandExecutionEngine.execute(...)`
- 在命令结束后归还 backlog 预算
- 在条件满足时恢复 `autoRead`

也就是说：

- drain loop 负责调度和生命周期
- execution support 负责把一次执行真正落成“命令 + 回包”

## 背压是怎么工作的

Yierdis 的背压不是只有一种，而是四类因素一起作用。

### 1. 单连接 pending 条数

当连接自己的：

- `pending >= backpressureHighWatermark`

就会尝试关闭该连接的 `autoRead`。

恢复条件则是：

- `pending <= backpressureLowWatermark`

### 2. 单连接 pending bytes

如果开启了 bytes 水位：

- `pendingBytes >= backpressureBytesHighWatermark`

也会触发 `autoRead` 关闭。

恢复条件则是：

- `pendingBytes <= backpressureBytesLowWatermark`

### 3. 全局 backlog 高水位

如果 `ExecutorBacklogBudget` 判断：

- 全局 queued tasks 或 queued bytes 已经达到高水位

也会触发更广义的 backpressure，并在预算恢复后做 global recovery。

### 4. Netty 输出缓冲不可写

如果 `--client-output-buffer-limit-bytes` 大于 `0`，`YierdisServerChannelInitializer` 会设置 Netty `WriteBufferWaterMark`。当 channel 变成不可写时：

- `WriteBufferBackpressureHandler` 调用 `executor.onTransportUnwritable(...)`
- 连接会进入传输层背压，避免继续读入更多请求
- 如果 channel 持续不可写超过 `--client-output-buffer-over-limit-millis`，server 会关闭这个慢客户端

当 channel 恢复可写时，handler 会取消慢客户端关闭任务，并交给 executor 重新评估是否可以恢复 `autoRead`。这条路径和 executor pending/backlog 背压共享恢复判断，不绕开连接本地状态。

## `ExecutorBackpressureController` 负责什么

这个类不是去决定“什么时候该背压”，而是负责：

- 记录哪些连接是被 executor 关掉 `autoRead` 的
- 封装 enable/disable 的统一动作
- 在全局压力恢复后做 best-effort 扫描恢复

这是一个重要分工：

- submitter / executionSupport 决定何时进入或退出背压
- controller 负责把这些决策转成一致动作

### 它为什么是 Netty-free 的

因为它依赖的是接口：

- `ExecutorBackpressureIo`
- `ExecutorBackpressureRuntime`
- `ExecutorBackpressureObserver`

真正的 Netty 行为由 `CommandExecutor` 在装配时提供。

这样做的好处是：

- 算法和 I/O 副作用解耦
- 核心控制逻辑更容易测试和复用

### global recovery 是怎么做的

当全局 backlog 恢复到低水位以下，controller 会：

1. 遍历 `keysWithAutoReadDisabled`
2. 跳过 inactive 或 closing 连接
3. 检查该连接自身的 pending 和 pendingBytes 是否也已回落
4. 满足条件才重新启用 `autoRead`

这说明 global recovery 不是“预算恢复了就全部放开”，而是：

- 全局条件和连接本地条件都要满足

## maintenance 为什么也走执行器

`CommandExecutor.executeMaintenance(...)` 会把 maintenance task 提交到同一个 executor 上执行。

这意味着：

- cleanup / maxmemory enforcement 不会绕过 owner thread
- 不会额外引入第二条直接操作 DB 的线程

这和 DB 的 owner-thread 约束是严格一致的。

## 统计和观测从哪来

执行器热路径维护了大量 `LongAdder` 计数器，例如：

- `submitAccepted`
- `submitRejectedQueueFull`
- `submitRejectedBytesBudget`
- `commandsExecuted`
- `commandsSkippedClosing`
- `backpressureEnter`
- `backpressureExit`
- `drainLimitedByMaxCommands`
- `drainLimitedByTimeBudget`

连接本地统计则放在 `NettyExecutionConnection` 里。

最终这些会被：

- `STATS`
- `INFO yierdis`

暴露出来。

## 对照源码时推荐看的顺序

1. `CommandExecutor`
   看总装和参数校验
2. `CommandExecutorSubmitter`
   看请求如何进入系统
3. `ExecutorBacklogBudget`
   看全局预算和水位
4. `ExecutorTaskQueue`
   看 GLOBAL/FAIR 调度
5. `CommandExecutorDrainLoop`
   看 cooperative drain
6. `CommandExecutorExecutionSupport`
   看命令执行和预算释放
7. `ExecutorBackpressureController`
   看 autoRead enter/exit/global recovery
8. `YierdisServerChannelInitializer`
   看 Netty write buffer watermark、慢客户端关闭和 idle timeout
9. `NettyExecutionConnection`
   看连接级统计和调度状态

## 最值得看的测试

- `CommandExecutorTest`
  看执行器主流程
- `CommandExecutorBackpressureTest`
  看 `queue_full`、autoRead enter/exit 和 global recovery
- `CommandExecutorFairSchedulingTest`
  看 fair scheduling 行为
- `YierdisServerBootstrapCommandWiringTest`
  看 runtime config 如何把这些机制接进 server，包括 output buffer watermark 和 idle timeout

## 一句话总结

Yierdis 的执行器不是一个“单线程 while-loop”，而是：

- 用 submitter 做 fail-fast 提交
- 用 budget 做全局约束
- 用 task queue 做调度
- 用 drain loop 做 cooperative 执行
- 用 backpressure controller 做连接级和全局恢复

这几层协作起来，才让它既保留了 Redis 风格单线程语义，又不会在压力下无界积压。
