# Executor And Backpressure

本文解释命令为什么不直接在 I/O 线程里执行，以及 executor 如何用队列、预算、调度和 Netty 读写控制保护系统。

## 先记住一句话

Yierdis 把“收包”和“执行命令”分开：Netty I/O 线程只解析并提交请求，`CommandExecutor` 在 owner executor 线程里串行执行 DB 访问；队列容量、queued bytes、连接 pending、全局 backlog 水位、Netty output writability 和 `autoRead` 一起形成背压。

## 主要对象

执行器链路的核心对象是：

- `CommandExecutor`：总装配层，持有 budget、task queue、submitter、drain loop、execution support 和 backpressure controller。
- `CommandExecutorSubmitter`：提交入口，负责 fail-fast reject、预算 reserve、连接 pending 统计和调度 drain。
- `ExecutorBacklogBudget`：全局 backlog 预算，限制 queue capacity 和 queued bytes，并给全局背压提供高低水位。
- `ExecutorTaskQueue`：调度队列，支持 `GLOBAL` 和 `FAIR`。
- `CommandExecutorDrainLoop`：cooperative drain loop，真正 poll task 并执行命令。
- `CommandExecutorExecutionSupport`：把 executor 任务接到 `CommandExecutionEngine`、`ReplyWriterFactory` 和 I/O adapter。
- `ExecutorBackpressureController`：统一执行 `autoRead` disable/enable 和 global recovery。
- `ExecutionConnectionContext`：每个连接的 executor 状态，包括 pending count、pending bytes、closing、fair queue state 和统计。
- `NettyExecutionConnection`：Netty channel 到 executor connection 的 adapter，挂载 `EngineSession` 和 `ExecutionConnectionContext`。

Netty 侧还有 `YierdisFastCommandHandler` 和 `YierdisServerChannelInitializer.WriteBufferBackpressureHandler`：前者把 decoded request 交给 executor，后者把 channel writability 变化反馈给 executor。

## 提交路径

请求提交主线是：

```text
RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter / ExecutionRequest
  -> YierdisFastCommandHandler.channelRead0(...)
  -> CommandExecutor.trySubmit(connection, request)
  -> CommandExecutorSubmitter.trySubmit(...)
```

`CommandExecutorSubmitter` 的顺序很重要：

1. 检查 executor 是否 running。
2. 根据全局 backlog、连接 pending count、连接 pending bytes 必要时关闭该连接 `autoRead`。
3. 调用 `ExecutorBacklogBudget.tryReserveSlot()` 预留 queue slot。
4. 从 `ExecutionRequest.retainedBytes()` 计算 retained bytes。
5. 调用 `tryReserveQueuedBytes(retainedBytes)` 预留 queued bytes。
6. 向 `ExecutorTaskQueue.offer(...)` 投递 `CommandExecutorTask`。
7. 在 `ExecutionConnectionContext.recordCommandEnqueued(...)` 增加 pending 和 pendingBytes。
8. 再次评估连接和全局背压，调度 drain loop。

失败时不会默默丢请求。`trySubmit(...)` 返回 reject reason，Netty handler 回 `ERR busy <reason>`，当前 reason 包括 `not_running`、`queue_full`、`bytes_budget`、`offer_failed`。

## backlog budget

`ExecutorBacklogBudget` 是全局预算，不按连接拆分。它维护：

- `queueCapacity`：queued task 硬上限。
- `queueMaxBytes`：queued retained bytes 硬上限，`0` 表示不启用 bytes cap。
- `queuedTasks`：当前已 reserve 但未释放的任务数。
- `queuedBytes`：当前已 reserve 的 retained bytes。

提交时先 reserve slot，再 reserve queued bytes，offer 失败或后续异常会回滚已 reserve 的预算。命令执行完成后，`CommandExecutorExecutionSupport` 释放 slot 和 queued bytes，并减少连接 pending 状态。

全局背压水位由 budget 根据硬上限推导：

- `globalBackpressureHighWatermark`：默认约为 queue capacity 的 75%。
- `globalBackpressureLowWatermark`：默认约为 high 的一半。
- `globalBackpressureBytesHighWatermark`：启用 `queueMaxBytes` 时默认约为 bytes cap 的 75%。
- `globalBackpressureBytesLowWatermark`：默认约为 bytes high 的一半。

高低水位提供 hysteresis，避免 `autoRead` 在边界附近频繁开关。

## GLOBAL 和 FAIR 调度

`ExecutorTaskQueue` 只负责排队和 poll，不理解命令语义。

`GLOBAL` 策略使用单个 `ArrayBlockingQueue`，所有连接共享 FIFO backlog。这条路径简单，适合把 executor 看成一个全局串行队列。

`FAIR` 策略给每个连接一条本地 queue，并用 `activeKeys` 做 round-robin。`ExecutionConnectionContext.queueState()` 保存该连接的 local queue 和 `scheduled` flag；`scheduled` 防止同一连接被重复放进 active set。生产 key 是 `NettyExecutionConnection`，测试可以用轻量 key state provider。

`FAIR` 的目标不是让命令并发执行，而是在多连接竞争时避免某个连接长期霸占 drain loop。

## drain loop

提交成功只代表任务进队列。真正执行发生在 `CommandExecutorDrainLoop`。

每次 drain tick 会不断 poll task，直到队列暂时为空，或者命中两个 cooperative budget 之一：

- `maxDrainCommands`
- `drainTimeLimitNanos`

单个 task 执行的大致顺序是：

1. 检查连接是否 active / closing。
2. 为 reply 分配 output buffer。
3. 创建 `ReplyWriter`。
4. 调用 `CommandExecutorExecutionSupport.execute(...)`。
5. 将 reply 写入 channel。
6. 如需 close-after-reply，flush 后关闭连接。
7. finally 中释放 request、backlog budget 和 connection pending 状态。

drain loop 使用 batched flush：单条命令通常只 `write`，tick 结束时统一 flush，减少高频命令下的 flush 次数。

## 执行支持和回包写出

`CommandExecutorExecutionSupport` 是 executor 与 command engine 的连接层。它负责：

- 通过 I/O adapter 检查 channel active/writable、分配 output、write/flush/close。
- 通过 `ReplyWriterFactory` 创建 `ReplyWriter`。
- 从 `ExecutionConnection` 获取 `EngineSession`。
- 调用 `CommandExecutionEngine.execute(session, request, writer)`。
- 命令结束后释放 `ExecutorBacklogBudget` 中的 slot/queued bytes。
- 更新 `ExecutionConnectionContext.recordCommandFinished(...)`。
- 在连接 pending、本地 bytes 和全局 backlog 都恢复后尝试恢复 `autoRead`。

`NettyExecutionConnection` 把 Netty `Channel`、`EngineSession` 和 `ExecutionConnectionContext` 绑在一起。事务、连接统计和 close-after-reply 都通过这个 connection root 传递，executor core 因此不需要直接依赖 Netty class。

## 背压来源

背压有四类来源。

第一类是 queue capacity。`queuedTasks >= queueCapacity` 时提交失败为 `queue_full`，并关闭当前连接 `autoRead`。

第二类是 queued bytes。`queueMaxBytes > 0` 且 reserve 后会超过上限时，提交失败为 `bytes_budget`，并关闭当前连接 `autoRead`。

第三类是 per-connection pending 状态。`ExecutionConnectionContext.pending()` 达到 `backpressureHighWatermark`，或 `pendingBytes()` 达到 `backpressureBytesHighWatermark`，该连接会被 executor 关闭 `autoRead`；恢复需要 pending 回落到 `backpressureLowWatermark`，pending bytes 回落到 `backpressureBytesLowWatermark`。

第四类是 Netty output writability。server 配置 `client-output-buffer-limit-bytes` 后，Netty channel 有 `WriteBufferWaterMark`。channel 变为不可写时，`WriteBufferBackpressureHandler` 调用 `CommandExecutor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；持续不可写超过 `client-output-buffer-over-limit-millis` 时，server 会关闭慢客户端。channel 恢复可写时，`onTransportWritable(...)` 调回 owner executor，由 execution support 统一判断是否恢复输入。

这四类背压最终都收敛到 `ExecutorBackpressureController.disableAutoRead(...)`，避免不同路径各自操作 Netty `autoRead`。

## global recovery

`ExecutorBackpressureController` 记录哪些连接是被 executor 关闭输入的。全局 backlog 恢复到低水位后，它不会无条件打开所有连接，而是做 best-effort recovery：

1. 遍历 `keysWithAutoReadDisabled`。
2. 跳过 inactive 或 closing 连接。
3. 检查该连接自身 pending count 是否低于 low watermark。
4. 检查 pending bytes 是否低于 bytes low watermark。
5. 检查 transport 是否 writable。
6. 条件全部满足才 clear executor-disabled flag 并 enable `autoRead`。

因此 global recovery 是“全局压力恢复 + 连接本地压力恢复 + Netty 可写”三者共同决定。

## maintenance task

`CommandExecutor.executeMaintenance(...)` 把 maintenance task 投递到同一个 owner executor。这样 expired cleanup、maxmemory enforcement 和 runtime maintenance 不会绕过 DB owner-thread 约束。

server 侧的 task 通常来自 `YierdisInstanceMaintenance`，它只委托 `YierdisInstanceRuntimeAccess.maintenanceTick()`。bootstrap 决定什么时候调度 tick，runtime 决定 tick 做什么，DB 仍只在 owner thread 上被访问。

## 统计和观测

executor 热路径用 `LongAdder` 和 connection context 记录观测值：

- submit accepted/rejected：`submitAccepted`、`submitRejectedQueueFull`、`submitRejectedBytesBudget`、`submitRejectedNotRunning`、`submitRejectedOfferFailed`
- 执行结果：`commandsExecuted`、`commandsSkippedClosing`、`closeAfterReply`
- backlog：`queuedTasks`、`queuedBytes`
- 背压：`channelsAutoReadDisabled`、`backpressureEnter`、`backpressureExit`
- drain budget：`drainLimitedByMaxCommands`、`drainLimitedByTimeBudget`
- connection stats：pending、pendingBytes、closing、inputDisabledByExecutor、commandsEnqueued、commandsRejected

这些数据进入 `CommandExecutor.StatsSnapshot`、`ExecutionConnectionContext.ConnectionStatsSnapshot`，再被 `STATS` / `INFO yierdis` 等观测命令使用。

## 推荐源码和测试

推荐源码顺序：

1. `CommandExecutor`
2. `CommandExecutorSubmitter`
3. `ExecutorBacklogBudget`
4. `ExecutorTaskQueue`
5. `CommandExecutorDrainLoop`
6. `CommandExecutorExecutionSupport`
7. `ExecutorBackpressureController`
8. `ExecutionConnectionContext`
9. `NettyExecutionConnection`
10. `YierdisServerChannelInitializer`

推荐测试：

- `CommandExecutorTest`：执行器主流程。
- `CommandExecutorBackpressureTest`：queue full、bytes budget、`autoRead` enter/exit 和 global recovery。
- `CommandExecutorFairSchedulingTest`：`GLOBAL` / `FAIR` 调度差异。
- `ExecutionConnectionContextTest`：连接 pending、pending bytes 和状态统计。
- `NettyExecutionAdapterIntegrationTest`：Netty adapter 到 executor request/reply 的边界。
- `YierdisServerBootstrapCommandWiringTest`：server 配置如何接入 output buffer watermark、idle timeout 和 executor。
