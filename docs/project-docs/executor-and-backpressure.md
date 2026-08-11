# Executor 与背压

本文解释命令为什么不直接在 I/O 线程里执行，以及 executor 如何用队列、预算、调度和 Netty 读写控制保护系统。

Yierdis 把“收包”和“执行命令”分开：Netty I/O 线程只解析并提交请求，`CommandExecutor` 在 owner executor 线程里串行执行 DB 访问；队列容量、queued bytes、连接 pending、全局 backlog 水位、Netty output writability 和 `autoRead` 一起形成背压。

## 主要对象

执行器链路的核心对象是：

- `CommandExecutor`：总装配层，持有 budget、task queue、submitter、drain loop、execution support 和 backpressure controller。
- `CommandExecutorSubmitter`：提交入口，负责 fail-fast reject、预算 reserve、连接 pending 统计和调度 drain。
- `ExecutorBacklogBudget`：全局 backlog 预算，限制 queue capacity 和 queued bytes，并给全局背压提供高低水位。
- `ExecutorTaskQueue`：调度队列，支持 `GLOBAL` 和 `FAIR`。
- `CommandExecutorDrainLoop`：cooperative drain loop，真正 poll task 并执行命令。
- `CommandExecutorExecutionSupport`：把 executor 任务接到生产环境中的 `CommandDispatcher` 准备入口、`RedisReplyRenderer`、`RedisReplyWriterFactory` 和 I/O adapter。
- `ExecutorBackpressureController`：直接读取 `ExecutionConnectionContext` 并通过真实 `ExecutionIoAdapter` 执行输入 disable/enable、关闭监听和 global recovery，不再经过投影 adapter。
- `ExecutionConnectionContext`：每个连接的 executor 状态，包括 pending count、pending bytes、closing、独立暂停原因和统计；它不持有调度队列。
- `NettyExecutionConnection`：Netty channel 到 executor connection 的 adapter，挂载 `EngineSession` 和 `ExecutionConnectionContext`。

Netty 侧还有 `NettyExecutionRequestIngress` 和 `YierdisServerChannelInitializer.WriteBufferBackpressureHandler`：前者把 registered request/reply slot 交给 executor，并在容量不足时等待；后者把 channel writability 变化反馈给 executor。

## 提交路径

请求提交主线是：

```text
RespRequestDecoder
  -> RetainedRespExecutionRequest / ExecutionRequest
  -> RegisteredRespMessage(request, replySlot)
  -> NettyExecutionRequestIngress
  -> CommandExecutor.tryAcquire(connection, retainedBytes)
  -> ExecutorAdmission.publish(request, replySlot)
```

`CommandExecutorSubmitter` 的顺序很重要：

1. 检查 executor 是否 running。
2. 检查连接是否已经 `closing`；已 closing 的连接在任何 queue slot / bytes budget 预留前直接拒绝。
3. 检查单个 request 是否永远不可能装入 configured bytes budget。
4. 用 `ExecutorBacklogBudget.tryReserve(...)` 在同一把锁下同时检查并预留 queue slot 和 queued bytes。
5. 预算成功时返回尚未发布的 `ExecutorAdmission`；ingress 再把 request 和 reply slot 一起 publish。
6. publish 时先在 `ExecutionConnectionContext.recordCommandEnqueued(...)` 增加 pending 和 pendingBytes，再向 `ExecutorTaskQueue.offer(...)` 投递 task。
7. 再次评估连接和全局背压，调度 drain loop。

queue slot 或 bytes budget 暂时不足会返回 `Unavailable`，不是终态拒绝。ingress 保留 submission、暂停输入，并通过 `onAdmissionAvailable(...)` 注册一次性容量回调后重试。只有 `request_too_large` 会在当前 reply slot 返回明确错误；not-running、connection-closing 或 publish invariant failure 会清理所有权并结束连接，避免打乱已注册 reply slot 的顺序。

## backlog budget

`ExecutorBacklogBudget` 是全局预算，不按连接拆分。它维护：

- `queueCapacity`：queued task 硬上限。
- `queueMaxBytes`：queued retained bytes 硬上限，`0` 表示不启用 bytes cap。
- `queuedTasks`：当前已 reserve 但未释放的任务数。
- `queuedBytes`：当前已 reserve 的 retained bytes。

admission 时在同一个临界区检查 task 与 byte 上限，两项都满足才一起增加计数，因此不存在只取得其中一项再回滚的中间状态。尚未 publish 的 admission 可以显式释放，publish 失败会回收 task 或关闭连接。命令执行完成后，`CommandExecutorExecutionSupport` 在同一把锁下归还两项预算并减少连接 pending 状态；满足当前 task/byte 条件的 capacity waiter 会在锁内摘下、锁外回调。

全局背压水位由 budget 根据硬上限推导：

- `globalBackpressureHighWatermark`：默认约为 queue capacity 的 75%。
- `globalBackpressureLowWatermark`：默认约为 high 的一半。
- `globalBackpressureBytesHighWatermark`：启用 `queueMaxBytes` 时默认约为 bytes cap 的 75%。
- `globalBackpressureBytesLowWatermark`：默认约为 bytes high 的一半。

高低水位提供 hysteresis，避免 `autoRead` 在边界附近频繁开关。

## GLOBAL 和 FAIR 调度

`ExecutorTaskQueue` 只负责排队和 poll，不理解命令语义。

`GLOBAL` 策略在队列锁内使用单个 `ArrayDeque`，所有连接共享 FIFO backlog。reply capacity 阻塞的头部单独保留，恢复或 stale reprepare 时仍先于后续任务执行。

`FAIR` 策略由同一个 `ExecutorTaskQueue` 用 identity-keyed 私有 state map 保存每个连接的 FIFO、阻塞头和 `scheduled` flag，再用 `activeKeys` 做 round-robin。空 state 会在不再 active 或 blocked 后删除，`ExecutionConnectionContext` 不暴露队列内部状态。生产 key 是 `NettyExecutionConnection`。

`FAIR` 的目标不是让命令并发执行，而是在多连接竞争时避免某个连接长期霸占 drain loop。

## drain loop

提交成功只代表任务进队列。真正执行发生在 `CommandExecutorDrainLoop`。

每次 drain tick 会不断 poll task，直到队列暂时为空，或者命中两个 cooperative budget 之一：

- `maxDrainCommands`
- `drainTimeLimitNanos`

单个 task 执行的大致顺序是：

1. 检查连接是否 active / closing。
2. 通过 `CommandDispatcher.prepare(session, request)` 准备 `PreparedCommand`，并根据 reservation shape 生成 reply plan。
3. 尝试预留 reply capacity；暂时不足时保留 prepared state 并等待容量回调。
4. 调用 `validateBeforeExecute()`；stale 时关闭并重新 prepare。
5. 创建不含 writer 的 `CommandExecutionContext`，执行 prepared command并取得 `CommandResult`。
6. 执行成功后创建 `RedisReplyWriter`，由 `RedisReplyRenderer` 渲染 `CommandResult.reply()`；`closeAfterReply` 为真时先把连接标为 closing，再把 reply 标为 ready。
7. 终态 finally 中释放 prepared command、request、backlog budget 和 connection pending 状态。

executor 不直接 write 或 flush transport。命令把 reply slot 标记为 READY 后，`ConnectionReplySequencer` 在连接 event loop 上按接收顺序写出连续 READY 的槽位，并为这一轮写出统一 flush；该过程不依赖 executor drain tick。

## 执行支持和回包写出

`CommandExecutorExecutionSupport` 是 executor 与命令准备、语义结果和 I/O adapter 的连接层。生产环境把 `CommandDispatcher::prepare` 作为窄的 `CommandExecutionEngine` 端口注入；这个端口只隔离 executor 与 command-core，不再承载另一套 engine 实现。它负责：

- 通过 reply slot 的 sink 写入语义结果；通过 I/O adapter 注册连接关闭监听，并在结果未知等终止路径关闭 transport。
- 从 `ExecutionConnection` 获取 `EngineSession`。
- 调用 `CommandDispatcher.prepare(session, request)`，并按 `PreparedCommand.reservationShape()` 规划/预留容量。
- 容量成功后建立请求级执行上下文，校验并执行 `PreparedCommand`，得到一个 `CommandResult`。
- 执行成功后才通过 `RedisReplyWriterFactory` 创建 writer，并由 `RedisReplyRenderer` 完成唯一一次命令结果渲染。
- 命令结束后释放 `ExecutorBacklogBudget` 中的 slot/queued bytes。
- 更新 `ExecutionConnectionContext.recordCommandFinished(...)`。
- 在连接 pending、本地 bytes 和全局 backlog 都恢复后尝试恢复 `autoRead`。

`NettyExecutionConnection` 把 Netty `Channel`、`EngineSession` 和 `ExecutionConnectionContext` 绑在一起。`EngineSession` 只拥有当前连接的 DB index、协议、事务、客户端元数据和连接统计；命令查找与执行语义由 dispatcher/prepared command 负责。事务、连接统计和 close-after-reply 都通过这个 connection root 传递，executor core 因此不需要直接依赖 Netty class。

`getOrCreate(...)` 通过 channel attr 保证同一条连接只拿到一个 root；`markClosing()` 会先把 `ExecutionConnectionContext` 置为 closing，再丢弃 `EngineSession` 里的事务状态，所以 `QUIT`、channel close 或 close-after-reply 不会留下继续排队的 snapshot。FAIR 调度也把它当作 per-connection key，而不是直接用 `Channel`。

## 背压来源

executor 路径有五类输入暂停来源。

第一类是 queue capacity。`queuedTasks >= queueCapacity` 时提交失败为 `queue_full`，并关闭当前连接 `autoRead`。

第二类是 queued bytes。`queueMaxBytes > 0` 且 reserve 后会超过上限时，提交失败为 `bytes_budget`，并关闭当前连接 `autoRead`。

第三类是 per-connection pending 状态。`ExecutionConnectionContext.pending()` 达到 `backpressureHighWatermark`，或 `pendingBytes()` 达到 `backpressureBytesHighWatermark`，该连接会被 executor 关闭 `autoRead`；恢复需要 pending 回落到 `backpressureLowWatermark`，pending bytes 回落到 `backpressureBytesLowWatermark`。

第四类是 reply capacity。当前 ordering scope 的头部无法取得回复容量时，executor 保留同一个 prepared task，把连接上下文标记为 `inputPausedByReply`，并等待 reply slot 的一次性容量回调；恢复这个头部前，不能仅因 backlog 水位下降而重新收包。

第五类是 Netty output writability。server 配置 `client-output-buffer-limit-bytes` 后，Netty channel 有 `WriteBufferWaterMark`。channel 变为不可写时，`WriteBufferBackpressureHandler` 调用 `CommandExecutor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；持续不可写超过 `client-output-buffer-over-limit-millis` 时，server 会关闭慢客户端。channel 恢复可写时，`onTransportWritable(...)` 调回 owner executor，由 execution support 统一判断是否恢复输入。

executor 的队列、字节、连接水位、reply capacity 和 transport 信号都通过 `ExecutorBackpressureController` 协调实际输入开关；controller 直接读取连接上下文中的独立暂停状态，并调用 `ExecutionIoAdapter`，避免恢复一个原因时覆盖另一个仍有效的原因。ingress pending deque 还会通过 `InboundReadCreditHandler` 记录 executor-admission 暂停，直到 pending submission 真正发布完毕。

## `autoRead`、writability 和 close-after-reply

需要明确写出单连接 pending、queued bytes、channel writability 和 flush/close 之间的关系，不要只停留在“有背压”这一层。

提交阶段先发生的是 backlog 和连接统计：

- `taskQueue.offer(...)` 成功后，`ExecutionConnectionContext.recordCommandEnqueued(...)` 增加 `pending` 和 `pendingBytes`
- 达到连接 high watermark、全局 backlog high watermark 或 queued bytes 上限附近时，executor 会关闭该连接 `autoRead`
- 这一步只阻止继续收包，不会取消已经入队的命令

执行阶段再叠加 transport 状态：

- `CommandExecutorExecutionSupport.execute(...)` 通过已注册 reply slot 的 sink 渲染语义结果，再把槽位标记为 READY
- `ConnectionReplySequencer` 在 event loop 上只写当前接收顺序中连续 READY 的槽位，并在该轮写出后 flush
- `WriteBufferBackpressureHandler` / `onTransportUnwritable(...)` 会把 channel 不可写也收敛成关闭 `autoRead`

`close-after-reply` 则是同一条链上的最后一步：

- 命令返回 `CommandResult.closeAfterReply(...)`，或 executor-thread 失败后补写 internal error 时，连接会先被标记为 `closing`
- reply sequencer 写出并 flush 这个 terminal slot，最终 write future 完成后再关闭 transport
- 因为 `closing` 已经置位，这条连接不会再进入 `autoRead` 恢复路径

所以 executor 恢复输入必须同时满足这些条件：

- executor 仍在运行
- 该连接不在 `closing`
- 当前没有 reply-capacity 暂停
- `pending <= backpressureLowWatermark`
- `pendingBytes <= backpressureBytesLowWatermark`（启用 bytes 水位时）
- 全局 backlog 已回落，且 transport 当前可写

这也是为什么“reply 已经写完”和“连接可以重新收包”不是一回事：前者只说明当前输出缓冲已经进入 flush/close 路径，后者还要同时满足本地 backlog、全局 backlog 和 Netty writability 三层条件。

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

server 侧的 task 直接调用 `YierdisInstanceRuntimeAccess.maintenanceTick()`。bootstrap 决定什么时候调度 tick，runtime 决定 tick 做什么，DB 仍只在 owner thread 上被访问。

## 统计和观测

executor 热路径用 `LongAdder` 和 connection context 记录观测值：

- submit accepted/rejected：`submitAccepted`、`submitRejectedQueueFull`、`submitRejectedBytesBudget`、`submitRejectedNotRunning`、`submitRejectedOfferFailed`
- 执行结果：`commandsExecuted`、`commandsSkippedClosing`、`closeAfterReply`
- backlog：`queuedTasks`、`queuedBytes`
- 背压：`channelsAutoReadDisabled`、`backpressureEnter`、`backpressureExit`
- drain budget：`drainLimitedByMaxCommands`、`drainLimitedByTimeBudget`
- connection stats：pending、pendingBytes、closing、inputDisabledByExecutor、commandsEnqueued、commandsRejected

这些数据进入 `CommandExecutor.StatsSnapshot`、`ExecutionConnectionContext.ConnectionStatsSnapshot`，再被 `STATS` / `INFO yierdis` 等观测命令使用。

## Bounded Reply Egress

executor 队列背压和 reply egress 是不同层次的 admission。每个请求先拥有 receive-order reply slot，`ReplyCapacityUnavailableException` 只在尚未产生可见 bytes 或 mutation 结果时允许延后重试；超过单回复上限或结果已经未知时连接必须关闭，不能补写普通 internal error。`FAIR` 可以让独立可运行连接绕过另一连接的本地回复等待，`GLOBAL` 保持全局 FIFO 头部不被跨越。

容量层级、slot/source/chunk 的终结所有权和 shutdown drain 的运维语义见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
