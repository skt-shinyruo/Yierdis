# Executor 与背压

命令为什么不直接在 I/O 线程里执行？executor 又如何用队列、预算、调度和 Netty 读写控制保护系统？这是本节要回答的两个问题。

Yierdis 把“收包”和“执行命令”分开：Netty I/O 线程只解析并提交请求，`CommandExecutor` 在 owner executor 线程里串行执行 DB 访问；队列容量、queued bytes、连接 pending、全局 backlog 水位、Netty output writability 和 `autoRead` 一起形成背压。

## owner thread 与线程切换点

理解背压之前先要理解谁在哪个线程上跑。

- **Netty worker event loop 线程**：跑 `RespRequestDecoder`、`NettyExecutionRequestIngress.channelRead(...)`，负责解析和提交。
- **owner executor 线程**：由 bootstrap 里的 `DefaultEventExecutorGroup(1)` 提供，被包装成 `NettySerialOwnerExecutor`。它跑 `CommandExecutorDrainLoop`，也就是真正的 `CommandDispatcher.prepare(...)`、`PreparedCommand.execute(...)` 和 DB mutation。
- **连接的 channel event loop 线程**：回复写出由 `ConnectionReplySequencer` 在它上面完成，不依赖 executor drain tick。

切换发生在提交路径上，而不是执行路径上：

```text
Netty worker event loop
  RespRequestDecoder -> RegisteredRespMessage
  -> NettyExecutionRequestIngress.channelRead(...)
  -> CommandExecutor.tryAcquire(connection, retainedBytes)   // 锁保护，任意线程可调
  -> ExecutorAdmission.publish(request, replySlot)           // 入队 + scheduleDrain
  -> ownerExecutor.execute(drainLoop)                        // 切到 owner thread
owner engine thread
  CommandExecutorDrainLoop.drainLoop()
  -> CommandExecutorExecutionSupport.execute(task)           // prepare / execute / render
  -> ConnectionReplySequencer 在 channel event loop 上写出
```

`SerialOwnerExecutor` 只有两个方法：`execute(Runnable)` 和 `inOwnerThread()`（默认实现 `requireOwnerThread()` 在非同线程时抛 `IllegalStateException`）。`NettySerialOwnerExecutor.inOwnerThread()` 直接转发 `delegate.inEventLoop()`。`CommandExecutor.start()` 在 owner 上执行 `bindToCurrentThread`，把这条线程标记成 DB owner；`executeMaintenance(...)`、`executeOwnerTask(...)`、`onTransportWritable(...)` 都经 owner 提交，并在任务体里先 `requireOwnerThread()`。因此“跨线程直接访问 DB”会 fail-fast，这就是单 owner 契约的执行方式。

## 主要对象

执行器链路的核心对象是：

- `CommandExecutor`：总装配层，持有 budget、task queue、submitter、drain loop、execution support 和 backpressure controller。
- `CommandExecutorSubmitter`：提交入口，承担 fail-fast reject、预算 reserve、连接 pending 统计和调度 drain。
- `ExecutorBacklogBudget`：全局 backlog 预算，限制 queue capacity 和 queued bytes，并给全局背压提供高低水位。
- `ExecutorTaskQueue`：调度队列，支持 `GLOBAL` 和 `FAIR`。
- `CommandExecutorDrainLoop`：cooperative drain loop，实际 poll task 并执行命令。
- `CommandExecutorExecutionSupport`：把 executor 任务接到生产环境中的 `CommandDispatcher` 准备入口、`RedisReplyRenderer`、`RedisReplyWriter` 和 I/O adapter。
- `ExecutorBackpressureController`：直接读取 `ExecutionConnectionContext` 并通过真实 `ExecutionIoAdapter` 执行输入 disable/enable、关闭监听和 global recovery，不再经过投影 adapter。
- `ExecutionConnectionContext`：每个连接的 executor 状态，包括 pending count、pending bytes、closing、独立暂停原因和统计；它不持有调度队列。
- `NettyExecutionConnection`：Netty channel 到 executor connection 的 adapter，挂载 `EngineSession` 和 `ExecutionConnectionContext`。

Netty 侧还有 `NettyExecutionRequestIngress` 和 `YierdisServerChannelInitializer.WriteBufferBackpressureHandler`：前者把 registered request/reply slot 交给 executor，并在容量不足时等待；后者把 channel writability 变化反馈给 executor。连接的输入开关最终经 `NettyExecutionIoAdapter` 落到 `InboundReadCreditHandler.pauseExecutorInput()/resumeExecutorInput()`（拿不到该 handler 时退化为 `setAutoRead`）。

## 提交路径

请求提交主线是：

```text
RespRequestDecoder
  -> RespDecodedMessage.Request(ByteArrayExecutionRequest)
  -> RegisteredRespMessage(decodedMessage, replySlot)
  -> NettyExecutionRequestIngress
  -> CommandExecutor.tryAcquire(connection, retainedBytes)
  -> ExecutorAdmission.publish(request, replySlot)
```

`CommandExecutorSubmitter.tryAcquire(...)` 的顺序很重要：

1. 检查 executor 是否 running；否则记 `NOT_RUNNING`。
2. 检查连接是否已经 `closing`；已 closing 的连接在任何 queue slot / bytes budget 预留前直接拒绝，记 `CONNECTION_CLOSING`。
3. 检查单个 request 是否永远不可能装入 configured bytes budget（`canEverReserveQueuedBytes`）；否则记 `REQUEST_TOO_LARGE`。
4. 用 `ExecutorBacklogBudget.tryReserve(...)` 在同一把锁下同时检查并预留 queue slot 和 queued bytes。
5. 预算成功时返回尚未发布的 `ExecutorAdmission`；ingress 再把 request 和 reply slot 一起 publish。
6. publish 时先在 `ExecutionConnectionContext.recordCommandEnqueued(...)` 增加 pending 和 pendingBytes，再向 `ExecutorTaskQueue.offer(...)` 投递 task。
7. 再次评估连接和全局背压，调度 drain loop。

不同结果的去向不同，这一点决定了客户端观察到什么：

| 结果 | 触发条件 | 服务端行为 | 客户端观感 |
| --- | --- | --- | --- |
| `Acquired`（正常） | 预留成功 | publish，任务入队 | 后续收到正常 reply |
| `Unavailable(QUEUE_SLOTS)` | `queuedTasks >= queueCapacity` | 暂停该连接输入，注册容量回调后重试；记 `submit_rejected_queue_full_total` | 请求被拖后，不是错误 |
| `Unavailable(QUEUE_BYTES)` | `queueMaxBytes > 0` 且预留后超限 | 同上；记 `submit_rejected_bytes_budget_total` | 请求被拖后，不是错误 |
| `Rejected(REQUEST_TOO_LARGE)` | 单请求本身超过 bytes hard limit | 当前 reply slot 写 `ERR request exceeds executor queue byte limit` | 拿到 `-ERR` |
| `Rejected(NOT_RUNNING)` | executor 已停 | 清理所有权并关闭连接 | 连接断开 |
| `Rejected(CONNECTION_CLOSING)` | 连接已 closing | 清理所有权、结束该 slot，不补 `ERR busy` | 连接关闭 |
| `Rejected(OFFER_FAILED)` | publish 时 offer 抛异常 | 回滚计数、清理 task、终止连接；记 `submit_rejected_offer_failed_total` | 连接断开 |

`Unavailable` 不是终态拒绝。ingress 保留 submission、暂停输入，并通过 `onAdmissionAvailable(...)` 注册一次性容量回调后重试。只有 `REQUEST_TOO_LARGE` 会在当前 reply slot 写出普通错误；not-running、connection-closing 或 publish invariant failure 会清理所有权并结束连接，避免打乱已注册 reply slot 的顺序。

`NettyExecutionRequestIngress` 还维护一个 pending submission FIFO 和一个容量注册句柄。FIFO 里只要还有未发布的提交，后面的协议错误或内部错误都不能插队刷出——那会破坏“回包顺序 = 收包顺序”。因此这些场景下会取消未发布 slot 并随连接拆除收敛（fail-closed EOF）。

## backlog budget

`ExecutorBacklogBudget` 是全局预算，不按连接拆分。它维护：

- `queueCapacity`：queued task 硬上限。
- `queueMaxBytes`：queued retained bytes 硬上限，`0` 表示不启用 bytes cap。
- `queuedTasks`：当前已 reserve 但未释放的任务数。
- `queuedBytes`：当前已 reserve 的 retained bytes。

这里的 retained bytes 是请求的 heap footprint 估算（见 glossary 的 retained bytes 条目），涵盖请求对象、argv 数组与槽位，以及每个参数的数组头与对齐 payload；因此高 `argc` 的小参数命令会比按纯 payload 求和更早触及 bytes cap。

admission 时在同一个临界区检查 task 与 byte 上限，两项都满足才一起增加计数，不存在只取得其中一项再回滚的中间状态。waiters 也挂在同一把锁上：`release(...)` 和 `onCapacityAvailable(...)` 都会在锁内挑出已经满足容量条件的 waiter，摘下来后在锁外回调，避免在持锁时重新进入 ingress。尚未 publish 的 admission 可以显式释放，publish 失败会回收 task 或关闭连接。命令执行完成后，`CommandExecutorExecutionSupport` 在同一把锁下归还两项预算并减少连接 pending 状态。

全局背压水位由 budget 根据硬上限推导，不单独暴露成 CLI 参数：

- `globalBackpressureHighWatermark`：`queueCapacity - queueCapacity/4`，默认 `1024 - 256 = 768`，约为容量 75%。
- `globalBackpressureLowWatermark`：`globalHigh / 2`，默认 `384`。
- `globalBackpressureBytesHighWatermark`：启用 `queueMaxBytes` 时约为 bytes cap 的 75%（`max(1, (queueMaxBytes/4)*3 + ...)`）；禁用时为 `0`。
- `globalBackpressureBytesLowWatermark`：bytes high 的一半。

高低水位提供 hysteresis，避免 `autoRead` 在边界附近频繁开关。`isGlobalBackpressureHigh()` 在任务数或字节任一越线时为真；`isGlobalBackpressureCleared()` 要求两者都回落到低水位。

## GLOBAL 和 FAIR 调度

`ExecutorTaskQueue` 只做排队和 poll，不理解命令语义。两种策略共用一把私有锁，但状态结构不同。

`GLOBAL` 策略在队列锁内使用单个 `ArrayDeque`，所有连接共享 FIFO backlog。reply capacity 阻塞的头部单独保留（`globalBlockedHead` + `globalBlockedHeadReady`），恢复或 stale reprepare 时仍先于后续任务执行，因此全局头部不会被跨越。

`FAIR` 策略在同一个 `ExecutorTaskQueue` 内维护 identity-keyed 私有 state map，为每个连接保存 FIFO、阻塞头和 `scheduled` flag，再用 `activeKeys` 做 round-robin。空 state 会在不再 active 或 blocked 后删除，`ExecutionConnectionContext` 不暴露队列内部状态。生产 key 是 `NettyExecutionConnection`（identity 语义，而不是 `Channel` 的 equals）。

`FAIR` 并不让命令并发执行；它的目标是在多连接竞争时，避免某个连接长期霸占 drain loop，也让等待自身 reply capacity 的连接不阻塞其他可运行连接。

## drain loop

提交成功只代表任务已进队列；真正的执行发生在 `CommandExecutorDrainLoop`。

`scheduleDrain()` 用 `drainScheduled` 做去重：一次只允许一个 drain 任务在 owner 上排队。`drainLoop()` 在 owner 线程不断 poll task，直到队列暂时为空，或者命中两个 cooperative budget 之一：

- `maxDrainCommands`（`--executorMaxDrain`）
- `drainTimeLimitNanos`（`--executorDrainMillis` 换算）

命中预算且队列仍有可运行任务时，分别累计 `drain_limited_max_commands_total` / `drain_limited_time_budget_total`，并再调度一轮，避免长时间独占 owner 线程。

单个 task 执行的大致顺序是：

1. 检查连接是否 active / closing（同时回看 transport 活性）；不满足就记 `commandsSkippedClosing` 并回收。
2. 调用 `CommandDispatcher.prepare(session, request)` 准备 `PreparedCommand`，并根据 reservation shape 生成 reply plan（协议版本 PB 在 prepare/预留时刻读取一次并捕获进 reply plan）。
3. 尝试预留 reply capacity；暂时不足时把任务 `block` 在队列头部、标记 `inputPausedByReply`、暂停输入并等待一次性容量回调。
4. 调用 `validateBeforeExecute()`；stale 时关闭并重新 prepare（`retryAtHead` 放回头部）。
5. 把同一个 `CommandSession` 交给 `PreparedCommand.execute(...)`，并取得 `CommandResult`。
6. 执行成功后创建 `RedisReplyWriter`，由 `RedisReplyRenderer` 渲染 `CommandResult.reply()`；`closeAfterReply` 为真时先把连接标为 closing，再把 reply 标为 ready。
7. 终态 finally 中释放 prepared command、request、backlog budget 和 connection pending 状态，并按条件尝试恢复输入。

executor 不直接 write 或 flush transport。命令把 reply slot 标记为 READY 后，`ConnectionReplySequencer` 在连接 event loop 上按接收顺序写出连续 READY 的槽位，并在这一轮写出后统一 flush；该过程不依赖 executor drain tick。

## 执行支持和回包写出

`CommandExecutorExecutionSupport` 是 executor 与命令准备、语义结果和 I/O adapter 的连接层。生产环境直接把 `CommandDispatcher::prepare` 作为 `BiFunction<CommandSession, ExecutionRequest, PreparedCommand>` 注入 executor。职责包括：

- 把语义结果写进 reply slot 的 sink；通过 I/O adapter 注册连接关闭监听，并在结果未知等终止路径关闭 transport。
- 从 `ExecutionConnection` 获取 `EngineSession`。
- 调用 `CommandDispatcher.prepare(session, request)`，并按 `PreparedCommand.reservationShape()` 规划/预留容量。
- 容量成功后校验 `PreparedCommand`，再使用当前 `CommandSession` 执行，得到一个 `CommandResult`。
- 执行成功后才为当前 session 和 reply sink 创建 `RedisReplyWriter`，并由 `RedisReplyRenderer` 完成唯一一次命令结果渲染。
- 命令结束后释放 `ExecutorBacklogBudget` 中的 slot/queued bytes，并更新 `ExecutionConnectionContext.recordCommandFinished(...)`。
- 在连接 pending、本地 bytes 和全局 backlog 都恢复后尝试恢复 `autoRead`。

`NettyExecutionConnection` 把 Netty `Channel`、`EngineSession` 和 `ExecutionConnectionContext` 绑在一起。`EngineSession` 只拥有当前连接的 DB index、协议、事务、客户端元数据和连接统计；命令查找与执行语义由 dispatcher/prepared command 负责。事务、连接统计和 close-after-reply 都通过这个 connection root 传递，executor core 因此不需要直接依赖 Netty class。

`getOrCreate(...)` 用 channel attr 保证同一条连接只拿到一个 root；`markClosing()` 会先把 `ExecutionConnectionContext` 置为 closing，再丢弃 `EngineSession` 里的事务状态，所以 `QUIT`、channel close 或 close-after-reply 不会留下继续排队的 snapshot。FAIR 调度也把它当作 per-connection key，而不是直接用 `Channel`。

## 背压来源

executor 的输入暂停来源共五类。

第一类是 queue capacity。`queuedTasks >= queueCapacity` 时 `ExecutorBacklogBudget.tryReserve` 返回 `BlockReason.QUEUE_SLOTS`，提交结果是 `Unavailable`，并关闭当前连接 `autoRead`。对应计数是 `submit_rejected_queue_full_total`。

第二类是 queued bytes。`queueMaxBytes > 0` 且 reserve 后会超过上限时，返回 `BlockReason.QUEUE_BYTES`，提交结果同样是 `Unavailable`，并关闭当前连接 `autoRead`。对应计数是 `submit_rejected_bytes_budget_total`。

第三类是 per-connection pending 状态。`ExecutionConnectionContext.pending()` 达到 `backpressureHighWatermark`，或 `pendingBytes()` 达到 `backpressureBytesHighWatermark`，该连接会被 executor 关闭 `autoRead`；恢复需要 pending 回落到 `backpressureLowWatermark`，pending bytes 回落到 `backpressureBytesLowWatermark`。这两条水位由 CLI 的 `--backpressureHigh/Low` 和 `--backpressureBytesHigh/Low` 直接给出，与 budget 推导出的全局水位是两套独立阈值。

第四类是 reply capacity。当前 ordering scope 的头部无法取得回复容量时，executor 保留同一个 prepared task，把连接上下文标记为 `inputPausedByReply`，并等待 reply slot 的一次性容量回调；恢复这个头部前，不能仅因 backlog 水位下降而重新收包。

第五类是 Netty output writability。server 配置 `client-output-buffer-limit-bytes` 后，Netty channel 有 `WriteBufferWaterMark`。channel 变为不可写时，`WriteBufferBackpressureHandler` 调用 `CommandExecutor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；持续不可写超过 `client-output-buffer-over-limit-millis` 时，server 经 `NettyExecutionConnection.initiateClose()` 关闭慢客户端（先收敛 closing 语义，再关闭 transport）。channel 恢复可写时，`onTransportWritable(...)` 调回 owner executor，由 execution support 统一判断是否恢复输入。

executor 的队列、字节、连接水位、reply capacity 和 transport 信号都通过 `ExecutorBackpressureController` 协调实际输入开关。`ExecutionConnectionContext` 为每个暂停原因维护独立 flag（`inputDisabledByExecutor`、`inputPausedByReply`、`closing`），`enableAutoReadIfWeDisabled(...)` 会先检查 reply 暂停和 transport 可写性，避免恢复一个原因时覆盖另一个仍有效的原因。ingress 侧的 pending deque 则由 `InboundReadCreditHandler` 记录 executor-admission 暂停，直到所有 pending submission 发布完毕。

**触发背压后客户端观察到什么**：大部分情况下什么错误都不会收到——连接只是不再读取新数据，已入队命令继续执行并回包，直到 backlog 回落才恢复收包。客户端表现为响应变慢、pipeline 里的后续请求被拖后。只有当单个请求超过 bytes hard limit 时才会立刻收到 `-ERR request exceeds executor queue byte limit`；连接关闭路径下客户端看到的是 EOF/断连，而不是一条替代错误。

## `autoRead`、writability 和 close-after-reply

提交阶段先处理 backlog 与连接统计：

- publish 时先调用 `ExecutionConnectionContext.recordCommandEnqueued(...)` 增加 `pending` 和 `pendingBytes`，再向 `taskQueue.offer(...)` 投递 task。
- 达到连接 high watermark、全局 backlog high watermark 或 queued bytes 上限附近时，executor 会关闭该连接 `autoRead`。
- 这一步只阻止继续收包，不会取消已经入队的命令。

执行阶段再叠加 transport 状态：

- `CommandExecutorExecutionSupport.execute(...)` 通过已注册 reply slot 的 sink 渲染语义结果，再把槽位标记为 READY。
- `ConnectionReplySequencer` 在 event loop 上只写当前接收顺序中连续 READY 的槽位，并在该轮写出后 flush。
- `WriteBufferBackpressureHandler` / `onTransportUnwritable(...)` 会把 channel 不可写也收敛成关闭 `autoRead`。

`close-after-reply` 则是同一条链上的最后一步：

- 命令返回 `CommandResult.closeAfterReply(...)`，或 executor-thread 失败后补写 internal error 时，连接会先被标记为 `closing`。
- reply sequencer 写出并 flush 这个 terminal slot，最终 write future 完成后再关闭 transport。
- 因为 `closing` 已经置位，这条连接不会再进入 `autoRead` 恢复路径。

所以 executor 恢复输入必须同时满足这些条件：

- executor 仍在运行；
- 该连接不在 `closing`；
- 当前没有 reply-capacity 暂停；
- `pending <= backpressureLowWatermark`；
- `pendingBytes <= backpressureBytesLowWatermark`（启用 bytes 水位时）；
- 全局 backlog 已回落，且 transport 当前可写。

因此“reply 已经写完”与“连接可以重新收包”并不等价：前者只说明当前输出缓冲已经进入 flush/close 路径，后者还要同时满足本地 backlog、全局 backlog 和 Netty writability 三层条件。

## global recovery

`ExecutorBackpressureController` 用 `connectionsWithAutoReadDisabled` 记录哪些连接是被 executor 关闭输入的，并用 `globalRecoveryScheduled` 保证同一时刻只有一次恢复扫描在 owner 上排队。全局 backlog 回到低水位后，它只做 best-effort recovery，不会无条件打开所有连接：

1. 遍历 `connectionsWithAutoReadDisabled`。
2. 跳过 inactive 或 closing 连接。
3. 检查该连接自身 pending count 是否低于 low watermark。
4. 检查 pending bytes 是否低于 bytes low watermark。
5. 检查 transport 是否 writable，且没有 reply-capacity 暂停。
6. 条件全部满足才 clear executor-disabled flag 并 enable `autoRead`（`backpressure_exit` 计数在这里累加）。

因此 global recovery 由三者共同决定：全局压力恢复、连接本地压力恢复和 Netty 可写。

## result-unknown 的来源

命令执行阶段的异常分两类，处理方式不同。

- **可安全补错**：尚未产生可见字节、也没有提交 mutation 时，`handleReplyExecutionFailure(...)` 会在当前 reply slot 写 `ERR internal error` 并把连接标为 close-after-reply。这是确定的失败，客户端能拿到一条错误。
- **result-unknown**：命令已经执行过（`executed == true`），或异常链里出现 `ResultUnknownException`（来自 `yier.bubu.redis.common.command.ResultUnknownException`）时，说明 mutation 可能已经提交、reply 字节可能已经可见，或写结果已经不可判定。此时 `closeResultUnknown(...)` 只做收敛：`markClosing()`、`reply.markResultUnknown()`、`reply.cancel()`、`ioAdapter.closeConnection(...)`，**不补写**任何替代回复。伪造 `-ERR internal error` 会声称一个可能和已可见 mutation / 部分 reply 矛盾的结果。

超限回复（`ReplyReservationResult.TOO_LARGE`，即单条 reply 超过 `replyMaxTotalBytes`）走 `closeOversizedReply(...)`：同样 cancel slot、关 transport，不尝试复用同一槽位补发内部错误。

## maintenance task

`CommandExecutor.executeMaintenance(...)` 把 maintenance task 投递到同一个 owner executor。这样 expired cleanup、maxmemory enforcement 和 runtime maintenance 不会绕过 DB owner-thread 约束。

server 侧的 task 直接调用 `YierdisInstanceRuntimeAccess.maintenanceTick()`。bootstrap 决定什么时候调度 tick，runtime 决定 tick 做什么，DB 访问仍只发生在 owner thread 上。shutdown 时 `shutdownGracefully()` / `close()` 先置 `running = false`、唤醒所有容量等待者，再在 owner 上 `drainLeftoverCommands()`，用 `recycleAndRelease` 回收未执行任务。

## 统计和观测

executor 热路径用 `LongAdder` 和 connection context 记录观测值，`CommandExecutor.statsSnapshot()` 汇总为 `StatsSnapshot`：

- submit accepted/rejected：`submit_accepted_total`、`submit_rejected_not_running_total`、`submit_rejected_closing_total`、`submit_rejected_queue_full_total`、`submit_rejected_bytes_budget_total`、`submit_rejected_offer_failed_total`
- 执行结果：`commands_executed_total`、`commands_skipped_closing_total`、`close_after_reply_total`
- backlog：`queued_tasks`、`queued_bytes`
- 背压：`channels_autoread_disabled`、`backpressure_enter_total`、`backpressure_exit_total`
- drain budget：`drain_limited_max_commands_total`、`drain_limited_time_budget_total`
- reply 头部延后：`deferred_fair_reply_heads`、`deferred_global_reply_heads`

连接级统计来自 `ExecutionConnectionContext.statsSnapshot()`（`ConnectionStatsView`），在 `STATS` 里以 `conn_` 前缀输出：`conn_pending`、`conn_pending_bytes`、`conn_autoread_disabled_by_executor`、`conn_closing`、`conn_commands_enqueued`、`conn_commands_executed`、`conn_commands_rejected`、`conn_commands_skipped_closing`、`conn_close_after_reply`、`conn_backpressure_enter`、`conn_backpressure_exit`。这些数据供 `STATS` / `INFO yierdis` 等观测命令使用。

## Bounded Reply Egress

executor 队列背压和 reply egress 是不同层次的 admission。每个请求先拥有 receive-order reply slot，`ReplyCapacityUnavailableException` 只在尚未产生可见 bytes 或 mutation 结果时允许延后重试；超过单回复上限或结果已经未知时连接必须关闭，不能补写普通 internal error。`FAIR` 可以让独立可运行连接绕过另一连接的本地回复等待，`GLOBAL` 保持全局 FIFO 头部不被跨越。

容量层级、slot/source/chunk 的终结所有权和 shutdown drain 的运维语义见 [`production-hardening-operations.md`](./production-hardening-operations.md)。