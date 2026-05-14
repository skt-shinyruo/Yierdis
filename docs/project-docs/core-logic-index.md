# Core Logic Index

本文把当前代码库里的核心逻辑集中成一份索引。它不替代
[`request-execution-flow.md`](./request-execution-flow.md)、
[`main-path-walkthrough.md`](./main-path-walkthrough.md)、
[`db-internals.md`](./db-internals.md) 或
[`executor-and-backpressure.md`](./executor-and-backpressure.md)，而是回答一个更直接的问题：

- 如果我要快速找“核心逻辑在哪里”，先打开哪些文件？
- 每个核心类到底负责什么？
- 哪些边界不能随手打破？

## 一张总图

生产主链可以压成下面这条路径：

```text
CLI args / process
  -> YierdisServer
  -> YierdisServerBootstrap
  -> YierdisInstance
  -> CommandExecutor
  -> Netty pipeline
  -> RespRequestDecoder
  -> RespCommandAdapter
  -> YierdisFastCommandHandler
  -> owner executor thread
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandSpec<T>.parse(...)
  -> typed command handler
  -> CommandSupport
  -> DbEngine / DbReads / DbWrites
  -> Yierdis*Ops
  -> YierdisDbMutationExecutor
  -> YierdisDbKeyLifecycle
  -> EntryRecord / typed ValueHandle / TypeRoot
  -> ReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
```

这条链里最重要的设计约束是：

- Netty I/O 线程不直接访问 DB。
- command 层不直接依赖 `YierdisDb` 实现类。
- RESP DTO 不进入 command 层，必须先适配成 `ExecutionRequest`。
- DB 写路径统一走 mutation + memory ledger，不裸写内存结构。
- 回包语义统一走 `ReplyWriter`，RESP 只负责编码成线上 bytes。

## Server 启动与组装

### `YierdisServer`

源码：

- [`YierdisServer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java)

核心方法：

- `main(String[] args)`

职责：

- 调用 `ServerConfig.fromArgs(args)` 解析 CLI 参数。
- 调用 `ForeignMemoryAutoModules.ensureFfmAvailable()` 确认当前 JVM 支持 JDK 25 FFM。
- 调用 `YierdisServerBootstrap.start(config)` 进入真正组装。
- 捕获可预期 CLI 错误并用稳定退出码退出。

它不是业务逻辑中心，也不应该持有 Netty pipeline、DB、命令注册或执行器细节。

### `YierdisServerArgs` / `YierdisServerRuntimeConfig`

源码：

- [`YierdisServerArgs.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java)
- [`YierdisServerRuntimeConfig.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java)

核心方法：

- `normalizeAndValidate()`
- `toRuntimeConfig()`
- `toArgv()`

职责：

- 用 picocli 定义 server 启动参数。
- 校验端口、DB 数量、队列容量、协议上限、背压水位、maxmemory、清理预算等。
- 把 CLI 字符串归一化成稳定的 runtime record 和 enum。
- 给 benchmark / 脚本提供同一套 argv 输出模型。

这层只做参数和配置模型，不直接创建 DB、Netty 或 executor。

### `ServerConfig` / `CommandExecutorConfigs` / `ForeignMemoryAutoModules`

源码：

- [`ServerConfig.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java)
- [`CommandExecutorConfigs.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java)
- [`ForeignMemoryAutoModules.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ForeignMemoryAutoModules.java)

核心方法：

- `ServerConfig.fromArgs(...)`
- `CommandExecutorConfigs.from(...)`
- `ForeignMemoryAutoModules.ensureFfmAvailable()`

职责：

- `ServerConfig` 把 CLI 参数解析结果包成 server composition root 使用的配置对象。
- `CommandExecutorConfigs` 把 runtime config 中的 queue、bytes budget、背压水位、drain budget 和 scheduling policy 映射成 executor config。
- `ForeignMemoryAutoModules` 在进程启动早期检查 `java.lang.foreign.Arena`，失败时给出明确 JDK 25 要求。

这组类是启动前置校验和配置适配层，不保存运行时状态。

### `YierdisServerBootstrap`

源码：

- [`YierdisServerBootstrap.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java)

核心方法：

- `start(String... args)`
- `start(ServerConfig config)`
- `startInternal()`
- `close()`
- `dbRouter(YierdisInstance instance)`
- `closeRuntimeAccess(...)`

职责：

- 作为 server 的 composition root。
- 创建 `YierdisInstance`。
- 创建 `NettyServerInfoProvider` 和 `SlowCommandGovernor`。
- 创建 `DefaultYierdisEngine`，注入 `DefaultCommandModules` 和 `ServerCommandModule`。
- 创建 `CommandExecutor<NettyExecutionConnection>`。
- 启动 executor，并在 owner thread 上调用 `runtimeAccess::bindToCurrentThread`。
- 创建 Netty boss / worker group。
- 设置 maintenance tick：worker event loop 只负责定时，真正 cleanup 通过 `executor.executeMaintenance(...)` 回到 owner thread。
- 最后绑定端口。
- 关闭时按反向顺序 best-effort 释放 server channel、cleanup future、executor、engine、instance runtime、Netty group。

关键边界：

- bootstrap 可以接线，但不拥有命令语义。
- bootstrap 不应该绕过 `YierdisInstanceRuntimeAccess` 直接操作 runtime engine。
- DB 和 executor 必须先就绪，再 `bind(port)` 对外服务。

### `YierdisServerChannelInitializer`

源码：

- [`YierdisServerChannelInitializer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java)

核心方法：

- `initChannel(SocketChannel ch)`
- `WriteBufferBackpressureHandler.channelWritabilityChanged(...)`
- `CloseOnReadIdleHandler.userEventTriggered(...)`

职责：

- 在连接建立时创建 `NettyExecutionConnection`，先把连接级根对象挂到 channel 上。
- 按固定顺序装配 pipeline：

```text
writeBufferBackpressure
  -> optional idleTimeout / idleTimeoutCloser
  -> respRequestDecoder
  -> respCommandAdapter
  -> respProtocolErrorReply
  -> commandHandler
```

- 根据 output-buffer 参数设置 Netty `WriteBufferWaterMark`。
- channel 不可写时调用 `executor.onTransportUnwritable(...)` 关闭输入。
- channel 恢复可写时调用 `executor.onTransportWritable(...)` 让 executor 重新评估是否恢复 `autoRead`。
- 慢客户端持续不可写超过宽限时间后关闭连接。
- 读空闲超时后关闭连接。

这层只接 Netty pipeline 和连接级保护，不解析命令语义，也不访问 DB。

### `NettyExecutionConnection`

源码：

- [`NettyExecutionConnection.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java)

核心方法：

- `getOrCreate(Channel channel, int txMaxCommands, long txMaxBytes)`
- `markClosing()`
- `session()`
- `context()`

职责：

- 作为 server 侧连接根对象。
- 持有 Netty `Channel`。
- 持有 engine-owned `EngineSession`。
- 持有 executor-owned `ExecutionConnectionContext`。
- 在 `markClosing()` 时先标记 executor context closing，再丢弃 session 上的事务队列。

关键边界：

- `EngineSession` 管 DB index、事务、client name、认证状态、RESP version。
- `ExecutionConnectionContext` 管 pending、pending bytes、closing、backpressure、FAIR 调度队列状态。
- 两者不要混放。

## Runtime 和多 DB 装配

### `YierdisInstance`

源码：

- [`YierdisInstance.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java)

核心方法：

- `create(YierdisInstanceConfig config)`
- `engine(int dbIndex)`
- `engines()`
- `runtimeAccess()`
- `observability()`
- `bindToCurrentThread()`
- `close()`

职责：

- 创建实例级 `YierdisFfmMemoryRuntime`。
- 决定 `maxmemoryScope` 是 `GLOBAL` 还是 `PER_DB`。
- `PER_DB` 下把 `maxmemoryBytes` 按 DB 数量分摊。
- 默认 `GLOBAL` 下让多个 DB 共享一个 FFM runtime。
- 创建 `RuntimeDbEngine[]`。
- `GLOBAL` maxmemory 打开时创建 `YierdisGlobalMaxmemoryGovernor`，并 attach 到每个 DB。
- 对上层只暴露 `DbEngine` 能力视图。

关键边界：

- `YierdisInstance` 是实例级资源 owner，不是单个 DB。
- 多 DB 和 global/per-db maxmemory 组织在这里，不放进 command 层。

### `YierdisInstanceRuntimeAccess`

源码：

- [`YierdisInstanceRuntimeAccess.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java)

核心方法：

- `bindToCurrentThread()`
- `maintenanceTick()`
- `close()`

职责：

- owner-thread-only runtime seam。
- 把所有 runtime DB engine 绑定到当前线程。
- maintenance tick 中逐 DB 执行过期清理。
- `PER_DB` maxmemory 下逐 DB enforce。
- `GLOBAL` maxmemory 下调用 instance resources 的全局 governor enforcement。
- shutdown 时关闭所有 DB 和 runtime resources。

这层是 server 调 runtime 生命周期的正确入口。

### `YierdisInstanceMaintenance`

源码：

- [`YierdisInstanceMaintenance.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceMaintenance.java)

核心方法：

- `maintenanceTick()`

职责：

- 作为 runtime 层的 Netty-free maintenance wrapper。
- server bootstrap 只负责调度它，具体过期清理和 maxmemory enforcement 策略仍留在 runtime seam。
- embedded 用户可以复用同一入口，但调用前必须确保 instance 已绑定到当前 owner thread。

### `YierdisInstanceResources`

源码：

- [`YierdisInstanceResources.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceResources.java)

核心方法：

- `engine(int dbIndex)`
- `engineViews()`
- `bindToCurrentThread()`
- `enforceGlobalMaxmemoryMaintenance()`
- `shutdownAll()`
- `startupFailure(...)`

职责：

- 保存实例级 runtime DB 数组、共享 FFM runtime 和 global maxmemory governor。
- 对外只暴露 `DbEngine` view，内部保留 `RuntimeDbEngine` 能力。
- 负责 DB owner-thread 绑定和关闭顺序。
- startup 失败时负责清理已经创建的 DB / FFM runtime，并把清理异常作为 suppressed failure。

### `YierdisGlobalMaxmemoryGovernor`

源码：

- [`YierdisGlobalMaxmemoryGovernor.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernor.java)

核心方法：

- `prepareWrite(long estimatedExtraBytes)`
- `enforceMaintenance()`
- `nextLruClock()`
- `evictUntilUnder(...)`
- `pickVictim(...)`

职责：

- 通过 `MaxmemoryCoordinator` SPI 跨多个 DB 协调 maxmemory。
- 写入前先全局清理过期 key。
- 计算 participant usage 和 shared usage。
- `noeviction` 下拒绝会增长内存的写入。
- `allkeys-random` / `allkeys-lru` 下跨 DB 采样或扫描 victim。
- 给 global LRU 提供统一 clock。

关键点：

- global scope 下 shared FFM runtime 的 usage 只按实例口径计入，不按 DB 重复相加。
- governor 只依赖 `yierdis-db-api` 的 maxmemory SPI，不依赖具体 `YierdisDb` 类型。

相关 SPI：

- `MaxmemoryCoordinator`：写入前 admission、跨 DB cleanup/evict 和全局 LRU clock。
- `MaxmemoryParticipant`：单 DB usage、key count、victim sampling/scanning 和本地 evict hook。
- `MaxmemoryCoordinatorAware`：runtime attach/detach 全局 coordinator。
- `RuntimeDbEngine`：把 `DbEngine`、runtime lifecycle 和 maxmemory participant hook 合成 runtime 可见 contract。

## Executor、调度和背压

### `CommandExecutor`

源码：

- [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)

核心方法：

- `start()`
- `trySubmit(C connection, ExecutionRequest request)`
- `executeMaintenance(Runnable task)`
- `executeOwnerTask(Runnable task)`
- `onTransportUnwritable(C connection)`
- `onTransportWritable(C connection)`
- `shutdownGracefully()`
- `statsSnapshot()`

职责：

- 装配 executor 子组件：
  - `ExecutorBacklogBudget`
  - `ExecutorTaskQueue`
  - `ExecutorBackpressureController`
  - `CommandExecutorSubmitter`
  - `CommandExecutorDrainLoop`
  - `CommandExecutorExecutionSupport`
- 在 `start()` 中先把 DB 绑定到 owner thread，再允许 drain。
- 对 I/O 层暴露 `trySubmit`，成功返回 `null`，失败返回 reject reason。
- maintenance 也走 owner executor。
- 对 transport writability 变化做 backpressure 接入。

关键边界：

- executor 不解析命令。
- executor 不创建 `CommandContext`。
- executor 只负责排队、预算、背压、owner-thread 调度、回包写出和连接关闭。

### `CommandExecutorSubmitter`

源码：

- [`CommandExecutorSubmitter.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java)

核心方法：

- `trySubmit(...)`

职责：

- 检查 executor 是否运行。
- 根据全局 backlog、连接 pending、连接 pending bytes 触发输入关闭。
- 先 reserve 全局 queue slot。
- 再 reserve queued bytes。
- 再投递到 `ExecutorTaskQueue`。
- 任一阶段失败时回滚已占预算。
- 记录 accepted / rejected 统计。
- 调度 drain loop。

拒绝原因：

- `not_running`
- `queue_full`
- `bytes_budget`
- `offer_failed`

### `ExecutorBacklogBudget`

源码：

- [`ExecutorBacklogBudget.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudget.java)

核心方法：

- `tryReserveSlot()`
- `tryReserveQueuedBytes(int bytes)`
- `releaseSlot()`
- `releaseQueuedBytes(int bytes)`
- `isGlobalBackpressureHigh()`
- `isGlobalBackpressureCleared()`

职责：

- 维护全局 queued task 数。
- 维护全局 queued bytes。
- 提供硬上限和高/低水位滞回。
- 避免请求在 executor 前无限堆积。

### `ExecutorTaskQueue`

源码：

- [`ExecutorTaskQueue.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorTaskQueue.java)

核心方法：

- `offer(K key, T task)`
- `poll()`
- `drainLeftoverTasks(...)`

职责：

- 只做调度排队，不理解命令语义和预算。
- `GLOBAL` 模式使用单个 FIFO queue。
- `FAIR` 模式使用连接本地 queue + `activeKeys` round-robin。

`FAIR` 需要的每连接状态来自 `ExecutionConnectionContext.queueState()`，底层 contract 是：

- `ExecutorKeyState<T>`：per-key local queue + scheduled flag。
- `ExecutorKeyStateProvider<K, T>`：根据 key 取得或创建 state。

这让 FAIR 算法只依赖调度状态，不依赖 Netty、session 或 DB。

### `CommandExecutorDrainLoop`

源码：

- [`CommandExecutorDrainLoop.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java)

核心方法：

- `markStarted()`
- `scheduleDrain()`
- `drainLeftoverCommands()`
- `drainLoop()`

职责：

- 在 owner executor 上取任务。
- 每轮最多执行 `maxDrainCommands` 条。
- 每轮最多执行到 `drainTimeLimitNanos`。
- 记录是否被条数或时间预算限制。
- 一轮结束后统一 `flushPending(...)`，减少高频 flush。
- 关闭时回收剩余任务。

### `CommandExecutorExecutionSupport`

源码：

- [`CommandExecutorExecutionSupport.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java)

核心方法：

- `execute(CommandExecutorTask<C> task, Collection<C> touchedConnections)`
- `flushPending(...)`
- `recoverInputIfPossible(C connection)`
- `recycleAndRelease(...)`

职责：

- 跳过 closing 连接，避免已关闭连接继续产生副作用。
- 通过 `ReplyWriterFactory` 创建 writer。
- 调用 `CommandExecutionEngine.execute(session, request, writer)`。
- 如果 writer 请求 close-after-reply，则标记连接 closing。
- 通过 `ExecutionIoAdapter` 写出 buffered reply。
- 释放 request、slot 和 bytes 预算。
- 在连接水位、bytes 水位、全局 backlog 都恢复后重新启用输入。
- 执行异常时 best-effort 写 `ERR internal error` 并关闭连接。

### `ExecutorBackpressureController`

源码：

- [`ExecutorBackpressureController.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java)

核心方法：

- `disableAutoRead(K key)`
- `enableAutoReadIfWeDisabled(K key)`
- `scheduleGlobalRecovery()`

职责：

- 记录哪些连接是 executor 关闭的 `autoRead`。
- 通过 Netty-free 的 `ExecutorBackpressureIo` 执行 I/O 副作用。
- 连接恢复时必须同时满足：
  - channel active
  - channel writable
  - connection not closing
  - pending <= low watermark
  - pendingBytes <= bytes low watermark
  - global backlog cleared

### `ExecutionConnectionContext`

源码：

- [`ExecutionConnectionContext.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java)

核心方法：

- `recordCommandEnqueued(...)`
- `recordCommandFinished(...)`
- `markClosing()`
- `markInputDisabledByExecutor()`
- `clearAutoReadDisabledByExecutor()`
- `statsSnapshot()`

职责：

- 保存 executor-local 连接状态。
- 保存 FAIR 调度所需 queue state。
- 统计 pending、pending bytes、accepted/rejected/executed/skipped/backpressure。
- 不保存 DB index、事务或 RESP version。

## Engine、Session 和事务

### `DefaultYierdisEngine`

源码：

- [`DefaultYierdisEngine.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java)

核心方法：

- `execute(Session session, ExecutionRequest request, ReplyWriter out)`
- `maintenanceTick()`

职责：

- 要求传入 `ServerSession`。
- 创建 `CommandContext(serverSession, out)`。
- 委托 `YierdisFastCommandProcessor` 执行命令。
- 把 maintenance 调用转给 runtime seam。

这层固定了外部主链：executor schedules, engine executes。

### `EngineSession`

源码：

- [`EngineSession.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java)

核心方法：

- `dbIndex()` / `setDbIndex(...)`
- `clientId()`
- `clientName()` / `setClientName(...)`
- `authenticated()` / `setAuthenticated(...)`
- `respVersion()` / `setRespVersion(...)`
- `transaction()`
- `discardTransaction()`
- `bindConnectionStatsSupplier(...)`

职责：

- 保存 engine-owned 连接业务会话态。
- 维护当前逻辑 DB index。
- 维护事务队列。
- 维护 client id/name、认证标记和 RESP 版本。
- 只通过 supplier 暴露连接统计，不拥有 executor counters。

### `EngineSession.DefaultTransactionState`

源码：

- [`EngineSession.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java)

核心方法：

- `begin()`
- `tryEnqueue(ExecutionRequest request)`
- `markAborted()`
- `discard()`
- `drain()`

职责：

- 管连接级 `MULTI` 状态。
- 按命令数和 retained bytes 限制事务队列。
- 入队时用 `ByteArrayExecutionRequest.copyOf(request)` 保存独立快照。
- 出错或超限后标记 aborted。
- discard / begin / close 时关闭自己持有的 request 快照。

关键点：

- 事务队列里不是特殊 IR，而是 `ExecutionRequest` 快照。
- 原始请求在 executor finally 中释放，不会影响事务队列。

### `ByteArrayExecutionRequest`

源码：

- [`ByteArrayExecutionRequest.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java)

核心方法：

- `copyOf(List<byte[]> args)`
- `copyOf(ExecutionRequest request)`
- `wrapReadOnly(byte[][] argv, int retainedBytes)`
- `fromUtf8(String commandName, List<String> args)`
- `argc()`
- `isNull(int index)`
- `len(int index)`
- `byteAt(...)`
- `copyToByteArray(...)`
- `readOnlyByteArray(...)`
- `retainedBytes()`

职责：

- 作为 heap-backed execution request。
- 支持 RESP 适配后的 read-only argv。
- 支持事务快照。
- 给 executor queued bytes 预算提供 `retainedBytes()`。

## Command 注册、解析和分发

### `CommandSpec<T>`

源码：

- [`CommandSpec.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java)

核心方法：

- `of(...)`
- `disallowedInMulti(...)`
- `parse(ExecutionRequest request)`
- `executeParsed(Object parsed, CommandContext ctx)`

职责：

- 把一个命令注册项统一成 descriptor + parser + typed handler + MULTI policy。
- 让事务入队前 parse 和正常执行 parse 复用同一套逻辑。

### `ArgReader` / `CommandArity` / `CommandParsers` / `CommandParseError`

源码：

- [`ArgReader.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ArgReader.java)
- [`CommandArity.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArity.java)
- [`CommandParsers.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParsers.java)
- [`CommandParseError.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseError.java)
- [`CommandParseResult.java`](../../yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseResult.java)

职责：

- `ArgReader` 统一按 `ExecutionRequest` argv bytes 读取参数、比较 ASCII option 和解析整数。
- `CommandArity` 表达 exact/min/range/one-of/pair-tail 等参数形状。
- `CommandParsers` 把 arity rule 和 typed mapper 组合成 `CommandParser<T>`。
- `CommandParseError` 集中映射 wrong arity、syntax、integer out of range 和自定义错误文案。
- `CommandParseResult` 在 parser 和 processor 之间携带 parsed value 或 parse error。

关键边界：

- parser 负责参数形状和语法；handler 负责业务执行和回包。
- 普通执行和事务入队前校验必须复用同一套 parser。

### `CommandRegistry`

源码：

- [`CommandRegistry.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java)

核心方法：

- `register(String name, CommandSpec spec)`
- `spec(ExecutionRequest request)`
- `descriptorByUpperName(String nameUpper)`
- `upperNamesSorted()`

职责：

- 保存命令名到 `CommandSpec` 的映射。
- 注册时构建 open-addressed hash table。
- 运行时直接从 `ExecutionRequest` 的 argv[0] bytes 做 ASCII case-insensitive lookup，避免先转字符串。
- 检查重复注册、命令名非 ASCII、MULTI 错误文案等。

### `YierdisFastCommandProcessor`

源码：

- [`YierdisFastCommandProcessor.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)

核心方法：

- 构造函数
- `execute(ExecutionRequest request, CommandContext ctx)`
- `validateBeforeQueue(...)`
- `executeSpec(...)`

职责：

- 构造时先注册 `TransactionCommands`，再注册注入的模块。
- 检查空命令和 argv[0]。
- 拒绝不允许的 null bulk string。
- 在事务 active 时：
  - `MULTI/EXEC/DISCARD` 立即执行；
  - 普通命令先 lookup spec；
  - 检查是否允许进入 MULTI；
  - 入队前先 parse；
  - 入队成功返回 `QUEUED`。
- 非事务入队路径：
  - lookup spec；
  - parse；
  - typed handler 执行；
  - 捕获 wrong type、command exception 和参数错误；
  - 根据 `CommandContext` 的 mutation outcome gate 触发 change event。

这层是命令执行总闸门。

### `TransactionCommands`

源码：

- [`TransactionCommands.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java)

核心方法：

- `multi(...)`
- `discard(...)`
- `exec(...)`

职责：

- 实现最小 `MULTI/EXEC/DISCARD`。
- `MULTI` 开启连接事务。
- `DISCARD` 清空事务。
- `EXEC` drain 队列，并逐条 replay 到同一个 `YierdisFastCommandProcessor`。
- replay 复用同一个 parser、handler、DB routing、错误映射和 mutation tracking。

### `DefaultCommandModules`

源码：

- [`DefaultCommandModules.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java)

核心方法：

- `create(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor)`

职责：

- 注册 transport-neutral 默认命令模块：
  - `CoreConnectionCommands`
  - `KeyCommands`
  - `StringCommands`
  - `HllCommands`
  - `ListCommands`
  - `HashCommands`
  - `SetCommands`
  - `ZSetCommands`

server-only 的 `HELLO/INFO/STATS` 不在这里，而是在 `ServerCommandModule`。

### `CommandSupport`

源码：

- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)

核心方法：

- `db(CommandContext ctx)`
- `dbReads(CommandContext ctx)`
- `dbWrites(CommandContext ctx)`
- `recordMutation(...)`
- `argView(...)`
- `argSlice(...)`
- `parseLong(...)`
- `parseScoreBound(...)`

职责：

- 按 `ServerSession.dbIndex()` 通过 `YierdisDbRouter` 选 DB。
- 把命令层限制在 `DbReads` / `DbWrites` 能力边界。
- 提供 request-scoped bytes view / slice，减少中间复制。
- 提供常用 Redis 参数解析工具。
- 把 DB 返回的 `MutationOutcome` 记录到 `CommandContext`。

关键边界：

- 命令实现一般不 import `YierdisDb`。
- 如果命令需要新 DB 能力，先扩 `yierdis-db-api`，再实现 `yierdis-db-memory`。

### `CoreConnectionCommands`

源码：

- [`CoreConnectionCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java)

核心方法：

- `register(...)`
- `ping(...)`
- `echo(...)`
- `command(...)`
- `select(...)`
- `client(...)`
- `auth(...)`
- `flushdb(...)`

职责：

- 实现 transport-neutral 的连接和 DB 生命周期命令。
- `PING/ECHO` 只处理基础回包。
- `COMMAND` 从当前 `CommandRegistry` 导出命令元数据。
- `SELECT` 修改 `ServerSession.dbIndex()`，不直接切换 DB 对象。
- `CLIENT SETINFO/SETNAME/GETNAME` 维护连接级兼容状态。
- `AUTH` 在未配置密码时固定返回 Redis 风格错误。
- `FLUSHDB` 调 `DbLifecycleOps.flushDb()` 并记录 mutation outcome。

这层不依赖 RESP 模型或 Netty，只依赖 command API、session contract 和 DB capability。

### `StringCommands`

源码：

- [`StringCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)

核心方法：

- `register(...)`
- `parseSet(...)`
- `set(...)`
- `get(...)`
- `append(...)`
- `setbit(...)`
- `getbit(...)`
- `bitcount(...)`
- `incrBy(...)`

职责：

- 解析 `SET` 的 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`。
- 把命令参数转成 `SetMode` / `ExpireOption` 等稳定类型。
- 通过 `support.dbReads(ctx)` 或 `support.dbWrites(ctx)` 调 DB 能力。
- 写命令结束后记录 mutation outcome。
- 用 `ReplyWriter` 写 Redis 风格回复。

它负责读懂用户命令，不负责直接修改 DB 内部结构。

### `KeyCommands`

源码：

- [`KeyCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java)

核心方法：

- `register(...)`
- `type(...)`
- `memory(...)`
- `object(...)`
- `keys(...)`
- `parseScan(...)`
- `scan(...)`
- `del(...)`
- `exists(...)`
- `expire(...)` / `pexpire(...)` / `expireat(...)` / `pexpireat(...)`
- `persist(...)`
- `ttl(...)` / `pttl(...)`

职责：

- 注册 keyspace、TTL 和内存观测相关命令。
- `MEMORY STATS` 优先通过 `ServerInfoProvider` 聚合全局视图，缺省回落到当前 DB。
- `KEYS` 通过 `SlowCommandGovernor` 限制最大结果和时间预算。
- `SCAN` 解析 `MATCH/COUNT`，以 `[cursor, keys]` 格式回包。
- 写命令通过 `DbWrites.keyspace()` 或 `DbWrites.ttl()` 落到 DB，并记录 mutation outcome。

这层负责 Redis 命令表面语义，不直接遍历 `NativeKeyDirectory`。

### `ListCommands` / `HashCommands` / `SetCommands`

源码：

- [`ListCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java)
- [`HashCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java)
- [`SetCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java)

核心方法：

- `ListCommands.push(...)` / `pop(...)` / `lrange(...)`
- `HashCommands.hset(...)` / `hget(...)` / `hgetall(...)` / `hlen(...)` / `hdel(...)`
- `SetCommands.sadd(...)` / `srem(...)` / `smembers(...)` / `sismember(...)` / `scard(...)`

职责：

- 把 list/hash/set 命令参数切成 request-scoped slice，减少中间复制。
- 读路径使用 `BulkStringSequence` / `BulkStringMapPairs` 流式写回。
- 写路径只调用 `DbWrites.lists()/hashes()/sets()`，并记录 mutation outcome。
- `LPOP/RPOP` 按是否带 `count` 区分 null bulk、null array 和 bulk string array 语义。
- `HGETALL` 用 map header 表达键值对，RESP2 由 reply writer 降级成 flat array。

### `ZSetCommands`

源码：

- [`ZSetCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java)

核心方法：

- `zadd(...)`
- `parseZRange(...)`
- `zrange(...)`
- `zrevrange(...)`
- `parseZRangeByScore(...)`
- `zrangebyscore(...)`
- `zrevrangebyscore(...)`
- `zremrangebyscore(...)`
- `zremrangebyrank(...)`
- `zrem(...)`

职责：

- 解析 sorted-set 命令的 score、rank、`WITHSCORES`、`REV` 和 `LIMIT`。
- `parseZRangeByScore(...)` 在 reverse 命令中交换 min/max 语义，让 DB API 仍接收稳定的 min/max。
- 所有 score range 通过 `CommandSupport.ScoreBound` 表达开闭区间。
- 写路径调用 `DbWrites.zsets()`，读路径返回 `BulkStringSequence`。

### `HllCommands`

源码：

- [`HllCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java)

核心方法：

- `pfadd(...)`
- `pfcount(...)`
- `pfmerge(...)`

职责：

- 注册最小 HyperLogLog 命令子集。
- HLL 在 DB 内部复用 string payload，但 command 层只依赖 `DbReads.hll()` / `DbWrites.hll()`。
- `PFCOUNT` 支持多个 key 的合并估算。
- `PFMERGE` 把 sources 合并到 dest 后返回 `OK`。

### `ServerCommandModule`

源码：

- [`ServerCommandModule.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java)

核心方法：

- `register(...)`
- `hello(...)`
- `info(...)`
- `stats(...)`

职责：

- 注册 server-facing 命令：
  - `HELLO`
  - `INFO`
  - `STATS`
- `HELLO` 协商当前连接 RESP version，并支持 `SETNAME`。
- `HELLO` 在 MULTI 中禁止。
- `INFO/STATS` 委托 `NettyServerInfoProvider`。

这些命令依赖 server runtime / observability，所以不放进 `yierdis-command-builtin`。

### `NettyServerInfoProvider` / `YierdisInstanceObservability`

源码：

- [`NettyServerInfoProvider.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java)
- [`YierdisInstanceObservability.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java)

核心方法：

- `NettyServerInfoProvider.info(...)`
- `NettyServerInfoProvider.stats(...)`
- `NettyServerInfoProvider.memoryStats(...)`
- `YierdisInstanceObservability.memoryStats()`
- `YierdisInstanceObservability.dbSummaries()`

职责：

- `INFO` 默认返回 Redis 风格文本段，`INFO yierdis` 返回结构化 map。
- `STATS` 输出 executor 全局计数和当前连接计数。
- `memoryStats(...)` 在 global maxmemory scope 下返回实例级聚合，避免共享 off-heap usage 被每个 DB 重复计入。
- runtime observability 汇总所有 DB 的 heap estimate、off-heap、reserved bytes、key count、expire count 和 rehash 状态。

## DB API 和 DB 内核

### `DbEngine` / `DbReads` / `DbWrites`

源码：

- [`DbEngine.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngine.java)
- [`DbReads.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbReads.java)
- [`DbWrites.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbWrites.java)

职责：

- 定义 command-facing storage 能力边界。
- 命令层依赖这些接口，而不是依赖 `YierdisDb`。
- 多 DB routing 返回的是 `DbEngine` 视图。

### `BulkStringValue` / `BulkStringSequence` / `BulkStringMapPairs` / `BulkStringSink`

源码：

- [`BulkStringValue.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringValue.java)
- [`BulkStringSequence.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSequence.java)
- [`BulkStringMapPairs.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapPairs.java)
- [`BulkStringSink.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSink.java)
- [`BulkStringReplyAdapter.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/BulkStringReplyAdapter.java)

职责：

- 在 DB API 层表达可流式消费的 bulk string、bulk string 序列和 field/value pair 序列。
- 让 `GET`、`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE` 等读路径不用先构造完整 heap collection。
- `BulkStringReplyAdapter` 把 storage result sink 接到 command-facing `ReplyWriter`。
- 保留 `BytesSlice` / off-heap slice 直接写 reply sink 的 fast path。

关键边界：

- DB/value/off-heap 层不依赖 RESP 或 `ReplyWriter`。
- 命令层负责先写 array/map header，再调用 result emit。

### `YierdisDbReads` / `YierdisDbWrites`

源码：

- [`YierdisDbReads.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbReads.java)
- [`YierdisDbWrites.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbWrites.java)

核心方法：

- `strings()`
- `hashes()`
- `lists()`
- `sets()`
- `zsets()`
- `hll()`
- `keyspace()`
- `ttl()`

职责：

- 把 DB 内部 `Yierdis*Ops` 组合成 command-facing facade。
- 按 read/write 能力拆分接口，命令实现可以只拿需要的视图。
- 让 `YierdisDb` 对上层暴露稳定能力，而不是暴露内部 collaborator。

### `YierdisDbEngineFactory` / `YierdisDbConfig` / `DbThreadGuard`

源码：

- [`YierdisDbEngineFactory.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java)
- [`YierdisDbConfig.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbConfig.java)
- [`DbThreadGuard.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/DbThreadGuard.java)

核心方法：

- `YierdisDbEngineFactory.create(...)`
- `YierdisDbConfig.create(...)`
- `DbThreadGuard.bindToCurrentThread()`
- `DbThreadGuard.checkThread()`
- `DbThreadGuard.checkThreadForShutdown()`

职责：

- `YierdisDbEngineFactory` 是 runtime 创建 DB 的默认 factory，可创建独占 FFM runtime 的 DB，也可使用实例共享 FFM runtime。
- `YierdisDbConfig` 归一化 maxmemory policy、samples、eviction budget 和 expire cleanup budget。
- `DbThreadGuard` 强制 DB 先绑定 owner thread，再允许访问；跨线程访问和关闭会 fail fast。

### `YierdisDb`

源码：

- [`YierdisDb.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java)

核心方法：

- 构造函数和 factory methods
- `reads()`
- `writes()`
- `expiration()`
- `memory()`
- `lifecycle()`
- `bindToCurrentThread()`
- `checkThread()`
- `cleanupExpired()`
- `enforceMaxmemoryMaintenance()`
- `attachMaxmemoryCoordinator(...)`
- `sampleCandidate(...)`
- `scanBestCandidate(...)`
- `evict(...)`
- `flushDb()`
- `shutdown()`

职责：

- 作为单 DB 状态 owner。
- 持有 storage roots、key directory、entry table、expire index、ledger、lifecycle、ops、facade。
- 通过 `YierdisDbComponentFactory` 收敛对象图创建。
- 对 command 层暴露能力 facade。
- 对 runtime / maxmemory SPI 暴露 maintenance 和 eviction hook。
- 用 `DbThreadGuard` 强制 owner-thread 访问。

关键边界：

- `YierdisDb` 不是一个大 Map。
- 具体读写逻辑应优先落到 `Yierdis*Ops` 和内部 collaborator。

### `YierdisDbStorageComponents`

源码：

- [`YierdisDbStorageComponents.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java)

核心方法：

- `create(...)`

职责：

- 解析或创建 FFM runtime 和 off-heap allocator。
- 创建：
  - `EntryTable`
  - `YierdisFfmBlobStore`
  - `NativeKeyDirectory`
  - `YierdisFfmExpireIndex`
  - `StringRoot`
  - `ListRoot`
  - `HashRoot`
  - `SetRoot`
  - `ZSetRoot`
- 记录资源 ownership。

默认路径固定使用 JDK 25 FFM，不再有多 backend 切换。

### `YierdisDbComponentFactory`

源码：

- [`YierdisDbComponentFactory.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java)

核心方法：

- `create(...)`

职责：

- 创建 `YierdisDbConfig`。
- 创建 `YierdisDbMemoryEstimator`。
- 创建 `YierdisDbMemoryLedger`。
- 创建 `YierdisDbMutationExecutor`。
- 创建 `YierdisDbExpirationSupport`。
- 创建 `YierdisDbMaxmemorySupport`。
- 创建 `YierdisDbKeyLifecycle`。
- 创建所有类型化 ops。
- 创建 reads / writes / expiration / memory / lifecycle facade。

这层防止 `YierdisDb` 构造函数继续膨胀。

### `YierdisDbRuntimeInternals`

源码：

- [`YierdisDbRuntimeInternals.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeInternals.java)

核心方法：

- `checkThread()`
- `executeMutation(...)`
- `keyLifecycle()`
- `ledger()`

职责：

- 给 `Yierdis*Ops` 提供最小内部入口。
- 让 ops 不直接拿完整 `YierdisDb`。

### `YierdisDbMutationExecutor`

源码：

- [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

核心方法：

- `execute(MutationPlan<T> plan)`
- `MutationPlan.upperBoundBytes()`
- `MutationPlan.apply()`
- `MutationResult.of(...)`

职责：

- 检查 owner thread。
- 先 `ledger.reserve(upperBoundBytes)`。
- 执行真正 mutation。
- 用实际 delta `ledger.commit(...)`。
- 如果 ledger OOM、off-heap OOM 或 runtime exception，则 rollback reservation。
- 把 ledger OOM 映射成 Redis 风格 OOM 错误。

这是 DB 写路径的统一模板。

### `YierdisDbMemoryLedger`

源码：

- [`YierdisDbMemoryLedger.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMemoryLedger.java)

核心方法：

- `reserve(long estimatedExtraBytes)`
- `commit(MemoryReservation reservation, long actualDeltaBytes)`
- `rollback(MemoryReservation reservation)`
- `resetUsage()`
- `usedBytes()`
- `reservedBytes()`

职责：

- 维护 DB 内部 ledger used bytes。
- 维护 mutation reservation bytes。
- 本地 scope 下写入前先 cleanup expired。
- 根据 policy 做 noeviction 判断或 eviction。
- global scope 下委托 `MaxmemoryCoordinator.prepareWrite(...)`。
- commit 阶段释放 reservation 并写入实际 delta。
- rollback 阶段只撤销 reservation。

关键点：

- maxmemory 不能等对象写完再补救，所以写入前必须先 reserve。
- `reservedBytes` 用于保护“估算通过但 mutation 未完成”的窗口。

### `YierdisDbKeyLifecycle`

源码：

- [`YierdisDbKeyLifecycle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)

核心方法：

- `keyHandle(...)`
- `entryRecord(...)`
- `liveEntryRecord(...)`
- `computeWithHandle(...)`
- `computeIfPresentWithHandle(...)`
- `removeIfExpired(...)`
- `setExpireAtMillis(...)`
- `removeExpire(...)`
- `newRecord(...)`
- `touchRecord(...)`
- `releaseValue(...)`
- `removeEntry(...)`
- `forEachKeyHandle(...)`
- `scan(...)`

职责：

- 连接 `NativeKeyDirectory` 和 `EntryTable`。
- 把 key bytes / `BytesView` 转成 key handle。
- 读取 live record 时做惰性过期删除。
- 维护 expire index 和 `EntryRecord.expireAtMillis` 一致性。
- 替换 entry 时释放旧 value handle。
- 删除 key 时释放 `StringRoot/ListRoot/HashRoot/SetRoot/ZSetRoot` 中的 payload。
- 更新 LRU clock。
- 给 TTL cleanup 和 maxmemory eviction 提供 key handle 遍历、随机采样和删除能力。

这是 DB 内核最关键的生命周期协作者。

### `YierdisDbExpirationSupport`

源码：

- [`YierdisDbExpirationSupport.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java)

核心方法：

- `cleanupExpired()`
- `cleanupExpired(long nowMillis)`

职责：

- 批量清理过期 key。
- 每轮随机采样 expire key。
- 如果过期比例低、达到最大循环数或达到时间预算，就停止。
- 删除过期 record 后更新 ledger used bytes。

它负责批量策略；单 key 生命周期动作仍在 `YierdisDbKeyLifecycle`。

### `YierdisDbMaxmemorySupport`

源码：

- [`YierdisDbMaxmemorySupport.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java)

核心方法：

- `evictUntilUnder(long limitBytes)`
- `sampleCandidate(MaxmemoryPolicy policy, long nowMillis)`
- `scanBestCandidate(...)`
- `evict(MaxmemoryCandidate candidate, long nowMillis)`
- `pickEvictionKey(...)`

职责：

- 单 DB 内执行 maxmemory eviction。
- 支持 `allkeys-random` 和 `allkeys-lru`。
- LRU 下按 samples 采样，samples 覆盖全部 key 时可扫描最佳候选。
- 删除 victim 前先处理过期 key。
- 删除成功后更新 ledger used bytes。

### `YierdisStringOps`

源码：

- [`YierdisStringOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)

核心方法：

- `set(...)`
- `append(...)`
- `setBit(...)`
- `incrBy(...)`
- `getStringValue(...)`
- `strlen(...)`
- `getBit(...)`
- `bitcount(...)`
- `storeSetValue(...)`
- `stringRecord(...)`
- `liveTouchedStringRecord(...)`

职责：

- 实现 string / bitmap 读写能力。
- 写操作先估算 upper bound，再走 `internals.executeMutation(...)`。
- `SET` 内处理 `NX/XX/GET/ExpireOption/KEEPTTL`。
- 写入时按内容选择 `STRING_INT` / `STRING_EMBSTR` / `STRING_RAW`。
- 读路径通过 `liveTouchedStringRecord(...)` 做惰性过期和 LRU touch。
- 通过 `StringRoot` 管理真实 string payload。

### `YierdisKeyspaceOps`

源码：

- [`YierdisKeyspaceOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisKeyspaceOps.java)

核心方法：

- `del(...)`
- `typeOf(...)`
- `existsKey(...)`
- `keys(...)`
- `scan(...)`

职责：

- 实现 `DEL/EXISTS/TYPE/KEYS/SCAN` 对应的 DB 能力。
- 写路径走 mutation executor，删除时通过 key lifecycle 统一释放 value payload、entry、key 和 expire index。
- `KEYS` 使用 `YierdisGlobMatcher` 按 byte glob 匹配，并接受最大结果数和时间预算。
- `SCAN` 使用 `ScanCursorV2`，按 cursor 在 key lifecycle 上增量遍历，并把 matched key 复制到输出列表。

### `YierdisListOps` / `YierdisHashOps` / `YierdisSetOps`

源码：

- [`YierdisListOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java)
- [`YierdisHashOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java)
- [`YierdisSetOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java)

核心方法：

- `YierdisListOps.lpush(...)` / `rpush(...)` / `lrange(...)` / `lpop(...)` / `rpop(...)`
- `YierdisHashOps.hset(...)` / `hget(...)` / `hgetall(...)` / `hlen(...)` / `hdel(...)`
- `YierdisSetOps.sadd(...)` / `srem(...)` / `smembers(...)` / `sismember(...)` / `scard(...)`

职责：

- 写操作先估算 key + entry + collection payload 的 upper bound，再走 `internals.executeMutation(...)`。
- 新 key 通过 `YierdisDbKeyLifecycle.newRecord(...)` 建立 `EntryRecord`。
- 已存在 key 会校验类型，不匹配时抛 `WrongTypeException`。
- 读路径返回 `BulkStringSequence` / `BulkStringMapPairs`，由 reply writer 逐项流式写出。
- 空集合写后会删除整个 key，避免留下空 value handle。

### `YierdisZSetOps`

源码：

- [`YierdisZSetOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java)

核心方法：

- `zadd(...)`
- `zrange(...)`
- `zrevrange(...)`
- `zrangeByScore(...)`
- `zrevrangeByScore(...)`
- `zrem(...)`
- `zremrangeByRank(...)`
- `zremrangeByScore(...)`

职责：

- 处理 sorted-set 的写入、rank range、score range 和批量删除。
- 写路径通过 `ZSetRoot` 修改 `ZSetValue`，并以 changed flag 判断 mutation outcome。
- 读路径同样以 `BulkStringSequence` 延迟计算 count 和 emit，避免命令层复制整份结果。
- 删除后如果 zset 为空，走 key lifecycle 删除 key。

### `YierdisHllOps`

源码：

- [`YierdisHllOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java)

核心方法：

- `pfadd(...)`
- `pfcount(...)`
- `pfmerge(...)`
- `requireHllHandle(...)`

职责：

- 在 DB 层实现 HLL 命令，但底层 payload 复用 `StringRoot`。
- HLL value 必须是合法 HLL string；普通 string 不能被当成 HLL 使用。
- `PFADD` 根据 sparse/dense 编码判断是否真正改变寄存器。
- `PFCOUNT/PFMERGE` 会把多个 HLL 合并到寄存器后估算或写回。

### `YierdisTtlOps`

源码：

- [`YierdisTtlOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisTtlOps.java)

核心方法：

- `expire(...)`
- `pexpire(...)`
- `expireAtSeconds(...)`
- `expireAtMillis(...)`
- `persist(...)`
- `ttlSeconds(...)`
- `ttlMillis(...)`
- `deleteImmediately(...)`

职责：

- 实现 TTL 写入、绝对时间过期、移除 TTL 和 TTL 查询。
- 过期时间小于等于当前时间时直接删除 key，并返回 value changed。
- 首次增加 TTL 时为 expire index entry 预留估算 bytes。
- TTL 查询遵循 Redis 风格返回值：不存在或已过期返回 `-2`，存在但无 TTL 返回 `-1`。
- 所有 TTL 变更仍走 mutation executor，保证 ledger 和 expire index 一致。

### `YierdisDbLifecycleOps` / `YierdisDbMemoryOps` / `YierdisDbIntrospection`

源码：

- [`YierdisDbLifecycleOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbLifecycleOps.java)
- [`YierdisDbMemoryOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryOps.java)
- [`YierdisDbIntrospection.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java)

核心方法：

- `YierdisDbLifecycleOps.flushDb()`
- `YierdisDbMemoryOps.memoryUsage(...)`
- `YierdisDbMemoryOps.memoryStats()`
- `YierdisDbMemoryOps.objectEncoding(...)`
- `YierdisDbIntrospection.snapshot(...)`

职责：

- lifecycle facade 提供 `FLUSHDB` 所需的清库能力。
- memory facade 提供 `MEMORY USAGE`、`MEMORY STATS` 和 `OBJECT ENCODING` 所需视图。
- introspection 按 `ScanCursorV2` 生成 `YierdisSnapshotEntry`，用于测试、调试和嵌入式 runtime 观测。

这些 `*Ops` 是命令最终落到 DB 内核的主要位置。

### `YierdisDbMemoryReporter`

源码：

- [`YierdisDbMemoryReporter.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java)
- [`DbMemoryAccounting.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java)
- [`YierdisMemoryStats.java`](../../yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/YierdisMemoryStats.java)

核心方法：

- `memoryUsage(...)`
- `memoryStats()`
- `usedBytesForMaxmemory()`
- `estimatedUsedBytes()`
- `keyCountEstimate()`
- `DbMemoryAccounting.snapshot(...)`

职责：

- 提供 `MEMORY USAGE` / `MEMORY STATS` 所需视图。
- 汇总 ledger、off-heap allocator、entry table、key directory、expire index、各 root native bytes。
- global scope 下配合 observability 避免共享 off-heap usage 被重复计入。
- `YierdisMemoryStats` 固化观测字段：maxmemory usage、reservation、off-heap usage、key/expire count、rehash 状态和 overall estimate。
- `DbMemoryAccounting` 负责把这些内部来源压成 explainable estimate，而不是精确 JVM heap measurement。

### `NativeKeyDirectory` / `YierdisKeyspace`

源码：

- [`NativeKeyDirectory.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java)
- [`YierdisKeyspace.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/YierdisKeyspace.java)
- [`ByteArrayKeyspace.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/ByteArrayKeyspace.java)
- [`ByteArrayHashMap.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/ByteArrayHashMap.java)
- [`ByteArrayHashSet.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/ByteArrayHashSet.java)
- [`YierdisFfmKeyspace.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmKeyspace.java)

核心方法：

- `put(...)`
- `get(...)`
- `remove(...)`
- `forEach(...)`
- `scan(...)`
- `randomKeyHandle()`
- `estimatedTableOverheadBytes()`
- `isRehashing()`

职责：

- `NativeKeyDirectory` 是当前 DB 的 key -> entry handle 目录。
- key bytes 存在 `YierdisFfmBlobStore`，directory slot 保存 key ref、entry handle、hash 和 state。
- 支持 tombstone、rehash、随机采样和 scan cursor。
- `YierdisKeyspace` 是早期/测试用 keyspace contract，heap/off-heap 实现保留为内部结构和回归测试对象。
- `ByteArrayHashMap` / `ByteArrayHashSet` 是内部 byte-key 容器，仍被 hash/set/zset 的 heap fallback 或辅助结构使用。

关键边界：

- key directory 只知道 key 到 entry handle 的映射，不知道 value 类型语义。
- value 释放和 expire index 维护必须由 `YierdisDbKeyLifecycle` 协调。
- 生产顶层 keyspace 入口优先看 `NativeKeyDirectory`，不要把 `ByteArrayHashMap` 当作 DB 主索引。

### `KeyHandleAccess`

源码：

- [`KeyHandleAccess.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandleAccess.java)

职责：

- 在 DB internal package 内桥接 heap/FFM `KeyHandle` 的实现细节。
- 允许 keyspace、expire index、scan 等内部组件在确认 handle 类型后访问 heap bytes 或 `YierdisFfmBytesRef`。
- 避免把 off-heap ref/address 直接暴露成 API 级 `KeyHandle` 契约。

### `EntryTable` / `EntryRecord`

源码：

- [`EntryTable.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java)
- [`EntryRecord.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java)
- [`EntryHandle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java)
- [`ValueHandle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java)

核心方法：

- `allocate(EntryRecord record)`
- `get(EntryHandle handle)`
- `replace(EntryHandle handle, EntryRecord record)`
- `release(EntryHandle handle)`
- `clear()`

职责：

- `EntryTable` 是 key directory 指向的 entry 存储表。
- `EntryRecord` 保存 `ValueType`、`ValueEncoding`、`ValueHandle`、`expireAtMillis`、LRU clock 等 metadata。
- `EntryHandle` 包装 `ENTRY_RECORD` 类型的 `NativeHandle`，不是 physical address。
- `ValueHandle` 包装 string/list/hash/set/zset 等 value/root 相关 `NativeHandle` raw value；当前多数 value handle 是 type-root-owned identity，不是 object table slot。
- entry metadata 使用 `YierdisStableNativeAllocator` 保存为 56 bytes native `ENTRY_RECORD`，避免所有 key metadata 都落在 Java object 上。
- `EntryTable` 读写 entry 时通过 allocator resolve 得到短生命周期 `NativeObjectView`，关闭 view 后释放 pin。

### `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot`

源码：

- [`StringRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java)
- [`ListRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java)
- [`HashRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java)
- [`SetRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java)
- [`ZSetRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java)

职责：

- 每个 type root 管理一种 logical type 的 payload map：`ValueHandle -> actual value`。
- root 提供类型内操作、encoding 查询、estimated bytes、native bytes、release 和 clear。
- `StringRoot` 管 off-heap string buffer，并支持 append、ensureLength、byteAt、setByteAt、slice/copy。
- collection roots 包装 `ListValue/HashValue/SetValue/ZSetValue`，并给 ops 提供同步的类型化方法。
- type roots 会给 `ValueHandle` 写入对应 `NativeObjectKind`，但当前集合 payload adapter 的控制结构仍主要由 root 自己管理，不等于整个集合对象已经完全交给 stable allocator，也不能把任意 `ValueHandle` 直接交给 allocator resolve。

关键边界：

- `EntryRecord` 只保存 handle 和 metadata，不保存 Java collection。
- 删除 key 时必须先由 key lifecycle 根据 `ValueType` 找到正确 root 释放 payload。
- handle 是跨层 stable identity；allocator-private page id、offset、span 和 raw address 不能传到 DB hot path 外。

### `NativeHandle` / stable native allocator

源码：

- [`NativeHandle.java`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java)
- [`NativeAllocator.java`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java)
- [`NativeAllocatorStats.java`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java)
- [`NativeObjectView.java`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectView.java)
- [`YierdisStableNativeAllocator.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java)
- [`YierdisNativeObjectTable.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java)
- [`YierdisNativePageAllocator.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java)
- [`YierdisNativeEpochManager.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeEpochManager.java)

核心方法：

- `NativeAllocator.allocate(kind, size)`
- `NativeAllocator.resolve(handle, mode)`
- `NativeAllocator.realloc(handle, newSize, policy)`
- `NativeAllocator.free(handle)`
- `NativeAllocator.beginEpoch(kind)`
- `NativeAllocator.defragOne(handle, maxMoveBytes)`
- `NativeAllocator.defragCycle(options)`
- `NativeAllocator.stats()`

职责：

- 提供 64-bit stable `NativeHandle` ABI：domain、kind、slot id、generation、flags。
- 用 object table 保存 handle -> metadata -> current physical block 的 indirection。
- 用 64 KiB page allocator 管理 small size-class、medium span 和 large span。
- 在 resolve view 时 pin 对象，view close 时 unpin。
- 用 epoch/quarantine 保护 freed 或 moved block，避免读者仍可见时释放。
- 用 generation、domain/kind 校验和 slot lifecycle 防 stale handle、double-free、wrong-kind 和 ABA 风险。
- 支持 `realloc` 的 in-place resize、move 扩容和失败回滚。
- 支持 active defrag，移动 unpinned object 时只发布 object table 新 location，DB graph 不需要重写 handle。
- 暴露 fragmentation、page counts、object kind counts、quarantine bytes、stale/double-free、realloc、defrag 和 allocation latency 指标。

完整语义见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

### `ValueEncoding` 和 value 实现

源码：

- [`ValueEncoding.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ValueEncoding.java)
- [`ListValue.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java)
- [`HashValue.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java)
- [`SetValue.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java)
- [`ZSetValue.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java)
- [`YierdisHyperLogLog.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java)
- [`YierdisListpack.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisListpack.java)
- [`ZSkipList.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSkipList.java)

职责：

- `ValueEncoding` 是 Redis-style 内部编码枚举：string int/embstr/raw、hash packed/ht、list packed/quicklist、set intset/ht、zset packed/skiplist。
- `ListValue` 小数据先用 packed listpack，超过阈值后转 quicklist。
- `HashValue` 小 hash 用 packed field/value list，超过阈值或字段过大后转 hash table / FFM map。
- `SetValue` 整数集合先用 intset，遇到非整数或超过阈值后转 hash set / FFM map。
- `ZSetValue` 小 zset 用 packed score/member list，超过阈值后转 skiplist + member map。
- `YierdisHyperLogLog` 复用 string bytes，维护 sparse/dense HLL 编码、MurmurHash、寄存器 merge 和 cardinality estimate。

### DB-local FFM 容器

源码：

- [`YierdisFfmBlobStore.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmBlobStore.java)
- [`YierdisFfmByteMap.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmByteMap.java)
- [`YierdisFfmListpack.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmListpack.java)
- [`YierdisFfmExpireIndex.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java)
- [`YierdisFfmIntSet.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmIntSet.java)
- [`YierdisFfmZSet.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmZSet.java)

职责：

- `YierdisFfmBlobStore` 是 DB 内部通用 bytes store，提供 ref-counted `YierdisFfmBytesRef`。
- `YierdisFfmByteMap` 是 off-heap key bytes + heap value 的开放寻址 map，供 hash/set 等结构切换到 native bytes 存储。
- `YierdisFfmListpack` 用 blob store 保存 listpack entry bytes。
- `YierdisFfmExpireIndex` 维护 key handle 到 expireAtMillis 的 off-heap 过期索引，支持渐进 rehash、随机 key 和 table 统计。
- `YierdisFfmIntSet` 是 off-heap intset。
- `YierdisFfmZSet` 是 off-heap sorted-set 变体，维护 member ref、score 和有序视图。

这些容器属于 DB 内部实现细节，外层命令和 runtime 只能通过 DB API 观察它们的效果。

## RESP 协议和回包

### `RespRequestDecoder`

源码：

- [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)

核心方法：

- `decode(...)`
- `tryReadArray(...)`
- `tryReadInline(...)`
- `findCrlfLine(...)`
- `parseInlineArgs(...)`

职责：

- 从 Netty `ByteBuf` 解析 RESP array / multibulk。
- 支持 inline command。
- 应用协议上限：
  - max bulk bytes
  - max args
  - max inline bytes
- 产出 `RespCommandRequest`。
- malformed RESP 产出 `RespProtocolError`，必要时进入 discard-to-LF 状态或关闭连接。

这层只负责协议 framing，不执行命令。

### `RespCommandAdapter` / `RespExecutionAdapter`

源码：

- [`RespCommandAdapter.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java)
- [`RespExecutionAdapter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java)

核心方法：

- `RespCommandAdapter.channelRead(...)`
- `RespExecutionAdapter.toExecutionRequest(...)`

职责：

- 把 `RespCommandRequest` 转成 `ByteArrayExecutionRequest`。
- 保持 protocol DTO 和 command execution contract 的边界。

### `RespProtocolErrorReplyHandler`

源码：

- [`RespProtocolErrorReplyHandler.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java)

核心方法：

- `channelRead(...)`

职责：

- 接收 decoder 产出的 `RespProtocolError`。
- 用 `ReplyWriter.protocolError(...)` 写错误。
- 根据 `closeAfterReply` 决定是否 flush 后关闭连接。

### `YierdisFastCommandHandler`

源码：

- [`YierdisFastCommandHandler.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java)

核心方法：

- `channelRead0(...)`
- `exceptionCaught(...)`
- `newReplyWriter(...)`

职责：

- 接收已经适配好的 `ExecutionRequest`。
- 只调用 `executor.trySubmit(connection, msg)`，不在 Netty I/O 线程执行命令。
- submit 被拒绝时直接写 `ERR busy <reason>`，并关闭/释放当前 request。
- protocol error 走 `protocolError(...)`，内部错误会标记连接 closing、关闭 autoRead，并在回包后关闭连接，避免已入队命令继续产生副作用。

这是 Netty pipeline 到 executor 的最后一道边界。

### `RespReplyWriter` / `RespReplyWriterFactory`

源码：

- [`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java)
- [`RespReplyWriterFactory.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java)

核心方法：

- `simpleString(...)`
- `error(...)`
- `protocolError(...)`
- `integer(...)`
- `bulkString(...)`
- `bulkString(BytesSlice slice)`
- `nullValue()`
- `arrayHeader(...)`
- `mapHeader(...)`
- `setHeader(...)`
- `pushHeader(...)`
- `requestCloseAfterReply()`
- `newWriter(Session session, BytesSink out)`

职责：

- 把 `ReplyWriter` 语义编码成 RESP2 / RESP3 bytes。
- 默认 RESP2。
- 如果 session 是 `ServerSession`，从 `session.respVersion()` 动态决定回包版本。
- RESP3 下 map/null/bool/double/set/push 使用 RESP3 类型。
- RESP2 下 map 降级为 flat key/value array。
- error 文案做 CRLF 清理、Redis error prefix 补齐和长度限制。
- bulk string 可以直接写 `BytesSlice`，避免强制先拼 heap byte array。

### `NettyExecutionIoAdapter`

源码：

- [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)

核心方法：

- `newReplySink(...)`
- `writeBufferedReply(...)`
- `flushPending(...)`
- `disableInput(...)`
- `enableInput(...)`
- `isActive(...)`
- `isWritable(...)`

职责：

- 把 executor 的 transport-neutral I/O seam 接到 Netty。
- 为每条命令分配 reply `ByteBuf`，包装为 `NettyByteBufSink`。
- close-after-reply 时 `writeAndFlush(...).addListener(CLOSE)`。
- 普通回包先 `write`，drain tick 结束后批量 flush。
- autoRead 修改切回 channel event loop 执行。

## Bytes 和 FFM / Off-Heap

### `BytesSource` / `BytesView` / `BytesSlice` / `BytesSink` / `DirectBytesSink`

源码：

- [`BytesSource.java`](../../yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java)
- [`BytesView.java`](../../yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java)
- [`BytesSlice.java`](../../yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSlice.java)
- [`BytesSink.java`](../../yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSink.java)
- [`DirectBytesSink.java`](../../yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/DirectBytesSink.java)

职责：

- 提供 Netty-free bytes contract。
- `BytesView` 用于请求级 lookup，不应直接存进 DB。
- `BytesSlice` 可被流式写入 `BytesSink`。
- `BytesSink` 是最小写口。
- `DirectBytesSink` 暴露 direct/off-heap fast-path 所需 writer index 和 memory address。

### `NettyByteBufSink`

源码：

- [`NettyByteBufSink.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java)

职责：

- 把 Netty `ByteBuf` 适配成 `DirectBytesSink`。
- 让 protocol / reply 写出路径保持 Netty-free，同时保留 ByteBuf fast-path。

### `YierdisFfmMemoryRuntime`

源码：

- [`YierdisFfmMemoryRuntime.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java)

核心方法：

- `allocateRegion(String owner, int bytes)`
- `usedBytes()`
- `close()`

职责：

- 管理 FFM `Arena` / `MemorySegment` region。
- 跟踪 live regions 和 runtime used bytes。
- close 时如果还有 live region，直接报 native memory leak。

### `YierdisFfmSlabAllocator`

源码：

- [`YierdisFfmSlabAllocator.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java)

核心方法：

- `allocate(int capacity)`
- `close()`
- slab-backed `OffHeapBuf.getByte/setByte/getBytes/setBytes/slice/close`
- slab-backed `OffHeapSlice.writeTo(...)`

职责：

- 在 FFM region 上做 slab allocation。
- 维护 allocator used bytes 和 reserved slab bytes。
- 支持 maxBytes hard cap。
- `OffHeapSlice.writeTo(...)` 可以直接流式写到 `BytesSink`。

### `YierdisForeignOffHeapAllocator`

源码：

- [`YierdisForeignOffHeapAllocator.java`](../../yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java)

核心方法：

- `allocate(int capacity)`
- `usedBytes()`
- `maxBytes()`
- `memoryRuntime()`
- `close()`

职责：

- 对外实现 `OffHeapAllocator`。
- 内部使用 `YierdisFfmSlabAllocator`。
- 维护 allocator-level used bytes。
- close 时检查 off-heap leak。
- 当全部 buffer 释放后重建 idle slab allocator，释放空闲 slab region。

## CLI 和 Benchmark

### `YierdisCli`

源码：

- [`YierdisCli.java`](../../yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java)

核心方法：

- `run(String[] args)`
- `runRepl(...)`
- `printReply(...)`
- `parseArgsToUtf8Bytes(...)`

职责：

- 提供单次命令和 REPL。
- 使用 `YierdisClient` 通过真实 TCP + RESP 访问 server。
- REPL 输入使用 `InlineCommandParser`。
- 按 Redis 风格打印 simple/error/integer/bulk/array/null。

### `YierdisClient`

源码：

- [`YierdisClient.java`](../../yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java)

核心方法：

- `connect(String host, int port)`
- `execute(List<byte[]> args, long timeoutMillis)`
- `executeUtf8(...)`

职责：

- blocking RESP client。
- 一次只允许一个 request/response。
- 超时、EOF 或 RESP parse failure 时关闭连接，避免 FIFO reply 错配。

### `InlineCommandParser`

源码：

- [`InlineCommandParser.java`](../../yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java)

核心方法：

- `parse(byte[] input, int off, int len, int maxArgs)`
- `splitUtf8(String line, int maxArgs)`

职责：

- 实现 Redis `sdssplitargs` 风格 inline 参数解析。
- 支持空白分隔、单/双引号、反斜杠转义和 `\xHH` 十六进制字节。
- 用于 CLI REPL，不是 server RESP decoder 的替代品。

### `RespClientCodec`

源码：

- [`RespClientCodec.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespClientCodec.java)

核心方法：

- `encodeCommand(...)`
- `writeCommand(...)`
- `readReply(...)`

职责：

- client / CLI / benchmark 共用 RESP writer 和 reader。
- 编码命令为 RESP array。
- 读取 simple string、error、integer、bulk、array、RESP3 null。

### `YierdisBench`

源码：

- [`YierdisBench.java`](../../yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java)

核心方法：

- `main(...)`
- `runThroughput(...)`
- `runLatency(...)`
- `waitReady(...)`
- `ThroughputWorker.call()`
- `LatencyWorker.call()`
- `RespCommandWriter`

职责：

- 纯 Java 压测工具。
- 默认可启动 server jar，也支持 connect-only。
- 用真实 TCP + RESP 路径跑 `PING/SET/GET`。
- throughput 模式支持 pipeline。
- latency 模式 pipeline=1 并记录分位数。
- strict replies 可校验 `PONG/OK/bulk length`。

## 测试、覆盖矩阵和架构护栏

### `operation-test-coverage-matrix.md`

源码：

- [`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md)
- [`OperationCoverageMatrixTest.java`](../../yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java)
- [`ServerOperationCoverageMatrixTest.java`](../../yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java)

职责：

- 文档矩阵把命令层、DB API、native 内部结构和 server-only 命令测试对应起来。
- `OperationCoverageMatrixTest` 防止新增默认命令后忘记补矩阵和集成测试。
- `ServerOperationCoverageMatrixTest` 防止 `HELLO/INFO/STATS` 这类 server-facing 命令脱离矩阵。

### 分层测试入口

核心测试文件：

- command processor：[`YierdisFastCommandProcessorModuleTest.java`](../../yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java)
- RESP decoder/writer/client：[`RespRequestDecoderTest.java`](../../yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java)、[`RespReplyWriterTest.java`](../../yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplyWriterTest.java)、[`RespClientCodecTest.java`](../../yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespClientCodecTest.java)
- executor/backpressure：[`CommandExecutorTest.java`](../../yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java)、[`CommandExecutorBackpressureTest.java`](../../yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java)、[`CommandExecutorFairSchedulingTest.java`](../../yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorFairSchedulingTest.java)
- DB internals：`yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/**`
- server integration：`yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/**`
- command integration：`yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/**`

职责：

- 单元测试验证每层核心 collaborator。
- integration tests 走真实 command processor 或真实 TCP/RESP 路径。
- matrix tests 维护“已实现命令必须有覆盖说明”的文档契约。

### 架构边界测试

源码：

- [`ArchitectureBoundaryTest.java`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java)
- [`ArchitectureDependencyRuleTest.java`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java)
- [`RespBoundaryGuardTest.java`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/resp/RespBoundaryGuardTest.java)
- [`YierdisDbArchitectureGuardTest.java`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java)
- [`architecture-policy.yml`](../../yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml)

职责：

- 防止 protocol DTO 泄漏到 command 层。
- 防止 command 层依赖 `YierdisDb` 具体实现。
- 防止 server 组装层绕过 runtime owner-thread seam。
- 防止 DB 内部 package 边界被上层直接穿透。
- 把 README/文档描述的模块依赖方向固化成可执行测试。

## 最重要的边界清单

### 协议边界

- `RespRequestDecoder` 只产出 protocol request 或 protocol error。
- `RespExecutionAdapter` 是 RESP request 到 `ExecutionRequest` 的桥。
- command 层不直接依赖 `RespCommandRequest`。

### 执行边界

- `YierdisFastCommandHandler` 只 submit，不执行命令。
- `CommandExecutor` 负责 owner-thread 调度。
- `DefaultYierdisEngine` 负责创建 `CommandContext` 并进入 command processor。

### 命令边界

- 命令注册统一走 `CommandSpec<T>`。
- 事务入队前必须用同一套 parser 校验。
- 命令通过 `DbReads/DbWrites` 访问 DB。

### DB 边界

- `YierdisDb` 是状态 owner。
- 写路径必须经过 `YierdisDbMutationExecutor` 和 `YierdisDbMemoryLedger`。
- key 生命周期和 payload 释放必须经过 `YierdisDbKeyLifecycle`。
- TTL cleanup 和 maxmemory eviction 不应绕过 owner-thread 约束。

### Runtime 边界

- 多 DB、FFM runtime ownership 和 global/per-db maxmemory 在 `YierdisInstance`。
- maintenance 和 shutdown 通过 `YierdisInstanceRuntimeAccess`。

### I/O 边界

- reply 写回通过 `ReplyWriter`。
- RESP writer 只编码语义，不决定业务行为。
- Netty autoRead 控制由 executor/backpressure controller 决策，Netty adapter 只执行副作用。

## 继续阅读

- 想看一条请求的完整执行过程：[`request-execution-flow.md`](./request-execution-flow.md)
- 想按源码顺序走一遍主链：[`main-path-walkthrough.md`](./main-path-walkthrough.md)
- 想深挖 DB 内核：[`db-internals.md`](./db-internals.md)
- 想深挖执行器和背压：[`executor-and-backpressure.md`](./executor-and-backpressure.md)
- 想理解命令和编码：[`commands-and-data-model.md`](./commands-and-data-model.md)
- 想改代码时找入口：[`development-navigation.md`](./development-navigation.md)
- 想确认测试入口和覆盖矩阵：[`testing-and-debugging.md`](./testing-and-debugging.md)、[`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md)
