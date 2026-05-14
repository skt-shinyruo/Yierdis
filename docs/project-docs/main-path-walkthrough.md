# Main Path Walkthrough

本文按源码阅读顺序走一遍 Yierdis 的主路径。它不是模块清单，也不替代
[`request-execution-flow.md`](./request-execution-flow.md)；后者回答“一条请求怎么流动”，本文回答“打开哪些类、按什么顺序读，才能把这条链真正看懂”。

建议先读：

- [`project-overview.md`](./project-overview.md)
- [`request-execution-flow.md`](./request-execution-flow.md)
- [`core-logic-index.md`](./core-logic-index.md)

读本文时最好一段一段对照源码。每一节都先给入口文件，再说明这个类在主路径里的边界。

## 一张路线图

把生产路径压缩成一条链，大致是：

```text
YierdisServer
  -> ServerConfig / ForeignMemoryAutoModules
  -> YierdisServerBootstrap
  -> YierdisInstance
  -> DefaultYierdisEngine
  -> CommandExecutor
  -> YierdisServerChannelInitializer
  -> NettyExecutionConnection
  -> RespRequestDecoder
  -> RespCommandAdapter
  -> YierdisFastCommandHandler
  -> CommandExecutorSubmitter
  -> CommandExecutorDrainLoop
  -> CommandExecutorExecutionSupport
  -> DefaultYierdisEngine.execute(...)
  -> YierdisFastCommandProcessor
  -> CommandSpec<T>.parse(...)
  -> typed command handler
  -> CommandSupport
  -> DbEngine / DbReads / DbWrites
  -> Yierdis*Ops
  -> YierdisDbMutationExecutor
  -> YierdisDbKeyLifecycle
  -> EntryRecord / typed ValueHandle / TypeRoot
  -> ReplyWriter / RespReplyWriter
  -> NettyExecutionIoAdapter
```

这条链里有五条边界最重要：

- Netty I/O 线程只负责收包、适配和提交，不直接访问 DB。
- RESP 协议对象不会进入命令层，必须先变成 `ExecutionRequest`。
- executor 只负责排队、背压、owner-thread 调度和 request 生命周期，不创建 `CommandContext`。
- engine 负责把 `Session + ExecutionRequest + ReplyWriter` 变成命令层的 `CommandContext`。
- DB 写入统一走 mutation plan、memory ledger 和 key lifecycle，不绕过内存预算直接改结构。

## 1. 进程入口：`YierdisServer`

源码：

- [`YierdisServer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java)
- [`ServerConfig.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java)
- [`ForeignMemoryAutoModules.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ForeignMemoryAutoModules.java)

`YierdisServer.main(...)` 是很薄的一层：

1. `ServerConfig.fromArgs(args)` 用 picocli 解析、归一化和校验启动参数。
2. 如果用户请求 help，`fromArgs(...)` 返回 `null`，进程直接结束。
3. `ForeignMemoryAutoModules.ensureFfmAvailable()` 确认当前 JVM 有 JDK 25 FFM API。
4. `YierdisServerBootstrap.start(config)` 进入真正的 server 组装。
5. `server.awaitClose()` 等待 server channel 关闭。

所以 `YierdisServer` 不是业务中心。它只做启动前校验、稳定退出和生命周期外壳。

## 2. 组装中心：`YierdisServerBootstrap`

源码：

- [`YierdisServerBootstrap.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java)
- [`CommandExecutorConfigs.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java)

`YierdisServerBootstrap` 是 server 的 composition root。最值得读的方法是 `startInternal()`，它按固定顺序完成组装：

1. 再次检查 FFM 可用性。
2. 从 runtime config 构造 `YierdisInstanceConfig`。
3. `YierdisInstance.create(...)` 创建实例和多 DB 资源。
4. 拿到 `runtimeAccess`、maintenance hook 和 `observability`。
5. 创建 `NettyServerInfoProvider`，绑定 observability。
6. 创建 `SlowCommandGovernor`。
7. 创建 `DefaultYierdisEngine`，注入 `DefaultCommandModules` 和 `ServerCommandModule`。
8. 创建 `CommandExecutor<NettyExecutionConnection>`，把 `commandEngine::execute` 作为唯一命令执行函数交给 executor。
9. 创建 Netty boss / worker group。
10. `executor.start()` 在 owner executor 上调用 `runtimeAccess::bindToCurrentThread`。
11. 如果开启 cleanup interval，用 worker event loop 做定时器，再通过 `executor.executeMaintenance(...)` 把 cleanup 调回 owner thread。
12. 装配 Netty `ServerBootstrap` 并 `bind(port)`。

这里的顺序很关键：DB、engine 和 executor 都先就绪，Netty 最后才对外 bind。第一个请求进来时，主执行路径已经完整存在。

`close()` 也值得看一遍。它按反向顺序 best-effort 关闭 server channel、cleanup future、executor、engine、instance runtime 和 Netty group。`closeRuntimeAccess(...)` 会尽量把 runtime close 调度回 owner thread，保持 DB 线程语义一致。

## 3. 实例和多 DB：`YierdisInstance`

源码：

- [`YierdisInstance.java`](../../yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java)

不要把 `YierdisInstance` 理解成“一个 DB”。它是 embedded instance API 和资源 owner，负责创建多个 `RuntimeDbEngine`，并对上层暴露 `DbEngine` 能力视图。

`YierdisInstance.create(...)` 的关键逻辑是：

- 至少创建 1 个逻辑 DB。
- 创建实例级 `YierdisFfmMemoryRuntime`。
- 根据 `maxmemoryScope` 选择 `GLOBAL` 或 `PER_DB` 预算。
- `GLOBAL` 默认让多个 DB 共享同一个 FFM runtime，并用 `YierdisGlobalMaxmemoryGovernor` 统一协调 eviction。
- `PER_DB` 会把 maxmemory 按 DB 数量分摊给每个 DB。
- 最终把 DB 数组、memory runtime 和 governor 装进 `YierdisInstanceResources`。

bootstrap 通过 `instance.engines()` 拿到的是 `DbEngine[]` 防御性拷贝。命令层后续只会看到 `DbEngine / DbReads / DbWrites`，不会直接依赖 `YierdisDb` 实现类。

## 4. 连接根对象和 pipeline

源码：

- [`YierdisServerChannelInitializer.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java)
- [`NettyExecutionConnection.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java)

`initChannel(...)` 先根据输出缓冲配置设置 Netty `WriteBufferWaterMark`，然后调用：

```java
NettyExecutionConnection.getOrCreate(ch, txMaxCommands, txMaxBytes)
```

这个对象是连接级根对象，里面有三块状态：

- Netty `Channel`：真实 transport。
- `EngineSession`：engine-owned 业务会话态，包含 DB index、transaction、client id/name、auth、RESP version。
- `ExecutionConnectionContext`：executor-owned 调度态，包含 pending、pending bytes、closing、backpressure、fair scheduling 状态和统计。

`EngineSession` 可以绑定一个只读 stats supplier 给 `INFO/STATS` 用，但它不拥有 executor 的 pending 计数。`NettyExecutionConnection.markClosing()` 会先标记 executor context closing，再丢弃 session 上的事务队列，避免 close 之后的已排队命令继续产生副作用。

pipeline 当前顺序是：

```text
writeBufferBackpressure
  -> optional idleTimeout
  -> optional idleTimeoutCloser
  -> respRequestDecoder
  -> respCommandAdapter
  -> respProtocolErrorReply
  -> commandHandler
```

`writeBufferBackpressure` 监听 channel writability：不可写时通知 executor 关闭输入，可写后让 executor 重新评估是否恢复 `autoRead`。如果配置了 idle timeout，还会插入读空闲关闭逻辑。

## 5. 协议边界：RESP 到 `ExecutionRequest`

源码：

- [`RespRequestDecoder.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java)
- [`RespCommandAdapter.java`](../../yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java)
- [`RespExecutionAdapter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java)

`RespRequestDecoder` 从 Netty `ByteBuf` 里解 RESP array 或 inline command，输出两类对象：

- `RespCommandRequest`
- `RespProtocolError`

正常命令会继续进入 `RespCommandAdapter`。这个 adapter 只做一件事：

```text
RespCommandRequest -> ByteArrayExecutionRequest.wrapReadOnly(...)
```

这一步让协议 DTO 停在 networking 边界内。进入 server handler 和 command 层之后，公共请求模型就是 `ExecutionRequest`。

协议错误会由 `respProtocolErrorReply` 写回错误并按协议安全性决定 close-after-reply。无法可靠恢复帧边界时，连接不能继续假装正常。

## 6. 提交入口：`YierdisFastCommandHandler`

源码：

- [`YierdisFastCommandHandler.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java)

`YierdisFastCommandHandler` 的输入类型已经是 `ExecutionRequest`。`channelRead0(...)` 做的事情很窄：

1. 从 channel attribute 取 `NettyExecutionConnection`。
2. 调用 `executor.trySubmit(connection, msg)`。
3. 如果成功，request 生命周期交给 executor，handler 直接返回。
4. 如果失败，立即写 `ERR busy <reason>`，再关闭当前 request。

这里不执行命令，因为当前线程仍是 I/O 线程。Yierdis 保持 Redis 风格的 owner-thread 语义：I/O 线程只提交，DB 只能由 command executor 线程访问。

`exceptionCaught(...)` 也体现了边界：协议错误尽量回包，内部错误会标记连接 closing、关闭输入，并在回包后关闭连接。

## 7. Executor：队列、预算、背压和 owner thread

源码：

- [`CommandExecutor.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)
- [`CommandExecutorSubmitter.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java)
- [`CommandExecutorDrainLoop.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java)
- [`CommandExecutorExecutionSupport.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java)
- [`ExecutorBackpressureController.java`](../../yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java)

`CommandExecutor` 本身是一个总控对象。构造时它组出：

- `ExecutorBacklogBudget`
- `ExecutorBackpressureController`
- `ExecutorTaskQueue`
- `CommandExecutorSubmitter`
- `CommandExecutorDrainLoop`
- `CommandExecutorExecutionSupport`

`start()` 会先在 owner executor 上执行 `bindToCurrentThread`，再把 drain loop 标记为 started。

提交阶段由 `CommandExecutorSubmitter` 负责。它会检查：

- executor 是否还在 running；
- 全局 queue slot 是否够；
- queued bytes 预算是否够；
- 当前连接 pending / pendingBytes 是否超过高水位；
- 全局 backlog 是否已经进入高水位。

拒绝时返回 `NOT_RUNNING`、`QUEUE_FULL`、`BYTES_BUDGET` 或 `OFFER_FAILED`。接受时记录连接 pending，保留 request retained bytes 预算，并调度 drain。

执行阶段由 `CommandExecutorDrainLoop` 在 owner executor 上跑。它按 `maxDrainCommands` 和 drain time budget 从队列取任务，逐条交给 `CommandExecutorExecutionSupport.execute(...)`，最后对本轮触达的连接批量 flush。

`CommandExecutorExecutionSupport` 才真正调用命令执行函数：

```text
ReplyWriter writer = replyWriterFactory.newWriter(session, sink)
commandProcessor.execute(session, request, writer)
ioAdapter.writeBufferedReply(connection, writer.closeAfterReplyRequested())
```

它还负责：

- closing 连接跳过副作用；
- close-after-reply 时标记连接 closing；
- 内部异常时 best-effort 写 `ERR internal error` 并关闭；
- finally 中关闭 request、释放 slot / bytes 预算；
- pending 和全局 backlog 回落后恢复输入。

`ExecutorBackpressureController` 不懂业务命令，也不依赖 Netty。它只通过 `ExecutorBackpressureIo` 关闭或恢复输入，并记录哪些连接是 executor 自己禁用了 `autoRead`。

## 8. Engine：从调度合同进入命令层

源码：

- [`YierdisEngine.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java)
- [`DefaultYierdisEngine.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java)
- [`EngineSession.java`](../../yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java)

engine 的外部入口固定为：

```java
void execute(Session session, ExecutionRequest request, ReplyWriter out);
```

`DefaultYierdisEngine` 会校验传入的是 `ServerSession`，然后创建：

```java
new CommandContext(serverSession, out)
```

再把请求交给 `YierdisFastCommandProcessor`。

这就是 executor 和 command 层之间的分界线。executor 不知道 `CommandContext`，command 层也不需要知道 Netty 或 executor queue。

`maintenanceTick()` 也在 engine 上暴露，但 bootstrap 会通过 `executor.executeMaintenance(...)` 调度它，保证 cleanup 仍在 owner thread 上访问 DB。

## 9. 命令分发：`YierdisFastCommandProcessor`

源码：

- [`YierdisFastCommandProcessor.java`](../../yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)
- [`DefaultCommandModules.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java)

processor 构造时会先注册事务命令，再注册外部注入的 command module。生产启动时，bootstrap 注入的是：

- `DefaultCommandModules.create(dbRouter(instance), infoProvider, slowGovernor)`
- `ServerCommandModule`

`DefaultCommandModules` 会注册：

- `CoreConnectionCommands`
- `KeyCommands`
- `StringCommands`
- `HllCommands`
- `ListCommands`
- `HashCommands`
- `SetCommands`
- `ZSetCommands`

执行时，processor 的顺序是：

1. 检查空命令和空 `argv[0]`。
2. 拒绝不允许的 null bulk string。
3. 如果事务 active，判断当前命令是否 `MULTI/EXEC/DISCARD`。
4. 普通事务入队命令先查 `CommandSpec`，再运行同一个 parser 做入队前校验。
5. 入队成功后 `EngineSession.transaction().tryEnqueue(request)` 保存 `ByteArrayExecutionRequest.copyOf(...)` 快照并返回 `QUEUED`。
6. 非入队路径查 `CommandSpec<T>`，parse 成 typed 参数，再执行 typed handler。
7. 捕获 wrong type、command exception、参数错误，统一写错误回复。
8. 如果命令真实改变了 value 或 TTL，再按 change event gate 触发事件。

事务不是另一套 IR。它排队和重放的仍是 `ExecutionRequest` 快照，重放时仍走同一套 parser、handler、DB 路由和错误映射。

## 10. 最短路径：`PING`

`PING` 不访问 DB，适合先确认主链前半段：

```text
ByteBuf
  -> RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter
  -> ExecutionRequest
  -> YierdisFastCommandHandler
  -> CommandExecutor.trySubmit(...)
  -> owner thread
  -> DefaultYierdisEngine.execute(...)
  -> YierdisFastCommandProcessor
  -> CoreConnectionCommands.ping(...)
  -> ReplyWriter.simpleString("PONG")
  -> NettyExecutionIoAdapter
```

读 `PING` 时重点看三件事：

- 请求什么时候从 protocol DTO 变成 `ExecutionRequest`。
- 命令什么时候从 I/O 线程切到 owner executor。
- handler 为什么只写语义回复，而不关心 RESP bytes 怎么编码。

## 11. 写路径：`SET`

`SET` 能把命令解析、DB 路由、内存预算、TTL 和 payload 生命周期串起来。

### 11.1 `StringCommands.set(...)`

源码：

- [`StringCommands.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)

`StringCommands` 通过 `CommandSpec` 注册 `SET`。`parseSet(...)` 负责解析：

- `NX`
- `XX`
- `GET`
- `EX`
- `PX`
- `EXAT`
- `PXAT`
- `KEEPTTL`

解析结果会变成稳定类型：

- `SetMode`
- `ExpireOption`
- `getOld`

`set(...)` 本身不直接修改 DB，而是调用：

```java
support.dbWrites(ctx).strings().set(...)
```

成功时写 `OK`，`GET` 模式写旧值，`NX/XX` 未生效时写 null bulk string。mutation outcome 会通过 `support.recordMutation(...)` 记录到 `CommandContext`，供 change event gate 使用。

### 11.2 `CommandSupport`

源码：

- [`CommandSupport.java`](../../yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)

`CommandSupport.dbWrites(ctx)` 的路线是：

```text
CommandContext
  -> ctx.session()
  -> YierdisDbRouter.dbFor(session)
  -> DbEngine
  -> DbWrites
```

bootstrap 创建 router 时用的是 `instance.engines()`。如果 session 的 DB index 越界，router 会回退到 DB 0。

这保证 typed command handler 只依赖能力接口，不依赖 `YierdisDb` 内部结构。

### 11.3 `YierdisStringOps.set(...)`

源码：

- [`YierdisStringOps.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)

这里才开始真正形成 storage mutation。它先 `internals.checkThread()`，确保当前线程是 DB owner thread；然后：

1. 计算 expire 目标时间和 `KEEPTTL` 语义。
2. 用 `YierdisDbMemoryEstimator.estimateStringWriteUpperBound(...)` 估算写入上界。
3. 如果需要 TTL，再加 TTL entry 预算。
4. 构造 `YierdisDbMutationExecutor.MutationPlan`。
5. 在 plan 里通过 `keyLifecycle.computeWithHandle(...)` 读旧 record、处理过期、判断 `NX/XX`、复制旧值、写入新字符串、计算实际 delta bytes。

注意这里仍然没有裸写全局状态。写入被包在 mutation plan 里，后续由 mutation executor 做预算、commit 和 rollback。

### 11.4 `YierdisDbMutationExecutor`

源码：

- [`YierdisDbMutationExecutor.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

mutation executor 是 maxmemory 和回滚语义的收口点：

```text
ledger.reserve(upperBoundBytes)
  -> plan.apply()
  -> ledger.commit(actualDeltaBytes)
```

如果 reserve 失败，会把 `MemoryLedgerOutOfMemoryException` 映射成 Redis 风格 OOM。off-heap 分配失败会映射成 off-heap OOM。任何 runtime exception 或 error 都会 rollback 已保留预算。

### 11.5 `YierdisDbKeyLifecycle`

源码：

- [`YierdisDbKeyLifecycle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)

key lifecycle 负责 keyspace、entry table、TTL 和 value root 的一致性。`SET` 路径里最重要的是：

- `computeWithHandle(...)` 统一处理 key handle 和 entry record 替换。
- `isKeyExpired(...)` / `removeExpire(...)` 处理已过期旧值。
- `setExpireAtMillis(...)` / `removeExpire(...)` 更新 expire index 和 entry record 上的 expire 字段。
- `releaseReplacedValue(...)` 避免旧 payload 泄漏。
- `touchRecord(...)` 更新访问元数据。

所以 `SET` 不是“把 map 里的 value 换掉”这么简单。它同时维护 key directory、entry table、TTL index、memory ledger 和 payload 生命周期。

### 11.6 `EntryRecord`、stable handle 和 `StringRoot`

源码：

- [`EntryRecord.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java)
- [`EntryHandle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java)
- [`ValueHandle.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java)
- [`TypeRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/TypeRoot.java)
- [`StringRoot.java`](../../yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java)

`EntryRecord` 保存的是元数据：key identity、`ValueHandle`、type、encoding、expire time、估算字节数和访问元数据。真实字符串 payload 在 `StringRoot` 里，通过 `ValueHandle` 访问。

`EntryHandle` 和 `ValueHandle` 都是 `NativeHandle` raw value 的 Java record 包装。`EntryHandle` 只能包装 `ENTRY_RECORD`，`EntryTable` 通过 stable allocator resolve handle 后读写 56 bytes native entry metadata。`ValueHandle` 使用 string/list/hash/set/zset 对应的 native kind，给 type root payload 一个稳定 identity；当前多数 value handle 由 root 自己解析，不能当成 object table slot 使用。

这意味着主链路里传递的是 handle，不是 physical address。allocator 如果通过 `realloc` 或 active defrag 移动 entry record，只更新 object table；`NativeKeyDirectory` 和 `EntryRecord` 里的 handle 不需要重写。更底层的 object table、pin/quarantine 和 defrag 协议见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

字符串常见编码包括：

- `STRING_INT`
- `STRING_EMBSTR`
- `STRING_RAW`

这层设计让 keyspace 的 entry 元数据和不同类型的 payload root 分开演进。

## 12. 回包写出

源码：

- [`ReplyWriter.java`](../../yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java)
- [`RespReplyWriter.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java)
- [`RespReplyWriterFactory.java`](../../yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java)
- [`NettyExecutionIoAdapter.java`](../../yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java)

命令层只调用语义 API：

- `simpleString(...)`
- `error(...)`
- `integer(...)`
- `bulkString(...)`
- `arrayHeader(...)`
- `requestCloseAfterReply()`

`RespReplyWriter` 根据 session 上的 RESP version 编码成 RESP2 或 RESP3。`RespReplyWriterFactory` 如果拿不到 `ServerSession`，默认用 RESP2；server 正常路径会传入 `EngineSession`，因此可以跟随 `HELLO` 协商后的版本。

`NettyExecutionIoAdapter` 为每次执行创建 reply buffer。普通回复先 `channel.write(...)`，本轮 drain 结束后由 `flushPending(...)` 批量 flush；close-after-reply 则直接 `writeAndFlush(...).addListener(CLOSE)`。

## 最容易读错的地方

- `YierdisServerBootstrap` 是接线中心，不是命令语义中心。
- `YierdisInstance` 是实例级资源 owner，不是单个 DB。
- `EngineSession` 管业务会话态，`ExecutionConnectionContext` 管调度态。
- `YierdisFastCommandHandler` 不执行命令，只提交到 executor。
- executor 不创建 `CommandContext`，它只传 `Session + ExecutionRequest + ReplyWriter`。
- `CommandSpec<T>` 是命令解析和 typed handler 的入口，不应该在 server handler 里重新解析参数。
- `SET` 的真实写入必须走 `YierdisDbMutationExecutor` 和 `YierdisDbKeyLifecycle`。
- backpressure 同时受单连接 pending、pending bytes、全局 queue slot、queued bytes 和 channel writability 影响。
- 事务队列保存的是 `ExecutionRequest` 快照，不是另一套命令 IR。

## 配合哪些测试读

启动和真实 socket 接线：

- [`YierdisServerBootstrapCommandWiringTest.java`](../../yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java)

executor、背压和 closing：

- [`CommandExecutorTest.java`](../../yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java)
- [`ExecutionConnectionContextTest.java`](../../yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutionConnectionContextTest.java)

engine 和 session：

- [`DefaultYierdisEngineTest.java`](../../yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java)
- [`EngineSessionTest.java`](../../yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java)

命令行为和事务：

- [`CommandProcessorTest.java`](../../yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandProcessorTest.java)
- [`TransactionCommandTest.java`](../../yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java)
- [`TransactionQueueCleanupTest.java`](../../yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TransactionQueueCleanupTest.java)

storage 和 off-heap 字符串：

- [`OffHeapStringStorageTest.java`](../../yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java)

架构边界：

- [`ArchitectureBoundaryTest.java`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java)

## 读完后继续看什么

- 协议细节：[`protocol-reference.md`](./protocol-reference.md)
- 命令和数据模型：[`commands-and-data-model.md`](./commands-and-data-model.md)
- DB 内核：[`db-internals.md`](./db-internals.md)
- executor 和背压：[`executor-and-backpressure.md`](./executor-and-backpressure.md)
- 配置和运维：[`configuration-and-operations.md`](./configuration-and-operations.md)
- FFM 和 off-heap：[`ffm-usage.md`](./ffm-usage.md)
- 开发导航：[`development-navigation.md`](./development-navigation.md)
