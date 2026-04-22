# Request Execution Flow

本文说明一条请求是如何从网络入口一路走到命令执行、DB 读写和回包写出的。

它重点回答两个问题：

- 一般情况下，一条命令会经过哪些核心对象
- 对 `PING` 和 `SET` 这种代表性命令，调用链分别是什么

## 先看全局主链

可以把一次请求的主链路压缩成 8 步：

1. `YierdisServer` 启动 server
2. `YierdisServerBootstrap` 组装 `YierdisInstance`、`YierdisFastCommandProcessor`、`NettyCommandExecutor`
3. `YierdisServerChannelInitializer` 为每个连接建立 Netty pipeline 和连接态
4. `CustomRequestDecoder` 把网络帧解成协议请求对象
5. `ProtocolCommandAdapter` 把协议请求适配为 `ExecutionRequest`
6. `YierdisFastCommandHandler` 把 `ExecutionRequest` 提交给 command executor
7. `NettyCommandDrainLoop` 在单线程 executor 上串行执行命令
8. `YierdisFastCommandProcessor` 分发到具体命令处理器，并通过 `ReplyWriter` 写回

其中真正保证 Redis 风格单线程语义的关键点是：

- I/O 线程不直接碰 DB
- DB 访问只发生在 command executor 线程上

## 启动阶段的组装

请求路径在 server 启动阶段就已经被拼好。

关键对象有三个：

### `YierdisInstance`

负责装配多 DB、runtime seam、instance 级资源 ownership。

### `YierdisFastCommandProcessor`

负责：

- 注册默认命令模块
- 运行时事务判定
- 命令查找与分发
- 错误转换
- 变更事件 gate

### `NettyCommandExecutor`

负责：

- I/O 线程提交命令
- command executor 线程串行执行
- 有界 backlog
- 背压
- drain budget
- reply flush batching

### 用代码逻辑再展开一层

如果把启动过程写成“对象如何接对象”，大致是下面这条链：

1. `YierdisServer.main(...)` 解析 CLI，并在真正启动前检查 FFM 是否可用
2. `YierdisServerBootstrap.startInternal()` 先根据 runtime config 构造 `YierdisInstanceConfig`
3. `YierdisInstance.create(...)` 创建多 DB、FFM runtime 和可选的全局 maxmemory governor
4. bootstrap 再创建 `NettyServerInfoProvider`
5. bootstrap 创建 `YierdisFastCommandProcessor`
6. bootstrap 创建 `NettyCommandExecutor`
7. `executor.start()` 会先在 executor 线程上执行 `bindToCurrentThread`
8. 之后 Netty pipeline 才开始真正接收外部请求

这条顺序的关键不是“代码写得有几层”，而是：

- DB 的 owner thread 在 server 开始工作前就被固定下来
- command processor 在启动时就已经知道有哪些命令模块
- server 只是把协议流量送到已经准备好的 command/runtime 组合里

## 连接建立时发生什么

每个连接建立时，`YierdisServerChannelInitializer` 会先初始化统一的连接状态根对象 `ServerConnectionContext`。

它里面同时持有三类状态：

- 会话态：`ServerSessionState`
- 运行态：pending、closing、计数器等
- 调度态：executor 相关 channel 状态

然后 pipeline 会按下面顺序挂上 handler：

1. `writeBufferBackpressure`
2. `customRequestDecoder`
3. `protocolCommandAdapter`
4. `protocolErrorReply`
5. `commandHandler`

理解这个顺序很重要：

- 协议错误先在 decoder 阶段被发现
- 协议对象只在 server 适配层里被转换
- 真正命令提交只接收 `ExecutionRequest`

### `ServerConnectionContext` 为什么是根对象

对初学者来说，`ServerConnectionContext` 最容易被低估。

它存在的意义是：server 只在一个地方拥有 `Channel.attr(...)`。

它把三个原本容易散落在各层的状态切片统一到一起：

- `ServerSessionState`
  连接级逻辑状态，比如 `dbIndex`、事务队列、closing 相关联动
- `ServerRuntimeState`
  连接的 pending、pendingBytes、计数器、autoReadDisabled 标记
- `NettyExecutorChannelState`
  调度器需要的队列 /公平调度状态

这样一来：

- pipeline handler 不必各自挂一份 channel state
- command executor 可以通过 channel 回到连接态
- `SELECT` / `MULTI` / backpressure 都能沿着同一根对象协作

## `ExecutionRequest` 为什么重要

Yierdis 当前的命令执行统一围绕 `ExecutionRequest` 展开，而不是围绕 protocol DTO。

这意味着：

- protocol 层和 command 层解耦
- 事务重放和 change event 也复用同一份命令快照语义
- 命令层不需要知道请求最初来自 JSON DTO 还是别的 transport

这也是仓库里多处架构护栏要守住的边界之一。

对初学者来说，可以把 `ExecutionRequest` 理解成：

- “协议层和命令层之间的公共语言”

一旦请求已经变成 `ExecutionRequest`：

- 命令层就不需要知道原始 JSON DTO 长什么样
- 事务队列里存的也是统一快照
- change event 记录的也是统一快照

## 提交到 command executor 之后

`YierdisFastCommandHandler` 收到 `ExecutionRequest` 后，不直接执行，而是调用 `NettyCommandExecutor.trySubmitWithReason(...)`。

提交阶段会做几件事：

- 检查 executor 是否还在运行
- 检查全局队列容量是否已满
- 检查 queued bytes 预算是否超限
- 更新连接 pending / pendingBytes
- 在需要时关闭该连接的 `autoRead`

如果提交失败，server 立即回 `ERR busy ...` 并关闭 /回收当前请求对象。

如果提交成功：

- executor 接管请求生命周期
- drain loop 之后会在 command executor 线程里真正执行

## drain loop 里发生什么

`NettyCommandDrainLoop` 是真正的执行核心。

每次执行一个命令时，它会：

1. 检查连接是否已经 closing
2. 为这条命令创建 `ReplyWriter`
3. 从连接里取出 `ServerSessionState`
4. 复用或重置一个 `CommandContext(session, out)`
5. 调用 `YierdisFastCommandProcessor.execute(...)`
6. 把输出 buffer 批量写回
7. 释放请求对象和 backlog 预算
8. 在 pending 降到低水位后尝试重新开启 `autoRead`

这条链说明：命令上下文的会话态和输出口是在 executor 线程内配对的，而不是在 I/O 线程内配好的。

### `CommandContext` 在这里起什么作用

`CommandContext` 很简单，但它是命令层看到的上下文入口：

- `session`
- `ReplyWriter out`

在 executor 线程里，`NettyCommandExecutionSupport` 会把：

- 当前连接的 `ServerSessionState`
- 当前命令的 `ReplyWriter`

装进一个可复用的 `CommandContext` 对象，再交给 `YierdisFastCommandProcessor`。

所以命令层能做的两件事其实非常固定：

- 通过 `ctx.serverSessionOrNull()` 读取连接态
- 通过 `ctx.out()` 写回复

命令层不需要也不应该直接知道 Netty 的 `ChannelHandlerContext`。

## `PING` 的调用链

`PING` 是最短的一条命令链，适合用来理解“非 DB 命令怎么走”。

### 路径

1. `CustomRequestDecoder`
2. `ProtocolCommandAdapter`
3. `YierdisFastCommandHandler`
4. `NettyCommandExecutor`
5. `NettyCommandDrainLoop`
6. `NettyCommandExecutionSupport`
7. `YierdisFastCommandProcessor`
8. `CoreConnectionCommands.ping(...)`
9. `ReplyWriter.simpleString("PONG")`
10. `JsonLineReplyWriter` 编码为 NDJSON

### 特征

- 不访问 DB
- 不修改会话状态
- 主要验证协议、提交、执行器和回包链路

## `SET` 的调用链

`SET` 是理解本项目 DB 内核设计的更好例子。

### 命令层

`YierdisFastCommandProcessor` 命中 `StringCommands.set(...)`。这里负责：

- 解析 `NX` / `XX`
- 解析 `GET`
- 解析 `EX` / `PX` / `EXAT` / `PXAT` / `KEEPTTL`
- 做语法和参数合法性校验
- 把选项映射成 `SetMode` 和 `ExpireOption`

然后命令层并不直接访问具体 DB 实现，而是走：

- `support.dbWrites(ctx).strings().set(...)`

### DB 路由

`CommandSupport.dbWrites(ctx)` 会按当前连接态里的 DB index 做路由。

也就是说：

- `SELECT` 改的是连接会话态
- 每次执行命令时，命令层再按当前会话态选中对应 DB

所以多 DB 行为是“连接级别的逻辑路由”，不是命令层硬编码某个 DB。

### 这里真正协作的 4 个对象

如果把这段 DB 路由逻辑拆开看，实际参与协作的是：

1. `ServerSessionState`
   保存当前连接选中的 `dbIndex`
2. `CommandContext`
   通过 `dbIndexProviderOrNull()` 暴露会话里的 DB index
3. `CommandSupport`
   调用 `dbRouter.dbFor(ctx.dbIndexProviderOrNull())`
4. `YierdisDbRouter`
   最终从 `YierdisInstance.engines()` 里挑出对应的 `DbEngine`

理解这 4 个对象后，初学者通常就能明白：

- `SELECT` 为什么只是改连接态
- 命令层为什么总是通过 `DbReads/DbWrites` 间接到 DB
- 多 DB 路由为什么不需要把 `dbIndex` 手工传遍所有命令实现

### 写入阶段

真正写入发生在 `YierdisStringOps.set(...)`。

这一步不是直接改对象，而是先走 mutation executor：

1. 估算本次写入的 `upperBoundBytes`
2. 进入 `YierdisDbMutationExecutor`
3. `ledger.reserve(...)`
4. 真正执行 mutation
5. `commit(...)` 或 `rollback(...)`

这一步是 `maxmemory / eviction / OOM` 的关键闭环。

### 如果把 `SET` 写路径再拆细一点

对初学者来说，`SET` 的代码逻辑可以按下面顺序理解：

1. `StringCommands.set(...)`
   解析选项并做语法校验
2. `CommandSupport.dbWrites(ctx).strings().set(...)`
   按当前连接路由到目标 DB 的 string write ops
3. `YierdisStringOps.set(...)`
   估算写入上界，准备 mutation plan
4. `YierdisDbMutationExecutor.execute(...)`
   先 `reserve`，再执行 mutation，最后 `commit` 或 `rollback`
5. `YierdisDbKeyLifecycle.computeWithHandle(...)`
   在 keyspace 上读写 key 和 object
6. `YierdisObject.newString(...)` 或 `overwriteWithString(...)`
   构造或覆盖真实字符串对象
7. `YierdisDbKeyLifecycle.setExpireAtMillis(...)` / `removeExpire(...)`
   处理 TTL

这条链有一个非常重要的工程化特点：

- 命令层不直接改对象
- `*Ops` 也不直接无保护地改内存
- 所有真正的 mutation 都被 memory ledger 和 lifecycle 包裹住

### key 生命周期

具体 mutation 里，字符串写入会通过 `YierdisDbKeyLifecycle.computeWithHandle(...)` 操作 keyspace。

这里统一处理：

- 是否已有旧值
- 旧 key 是否已经过期
- `NX/XX` 语义
- 覆盖旧 payload 时的释放
- TTL 设置或删除
- LRU touch
- used bytes 调整

### value 表示

新字符串值最终会落成 `YierdisObject`。

当前字符串内部编码会根据内容和长度选择：

- `STRING_INT`
- `STRING_EMBSTR`
- `STRING_RAW`

如果当前路径启用了 off-heap allocator，payload 会进入 `OffHeapBuf`；否则会留在 heap。

初学者读这里时，可以带着一个简单问题：

- “为什么 `SET` 不是把 `byte[]` 塞进 `Map<byte[], byte[]>` 就结束？”

答案是：

- 项目想保留 Redis 风格的内部编码切换
- 还要考虑 TTL、内存记账、off-heap、LRU touch、wrong-type 等行为

所以 value 最终不是一个原始字节数组，而是带类型、编码和 payload 生命周期的 `YierdisObject`。

## 事务怎么排队和重放

事务逻辑主要不在 protocol 层，而在连接态和命令处理器里。

### 入队阶段

`YierdisFastCommandProcessor.execute(...)` 在真正执行命令前，会先看：

- `ctx.serverSessionOrNull()`
- `session.transaction()`

如果当前连接正处于 `MULTI` 状态，并且命令不是 `MULTI/EXEC/DISCARD` 本身，那么处理器不会立刻执行命令，而是：

1. 检查该命令是否允许在事务中出现
2. 调用 `tx.tryEnqueue(request)`
3. 成功则返回 `QUEUED`
4. 队列超限则把事务标记成 aborted

### 为什么事务队列存的是快照

`ServerSessionState.ConnectionTransactionState.tryEnqueue(...)` 不会直接把当前 `ExecutionRequest` 引用放进队列，而是：

- 调用 `ByteArrayExecutionRequest.copyOf(request)` 做一份快照

原因是：

- 当前请求对象的生命周期归 executor 管理
- 事务里的命令必须独立存活到未来的 `EXEC`
- 避免原请求对象被回收后事务队列里留下悬空引用

### `EXEC` 时发生什么

`EXEC` 最终会从事务队列里 `drain()` 出这些命令快照，再逐条重新走命令执行逻辑。

因此，事务不是“把写操作先记成某种特殊 IR 再解释”，而是：

- 把统一的 `ExecutionRequest` 快照先排队
- 到 `EXEC` 时再逐条重放

这也解释了为什么这个项目特别强调 `ExecutionRequest/ExecutionRecord` 是统一边界。

## 错误、关闭和背压

这条请求链还有三个容易忽略但很关键的侧面：

### 1. 协议错误尽量可恢复

decoder 会尽量把协议错误转成 `ProtocolError` 事件，再由上层统一编码错误响应；在无法安全继续同步帧边界时，才会关闭连接。

### 2. 连接进入 closing 后，已入队命令会被跳过

如果连接因为 `QUIT` 或内部错误进入 closing，后续已经入队但尚未执行的命令不会再产生副作用，而是只做资源回收。

### 3. 背压是连接级和全局预算一起生效

进入背压不只取决于单连接 pending 条数，还和：

- pending bytes
- 全局队列容量
- 全局 queued bytes 预算

一起决定。

## 背压到底由谁决定

对初学者来说，最容易混淆的是“背压到底在 Netty 里，还是在 executor 里”。

更准确的说法是：

- Netty 提供 `autoRead` 和 channel writability 这些机械开关
- 什么时候关、什么时候开，由 executor 侧的背压逻辑决定

真正参与协作的对象有：

- `NettyCommandSubmitter`
  入队时检查队列容量和 bytes 预算
- `ExecutorBacklogBudget`
  管全局 slot 和 queued bytes
- `ExecutorBackpressureController`
  负责进入 /退出 backpressure，以及恢复 `autoRead`
- `NettyCommandDrainLoop`
  命令执行完成后释放 slot 和 bytes 预算
- `ServerConnectionContext`
  记录单连接 pending / pendingBytes 和状态计数

也就是说：

- 入队阶段负责“发现压力过大”
- drain 阶段负责“压力释放后恢复”

这不是某个单独类一把抓，而是 submitter、drain loop 和 backpressure controller 三方协作。

## 建议先看的测试

如果你想把本文里的代码逻辑和真实行为对上，下面这些测试最适合初学者：

- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  看启动后 server、核心命令和 server 命令是怎么真正接在一起的
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
  看 drain 限制、queued bytes 预算、`QUIT` 后跳过后续命令等执行器行为
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`
  看 `SET`、`GET`、`NX`、`KEEPTTL` 等命令行为
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
  看事务 `QUEUED`、`EXEC` 重放和队列行为
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
  看字符串 off-heap 写入、读取和释放

## 读这条链时推荐打开的文件

- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`

如果你想先继续按源码顺序把这条主链读透，下一篇建议看 [`main-path-walkthrough.md`](./main-path-walkthrough.md)。

如果你想把这条链路里出现的 request/reply 线格式讲清楚，接着看 [`protocol-reference.md`](./protocol-reference.md)。

如果你想进一步理解命令分发落到哪些逻辑类型和内部编码上，接着看 [`commands-and-data-model.md`](./commands-and-data-model.md)。

如果你想进一步把提交、drain、queue budget 和 autoRead 背压机制讲细，接着看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

如果你打算真正改命令行为，再继续看 [`development-navigation.md`](./development-navigation.md)。
