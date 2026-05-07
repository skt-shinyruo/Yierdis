# Main Path Walkthrough

本文是一篇“源码导读式”文档，目标不是列出所有模块，而是带着初学者沿着一条最核心的主链，理解 Yierdis 是怎么真正跑起来的。

如果你已经看过：

- [`project-overview.md`](./project-overview.md)
- [`request-execution-flow.md`](./request-execution-flow.md)

那这篇文档就是下一步：把抽象流程落到具体类、关键方法和对象传递上。

## 建议的阅读方式

最适合的方式不是把本文一次性读完，而是：

1. 先读一个阶段
2. 打开对应源码文件
3. 顺着类和方法名对照代码
4. 再回来读下一个阶段

这篇文档假设你最关心的问题是：

- server 是怎么从 `main()` 变成一个能收请求的进程的？
- 一条请求是怎么从 Netty 流到命令层和 DB 的？
- `SET` 这种写命令到底是怎么安全落到内存里的？

## 主链一览

可以先把主链压成下面这条“类接类”的路线：

```text
YierdisServer
  -> YierdisServerBootstrap
  -> YierdisInstance
  -> YierdisServerChannelInitializer
  -> CustomRequestDecoder
  -> ProtocolCommandAdapter
  -> YierdisFastCommandHandler
  -> CommandExecutor
     -> CommandExecutorSubmitter
     -> CommandExecutorDrainLoop
     -> CommandExecutorExecutionSupport
  -> YierdisFastCommandProcessor
  -> StringCommands / CoreConnectionCommands / ...
  -> CommandSupport
  -> DbReads / DbWrites
  -> YierdisStringOps / YierdisKeyspaceOps / ...
  -> YierdisDbMutationExecutor
  -> YierdisDbKeyLifecycle
  -> YierdisObject
  -> ReplyWriter
  -> JsonLineReplyWriter
```

先不要被类名吓到。真正读的时候，可以把它们分成 6 个阶段。

## 阶段 1：进程入口

入口文件是：

- [`apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServer.java`](../apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServer.java)

这个类很小，但它决定了启动的最外层行为。

### 它做了什么

`main(String[] args)` 的逻辑基本只有四步：

1. `ServerConfig.fromArgs(args)`
   解析命令行参数
2. `ForeignMemoryAutoModules.ensureFfmAvailable()`
   确保当前 JVM 支持 JDK 25 FFM
3. `YierdisServerBootstrap.start(config)`
   真正创建并启动 server
4. `server.awaitClose()`
   进程阻塞等待 server 关闭

### 初学者要注意什么

- `YierdisServer` 不是业务逻辑中心
- 它更像一个“外壳”，负责：
  - 启动前校验
  - 参数错误的稳定退出
  - 把真正的组装委托给 bootstrap

如果你在读代码时还没搞懂“server 到底是谁组起来的”，说明你还没真正进入下一层。

## 阶段 2：server 组装中心

组装中心在：

- [`apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`](../apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java)

这是整个主链里最重要的类之一。

### 它拥有什么

这个类长期持有并最终负责关闭的核心资源包括：

- `YierdisInstance`
- `CommandExecutor`
- command executor 对应的 `EventExecutorGroup`
- Netty `bossGroup`
- Netty `workerGroup`
- `serverChannel`
- 可选的定时 cleanup future

可以把它理解成：

- “把 DB、执行器、Netty 和生命周期都握在一起的总装类”

### `startInternal()` 的核心逻辑

最值得读的方法是 `startInternal()`。

它的步骤顺序大致是：

1. 再次检查 FFM 可用性
2. 根据 `runtimeConfig` 组出 `YierdisInstanceConfig`
3. `YierdisInstance.create(...)`
4. 从 instance 上拿到：
   - `runtimeAccess`
   - `maintenance`
   - `observability`
5. 创建 `NettyServerInfoProvider`
6. 创建 `SlowCommandGovernor`
7. 创建 `DefaultYierdisEngine`，由 engine 内部持有当前命令处理实现
8. 创建 `CommandExecutor`
9. 启动 executor，把 DB 绑定到 executor 线程
10. 如果开启了 cleanup interval，则调度 maintenance task
11. 组装 Netty `ServerBootstrap`
12. `bind(port)`

### 这里最值得注意的设计点

#### 1. DB 先于 Netty 就绪

bootstrap 先把 instance、command processor、executor 都组好，最后才真正 `bind`。

这意味着：

- 收到第一个请求时，核心执行路径已经完整存在
- Netty 不是去“边跑边找 DB”，而是把请求送给已经就绪的系统

#### 2. 命令执行器是 DB 的 owner thread

`executor.start()` 不是简单地启动一个线程，而是会在 executor 线程里先执行：

- `runtimeAccess::bindToCurrentThread`

也就是说：

- DB 从启动开始就绑定在命令执行器线程上
- 这就是 Redis 风格单线程命令语义的基础

#### 3. cleanup 不是独立 DB 线程

定时过期清理不是另外起一个“后台 DB 线程”，而是：

- 用 Netty worker event loop 做定时器
- 再通过 `executor.executeMaintenance(...)` 把真正的 maintenance 调度回 DB owner thread

这样可以避免：

- 多线程同时直接碰 DB
- cleanup 和正常命令并发修改同一份 DB 状态

## 阶段 3：实例级装配

实例装配中心在：

- [`libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`](../libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java)

### 这个类不要误解成什么

初学者最容易误解的是：

- “`YierdisInstance` 就是数据库”

其实更准确的理解是：

- “`YierdisInstance` 是一个实例级组装器和资源 owner”

它负责的不是单个 key 的读写，而是：

- 这个实例有几个逻辑 DB
- DB 共享还是独占 FFM runtime
- 是否需要全局 maxmemory governor
- 对外暴露哪些 runtime seam

### `create(...)` 做了什么

这个方法的代码逻辑值得逐段看：

1. 读配置里的 `databases`
2. 判断 `maxmemoryScope` 是 `GLOBAL` 还是 `PER_DB`
3. 创建实例级 `YierdisFfmMemoryRuntime`
4. 决定使用哪个 `DbEngineFactory`
5. 为每个 DB 创建一个 `RuntimeDbEngine`
6. 如果是全局 maxmemory，再创建 `YierdisGlobalMaxmemoryGovernor`
7. 把 governor attach 到每个 DB
8. 最后把 DB 数组、memory runtime 和 ownership 信息装进 `YierdisInstanceResources`

### 为什么这里要返回 `DbEngine[]`

`YierdisInstance.engines()` 暴露的是 `DbEngine[]`，不是 `YierdisDb[]`。

这是为了让上层看到的是能力视图，而不是实现类。

对初学者来说，可以这样记：

- `YierdisInstance` 是“多 DB 机房”
- `DbEngine` 是给命令层看的“统一控制面板”
- `YierdisDb` 是控制面板背后的具体机器

## 阶段 4：连接建立和 pipeline

入口在：

- [`apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`](../apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java)

### `initChannel(...)` 先做什么

它做的第一件事不是加 decoder，而是：

- `NettyExecutionConnection.getOrCreate(...)`

这一步非常关键，因为它把连接级状态先挂到了 `Channel` 上。

### 为什么先建 `NettyExecutionConnection`

因为后面的很多 handler 都需要共享同一份连接状态：

- `SELECT` 用的 session state
- `MULTI` 用的事务队列
- pending / pendingBytes
- 背压 enter / exit 计数
- closing 标记
- 公平调度的 channel state

如果没有这个根对象，各层会很容易各自挂一份状态，最后变成：

- 协议层一份
- 命令层一份
- 执行器一份

这个项目刻意避免了那种局面。

### pipeline 顺序为什么这么排

`initChannel(...)` 里的顺序是：

1. `writeBufferBackpressure`
2. `customRequestDecoder`
3. `protocolCommandAdapter`
4. `protocolErrorReply`
5. `commandHandler`

可以这样理解：

- 先监听 Netty 自己的出站写缓冲背压
- 再把网络 bytes 解成协议请求
- 再把协议请求适配成统一的 `ExecutionRequest`
- 如果是协议错误，统一交给错误回包 handler
- 最后才进入命令提交逻辑

所以 `YierdisFastCommandHandler` 的输入已经不是“任意对象”，而是命令层真正认识的 `ExecutionRequest`。

## 阶段 5：协议对象变成命令对象

桥接类是：

- [`libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty/ProtocolCommandAdapter.java`](../libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty/ProtocolCommandAdapter.java)

### 它做的事非常专一

它只做一件事：

- 把 protocol request 变成 `ByteArrayExecutionRequest`

如果输入是：

- `CustomProtocolV1ArgvRequest`

它就把每个参数取出来，转成 `byte[][] argv`，再包成 `ExecutionRequest`。

### 为什么这一层必须存在

因为项目的边界设计要求：

- protocol 层的 DTO 不直接漏进命令层
- 命令层统一基于 `ExecutionRequest`

对初学者来说，这一层很值得认真看，因为它是“分层真正落地”的最好例子：

- 上一层只懂协议
- 下一层只懂命令执行契约
- 中间有一个非常窄的翻译器

## 阶段 6：提交到 command executor

命令提交入口在：

- [`apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`](../apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java)

### `channelRead0(...)` 在做什么

这个 handler 也很克制。它不会自己执行命令，而是：

1. 调用 `nettyExecutor.trySubmitWithReason(ctx, msg)`
2. 如果成功：
   - executor 接管请求对象生命周期
   - handler 直接返回
3. 如果失败：
   - 构造一个 `ERR busy ...`
   - 立即写回
   - 自己负责关闭 /回收当前请求对象

### 这里为什么不直接执行命令

因为这仍然是在 I/O 线程里。

项目想保住的语义是：

- I/O 线程负责“把命令送进去”
- command executor 线程负责“真正碰 DB”

所以这里的职责只到“提交”为止。

## 阶段 7：队列、预算和背压

这部分的核心类有 4 个：

- [`CommandExecutor`](../libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java)
- [`CommandExecutorSubmitter`](../libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java)
- [`CommandExecutorDrainLoop`](../libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java)
- [`ExecutorBackpressureController`](../libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java)

### 先看 `CommandExecutor`

它不是单一算法类，而是一个“组装容器”。

它在构造时会把下面这些东西拼起来：

- `ExecutorTaskQueue`
- `ExecutorBacklogBudget`
- `ExecutorBackpressureController`
- `CommandExecutorExecutionSupport`
- `CommandExecutorSubmitter`
- `CommandExecutorDrainLoop`

也就是说：

- `CommandExecutor` 自己更像总控台
- 真正的入队、执行、恢复逻辑分散在各个协作者里

### 再看 `CommandExecutorSubmitter`

初学者读这部分时，可以带着一个固定问题：

- “请求在什么条件下会被拒绝？”

入队阶段主要检查：

- executor 是否还在运行
- 全局 queue slot 是否还有空位
- queued bytes 预算是否够

如果不够：

- 返回具体 reject reason
- 更新连接统计
- 关闭连接 `autoRead`

所以“背压”并不是执行阶段才发生，入队阶段就已经开始生效了。

### 然后看 `CommandExecutorDrainLoop`

这个类是在 command executor 线程上真正“取任务并执行”的地方。

每条命令的大致路径是：

1. 拿到 `CommandExecutorTask`
2. 检查 channel 是否 active 或 closing
3. 为本次执行分配出站 buffer
4. 创建 `ReplyWriter`
5. 调用 `executionSupport.executeCommand(...)`
6. 根据 `closeAfterReplyRequested()` 决定正常 flush 还是 flush 后 close
7. 最后释放请求对象和 backlog 预算

### 最后看 `ExecutorBackpressureController`

这个类本身不懂业务命令，它只是：

- 负责 enter backpressure
- 负责 exit backpressure
- 负责在合适时机重新开启 `autoRead`

所以对初学者最重要的理解是：

- `CommandExecutorSubmitter` 负责“发现压力”
- `CommandExecutorDrainLoop` 负责“释放压力后的回收”
- `ExecutorBackpressureController` 负责“把 enter / exit / recovery 变成统一策略”

## 阶段 8：真正执行命令

这一层的桥梁是：

- [`CommandExecutorExecutionSupport`](../libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java)

### 它做了哪几件关键事

1. 从 channel 上拿到 `EngineSession`
2. 创建或复用 `CommandContext`
3. 调 `YierdisEngine.execute(session, request, writer)`
4. 命令执行结束后释放 slot 和 bytes 预算
5. 在条件满足时恢复 `autoRead`

### 为什么 `CommandContext` 很关键

`CommandContext` 本身很简单，但它把命令执行所需要的两个入口固定下来：

- `session`
- `ReplyWriter out`

这让命令层可以保持 transport-agnostic：

- 想读连接态，看 `ctx.session()`
- 想写回包，看 `ctx.out()`

而不需要知道 Netty 的 `ChannelHandlerContext`。

## 阶段 9：命令分发

命令分发中心在：

- [`libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`](../libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java)

### 构造时它做了什么

构造阶段，它会把默认命令模块注册进 `CommandRegistry`，包括：

- `TransactionCommands`
- `CoreConnectionCommands`
- `KeyCommands`
- `StringCommands`
- `HllCommands`
- `ListCommands`
- `HashCommands`
- `SetCommands`
- `ZSetCommands`

server 额外命令则通过 `extraModules` 注入，例如：

- `ServerCommandModule`

### 执行时它做了什么

`execute(request, ctx)` 的大致顺序是：

1. 检查空命令 / 空 argv[0]
2. 检查不允许的 null bulk string
3. 看当前连接是否处于事务态
4. 如果事务 active，则决定：
   - 立即执行 `MULTI/EXEC/DISCARD`
   - 还是把命令入队并返回 `QUEUED`
5. 如果不是事务入队路径，则：
   - 查命令 spec
   - 调 handler
   - 捕获 wrong-type / command / OOM / 参数错误
   - 必要时发 change event

可以把它理解成：

- “命令执行的总闸门”

所有命令在真正落到 `*Commands` 之前，都会先过这道闸。

## 阶段 10：`SET` 的完整写路径

这是整条主链里最值得初学者认真看的部分。

### 第 1 层：`StringCommands.set(...)`

文件：

- [`libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`](../libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java)

它负责：

- 解释用户输入
- 解析 `NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL`
- 把这些选项转成命令层的稳定类型：
  - `SetMode`
  - `ExpireOption`

它不负责：

- 真正修改 DB 内存结构

### 第 2 层：`CommandSupport`

文件：

- [`libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`](../libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java)

这里最关键的一句是：

- `dbWrites(ctx)`

它做的事情是：

- 从 `CommandContext` 里拿到会话 DB index
- 交给 `YierdisDbRouter`
- 返回当前 DB 的 `DbWrites`

也就是说，命令层看到的不是具体 DB 类，而是当前 DB 的写能力视图。

### 第 3 层：`YierdisStringOps.set(...)`

文件：

- [`libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`](../libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java)

这是“真正开始把 `SET` 落成 mutation”的地方。

它的逻辑可以概括成：

1. 先算写入 upper bound
2. 构造一个 mutation plan
3. 把 plan 交给 `internals.executeMutation(...)`

初学者这里最该注意的是：

- `YierdisStringOps` 仍然不会直接裸写内存
- 它先把这次写操作包成一个受保护的 mutation plan

### 第 4 层：`YierdisDbMutationExecutor`

文件：

- [`libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java`](../libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java)

它的核心模板是：

1. `ledger.reserve(upperBoundBytes)`
2. 执行真正 mutation
3. `ledger.commit(...)`
4. 如果出错则 `rollback(...)`

这一层是：

- maxmemory
- OOM
- 回滚

这些行为真正收敛的地方。

### 第 5 层：`YierdisDbKeyLifecycle`

文件：

- [`libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`](../libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java)

它是 key 生命周期的统一入口。

在 `SET` 路径里，它会负责：

- 在 keyspace 上 `computeWithHandle(...)`
- 判断旧 key 是否已经过期
- 删除旧 TTL
- 设置新 TTL
- 释放旧 payload
- 调整 used bytes
- touch 对象做 LRU/last-access 更新

可以把它理解成：

- “所有和 key 生命周期有关的事情，都尽量在这里集中”

### 第 6 层：`YierdisObject`

文件：

- [`libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`](../libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java)

最终字符串并不是简单的 `byte[]`，而是一个：

- 有逻辑类型
- 有内部编码
- 有 payload 生命周期

的对象。

对字符串来说，常见编码有：

- `STRING_INT`
- `STRING_EMBSTR`
- `STRING_RAW`

如果启用了 off-heap allocator，payload 还可能被存进 `OffHeapBuf`。

### 为什么这条路径值得初学者反复看

因为它把这个项目的很多核心设计都串起来了：

- 命令层只做解释
- 路由层只做能力选择
- `*Ops` 负责把语义落成 mutation
- mutation executor 负责预算和回滚
- key lifecycle 负责 key / TTL / payload 生命周期
- value object 负责内部编码和真正数据表示

你一旦把 `SET` 这条链看明白了，再看别的写命令就会轻松很多。

## 阶段 11：回包写出

回包最终通过：

- `ReplyWriter`
- `JsonLineReplyWriter`

落成 NDJSON。

这里的关键理解是：

- 命令层不关心具体协议 JSON 怎么拼
- 命令层只调用 `out.simpleString(...)`、`out.integer(...)`、`out.bulkString(...)` 这类 API
- server 侧的 writer 再把这些语义编码成协议格式

这也是为什么项目反复强调：

- `ReplyWriter` 是 server write-back 的语义 authority

## 这条主链最适合配合哪些测试一起读

如果你准备真正边看源码边理解行为，最推荐的测试组合是：

### 1. 启动和整体接线

- [`apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`](../apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java)

看点：

- server 是否真的把核心命令和 server 命令一起接起来
- `HELLO/INFO/STATS/SET/GET/SELECT` 在真实 socket 下是否可用

### 2. 执行器行为

- [`libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`](../libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java)

看点：

- `maxDrainCommands`
- queued bytes budget
- `QUIT` 后跳过后续命令
- internal error 后 closing 行为

### 3. `SET` 和基础命令行为

- [`tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandProcessorTest.java`](../tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandProcessorTest.java)

看点：

- `SET/GET`
- `NX`
- `GET`
- `KEEPTTL`

### 4. 事务路径

- [`tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`](../tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java)

看点：

- `MULTI/EXEC/DISCARD`
- `QUEUED`
- 事务队列快照和重放

### 5. off-heap 字符串路径

- [`libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`](../libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java)

看点：

- `SET` 后 native bytes 增长
- `GET` 是否走 off-heap slice 读路径
- 删除或过期后内存是否释放

## 初学者最容易卡住的 5 个点

### 1. 以为 I/O 线程在直接执行命令

不是。I/O 线程只负责把请求送进 executor。

### 2. 以为 `YierdisInstance` 就是 DB

不是。它是实例级装配器和资源 owner。

### 3. 以为命令层直接调用 `YierdisDb`

不是。命令层只看 `DbEngine -> DbReads/DbWrites`。

### 4. 以为事务队列里存的是某种特殊 IR

不是。它存的是 `ExecutionRequest` 快照。

### 5. 以为 backpressure 只和单连接 pending 数有关

不是。它还和全局 slot、queued bytes 预算、channel writability 协作。

## 读完这篇后下一步看什么

- 如果你想把这条主链前半段的 request/reply 协议细节补齐，看 [`protocol-reference.md`](./protocol-reference.md)。
- 如果你想把命令分发和底层数据结构编码补齐，看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- 如果你想知道运行参数、观测命令和背压护栏怎么看，看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- 如果你想继续看“改需求时该怎么下手”，下一篇看 [`development-navigation.md`](./development-navigation.md)。
- 如果你想继续深挖 off-heap / FFM 在这条主链里是怎么工作的，看 [`ffm-usage.md`](./ffm-usage.md)。
