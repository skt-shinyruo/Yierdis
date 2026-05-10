# Project Introduction

本文是一篇面向初学者的“项目详细介绍”文档。

它和 [`project-overview.md`](./project-overview.md) 的区别是：

- 这篇更偏“这个项目为什么存在、想做什么、有哪些核心设计取舍、适合怎么读”
- `project-overview.md` 更偏“从代码出发建立整体心智模型”

如果你是第一次接触这个仓库，建议把本文当成第一站。

## 一句话先说清楚

Yierdis 是一个用 Java 25、Netty 和 JDK FFM 实现的单机内存 KV 服务端。它参考 Redis 的设计思路，但不追求 Redis 原生协议兼容，也不追求做一个生产级 Redis 替代品。

更准确地说，它是一个“Redis 风格系统”的教学样本、实验平台和工程化实现：

- 它实现了一个真正可运行的 TCP 服务端
- 它实现了 Redis 风格的数据结构和命令子集
- 它实现了 TTL、maxmemory、近似淘汰、连接级事务队列、背压和观测
- 它还把 JDK 25 FFM 当作默认的 native-memory 底座来使用

所以，理解这个项目最好的姿势不是：

- “这是 Java 写的 Redis”

而是：

- “这是一个用 Java 认真复刻 Redis 设计思想、同时保留清晰边界和教学价值的系统实现”

## 这个项目想解决什么问题

从仓库结构和代码设计来看，Yierdis 主要在解决下面几类问题。

### 1. 用 Java 重新讲一遍 Redis 风格系统是怎么工作的

很多人知道 Redis 的命令怎么用，但并不真正理解：

- 请求为什么通常不直接在 I/O 线程里改 DB
- 为什么内部会有 `int`、`embstr`、`intset`、`skiplist` 这类编码
- TTL、maxmemory、事务、背压这些机制到底如何协作

Yierdis 的价值之一，就是把这些机制拆成更容易读懂的 Java 代码。

### 2. 给“协议层 / 命令层 / DB 层 / runtime 层”做清晰分层

普通练手 KV 项目经常把所有东西塞进一个 server 类或一个大 Map 封装里，最后只能“跑起来”，很难继续演化。

Yierdis 明显不是这种思路。它故意把系统拆成：

- protocol lane
- command lane
- db lane
- runtime / executor lane
- server wiring lane

这让读者不只是看到“功能实现”，还能看到“边界设计”。

### 3. 把 FFM 当成真实的存储底座，而不只是一个语言特性演示

很多 FFM 示例停留在：

- 调一个 native function
- 分配一块 off-heap memory

Yierdis 更进一步。它把 FFM 用在：

- keyspace
- expires
- string / hash / list / set / zset / HLL 的部分内部结构

也就是说，FFM 在这里不是边角料，而是系统级设计的一部分。

## 这个项目明确不想解决什么问题

这部分很重要，因为它决定你读代码时应该带什么预期。

Yierdis 明确不是 Redis 的 drop-in replacement，也不打算覆盖完整的 Redis 能力面。当前明确不在范围内的内容包括：

- AOF / RDB 持久化
- 主从复制 / 集群
- Lua
- ACL / TLS
- PubSub
- 完整的 Redis 客户端生态兼容

这意味着：

- 你不能把它当作“换个协议就能上生产的 Redis”
- 你也不应该用“为什么它没有实现 Redis 全部功能”这种标准来评价它

它真正强调的是：

- 用相对可控的复杂度，把 Redis 风格系统里最值得学习的部分做出来

## 为什么它比一个普通内存 KV 服务更有意思

如果只是做一个“把 key 存到 Map 里，再支持 `SET/GET`”的服务，其实不需要这么多模块，也不需要这么多运行时机制。

Yierdis 更有意思的地方在于，它不只是一个 KV API 壳子，而是在复刻一套“小型数据库内核”的味道。

### 1. 它不是只有字符串，而是有多种 Redis 风格数据结构

当前实现覆盖了：

- string
- list
- hash
- set
- zset
- HLL
- bitmap 风格能力

这让命令层不再是简单 CRUD，而是开始接近数据结构服务器。

### 2. 它不是只关注“命令能跑”，还关注“内部编码怎么选”

例如：

- string：`int` / `embstr` / `raw`
- hash：packed / hashtable
- list：listpack / quicklist 风格
- set：intset / hashtable
- zset：packed / skiplist 风格

这说明它关注的不是“接口表面”，而是“值在内部应该长什么样”。

### 3. 它不是简单同步调用，而是有明确的线程模型

Yierdis 明确区分了：

- Netty I/O 线程：负责收包、解码、提交请求
- command executor 线程：负责串行执行命令和访问 DB

这就是它用来保留“Redis 风格单线程命令语义”的核心设计。

### 4. 它不是一味追求吞吐，而是显式实现了运行时护栏

例如：

- bounded queue
- queued-bytes budget
- 连接级 backpressure
- transaction queue limits
- protocol limits
- maxmemory 和淘汰策略

这类代码让它比“实验性质的小玩具”更接近真正的系统实现。

### 5. 它对边界很认真

这个项目不只是“文档里说分层”，还通过模块拆分和测试把边界守住，例如：

- `yierdis-command-builtin` 不直接依赖 `yierdis-db-memory`
- `protocol` 车道不变成命令语义的真相源
- `server` 只负责组装，不重新拿回所有责任

这让它对学习“如何设计代码库”也有价值。

## 你可以把整个项目看成 5 层

如果你只想先建立一张脑图，建议记住下面这 5 层：

```text
client / bench / tests
        |
server wiring
        |
protocol lane
        |
command lane
        |
db + runtime + memory
```

更具体一点：

### 1. protocol lane

负责“线上字节长什么样”：

- RESP request/reply
- RESP client codec / reply writer
- Netty decoder / protocol adapter

代表文件：

- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespClientCodec.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`

### 2. execution / command lane

负责“执行入口在哪里，以及命令怎么被注册、分发、校验和回包”：

- `YierdisEngine` / `DefaultYierdisEngine`
- `ExecutionRequest`
- `ReplyWriter`
- `YierdisFastCommandProcessor`
- 各个 `*Commands`

代表文件：

- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`

### 3. db lane

负责“真正的数据结构和读写语义”：

- `YierdisDb`
- `YierdisStringOps`
- `YierdisHashOps`
- `YierdisListOps`
- `YierdisSetOps`
- `YierdisZSetOps`

代表文件：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`

### 4. runtime / executor / memory

负责“实例如何存活、线程如何协作、资源如何约束”：

- `YierdisInstance`
- `CommandExecutor`
- maxmemory 协调
- maintenance
- backpressure

代表文件：

- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java`

### 5. server wiring

负责“把前面几层真的拼成一个能跑的 server”：

- 参数解析
- bootstrap
- Netty pipeline
- protocol -> execution bridge

代表文件：

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`

## 启动时到底发生了什么

如果你把项目当成一个真实服务端来看，最值得先理解的是启动过程。

大致顺序是：

1. `YierdisServer.main(...)` 读取 CLI 参数
2. `ServerConfig` 和 `YierdisServerRuntimeConfig` 完成参数归一化
3. `ForeignMemoryAutoModules.ensureFfmAvailable()` 检查当前 JVM 是否支持 JDK 25 FFM
4. `YierdisServerBootstrap.start(...)` 开始真正组装系统
5. 创建 `YierdisInstance`
6. 创建 `DefaultYierdisEngine`
7. 创建 `CommandExecutor`
8. 启动 executor，把 DB 绑定到 owner thread
9. 创建 Netty pipeline 并开始监听端口

这条链的关键意义是：

- server 并不是“先收请求，再临时找 DB”
- 它会先把命令执行、DB、runtime 和 pipeline 都装好，再真正对外服务

## 一条请求在系统里怎么流动

初学者可以先记住这条主链：

```text
socket bytes
  -> RespRequestDecoder
  -> RespCommandAdapter
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> CommandExecutorDrainLoop
  -> YierdisEngine
  -> YierdisFastCommandProcessor
  -> *Commands
  -> DbReads / DbWrites
  -> Yierdis*Ops
  -> ReplyWriter
  -> RespReplyWriter
```

这条路径体现了本项目几个最核心的思想：

- 协议层和命令层解耦
- I/O 线程不直接访问 DB
- 命令层通过能力接口而不是具体 DB 实现类工作
- 最终所有回包都统一收敛到 `ReplyWriter`

如果你接下来想顺着这条链继续读，直接看：

- [`request-execution-flow.md`](./request-execution-flow.md)
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)

## 本项目有哪些代表性运行时能力

如果你只从“能不能 `SET/GET`”来理解它，会低估这个仓库的价值。更值得注意的是下面这些运行时能力：

### 1. 多逻辑 DB

支持 `SELECT`，并通过 `YierdisInstance` 管理多个逻辑 DB。

### 2. TTL 和后台清理

不仅支持 TTL 命令，还实现了：

- 访问时惰性删除
- 轻量后台 cleanup tick

### 3. maxmemory 和近似淘汰

支持：

- `noeviction`
- `allkeys-random`
- `allkeys-lru`

虽然实现是简化版，但设计方向和学习价值都很明确。

### 4. 队列预算和背压

当请求积压时，系统不是“无限排队”，而是通过：

- queue capacity
- queued-bytes budget
- 连接级 autoRead 控制

来保护自己。

### 5. 可观测性

支持：

- `INFO`
- `INFO yierdis`
- `STATS`
- `MEMORY STATS`

这让它不仅能运行，还能被观察。

## 适合用什么心态读这个仓库

最合适的阅读心态是：

- 把它当作一个“小而认真”的 Redis 风格系统实现
- 关注它怎么分层、怎么约束复杂度、怎么把运行时机制接起来

不太合适的阅读心态是：

- 盯着“为什么没有实现 Redis 全家桶”
- 或者把它和纯教学 toy project 一样看待

因为这个项目真正的价值在于，它介于两者之间：

- 比 toy project 更完整
- 比生产级 Redis clone 更聚焦、更可读

## 哪些源码入口最值得先打开

如果你准备从文档切进源码，建议先看：

### 进程和启动

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

### 连接和协议入口

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`

### 执行器和回包

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`

### 命令与 DB

- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`

## 哪些测试最能帮助你建立信心

如果你不想一上来就啃实现类，建议先看这些测试：

- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
  看 server 是不是真的把命令、协议和 observability 接通了
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`
  看 RESP 协议错误如何返回错误并关闭连接
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java`
  看背压和 `ERR busy ...` 的行为
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandProcessorTest.java`
  看命令处理主流程
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  看边界并不只是文档约定，而是有测试护栏

## 推荐阅读顺序

如果你是第一次进入仓库，推荐顺序是：

1. [`project-introduction.md`](./project-introduction.md)
2. [`project-overview.md`](./project-overview.md)
3. [`request-execution-flow.md`](./request-execution-flow.md)
4. [`main-path-walkthrough.md`](./main-path-walkthrough.md)
5. [`protocol-reference.md`](./protocol-reference.md)
6. [`commands-and-data-model.md`](./commands-and-data-model.md)
7. [`configuration-and-operations.md`](./configuration-and-operations.md)
8. [`testing-and-debugging.md`](./testing-and-debugging.md)
9. [`glossary.md`](./glossary.md)
10. [`module-architecture.md`](./module-architecture.md)
11. [`development-navigation.md`](./development-navigation.md)

## 一句话总结

Yierdis 的核心价值不是“实现了多少命令”，而是：

- 它把 Redis 风格系统里最值得学习的那些设计问题
- 用比较清晰的 Java 模块和可运行的代码
- 真正落成了一个可以读、可以跑、可以验证的工程样本

如果你接下来准备顺着代码看“它到底是怎么做到的”，下一篇建议看 [`project-overview.md`](./project-overview.md)。
