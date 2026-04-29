# Project Overview

本文从代码出发，说明 Yierdis 是什么，不是什么，以及读代码时最先应该建立哪些整体认知。

如果你还没有建立对项目整体目标、设计取舍和阅读姿势的第一印象，建议先看 [`project-introduction.md`](./project-introduction.md)。

## 一句话定位

Yierdis 是一个使用 Java 25 + Netty + JDK FFM 实现的单机内存 KV 服务端。它的目标不是做 Redis 的 drop-in replacement，而是参考 Redis 的设计与实现方式，做一个“Redis 风格”的教学 /实验 /工程化实现。

更准确地说，它同时在做四件事：

- 实现一个自定义协议的 TCP 服务端
- 实现一套 Redis 风格但刻意简化的命令层
- 实现一个支持多逻辑 DB、TTL、maxmemory、近似淘汰的数据引擎
- 用 JDK 25 `java.lang.foreign` FFM API 承载默认的 native-memory 路径

## 它不是什么

Yierdis 明确不打算成为 Redis 原生协议兼容服务，也不打算覆盖 Redis 的完整能力面。代码和 `README.md` 都清楚地把很多复杂能力排除在范围之外。

当前明确不做的内容包括：

- AOF / RDB 持久化
- 复制 / 集群
- Lua
- ACL / TLS
- PubSub
- 完整的 Redis 生态兼容

因此，理解这个项目时更合适的心态是：

- 把它当成一个“Redis 风格系统的实现练习与工程化样本”
- 而不是把它当成“Java 版 Redis”

## 技术和运行时特征

项目当前最重要的技术特征有四个：

### 1. 自定义协议，而不是 RESP

对外协议是 `Custom Protocol v1`：

- request：`<len>:<json>\n`
- reply：NDJSON

这意味着：

- 服务端协议层和命令层是解耦的
- 内置 CLI、bench、测试工具都围绕这套协议工作
- 不能直接拿 Redis 客户端来连

### 2. 命令执行保持单线程语义

Netty I/O 线程只负责收包和入队，真正访问 DB 的是单独的 command executor 线程。这是项目用来保持“Redis 风格单线程命令语义”的核心做法。

因此你看到的不是“整个 server 单线程”，而是：

- I/O 层多线程或单线程都可以
- DB 访问只在 owner thread 上发生

### 3. 默认把 FFM 当作统一的 native-memory substrate

这个项目对 FFM 的使用重点不是调用 native function，而是把它当成统一的 off-heap 存储底座来承载：

- keyspace
- expires
- string / hash / list / set / zset / HLL 的部分内部结构

如果你已经看过 `docs/ffm-usage.md`，会发现这里的核心关键词是：

- `YierdisFfmMemoryRuntime`
- `YierdisFfmKeyspace`
- `YierdisFfmExpireIndex`

### 4. 不只是实现命令，还在复刻 Redis 风格内部编码

Yierdis 的实现重点不仅是“命令能跑通”，还包括 Redis 风格的内部表示和升级路径。例如：

- string：`int` / `embstr` / `raw`
- hash：packed / hashtable
- list：listpack / quicklist 风格
- set：intset / hashtable
- zset：packed / skiplist 风格

这也是为什么很多代码会比“普通 KV 服务”更像数据库内核，而不是应用层 CRUD。

## 代码级主视角

如果你只保留一张脑图，建议记成下面四层：

### 1. protocol lane

负责“线上长什么样”：

- Custom Protocol v1 的 DTO
- JSON codec
- Netty decoder

### 2. core lane

负责“命令和 DB 怎么对话”：

- `ExecutionRequest`
- `ReplyWriter`
- `DbEngine`
- `DbReads` / `DbWrites`
- 命令注册和命令处理器

### 3. runtime / memory / executor

负责“实例怎么活、线程怎么协作、内存怎么约束”：

- 多 DB 组装
- owner thread 生命周期
- maxmemory 协调
- 有界队列和背压

### 4. server shell

负责把上面几层真的拼起来：

- 参数解析
- Netty pipeline
- 协议请求适配成 `ExecutionRequest`
- command executor 绑定到 runtime seam

## 从 `main()` 到一个可工作的 server

如果你是初学者，最值得先建立的一条具体代码逻辑是：

- `YierdisServer.main(...)` 不是自己启动 Netty，而是先把“启动需要的依赖”整理好
- `YierdisServerBootstrap.start(...)` 才是真正的组装中心

这条启动链的代码逻辑可以概括成：

1. `YierdisServer` 解析 CLI 参数，拿到 `ServerConfig`
2. 启动前调用 `ForeignMemoryAutoModules.ensureFfmAvailable()`，确保当前 JVM 支持 `java.lang.foreign`
3. `YierdisServerBootstrap` 把 `ServerConfig` 转成 runtime config
4. 基于 runtime config 创建 `YierdisInstance`
5. 基于 instance 和 server 观测信息创建 `DefaultYierdisEngine`
6. 基于 engine 创建 `CommandExecutor`
7. 启动 executor，把 DB 绑定到 executor 线程
8. 创建 Netty 的 boss / worker group 和 channel pipeline
9. `bind(port)` 后进入正常工作状态

初学者可以把这条链理解为：

- `YierdisServer` 负责“进程入口和错误处理”
- `YierdisServerBootstrap` 负责“真正把所有零件接起来”

这也是为什么想读懂“项目怎么跑起来”，通常先打开这两个文件就够了。

## `YierdisInstance` 在启动里到底做了什么

`YierdisInstance` 容易被误解成“DB 本身”，但它更像一个实例级装配器。

它真正做的事情包括：

- 决定逻辑 DB 数量
- 创建实例级的 `YierdisFfmMemoryRuntime`
- 根据 `maxmemoryScope` 决定 DB 是共享一个 runtime，还是每个 DB 各自持有 runtime
- 为每个 DB 创建 `RuntimeDbEngine`
- 在需要时创建全局 `YierdisGlobalMaxmemoryGovernor`
- 给各个 DB 挂上 `MaxmemoryCoordinator`
- 暴露 `engine(int)` 和 `engines()` 这种能力视图

换句话说：

- `YierdisDb` 关心“单个 DB 里的数据和策略”
- `YierdisInstance` 关心“整个实例有几个 DB，它们的资源怎么统一管理”

这对初学者很重要，因为它解释了为什么“多 DB”和“单 DB 内部数据结构”是分开放在不同模块和类里的。

## `YierdisDb` 不是一个大 Map，而是一个协作者集合

很多人第一次看到 `YierdisDb` 会以为它就是一个“大而全”的数据库类。实际上它更像一个状态 owner，加上一组高密度协作者。

在构造阶段，`YierdisDb` 会把下面这些东西拼起来：

- keyspace：负责 key -> object 的主索引
- expires：负责 key -> expireAt 的过期索引
- `YierdisStringOps`
- `YierdisHashOps`
- `YierdisListOps`
- `YierdisSetOps`
- `YierdisZSetOps`
- `YierdisHllOps`
- `YierdisTtlOps`
- `YierdisKeyspaceOps`
- `YierdisDbMemoryLedger`
- `YierdisDbMutationExecutor`
- `YierdisDbKeyLifecycle`

这里最值得记住的一个思路是：

- `YierdisDb` 不是把所有逻辑都直接写在自己里面
- 它更多是在做“对象图的 owner 和统一入口”

这也是为什么你在追踪一个具体命令时，最后常常会落到某个 `*Ops` 类，而不是一直停在 `YierdisDb` 本体里。

## `DbEngine` 视角为什么存在

初学者读命令层时，经常会问：

- 既然最后还是 `YierdisDb` 在干活，为什么不让命令层直接调用 `YierdisDb`？

答案是：项目故意把命令层依赖降到了 `DbEngine -> DbReads/DbWrites` 这个能力边界。

这带来三件事：

- 命令层只依赖“能做什么”，不依赖“怎么做”
- `YierdisDb` 可以继续拆内部协作者，而命令层不必跟着改
- 多 DB 路由只需要返回不同的 `DbEngine` 视图，而不是让命令层认识不同实现类

如果你是初学者，可以把它理解成：

- `DbEngine` 是 command 层看到的“数据库控制面板”
- `YierdisDb` 是这块控制面板背后的真正机器

## 推荐带着问题读代码

对于初学者，比起“把所有文件看一遍”，更有效的方法是带着下面这些问题读：

- server 是在哪一步把 DB、命令层和 Netty 拼起来的？
- 一条请求为什么不会直接在 I/O 线程里改 DB？
- 为什么 `StringCommands` 调的是 `DbWrites`，而不是 `YierdisDb`？
- `YierdisDb` 里的 keyspace、expires、memory ledger 各自负责什么？
- 多 DB 是怎么从连接态一路传到 DB 路由的？

如果你已经准备顺着一条真实请求往下追，下一篇应该看 [`request-execution-flow.md`](./request-execution-flow.md)。

## 主要入口

如果你第一次读代码，最值得先打开的入口如下：

### 进程和启动入口

- `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/YierdisServer.java`
- `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

### 请求主链路入口

- `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- `yierdis-protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/netty/ProtocolCommandAdapter.java`
- `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`

### 命令处理主入口

- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`

### DB 主入口

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

### runtime 主入口

- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

## 已有文档如何配合阅读

- 想先理解“项目在业务和系统层面是什么”，继续读本文即可。
- 想先从更完整的项目背景和设计取舍开始，看 [`project-introduction.md`](./project-introduction.md)。
- 想沿着一次请求走到底，看 [`request-execution-flow.md`](./request-execution-flow.md)。
- 想先把线上协议和回包格式讲清楚，看 [`protocol-reference.md`](./protocol-reference.md)。
- 想把命令层和内部编码对应起来，看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- 想知道启动参数、观测命令和运行时护栏怎么工作，看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- 想知道该跑哪些测试、怎么排障，看 [`testing-and-debugging.md`](./testing-and-debugging.md)。
- 想理解模块边界和依赖方向，看 [`module-architecture.md`](./module-architecture.md)。
- 想知道改需求时该从哪几个文件下手，看 [`development-navigation.md`](./development-navigation.md)。
- 想理解 FFM / off-heap 的底层路径，看现有的 `docs/ffm-usage.md` 与 `docs/offheap-copy-behavior.md`。

## 建议的第一轮阅读顺序

1. `README.md`
2. `docs/project-introduction.md`
3. `docs/project-overview.md`
4. `docs/request-execution-flow.md`
5. `docs/main-path-walkthrough.md`
6. `docs/protocol-reference.md`
7. `docs/commands-and-data-model.md`
8. `docs/configuration-and-operations.md`
9. `docs/testing-and-debugging.md`
10. `docs/glossary.md`
11. `docs/module-architecture.md`
12. `docs/development-navigation.md`

如果只是想快速知道“这个仓库值不值得继续深入”，读完本文通常已经够了。
