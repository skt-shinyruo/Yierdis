# Module Architecture

本文说明 Yierdis 的 Maven 模块是怎么拆的、依赖方向是什么、以及代码层和测试层如何一起守住这些边界。

## 一眼看懂的依赖图

可以先把项目看成“两条平行车道 + 一层最外组装”：

```text
yierdis-common-bytes
├─ yierdis-memory-api ──> yierdis-memory-ffm
│
├─ yierdis-db-api ──> yierdis-db-memory
│        ├──────────────> yierdis-server-runtime-api
│        ├──────────────> yierdis-memory-api
│        └──────────────> yierdis-memory-ffm
│
├─ yierdis-server-api ──> yierdis-server-core
│        └──────────────> yierdis-server-executor
│
├─ yierdis-command-api ──> yierdis-command-core
│        └──────────────> yierdis-command-builtin
│
├─ yierdis-networking-custom-v1
│        └──────────────> yierdis-networking-custom-v1-execution
│                         └──> yierdis-networking-netty
│
├─ yierdis-cli        -> yierdis-networking-custom-v1 + yierdis-networking-netty
├─ yierdis-benchmark  -> yierdis-networking-custom-v1 + yierdis-networking-netty
└─ yierdis-server-main -> server-api + db-api + command-api/core/builtin + server-core
                         + server-runtime/api + server-executor
                         + yierdis-networking-custom-v1-execution
                         + yierdis-networking-netty + yierdis-memory-ffm
```

理解这张图时，最重要的判断不是“谁依赖谁”，而是“谁负责线上协议、谁负责执行契约、谁负责 DB、谁负责最后的组装”。

## Package Ownership

Java package names now mirror module ownership. Active production package
families are:

- `yier.bubu.redis.app.server`
- `yier.bubu.redis.app.client`
- `yier.bubu.redis.app.bench`
- `yier.bubu.redis.execution.api`
- `yier.bubu.redis.execution.engine`
- `yier.bubu.redis.execution.executor`
- `yier.bubu.redis.storage.api`
- `yier.bubu.redis.storage.api.result`
- `yier.bubu.redis.storage.memory`
- `yier.bubu.redis.runtime.api`
- `yier.bubu.redis.runtime.embedded`
- `yier.bubu.redis.memory.api`
- `yier.bubu.redis.memory.foreign`
- `yier.bubu.redis.protocol.custom.v1.wire`
- `yier.bubu.redis.protocol.custom.v1.json`
- `yier.bubu.redis.protocol.custom.v1.reply`
- `yier.bubu.redis.protocol.custom.v1.execution`
- `yier.bubu.redis.protocol.custom.v1.netty`

## 聚合模块

下面几个模块主要是 parent / aggregator，本身不是运行时代码：

- `yierdis-parent`
- `yierdis-common`
- `yierdis-memory`
- `yierdis-networking`
- `yierdis-server`
- `yierdis-command`
- `yierdis-db`

它们的作用主要是：

- 统一模块结构
- 统一版本和构建配置
- 让依赖分组更容易理解

## bytes 基础层

### `yierdis-common-bytes`

提供中立的 bytes 抽象，被 protocol 和 core 两边共享。

它的角色是：

- 避免上层逻辑过早绑定到 Netty
- 为 `BytesView`、`BytesSlice`、`BytesSink` 这类接口提供公共基础

### `yierdis-networking-netty`

只负责把中立的 bytes 抽象接到 Netty。

它存在的意义是：

- Netty 适配层集中
- core / protocol 的非 Netty 代码不需要知道 `ByteBuf`

## protocol 车道

protocol 车道只负责“线上协议长什么样”，不负责命令执行语义。

### `yierdis-networking-custom-v1`

放 Custom Protocol v1 的 wire-only 内容，主要包名为 `yier.bubu.redis.protocol.custom.v1.wire.*`，并包含协议私有的 `json` / `reply` 子域：

- 协议侧 DTO
- `ProtocolLimits`
- JSON parser / writer
- request frame encoder / request payload parser
- reply parser / inspector
- 协议侧 reply model

它不依赖 execution、command、storage、runtime、server/app 或 Netty。

### `yierdis-networking-custom-v1-execution`

负责 `yier.bubu.redis.protocol.custom.v1.execution.*`：

- Custom Protocol v1 DTO -> `ExecutionRequest`
- `JsonLineReplyWriter`
- `JsonLineReplyWriterFactory`

它可以依赖 `yierdis-server-api` 和 `yierdis-networking-custom-v1`，但仍然是 Netty-free 的纯执行适配层。

### `yierdis-networking-netty`

负责 `yier.bubu.redis.protocol.custom.v1.netty.*`：

- Netty decoder
- Netty handler glue
- protocol error event -> Custom Protocol v1 reply 写回

它是 protocol 车道中唯一真正碰 Netty 的模块。它只把 Netty pipeline 接到 wire/adapter，不拥有命令解析、DB 访问或 runtime 组装。

## core 车道

core 车道负责“命令和 DB 怎么对话”，而不是“线上怎么发包”。

### `yierdis-server-api`

这是执行契约层，放的是 transport-agnostic 的命令执行语义对象（包名为 `yier.bubu.redis.execution.api.*`），例如：

- `ExecutionRequest`
- `ExecutionRecord`
- `ReplyWriter`
- `Session`

它的作用是把命令执行从 protocol DTO 里抽出来。

### `yierdis-db-api`

这是 command-facing DB 能力边界，包名为 `yier.bubu.redis.storage.api.*` 和 `yier.bubu.redis.storage.api.result.*`，放的是：

- `DbEngine`
- `DbReads`
- `DbWrites`
- `MemoryOps`
- `KeyHandle`
- 各种 `*ReadOps` / `*WriteOps`

它的意义是：

- 命令层只依赖稳定的能力接口
- 不直接依赖 `YierdisDb` 具体实现
- TTL / maxmemory 等 storage pressure path 可以通过 API 级 `KeyHandle` 表达 key identity

对初学者来说，可以把它理解成：

- `yierdis-db-api` 定义“命令层可以向 DB 提什么要求”
- `yierdis-db-memory` 决定“这些要求具体怎么完成”

### `yierdis-server-runtime-api`

这是 embedded runtime contract 边界，包名为 `yier.bubu.redis.runtime.api.*`，放的是：

- `YierdisInstanceConfig`
- `YierdisChangeEvent`
- `YierdisChangeSink`

它的意义是：

- server / embedded users 直接依赖实例配置 API
- command / storage implementation 通过 `runtime.api` 中的 change-tracking SPI 协作
- runtime API 源码只由 `yierdis-server-runtime-api` 拥有

### `yierdis-db-memory`

这是实际的 DB / storage 实现，包括：

- keyspace
- expires
- strings / hash / list / set / zset / HLL
- TTL
- maxmemory
- memory accounting
- internal `yier.bubu.redis.storage.memory.internal.key.KeyHandle`

这里是数据结构和存储策略最密集的模块。

`yierdis-db-memory` 里的 internal `KeyHandle` 实现 API 层的 `storage.api.KeyHandle`。热点 TTL cleanup 和 maxmemory eviction 走 handle API；heap `byte[]` 主要保留在协议边界、显式 materialization、测试和 client-facing 结果里。

### `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin`

这是传输无关的命令层。

它的职责是：

- 注册和分发命令
- 参数解析
- 把命令翻译成 `DbReads/DbWrites` 调用
- 不直接依赖 DB 实现

这个模块是整个架构里最刻意做“依赖倒置”的地方。

从代码逻辑上看，这三层的职责可以再具体化成：

- `YierdisFastCommandProcessor`
  位于 `yierdis-command-core`，负责命令注册、`CommandSpec<T>` 查找、事务前置判定、错误转换和最终分发
- `CommandSupport`
  位于 `yierdis-command-builtin`，负责把 `CommandContext` 里的会话态、DB 路由和参数解析 helper 收敛到一起
- 各个 `*Commands`
  位于 `yierdis-command-builtin`，负责具体命令的 typed parser、参数语义和回包语义

也就是说，命令层更像“翻译层”，而不是“存储实现层”。

生产命令注册已经收敛到 `CommandSpec<T>`：descriptor、parser、typed handler 和 MULTI policy 是一个整体。旧的 handler-only 注册和生产 `Command` 兼容执行路径不再是主路径。

### `yierdis-server-core`

这是 command execution kernel 边界，包名为 `yier.bubu.redis.execution.engine.*`。

它的职责是：

- 对外暴露唯一的命令执行入口 `YierdisEngine.execute(Session, ExecutionRequest, ReplyWriter)`
- 把 server 从 `YierdisFastCommandProcessor` 的具体构造里解耦出来
- 提供 `EngineSession`，承接连接级 DB index、transaction、client metadata 和认证状态
- 通过只读 `ConnectionStatsView` 支持 `INFO/STATS` 观测，而不拥有 executor 计数器
- 把定时 maintenance 入口也收敛到 engine 上

当前 `DefaultYierdisEngine` 仍然委托已有的 `YierdisFastCommandProcessor` 执行命令，也委托 runtime maintenance hook 做清理。

因此它不是新的命令实现层，而是把“谁是执行入口、谁创建命令上下文”固定下来：

- server 只调用 `YierdisEngine.execute(...)`
- executor 只接收 `engine::execute`
- maintenance 只调用 `engine.maintenanceTick()`

### `yierdis-server-runtime`

负责 runtime 级组装，而不是协议处理，包名为 `yier.bubu.redis.runtime.embedded.*`。

它做的事情包括：

- 多 DB 实例装配
- owner-thread runtime seam
- instance 生命周期
- global maxmemory 协调

它有意不重新承担 command processor 组装职责。

初学者如果只记一句话，可以记成：

- `yierdis-server-runtime` 负责“实例怎么活”
- `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin` 负责“命令怎么解释”

这两个模块在职责上是并排关系，不是上下级关系。

## 运行时 / 内存 / 调度辅助模块

### `yierdis-memory-api`

这是 off-heap contract 模块，当前兼容面包括：

- `OffHeapAllocator`
- `OffHeapBuf`
- `OffHeapSlice`
- `OffHeapOutOfMemoryException`

这些类型的包名是 `yier.bubu.redis.memory.api`；模块边界已经迁到 `yierdis-memory-api`。

需要使用这些 off-heap contract 的生产模块应该直接依赖 `yierdis-memory-api`。旧 API 聚合模块不重新导出这组类型，也不作为兼容桥。命令层不直接依赖这个模块；storage / DB 层负责把 off-heap backend 的失败转换成 `yierdis-db-api` 能表达的命令错误。

### `yierdis-memory-ffm`

这是 JDK 25 FFM backend，包名为 `yier.bubu.redis.memory.foreign.*`。

它为 `yierdis-db-memory` 提供：

- native memory runtime
- FFM allocator
- region / span / access 抽象

这里的角色是“内存底座”，不是“命令层的一部分”。

### `yierdis-server/yierdis-server-executor`

这是 Netty-free 的调度 / 背压算法层，包名为 `yier.bubu.redis.execution.executor.*`。

它抽出来的价值在于：

- 提交和背压决策逻辑不必散落在 server 里
- 算法层可以不和具体 I/O 实现绑死
- 它只知道 `Session`、`ExecutionRequest`、`ReplyWriter` 这些执行契约，不创建 `CommandContext`

它不拥有 selected DB、transaction queue、client name 或 authenticated 这类命令会话语义；这些状态属于 `EngineSession`。

## 最外层壳子

### `yierdis-server-main`

这是唯一真正把 protocol lane 和 core lane 拼起来的模块，包名为 `yier.bubu.redis.app.server.*`。

它负责：

- 进程入口
- server-local 参数转 runtime config
- Netty pipeline
- protocol request -> `ExecutionRequest` 的适配
- `NettyExecutionConnection` 的创建和 channel attr ownership
- command executor、engine 和 runtime seam 的对接
- server-facing command module

换句话说，server 是“组装层”，不是“把所有逻辑都重新实现一遍的层”。

从代码逻辑上看，server 模块最核心的价值是把两条车道接起来：

- protocol 车道产出协议请求对象
- `yierdis-networking-netty` 里的 `ProtocolCommandAdapter` 调用纯 execution adapter，把它变成 `ExecutionRequest`
- server 创建持有 `EngineSession` 的 `NettyExecutionConnection`
- server 把 `ExecutionRequest` 提交给 `CommandExecutor`
- executor 在 owner thread 上调用 `YierdisEngine`
- engine 再把请求交给当前命令处理实现
- reply 最后再通过 `JsonLineReplyWriter` 落回 Custom Protocol v1 NDJSON 输出

这也是为什么很多“看起来可以顺手改到 core 里”的事情，其实应该只在 server 做。

### `yierdis-cli`

client 的生产依赖主要走 protocol 车道，包名为 `yier.bubu.redis.app.client.*`，说明它的定位是：

- 会说 Custom Protocol v1 的客户端 / CLI
- 不是嵌入式 command/core 执行环境

### `yierdis-benchmark`

bench 主要依赖协议模块，包名为 `yier.bubu.redis.app.bench.*`，并在本模块内维护 server launch argv 模型，说明它在设计上更像：

- 外部压测工具
- 而不是直接嵌入 DB 内部的 benchmark harness

## 最重要的边界原则

下面几条是读代码和改代码时最值得记住的边界原则。

### 1. protocol 不等于 command contract

`ExecutionRequest` / `ReplyWriter` 由 `yierdis-server-api` 拥有，不在 `yierdis-networking-custom-v1`。旧 `core-contract` artifact 已从 active Maven 图中退休。

这意味着：

- protocol DTO 不是命令层的事实来源
- server 最终写回语义仍以 `ReplyWriter` 为准

### 2. `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin` 不能依赖 `yierdis-db-memory` 或 memory backend

命令层只能通过 `yierdis-db-api` 看 `DbEngine` / `DbReads` / `DbWrites`，不能直接看 `YierdisDb`，也不能 import `yier.bubu.redis.memory.api.*` 或依赖 `yierdis-memory-api`。

这样做的目的很明确：

- 命令层不和具体存储实现耦死
- DB 能力边界在 API 层稳定下来
- maxmemory / off-heap OOM 这类 storage pressure 错误先在 DB/API 边界转换，再由命令层按 `YierdisCommandException` 回包

### 3. custom-v1 adapter 才是 protocol -> execution 的桥

协议请求适配为 `ExecutionRequest` 的桥属于 `yierdis-networking-custom-v1-execution`；Netty handler 属于 `yierdis-networking-netty`；server 只负责把这些 adapter 接进 pipeline。

这让 protocol 车道和 core 车道可以各自演进，而不至于互相拖住。

### 4. runtime 负责实例生命周期，不负责命令组装

`YierdisInstance` 负责 DB 生命周期、owner-thread 协作和资源 ownership，但不重新承担 command processor 装配职责。

### 5. engine 是命令执行入口

server 不再直接构造 `YierdisFastCommandProcessor`，而是构造 `YierdisEngine`。

这条边界的意义是：

- command processor 的注册、事务、错误转换等细节不继续散落到 bootstrap
- maintenance 也通过同一个 engine 入口回到 runtime/storage
- `EngineSession` 承接 selected DB、transaction 和 client metadata
- `yierdis-server-executor` 只调度 `Session + ExecutionRequest + ReplyWriter`，不创建 `CommandContext`

如果把一次请求跨模块的过程写成最短路径，大致是：

1. `yierdis-networking-netty`
   把网络输入交给 decoder
2. `yierdis-networking-custom-v1-execution`
   把协议请求适配为 `ExecutionRequest`
3. `yierdis-server-main`
   创建 `NettyExecutionConnection` 并提交执行请求
4. `yierdis-server-executor`
   做排队、背压和 owner-thread 调度
5. `yierdis-server-core`
   接收 `Session + ExecutionRequest + ReplyWriter`，创建 command context
6. `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin`
   通过 `CommandSpec<T>` parse 后分发到 typed handler
7. `yierdis-db-api`
   通过 `DbReads/DbWrites` 暴露能力边界
8. `yierdis-db-memory`
   执行实际读写
9. `yierdis-networking-custom-v1-execution`
   通过 `JsonLineReplyWriter` 把结果编码回协议

初学者如果能把这 9 步记住，读整个仓库时就不容易迷路。

## 构建和测试层面的护栏

这些边界不只是文档约定，仓库里有明确的护栏。

### `yierdis-command-builtin` 硬依赖护栏

`yierdis-command/yierdis-command-builtin/pom.xml` 的边界由
`architecture-policy.yml` 和 `ArchitectureBoundaryTest` 持续校验，禁止：

- `yierdis-db-memory`
- `yierdis-memory-api`
- legacy networking/protocol model artifacts（例如 architecture policy 里的 yierdis-networking-model）
- `yierdis-networking-custom-v1`
- `yierdis-networking-custom-v1-execution`
- `yierdis-networking-netty`

这是硬依赖护栏；如果 POM 或源码重新引入这些实现侧依赖，架构测试会失败。

### `ArchitectureBoundaryTest`

这个测试会扫描源码，确保：

- `yierdis-db-memory` / `yierdis-db-api` / `yierdis-command-*` 不 import `yier.bubu.redis.protocol.*`
- `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin` 不依赖 `yierdis-memory-api`，也不 import `yier.bubu.redis.memory.api.*`
- `yierdis-db-api` 保持中立 contract 模块，不依赖 command、protocol、application/server、Netty、concrete storage implementation 或 `yierdis-memory-ffm`
- `yierdis-server-runtime-api` 保持中立 contract 模块，不依赖 command/storage implementation、protocol、application/server、Netty 或 `yierdis-memory-ffm`
- 旧 core 系列 artifact 不再出现在 active Maven 图
- `yierdis-memory-api` 保持中立 contract 模块，不依赖 command、storage implementation、protocol、runtime implementation、server/app、Netty 或 `yierdis-memory-ffm`
- `yierdis-networking-custom-v1` 不依赖 execution、command、storage、runtime、server/app 或 Netty
- `yierdis-networking-custom-v1-execution` 不依赖 command、storage、runtime、server/app 或 Netty
- `yierdis-networking-netty` 不依赖 command、storage、runtime 或 server/app
- bootstrap 不重新内联 owner-thread 生命周期逻辑
- request DTO 和 `ExecutionRequest` 的边界不被重新打穿
- server bootstrap 不绕过 `YierdisEngine` 直接接线 command processor 或 maintenance
- `YierdisEngine` 和 executor seam 继续暴露 `Session + ExecutionRequest + ReplyWriter`
- `yierdis-server-executor` 不重新拥有 `CommandContext`、selected DB 或 transaction state
- command parsing 不泄漏到 server/executor/runtime/protocol/storage
- TTL/maxmemory pressure path 继续使用 `KeyHandle`

这类测试的意义，不只是“防止依赖写错”，更是在保护整个阅读模型：

- 协议层看到的对象和命令层看到的对象必须继续分离
- runtime 必须继续只管生命周期
- server 必须继续只做组装，而不是把所有责任拿回去
- engine 必须继续是 command execution 的唯一入口
- executor 必须继续只是调度和背压，不变成命令语义层

### `ReplySsoTGuardTest`

这个测试保证：

- server/core 生产代码不把 `ReplyValue` 当回包 authority
- protocol reply model 不重新成为命令层回包语义的真相源

## 读模块架构时推荐的文件

- `README.md`
- `pom.xml`
- `yierdis-command/yierdis-command-builtin/pom.xml`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/custom/v1/ReplySsoTGuardTest.java`
- `yierdis-networking/yierdis-networking-custom-v1-execution/src/main/java/yier/bubu/redis/protocol/custom/v1/execution/CustomProtocolV1ExecutionAdapter.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty/ProtocolCommandAdapter.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`

## 一句话总结

Yierdis 的模块设计重点，不是“按包名分目录”，而是：

- protocol 负责线上协议
- `yierdis-server-api` / `yierdis-db-api` / `yierdis-server-runtime-api` 负责执行契约、DB 能力边界和 embedded runtime contract
- `yierdis-memory-api` 负责 off-heap contract 兼容面
- engine 负责统一命令执行入口
- `yierdis-db-memory` 负责真实存储
- `yierdis-server-runtime` 负责实例生命周期
- `yierdis-server-main` 负责最后的组装

如果你准备真正动手改功能，读完本文后建议继续看 [`development-navigation.md`](./development-navigation.md)。

如果你想继续理解 bytes 抽象为什么单独拆模块，以及它如何连接 protocol/off-heap/Netty 写回，再看 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## 面向初学者的模块阅读顺序

如果你想把模块和代码逻辑一起读懂，建议按这个顺序：

1. `yierdis-server-main`
   先知道项目怎么启动、怎么收请求
2. `yierdis-server-core`
   再知道 server 最终把请求交给哪个执行入口
3. `yierdis-command-api` / `yierdis-command-core` / `yierdis-command-builtin`
   再知道命令是怎么被解释和分发的
4. `yierdis-db-api`
   再知道命令层到底能向 DB 要什么能力
5. `yierdis-db-memory`
   最后再进入实际存储实现
6. `yierdis-server-runtime`
   回过头理解多 DB、owner thread 和实例级生命周期
7. `yierdis-networking-custom-v1` / `yierdis-networking-custom-v1-execution` / `yierdis-networking-netty`
   最后补协议细节和外部工具链

这样读会更符合“先看主路径，再看细节分层”的学习顺序。
