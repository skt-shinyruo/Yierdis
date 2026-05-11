# Codebase Guide

本文是 Yierdis 代码库的导航页，目标不是替代 `README.md`，而是把“这个项目是什么、请求怎么跑、模块怎么拆、改需求时该看哪里”整理成一组更适合读代码时查阅的文档。

如果你是第一次进入仓库，建议按下面顺序阅读：

1. [`project-introduction.md`](./project-introduction.md)
2. [`project-overview.md`](./project-overview.md)
3. [`request-execution-flow.md`](./request-execution-flow.md)
4. [`main-path-walkthrough.md`](./main-path-walkthrough.md)
5. [`protocol-reference.md`](./protocol-reference.md)
6. [`commands-and-data-model.md`](./commands-and-data-model.md)
7. [`db-internals.md`](./db-internals.md)
8. [`executor-and-backpressure.md`](./executor-and-backpressure.md)
9. [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)
10. [`configuration-and-operations.md`](./configuration-and-operations.md)
11. [`client-and-bench-internals.md`](./client-and-bench-internals.md)
12. [`testing-and-debugging.md`](./testing-and-debugging.md)
13. [`glossary.md`](./glossary.md)
14. [`module-architecture.md`](./module-architecture.md)
15. [`development-navigation.md`](./development-navigation.md)

## 文档分工

### 代码库导览

- [`project-introduction.md`](./project-introduction.md)
  面向第一次进入仓库的读者，先从“项目为什么存在、想解决什么问题、为什么这样设计”建立整体认知。
- [`project-overview.md`](./project-overview.md)
  说明项目定位、能力边界、主要模块和代码入口，回答“这个项目到底在做什么”。
- [`request-execution-flow.md`](./request-execution-flow.md)
  说明从 Netty 收包到命令执行、DB 读写、回包写出的主链路，并用 `PING` 和 `SET` 做例子。
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)
  按源码阅读顺序，把 `YierdisServerBootstrap -> CommandExecutor -> YierdisEngine -> YierdisFastCommandProcessor -> YierdisStringOps` 这条主链逐段串起来。
- [`db-internals.md`](./db-internals.md)
  专门展开 `YierdisDb`、key lifecycle、mutation executor、memory ledger、TTL 和 maxmemory 在单 DB 内核里是如何协作的。
- [`executor-and-backpressure.md`](./executor-and-backpressure.md)
  专门展开执行器、队列预算、GLOBAL/FAIR 调度、连接级背压和 global recovery 的内部机制。
- [`module-architecture.md`](./module-architecture.md)
  说明 Maven 模块的职责、依赖方向和架构护栏，回答“哪些模块能依赖哪些模块”。
- [`development-navigation.md`](./development-navigation.md)
  面向改代码时的实际任务，回答“我要改 `SET` / 新增命令 / 改协议 / 看背压，该从哪几个文件开始”。

### 协议与数据

- [`protocol-reference.md`](./protocol-reference.md)
  说明 RESP request/reply、inline command、`HELLO 3`、协议错误断连和协议上限。
- [`commands-and-data-model.md`](./commands-and-data-model.md)
  说明命令模块怎么注册、命令家族怎么分、逻辑类型和内部编码怎么对应，以及 HLL 为什么复用 string。
- [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)
  说明 `BytesView`、`BytesSlice`、`BytesSink`、`DirectBytesSink` 为什么存在，以及它们如何连接 protocol、reply 写回和 off-heap fast-path。
- [`glossary.md`](./glossary.md)
  把仓库里反复出现的术语集中解释，帮助你把 protocol、command、db、runtime 这些层次对齐。

### 运行与贡献

- [`configuration-and-operations.md`](./configuration-and-operations.md)
  说明启动参数如何流入 runtime config，以及背压、淘汰、maintenance、观测命令在运行时是怎么工作的。
- [`client-and-bench-internals.md`](./client-and-bench-internals.md)
  说明 CLI、Netty client、bench、smoke/bench 脚本是如何沿着真实协议路径工作和验证 server 的。
- [`testing-and-debugging.md`](./testing-and-debugging.md)
  说明测试如何按层组织、改不同类型代码该跑哪些测试，以及常见故障应从哪一层排起。

### 现有 FFM / Off-Heap 文档

- [`ffm-beginner-guide.md`](./ffm-beginner-guide.md)
  面向第一次接触 FFM 的读者，重点是 JDK FFM 本身的心智模型。
- [`ffm-usage.md`](./ffm-usage.md)
  说明 FFM 在 Yierdis 里的实际落点和生命周期组装。
- [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)
  说明 heap / off-heap / direct buffer 之间哪些路径会发生拷贝。

## 这组文档的边界

- 快速启动、常用命令和脚本入口，仍建议先看仓库根部的 `README.md`。
- 本组文档会把协议、编码、配置和测试背后的代码逻辑讲得更细，但不替代 README 的“先跑起来”角色。
- FFM / native memory 的底层细节，仍以现有 `docs/project-docs/ffm-*.md` 和 `docs/project-docs/offheap-copy-behavior.md` 为主。
- 本组文档重点覆盖代码结构、执行流程、模块边界、协议细节、数据模型、DB 内核、执行器/背压、bytes 抽象、运行时配置和开发导航。

## 推荐阅读方式

- 如果你只想快速判断项目定位，先看 [`project-overview.md`](./project-overview.md)。
- 如果你还没建立“这个项目为什么值得读”的整体印象，先看 [`project-introduction.md`](./project-introduction.md)。
- 如果你准备跟踪一次请求的完整路径，先看 [`request-execution-flow.md`](./request-execution-flow.md)。
- 如果你想开始一边看文档一边跟源码，接着看 [`main-path-walkthrough.md`](./main-path-walkthrough.md)。
- 如果你还没真正看懂线上协议长什么样，接着看 [`protocol-reference.md`](./protocol-reference.md)。
- 如果你想把命令实现和内部编码对应起来，再看 [`commands-and-data-model.md`](./commands-and-data-model.md)。
- 如果你想继续看单 DB 内核内部怎么协作，再看 [`db-internals.md`](./db-internals.md)。
- 如果你想把执行器、队列预算和背压细节讲清楚，再看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。
- 如果你想理解 bytes 抽象为什么是协议层和 off-heap 之间的关键接缝，再看 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。
- 如果你准备启动、调参、看观测指标，看 [`configuration-and-operations.md`](./configuration-and-operations.md)。
- 如果你想知道 CLI、client 和 bench 是怎么沿真实协议路径工作的，看 [`client-and-bench-internals.md`](./client-and-bench-internals.md)。
- 如果你准备改完代码以后验证或排障，看 [`testing-and-debugging.md`](./testing-and-debugging.md)。
- 如果你总是被类名和术语绊住，随手配合 [`glossary.md`](./glossary.md) 一起看。
- 如果你准备改模块边界或新增依赖，先看 [`module-architecture.md`](./module-architecture.md)。
- 如果你已经准备动手改代码，直接看 [`development-navigation.md`](./development-navigation.md)。
