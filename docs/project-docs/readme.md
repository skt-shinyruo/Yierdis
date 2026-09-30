# 代码库文档地图

Yierdis 的内部代码库文档地图如下。根目录 `README.md` 覆盖项目定位、环境、构建和启动；本目录的文档面向要研读源码与参与系统演进的工程师，按技术主题给出代码级的深度说明。

---

## 一、按目标选择阅读路径

### 路径 1：第一轮通读（建立全局骨架认知，约 5 篇）
1. [`readme.md`](./readme.md)（本文）— 建立文档全景索引与模块分类心智。
2. [`project-overview.md`](./project-overview.md) — 了解系统定位、9 大模块依赖拓扑与 12 个核心入口。
3. [`request-execution-flow.md`](./request-execution-flow.md) — 掌握一次请求从 Netty Socket 到 Reply 的端到端完整时序与 Netty 适配层。
4. [`native-memory-runtime.md`](./native-memory-runtime.md) — 掌握 JDK 25 FFM 堆外内存架构、JVM 约束推导与 Region 运行时。
5. [`configuration-and-operations.md`](./configuration-and-operations.md) — 掌握启动配置、运维调优、生产容量加固与关闭验收。

### 路径 2：进阶源码精读与自我测试（强烈推荐）
- 直接研读 **[`source-reading-guide.md`](./source-reading-guide.md)**：包含四阶段递进研读路线图、三大核心链路穿透指引，以及 **22 个深度架构思考题**（附思考线索）。

### 路径 3：功能改造与定位排障
1. 先根据下文的「六大技术领域文档矩阵」定位要修改的模块。
2. 读 [`development-navigation.md`](./development-navigation.md)：把改动类型精确映射到受影响的类、文件与影响半径。
3. 读 [`testing-and-debugging.md`](./testing-and-debugging.md)：确认运行哪些测试分级套件（Unit / Smoke / Stress）以及排障手段。
4. 动协议/命令/DB 前，先研读对应领域的专题文档，严禁仅修改浅层表现。

### 路径 4：请求链路深度穿透（追踪单条命令时序）
1. [`request-execution-flow.md`](./request-execution-flow.md)：主链、最短路径、错误分支与线程切换点。
2. [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)：`CommandDispatcher` 预检派发与 Session/Router 进程内委托。
3. [`transaction-and-replay.md`](./transaction-and-replay.md)：`MULTI`/`EXEC` 的排队与 Replay 状态机。
4. [`executor-and-backpressure.md`](./executor-and-backpressure.md)：单线程执行器提交预算与 Netty 背压的精确口径。

---

## 二、六大核心技术领域文档矩阵（共 21 篇）

本目录全量技术文档已收敛为 21 篇高内聚的核心技术文档，按 6 大领域分工如下：

### 1. 导读、索引与规范（Entrypoints & Specifications）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`readme.md`](./readme.md) | **文档全景地图**：分类矩阵、阅读路径与维护约定（本文） | — |
| [`source-reading-guide.md`](./source-reading-guide.md) | **源码精读指南**：保姆级四阶段精读路线与 22 个架构思考题 | 全工程核心类 |
| [`project-overview.md`](./project-overview.md) | **系统概览与模块架构**：能力边界、9 大模块依赖拓扑与 leaf POM 规则 | `YierdisServerBootstrap`, 各 leaf POM |
| [`glossary.md`](./glossary.md) | **术语字典**：集中解释高频概念与代码术语映射 | 见词条索引 |

### 2. 系统架构主线与请求生命周期（Architecture & Execution Flow）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`request-execution-flow.md`](./request-execution-flow.md) | **请求全生命周期与 Netty 适配**：端到端时序、Netty 7 大 Handler 装配、有界写回与背压 | `CommandExecutor`, `RedisReplyRenderer`, `NettyServerChannelInitializer` |

### 3. RESP 协议与命令执行系统（Protocol & Command Engine）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`protocol-reference.md`](./protocol-reference.md) | RESP2/3 协议支持矩阵、Inline 限制与解码防卫 | `RespRequestDecoder`, `RespReplyWriter` |
| [`commands-and-data-model.md`](./commands-and-data-model.md) | 基础命令集契约、类型系统与内存数据模型映射 | `CommandSpec`, `YierdisDb` |
| [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md) | **命令分发核与委托契约**：预检派发与 Session/Router/Provider 进程内委托 | `CommandDispatcher`, `PreparedCommand`, `YierdisDbRouter` |
| [`transaction-and-replay.md`](./transaction-and-replay.md) | `MULTI`/`EXEC` 事务状态机、排队暂存与原子 Replay | `TransactionState`, `ExecutionQueue` |

### 4. 存储引擎内核与 JDK 25 FFM 堆外内存（Storage & Native Memory）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`db-internals.md`](./db-internals.md) | DB 内部结构：键空间、开放寻址表、墓碑压缩与两阶段提交 | `NativeKeyDirectory`, `EntryTable` |
| [`db-design-analysis.md`](./db-design-analysis.md) | 存储引擎深度设计推导：为什么这样设计、代价与取舍 | `YierdisDbKernel`, `MutationPlan` |
| [`native-memory-runtime.md`](./native-memory-runtime.md) | **堆外内存全景**：JVM 约束推导、C 对照、JDK 25 FFM 原语与 Region 运行时 | `YierdisFfmMemoryRuntime`, `YierdisNativeObjectTable`, `Arena` |
| [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | 物理分配器：Slab/Size-Class 分级管理与 Stable Handle 间接寻址 | `YierdisNativePageAllocator`, `StableMemoryHandle` |
| [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) | **拷贝行为与内核边界**：全链路 copy 场景、拷贝成本拆解与系统调用边界判定 | `BytesSlice`, `MemorySegment.copy` |

### 5. 资源治理、流控与客户端（Governance, Flow Control & Clients）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`executor-and-backpressure.md`](./executor-and-backpressure.md) | 单线程执行器容量口径、入队预算与 Netty 双向背压流控 | `CommandExecutor`, `SubmissionBudget` |
| [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) | **内存管理与淘汰**：TTL 过期生命周期、ExpiresIndex、物理限额与驱逐算法 | `EvictionPool`, `MemoryLedger`, `ExpiresIndex` |
| [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) | 字节抽象（`BytesView`/`BytesSlice`）、ASCII 快速路径与零拷贝优化 | `BytesSlice`, `NativeBytesSlice` |
| [`client-and-bench-internals.md`](./client-and-bench-internals.md) | **客户端与压测引擎**：C vs Java 客户端选型、内置 CLI 与双压测引擎实现 | `YierdisCli`, `NioBenchmarkClient`, `BenchmarkRunner` |

### 6. 工程运维、安全加固与测试排障（Operations, Hardening & QA）
| 文档 | 核心定位与职责 | 核心关联类与入口 |
|---|---|---|
| [`configuration-and-operations.md`](./configuration-and-operations.md) | **配置、运维与生产加固**：全量参数字典、硬容量准入、Result-Unknown、优雅停机与验收 | `ServerConfig`, `yierdis.conf`, `YierdisServerRuntimeConfig` |
| [`development-navigation.md`](./development-navigation.md) | 代码修改导航：将改动类型精确映射到代码类与验证范围 | — |
| [`testing-and-debugging.md`](./testing-and-debugging.md) | 测试分级矩阵、排障诊断工具集与回归质量防线 | `yierdis-tests` |
| [`docs/adr/`](../adr/) | 架构决策记录（如 [`0001` server 启动改为单一配置文件入口](../adr/0001-config-file-startup.md)） | — |

---

## 三、核心命令链路基准（全系统唯一权威模型）

所有涉及命令处理与执行的专题文档，均必须以当前唯一真实链路为准：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

- **事务 Preflight**：事务 queueable 命令在 `parse` 阶段做预检，`EXEC` Replay 接管子 `PreparedCommand` 和 retained request 的所有权。
- **流式消费**：语义流式 source 由对应 `PreparedCommand` 持有，Renderer 同步消费结果后再由 Executor 关闭；`QUIT` 通过 `CommandResult` 表达 reply 后触发关闭。
- **职责隔离**：`EngineSession` 仅拥有连接 Session 状态；普通 Command Handler **绝不直接使用** `RedisReplyWriter`，业务结果统一由 Renderer 写出，Executor/Ingress 控制路径直接处理协议错误与终止回复。

---

## 四、文档维护与演进约定

1. **真实性第一**：文档中的类名、方法名、常量、配置项和文件路径必须能在仓库源码中直接找到并保持一致；重构源码时必须同步更新文档。
2. **链接规范**：相对链接保持 `[文字](./xxx.md)` 或 `[文字](./xxx.md#锚点)` 格式，确保在 GitHub 与本地 Markdown 预览中均能顺畅跳转。
3. **论证充分**：描述“为什么”时必须给出源码依据、性能数据或反面对比，严禁只下结论而不给出论据。