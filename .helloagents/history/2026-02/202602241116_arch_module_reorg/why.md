# Change Proposal: 架构模块重组（大拆分/重组模块，收敛边界与可测试性）

## Requirement Background

目前项目已经形成“SSOT 模块 Netty-free、Netty 只在 adapter/server/client/bench 出现”的总体方向（例如 `yierdis-protocol-model` / `yierdis-protocol-codec` / `yierdis-core` 目标 Netty-free）。但在深入阅读代码后，仍存在几类会持续放大维护成本与回归风险的架构问题：

1. **边界被 `db` 包类型污染：**`ops`/`command` 仍直接依赖 `yier.bubu.redis.db.*`（例如 `YierdisBytesView`、`ValueType`、`ScanCursorV2` 等），导致“命令语义/能力边界”无法稳定隔离具体 DB 实现演进。
2. **连接级状态对象过胖且跨线程语义隐含：**`yierdis-server` 的 `ServerConnectionState` 同时承载 session（SELECT/AUTH/MULTI）+ executor/backpressure（pending/pendingBytes/closing/autoRead）+ observability counters，I/O 线程与 executor 线程都可能触达同一对象，隐含竞态窗口与责任不清。
3. **背压/autoRead 控制来源多头：**pipeline `channelWritabilityChanged`、executor 队列水位、pendingBytes、global backlog budget 等都可能触发 autoRead 变更，形成“分布式状态机”，调参/排障成本高。
4. **架构护栏覆盖不均：**现有护栏（例如 `ArchitectureBoundaryTest`）偏向单点约束（仅扫描某些 import），对关键边界（ops↔db、command↔db.impl、Netty 渗透、模块依赖方向）缺少系统性编译期/测试期约束。
5. **off-heap unsafe 后端依赖 Netty internal：**`io.netty.util.internal.PlatformDependent` 被用于 raw memory allocate/free/copy（当前限制在 unsafe 后端内部，但仍带来升级与可移植性风险）。

为避免“新增能力时倒逼大改”，本变更选择 **一次性进行模块大拆分/重组**：用 Maven 模块边界形成编译期护栏，并将 executor/连接态/背压的复杂性重排到更可测、可替换的形态。

## Change Content

1. **引入新的 core API/model 模块并拆分 `yierdis-core`：**
   - 新增 `yierdis-core-api`：承载命令层/上层依赖的稳定接口（`DbEngine` + 子 ops + 稳定数据类型），禁止依赖 `yier.bubu.redis.db.*` 与 `io.netty.*`。
   - 新增 `yierdis-core-db`：承载具体 DB/存储实现（原 `yierdis-core` 的 `db/**`），实现 `DbEngine`，依赖 `offheap-api` 等底层模块。
   - 新增 `yierdis-core-command`：承载命令实现（原 `yierdis-core` 的 `command/**`），只依赖 `yierdis-core-api` + `yierdis-protocol-model`，禁止直接依赖 `core-db` 的实现包。
   - 新增 `yierdis-core-runtime`：承载 `YierdisInstance` 等装配与生命周期（原 `runtime/**`），依赖 `core-db` + `core-command`。
   - 保留 `yierdis-core` 作为兼容聚合（migration aggregator）：在迁移期聚合 `core-api` + `core-runtime`，便于 server/client 逐步切换依赖（类似 `yierdis-protocol` 的定位）。
2. **消除 `ops`/`command` 对 `db` 类型的直接依赖：**
   - 用 `yierdis-bytes` 的抽象替代 `YierdisBytesView` 的角色（引入 `BytesView` 或直接使用 `BytesSlice`/`BytesSource` + length），将 “key/view” 的最小能力收敛到中立模块。
   - 将 `ValueType`、`ScanCursorV2` 等对上层稳定的类型迁移到 `core-api`（或 `core-model` 子包）以避免 `ops`/`command` import `db` 包。
3. **把 executor 拆分为 core + Netty adapter：**
   - 新增 `yierdis-executor-core`（Netty-free）：只负责队列/预算/公平调度/背压决策，不直接触达 `Channel/ByteBuf/autoRead`。
   - 在 `yierdis-server` 中保留 Netty adapter：负责 `Channel` 生命周期、autoRead 控制、writability 事件接入与 reply 写回。
4. **拆分 server 连接态：**
   - 将 `ServerConnectionState` 拆为：`ServerSessionState`（仅 session/tx；仅 executor owner thread 访问）与 `ServerRuntimeState`（pending/backpressure/counters；原子字段，多线程可更新），并明确所有跨线程更新点。
5. **补齐“系统性护栏”与回归测试矩阵：**
   - Maven Enforcer / 禁止依赖规则 + ArchUnit（或等价）包级依赖方向检查。
   - 增加 backpressure/autoRead、closing/skip-side-effects、tx queue limit 等关键性质测试。
6. **off-heap unsafe 依赖隔离：**
   - 将 `PlatformDependent` 使用收敛到单一 façade（或拆分为可选 netty-unsafe bridge 模块），并提供可替换实现路径（优先 Foreign backend；unsafe backend 以最小 API 绑定）。

## Impact Scope

- **Modules:**
  - 新增：`yierdis-core-api`、`yierdis-core-db`、`yierdis-core-command`、`yierdis-core-runtime`、`yierdis-executor-core`
  - 调整：根 `pom.xml` modules、`yierdis-server` 依赖、`yierdis-client`/`yierdis-bench`（如涉及嵌入式/测试工具）、`yierdis-offheap-*`（仅隔离策略与测试）
- **Files:** 预计为多文件大范围改动（模块拆分、包迁移、依赖重写、测试/文档更新）
- **APIs:** 对外协议与命令语义不变；内部 API（core 依赖树/类型归属/构造入口）会产生迁移成本
- **Data:** 无持久化数据结构变更（仍为内存数据库）；需要关注内存口径/预算统计一致性

## Core Scenarios

### Requirement: R1 核心模块拆分与依赖方向强约束
<a id="r1"></a>
**Module:** Maven multi-module / core

通过 Maven 模块边界形成编译期护栏，确保依赖方向清晰可持续。

#### Scenario: S1 ops/command 不再 import `yier.bubu.redis.db.*`（编译期保证）
<a id="r1-s1"></a>
条件：`yierdis-core-api` 承载 ops + 稳定类型，`yierdis-core-command` 只依赖 `core-api`。
- 期望：`yierdis-core-api` / `yierdis-core-command` 编译期无法引用 `core-db` 的实现包
- 期望：server/client 依赖树更清晰（runtime 装配聚合由 `core-runtime` 提供）

#### Scenario: S2 `yierdis-core` 作为兼容聚合，迁移期保持对外入口稳定
<a id="r1-s2"></a>
条件：保留 `yierdis-core` 作为 migration aggregator。
- 期望：迁移期最小化对 server/client 的 pom 改动（逐步切换到新模块）

### Requirement: R2 连接态职责拆分与线程语义显式化
<a id="r2"></a>
**Module:** server

将 session/事务队列与背压/计数解耦，并明确哪些状态由 I/O 线程更新、哪些仅由 executor owner thread 访问。

#### Scenario: S1 closing/tx discard 不产生跨线程竞态与副作用
<a id="r2-s1"></a>
条件：异常触发 close 时，已入队任务应被回收但不再触发 DB side-effects。
- 期望：closing 语义一致且可测试（含 close-after-reply）
- 期望：事务队列清理不会与 enqueue/drain 并发冲突

### Requirement: R3 背压与 autoRead 控制收敛为单一状态机
<a id="r3"></a>
**Module:** server / executor

将背压触发源（队列水位、pendingBytes、channel writability、global budget）收敛到统一 controller，避免多头控制导致抖动。

#### Scenario: S1 autoRead 禁用/恢复路径可预测且可回归
<a id="r3-s1"></a>
条件：在不同触发源组合下（writability 变化、队列满、global backpressure）反复进出背压。
- 期望：autoRead 不会被错误启用（仍 unwritable 时不会开启）
- 期望：恢复策略不遗漏（具备稳定的全局恢复调度）

### Requirement: R4 架构护栏系统化（Maven/测试双保险）
<a id="r4"></a>
**Module:** build / tests

#### Scenario: S1 任意边界回退都在 CI 阶段 fail-fast
<a id="r4-s1"></a>
条件：开发者误引入不允许的依赖（例如 `core-api` 引用 `db`、`core-model` 引用 netty）。
- 期望：Maven enforcer 或 ArchUnit 在 `mvn test` 阶段直接失败，并输出可定位的违规文件/包

### Requirement: R5 off-heap unsafe 的 Netty internal 依赖隔离与可替换性
<a id="r5"></a>
**Module:** offheap

#### Scenario: S1 `PlatformDependent` 使用收敛且可被替换/审计
<a id="r5-s1"></a>
条件：unsafe 后端仍可用，但 Netty internal 的使用点高度集中。
- 期望：依赖升级影响面可控（仅后端内部 façade）
- 期望：Foreign backend 保持纯 JDK 路径作为长期替代

## Risk Assessment

- **Risk:** 模块拆分/包迁移导致大范围编译错误与回归（高风险、广影响）。  
  **Mitigation:** 以“每一步可编译、可回归”为原则；保留兼容聚合模块；任务拆解为可验证的增量；每阶段执行 `mvn test`。
- **Risk:** 背压/autoRead 状态机重排导致吞吐或延迟退化、甚至连接卡死。  
  **Mitigation:** 将 backpressure controller 逻辑单测化；加入压力/抖动回归；保留关键指标（enter/exit/reject）口径一致性测试。
- **Risk:** 连接态拆分引入新的竞态/资源泄漏（tx queue、pendingBytes 统计、close-after-reply）。  
  **Mitigation:** 明确线程归属；把跨线程写入限定在原子字段；为 closing/skip-side-effects 增加端到端集成测试。
