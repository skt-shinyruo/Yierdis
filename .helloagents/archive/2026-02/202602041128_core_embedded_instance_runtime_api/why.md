<!-- migrated_from: history/2026-02/202602041128_core_embedded_instance_runtime_api/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Core Embedded Instance Runtime API (Netty-free Instance SSOT)

## Requirement Background

当前项目的“实例级（instance-level）”语义主要由 `yierdis-server` 在 bootstrap 过程中完成装配：

1. **可嵌入（embedded）用法缺失**：如果希望在不启动 Netty 的情况下复用 DB + 命令处理器（例如 bench、工具、单元/集成测试、嵌入式应用），目前需要手工拼装 `YierdisDb[]`、路由、生命周期与 maxmemory 语义，容易出现重复实现与语义漂移。
2. **实例级职责分散**：多 DB + global maxmemory 目前通过 `YierdisDb.enableGlobalMaxmemory(...)` 注入内部协调器实现，但协调器定义在 `YierdisDb` 内部，导致实例语义与单 DB 引擎耦合，后续演进（Ledger/KeyHandle/Cursor/持久化/复制）容易牵动核心路径。
3. **生命周期与资源归属不清晰**：off-heap allocator 的 owner 语义、DB shutdown、全局协调器的共享状态缺少统一封装，增加泄漏与双计数风险。
4. **不利于“冻结 SSOT”**：我们希望把“实例级预算/淘汰/LRU 时钟/DB 列表”等稳定语义收敛为少量 SSOT 契约，server 仅负责执行与装配；否则未来很容易为了扩展/治理在多个模块同时改动，从而发生架构级大改。

本变更目标：在 `yierdis-core` 暴露一个 **Netty-free 的 embedded instance API**，并将实例级职责提炼为 core 的 runtime 语义层（SSOT），server 仅负责执行器绑定与运维治理（slowlog/metrics/限流/时间片）。

## Change Content

1. 在 `yierdis-core` 新增 runtime 子包，提供可嵌入的 `YierdisInstance` API（创建/关闭/DB 路由/命令处理装配）。
2. 将“实例级语义”从 `YierdisDb` 内部抽离为 runtime（global maxmemory / global LRU clock / shared allocator view），并保留兼容入口供 server 使用。
3. 统一生命周期：明确 allocator 的 owner 语义与 close 顺序，避免共享 allocator 场景重复 close 或 usedBytes 双计数。
4. 以最小回归锚点锁定行为：multi-db + global maxmemory + shared off-heap 只计一次；并确保与现有命令测试不回退。
5. 同步知识库：更新 `helloagents/wiki/arch.md`、`helloagents/wiki/modules/server.md`、`helloagents/wiki/modules/db.md`，把 instance/runtime 边界与 SSOT 写清楚，作为后续 Ledger/KeyHandle/Cursor 落地前置。

## Impact Scope

- **Modules:**
  - `yierdis-core`（新增 runtime API、抽离实例级协调逻辑）
  - `yierdis-server`（bootstrap 迁移为调用 core instance API 或使用兼容适配）
  - `helloagents/wiki`（架构与模块 SSOT 更新）
- **Files (high-level):**
  - `yierdis-core/src/main/java/yier/bubu/redis/runtime/*`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`（去耦/适配）
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`（装配迁移）
  - `yierdis-core/src/test/java/yier/bubu/redis/runtime/*`（回归锚点）
- **APIs:**
  - 新增：`YierdisInstance`（embedded instance API，Netty-free）
  - 保持：Redis 命令对外语义不变（目标：行为回归不回退）
- **Data:**
  - 仅内存结构与装配方式变化；无持久化存量数据兼容负担

## Core Scenarios

### Requirement: Embedded Instance API (Netty-free)
**Module:** core/runtime, command
提供可嵌入的 instance API，使 bench/工具/测试无需依赖 `yierdis-server`。

#### Scenario: Create instance and execute commands without server
条件：不启 Netty，仅在当前线程绑定 instance 并执行命令。
- 预期：可以创建 `YierdisInstance`（N DB 可选），得到 `YierdisFastCommandProcessor` 或 `YierdisDbRouter` 并执行命令；关闭 instance 后资源可回收。

### Requirement: Instance Responsibilities SSOT (global maxmemory / LRU clock)
**Module:** core/runtime, db
将实例级预算与全局时钟作为 runtime 的单一权威（SSOT），避免分散在 DB 与 server 装配中。

#### Scenario: Global maxmemory counts shared off-heap once across DBs
条件：multi-db + `maxmemoryScope=GLOBAL` + shared off-heap allocator。
- 预期：maxmemory 预算口径中 off-heap used bytes **只计一次**；淘汰与拒写语义稳定且可回归。

### Requirement: Backward Compatibility for server wiring
**Module:** server
server 依赖 core（单向依赖），并能在不引入 DB 并发语义的前提下迁移到 instance API。

#### Scenario: Server bootstrap uses core instance API without behavior regression
条件：现有 `YierdisServerBootstrap` 仍负责 Netty/执行器/定时任务，但 DB 装配走 core 的 instance API。
- 预期：现有测试不回退；启动参数语义保持一致（per-db / global maxmemory 行为不变）。

## Risk Assessment

- **Risk:** 新增 `YierdisInstance` 形成新的稳定 API 面，未来修改成本更高。  
  **Mitigation:** API 设计保持最小、明确分层：runtime 只包含语义模型与装配，不包含线程/网络/metrics；server 层负责可变治理策略。
- **Risk:** 抽离 global maxmemory 协调逻辑可能引入行为回归（淘汰策略、拒写点、LRU 时钟）。  
  **Mitigation:** 以现有 `MaxmemoryEvictionTest`/`MaxmemoryDoubleReplyRegressionTest` 为回归底座，新增 multi-db + shared allocator 锚点测试，优先锁定语义再优化实现。
- **Risk:** allocator owner/close 语义调整可能导致 off-heap 泄漏或 double-close。  
  **Mitigation:** 明确 owner 标记与 close 顺序；复用现有 off-heap 泄漏回归锚点（`allocator.usedBytes()`）并补充 instance close 回归用例。
