<!-- migrated_from: history/2026-01/202601161128_arch_refactor/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: 架构重构（命令/DB 解耦 + 执行模型硬化 + 预算/背压可解释化）

## Requirement Background

Yierdis 当前代码定位“教学/演示 + 兼容 Redis 协议最小子集”，并在性能路径上引入了：Netty pipeline、零拷贝命令解码、单线程执行器、有界队列/背压、maxmemory/淘汰、以及 off-heap（含 Unsafe address capability）等机制。

随着功能与优化逐步叠加，系统逐渐暴露出以下结构性问题（并非单点 bug）：

1. **核心层职责集中且耦合偏高**：`YierdisDb` 与 `YierdisFastCommandProcessor` 作为“单文件承载大量职责”的集中点，会显著抬高新增命令/新类型/新策略时的修改半径与测试成本。
2. **线程模型依赖外部纪律**：单线程语义主要靠 server/执行器“正确使用”的约定维护，若未来出现绕过执行器或 DB 未绑定线程等误用，风险是竞态/一致性问题，且排查成本高。
3. **maxmemory 预算口径与真实内存存在结构性偏差**：目前属于“估算 + off-heap usedBytes”模型，但缺少可解释的分解与观测锚点，容易出现“为何 OOM/为何淘汰/为何 tail latency 抬高”难以解释的情况。
4. **off-heap/Unsafe 路线复杂度偏高**：raw address 能力天然接近 C 风格内存管理，收益与风险不对称；同时后端加载若依赖反射，错误会在运行期才暴露，影响可运维性。
5. **零拷贝解码 + 全局队列的驻留/公平性风险**：排队会延长 frame 生命周期；若 bytes 预算口径偏乐观或调度不公平，热点连接可能长期占用资源，连带影响其他连接，表现为“频繁 busy/不稳定延迟”。

本次改造目标是在不改变“单机内存版/教学定位”的前提下，引入更接近生产的工程边界：**硬约束（线程/生命周期）+ 模块化（可演进）+ 可解释（观测/预算）+ 可开关（性能特性）**。

## Change Content

1. **命令层模块化与可测试性提升**
   - 将命令执行从“单一巨大类”拆分为“注册表 + 上下文 + 领域命令集合”的结构，统一参数校验与错误映射策略。
2. **DB 单线程语义硬化（防误用）**
   - 将“必须在唯一线程访问 DB”的规则固化为 fail-fast 约束，消除“忘了 bind 仍可在任意线程访问”的灰区。
3. **maxmemory/淘汰/过期的预算可解释化**
   - 引入明确的预算分解输出（估算/实占），并补齐结构性大头（rehash/索引等）的估算项，使 OOM/evict 行为更可解释。
4. **off-heap 风险收敛**
   - 将高风险能力（keys/expires 的 off-heap）改为显式开关；后端加载改为启动期强校验（可提前失败、错误可读），并增强泄漏/关闭路径回归。
5. **零拷贝/队列驻留风险与公平性治理**
   - 将 queued/backpressure 的 bytes 口径升级为“更接近真实 retained bytes”，并在必要时支持 frame compaction（把大驻留体积降为精确 frame）。
   - 执行器调度引入“连接级公平性”（round-robin/限额），避免热点连接长期挤占全局 backlog。

## Impact Scope

- **Modules**
  - `yierdis-core`（command/db：拆分与硬化）
  - `yierdis-server`（handler/executor：强制单入口 + 公平调度 + compaction）
  - `yierdis-args`（新增/调整可配置项，保持参数 SSOT）
  - `yierdis-protocol` / `yierdis-protocol-netty`（retained bytes 口径与 frame 生命周期能力）
  - `yierdis-offheap-*`（后端发现/可用性校验、可选能力开关）
  - `helloagents/wiki/*`（同步架构与模块文档）

- **APIs**
  - 内部 API：`RespFrame` 增强“retained bytes”口径；命令层新增 registry/context；执行器新增调度策略接口（或等价结构）。
  - 对外命令：可选新增 `INFO`（最小实现，重点输出 memory 预算分解），或扩展 `MEMORY STATS`（二选一，见 how.md 的 ADR）。

- **Data**
  - 无持久化/无磁盘数据变更（仍为单机内存版）。

## Core Scenarios

### Requirement: 命令层模块化与可测试性
**Module:** yierdis-core (command)

#### Scenario: 新增命令/修改命令实现不触碰 DB/执行器
- 预期：新增命令只需在对应 domain 的 commands/handler 中实现并注册，不需要修改执行器/协议层。
- 预期：参数错误/语法错误/类型错误的错误码与文案风格保持一致。

#### Scenario: 低分配热路径保持可控
- 预期：在拆分后仍可复用 request-scoped scratch（bytes view/list slice 等），避免“拆分带来大量临时对象”。

### Requirement: DB 单线程语义硬化（防误用）
**Module:** yierdis-core (db) + yierdis-server (executor)

#### Scenario: DB 未绑定线程时访问立即失败
- 条件：DB 未调用 bind、或在非 owner 线程访问
- 预期：fail-fast 抛出明确异常（测试可覆盖），避免静默竞态风险。

#### Scenario: Server 侧无法绕过执行器直接访问 DB
- 条件：server pipeline 处理命令
- 预期：所有命令都通过执行器的单线程执行路径进入 DB；不存在“不走 executor 的 handler 构造路径”。

### Requirement: maxmemory 预算可解释与稳定
**Module:** yierdis-core (db) + yierdis-core (command)

#### Scenario: OOM / 淘汰行为可解释
- 预期：在触发拒写/淘汰时，能够输出预算分解（至少：heapEstimate / offheapUsed / keyspaceOverhead / expiresOverhead）。
- 预期：`MEMORY USAGE`/诊断命令能够清晰说明其为“估算”并给出分解口径。

### Requirement: off-heap 风险收敛（默认安全、可选增强）
**Module:** yierdis-offheap-* + yierdis-core (db)

#### Scenario: 默认 off-heap 只覆盖 value（低风险收益路径）
- 预期：即使启用 off-heap backend，默认仍使用 heap keyspace/expires；keys/expires 的 off-heap 必须显式开启。

#### Scenario: 后端不可用时启动期直接失败
- 预期：错误信息明确“缺失哪个模块/需要什么 profile”，避免运行中才抛异常。

### Requirement: 零拷贝/队列驻留风险与公平性治理
**Module:** yierdis-protocol(-netty) + yierdis-server (executor)

#### Scenario: bytes 预算口径更接近实际 retained memory
- 预期：执行器的 bytes 预算使用 retained bytes（尽量接近底层 buf 的真实占用），避免仅按 frame length 低估。

#### Scenario: 必要时支持 frame compaction，降低驻留体积
- 条件：frame 的 retained bytes 明显大于其逻辑长度
- 预期：执行器可将 frame 复制为“精确长度 buffer”并释放大底层 buf，降低驻留与抖动。

#### Scenario: 多连接公平调度，避免热点连接挤占
- 预期：在多个连接同时高并发时，不出现明显 starvation；热点连接不会长期独占全局 backlog。

## Risk Assessment

- **Risk:** 大规模重构导致协议/命令语义回归（兼容性风险）
  - **Mitigation:** 以现有单测为基线，新增回归测试（公平性/retained bytes/compaction/maxmemory 诊断），并以 `scripts/smoke.sh` 做端到端 smoke。
- **Risk:** 预算口径调整可能改变淘汰触发点（行为变化）
  - **Mitigation:** 保持“默认策略不变”，但提升解释性；必要时通过配置开关保留旧口径以对比。
- **Risk:** off-heap 能力开关与加载策略调整影响启动/运行方式
  - **Mitigation:** 在 how.md 中给出迁移与兼容策略，确保默认配置可直接运行。

