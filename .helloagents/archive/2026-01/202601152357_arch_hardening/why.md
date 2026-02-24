<!-- migrated_from: history/2026-01/202601152357_arch_hardening/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: 架构加固（Shutdown / Backlog Bytes / Split-package / Off-heap Capabilities / Version SSOT）

## Requirement Background

当前仓库整体分层清晰（`protocol`/`core` 作为 SSOT，`server`/`client`/`bench` 做装配），但在“长期演进 + 可维护性/可解释性”维度存在一些结构性风险，容易在高压场景或后续重构中放大：

1. **关停生命周期竞态风险**：服务端关停阶段缺少“停止接入 → drain 执行器 → 在 owner thread 关闭 DB → 等待 Netty 线程池退出”的强约束，存在并发访问/关闭后访问的竞态窗口。
2. **积压内存不可解释风险**：当前反压与队列上限主要以“命令条数”为单位，但命令可能携带极大 bulk（decoder 默认允许 64MiB），积压少量大包即可导致巨量 ByteBuf 驻留（且不受 `maxmemory` 约束）。
3. **split-package 边界风险**：`yierdis-protocol` 与 `yierdis-protocol-netty` 共享同一 Java package（`yier.bubu.redis.protocol`），削弱模块边界与可见性约束，未来做 JPMS/强封装也会更困难。
4. **off-heap 可插拔语义不一致**：存在“API 声称可插拔，但 core 侧对 unsafe 后端有显式 `instanceof` 认知”的耦合点，导致后端能力与行为不够一致/可解释。
5. **版本/文档漂移**：`HELLO` 等输出的版本信息与 Maven 版本、知识库描述存在漂移风险，长期会侵蚀 SSOT 的可信度。

本变更旨在提供一套“长期演进方案”，以较大但可控的重构代价，系统性消除上述问题，并把关键约束写入代码与知识库。

## Change Content

1. **优雅关停（Graceful Shutdown）链路闭环**
   - 明确服务端关停顺序：停止接入 → 停止读入（必要时）→ drain 执行器队列 → 在执行器线程内关闭 DB/allocator → 等待 Netty 线程池与 executor group 退出。
   - 为执行器提供可等待的关停契约（可观测、可测试）。

2. **按 bytes 的积压预算（Backlog Bytes Budget）**
   - 引入全局/连接级的 backlog bytes 预算（与现有“条数”并存，作为更可靠的第一约束）。
   - 使 decoder 上限（max bulk/max args/max line）与 backlog bytes 预算联动且可配置，避免“大包积压打穿内存”的不可解释行为。

3. **消除 split-package，强化边界（protocol-netty 迁出）**
   - 将 Netty codec/adapters 移出 `yier.bubu.redis.protocol` 包，迁移到独立包（建议：`yier.bubu.redis.protocol.netty`）。
   - 通过包名与依赖方向形成“额外护栏”，避免未来的包级可见性穿透。

4. **off-heap capabilities 抽象与 DB 集成点统一**
   - 在 `yierdis-offheap-api` 引入 capabilities / SPI（例如 DB 集成能力：keyspace/expires 是否可 off-heap、是否支持 direct write 等）。
   - `yierdis-core` 不再通过 `instanceof` 识别具体后端类型，而是通过能力接口获取可选集成点（unsafe/netty/foreign 的差异显式化）。

5. **Version SSOT 与知识库同步机制**
   - 将对外可见版本（HELLO、日志等）改为从构建元数据/资源注入读取，避免硬编码漂移。
   - 在完成代码改造后同步更新 `helloagents/wiki/*` 与 `helloagents/CHANGELOG.md`，保持“文档与代码一致”。

## Impact Scope

- **Modules:**
  - `yierdis-server`（关停顺序、执行器契约、decoder/executor 配置注入）
  - `yierdis-args`（新增/调整配置项，作为参数 SSOT）
  - `yierdis-protocol`（必要的协议抽象增强：例如 retained bytes/帧长度暴露、版本读取）
  - `yierdis-protocol-netty`（包迁移、codec 构造可配置、职责进一步收敛）
  - `yierdis-core`（移除对具体 off-heap 后端的 `instanceof` 耦合，统一 capabilities 接口）
  - `yierdis-offheap-api` / `yierdis-offheap-unsafe` / `yierdis-offheap-netty`（capabilities/SPI 与实现）
  - `helloagents/wiki/*`（文档同步）

- **APIs/CLI:**
  - server args 可能新增：bytes-based backlog 预算、decoder 上限配置等（以 `yierdis-args` 为 SSOT）。
  - Java package 迁移属于“源码级破坏性变更”（对仓库外部引用有潜在影响），需在知识库/变更说明中明确。

## Core Scenarios

### Requirement: 优雅关停（Graceful Shutdown）
**Module:** server
服务端在任意时刻收到关停信号，都应满足：
- 不出现“执行器线程仍在处理命令，但 DB/off-heap 已关闭”的竞态
- 队列中残留命令会被回收（frame/ByteBuf 不泄漏）
- 线程池可在可控时间内退出（可观测、可测试）

#### Scenario: in-flight backlog 下关停
条件：存在排队命令（含大 bulk），同时触发关停
- 预期：停止接入后 drain 完成再关闭 DB；无异常竞态；最终资源归零/回收可验证

### Requirement: Backlog Bytes Budget
**Module:** server/protocol-netty
对“命令积压”形成可解释、可控的预算机制：
- 全局 backlog bytes 与连接级 backlog bytes 达到阈值时触发 backpressure / fail-fast
- 与 decoder 的 max bulk/max args 等上限一致配合，避免大包内存驻留不可控

#### Scenario: 少量大包积压不应打穿内存
条件：客户端发送多个大 bulk 写命令并制造积压
- 预期：服务端按 bytes 预算拒绝/反压，内存驻留可解释，且不会无限增长

### Requirement: 消除 split-package（边界护栏）
**Module:** protocol/protocol-netty
协议 SSOT 与 Netty adapter 必须有清晰边界：
- `yierdis-protocol` 独占 `yier.bubu.redis.protocol` 包
- Netty codec/adapters 迁移到独立包（如 `...protocol.netty`），避免 split-package

#### Scenario: 构建与测试通过
条件：完成包迁移与引用更新
- 预期：`mvn test` 通过，且依赖方向约束仍成立（core/protocol 不依赖 netty）

### Requirement: off-heap capabilities 统一
**Module:** offheap/core
off-heap 后端差异应以“能力”显式化：
- core 通过 capabilities 获取可选 DB 集成点（keyspace/expires/string fast-path）
- unsafe/netty/foreign 的差异可解释，避免隐藏的 `instanceof` 分支扩散

#### Scenario: 不同后端行为可预测
条件：分别启动 `none/netty/unsafe/foreign`
- 预期：命令语义一致；能力差异仅体现在性能/内存路径，不体现在不可解释的行为漂移

### Requirement: Version SSOT
**Module:** protocol/args
对外版本输出必须与构建版本一致：
- `HELLO` 中 version 与 Maven 版本一致（或清晰说明差异，例如 `-SNAPSHOT`）
- 版本来源是构建元数据/资源注入，而不是硬编码常量

#### Scenario: HELLO 返回 version 可验证
条件：构建并运行 server，执行 `HELLO 2/3`
- 预期：返回的 version 可追溯、与构建一致

## Risk Assessment

- **风险：package 迁移带来破坏性变更**
  - 缓解：一次性完成仓库内全量引用更新；在知识库与 CHANGELOG 中记录迁移说明；必要时提供迁移脚本/说明。
- **风险：引入 bytes accounting 带来额外开销**
  - 缓解：只做 O(1) 计数与阈值判断；保持 hot path 避免额外分配；用基准/压测对比验证。
- **风险：capabilities/SPI 设计过度复杂**
  - 缓解：能力接口保持最小集合；先覆盖 “keyspace/expires/string” 三类关键集成点；其余能力后续迭代。

