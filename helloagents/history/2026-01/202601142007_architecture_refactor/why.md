# Change Proposal: architecture_refactor

## Requirement Background

当前代码库的定位是“教学/实验导向的 Redis 风格服务端”，以 **Java 17 + Netty** 为基础，并提供可选的 off-heap 后端与可重复压测工具。随着功能演进，架构在以下方面出现了系统性问题，已经开始影响正确性一致性、维护成本与扩展效率：

- **模块边界不清晰**：`yierdis-server` 同时承载 server/runtime、client、RESP 协议、命令处理、DB 与数据结构，导致依赖方向不稳定、复用困难、改动牵一发而动全身。
- **配置/参数体系分散**：server、bench、脚本维护各自参数解析与默认值，容易出现“参数漂移”，也不利于 fail-fast 校验与一致性测试。
- **命令执行/反压路径存在重复实现与一致性风险**：存在多套 executor/队列/drain/backpressure 思路，行为差异会放大排障成本。
- **协议链路不对称**：server 侧偏向手写写出路径（`RespWriter`），client/测试走 `RespEncoder/RespDecoder`，bench 侧又自实现 RESP 写入/校验，长期会引入 subtle incompatibility。
- **off-heap API 与 Netty 耦合**：off-heap 抽象层直接依赖 `ByteBuf`，导致可移植性差，且加深“存储/协议/网络库”之间的耦合。

本提案的目标是：在不脱离项目教学属性的前提下，建立可持续演进的模块化结构与 SSOT（Single Source of Truth）原则，降低一致性风险，并为后续优化（性能/RESP3 子集扩展/off-heap 后端）提供清晰边界。

## Product Analysis

### Target Users and Scenarios

- 希望理解 Redis 内部结构与网络协议的学习者
- 需要一个可控、可重复的实验平台进行性能/淘汰/TTL/off-heap 研究的工程师
- 需要通过测试与 bench 快速验证新命令或存储结构实现的贡献者

### Value Proposition and Success Metrics

价值主张（Value Proposition）：

- **一致性**：server/client/bench 在 RESP 语义与参数语义上保持一致，减少“测的不是同一个东西”的风险。
- **可维护性**：清晰的模块边界与依赖单向，使得改动影响范围可预测。
- **可扩展性**：新增命令/新增 RESP3 子集/新增 off-heap 后端时，有明确落点与接口。

成功标准（Success Metrics）：

- `mvn test` 在全模块通过，且新增/增强的 round-trip 与兼容性测试覆盖关键路径
- bench 与 server 在关键参数（maxmemory/offheap/backpressure/executor 等）的语义一致，并能在 `--help` 中被清晰呈现
- 主要模块的依赖方向可用简单规则表述（例如：`*-server` 仅依赖 `*-core`/`*-protocol` 等），并可在 CI 中做基础校验（可选）

### Humanistic Care

- 将“复杂系统的可解释性”作为第一原则：把隐含行为（淘汰、过期、反压、off-heap OOM）显式化为可配置、可测试、可文档化的规则
- 降低贡献门槛：减少同一概念在不同模块里出现多个实现（减少学习成本与误用风险）

## Change Content

核心变更思路（High-level）：

1. **模块化拆分与依赖单向**
   - 抽取 `protocol`（RESP 模型 + codec SSOT）、`core`（DB/命令处理/淘汰/过期/存储结构）等基础模块
   - 将 Netty 集成保留在 `server` / `client`（或 netty-adapter）层，避免网络库侵入核心逻辑
2. **配置与参数 SSOT**
   - 引入统一的 args/配置解析方案（允许少量三方依赖），并将关键参数模型化（枚举/单位/约束）
3. **执行器与反压 SSOT**
   - 收敛 executor 实现，明确队列、drain、反压、水位线、时间预算的语义，并由测试兜底
4. **协议链路一致化**
   - server/client/bench 共用 RESP 编解码核心；bench 允许保留 raw socket，但编码/解码语义来自同一实现
5. **off-heap API 去 Netty 依赖**
   - off-heap 抽象层使用自定义字节输出接口或 JDK 类型（`byte[]/ByteBuffer/OutputStream`），Netty 适配在独立模块中提供

## Impact Scope

影响范围（预期会修改/新增）：

- Maven 模块结构：根 `pom.xml` 与各子模块 `pom.xml`
- 代码目录与包结构：`yierdis-server` 内的 `protocol`/`client`/`db`/`command` 等可能被迁移
- bench：从“自实现 RESP”升级为“复用 codec”，并与 server 参数语义对齐
- off-heap：`yierdis-offheap-api` 的接口变更与后端适配调整
- 文档：`helloagents/wiki/arch.md`、`helloagents/wiki/modules/*` 与 `README.md`（在落地实现阶段同步）

## Core Scenarios

### Requirement: module-boundary-ssot

**目标**：建立清晰的模块边界与单向依赖，降低耦合与改动外溢。

#### Scenario: extract-protocol-and-core

- 将 RESP 相关实现从 server/runtime 中抽离为独立模块（供 server/client/bench 复用）
- 将 DB/命令处理从网络/编解码细节中剥离，形成 core 逻辑层

### Requirement: config-ssot

**目标**：统一参数定义、默认值与校验逻辑，避免参数漂移，并提升可用性与可测试性。

#### Scenario: server-and-bench-args-aligned

- server 与 bench 共用同一套参数模型（至少共享 maxmemory/offheap/backpressure/executor 的语义）
- `--help`/错误提示/默认值表达一致，并能通过测试覆盖关键约束

### Requirement: executor-ssot

**目标**：命令执行与维护任务执行路径收敛，反压语义清晰可测。

#### Scenario: single-executor-impl

- 保留一个 canonical executor（含队列、drain、时间预算、水位线、统计），其它实现要么移除要么变为薄封装
- 明确“何时拒绝提交/何时恢复/何时降级”的规则，并由单测/压力回归兜底

### Requirement: resp-codec-ssot

**目标**：RESP 编解码成为 SSOT，减少 server/client/bench 三方实现偏差。

#### Scenario: bench-reuses-codec

- bench 在不强制依赖 Netty 网络栈的前提下，复用同一 RESP codec 进行编码/解码或语义校验
- server 的写出路径与 client 的解码路径在行为上可 round-trip 对齐（至少覆盖 RESP2 + RESP3 最小子集）

### Requirement: offheap-decouple-netty

**目标**：off-heap 抽象层不直接依赖 `ByteBuf`，可被 core 层与不同网络适配层复用。

#### Scenario: netty-adapter-is-optional

- `yierdis-offheap-api` 只暴露与 Netty 无关的接口
- Netty 适配（如需要）放入 `yierdis-offheap-netty` 或 server 侧适配模块中，按需依赖

## Risk Assessment

- **Breaking changes 风险**：模块迁移与 API 调整会影响包名/依赖；需要通过渐进式迁移与兼容层降低风险
- **性能回退风险**：抽象与复用可能引入额外分配/拷贝；需要在关键路径保留 fast-path，并用 bench 回归验证
- **测试缺口风险**：一致性问题往往靠测试兜底；需要补齐 codec round-trip、executor 语义、参数校验等测试

