# Change Proposal: 架构护栏与可观测性加固（arch_guardrails）

## Requirement Background

当前整体模块边界已经比较清晰（`yierdis-core` / `yierdis-protocol` 维持 Netty-free，Netty 相关实现下沉到 `yierdis-protocol-netty`；off-heap 通过多后端模块拆分并可插拔）。但在长期演进中，仍存在一些“容易退化”的风险点：

1. **语义/命名误导：**`yierdis-protocol` 依赖 `yierdis-offheap-api`，主要是为了复用 `YierdisBytesSource/YierdisBytesSink` 等通用 bytes 抽象，但模块名 “offheap” 容易让贡献者误判协议层被 off-heap 绑架。
2. **SPI 插拔可用性与可运维性：**当前后端通过 `ServiceLoader` provider 注册，并保留反射 fallback。若未来类名/资源合并策略变化，可能出现“可用性漂移”或排障不直观。
3. **命令拆分后的遗漏风险：**命令实现已按 domain 拆分为多个 `*Commands`，但新增命令时可能出现“实现已写但忘记注册”的静默遗漏。
4. **连接级状态隔离：**RESP2/RESP3 协商属于连接级状态，需要长期防止通过静态变量/全局缓存泄漏，导致多连接间串扰。
5. **内存口径解释：**maxmemory/统计口径属于“可解释的估算”，需要确保文档与命令输出一致，避免误解。

## Change Content

1. 明确 “bytes 抽象” 与 “off-heap 能力” 的边界含义，并在文档中固定为约束（SSOT）。
2. 强化 off-heap 后端发现/选择的可观测性：启动期输出可读诊断信息，并对缺失依赖提供可操作的报错。
3. 为命令注册建立护栏：通过测试或启动期自检，让“遗漏注册”可被自动发现。
4. 为连接级协议状态建立护栏：通过测试与约束，确保状态只绑定连接生命周期。
5. 统一并强化内存统计与文档解释：让 `MEMORY STATS` / `maxmemory` 行为更可预期、更易排障。

## Impact Scope

- **Modules:** `yierdis-protocol`, `yierdis-protocol-netty`, `yierdis-offheap-api`, `yierdis-offheap-*`, `yierdis-core`, `yierdis-server`, `helloagents/wiki/*`
- **Files (expected):**
  - off-heap provider/diagnostics：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java`
  - server startup wiring：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java` / `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`
  - command registry guard tests：`yierdis-core/src/test/java/yier/bubu/redis/command/*`
  - protocol session isolation tests：`yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/*`
  - knowledge base：`helloagents/wiki/arch.md`, `helloagents/wiki/modules/protocol.md`, `helloagents/wiki/modules/offheap.md`, `helloagents/wiki/modules/db.md`

## Core Scenarios

### Requirement: Bytes 抽象边界可解释（Docs + Guardrail）
**Module:** protocol / offheap-api
将 `yierdis-offheap-api` 中的 “通用 bytes 抽象” 与 “allocator/off-heap 后端能力” 在文档中明确区分。

#### Scenario: 新贡献者阅读依赖图
条件：贡献者查看 `pom.xml` 的依赖图或 IDE 依赖树
- 预期：能在 `arch.md` 与相关模块文档中明确看到“为何 protocol 依赖 offheap-api”的解释
- 预期：清楚哪些类型属于通用 I/O 抽象（可被 protocol 复用），哪些属于 off-heap 能力（由后端模块提供）

### Requirement: Off-heap 后端发现与启动诊断（Observability）
**Module:** offheap-api / server
通过 `ServiceLoader` 发现 providers，并在启动期输出“可用后端/选中后端/能力/上限”等信息；缺失时输出可操作指引。

#### Scenario: 启动时选择后端
条件：使用 `--offheapBackend` / `--offheapMaxBytes` 启动 server
- 预期：日志输出可用 providers 与最终选择结果
- 预期：当后端不可用时，报错包含“需要添加哪个模块/启用哪个 profile/运行时需要哪些参数”的提示

### Requirement: 命令注册遗漏可被自动发现（Command Registry Guard）
**Module:** core
为命令层建立“注册完整性”护栏，避免新增命令时忘记注册。

#### Scenario: 新增命令但忘记注册
条件：新增 `*Commands` 中的 handler，但未在 `YierdisFastCommandProcessor` 注册
- 预期：测试阶段立即失败（或启动期自检失败），并给出明确提示

### Requirement: 连接级协议状态隔离（Session Isolation）
**Module:** protocol-netty / server
明确 RESP2/RESP3 等状态只存放于连接生命周期容器（`RespSession` / Netty `Channel.attr`）。

#### Scenario: 多连接协议切换不串扰
条件：连接 A 执行 `HELLO 3`，连接 B 仍为默认 RESP2
- 预期：A 的回复按 RESP3 编码，B 仍按 RESP2 编码
- 预期：状态不会通过静态变量/全局缓存影响其他连接

### Requirement: 内存口径可解释（Memory Accounting Explainability）
**Module:** core / wiki
统一 `MEMORY STATS` / `maxmemory` 的解释口径与文档描述，强调“估算但稳定可解释”。

#### Scenario: 用户排查 maxmemory/eviction 行为
条件：用户开启 `maxmemory` 并观察 `MEMORY STATS` 输出
- 预期：文档说明哪些字段为估算、哪些来自 `offHeapAllocator.usedBytes()`
- 预期：用户可据此理解 OOM/eviction 触发原因与调参路径

## Risk Assessment

- **Risk:** 引入启动期自检/日志可能改变部分异常路径（更早失败）
  - **Mitigation:** 仅在启动期执行，失败信息保持可操作且不隐藏 root cause
- **Risk:** 对 ServiceLoader/fallback 策略调整可能影响某些 build/shade 场景
  - **Mitigation:** 保持 shade `ServicesResourceTransformer` 作为 SSOT；为关键场景补充回归测试
- **Risk:** 新增护栏测试可能导致未来重构需要同步更新命令清单
  - **Mitigation:** 只约束“最小命令集/关键命令集”，避免过度耦合实现细节
