# Change Proposal: DB 生产级契约落地（全量切换新实现）

## Requirement Background

当前代码库已经冻结了 KeyHandle / MemoryLedger / ScanCursorV2 的契约，并补齐了一部分契约测试与回归测试，但“契约落地（landing）”仍存在明显缺口：

1. **KeyHandle 仅有契约与最小实现**：keyspace / expire / scan 仍以 `byte[] canonicalKey` 为中心，off-heap keys 场景下热路径容易产生 heap copy。
2. **MemoryLedger 仍未成为写路径 SSOT**：`prepareWrite/enforceMaxmemory/usedBytes` 仍分散维护，拒写点与可观测口径存在漂移风险。
3. **SCAN 仍是 cursor v1**：对 rehash 的处理不够显式，且缺少“可 time-slice 的迭代 API”作为 SSOT。
4. **server 侧治理与扩展前置条件仍缺失**：shard/router/slow command 治理边界、生产能力扩展（AOF/RDB/replication/ACL/modules）的 guardrails 没有形成可落地的接口与回归基座。
5. **DB core 仍为单体编排**：存储/命令/记账/淘汰/过期耦合度高，后续能力扩展的改动面偏大。

本变更按历史任务清单顺序补齐上述 `[-]` 项，并按要求 **默认切到新实现、移除 legacy 兼容路径**，以确保 SSOT 契约在代码中真正落地。

## Product Analysis

### Target Users and Scenarios
- **User Groups:** Yierdis 核心开发者、server 集成者、性能/稳定性回归维护者
- **Usage Scenarios:** off-heap keys 生产负载、multi-db + global maxmemory、长尾命令（SCAN/KEYS）隔离治理、后续引入持久化/复制/ACL/模块系统
- **Core Pain Points:** 内存预算口径漂移、off-heap 热路径多余 heap copy、SCAN 在 rehash 下的可推进性与可观测性不足、核心组件耦合导致扩展成本高

### Value Proposition and Success Metrics
- **Value Proposition:** 以 KeyHandle/Ledger/CursorV2 作为 SSOT，将“key identity / 内存预算 / 迭代模型”收敛为可验证契约，降低扩展与回归风险
- **Success Metrics:** `mvn test` 全绿；新增关键回归覆盖 keysOffHeapEnabled 读路径不产生 canonical heap copy；maxmemory 拒写点/淘汰行为与无双 reply 回归稳定；SCAN 在 rehash + 变更数据集下可推进且可终止

### Humanistic Care
本变更不涉及 PII 处理与用户隐私；通过 fail-fast 与更可解释的可观测字段，降低线上排障成本与误操作风险。

## Change Content

1. **KeyHandle Landing**：将 keyspace / expire / scan 的内部交互统一到 KeyHandle（heap/off-heap），定义“不隐式 canonical heap copy”的约束，并以回归测试锁定。
2. **Ledger Landing**：将写路径与 maxmemory 预算判定统一到 MemoryLedger（reserve → mutate → commit/rollback），并让 `MEMORY STATS/INFO` 以 ledger 为权威口径。
3. **ScanCursorV2 + Iterator SSOT**：引入 rehash-aware 的 cursor v2（仍保持 bulk string 数字兼容），并实现可 time-slice 的 keyspace iterator 作为 SCAN 的 SSOT。
4. **默认新实现、移除 legacy**：删除 legacy cursor/legacy ledger/legacy keyhandle 路径与对应文档描述，确保只有一条行为路径可维护。
5. **生产能力扩展前置条件（Guardrails）**：冻结“扩展能力必须遵守契约”的约束，并提供事件流/快照接口（供 AOF/RDB/replication 复用）的最小可回归基座。
6. **执行模型与隔离策略**：明确单线程 DB 语义不变，扩展通过 instance 层 shard/router（默认单 shard，可选多 shard）；增加慢命令治理接口与回归。
7. **DB Core 组件化拆分**：引入 `ValueOps/ExpirationManager/EvictionCoordinator/MemoryLedger/DbEngine` 等边界，逐步将 `YierdisDb` 收敛为 orchestrator，降低存储-命令耦合。

## Impact Scope
- **Modules:** `yierdis-core`, `yierdis-server`
- **Files:** 涉及 keyspace/expire/scan/db/memory/runtime/command 与对应测试
- **APIs:** 主要为内部 API；对外 RESP 协议保持兼容（SCAN cursor 仍为数字 bulk string，语义 best-effort）
- **Data:** 无持久化数据结构变更（本包提供事件/快照接口，但不启用真实持久化）

## Core Scenarios

### Requirement: KeyHandle Landing（No Canonical Heap Copy）
**Module:** yierdis-core
将 keyspace/expires/scan 的内部路径统一到 KeyHandle，避免 off-heap keys 热路径产生 canonical heap copy。

#### Scenario: Off-heap keys read path has zero heap allocation hot path
在 `keysOffHeapEnabled=true` 时：
- `GET/EXISTS/TYPE/TTL` 的 key 查找不触发 canonical heap copy
- 允许少量不可避免的对象分配，但禁止“以返回 heap key bytes 作为内部 SSOT”

#### Scenario: Key identity is stable across backends
- heap/off-heap 两后端对同一 bytes 内容的 equality 语义一致
- dictHash 仅用于索引，不作为 equality 判定

### Requirement: Ledger-based writes（Reserve → Mutate → Commit）
**Module:** yierdis-core
写路径以 ledger.reserve 作为拒写点，保证 reply 前拒写，避免双 reply。

#### Scenario: Reject happens before reply is written (no double reply)
- 当预算不足时，写命令在写入 reply 前失败
- 不出现“写成功后才 OOM”导致协议错误

#### Scenario: Memory accounting is single source and reproducible
- 不出现 usedBytes/reservedBytes 负数
- rollback 能覆盖异常路径，拒写点稳定

#### Scenario: Global maxmemory works across multiple DBs
- multi-db + global maxmemory 下可淘汰/拒写且行为稳定（best-effort）
- shared allocator 的 off-heap used bytes 只计一次，不出现双计数

### Requirement: SCAN cursor v2 can always make progress
**Module:** yierdis-core
引入 rehash-aware cursor v2 与 time-slice iterator，保证 best-effort 但可推进可终止。

#### Scenario: Scan cursor can always make progress
- cursor=0 终止
- 在 rehash + 插入/删除/过期并发变化下，SCAN 仍可推进，不会卡死

### Requirement: Server governance & isolation strategy
**Module:** yierdis-server
明确 instance/shard/router/slow command 治理边界，并为慢命令提供隔离接口与回归。

#### Scenario: Slow command isolation does not stall the whole instance
- `KEYS/SCAN` 等大范围命令不拖垮 executor 可用性
- 具备预算/分页/限流/观测的最小接口

### Requirement: Production extension guardrails
**Module:** yierdis-core / yierdis-server
为 AOF/RDB/replication/ACL/modules 的接入提供契约约束与事件/快照接口基座。

#### Scenario: Add persistence without rewriting DB core contracts
- 事件流/快照接口不泄漏 keyspace 内部实现细节
- 可被后续持久化/复制消费

### Requirement: DB core decomposition reduces coupling
**Module:** yierdis-core
降低存储-命令耦合，确保新增命令/编码改动面可控且可回滚。

#### Scenario: YierdisDb shrinks to an orchestrator
- 核心逻辑分散到组件，`YierdisDb` 仅保留线程语义、装配与编排

## Risk Assessment
- **Risk:** 改动面大、回归风险高（keyspace/expire/scan/maxmemory/命令写路径/server 装配）
  - **Mitigation:** 分阶段任务执行、每阶段新增回归、以 `mvn test` 为硬闸、优先锁定拒写点/SCAN 可推进性/无 canonical heap copy
- **Risk:** 性能退化（新增抽象导致额外分配或更慢查找）
  - **Mitigation:** 热路径以 handle/view 传递；避免生成 heap canonical key；必要时增加 micro benchmark 或分配计数断言
- **Risk:** 兼容性行为变化（删除 legacy 模式）
  - **Mitigation:** 更新文档与测试；保持 RESP 语义兼容（如 SCAN cursor 仍为数字字符串）但不提供旧模式切换
