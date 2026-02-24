<!-- migrated_from: history/2026-02/202602031814_db_prod_arch_contracts/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: DB Production-Grade Architecture Contracts (Redis Alignment)

## Requirement Background

当前 `yierdis-core` 的 DB 实现（`YierdisDb`）以“教学/演示”目标为主，具有较强的可读性与可解释性，但在“对标 Redis、用于生产环境、并确保后续不会发生大规模架构重写”的目标下，存在以下核心风险：

1. **内部契约未冻结**：key 身份、内存预算、迭代游标等核心语义分散在实现细节中，未来补齐生产能力（持久化/复制/ACL/模块等）时容易被迫重写核心路径。
2. **实例级职责与 DB 级职责耦合**：全局 maxmemory/LRU/多 DB 协调与单 DB 数据结构混在一起，导致演进成本高、测试难度大、边界不清晰。
3. **off-heap keys 读路径可能引入隐性 heap copy**：在高 QPS/低延迟场景下，这会导致 GC 抖动并使“启用 off-heap keys”无法获得稳定收益。
4. **maxmemory/淘汰/拒写的口径缺乏单一权威（SSOT）**：当前可解释的估算口径适合教学，但生产化需要可复现、可观测、可证明的预算/预留/提交流程。
5. **SCAN/迭代语义与实现绑定**：若游标语义不稳定或与 rehash/过期副作用强耦合，将难以对标 Redis 客户端生态的预期与实践。
6. **执行模型与隔离/扩展策略未冻结**：当前 DB 通过单线程绑定实现一致性，但生产化需要明确并固化“慢命令/大 key 的隔离策略”和“扩展策略（多实例/分片 vs 单进程多 shard）”，否则后续很容易触发架构级重构。
7. **存储引擎与命令语义耦合过深**：`YierdisDb` 同时承担“数据结构实现 + 命令语义 + 内存记账 + 淘汰/过期策略”等职责，形成超大聚合体；生产化后每次演进都会跨类型/跨路径联动改动，回归面大，难以保证“不大改”。

本方案目标：以“冻结 3 个 SSOT 契约”为第一优先级，围绕 `KeyHandle`、`MemoryLedger`、`Cursor` 建立可演进的内核边界；在不推倒重写的前提下，逐步把实例级职责从 `YierdisDb` 中搬出，并将 keyspace/expire/scan 与 memory accounting 收敛到统一抽象层，为后续生产能力扩展提供稳定基座。

## Change Content

1. 引入并冻结 3 个内部 SSOT 契约：`KeyHandle` / `MemoryLedger` / `ScanCursor`（新语义）
2. 引入实例级运行时/上下文（Instance Runtime），承载全局 maxmemory、全局 LRU clock、共享 allocator 口径等职责
3. 将 keyspace/expires/scan 全部迁移为基于 `KeyHandle` 的实现，消除 off-heap keys 场景下读路径 canonical heap copy
4. 将所有写入路径迁移为基于 `MemoryLedger` 的“预留-变更-提交”流程，统一 OOM/淘汰/拒写语义与可观测性输出
5. 补齐最小但关键的回归测试集，锁住对标 Redis 的关键行为（TTL 返回码、maxmemory 拒写点、SCAN cursor 可推进性）
6. 在契约稳定后，再逐步引入生产能力（持久化/复制/ACL/模块）并确保不会回头改动内核契约
7. 明确并文档化生产执行模型：保持 DB 单线程语义为 SSOT，并在实例层引入可选 shard/router 以支持多核扩展；同时制定慢命令/大 key 的隔离与观测策略（不侵入 DB 内核契约）
8. 拆分 `YierdisDb` 的职责：将类型操作（String/Hash/List/Set/ZSet/HLL）、过期、淘汰、记账等抽离为独立组件，`YierdisDb` 收敛为“单 DB 引擎编排器”（保持外观 API 与命令层最小变更，逐步迁移）

## Impact Scope

- **Modules:**
  - `yierdis-core`（db/keyspace/expires/value/memory stats/scan）
  - `yierdis-server`（实例级装配、DB/allocator/ledger/coordinator 绑定）
  - `yierdis-args`（新增/调整配置项：maxmemory scope、scan 行为、安全阈值等的稳定参数化）
  - `helloagents/wiki`（架构与模块 SSOT 文档更新）
- **Files (high-level):**
  - DB 内核：`yierdis-core/src/main/java/yier/bubu/redis/db/*`
  - off-heap：`yierdis-core/src/main/java/yier/bubu/redis/db/offheap/*`
  - server bootstrap：`yierdis-server/src/main/java/yier/bubu/redis/*`
  - tests：`yierdis-core/src/test/java/yier/bubu/redis/db/*` / `yierdis-core/src/test/java/yier/bubu/redis/command/*`
- **APIs:**
  - 对外 Redis 命令 API 不变（目标：保持兼容并逐步对齐）
  - 内部 API 将新增/调整（KeyHandle、Ledger、Cursor、Instance Runtime）
- **Data:**
  - 内存数据结构迁移（无持久化存量数据；但需保证命令语义与兼容测试稳定）

## Core Scenarios

### Requirement: SSOT Contracts Freeze (KeyHandle / MemoryLedger / Cursor)
**Module:** db
冻结并文档化 3 个内部契约，作为后续演进的“不可轻易更改边界”。

#### Contract: KeyHandle（key identity SSOT）
不可变边界（v1）：
- **字段/能力：** `len + byteAt` 的只读 bytes view；`dictHash`（用于 keyspace 索引，可能包含 per-dict seed）
- **语义：**
  - key 身份以 bytes 内容为准，必须能在 heap/off-heap 后端下稳定表示
  - **禁止**“为了 canonicalKey”而隐式产生 heap copy；若需要 bytes 物化，必须由上层显式触发（例如 reply/持久化）
  - equality/hashCode 语义以 bytes 内容为准（不要求 dictHash 跨实例一致）
- **兼容策略：**
  - 新增字段/能力优先以“新增方法/新实现”方式引入，保持旧行为可用
  - 任何影响 bytes view 或 equality 的改动视为破坏性变更，必须通过 feature flag（如 `--keyHandleMode legacy|handle`）灰度
- **弃用路径：** legacy `canonicalKey(byte[]/BytesView)` 仍保留一段时间，但在 handle 模式下不得作为热路径依赖

#### Contract: MemoryLedger（maxmemory SSOT）
不可变边界（v1）：
- **字段/能力：** `limitBytes/usedBytes/reservedBytes`；`reserve → commit/rollback` 两阶段语义
- **语义：**
  - 预算判定以 ledger 为唯一权威（SSOT）；拒写点可复现、可回归
  - reserve/commit/rollback 不允许出现负数/下溢；异常路径必须可 rollback
  - OOM message 对齐 Redis：`OOM command not allowed when used memory > 'maxmemory'.`
- **兼容策略：** 通过 `--ledgerMode legacy|ledger` 逐步迁移写路径（默认 legacy，可回滚）
- **弃用路径：** 现有 `usedBytes + prepareWrite/enforceMaxmemory` 作为 legacy 路径保留，待 ledger 覆盖后弃用

#### Contract: ScanCursor（SCAN cursor SSOT）
不可变边界（v1）：
- **字段/能力：** cursor 为 RESP bulk string 的“数字字符串”；`0` 表示结束
- **语义：**
  - best-effort：不保证强一致，但必须“可推进、可终止”
  - `COUNT` 为 hint，不得导致单次扫描跑完整个 keyspace 或死循环
- **兼容策略：**
  - v1（offset cursor）默认启用；v2（rehash-aware cursor）通过 `--scanCursorV2` 灰度（默认关闭）
- **弃用路径：** v2 稳定后逐步默认启用，并保留 v1 作为兼容开关与排障手段

#### Scenario: Key identity is stable across backends
条件：同一 key 可能来自 `byte[]` / `YierdisBytesView` / 协议帧 slice；后端可能是 heap 或 off-heap。
- 预期：key 身份可稳定定位（不依赖 heap copy 的 canonicalKey），并具备可复用的 hash/len/bytes 访问方式

#### Scenario: Memory accounting is single-source and reproducible
条件：写入/覆盖/删除/淘汰/过期会改变内存占用；存在 off-heap allocator。
- 预期：预算判定只依赖 `MemoryLedger` 的单一口径；行为可观测、可解释、可回归

#### Scenario: SCAN cursor can always make progress
条件：rehash 进行中、keyspace 发生插入/删除、存在过期清理。
- 预期：SCAN 游标语义稳定（best-effort 但可推进）；不会因实现细节导致死循环/无法推进

### Requirement: DB Core Decomposition (Reduce Storage-Command Coupling)
**Module:** db, command
将 `YierdisDb` 的“超大聚合体”职责拆分为可演进的组件边界，降低新增特性/修复问题时的联动改动范围。

#### Scenario: YierdisDb shrinks to an orchestrator
条件：完成拆分后，数据结构与策略分别由独立组件承载。
- 预期：`YierdisDb` 主要负责线程语义、组件装配、统一事务边界与跨组件编排，不再堆叠各类型命令实现细节

#### Scenario: Adding a new command or encoding touches limited code
条件：新增一个命令（或新增一种编码升级策略）。
- 预期：只需修改对应类型的 ops/engine 组件与少量 wiring；不需要在 `YierdisDb` 中跨段修改多个不相关路径

### Requirement: Instance Responsibilities Extraction (Global maxmemory / LRU clock)
**Module:** server, db
将全局职责从单 DB 中抽离，形成实例级运行时（Instance Runtime）与 DB 的清晰边界。

职责清单（SSOT）：
- **多 DB 装配与路由：** instance 负责创建 DB 数组，并按连接态（`SELECT` 的 dbIndex）路由到目标 DB
- **全局预算（global maxmemory）：** 当 `--maxmemoryScope global` 时，预算判定与淘汰决策以实例为单位执行（更贴近 Redis）
- **全局 LRU clock（allkeys-lru）：** 多 DB 全局淘汰需要共享时钟来源，避免每个 DB 的局部 clock 造成非预期 victim 选择
- **共享 allocator 口径：** 多 DB 共享同一个 off-heap allocator 时，避免 allocator.usedBytes() 在每个 DB 重复计入导致过度淘汰

#### Scenario: Global maxmemory works across multiple DBs
条件：`SELECT` 多 DB、`--maxmemoryScope global`、共享 off-heap allocator。
- 预期：maxmemory 预算与淘汰决策为实例级；off-heap usedBytes 不重复计入；行为与 Redis 更一致

### Requirement: KeyHandle-based Keyspace/Expire/Scan (No canonical heap copy)
**Module:** db
keyspace / expires / scan 全部改为基于 `KeyHandle` 的实现，禁止 off-heap keys 场景下读路径 canonical heap copy。

#### Scenario: Off-heap keys read path has zero heap allocation (hot path)
条件：频繁 `GET/EXISTS/TYPE/TTL`，key 存储在 off-heap。
- 预期：热路径不产生 heap copy（除非显式需要返回 key bytes 给客户端）

### Requirement: Ledger-based Writes (Reserve → Mutate → Commit)
**Module:** db, command
所有写入路径通过 `MemoryLedger` 统一进行容量预留、淘汰触发与 OOM 拒写，避免口径漂移。

#### Scenario: Reject happens before reply is written (no double reply)
条件：触达 maxmemory、淘汰不足或 noeviction。
- 预期：拒写/OOM 必须在写 reply 前发生；不出现“写成功后才 OOM / 双 reply / 协议损坏”

### Requirement: Production Extension Guardrails (Post-contract capabilities)
**Module:** core, server
在契约稳定后引入持久化/复制/ACL/模块，必须遵守冻结的 KeyHandle/Ledger/Cursor 契约，避免再次大改内核。

#### Scenario: Add persistence without rewriting DB core contracts
条件：新增 AOF/RDB（或简化版）能力。
- 预期：持久化只依赖冻结契约获取 key/value/变更事件；不反向侵入 keyspace/expire 的内部实现

### Requirement: Execution Model & Isolation Strategy (Production Readiness)
**Module:** server, core
明确并冻结执行模型与隔离策略，确保“生产可用”不会倒逼 DB 内核重构。

#### Scenario: Slow command isolation does not stall the whole instance
条件：出现 `KEYS/SCAN` 大范围遍历、`LRANGE` 大范围返回、或大 value 读写等潜在慢操作。
- 预期：具备明确的时间预算/分页/COUNT hint/限流策略；可观测（slowlog/metrics）；不会导致 executor 长时间不可用

#### Scenario: Scaling strategy is explicit and does not change DB core contracts
条件：需要用多核提升吞吐（单机多核）或横向扩展（多实例）。
- 预期：清晰选择并固化策略：
  - 多实例/分片（推荐优先）：通过外部或 server 层 router 分流
  - 单进程多 shard（可选）：多个 shard 各自单线程执行器，跨 shard 命令明确限制/策略
  - 两者都不应要求修改 `YierdisDb` 的单线程与 KeyHandle/Ledger/Cursor 契约

## Risk Assessment

- **Risk:** 这是一个长期的内核演进方案，涉及多模块与核心抽象变更，存在兼容性回归与性能回归风险。  
  **Mitigation:** 以“冻结契约 + 小步迁移 + 行为回归测试锁定”为主线推进；每个阶段都保证对外命令语义稳定。
- **Risk:** KeyHandle/Cursor 设计不当会导致后续扫描/过期/淘汰行为难以对齐 Redis。  
  **Mitigation:** 在 how.md 中记录 ADR 与替代方案；优先锁定最小必要语义，再逐步扩展。
- **Risk:** 记账口径从估算升级为 ledger 会影响现有 maxmemory 行为。  
  **Mitigation:** 保留“可解释估算”的输出，但把“预算判定”统一落到 ledger；通过回归测试锁住关键拒写点与淘汰策略。
- **Risk:** 执行模型/隔离策略若不提前冻结，生产化过程中引入多核/慢命令治理会倒逼 DB 内核重构。  
  **Mitigation:** 先把“DB 单线程语义 + shard/router 层扩展”的策略固化为 ADR，并将慢命令/大 key 的预算/限流/观测作为 server 层可插拔能力推进。
