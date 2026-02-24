<!-- migrated_from: history/2026-02/202602031814_db_prod_arch_contracts/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: DB Production-Grade Architecture Contracts (Redis Alignment)

Directory: `helloagents/plan/202602031814_db_prod_arch_contracts/`

---

## Status Update (2026-02-04)

本包在执行时将大量落地项标记为 “后续包推进”（`[-]`）。这些延后项已在后续方案包中作为默认新实现落地并通过全量回归：
- 方案包：`helloagents/history/2026-02/202602041630_db_prod_arch_contracts_full_newimpl/`
- 策略：默认切换新实现，移除 legacy（cursorV1/legacy ledger/legacy keyhandle）

本文件保留当时执行状态用于追溯；如需查看最终落地结果，请以 202602041630 方案包为准。

## 1. Contract Freeze (SSOT)
- [√] 1.1 明确 3 个契约的“不可变边界”（字段/语义/兼容策略/弃用路径），写入 why.md+how.md 的 ADR，verify why.md#requirement-ssot-contracts-freeze-keyhandle--memoryledger--cursor
- [√] 1.2 在知识库 `helloagents/wiki/modules/db.md` 中新增“Contracts”小节：KeyHandle/Ledger/Cursor 的 SSOT 定义与版本化策略，verify why.md#requirement-ssot-contracts-freeze-keyhandle--memoryledger--cursor，depends on task 1.1
- [√] 1.3 新增 `KeyHandle` 契约测试：hash/equals/bytes view 语义（heap/off-heap 两模式），verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 1.1
- [√] 1.4 新增 `ScanCursor` 契约测试：cursor=0 终止、游标单调推进（best-effort）、COUNT hint 不会卡死，verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 1.1
- [√] 1.5 新增 `MemoryLedger` 契约测试：reserve/commit/rollback 的不变量（不出现负数、拒写点稳定），verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 1.1
- [√] 1.6 新增契约级集成测试骨架：TTL 返回码 / maxmemory 拒写点 / SCAN 可推进（覆盖 heap/off-heap & global/per-db），verify why.md#requirement-ssot-contracts-freeze-keyhandle--memoryledger--cursor，depends on task 1.2

## 2. Instance Runtime (Extract Global Responsibilities)
- [√] 2.1 明确 Instance Runtime 的职责清单与 API：global LRU clock / budget / eviction coordinator / shared allocator view，verify why.md#requirement-instance-responsibilities-extraction-global-maxmemory--lru-clock
- [-] 2.2 在 `yierdis-server` 引入实例上下文对象（例如 `YierdisInstanceRuntime`），并在 bootstrap 中构造与注入，verify why.md#requirement-instance-responsibilities-extraction-global-maxmemory--lru-clock，depends on task 2.1
  > Note: 已采用 core 的 Netty-free `yier.bubu.redis.runtime.YierdisInstance` 作为实例上下文 SSOT（server bootstrap 复用装配语义），本包不再新增 server 专用 runtime 类。
- [-] 2.3 抽离 `GlobalMaxmemoryCoordinator`：从 `YierdisDb` 迁移到实例层并提供适配层（保持命令层不感知），verify why.md#requirement-instance-responsibilities-extraction-global-maxmemory--lru-clock，depends on task 2.2
  > Note: 已在先前实现中抽离为 `yier.bubu.redis.db.YierdisGlobalMaxmemoryCoordinator` 并由 instance 层装配启用，本包不重复执行。
- [-] 2.4 统一 LRU clock：将“全局时钟”移动到实例层，并提供 per-db touch 的统一入口，verify why.md#requirement-instance-responsibilities-extraction-global-maxmemory--lru-clock，depends on task 2.2
  > Note: 当前 allkeys-lru 的全局 clock 已由 global maxmemory 协调器提供（跨 DB）；进一步抽象为独立 instance clock 组件延期。
- [-] 2.5 增加 server 级回归测试：多 DB + global maxmemory + shared allocator 场景不双计数且可淘汰，verify why.md#scenario-global-maxmemory-works-across-multiple-dbs，depends on task 2.3
  > Note: 已通过 core 的 instance 回归测试覆盖（多 DB + global maxmemory + shared allocator 不双计数）；server 级集成测试延期。

## 3. KeyHandle Landing (Keyspace / Expire / Scan)
- [-] 3.1 为 `YierdisKeyspace` 增加 KeyHandle 入口（lookup/compute/remove/iterate），并定义“不得隐式 heap copy”的约束，verify why.md#requirement-keyhandle-based-keyspaceexpirescan-no-canonical-heap-copy
  > Note: 本包仅建立 `KeyHandle` 契约类型与回归测试，未改造 keyspace/expires/scan 的内部 API（需按“最小闭环”另包推进）。
- [-] 3.2 heap keyspace：实现 heap `KeyHandle`（基于 canonical `byte[]`，避免额外复制），并补齐单元测试，verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 3.1
  > Note: 已提供 heap `KeyHandle` 的最小实现与契约测试，但未接入 heap keyspace 的 handle API（依赖 task 3.1）。
- [-] 3.3 off-heap keyspace：实现 address `KeyHandle`（addr/len/hash），并补齐单元测试，verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 3.1
  > Note: 已提供 off-heap `KeyHandle` 的最小实现与契约测试，但未接入 off-heap keyspace 的 handle API（依赖 task 3.1）。
- [-] 3.4 改造 `YierdisBytesView`/命令层 key 传递：优先走 view → handle 的路径，减少 `toByteArray()`，verify why.md#requirement-keyhandle-based-keyspaceexpirescan-no-canonical-heap-copy，depends on task 3.1
  > Note: 未改造命令层；需在 keyspace 支持 handle 后才能安全迁移。
- [-] 3.5 `YierdisExpireIndex`：新增 handle 友好 API（set/get/remove/random），并先实现 heap 路径，verify why.md#requirement-keyhandle-based-keyspaceexpirescan-no-canonical-heap-copy，depends on task 3.2
  > Note: 未落地（需在 keyspace/expires 引入 handle 入口后同步推进）。
- [-] 3.6 `YierdisExpireIndex`：实现 off-heap keys 路径（引用 keyspace handle/address，不额外分配 key bytes），verify why.md#requirement-keyhandle-based-keyspaceexpirescan-no-canonical-heap-copy，depends on task 3.3
  > Note: 未落地（需与 off-heap keyspace 的 handle 入口一起最小闭环）。
- [-] 3.7 引入 `ScanCursorV2`（rehash-aware）对象模型与序列化规则（bulk string 数字兼容），verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.1
  > Note: 未落地（需先补齐 keyspace iterator 能力与 handle 入口）。
- [-] 3.8 Keyspace iterator：实现“可 time-slice 的迭代 API”（支持 COUNT hint 与 rehash 双表），verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.7
  > Note: 未落地（需与 ScanCursorV2 一起设计/实现）。
- [-] 3.9 `SCAN` 命令：切换为基于 iterator + cursorV2（默认无副作用），并增加回归测试覆盖 rehash/变更数据集，verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.8
  > Note: 未落地（当前仍为 cursor v1 + best-effort 过期顺手清理语义）。
- [-] 3.10 兼容策略：保留 legacy cursor 与旧 scan 行为（配置开关 + INFO 可观测），verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.9
  > Note: 未落地（需在引入 v2 之后再补齐开关与可观测性字段）。
- [-] 3.11 off-heap keys 热路径回归：`GET/EXISTS/TYPE/TTL` 在 keysOffHeapEnabled 下不产生 canonical heap copy（基于分配计数/基准断言），verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path，depends on task 3.3
  > Note: 未落地（需 keyspace handle landing 完成后才能稳定断言“零 canonical copy”）。

## 4. Ledger Landing (Reserve → Mutate → Commit)
- [-] 4.1 设计 ledger 的 SSOT 口径：哪些 bytes 计入预算、哪些仅做解释性统计、off-heap 如何计入，verify why.md#requirement-ledger-based-writes-reserve--mutate--commit
  > Note: 本包未定义完整“预算口径表”；目前仅冻结接口语义与 OOM message（后续落地写路径前需补齐口径细节）。
- [√] 4.2 实现 `MemoryLedger` 核心 API：reserve/commit/rollback/used/limit 与错误语义（OOM message 对齐），verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.1
- [-] 4.3 将 `prepareWrite` 重构为 `ledger.reserve(estimatedExtra)`（内部包含 expire cleanup + eviction），并保持“reply 前拒写”，verify why.md#scenario-reject-happens-before-reply-is-written-no-double-reply，depends on task 4.2
  > Note: 未接入 DB 写路径（当前仍为 legacy prepareWrite/enforceMaxmemory）。
- [-] 4.4 将 `enforceMaxmemory` 重构为 ledger 驱动（后台/周期性强制），verify why.md#requirement-ledger-based-writes-reserve--mutate--commit，depends on task 4.2
  > Note: 未落地（需在 ledger 接入后再迁移后台强制逻辑）。
- [-] 4.5 为每类写命令定义“estimatedExtraBytes”策略（保守上界即可）：SET/APPEND/LPUSH/RPUSH/HSET/SADD/ZADD/PFADD，verify why.md#requirement-ledger-based-writes-reserve--mutate--commit，depends on task 4.3
  > Note: 未落地（依赖 ledger 接入后逐族命令迁移）。
- [-] 4.6 命令层迁移：逐个命令替换为 ledger reserve（先 String/Hash/List/Set/ZSet，再 HLL），verify why.md#requirement-ledger-based-writes-reserve--mutate--commit，depends on task 4.5
  > Note: 未落地（需按最小闭环逐族迁移）。
- [-] 4.7 DB 内 delta 统一：收敛 `usedBytes/estimatedBytes` 的维护点，确保异常路径可 rollback，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.2
  > Note: 未落地（ledger 接入后再做 delta 统一）。
- [-] 4.8 可观测输出统一：`MEMORY STATS`/`INFO` 输出 ledger 权威字段，并增加“当前模式（legacy/ledger）”与“cursor/keyhandle 模式”字段，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.2
  > Note: 未落地（当前观测仍以 legacy 估算口径为主）。

## 5. Regression & Quality Gate
- [√] 5.1 TTL 语义回归：锁定 `TTL/PTTL/EXPIRE/PEXPIRE/PERSIST` 返回码（-2/-1/正数）与边界，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible
- [√] 5.2 maxmemory 回归：锁定 preflight 拒写点与“无双 reply”回归用例（含 global/per-db），verify why.md#scenario-reject-happens-before-reply-is-written-no-double-reply
- [√] 5.3 SCAN 回归：锁定 cursor 可推进性（rehash + 插入/删除 + 过期）与终止条件，verify why.md#scenario-scan-cursor-can-always-make-progress
- [-] 5.4 兼容性回归：锁定 legacy 模式（cursorV1/legacy ledger/legacy keyhandle）仍能通过既有测试，verify why.md#requirement-ssot-contracts-freeze-keyhandle--memoryledger--cursor
  > Note: 当前尚未引入 `cursorV2/ledger/keyhandle` 的模式开关，本任务待引入 feature flag 后再补齐。
- [-] 5.5 off-heap 回归：锁定 keysOffHeapEnabled 下读路径分配（尽可能用分配计数/基准断言）与 usedBytes 泄漏锚点，verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path
  > Note: 未落地“零 canonical copy/分配计数”断言（依赖 keyhandle landing 与分配计数基础设施）。

## 6. Security Check
- [√] 6.1 Execute security check（per G9：输入净化、错误净化、资源释放、off-heap 生命周期、拒写路径一致性）

## 7. Documentation Update (Knowledge Base)
- [√] 7.1 更新 `helloagents/wiki/arch.md`：补齐 Instance Runtime / Ledger / KeyHandle 的架构图与依赖方向说明
- [√] 7.2 更新 `helloagents/wiki/modules/db.md`：将 KeyHandle/Ledger/Cursor 契约写为 SSOT，并标注兼容策略与弃用路径
- [-] 7.3 如新增模块/包：补齐对应 `helloagents/wiki/modules/*.md` 并补充 ADR 索引（如何.md 内 ADR 链接）
  > Note: 本次新增包为 db 内部子包（`db.key/db.memory`），已在 `arch.md` 与 `modules/db.md` 中补充说明，未新增独立 module 文档。
- [-] 7.4 更新 `helloagents/wiki/modules/server.md`：补齐 instance/shard/router/slow command 治理的职责边界与配置项
  > Note: 本次未推进 shard/router/slow command 治理设计与配置项，server 模块文档细化延期。

## 8. Production Extension Preconditions (M4 Guardrails)
- [-] 8.1 定义“生产能力扩展前置条件”清单：持久化/复制/ACL/模块接入必须遵守冻结契约，verify why.md#requirement-production-extension-guardrails-post-contract-capabilities
  > Note: 本包未推进持久化/复制/ACL/模块能力设计；仅先冻结内核契约与回归测试基座。
- [-] 8.2 设计变更事件/快照接口（供 AOF/RDB/replication 复用），不侵入 keyspace 实现细节，verify why.md#scenario-add-persistence-without-rewriting-db-core-contracts，depends on task 8.1
  > Note: 未落地（待 KeyHandle/Ledger/Cursor landing 完成后再设计事件/快照接口）。
- [-] 8.3 增加“事件流/快照接口”回归测试：确保不会泄漏内部实现细节且可被持久化/复制消费，verify why.md#requirement-production-extension-guardrails-post-contract-capabilities，depends on task 8.2
  > Note: 未落地（依赖 task 8.2）。

## 9. Execution Model & Isolation Strategy (Production Readiness)
- [-] 9.1 冻结“执行模型与扩展策略”ADR：单线程 DB 语义不变，扩展通过 instance 层 shard/router（默认单 shard；可选多 shard），verify why.md#requirement-execution-model--isolation-strategy-production-readiness
  > Note: 本包未推进 shard/router 扩展与 ADR 冻结（需与 server 治理/观测一起设计）。
- [-] 9.2 明确跨 shard 命令边界与策略（限制/失败语义/可选支持列表），并写入 wiki（arch + server/db 模块），verify why.md#scenario-scaling-strategy-is-explicit-and-does-not-change-db-core-contracts，depends on task 9.1
  > Note: 未落地（依赖 task 9.1）。
- [-] 9.3 设计并实现慢命令治理接口（时间预算/分页/限流/观测），优先覆盖 `KEYS/SCAN` 与大范围返回类命令，verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance，depends on task 9.1
  > Note: 未落地（需 server 层治理与观测设计一起推进）。
- [-] 9.4 增加慢命令/隔离回归测试：确保在高负载下 executor 可用性与延迟不被长尾放大，verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance，depends on task 9.3
  > Note: 未落地（依赖 task 9.3）。

## 10. DB Core Decomposition (Reduce Storage-Command Coupling)
- [-] 10.1 定义拆分边界与命名：`ValueOps`/`ExpirationManager`/`EvictionCoordinator`/`MemoryLedger`/`DbEngine`（保留 `YierdisDb` 入口），verify why.md#requirement-db-core-decomposition-reduce-storage-command-coupling
  > Note: 本包未推进 `YierdisDb` 组件化拆分（属于大规模重构，建议独立成可回滚的小步包）。
- [-] 10.2 建立 `ops` 包与最小接口（先 StringOps）：将 `set/get/append/strlen/bitops` 从 `YierdisDb` 迁移为组件实现，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.1
  > Note: 未落地（依赖 task 10.1）。
- [-] 10.3 增加 StringOps 回归测试与性能 smoke（确保语义不变且无额外分配），verify why.md#scenario-adding-a-new-command-or-encoding-touches-limited-code，depends on task 10.2
  > Note: 未落地（依赖 task 10.2）。
- [-] 10.4 逐类迁移（HashOps/ListOps/SetOps/ZSetOps/HllOps）：每次只搬一个类型，保持每次 PR 可回滚，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.2
  > Note: 未落地（依赖 task 10.2）。
- [-] 10.5 过期/淘汰/记账进一步组件化：将 `cleanupExpired/evictUntilUnder/touch/usedBytesForMaxmemory` 等逻辑迁移到 `ExpirationManager/EvictionCoordinator/MemoryLedger`，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.4
  > Note: 未落地（依赖 task 10.4）。
- [-] 10.6 最终收敛：`YierdisDb` 仅保留线程语义、组件装配与跨组件编排；补齐文档与 ADR-006 细节，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.5
  > Note: 未落地（依赖 task 10.5）。
