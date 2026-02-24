<!-- migrated_from: history/2026-02/202602041630_db_prod_arch_contracts_full_newimpl/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: DB Production-Grade Architecture Contracts (Full New Implementation)

Directory: `helloagents/plan/202602041630_db_prod_arch_contracts_full_newimpl/`

---

## 0. Preflight (Reality Baseline)
- [√] 0.1 盘点当前代码中仍依赖 `byte[] canonicalKey` / cursor v1 / legacy accounting 的路径，输出影响面清单（仅内部记录），verify why.md#requirement-background
  > Note: cursor v1 已删除；写入路径已切换 MemoryLedger.reserve→commit/rollback；`canonicalKey` 仅作为 heap expire index 等非热路径的实现细节保留。

## 2. Instance Runtime (Extract Global Responsibilities)
- [√] 2.2 对齐 server 装配：确认 `yierdis-server` 完整使用 core `YierdisInstance`（删除/合并任何残留的 server 专用 runtime 语义），verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance
- [√] 2.4 统一 LRU clock：将 LRU clock 的 SSOT 明确为 instance 层（全局模式下跨 DB 单调递增），并移除 DB 内独立 clock 退化路径，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 2.2
- [√] 2.5 增加 server 级回归：多 DB + global maxmemory + shared allocator 场景可淘汰且不双计数（以 server 集成测试覆盖），verify why.md#scenario-global-maxmemory-works-across-multiple-dbs，depends on task 2.4

## 3. KeyHandle Landing (Keyspace / Expire / Scan)
- [√] 3.1 改造 `YierdisKeyspace`：新增/替换为 KeyHandle 入口（lookup/compute/remove/iterate），并显式规定“不得隐式 canonical heap copy”，verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path
- [√] 3.2 heap keyspace：实现 heap `KeyHandle` 的 keyspace 落地（复用 canonical `byte[]`，避免额外复制），并补齐单元测试，verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 3.1
- [√] 3.3 off-heap keyspace：实现 address `KeyHandle` 的 keyspace 落地（addr/len/hash），并补齐单元测试，verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 3.1
- [√] 3.4 命令层 key 传递：优先 `YierdisBytesView` → `KeyHandle` 路径，减少 `RespCommand.toByteArray()`（热路径以 view/handle 查找），verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path，depends on task 3.1
- [√] 3.5 `YierdisExpireIndex`：新增 handle 友好 API（set/get/remove/random），先落地 heap 路径，verify why.md#scenario-key-identity-is-stable-across-backends，depends on task 3.2
- [√] 3.6 `YierdisExpireIndex`：落地 off-heap keys 路径（引用 keyspace handle/address，不额外分配 canonical heap key bytes），verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path，depends on task 3.3
- [√] 3.7 引入 `ScanCursorV2`（rehash-aware）对象模型与序列化规则（bulk string 数字兼容），verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.1
- [√] 3.8 Keyspace iterator：实现“可 time-slice 的迭代 API”（支持 COUNT hint 与 rehash 双表），verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.7
- [√] 3.9 `SCAN` 命令：切换为 iterator + cursorV2，并增加回归覆盖 rehash/插入/删除/过期，verify why.md#scenario-scan-cursor-can-always-make-progress，depends on task 3.8
- [√] 3.11 off-heap keys 热路径回归：`GET/EXISTS/TYPE/TTL` 在 keysOffHeapEnabled 下不触发 canonical heap copy（基于分配计数/断言/可观测 hook），verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path，depends on task 3.4

## 4. Ledger Landing (Reserve → Mutate → Commit)
- [√] 4.1 定义 ledger 的 SSOT 口径：哪些 bytes 计入预算、哪些仅做解释性统计、off-heap 如何计入，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible
  > Note: 已在 `helloagents/wiki/modules/db.md` 的 MemoryLedger/maxmemory 小节补齐口径说明与可观测字段定义。
- [√] 4.3 将 `prepareWrite` 重构为 `ledger.reserve(estimatedExtra)`（内部包含 expire cleanup + eviction），并保持“reply 前拒写”，verify why.md#scenario-reject-happens-before-reply-is-written-no-double-reply，depends on task 4.1
- [√] 4.4 将 `enforceMaxmemory` 重构为 ledger 驱动（后台/周期性强制），verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.3
  > Note: `YierdisDb.enforceMaxmemory()` 已收敛为 `ledger.reserve(0)` 的 best-effort 维护语义。
- [√] 4.5 为每类写命令定义 `estimatedExtraBytes` 策略（保守上界）：SET/APPEND/LPUSH/RPUSH/HSET/SADD/ZADD/PFADD，verify why.md#scenario-reject-happens-before-reply-is-written-no-double-reply，depends on task 4.3
- [√] 4.6 命令层迁移：逐个命令替换为 ledger reserve（先 String/Hash/List/Set/ZSet，再 HLL），verify why.md#scenario-reject-happens-before-reply-is-written-no-double-reply，depends on task 4.5
- [√] 4.7 DB 内 delta 统一：收敛 used/estimated 的维护点，确保异常路径可 rollback，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.3
- [√] 4.8 可观测输出统一：`MEMORY STATS`/`INFO` 输出 ledger 权威字段，并移除 legacy 口径字段，verify why.md#scenario-memory-accounting-is-single-source-and-reproducible，depends on task 4.7
  > Note: `MEMORY STATS` 输出 `ledger_*`/`effective_used_bytes_for_maxmemory`；`INFO` 输出 `yierdis_ledger_*` 与 maxmemory 可解释指标；并由 `MemoryStatsCommandTest` 锁定字段集合稳定性。

## 5. Regression & Quality Gate
- [√] 5.4 移除 legacy 兼容模式与对应回归（cursorV1/legacy ledger/legacy keyhandle），确保全量测试仍通过，verify how.md#adr-004-删除-legacy-模式cursorv1legacy-ledgerlegacy-keyhandle
- [√] 5.5 off-heap 回归：锁定 keysOffHeapEnabled 下读路径分配（尽可能用分配计数/基准断言）与 usedBytes 泄漏锚点，verify why.md#scenario-off-heap-keys-read-path-has-zero-heap-allocation-hot-path，depends on task 3.11

## 6. Security Check
- [√] 6.1 Execute security check（per G9：输入净化、错误净化、资源释放、off-heap 生命周期、拒写路径一致性）
  > Note: 本轮变更不连接生产服务、不引入明文密钥；错误输出做 CR/LF 过滤与限长；off-heap allocator 生命周期由 instance/db 统一收敛并补齐回归。

## 7. Documentation Update (Knowledge Base)
- [√] 7.1 更新 `helloagents/wiki/arch.md`：补齐 instance/runtime、KeyHandle、Ledger、CursorV2、事件/快照、shard/router/slow command 的依赖方向与边界，verify how.md#architecture-design
- [√] 7.2 更新 `helloagents/wiki/modules/db.md`：将 KeyHandle/Ledger/CursorV2 写为唯一 SSOT，并更新 maxmemory/scan/expire 的可观测字段说明，verify how.md#architecture-decision-adr
- [√] 7.4 更新 `helloagents/wiki/modules/server.md`：补齐 instance/shard/router/slow command 治理职责边界与配置项，verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance

## 8. Production Extension Preconditions (M4 Guardrails)
- [√] 8.1 定义“生产能力扩展前置条件”清单：持久化/复制/ACL/模块接入必须遵守冻结契约，verify why.md#scenario-add-persistence-without-rewriting-db-core-contracts
  > Note: 以 `helloagents/wiki/arch.md` 的 Guardrails 小节作为 SSOT（snapshot/change sink/ledger/off-heap/threading）。
- [√] 8.2 设计并实现变更事件/快照接口（供 AOF/RDB/replication 复用），不侵入 keyspace 实现细节，verify why.md#scenario-add-persistence-without-rewriting-db-core-contracts，depends on task 8.1
  > Note: 变更事件以命令 argv 形式落地（`YierdisChangeSink`/`YierdisChangeEvent`）；快照以 `ScanCursorV2` 分批推进（`YierdisSnapshot`/`YierdisSnapshotEntry`，当前对 STRING 提供 value bytes）。
- [√] 8.3 增加“事件流/快照接口”回归测试：确保不泄漏内部实现细节且可被持久化/复制消费，verify why.md#scenario-add-persistence-without-rewriting-db-core-contracts，depends on task 8.2

## 9. Execution Model & Isolation Strategy (Production Readiness)
- [√] 9.1 冻结“执行模型与扩展策略”ADR：单线程 DB 语义不变，扩展通过 instance 层 shard/router（默认单 shard；可选多 shard），verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance
- [√] 9.2 明确跨 shard 命令边界与策略（限制/失败语义/可选支持列表），并写入 wiki（arch + server/db 模块），verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance，depends on task 9.1
  > Note: 已在 `helloagents/wiki/arch.md` 的“执行模型与扩展策略（冻结）”小节给出 shard/router 的默认策略与跨 shard 命令边界（含例子与失败语义）。
- [√] 9.3 设计并实现慢命令治理接口（时间预算/分页/限流/观测），优先覆盖 `KEYS/SCAN` 与大范围返回类命令，verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance，depends on task 9.1
  > Note: `KEYS` 引入 `SlowCommandGovernor`（默认 20ms 预算）并在 `YierdisDb.keys(..., maxMatches, budgetNanos)` 内 fail-fast；`SCAN` 维持 cursorV2 + time-slice（COUNT→maxSteps）语义。
- [√] 9.4 增加慢命令/隔离回归测试：确保高负载下 executor 可用性与延迟不被长尾放大，verify why.md#scenario-slow-command-isolation-does-not-stall-the-whole-instance，depends on task 9.3
  > Note: 回归覆盖 `KeysBudgetTest`（time budget/result limit fail-fast）；executor drain time budget/fair scheduling 已由既有 server tests 覆盖。

## 10. DB Core Decomposition (Reduce Storage-Command Coupling)
- [√] 10.1 定义拆分边界与命名：`ValueOps`/`ExpirationManager`/`EvictionCoordinator`/`MemoryLedger`/`DbEngine`（保留 `YierdisDb` 入口），verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator
  > Note: 已新增 `yier.bubu.redis.ops.*` 边界（`DbEngine/ValueOps/*Ops/ExpirationManager/EvictionCoordinator`）并由 `YierdisDb` 作为 orchestrator 实现。
- [√] 10.2 建立 `ops` 包与最小接口（先 StringOps）：将 `set/get/append/strlen/bitops` 从 `YierdisDb` 迁移为组件实现，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.1
- [√] 10.3 增加 StringOps 回归测试与性能 smoke（确保语义不变且无额外分配），verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.2
  > Note: 语义回归由现有 STRING/bitops 单测覆盖；零拷贝/零 canonical heap copy 由 `OffHeapKeysZeroCopyReadPathTest` 锁定热路径基线。
- [√] 10.4 逐类迁移（HashOps/ListOps/SetOps/ZSetOps/HllOps）：每次只搬一个类型，保持每次可回滚，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.2
- [√] 10.5 过期/淘汰/记账进一步组件化：将 `cleanupExpired/evictUntilUnder/touch/usedBytesForMaxmemory` 等逻辑迁移到 `ExpirationManager/EvictionCoordinator/MemoryLedger`，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.4
- [√] 10.6 最终收敛：`YierdisDb` 仅保留线程语义、组件装配与跨组件编排；补齐 ADR 细节与文档，verify why.md#scenario-yierdisdb-shrinks-to-an-orchestrator，depends on task 10.5

## 11. Testing
- [√] 11.1 `mvn test` 全绿（含 server + core 全量回归），并记录关键回归覆盖点，verify why.md#risk-assessment
  > Note: 已在 repo 根目录执行 `mvn test` 并通过；新增/关键回归点包括 `OffHeapKeysZeroCopyReadPathTest`、`ScanCursorContractTest`、`MemoryLedgerContractTest`、`KeyHandleContractTest` 等。
