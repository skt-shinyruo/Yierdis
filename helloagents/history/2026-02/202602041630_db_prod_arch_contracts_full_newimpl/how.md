# Technical Design: DB 生产级契约落地（全量切换新实现）

## Technical Solution

### Core Technologies
- Java 17 + Maven
- JUnit 4（回归与契约测试）
- off-heap：`yierdis-offheap` / `YierdisOffHeapAddressAllocator`
- RESP 协议：`yierdis-protocol` / server：`yierdis-server`

### Implementation Key Points
1. **Key identity SSOT**：以 `KeyHandle` 统一 heap/off-heap 的 key 表示；内部路径禁止依赖 `byte[] canonicalKey` 作为 SSOT。
2. **TTL index SSOT**：ExpireIndex 支持基于 `KeyHandle` 的 set/get/remove/random，并能在 rehash 下配合 keyspace iterator。
3. **SCAN SSOT**：引入可 time-slice 的 keyspace iterator + cursor v2（rehash-aware）；cursor 序列化保持“数字 bulk string”兼容。
4. **Memory budget SSOT**：写路径以 ledger.reserve 为拒写点（reply 前拒写），commit/rollback 覆盖异常路径；maxmemory 预算、淘汰、过期清理由 ledger 统一编排。
5. **server 治理边界**：server 仅负责执行/装配/治理（budget/分页/限流/观测）；核心语义留在 core。
6. **组件化拆分**：将 `YierdisDb` 的存储/过期/淘汰/记账/值操作逐步拆分为组件接口，确保每次改动可回滚且覆盖回归。

## Architecture Design

```mermaid
flowchart TD
    subgraph Server[yierdis-server]
        EXEC[Executor / Governance]
        BOOT[Bootstrap / Wiring]
        ROUTER[Router / Shard]
    end

    subgraph Core[yierdis-core]
        INST[YierdisInstance (SSOT)]
        DB[YierdisDb (Orchestrator)]
        KS[Keyspace (KeyHandle)]
        EX[ExpireIndex (KeyHandle)]
        LED[MemoryLedger (SSOT)]
        EVICT[EvictionCoordinator]
        EXP[ExpirationManager]
        OPS[ValueOps/*]
        SCAN[KeyspaceIterator + ScanCursorV2]
        SNAP[Event/Snapshot API]
    end

    BOOT --> INST
    EXEC --> INST
    ROUTER --> INST
    INST --> DB
    DB --> KS
    DB --> EX
    DB --> LED
    DB --> EVICT
    DB --> EXP
    DB --> OPS
    DB --> SCAN
    DB --> SNAP
```

## Architecture Decision ADR

### ADR-001: KeyHandle 作为 key identity SSOT（移除 canonical heap key 依赖）
**Context:** off-heap keys 热路径需要避免 canonical heap copy；现有 `canonicalKey(...) -> byte[]` 容易在查找/过期/扫描中触发复制。  
**Decision:** keyspace/expires/scan 统一以 `KeyHandle` 作为内部 SSOT；对外仍可接受 `YierdisBytesView`/`byte[]`，但内部立即转为 handle/view 路径。  
**Rationale:** 把“key identity”与“key bytes copy”解耦；让 off-heap keys 的存储与查找都只依赖 `(addr,len,dictHash)` 或 `(byte[],dictHash)`。  
**Alternatives:** 保留 `canonicalKey` 并尝试缓存 heap copy → Rejection reason: 仍会产生 copy，且引入生命周期/一致性复杂度。  
**Impact:** 需要修改 keyspace/expire/scan 相关接口与调用链；需要新增回归来锁定“不产生 canonical heap copy”。  

### ADR-002: MemoryLedger 作为 maxmemory/拒写点 SSOT（reserve → commit/rollback）
**Context:** `usedBytes` 分散维护 + `prepareWrite/enforceMaxmemory` 的 best-effort 逻辑容易漂移；异常路径 rollback 难以保证一致。  
**Decision:** 写路径以 ledger.reserve 作为拒写点，commit/rollback 覆盖 mutate 成功/失败路径；`MEMORY STATS/INFO` 以 ledger 为权威口径。  
**Rationale:** 将预算判定与错误语义收敛到单点，避免双 reply 与预算漂移。  
**Alternatives:** 继续维护 `usedBytes` 并补更多注释/测试 → Rejection reason: 无法从结构上避免漂移与漏 rollback。  
**Impact:** 需要改造 DB 写路径与全局 maxmemory 协调器的统计/淘汰策略。  

### ADR-003: ScanCursorV2（rehash-aware）+ time-slice iterator
**Context:** cursor v1 在 rehash 下缺少显式模型；缺少 iterator SSOT 导致 scan 行为难以稳定验证。  
**Decision:** 引入 cursor v2（仍以数字字符串序列化），并提供 keyspace iterator（支持 COUNT hint、rehash 双表、time-slice）。  
**Rationale:** 把“可推进性/终止条件/rehash 处理”从命令层下沉到 keyspace SSOT，实现可测试的行为。  
**Alternatives:** 继续在命令层做 best-effort 扫描 → Rejection reason: 逻辑分散且难以覆盖 rehash 细节。  
**Impact:** 需要更新 `SCAN` 实现与回归；需要调整 off-heap keyspace 以暴露 iterator 所需信息。  

### ADR-004: 删除 legacy 模式（cursorV1/legacy ledger/legacy keyhandle）
**Context:** 多模式共存会导致维护成本指数增长，且与“SSOT 契约冻结”目标冲突。  
**Decision:** 默认切到新实现，移除 legacy 切换与兼容回归；对外 RESP 行为保持兼容，但不提供旧模式回退。  
**Rationale:** 单路径可维护，测试覆盖更集中。  
**Alternatives:** 保留 feature flag 作为回滚 → Rejection reason: 与当前明确要求冲突。  
**Impact:** 需要同步更新文档与历史回归描述。  

### ADR-005: 事件流/快照接口作为持久化/复制/ACL/模块接入前置条件
**Context:** 后续引入 AOF/RDB/replication/ACL/modules 需要稳定、非侵入的扩展点；不能泄漏 keyspace 内部实现。  
**Decision:** 定义变更事件与快照接口（只暴露契约化模型），先提供回归基座，不在本次启用真实持久化。  
**Rationale:** 先建立 guardrails，避免未来为扩展而重写 DB 内核。  
**Impact:** 新增接口与回归，server 文档明确扩展边界。  

### ADR-006: DB core decomposition（降低存储-命令耦合）
**Context:** `YierdisDb` 单体导致扩展与回归成本高，难以做到“每次只动一个小组件”。  
**Decision:** 引入 `ValueOps/ExpirationManager/EvictionCoordinator/MemoryLedger/DbEngine` 等边界，逐类迁移并让 `YierdisDb` 收敛为 orchestrator。  
**Rationale:** 支持可回滚的小步重构，限制改动面。  
**Impact:** 多阶段迁移，需要稳定回归与文档同步。  

## Security and Performance
- **Security:**
  - 不连接任何生产服务；不引入明文密钥/Token；删除 legacy 路径时保证错误信息净化不泄漏内部细节
  - off-heap 生命周期：handle/iterator 只能在调用期使用，禁止跨请求持有
- **Performance:**
  - 热路径优先使用 `YierdisBytesView`/`KeyHandle` 传递，禁止 canonical heap copy
  - SCAN/KEYS 等慢命令提供预算/分页/限流/观测接口，避免阻塞 instance

## Testing and Deployment
- **Testing:**
  - `mvn test` 全量回归
  - 新增关键回归：keysOffHeapEnabled 下 `GET/EXISTS/TYPE/TTL` 不触发 canonical heap copy；SCAN cursor v2 在 rehash/变更数据集下可推进；ledger 拒写点/rollback/无双 reply
  - server 集成：bootstrap 资源归属与 close 顺序不 double-close
- **Deployment:**
  - 仅代码级变更，无外部依赖升级；部署风险主要来自行为回归，需在 CI/预发环境先跑全量回归与压测
