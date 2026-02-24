<!-- migrated_from: history/2026-02/202602041128_core_embedded_instance_runtime_api/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: Core Embedded Instance Runtime API (Netty-free Instance SSOT)

## Technical Solution

### Core Technologies
- Java 17
- Maven multi-module
- `yierdis-core` 保持 Netty-free（允许依赖 `yierdis-protocol`/`yierdis-offheap-api`）
- server 侧（`yierdis-server`）继续承载 Netty、执行器、运维治理策略

### Implementation Key Points

1. **在 core 中引入 runtime 语义层（SSOT）：**
   - `YierdisInstance`：对外暴露的 embedded instance API（创建/路由/关闭）
   - `YierdisInstanceRuntime`：实例级语义与共享状态（global maxmemory/LRU clock/shared allocator view）
   - `YierdisInstanceConfig`（或 Builder）：集中配置（DB 数量、maxmemory、策略、off-heap、keysOffHeapEnabled 等）

2. **抽离 global maxmemory 协调逻辑：**
   - 从 `YierdisDb` 内部抽出协调器实现，移动到 runtime（或 db.runtime 子包）
   - `YierdisDb` 仍保持“单 DB 引擎”职责：keyspace/expires/value/memory delta
   - 通过注入 runtime 或接口，让 DB 在 global 模式下使用 instance 的 SSOT（预算判定、全局 LRU clock）

3. **保持单线程 DB 语义为 SSOT：**
   - instance 不引入并发访问 DB
   - instance 提供 `bindToCurrentThread()`（绑定所有 DB 到当前线程）以便 embed/bench/server executor 复用

4. **server 只做执行与装配：**
   - `YierdisServerBootstrap` 迁移为使用 core 的 `YierdisInstance` 创建 DB 资源
   - server 仍负责：executor 线程、定时过期清理触发、backpressure、frame compaction、metrics/slowlog

5. **兼容性策略：**
   - 保留 `YierdisDb.enableGlobalMaxmemory(...)` 作为兼容入口，但其内部实现改为委托给 runtime（避免外部调用点立刻重写）
   - 新增 API 与旧路径并存，逐步迁移（以测试与可观测输出为验收门槛）

## Architecture Design

```mermaid
flowchart TD
    subgraph Core[yierdis-core]
      I[YierdisInstance]
      R[YierdisInstanceRuntime]
      DBs[YierdisDb[]]
      Cmd[YierdisFastCommandProcessor]
      Router[YierdisDbRouter]
    end

    subgraph Server[yierdis-server]
      Boot[YierdisServerBootstrap]
      Exec[NettyCommandExecutor]
      Netty[Netty Pipeline]
      Gov[Governance: slowlog/metrics/limits]
    end

    Boot --> I
    I --> R
    I --> DBs
    I --> Router
    Router --> Cmd

    Exec --> DBs
    Netty --> Exec
    Gov --> Exec
```

## Architecture Decision ADR

### ADR-001: Place embedded instance API in yierdis-core (Netty-free)
**Context:** 目前实例装配集中在 `yierdis-server`，bench/工具/测试若想复用 DB + 命令处理器，需要重复装配与生命周期逻辑。  
**Decision:** 在 `yierdis-core` 提供 `YierdisInstance`（embedded instance API），并保持 Netty-free。  
**Rationale:** 将“实例语义与生命周期”作为 core 的稳定能力输出，减少重复实现与语义漂移；同时避免 core 反向依赖 server。  
**Alternatives:** instance API 放在 server → Rejection reason: 违背“可嵌入/Netty-free”，无法作为通用库能力输出。  
**Impact:** core 新增 runtime 包；server 迁移为调用 core instance API。

### ADR-002: Extract global maxmemory coordinator into runtime SSOT
**Context:** global maxmemory 协调器目前是 `YierdisDb` 的内部实现细节，实例级语义与单 DB 引擎耦合。  
**Decision:** 将 global 协调逻辑抽离到 `YierdisInstanceRuntime`（或其内部组件），DB 在 global 模式下只做 per-db 操作入口。  
**Rationale:** 实例级预算/淘汰/LRU clock 应作为 instance SSOT；后续引入 Ledger/KeyHandle/Cursor 时，避免再次牵动 DB 内核重构。  
**Alternatives:** 保持现状（coordinator 内嵌在 `YierdisDb`）→ Rejection reason: 实例语义无法独立演进与测试，容易导致未来大改。  
**Impact:** `YierdisDb` 增加 runtime 注入/委托点；保留兼容入口但实现迁移到 runtime。

### ADR-003: Keep DB single-thread semantics; runtime does not introduce concurrency
**Context:** Redis 生态预期与项目现有实现都依赖“单线程 DB 语义”，并通过 fail-fast 检测误用。  
**Decision:** `YierdisInstance` 仅负责装配与语义，不引入并发访问；server 通过 executor 绑定线程，embed 通过 bindToCurrentThread 绑定。  
**Rationale:** 避免在 instance API 层引入并发与锁，从根源上防止未来因为并发语义导致全面重写。  
**Alternatives:** 让 instance 直接支持多线程并发访问 DB → Rejection reason: 与 Redis 语义不一致，且会牵动 keyspace/ttl/eviction/iterator 全面重构。  
**Impact:** runtime/API 文档明确“单线程绑定”要求；测试覆盖误用 fail-fast。

## Security and Performance

- **Security:**
  - runtime 不暴露敏感信息；所有异常与错误信息保持现有净化策略（避免日志注入/信息泄漏）。
  - off-heap allocator owner 语义清晰：避免 double-close 与泄漏；close 顺序可回归验证。
- **Performance:**
  - instance 装配不引入额外热路径分配；global maxmemory 计算与淘汰保持 best-effort + 时间预算。
  - runtime 提供可观测字段（后续可在 INFO/MEMORY STATS 输出模式与关键值），便于压测与排障。

## Testing and Deployment

- **Testing:**
  - 新增 runtime/instance 级回归锚点：multi-db + global maxmemory + shared allocator 不双计数
  - 保持并复用现有 maxmemory/双 reply 回归用例，确保行为不回退
- **Deployment:**
  - server 迁移采用“等价替换”的装配方式：先保证功能与行为一致，再逐步引入更多 runtime 可观测输出与治理能力
