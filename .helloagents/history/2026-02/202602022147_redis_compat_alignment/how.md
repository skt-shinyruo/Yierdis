# Technical Design: Redis 兼容性对齐与功能扩展（RESP3 / 事务 / 全局 maxmemory）

## Technical Solution

### Core Technologies
- Java 17
- Netty（TCP I/O、pipeline、单线程执行器）
- Picocli（server/client 参数解析）
- RESP 协议实现：`yierdis-protocol`（SSOT）+ `yierdis-protocol-netty`（adapter）

### Implementation Key Points

1. **RESP3 写出语义统一**
   - 将 `RespWriter` 作为 RESP2/RESP3 写出语义的 SSOT。
   - 扩展 `yierdis-protocol-netty` 的 `RespEncoder`，覆盖所有 `RespObject` 类型（含 RESP3 set/boolean/double/bignum/verbatim/blob error/push/attribute/null）。
   - 增加 round-trip 测试：`RespObject` → `RespEncoder` → `RespDecoder` → `RespObjectParser`，确保一致性与可解析性。

2. **CLI 输出增强（RESP3 友好展示）**
   - CLI 当前对 array/map 展示较友好；补齐 set/boolean/double/bignum/attribute/push/verbatim/blob error 的打印分支。
   - 保持输出风格接近 `redis-cli`（逐项编号、嵌套缩进）。

3. **事务队列安全上限 + Redis 风格 EXECABORT**
   - 在连接态事务实现（`ServerConnectionState.TransactionState`）中引入：
     - `maxQueuedCommands`（条数上限）
     - `maxQueuedBytes`（累计 bytes 上限，估算口径为 argv payload bytes 总和）
     - `aborted` 状态（入队错误触发）
   - 入队时超限：返回错误并标记 aborted；EXEC 返回 `EXECABORT` 并清空队列；DISCARD 清空并复位。
   - 设计保持 core Netty-free：core 通过 `RespTransactionState` 访问连接态，必要时扩展该接口（或让 enqueue 抛出可识别异常）。

4. **maxmemory：从“按 DB 分摊”升级为“实例级全局预算”**
   - 目标：在 server 多 DB 场景下，`--maxmemoryBytes` 作为全局预算生效，并跨 DB 统一淘汰。
   - 建议引入“全局内存预算协调器”（MemoryBudget Coordinator）：
     - 运行在 command executor 单线程上（保持 DB 线程语义）
     - 统计口径：sum(各 DB heap 估算 usedBytes) + shared off-heap allocator.usedBytes（只计一次）+ 必要的结构开销估算
     - 淘汰策略：allkeys-random / allkeys-lru（近似采样）
     - 时间预算：每次 prepare/enforce 有上限，避免长尾
   - 兼容策略：引入 `--maxmemoryScope global|per-db`（默认 global），保留教学/旧行为对比能力。

5. **busy 可诊断性**
   - 增强 `ERR busy` 的原因表达：优先保持 RESP error 兼容形态，增加可读原因（例如 `ERR busy: queue_full`）。
   - 同步增强 `STATS`：确保能映射到明确的拒绝原因计数（已存在 rejected_* 字段，可补齐与 busy 输出的一致性说明）。

## Architecture Design

```mermaid
flowchart TD
    C[Client / redis-cli / yierdis-client] -->|TCP| N[Netty Pipeline]
    N --> D[RespCommandDecoder (RESP2 + inline)]
    D --> H[YierdisFastCommandHandler]
    H --> Q[NettyCommandExecutor Queue + Backpressure]
    Q --> X[Single-thread Executor]
    X --> P[YierdisFastCommandProcessor]
    P --> DB[YierdisDb (per logical DB)]
    P --> W[RespWriter (SSOT)]

    subgraph Optional
        B[Global MemoryBudget Coordinator]
        T[Transaction Queue Limits]
    end

    P --> B
    P --> T
```

## Architecture Decision ADR

### ADR-001: 以 RespWriter 作为 RESP2/RESP3 写出 SSOT
**Context:** server fast-path 使用 `RespWriter`，但 `RespEncoder` 的覆盖不完整，未来扩展 RESP3 容易出现写出语义漂移。  
**Decision:** 在 Netty codec 侧写出 `RespObject` 时也统一复用 `RespWriter` 语义，补齐所有类型分支。  
**Rationale:** 单一 SSOT 降低分支漂移风险，测试也可统一用 round-trip 验证。  
**Alternatives:** 直接在 `RespEncoder` 手写协议编码 → Rejection reason: 维护成本高，易与 `RespWriter` 漂移。  
**Impact:** `RespEncoder` 需要扩展类型覆盖；新增协议 round-trip 测试。

### ADR-002: MULTI 入队错误采用 Redis 风格 EXECABORT
**Context:** 当前 MULTI 队列无上限，容易堆 OOM；且缺少“入队阶段错误如何影响 EXEC”的一致语义。  
**Decision:** 引入 `maxQueuedCommands/maxQueuedBytes`，入队超限返回错误并标记 transaction aborted；EXEC 返回 EXECABORT 并清空队列。  
**Rationale:** 更接近 Redis 的用户心智模型，同时提供硬限制防止 OOM。  
**Alternatives:** 超限直接自动 DISCARD 并退出 MULTI → Rejection reason: 行为偏离 Redis，且可能让客户端误以为事务仍有效。  
**Impact:** `RespTransactionState`/`ServerConnectionState` 需要扩展；增加事务超限回归测试。

### ADR-003: maxmemory 由 per-db 分摊升级为 global 全局预算（可保留兼容开关）
**Context:** 当前按 DB 分摊与 Redis 实例级口径不一致；多 DB 下使用体验与预期偏差大。  
**Decision:** 引入全局预算协调器，实现跨 DB 的 allkeys-random/allkeys-lru 近似淘汰；提供 `maxmemoryScope` 兼容开关。  
**Rationale:** 更贴近 Redis；同时保留教学对比能力与兼容迁移路径。  
**Alternatives:** 继续 per-db 分摊但增强文档解释 → Rejection reason: 不能满足“更贴近 Redis”的目标。  
**Impact:** 需要跨 DB 的 eviction/统计；`MEMORY STATS` 与 `INFO MEMORY` 口径调整；可能影响既有测试预期。

### ADR-004: busy 错误保持兼容形态但增强原因表达
**Context:** `ERR busy` 对客户端不透明，定位依赖日志/统计。  
**Decision:** busy 错误仍以 `-ERR ...` 形式返回，但增加原因短码；并在 wiki 中建立“原因→调参项”映射表。  
**Rationale:** 兼容 `redis-cli` 的错误处理，同时降低排障成本。  
**Alternatives:** 使用非标准 error type 或 push 消息 → Rejection reason: 兼容性差、实现复杂。  
**Impact:** 需要在拒绝路径携带原因，补齐文档与测试。

## API Design（CLI Args）

> 本项目无 HTTP API，主要对外接口是 TCP/RESP 与 server CLI 参数。以下为新增/调整建议。

- 新增（建议）：
  - `--transactionQueueMaxCommands <n>`：MULTI 队列最大命令条数（0 表示不限制，建议给出安全默认值）
  - `--transactionQueueMaxBytes <bytes>`：MULTI 队列累计 payload bytes 上限（0 表示不限制）
  - `--maxmemoryScope global|per-db`：maxmemory 口径选择（默认 global）
- 调整（语义对齐）：
  - `--maxmemoryBytes`：从“按 DB 分摊”调整为“实例级全局预算”（global 模式下）
  - `MEMORY STATS`：输出口径调整为全局（或提供兼容参数/子命令，避免破坏已有依赖）

## Security and Performance

- **Security：**
  - MULTI 队列上限（条数/bytes）作为 DoS 防护的一部分，避免用户可控输入导致 OOM。
  - RESP3 error/blob error 等消息统一净化，避免 CRLF 注入造成 response splitting。
  - 参数校验：新增 CLI args 必须在 parse/normalize 阶段做边界检查（>=0、上下界与互斥规则）。
- **Performance：**
  - RESP3 编码统一后，保持 fast-path：编码尽量写入 `ByteBuf`，避免额外对象树分配。
  - 全局淘汰采用采样近似 + 时间预算，避免在高压写入时引入长尾延迟。
  - busy 原因短码应来自已有计数/状态，避免在热路径做字符串拼接（可预置 byte[]）。

## Testing and Deployment

- **Testing：**
  - 协议一致性测试：RespEncoder round-trip（覆盖 RESP3 类型与嵌套结构）
  - 事务测试：MULTI 超限、EXECABORT、DISCARD 复位、连接关闭清理
  - maxmemory 测试：global vs per-db 行为差异（多 DB 写入触发淘汰、INFO/MEMORY STATS 口径）
  - busy 测试：触发 queue_full/bytes_budget/not_running 的拒绝路径，并验证原因/统计可对应
- **Deployment/验证：**
  - 使用 `scripts/smoke.sh` 做最小链路回归
  - 使用 `scripts/bench.sh` 做 backpressure/吞吐变化对比（重点观察 busy 比例与 tail latency）

