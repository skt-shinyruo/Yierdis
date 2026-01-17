# 技术设计：Yierdis 架构深度重构（Pipeline 化 + 执行器组件化 + 连接态解耦）

## Technical Solution

### Core Technologies
- Java 17
- Netty（TCP + ByteBuf）
- RESP2/最小 RESP3（HELLO 3 + map/null）
- JUnit 4（单元/集成测试）

### Implementation Key Points
1. **显式 Pipeline（四段）**
   - Decode：`RespCommandDecoder` 输出 `RespCommand`
   - Schedule：执行器接管并做排队/预算/反压
   - Execute：单线程执行命令（调用 `YierdisFastCommandProcessor`/`YierdisDb`）
   - Encode：使用 `RespWriter` 直接写回 ByteBuf，并通过 flush 合并降低 syscalls
2. **执行器组件化（保持语义不变）**
   - `QueuePolicy`：GLOBAL/FAIR 的排队与轮询抽象
   - `BytesBudget`：以 `RespFrame.retainedBytes()` 作为统一预算口径
   - `BackpressureController`：条数 + bytes 双水位线（滞回）+ autoRead 管控
   - `FlushCoalescer`：每 tick 聚合 flush targets
   - `FrameCompactor`：仅对“长度小但 retained 大”的 frame 做保守 copy
   - `Stats`：热路径计数器与 INFO/STATS 输出对齐
3. **连接态解耦**
   - `ConnectionContext` 仅保留：
     - 协议协商（`RespProtocol`）
     - 连接级统计容器（通用/可观测）
   - 执行器调度状态迁移到 server：
     - 方案 A：server 私有 `Channel.attr`（清晰分层）
     - 方案 B：server 在 `ConnectionContext` 之外附加 state 对象（仍保持“单一入口读取”习惯）
4. **协议严格性统一**
   - request decoder 只允许：
     - `*`（array of bulk strings）
     - inline command（sdssplitargs）
   - 对任何“RESP reply 前缀/RESP3 类型前缀/不可打印控制字符”统一返回 protocol error
5. **测试优先、分阶段落地**
   - 优先补测试锁定语义，再拆分组件，避免“改着改着不知道对不对”

## Architecture Design

```mermaid
flowchart TD
  subgraph NettyPipeline[Netty Channel Pipeline]
    D[RespCommandDecoder]
    H[YierdisFastCommandHandler]
  end

  subgraph Executor[Server Executor (single thread)]
    S[Scheduler/QueuePolicy]
    B[BytesBudget]
    P[BackpressureController]
    C[FrameCompactor]
    E[Execute Command]
    F[FlushCoalescer]
  end

  subgraph Core[SSOT]
    CP[YierdisFastCommandProcessor]
    DB[YierdisDb]
    W[RespWriter + RespSession]
  end

  D --> H --> S
  S --> B --> P --> C --> E --> F
  E --> CP --> DB
  E --> W
```

## Architecture Decision ADR

### ADR-20260117-01：将执行器拆为 Pipeline + 组件（而非继续在单类内演进）
**Context：**`NettyCommandExecutor` 已承载过多职责，新增功能会进一步增加复杂度并降低可测试性。  
**Decision：**引入显式 Pipeline，并将执行器拆为多个可独立验证的组件；保持 DB 单线程语义与现有反压/预算策略不变。  
**Rationale：**组件化能把“正确性与性能风险”隔离，允许用小范围测试锁定行为，并降低未来改动的爆炸半径。  
**Alternatives：**
- 继续在单类里增加注释与局部重构 → 拒绝原因：复杂度仍集中、难形成可复用/可测试边界  
**Impact：**需要新增类与测试；短期改动面增大，但长期维护成本下降。

### ADR-20260117-02：ConnectionContext 仅表达协议会话，执行器状态归属 server
**Context：**`ConnectionContext` 当前同时承载协议协商与执行器调度状态，形成跨层语义耦合。  
**Decision：**将执行器调度状态迁移到 server 私有 state；`ConnectionContext` 仅保留协议会话与通用统计容器。  
**Rationale：**降低层间耦合，让 protocol-netty 只承担 codec/session 适配职责；执行器可独立演进。  
**Alternatives：**
- 继续把所有状态放在 `ConnectionContext` → 拒绝原因：语义耦合持续扩大，未来更换执行策略牵连协议层  
**Impact：**需要调整 state 存取点、修订 INFO/STATS 与测试；但边界更清晰。

### ADR-20260117-03：请求解码严格化（保留 inline，拒绝 RESP reply/非法前缀）
**Context：**某些非法输入可能被当作 inline 命令导致“unknown command”而非 protocol error。  
**Decision：**显式维护“允许的 request 首字节集合”，对 RESP reply/RESP3 前缀统一抛 protocol error；必要时可选择 close 连接。  
**Rationale：**更易诊断、更接近 Redis 的错误语义，避免误导。  
**Impact：**可能影响极少数依赖“宽松解析”的调试输入，但整体更安全可控。

## Security and Performance
- **Security：**
  - **输入校验：**request decoder 仅接受 `*` array-of-bulk-strings 与 inline command；对 RESP reply（含常见 RESP3 类型前缀）与控制字符前缀统一判为 `Protocol error`，避免误路由为 inline 导致执行层状态错乱。
  - **DoS 上限：**继续保持 `maxBulkBytes/maxArgs/maxLineBytes` 上限（协议输入护栏），并在 server 执行器侧同时保留“条数(capacity) + bytes(retainedBytes)”双预算，防止少量大 bulk 积压导致驻留不可解释或 OOM。
  - **错误消息净化：**异常返回路径统一走 `safeErrorMessage(...)` 做 CR/LF 过滤与限长；protocol error 返回 `ERR Protocol error: ...` 并关闭连接，避免 decoder 状态不一致与重复错误导致的资源占用。
  - **资源释放：**`RespCommand.close()` 在执行器 `finally` 保障执行；submit 失败路径由 caller 负责 close（busy/关闭均覆盖）；frame compaction 通过 `RespCommandBuilder.replaceFrame(...)` 替换时关闭旧 frame，避免 ByteBuf 泄漏；`shutdownGracefully()` 会 drain backlog 并回收帧与预算。
- **Performance：**
  - **组件化：**将预算与 compaction 抽出为轻量组件，不改变单线程命令语义与背压闭环；热路径仍以原子计数与复用容器为主，避免引入高频对象分配。
  - **compaction：**仅在 retained/length 显著不成比例且 frame 长度 ≤ `maxCopyBytes` 时触发，避免对大 payload 复制；拷贝目标缓冲区为精确大小（降低 pooled 大底层 buf 驻留）。
  - **调度：**FAIR 策略仅在启用时使用 per-channel round-robin；调度 state 下沉为 server 私有 `Channel.attr`，避免 protocol 层携带调度策略细节。
  - **flush 合并：**维持“同一轮 drain 内对同一连接 write 聚合，末尾统一 flush”的策略（每 tick 每 channel 至多一次 flush），降低 syscalls 与 event-loop 抖动。

## Testing and Deployment
- **Testing：**
  - `mvn test` 作为基线
  - 新增集成测试覆盖：protocol error 语义、QUIT/backlog、反压滞回、bytes budget、compaction、资源释放
  - 可选：针对 retained bytes 与 off-heap usedBytes 的回归锚点测试
- **Deployment：**
  - 分阶段落地：先加测试与护栏 → 再拆分组件 → 再做行为严格化与边界收敛
