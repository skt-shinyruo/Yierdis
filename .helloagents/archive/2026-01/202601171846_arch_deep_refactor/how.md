<!-- migrated_from: history/2026-01/202601171846_arch_deep_refactor/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: 架构深度重构（协议/执行/客户端分层与 SSOT 收敛）

## Technical Solution

### Core Technologies
- Java 17（Maven 多模块）
- Netty 4.1（server/client I/O 与 pipeline）
- picocli（CLI 参数建模与校验；统一 server/bench/client 的参数体验）
- JUnit 4（单元/集成测试）

### Implementation Key Points

1. **协议会话与 server 连接态解耦**
   - `protocol-netty` 仅提供 Netty 侧的 `RespSession` 实现（RESP2/RESP3 协商 SSOT）
   - `server` 维护独立的连接级运行时状态（pending、pendingBytes、autoRead disable 标志、closing、连接级统计）
   - 以 `Channel.attr` 作为唯一绑定方式，避免跨模块共享实现细节

2. **协议上限默认值 SSOT**
   - 在 `yierdis-protocol` 中新增统一常量（例如 `RespLimits`），覆盖：
     - maxBulkBytes/maxArgs/maxLineBytes
     - maxArrayLen/maxNestingDepth（用于 reply parser/decoder 的安全上限）
   - `yierdis-args` 默认值、`protocol-netty` 的 decoder 默认构造、`RespObjectParser` 默认构造统一引用该常量
   - 增加一致性单测：任一默认值漂移都会导致测试失败

3. **编码输出路径收敛**
   - 以 `RespWriter` 作为唯一语义写出实现
   - `RespEncoder`（若保留）仅作为 adapter：把 `RespObject` 写入 `ByteBuf` 时内部调用 `RespWriter`，避免重复实现导致行为不一致
   - 明确 RESP2/RESP3 的能力边界：encoder/decoder/parser/CLI 展示需对齐“最小 RESP3 子集（map/null）”

4. **执行器组件化与可验证性重构**
   - 拆分 `NettyCommandExecutor` 的职责边界：
     - 队列/调度策略（GLOBAL/FAIR）
     - backlog 预算（tasks + bytes）
     - backpressure 控制（per-conn 与全局恢复）
     - frame compaction（提交阶段可选 copy 以降低驻留）
     - drain loop（预算控制与 flush 合并）
   - 为关键不变量提供单测：
     - slot/bytes reserve/release 配平
     - pending/pendingBytes 不出现负数（含异常路径与关闭路径）
     - autoRead 状态机滞回与全局恢复逻辑

5. **client/CLI 深度加固**
   - CLI 参数用 picocli 建模（与 server/bench 同栈），并保留现有参数兼容
   - client 的 response queue 引入边界策略（有界队列或溢出关闭连接），并在异常/断连时主动回收帧与唤醒等待线程
   - 把“退出码决策”集中在 main 层，避免 `System.exit` 蔓延到可复用逻辑

## Architecture Design

本次重构的核心是“把连接语义拆成两个正交对象”，并把默认值/写出路径收敛为 SSOT。

```mermaid
flowchart LR
  subgraph ProtocolSSOT[yierdis-protocol (SSOT, Netty-free)]
    Limits[RespLimits (defaults)]
    Writer[RespWriter (唯一语义写出)]
    ObjParser[RespObjectParser (调试/CLI)]
  end

  subgraph ProtocolNetty[yierdis-protocol-netty (adapter)]
    Sess[Netty RespSession impl (RESP2/RESP3)]
    ReqDec[RespCommandDecoder]
    RespDec[RespDecoder]
    Frame[NettyRespFrame]
  end

  subgraph Server[yierdis-server]
    ConnState[ServerConnectionState (pending/backpressure/counters)]
    Exec[NettyCommandExecutor (组件化)]
    Handler[YierdisFastCommandHandler]
  end

  subgraph Client[yierdis-client]
    Cli[CLI (picocli)]
    CConn[Client (queue bounded)]
  end

  ProtocolNetty --> ProtocolSSOT
  Server --> ProtocolNetty
  Server --> ConnState
  Server --> Exec
  Client --> ProtocolNetty
```

## Architecture Decision ADR

### ADR-20260117-04: 连接级状态二分：协议会话 vs server 运行时状态
**Context:** 当前 `ConnectionContext` 承载了协议协商与多类 server 运行时语义，容易造成模块边界含混与依赖误用。  
**Decision:** `protocol-netty` 只保留 `RespSession` 实现与协议相关的最小连接态；server 运行时连接态下沉到 `yierdis-server` 私有对象。  
**Rationale:** 让“协议适配层”语义纯粹，降低维护成本；同时让 server 的背压/统计/关闭策略可以演进而不绑定到 protocol 模块。  
**Alternatives:** 继续在 `ConnectionContext` 中承载全部状态 → Rejection reason: 边界模糊、长期维护成本高、易引发抽象泄漏。  
**Impact:** 需要更新 server/executor/info/stats 等引用；需要新增/调整测试覆盖异常路径与资源释放。

### ADR-20260117-05: 协议默认上限 SSOT 收敛到 yierdis-protocol
**Context:** maxBulk/maxArgs/maxLine 等默认值分散在 args/decoder/parser，存在漂移风险。  
**Decision:** 引入 `RespLimits`（或等价常量类）作为默认上限 SSOT；args/codec/parser 默认构造全部引用；单测锁定一致性。  
**Rationale:** 把“安全上限”当成协议契约的一部分，应该由协议 SSOT 模块定义。  
**Alternatives:** 保持分散定义 → Rejection reason: 漂移难发现；review 成本高。  
**Impact:** 需要调整多个模块的默认构造与注解默认值；需要补齐一致性测试。

### ADR-20260117-06: RespWriter 成为唯一语义写出实现
**Context:** 存在多条编码路径（writer vs encoder），长期会导致行为不一致与安全策略漂移。  
**Decision:** 语义写出收敛到 `RespWriter`；`RespEncoder`（若保留）仅作为 adapter 调用 `RespWriter`。  
**Rationale:** 单点实现更易验证与加固；server/client/test/bench 行为更一致。  
**Alternatives:** 同时维护两套实现 → Rejection reason: 维护成本高、风险大。  
**Impact:** 需要改造 encoder 的测试与使用点；需要对齐 RESP3 最小子集能力与输出一致性。

## Security and Performance

- **Security:**
  - 保持现有 DoS 防护：maxBulk/maxArgs/maxLine 等硬上限必须一致并可配置
  - 输出侧安全净化保持一致：error 文本 CR/LF 处理、防止 response splitting
  - 资源所有权清晰：ByteBuf/RespFrame/off-heap 的 close/release 路径必须在异常路径也可达
- **Performance:**
  - 保持 server fast-path：命令执行仍以 `RespWriter` 直接写回，避免不必要对象分配
  - frame compaction 继续作为可选策略：仅对“小长度但 retained 大”的 frame 做保守 copy
  - client 侧引入队列边界需避免热路径过多锁竞争（优先使用轻量策略/明确关闭策略）

## Testing and Deployment

- **Testing:**
  - 单测：默认值一致性、encoder/writer 一致性、decoder 语义、连接态拆分后 executor 不变量
  - 集成：server + client/CLI 的基础命令回归（RESP2/RESP3 + inline）
  - 资源：ByteBuf 泄漏与 off-heap usedBytes 回归（重点覆盖异常路径）
- **Deployment:**
  - 以小步提交落地：每个阶段保持 `mvn test` 通过
  - 若发生行为回归：按阶段回滚（优先回滚边界拆分与 encoder 改造阶段）
