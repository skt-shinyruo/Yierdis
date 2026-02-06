# Change Proposal: RESP 解析 SSOT 收敛与质量兜底

## Requirement Background

当前 RESP 协议解析在多个层次存在“多路径实现 + 回退逻辑”的组合：

1. `yierdis-protocol-netty` 的 request decoder（`RespCommandDecoder`）包含 fast-path、materialize 回退、skip/scan 逻辑，以及 RESP2/RESP3 混合兼容（attributes/streamed/scalar 等）分支。
2. `yierdis-protocol-netty` 的 reply frame decoder（`RespDecoder`）也存在一套独立的 skip/scan（含 streamed/attributes）。
3. `yierdis-protocol` 的 `RespObjectParser`（用于 CLI/测试/调试）又实现了一套解析与 streamed 处理逻辑。

这种“手写状态机 + 哨兵值/异常回退”的做法在性能上合理，但质量上容易出现：
- **分支爆炸**：可读性与可维护性下降，修改风险增大。
- **语义重复**：同一协议语义在多处实现，容易出现 SSOT 漂移（行为不一致但不易被发现）。

因此需要把“RESP wire 级别 scan/skip 的语义”收敛为单一来源，并用 **golden cases + fuzz + round-trip（encode→decode）** 锁死行为边界，降低漂移概率。

## Change Content

1. **协议层 SSOT：RESP wire scan/skip 收敛到 `yierdis-protocol`**
   - 引入 Netty-free 的 RESP wire 工具（基于 `BytesSource`/`RespFrame`），统一：
     - CRLF 扫描、ASCII 数字解析（int/long）、limits 校验
     - `skipOne`（含 streamed blob / streamed aggregates / attributes / push / map / set / scalar 等）
2. **Netty adapter 复用 SSOT：减少 `protocol-netty` 的重复语义实现**
   - `RespDecoder`（reply 切帧）委托给 SSOT 的 skipper 计算 frame endIdx（wire-preserving）。
   - `RespCommandDecoder` 在 attributes/streamed/skip-scan 分支委托给 SSOT（保留 bulk-array fast-path 以避免性能回退）。
3. **测试矩阵锁定行为（质量兜底）**
   - Golden cases：覆盖 attributes 链式包裹、streamed blob、streamed aggregates、inline vs array 严格性、protocol error → close、limits 一致性、中途截断/半包、skip/scan 回退正确性。
   - Round-trip：同时覆盖 wire 字节级一致与语义一致（解析后可被 `RespObjectParser` 正确理解）。
   - 短 fuzz：纳入默认 `mvn test`（≤3s），固定 seed，失败输出 seed 可复现。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（SSOT：RESP wire support / skipper / parser 语义一致性）
  - `yierdis-protocol-netty`（Netty adapter：`RespDecoder` / `RespCommandDecoder` 复用 SSOT）
  - `yierdis-server`（集成用例：protocol error → close 等行为回归保障，尽量复用现有测试）
  - `helloagents/wiki`（模块说明与 ADR 同步）
- **Files（重点，非穷尽）：**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`（用于 round-trip/语义一致性验证）
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/*`
  - `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/*`
  - `yierdis-server/src/test/java/yier/bubu/redis/*`
- **APIs:**
  - 内部协议工具 API（新增）：wire scan/skip 支持（供 decoder/parser 复用）
  - 对外行为：解码/切帧结果与错误策略保持兼容（通过测试锁定）
- **Data:** 无

## Core Scenarios

### Requirement: SSOT Wire Skipper（协议 scan/skip 语义收敛）
**Module:** protocol

将 RESP wire 级别的“扫描 CRLF、解析数字、skipOne（含 streamed/attributes）”收敛为 Netty-free SSOT，避免多处重复实现带来的漂移风险。

#### Scenario: Attributes 链式包裹 + streamed 类型
- 输入包含 `|...|...<value>` 的链式 attributes（attributes 内部值允许复杂嵌套/streamed）。
- 期望：
  - skipper 能正确跳过 attributes map，并定位真实 value/命令的边界；
  - limits（bulk/array/nesting/line）一致生效；
  - 对截断数据返回“需要更多数据”的信号（供 Netty streaming adapter 回滚 readerIndex）。

### Requirement: Netty Decoder Delegation（adapter 复用 SSOT）
**Module:** protocol-netty

`RespDecoder`（reply framing）与 `RespCommandDecoder`（request decoding）在 skip/scan/streamed/attributes 分支复用 SSOT wire skipper，减少分支爆炸与语义重复。

#### Scenario: reply 切帧（`RespDecoder`）wire-preserving + 半包恢复
- 任意合法 reply（含 streamed/attributes）应被切成单个 frame，且 frame bytes 与输入 wire 完全一致。
- 输入分片（半包/截断）时应等待更多数据；禁止 readerIndex 漂移或死循环。

#### Scenario: request 解码（`RespCommandDecoder`）严格性 + 回退路径一致
- inline vs array：top-level 非 `*` 仍按 inline 解析（Redis-like），但命令形态保持“只接受 bulk/scalar arg”的严格性策略。
- streamed blob/string 与 streamed aggregates 在 request 场景可被正确跳过/必要时 materialize。
- protocol error 需与 server 语义一致：返回 `-ERR Protocol error: ...` 并关闭连接（由集成测试锁定）。

### Requirement: Test Matrix Lock-In（golden/round-trip/fuzz）
**Module:** protocol-netty / protocol / server

以测试作为最后护栏，确保未来在优化/重构分支时不会引入行为漂移。

#### Scenario: Golden cases + round-trip + 短 fuzz（默认 mvn test）
- Golden：覆盖 attributes/streamed/strictness/limits/半包/skip-scan fallback。
- Round-trip：`RespObject` encode → `RespDecoder` decode（wire 相等）→ `RespObjectParser` parse（语义可解析）。
- Fuzz：固定 seed、≤3s、失败打印 seed，可最小化输入复现。

## Risk Assessment

- **Risk:** 性能回退（把 ByteBuf 特化扫描改为通用 BytesSource 扫描可能变慢）
  - **Mitigation:** 保留 bulk-array fast-path；SSOT skipper 主要用于 attributes/streamed/skip-scan 分支；必要时增加微基准与热点验证。
- **Risk:** 错误消息/关闭连接语义改变导致兼容性回归
  - **Mitigation:** 以现有 server/codec 测试为基线扩展 golden cases；保持 `Protocol error:` 文案稳定；protocol error → close 由集成测试锁定。
- **Risk:** 流式/截断输入处理不当导致死循环或 readerIndex 漂移
  - **Mitigation:** 增加半包/截断/随机分片 fuzz；关键路径强制“要么前进要么返回等待”。

