<!-- migrated_from: history/2026-02/202602061216_resp_codec_test_matrix/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: RESP 解析质量兜底（Golden + Round-trip + Fuzz + 一致性差分）

## Requirement Background

当前 RESP 协议解析在仓库内呈现“多路径实现 + 多层级回退”的典型形态：

1. request 解码侧（`yierdis-protocol-netty`：`RespCommandDecoder`）包含 fast-path、materialize 回退、skip/scan 逻辑，以及 RESP2/RESP3 混合兼容（attributes/streamed/scalar 等）分支。
2. reply 解码侧（`yierdis-protocol-netty`：`RespDecoder`）以切帧为主，但依赖 wire skipper 对 streamed/attributes 等形态进行 scan/skip。
3. 语义解析侧（`yierdis-protocol`：`RespObjectParser`）用于 CLI/测试/调试，具备对 RESP3 streamed/attributes/aggregate 的对象化解析能力。

这类“手写状态机 + 哨兵值/异常回退”的解码器在性能上常见且合理，但质量上会带来两个持续风险：
- **分支爆炸**：可读性下降，修改点多且容易漏测。
- **语义重复**：同一协议语义在多处实现/复用不充分时，会导致 SSOT 漂移（行为不一致且难以察觉）。

因此本变更优先以测试作为最终护栏：通过 **黄金用例（golden cases）+ 一致性差分（differential）+ round-trip（encode→decode→parse）+ 轻量 fuzz** 锁定行为边界，降低未来迭代引入漂移的概率。

## Change Content

1. **黄金用例矩阵（Golden cases）**
   - 覆盖 attributes 链式包裹、streamed blob、streamed aggregates、inline vs array 严格性、limits 一致性、半包/截断恢复、skip/scan 回退正确性等关键路径。
2. **Round-trip 兜底（encode→decode→parse）**
   - 使用既有 `RespWriter/RespEncoder` 生成 wire bytes；
   - 通过 `RespDecoder` 切帧（wire-preserving）；
   - 再由 `RespObjectParser` 解析，确保“切帧后的 wire”可被 SSOT parser 正确理解。
3. **一致性差分测试（Differential consistency）**
   - 对同一份 wire 输入，校验 `RespWireSkipper` 的 skip 结果、`RespDecoder` 的切帧结果、`RespObjectParser` 的解析结果之间的一致性（至少在“可解析/可切帧/可 skip”层面一致）。
4. **轻量 fuzz（默认纳入 `mvn test`）**
   - 固定 seed、有限迭代次数、时间预算 ≤3s；
   - 核心覆盖：随机分片（packetization）、随机合法输入组合、以及对截断输入的稳健性（不得死循环/readerIndex 漂移）。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（SSOT：`RespWireSkipper`/`RespObjectParser` 的一致性验证与边界测试）
  - `yierdis-protocol-netty`（`RespCommandDecoder`/`RespDecoder` 的 golden/round-trip/fuzz 测试）
  - `yierdis-server`（协议错误 close / 非致命错误 keep-open 的集成回归）
  - `helloagents/wiki`（策略与 ADR 记录）
- **Files（重点，非穷尽）：**
  - `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespWireSkipperTest.java`
  - `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespObjectParserStreamedTest.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderResp3RequestTest.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespDecoderStreamedTest.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespRoundTripTest.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCodecFuzzTest.java`（新增）
  - `yierdis-server/src/test/java/yier/bubu/redis/FastPipelineTest.java`
  - `yierdis-server/src/test/java/yier/bubu/redis/Resp3StreamedRequestIntegrationTest.java`
- **APIs:** 无对外 API 变更（以测试为主；必要时仅新增测试辅助工具）
- **Data:** 无

## Core Scenarios

### Requirement: Request 解码的兼容性与严格性锁定
**Module:** protocol-netty / server
锁定 `RespCommandDecoder` 的行为边界，避免 fast-path/materialize/skip-scan 分支在后续改动中产生漂移。

#### Scenario: attributes 链式包裹（含 streamed 值）可被正确跳过
输入形如 `|...|...*...`，attributes map 内部 value 允许 streamed blob / streamed aggregates。
- attributes 被完全跳过，不进入命令 argv 语义层
- 后续真实命令可被正确解码（半包/分片输入也应稳定）

#### Scenario: streamed blob / streamed aggregates 的 request 形态行为稳定
输入包含 `$?\r\n...;0\r\n` 作为参数，或 `*?\r\n...\r\n.\r\n` 作为 streamed command array。
- 解码输出 argv bytes 与语义预期一致
- 对非法 chunk/非法终止符触发 protocol error（由 server 侧关闭连接策略兜底）

#### Scenario: inline vs array 严格性（Redis-like top-level inline）
top-level 非 `*` 时按 inline command 解析；array 形态的参数仍维持“只接受 bulk/scalar”的策略。
- 非法控制字符/明显 malformed wire → protocol error（应 close）
- 业务级别/参数校验错误（如 null bulk string key）→ 返回 ERR，但连接保持可用（keep-open）

### Requirement: Reply 切帧 wire-preserving 与 streamed/attributes 组合稳定
**Module:** protocol-netty / protocol

#### Scenario: `RespDecoder` 对 streamed/attributes 组合切帧正确且可半包恢复
- 任意合法 reply（含 streamed/attributes/nested aggregates）被切成单一 frame
- frame bytes 与输入 wire 完全一致（wire-preserving）
- 分片输入不会导致 readerIndex 漂移/死循环

### Requirement: Limits 口径一致性与协议错误策略可回归
**Module:** protocol / protocol-netty / server

#### Scenario: limits（bulk/array/nesting/line）在多实现处口径一致
- `RespLimits` 默认值作为 SSOT
- 超限输入触发 `Protocol error:`（错误类别一致；必要时文案仅校验前缀/关键词，避免过拟合）

#### Scenario: protocol error 是否 close 行为被集成测试锁定
- fatal protocol error：必须 close
- 非 fatal（命令可被解码但参数非法）：返回 ERR，连接保持 open

### Requirement: Round-trip + 短 fuzz 作为回归兜底
**Module:** protocol-netty / protocol

#### Scenario: encode→decode→parse 的双口径兜底
- encoder 输出 wire 经 decoder 切帧后保持一致
- 切帧后的 wire 可被 `RespObjectParser` 解析（用于调试/测试 SSOT）

#### Scenario: 轻量 fuzz（固定 seed）覆盖随机分片与合法组合
- 重点验证：无死循环、无 readerIndex 漂移、无资源泄漏迹象（最佳努力）

## Risk Assessment

- **Risk:** fuzz 用例引入不稳定（随机性导致偶发失败或耗时波动）
  - **Mitigation:** 固定 seed；限制迭代次数与 payload 大小；设置总耗时预算（≤3s）；失败打印 seed 便于复现。
- **Risk:** 测试过度依赖错误文案导致脆弱
  - **Mitigation:** 对错误文案采用“类别/前缀/关键字”断言；对 close 行为用集成测试锁定。
- **Risk:** 只加测试但未立即减少分支复杂度
  - **Mitigation:** 先用测试锁边界，再按测试护航逐步收敛/重构重复分支（降低一次性重构风险）。

