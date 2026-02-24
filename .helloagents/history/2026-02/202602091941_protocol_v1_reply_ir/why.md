# Change Proposal: Custom Protocol v1 Reply IR（协议语义中间层）与错误模型收敛

## Requirement Background

当前协议抽象与 Custom Protocol v1 的 wire 语义存在结构性错位，导致实现分散且长期容易漂移：

1. **内部抽象偏 RESP 风格：**命令层依赖 `ReplyWriter/ReplySink`（包含 map/array header、bulk bytes、嵌套 error 等语义），但 wire 是 **JSON request + NDJSON reply**。JSON 在类型系统上无法表达 RESP 的若干语义（例如 map key 的任意类型/二进制 bulk string），只能靠实现侧 best-effort 或隐式约束。
2. **JSON object key 约束迫使 best-effort：**现有 `JsonLineReplyWriter` 为了产出合法 JSON，会在 key 位置做占位/回退（例如非 string key 的 fallback），这会让“命令层以为协议无关”的假设变弱，并掩盖错误用法（错误不会 fail-fast）。
3. **协议错误输出逻辑重复：**decoder 自己拼 JSON error envelope；handler/writer 又各自做 message 净化/限长。短期看规则相似（CRLF→space、256 截断），但**默认 message、封装位置、输出触发点**分散在多处，长期很容易出现“错误格式/净化策略不一致”的漂移。

本次改造允许 breaking change，目标是将 **Custom Protocol v1 的 reply 语义与编码规则收敛为单点 SSOT**，并将 decoder 的职责收窄为 framing/parse/resync。

## Change Content

1. 引入 **Reply IR（协议语义中间层）** 作为“命令层输出语义”的 SSOT：把 reply 的标量/聚合/bytes/error 语义显式化，避免依赖 JSON 的隐式限制。
2. 将 **NDJSON 编码规则单点化**：所有 reply（含 protocol/command/internal error）统一经由同一套 encoder 产出，禁止各处手写 JSON envelope。
3. **decoder 职责收敛**：`CustomRequestDecoder` 仅负责 framing/parse/校验与 resync，遇到协议错误只上报“protocol error 事件/消息”，不直接写回 JSON reply。
4. 明确并固化 **bytes 与 map 的 wire 表示**（breaking）：提供可验证、可测试、无歧义的 JSON 表达，彻底移除 “object key fallback / UTF-8 best-effort” 这类隐藏策略。
5. 补齐 **wire-level golden tests / error format consistency tests / protocol conformance tests**，并更新 client/bench 的解析与展示逻辑以对齐新协议。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（Reply IR + encoder + 统一错误净化/限长）
  - `yierdis-protocol-netty`（decoder 输出 protocol error 事件；移除手写 JSON error reply）
  - `yierdis-server`（pipeline/handler 统一走 encoder；集成测试/黄金测试）
  - `yierdis-client`、`yierdis-bench`（NDJSON reply 解析与展示对齐新 wire 语义）
  - `helloagents/wiki/*`（协议规范与模块边界文档同步）
- **APIs:**
  - Custom Protocol v1 reply 的 `result` 表示（breaking）
  - error value / error envelope 的一致性规则（breaking 但稳定化）
- **Data:** 无（仅协议/编码层改造）

## Core Scenarios

### Requirement: R1 Reply IR 作为协议语义 SSOT
<a id="r1"></a>
**Module:** yierdis-protocol / yierdis-core

命令层输出以 Reply IR 表达“语义形状”（标量/聚合/bytes/error），由协议层 encoder 负责映射为 NDJSON。命令层不再依赖“JSON object key 必须 string”的隐式前提。

#### Scenario: S1 map key 与 bulk bytes 全保真（无 best-effort）
<a id="r1-s1"></a>
条件：命令返回 map（例如 `HGETALL`、`MEMORY STATS`），key/value 均可能为 bulk bytes。
- 期望：wire 中 map 的表示不依赖 JSON object key；key/value 可为任意 Reply IR 值（含 bytes）。
- 期望：移除所有“非 string key → 占位/回退”的策略；错误用法必须可检测（fail-fast 或显式错误值）。

#### Scenario: S2 允许在 map key 位置输出非字符串值（可表达且可测试）
<a id="r1-s2"></a>
条件：命令层（或未来扩展）在 key 位置输出 integer/boolean/null/bytes。
- 期望：wire 表示能无歧义表达该 key（例如通过 entries 结构，而非 JSON object）。
- 期望：不再发生“key 被隐式转为字符串”这类漂移行为。

### Requirement: R2 NDJSON 编码与错误模型单点化（Encoder SSOT）
<a id="r2"></a>
**Module:** yierdis-protocol

将 reply 的 envelope、error.kind、message 净化/限长等规则集中到协议层 encoder，形成唯一 SSOT。

#### Scenario: S1 所有 error envelope 由同一 encoder 生成
<a id="r2-s1"></a>
条件：协议错误（decoder）、命令错误（`out.error`）、内部错误（handler/executor）。
- 期望：输出 envelope 字段、error.kind、message 规则一致，且可通过测试锁定。
- 期望：避免 decoder/handler/writer 各自拼 JSON 导致的长期漂移。

#### Scenario: S2 CRLF 注入防护与 message 截断策略一致
<a id="r2-s2"></a>
条件：错误 message 包含 `\\r/\\n` 或超长内容。
- 期望：统一替换 `\\r/\\n` 为单个空格，统一 256 字符截断（或在方案中定稿的上限）。
- 期望：wire-level 测试覆盖并作为回归门禁。

### Requirement: R3 decoder 仅负责 framing/parse/resync
<a id="r3"></a>
**Module:** yierdis-protocol-netty / yierdis-server

decoder 不直接写回 NDJSON reply；只在解析失败时上报 protocol error 事件/消息，并负责丢弃到下一帧边界（resync）。

#### Scenario: S1 非法帧返回 protocol error 且后续帧仍可执行
<a id="r3-s1"></a>
条件：输入包含 junk header / invalid JSON / schema invalid，后续紧跟合法帧（pipeline）。
- 期望：服务端返回 protocol error reply，并继续执行后续合法帧（连接保持可用，除非触发 DoS 上限）。

### Requirement: R4 Wire-level tests（golden + consistency + conformance）
<a id="r4"></a>
**Module:** yierdis-protocol / yierdis-protocol-netty / yierdis-server / yierdis-client

新增面向 wire 的测试体系，锁定协议输出与错误一致性，避免未来重构引入漂移。

#### Scenario: S1 golden tests 覆盖代表性 reply 形状
<a id="r4-s1"></a>
条件：覆盖 scalar、array、map、nested error、bytes（含非 UTF-8）等组合。
- 期望：NDJSON 输出逐字节对齐 golden，作为协议回归门禁。

#### Scenario: S2 error format consistency tests 覆盖所有错误来源
<a id="r4-s2"></a>
条件：decoder protocol error、handler exception、command-layer error、internal error。
- 期望：error.kind/message/envelope 的规则完全一致（仅 kind 不同），并且可重复验证。

## Risk Assessment

- **Risk:** breaking change 将影响所有 client/bench 与现有测试基线。  
  **Mitigation:** 同步更新 `yierdis-client`/`yierdis-bench` 解析与展示逻辑，并用 golden tests 锁定新协议。
- **Risk:** Reply IR 引入可能增加分配/CPU 开销（尤其是大结果集）。  
  **Mitigation:** 采用“IR 语义稳定 + encoder 单点化”的设计，但实现上尽量保持 streaming 友好；必要时为大结果集保留低分配路径，并以基准测试验证。
- **Risk:** DoS/内存风险（超大 reply/恶意输入）。  
  **Mitigation:** 维持并补强现有 payload/header/discard 上限；补充协议级测试覆盖边界条件。

