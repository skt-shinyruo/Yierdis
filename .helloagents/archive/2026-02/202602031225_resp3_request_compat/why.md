<!-- migrated_from: history/2026-02/202602031225_resp3_request_compat/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: RESP3 Request 兼容性对齐（decoder 支持 attributes + scalar）

## Requirement Background

当前 server 的 request 解码器 `RespCommandDecoder` 以 RESP2 的 `* array-of-bulk-strings` 作为主路径，并额外支持 inline command（调试用）。为了避免把 RESP reply 前缀误判为 inline，现有实现会对常见 RESP3 类型前缀直接判定 `Protocol error`。

这在“客户端严格按 RESP2 发送请求”的路径下没有问题，但当客户端/代理尝试以 RESP3 request 形态发送（尤其是带 `|` attribute 前缀、或数组元素使用 RESP3 标量类型）时，会被 decoder 直接拒绝，导致连接层面不兼容。

本变更目标是：在不破坏现有 fast-path（零拷贝 argv slice）的前提下，扩展 request 解码能力，使其对 RESP3 request 更兼容，并尽量对齐 Redis 生态的使用边界。

## Change Content

1. 扩展 `RespCommandDecoder`：支持跳过 RESP3 `| attribute` 前缀（忽略 attributes map，仅将其后跟随的命令作为 request 解析）。
2. 扩展命令数组元素支持：在 `* array` 内允许 RESP3 标量类型（例如 `_ / + / : / # / , / ( / =`），并将其映射为 argv 的 bytes view（保持二进制安全与零拷贝 slice）。
3. 增加协议回归测试：覆盖 attributes 包裹请求、RESP3 标量参数、以及 top-level 严格性（仍拒绝非法前缀/非命令形态）。
4. 同步知识库：更新 “协议边界” 与 `protocol-netty` 模块说明，确保 SSOT 以代码为准。

## Impact Scope

- **Modules:**
  - yierdis-protocol-netty
  - helloagents/wiki（文档同步）
- **Files（重点，非穷尽）：**
  - yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java
  - yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderStrictnessTest.java
  - yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderResp3RequestTest.java（新增）
  - helloagents/wiki/overview.md
  - helloagents/wiki/modules/protocol-netty.md
- **APIs（广义）：**
  - TCP/RESP request 兼容边界（更宽松的 RESP3 request 接受范围）
- **Data:** 无（仅协议解析）

## Core Scenarios

### Requirement: R1_RESP3 request 解码兼容
**Module:** protocol-netty

将 RESP3 request（attributes + 标量）解析为 `RespCommand(argv)`，对上层命令处理保持无感。

#### Scenario: S1_attributes 前缀不再破坏连接
条件：客户端发送 `| attribute` + `* command array`。

- 期望结果：
  - decoder 能正确跳过 attributes map
  - 后续命令仍按正常 pipeline 执行（不触发 `Protocol error`）

#### Scenario: S2_数组元素支持 RESP3 标量类型
条件：客户端发送 `* command array`，其中部分参数不是 `$ bulk string`，而是 RESP3 标量（例如 `:123` / `_` / `=verbatim` 等）。

- 期望结果：
  - decoder 能将标量内容映射到 argv bytes（零拷贝 slice）
  - 对不支持的复杂类型（map/set/push/attribute 作为参数）保持明确错误（避免把结构体误当参数）

#### Scenario: S3_top-level 仍保持防误解析的严格性
条件：客户端发送 top-level 非命令形态（例如 `_`、`%`、`>` 等）或控制字符前缀。

- 期望结果：
  - 仍返回 `Protocol error`（避免把 reply/无效数据误判为 inline command）

## Risk Assessment

- **Risk：request decoder 接受范围扩大可能引入“非预期输入被解析”的行为差异。**
  - Mitigation：仅放宽 `| attribute`（作为前缀）与 `* array` 内的标量类型；top-level 仍坚持 “命令必须是 array/inline” 的策略；同时保持上限参数（maxArgs/maxBulkBytes/maxLineBytes/maxNestingDepth）防止 DoS。

