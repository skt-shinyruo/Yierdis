<!-- migrated_from: history/2026-02/202602031537_resp3_full_coverage/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: RESP3 全覆盖（request + reply + streamed + push + attributes）

## Requirement Background

项目当前的 RESP3 支持处于“可用但不完整”的状态：

- reply 侧（server → client）：已覆盖 RESP3 的常见类型（map/set/push/attribute/boolean/double/null/verbatim/blob error 等），但 **streamed strings / streamed aggregates** 仍未支持（遇到 `$?` / `*?` / `%?` / `~?` 会直接 protocol error）。
- request 侧（client → server）：主路径仍是 RESP2 `*` + `$`；此前为兼容性做过 attributes/标量的增量，但这并不是“完整 RESP3”。
- client/bench：当前客户端模型是“严格 1 请求 ↔ 1 响应 FIFO”，如果连接出现 RESP3 push（异步 out-of-band）会存在响应错配风险。

你明确要求：
1) request + reply 全面对齐 RESP3（含 push/attributes/streamed 类型）  
2) 以最新版规范/实现为准  
3) 支持 streamed strings + streamed aggregates  
4) 严格按 Redis 的错误模型与边界（协议错误应返回 `ERR Protocol error...` 并关闭连接）  
5) 允许引入通用 RESP3 解析（正确性优先，性能可后置）

## Change Content

1. **RESP3 streamed 支持（SSOT）**：在 `yierdis-protocol` 层补齐 streamed strings（`$? ... ;0`）与 streamed aggregates（`*?/%?/~? ... .`）的语义解析，使 `RespObjectParser` 能完整解析 RESP3 全类型。
2. **RESP3 framing/decoder 全覆盖**：在 `yierdis-protocol-netty` 的 `RespDecoder`（frame decoder）补齐 streamed skip 能力，确保 client/server 在 Netty pipeline 层能切出完整 frame。
3. **request 侧完整 RESP3 解码**：实现“严格 Redis 风格”的 request decoder：只接受“命令形态”（array/inline），并支持 attributes 包裹 array、以及 blob string 的 streamed 形式（必要时 materialize）。
4. **push/attributes 对齐 client 模型**：增强 `yierdis-client` 使其能在 RESP3 push 存在时保持 request/response 不错配，并提供可用的 push 处理/暴露方式（例如回调或独立队列）。
5. **测试矩阵锁定行为**：新增/扩展协议测试，覆盖 streamed string、streamed aggregate、attributes 包裹、push 乱序到达、以及 protocol error → close 的行为。
6. **知识库 SSOT 同步**：更新 `helloagents/wiki/*` 与 README，明确 RESP3 支持范围、严格性策略、以及与 Redis 当前“是否使用 streamed”之间的边界说明（以本项目实现为准）。

## Impact Scope

- **Modules:**
  - yierdis-protocol（SSOT：类型/解析/写出语义）
  - yierdis-protocol-netty（Netty codec：frame decoder/encoder/request decoder）
  - yierdis-server（pipeline 装配/协议错误 close 行为一致性）
  - yierdis-client（push 处理、RESP3 展示）
  - yierdis-bench（如需：push 兼容与解析）
  - helloagents/wiki（文档同步）
- **Files（重点，非穷尽）：**
  - yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java
  - yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java
  - yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java（可能重构/替换）
  - yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java
  - yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java
- **APIs（广义）：**
  - TCP/RESP：request/reply 的可接受类型集合与严格性（protocol error → close）
  - client push 事件处理方式（新增接口/行为）
- **Data:** 无（协议/网络层变更）

## Core Scenarios

### Requirement: R1_RESP3 streamed strings / aggregates 全解析
**Module:** protocol / protocol-netty

#### Scenario: S1_streamed blob string
条件：收到 `$?` 开头的 streamed blob string（chunk 由 `;len` 声明，` ;0` 结束）。
- 期望结果：
  - 能完整切帧与解析为等价的 blob string 值（并受 maxBulkBytes 限制）
  - 半包/粘包下不误判，不越界，不泄漏缓冲区

#### Scenario: S2_streamed aggregates（array/map/set）
条件：收到 `*?` / `%?` / `~?` 开头的 streamed aggregate，使用 `.\r\n` 结束。
- 期望结果：
  - 能完整切帧与解析为等价的 array/map/set（并受 maxArrayLen/maxNestingDepth 限制）
  - map 语义保持严格：元素数必须为偶数，否则视为协议错误

### Requirement: R2_request 侧完整 RESP3 命令解码（严格模式）
**Module:** protocol-netty / server

#### Scenario: S1_attributes 包裹命令 + streamed 参数
条件：客户端发送 `|<attrs>` 包裹 `*<cmd>`，其中参数使用 `$?` streamed blob string。
- 期望结果：
  - server 能正确解析并执行命令（attributes 可忽略或按策略保存，但不影响命令语义）
  - 非命令形态输入（top-level 非 array/inline）返回 protocol error 并关闭连接

### Requirement: R3_push/attributes 不破坏 client 的 request/response 配对
**Module:** client / protocol-netty

#### Scenario: S1_push 与 reply 乱序到达
条件：连接在等待某个命令 reply 的过程中，先收到 `>` push，再收到真正的 reply（或相反）。
- 期望结果：
  - push 被路由到 push handler（或独立队列）
  - request 的 reply 仍能被正确取回，不发生错配/超时后的连接未知状态

## Risk Assessment

- **Risk：实现复杂度高，协议角落多（streamed + nesting + push）。**
  - Mitigation：以 `yierdis-protocol` 为 SSOT；用“切帧→对象解析→行为断言”的测试矩阵锁定；对所有路径加上限与回滚语义。
- **Risk：与 Redis 当前“是否实际使用 streamed”存在认知/文档差异。**
  - Mitigation：以本项目目标为“RESP3 规范全覆盖 + Redis 风格严格错误模型”；在 README/wiki 明确说明“本项目支持 streamed（即使 Redis 核心命令未使用）”。

