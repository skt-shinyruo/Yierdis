<!-- migrated_from: history/2026-01/202601071641_resp3_inline/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: 支持 RESP3 + Inline（提升 Redis 客户端兼容性）

## Requirement Background

当前服务端实现以教学/实验为目标，网络协议层仅支持 RESP2 的 multi-bulk（`*...` + `$...`）请求形式，并且 `HELLO 3` 会直接返回错误提示。  
这会导致：

- 部分客户端（尤其是默认使用 RESP3 的 `redis-cli` / 新版 SDK）在默认配置下无法顺利完成握手与探测；
- 使用 telnet / nc 等工具进行快速交互调试时，无法通过 inline command（`PING\r\n`）发起请求；
- 兼容性问题容易被误判为“服务不可用”，影响本项目作为 Redis-like 学习/对比基线的价值。

## Change Content

1. 增加 **Inline command** 请求解析能力（最小可用：空白分隔参数 + CRLF 行结束）。
2. 增加 **RESP3 协商与响应** 能力：
   - 支持 `HELLO 3`，并在连接级别记录协商后的协议版本；
   - 在 RESP3 模式下，至少对“nil / HELLO 返回结构”等关键点按 RESP3 语义返回，以保证常见客户端可用。

## Impact Scope

- **Modules:**
  - `yierdis-server`（Netty pipeline、协议解析、命令处理、响应写出）
- **Files (预计):**
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/CommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespCommandDecoder.java`（或新 decoder）
  - `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespWriter.java`（或新增 RESP3 writer）
  - `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- **APIs:** 无对外 HTTP API；仅影响 TCP/RESP 协议兼容性
- **Data:** 无持久化数据结构变更

## Core Scenarios

### Requirement: Redis 客户端默认可用（RESP3 兼容）
**Module:** `yierdis-server`
在不要求客户端显式指定 `--resp2` 的情况下，允许常见客户端完成握手与基础命令交互。

#### Scenario: `redis-cli` 默认握手
条件：客户端连接后发送 `HELLO 3`（可能在后续还会发送 `COMMAND` 等探测）。  
- 期望：服务端接受 `HELLO 3` 并返回可解析的 RESP3 响应；连接进入 RESP3 模式。

#### Scenario: RESP3 下的 nil 返回
条件：RESP3 模式下执行 `GET missing`。  
- 期望：服务端返回 RESP3 的 nil（而不是仅 RESP2 的 `$-1`），避免客户端解析失败。

### Requirement: Inline command 可用于快速调试
**Module:** `yierdis-server`

#### Scenario: telnet/nc 直接发送 `PING`
条件：客户端发送 `PING\r\n`。  
- 期望：服务端能解析并返回正确响应（`PONG`）。

## Risk Assessment

- **Risk:** 增加协议分支后可能引入兼容性回退错误（例如 RESP2 客户端被误判为 RESP3 或 inline）。  
  **Mitigation:** 保持默认 RESP2；仅在收到 `HELLO 3` 后切换；inline 仅在首字节非 `*` 时触发且受行长度限制。

- **Risk:** 新增解析路径可能带来 DoS 风险（超长行/超多参数）。  
  **Mitigation:** 复用现有上限（max args / max line bytes），并对 inline 增加显式上限与失败快速返回。

