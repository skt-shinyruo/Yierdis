# client

## Purpose

提供内置的极简 RESP CLI 客户端，便于本地调试与脚本化测试。

## Module Overview

- **Responsibility:** 连接管理、命令输入、回复解码（frame-based）、输出显示（支持 hex；按需解析）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-02

## Specifications

### Requirement: 便捷调试
**Module:** client
提供单次执行与交互两种模式，默认连接 `127.0.0.1:6378`。

#### Scenario: 二进制输出观察
条件：用户使用 `--hex`
- 预期：bulk string 用 hex 输出，便于观察二进制数据结构（例如 bitmap/hll）

### Requirement: 超时后连接不可复用（避免响应错配）
**Module:** client
由于 RESP 请求/响应是严格 FIFO 配对，若单次执行等待响应超时，连接会进入“响应可能延迟到达”的未知状态。
因此 client 在超时后会关闭连接并标记不可复用，避免后续请求响应错配。

### Requirement: response queue 边界化（资源上限 + 异常唤醒）
**Module:** client
client 采用“单请求-单响应”模型（不支持 pipelining），但仍需防御异常路径导致的回复堆积：
- response queue 采用 **有界队列**（防止对端 flood 或协议错配导致 OOM）
- 当队列溢出：立即关闭连接，并向等待线程投递 terminal 信号（避免一直阻塞到超时）
- 当连接断开/异常：同样投递 terminal 信号，确保 `execute()` 能尽快返回错误

### Requirement: RESP3 reply 解码兼容（最小集合）
**Module:** client
client/CLI 基于 `yierdis-protocol-netty` 的 frame decoder 解析 server reply，为了对齐 Redis 生态工具链（以及测试断言），需要识别 RESP3 常见 reply 前缀的最小集合：
- collection：`%`（map）、`~`（set）、`>`（push）、`|`（attribute）
- scalar：`#`（boolean）、`,`（double）、`(`（big number）、`=`（verbatim string）、`!`（blob error）

已知限制：
- client 仍然是“单请求-单响应”模型，**不支持 PubSub/订阅模式**；即使 decoder 能识别 `>` push，仍可能出现“push 抢占 reply/响应错配”的语义问题，需要在后续里程碑引入 push 分流与 subscribe API。

## Dependencies

- `yierdis-protocol-netty`（客户端侧复用 RESP codec；回复解码输出 `RespFrame`）
- Netty（连接管理与 IO）
- picocli（CLI 参数解析）

## Change History

- 2026-01-15：依赖切换：RESP codec 下沉到 `yierdis-protocol-netty`，`yierdis-protocol` 保持 Netty-free SSOT。
- 2026-01-16：行为加固：`execute()` 超时后关闭连接并标记 client 不可复用，避免 RESP FIFO 响应错配风险。
- 2026-01-17：协议栈收敛：client/CLI 收敛为 frame-based 回复（`RespDecoder` 输出 frame）；对象模型解析仅用于输出/调试（例如 CLI 展示）。
- 2026-01-17：CLI 参数解析收敛到 picocli；response queue 边界化（有界队列 + 溢出关闭 + close/exception 唤醒）。
- 2026-02-02：RESP3 reply 扩展：decoder/解析器补齐 map/set/push 等常见类型，提升生态兼容与测试可观测性（client 仍保持单请求-单响应）。
