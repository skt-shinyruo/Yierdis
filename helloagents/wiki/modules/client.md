# client

## Purpose

提供内置的极简 RESP2 CLI 客户端，便于本地调试与脚本化测试。

## Module Overview

- **Responsibility:** 连接管理、命令输入、回复解码（frame-based）、输出显示（支持 hex；按需解析）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-17

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

## Dependencies

- `yierdis-protocol-netty`（客户端侧复用 RESP codec；回复解码输出 `RespFrame`）
- Netty（连接管理与 IO）

## Change History

- 2026-01-15：依赖切换：RESP codec 下沉到 `yierdis-protocol-netty`，`yierdis-protocol` 保持 Netty-free SSOT。
- 2026-01-16：行为加固：`execute()` 超时后关闭连接并标记 client 不可复用，避免 RESP FIFO 响应错配风险。
- 2026-01-17：协议栈收敛：client/CLI 收敛为 frame-based 回复（`RespDecoder` 输出 frame）；对象模型解析仅用于输出/调试（例如 CLI 展示）。
