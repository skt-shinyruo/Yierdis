# protocol

## Purpose

实现 RESP2 协议的数据结构与编解码（解码为命令对象，编码为响应）。

## Module Overview

- **Responsibility:** RESP2 framing、命令解析、响应序列化
- **Status:** ✅Stable
- **Last Updated:** 2026-01-03

## Specifications

### Requirement: RESP2 命令解码
**Module:** protocol
将 TCP 字节流解码为 RESP2 命令（含 argc 与每个参数的 bytes view）。

#### Scenario: 二进制安全参数
条件：客户端发送包含任意字节的 bulk string 参数
- 预期：服务端按原样读取 bytes，不进行 UTF-8 强制解码（仅在错误提示等需要时转换）

#### Scenario: 协议错误（非法 RESP）
条件：客户端发送非法 RESP2 请求（例如非 `*` 开头、bulk string CRLF 不完整等）
- 预期：服务端返回 `ERR Protocol error: ...` 并关闭连接（防止半包/乱序状态继续污染后续解析）

#### Scenario: null bulk string 参数（`$-1`）
条件：客户端发送包含 `$-1` 的 bulk string 参数
- 预期：仅允许 `PING/ECHO` 的单参数消息为 `$-1`（返回 `(nil)`）；其余命令若出现 `$-1` 参数，返回 `ERR Protocol error: null bulk string`（避免触发 NPE/断线）

## Dependencies

- （无）

## Change History

- 2026-01-03：补充协议错误与 `$-1`（null bulk string）参数的错误处理约定。
