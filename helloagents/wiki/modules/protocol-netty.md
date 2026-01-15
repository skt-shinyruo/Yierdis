# protocol-netty

## Purpose

为 `yierdis-server` / `yierdis-client` / `yierdis-bench` 提供 Netty 侧的 RESP codec 与适配层：在不让 SSOT 模块（`yierdis-protocol` / `yierdis-core`）直接依赖 `io.netty.*` 的前提下，复用 Netty pipeline 的编解码能力。

归属：`yierdis-protocol-netty`（`yier.bubu.redis.protocol.*`），作为 **Netty adapter**（非 SSOT：允许依赖 Netty）。

## Module Overview

- **Responsibility:** Netty codec（`RespCommandDecoder` / `RespDecoder` / `RespEncoder`）+ `RespFrame/RespSession` 的 Netty 实现（frame ownership / release）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-15

## Specifications

### Requirement: 命令解码（RESP2 multi-bulk + inline）
**Module:** protocol-netty

将 Netty `ByteBuf` 字节流解码为 `RespCommand`：
- 支持 RESP2 multi-bulk（`*<argc> ...`）作为主路径
- 支持 inline command（调试用，兼容 `sdssplitargs` 风格：引号/转义/`\\xHH`）
- 保持参数 **二进制安全**：bulk string 不强制 UTF-8 解码

### Requirement: 连接级 RESP2/RESP3 协议状态（session）
**Module:** protocol-netty

RESP2/RESP3 的协商属于连接级状态：
- Netty 侧通过 `NettyRespSession(Channel)` 将状态绑定到连接（典型实现为 `Channel.attr`）
- SSOT 的 `RespWriter` 仅依赖 `RespSession` 抽象，不直接依赖 Netty

### Requirement: ByteBuf ownership/release（泄漏风险控制）
**Module:** protocol-netty

在 codec 与业务之间必须形成明确的资源所有权边界：
- `NettyRespFrame` 封装 `ByteBuf`，并在 `close()` 时执行 `release()`
- 业务侧仅通过 `RespFrame` 抽象读取 bytes view，并在命令结束后调用 `RespCommand.recycle()` 触发 frame 回收
- 异常路径必须同样保证 recycle/close（避免 ByteBuf 泄漏）

## Dependencies

- `yierdis-protocol`（RESP 对象模型 + `RespFrame/RespSession` 抽象 + `RespWriter`）
- Netty（buffer/codec/pipeline）

## Change History

- 2026-01-15：从 `yierdis-protocol` 拆分：将 Netty codec/adapters 下沉到 `yierdis-protocol-netty`，SSOT 模块保持 Netty-free。
