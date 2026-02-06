# protocol-netty

## Purpose

为 `yierdis-server` / `yierdis-client` / `yierdis-bench` 提供 Netty 侧的 RESP codec 与适配层：在不让 SSOT 模块（`yierdis-protocol` / `yierdis-core`）直接依赖 `io.netty.*` 的前提下，复用 Netty pipeline 的编解码能力。

归属：`yierdis-protocol-netty`（`yier.bubu.redis.protocol.netty.*`），作为 **Netty adapter**（非 SSOT：允许依赖 Netty）。

## Module Overview

- **Responsibility:** Netty codec（`RespCommandDecoder` / `RespDecoder`）+ `RespFrame/RespSession` 的 Netty 实现（frame ownership / release）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-06

## Specifications

### Requirement: 命令解码（RESP2 multi-bulk + inline）
**Module:** protocol-netty

将 Netty `ByteBuf` 字节流解码为 `RespCommand`：
- 支持 RESP2 multi-bulk（`*<argc> ...`）作为主路径（Redis 生态最常见请求形态）
- 支持 inline command（调试用，兼容 `sdssplitargs` 风格：引号/转义/`\\xHH`）
- 兼容 RESP3 request（严格命令形态）：
  - 允许在命令前携带 `|` attributes（忽略 attributes map，仅解析其后真实命令）
  - 允许在 `*` 命令数组内使用部分 RESP3 标量类型（例如 `+`/`:`/`_`/`#`/`,`/`(`/`=`）作为参数，并映射为 argv bytes view（保持二进制安全）
  - 支持 `$?` streamed blob string 作为参数（chunk 非连续时会 materialize 为连续 argv bytes）
  - 支持 `*?` streamed array 作为命令容器（直到 `.` END；受 `maxArgs` 上限保护）
- top-level 非 `*` 按 inline command 解析（Redis-like）：包括看起来像 RESP2/RESP3 reply 前缀的文本（例如 `%0`、`#t`、`_` 等）也会被当作命令名 token，通常在执行层返回 unknown command 并保持连接可用（避免把“误用/探测输入”升级为 fatal protocol error）
- 仍对控制字符与结构性 malformed request 判为 `Protocol error`，并由 server handler 返回 `-ERR Protocol error: ...` 后关闭连接（防止状态错乱与资源占用）
- 保持参数 **二进制安全**：bulk string 不强制 UTF-8 解码
- 支持输入上限参数化：`maxBulkBytes/maxArgs/maxLineBytes`（与 server args SSOT 对齐，避免 DoS 风险）
 - attributes 跳过路径额外受嵌套深度限制（`RespLimits.DEFAULT_MAX_NESTING_DEPTH`），避免恶意构造的深层结构体导致 decode 长尾

### Requirement: reply 切帧（RespDecoder）支持 streamed
**Module:** protocol-netty

`RespDecoder` 的职责是“切帧而非语义解析”：从 ByteBuf 中定位一个完整 RESP reply，并输出 `NettyRespFrame`（zero-copy slice）。

为实现 RESP3 全覆盖并适配生态代理/客户端的边界输入，reply 侧切帧补齐 streamed 类型：
- streamed blob string：`$? ... ;0`
- streamed aggregates：`*?/%?/~? ... .`
- attributes（`|...`）依旧被视为“attributes + 后续 reply”组成的一个逻辑 frame（避免上层将 attributes 与真实 reply 错配）

严格性与安全约束：
- 半包/粘包场景下必须可回滚并等待更多数据（不允许误吞字节导致后续错帧）
- streamed 累计长度/元素数/嵌套深度受 `maxBulkBytes/maxArrayLen/maxNestingDepth` 保护
- 协议错误统一抛出 `Protocol error: ...` 由 server handler 返回 `-ERR ...` 并关闭连接（Redis 风格）

### Requirement: 连接级 RESP2/RESP3 协议状态（session）
**Module:** protocol-netty

RESP2/RESP3 的协商属于连接级状态：
- Netty 侧通过 `ConnectionContext`（实现 `RespSession`）将状态绑定到连接（典型实现为 `Channel.attr`）
- SSOT 的 `RespWriter` 仅依赖 `RespSession` 抽象，不直接依赖 Netty
- 说明：`ConnectionContext` **仅承载协议会话**（RESP2/RESP3）；server 侧背压/统计/closing 等运行时连接状态属于服务端实现细节，收敛到 server 模块的 `ServerConnectionState`；执行器调度 state 属于 server 内部实现细节，收敛到 `NettyExecutorChannelState`（避免 protocol 模块被调度策略绑死）

### Requirement: 编码输出语义收敛（writer SSOT）
**Module:** protocol-netty

为避免 server fast-path（`RespWriter`）与 Netty codec（`RespEncoder`）的输出行为漂移：
- `RespEncoder` 仅作为 Netty adapter：写出时内部调用 `RespWriter`（通过 `NettyByteBufSink` 适配到 `ByteBuf`）
- CR/LF 净化与限长等安全语义以 `RespWriter` 为唯一权威
 - `RespEncoder` 的类型覆盖必须与 `RespObject`/`RespWriter` 保持一致（含 RESP3 扩展类型），并通过单测锁定

### Requirement: ByteBuf ownership/release（泄漏风险控制）
**Module:** protocol-netty

在 codec 与业务之间必须形成明确的资源所有权边界：
- `NettyRespFrame` 封装 `ByteBuf`，并在 `close()` 时执行 `release()`
- 业务侧仅通过 `RespFrame` 抽象读取 bytes view，并在命令结束后调用 `RespCommand.recycle()` 触发 frame 回收
- 异常路径必须同样保证 recycle/close（避免 ByteBuf 泄漏）
- `NettyRespFrame` 额外提供：
  - `length()`：逻辑帧长度（稳定，不随底层 `ByteBuf` capacity 变化）
  - `retainedBytes()`：更接近真实驻留内存的估算（考虑 slice/pooled buf），供 server 执行器做 backlog/backpressure 的 bytes 预算口径
- 为支持 server 侧 **frame compaction**（“以拷贝换确定性”）：`RespCommandBuilder` 提供安全的 frame 替换能力（替换时关闭旧 frame，避免泄漏）

## Dependencies

- `yierdis-protocol`（RESP 对象模型 + `RespFrame/RespSession` 抽象 + `RespWriter`）
- `yierdis-bytes-netty`（`ByteBuf` ↔ `BytesSink` 适配）
- Netty（buffer/codec/pipeline）

## Change History

- 2026-01-15：边界加固：netty codec/adapters 迁移到独立包 `yier.bubu.redis.protocol.netty`，`yierdis-protocol` 独占 `yier.bubu.redis.protocol`（消除 split-package）。
- 2026-01-16：增加 `RespFrame.retainedBytes()` 口径与 `RespCommandBuilder.replaceFrame(...)`，为执行器 bytes 预算与 compaction 提供协议层支撑。
- 2026-01-17：request 解码严格化：明确允许集合（array + inline），对控制字符与结构性 malformed request 统一 protocol error；连接态二分：`ConnectionContext` 仅表达协议会话，server 运行时连接状态迁移到 `ServerConnectionState`；`RespEncoder` 写出语义收敛为 `RespWriter`。
- 2026-02-01：reply 切帧 decoder（`RespDecoder`）扩展 RESP3 前缀覆盖（set/push/attribute/boolean/double/verbatim/blob error 等），与 `RespWriter/RespObjectParser` 的类型集合保持一致；同时补齐“把 reply 前缀当成 inline request”这一类误用的协议错误测试用例。
- 2026-02-03：request decoder 兼容扩展：支持 RESP3 `|` attributes 前缀（忽略 metadata）与 `*` 命令数组内的部分 RESP3 标量类型参数（提升对 Redis/代理的 request 兼容性）。
- 2026-02-06：补齐 RESP codec 质量兜底：wire skipper strictness/limits 边界、attributes parser 解析，以及随机分片短 fuzz 对 argv 全量断言（降低 fast-path/materialize/skip-scan 漂移风险）。
