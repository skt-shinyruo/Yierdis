# protocol-netty

## Purpose

为 `yierdis-server` / `yierdis-client` / `yierdis-bench` 提供 Netty 侧的自定义协议编解码与适配层：在不让 SSOT 模块（`yierdis-protocol` / `yierdis-core`）直接依赖 `io.netty.*` 的前提下，复用 Netty pipeline 的解码能力。

归属：`yierdis-protocol-netty`（`yier.bubu.redis.protocol.netty.*`），作为 **Netty adapter**（非 SSOT：允许依赖 Netty）。

## Module Overview

- **Responsibility:** Custom Protocol v1 decoder（server）+ NDJSON line decoder（client/bench）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-09

## Specifications

### Requirement: Custom Protocol v1 request decoder（server）
**Module:** protocol-netty

`CustomRequestDecoder` 将 ByteBuf 字节流解码为协议无关 `Command`（当前为 `CustomCommand`）：

- framing：`<len>:<json>\\n`
  - `<len>`：十进制 ASCII，表示 JSON payload 的 UTF-8 字节长度
  - `<json>`：JSON object，schema 严格（`cmd` string；`args` array，元素仅允许 `string|null`）
- payload 读取与解析（低拷贝）：
  - 默认路径：基于 `ByteBuf` slice 读取 payload，避免在 decoder 中按 `len` 分配整帧 heap `byte[]`
  - JSON 解析：优先走 `ByteBuffer` 视图（direct buffer 场景），或直接使用底层数组视图（heap buffer 场景）
  - 退化路径：当 payload 无法暴露为单段 NIO buffer（例如 composite/cumulation 形态）时，回退为受 `maxPayloadBytes` 约束的临时拷贝解析（行为与旧实现一致，但仅在少数场景触发）
- 安全上限：
  - `maxPayloadBytes`：限制单帧 payload 字节数
  - `maxHeaderBytes`：限制 header 扫描长度（防 DoS）
  - `maxArgs`：限制 argv 长度（防 DoS）
- 错误模型（尽量可恢复）：
  - 解码/校验失败：decoder 只输出 `ProtocolError` 事件（不直接写回 NDJSON reply）
  - 回包：由上层 pipeline handler 统一调用协议层 encoder/writer 编码为 `ok=false` 的 NDJSON error envelope（避免重复与漂移）
  - resync：header 级错误会进入 discard-to-LF，尽量丢弃到下一帧边界（`\\n`）后继续读取后续帧
  - 兜底：若长时间无法找到边界（discard 超上限），允许断连（DoS 防护）

建议的 server pipeline 形态（示意）：
- `CustomRequestDecoder` → `ProtocolErrorReplyHandler` → `YierdisFastCommandHandler`

### Requirement: NDJSON line decoder（client/bench）
**Module:** protocol-netty

`JsonLineDecoder` 负责将 inbound bytes 按 `\\n` 切分为单行 `byte[]`（去掉末尾 `\\n` 与可选 `\\r`）：

- `maxLineBytes` 限制单行最大长度，避免 reply flood 导致 OOM
- 上层可按需用 `JsonParser` 解析 JSON（或按前缀做轻量校验）

## Dependencies

- `yierdis-protocol`（`Command/ReplyWriter` 抽象 + `CustomCommand` + JSON codec）
- `yierdis-bytes-netty`（`ByteBuf` ↔ bytes 适配）
- Netty（buffer/codec/pipeline）

## Change History

- 2026-02-06：新增 Custom Protocol v1 Netty codec（`CustomRequestDecoder` + `JsonLineDecoder`），对外协议采用 JSON framing。
- 2026-02-09：`CustomRequestDecoder` payload 读取低拷贝化（slice + `ByteBuffer` 解析），并补充退化路径说明。
