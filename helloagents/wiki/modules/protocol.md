# protocol

## Purpose

提供 **协议无关** 的命令/回复抽象与 Reply IR（供 core 命令层使用），并将 Custom Protocol v1 的 JSON/NDJSON codec 实现与模型/端口解耦，降低协议替换与 embedded 复用成本。

## Module Overview

- **Responsibility:** `protocol-model`（端口/模型 SSOT）+ `protocol-codec`（JSON + v1 codec SSOT）+ `yierdis-protocol`（兼容聚合层）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-21

## Specifications

### Requirement: 协议无关抽象（core API）
**Module:** yierdis-protocol-model

为避免 core 命令层绑定某个 wire protocol，协议层定义最小抽象：

- `Command`：argv 风格的 byte-oriented 访问（支持 `null` 参数语义）
- `ReplyWriter`：协议无关的 reply 形状写出 API（标量/聚合/null/close-after-reply/protocolError）
- `ReplySink`：用于 streaming bulk-string 值写出的窄接口（仅 bulk string 子集；不包含 array/map header 等 reply 形状）
- `Session` / `ServerSession`：连接级状态（dbIndex/auth/clientName/MULTI 事务队列等）
- `TransactionState`：MULTI/EXEC/DISCARD 的队列与上限保护

核心约束：

- core 只能依赖这些抽象；server/client 的 Netty/IO 细节不得渗透到 core
- `ReplyWriter.requestCloseAfterReply()` 只表达语义，实际关闭由 transport 层落实
- db/value 层允许依赖 `ReplySink`（用于低分配 streaming 写出），但不得依赖 `ReplyWriter`（避免协议/回复形状耦合）

### Requirement: Custom Protocol v1 reply（NDJSON）
**Module:** yierdis-protocol-codec（实现） + yierdis-protocol-model（Reply IR）

Custom Protocol v1 的 reply 采用 NDJSON（每个 reply 一行 JSON）：

- success envelope：`{"ok":true,"result":...}\\n`
- error envelope：`{"ok":false,"error":{"kind":"command|protocol|internal","message":"..."}}\\n`

值与聚合类型映射（稳定 wire 语义，禁止 best-effort 漂移）：

- null/boolean/integer/double/string → JSON 原生值
- bytes（bulk string）：若为 **严格 UTF-8** 字节序列，则输出 JSON string（可逆）；否则输出 tagged value：`{"$b64":"<base64>"}`（语义保真，避免信息丢失）
  - 实现要点：bytes value 编码由 `CustomProtocolV1NdjsonEncoder`（SSOT）以 streaming 方式完成；当输入为 `BytesSlice` 且 strict UTF-8 且无需 JSON escape 时，直接走 `BytesSlice.writeTo(BytesSink)`，以便 `NettyByteBufSink`/off-heap slice 命中 fast-path；需要 escape 或 invalid UTF-8 时，按固定 chunk（8KB）读取并编码，避免按 value 大小分配 heap `byte[]`。
- array → JSON array
- map/attribute → tagged value：`{"$map":[[k,v],...]}`（entries 结构；key/value 均为 value，可表达任意类型，避免 JSON object key 只能为 string 的限制）
- nested error（数组元素/值域内错误）→ tagged value：`{"$error":{"kind":"command|protocol|internal","message":"..."}}`

错误 message 规则（SSOT）：
- CR/LF 统一替换为空格，防止 response splitting
- message 为空/blank 时使用 kind 的默认 message
- 默认最大长度 256 字符（可配置时以代码为准）

职责边界约束：
- decoder/handler/writer 禁止各自手写 JSON error envelope；回包必须走 `CustomProtocolV1NdjsonEncoder`（SSOT）

### Requirement: 最小 JSON codec（无三方依赖）
**Module:** yierdis-protocol-codec

为了避免引入重量级依赖，同时可控安全边界：

- `JsonWriter`：单行输出、控制字符转义、避免 response splitting
- `JsonParser`：strict UTF-8 + object/array/string/number/bool/null（不支持注释/尾逗号）
- `JsonLimits`：最大深度/长度等安全上限（DoS 防护）

## Dependencies

- `yierdis-protocol-model`：依赖 `yierdis-bytes`（`BytesSource/BytesSink/BytesSlice` 通用 bytes 抽象）
- `yierdis-protocol-codec`：依赖 `yierdis-protocol-model` + `yierdis-bytes`
- `yierdis-protocol`（兼容聚合层）：聚合依赖 `yierdis-protocol-model` + `yierdis-protocol-codec`

## Change History

- 2026-02-06：引入协议无关抽象 + Custom Protocol v1（JSON codec + NDJSON reply），并移除旧协议遗留实现与测试。
- 2026-02-09：reply bytes value 编码贯通 `BytesSlice` 直写/少拷贝链路（streaming strict UTF-8 + JSON escape + `$b64` fallback），详情：`helloagents/history/2026-02/202602092316_reply_byteslice_streaming_encoder/`。
- 2026-02-21：协议层拆分为 `yierdis-protocol-model` / `yierdis-protocol-codec`，并保留 `yierdis-protocol` 作为兼容聚合层；core 依赖收敛到 model，避免编译期引入 codec。
