# protocol

## Purpose

提供 **协议无关** 的命令/回复抽象（供 core 命令层使用），以及 Custom Protocol v1 所需的最小 JSON codec 与 reply writer 实现。

## Module Overview

- **Responsibility:** `Command/ReplyWriter/ReplySink/Session` 抽象 + Custom Protocol v1 JSON codec
- **Status:** ✅Stable
- **Last Updated:** 2026-02-08

## Specifications

### Requirement: 协议无关抽象（core API）
**Module:** protocol

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
**Module:** protocol

Custom Protocol v1 的 reply 采用 NDJSON（每个 reply 一行 JSON）：

- success envelope：`{"ok":true,"result":...}\\n`
- error envelope：`{"ok":false,"error":{"kind":"command|protocol|internal","message":"..."}}\\n`

聚合类型映射：

- array → JSON array
- map/set/push → JSON object / JSON array（保持最小集合语义；以 `ReplyWriter` 的调用形状为准）

### Requirement: 最小 JSON codec（无三方依赖）
**Module:** protocol

为了避免引入重量级依赖，同时可控安全边界：

- `JsonWriter`：单行输出、控制字符转义、避免 response splitting
- `JsonParser`：strict UTF-8 + object/array/string/number/bool/null（不支持注释/尾逗号）
- `JsonLimits`：最大深度/长度等安全上限（DoS 防护）

## Dependencies

- `yierdis-bytes`（`BytesSource/BytesSink/BytesSlice` 通用 bytes 抽象）

## Change History

- 2026-02-06：引入协议无关抽象 + Custom Protocol v1（JSON codec + NDJSON reply），并移除旧协议遗留实现与测试。
