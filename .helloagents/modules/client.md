<!-- migrated_from: wiki/modules/client.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# client

## Purpose

提供内置的极简 client/CLI，便于本地调试与脚本化测试（基于 Custom Protocol v1）。

## Module Overview

- **Responsibility:** 连接管理、命令输入、reply line 解码（NDJSON）、输出显示（单行 JSON；可选 hex）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-21

## Specifications

### Requirement: Custom Protocol v1 request/reply
**Module:** client

- request framing：`<len>:<json>\\n`（`len` 为 JSON payload 的 UTF-8 字节长度）
- payload schema：JSON object，至少包含 `cmd`（string）；可选 `args`（array，元素仅允许 `string|null`）
- reply framing：NDJSON（每个 reply 一行 JSON，以 `\\n` 结尾）
  - success：`{"ok":true,"result":...}`
  - error：`{"ok":false,"error":{"kind":"command|protocol|internal","message":"..."}}`

### Requirement: 超时后连接不可复用（避免响应错配）
**Module:** client

自定义协议同样遵循严格 FIFO 的 request/response 配对。若单次执行等待响应超时，连接会进入“响应可能延迟到达”的未知状态。
因此 client 在超时后会关闭连接并标记不可复用，避免后续请求响应错配。

### Requirement: response queue 边界化（资源上限 + 异常唤醒）
**Module:** client

client 采用“单请求-单响应”模型（不支持 pipelining），但仍需防御异常路径导致的回复堆积：

- response queue 采用 **有界队列**（防止对端 flood 或协议错配导致 OOM）
- 当队列溢出：立即关闭连接，并向等待线程投递 terminal 信号（避免一直阻塞到超时）
- 当连接断开/异常：同样投递 terminal 信号，确保 `execute()` 能尽快返回错误

### Requirement: 输出稳定（CLI）
**Module:** client

- CLI 默认打印服务端返回的 JSON 单行文本（便于脚本与日志收集）
- `--hex`：打印 raw JSON reply line 的十六进制表示（用于抓包/对比，不改变协议）

## Dependencies

- `yierdis-protocol-netty`（`JsonLineDecoder`）
- `yierdis-protocol-codec`（`JsonParser/JsonValue`）
- Netty（连接管理与 IO）
- picocli（CLI 参数解析）

## Change History

- 2026-02-06：切换到 Custom Protocol v1（移除旧协议兼容与旧展示逻辑）。
