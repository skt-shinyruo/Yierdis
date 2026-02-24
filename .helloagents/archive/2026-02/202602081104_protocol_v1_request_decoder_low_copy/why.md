<!-- migrated_from: history/2026-02/202602081104_protocol_v1_request_decoder_low_copy/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Custom Protocol v1 request 解码低拷贝化

## Requirement Background

当前 Custom Protocol v1 的 request framing 为 `<len>:<json-payload>\n`。默认允许的 payload 上限为 64MiB（`ProtocolLimits.DEFAULT_MAX_REQUEST_PAYLOAD_BYTES`），且 Netty decoder 在读取 payload 时会按 `len` 直接分配一个新的 heap `byte[]` 并将整帧拷贝进去，再进行 JSON 解析。

该实现路径在“误部署到更开放网络环境 / 被大包请求冲击”时存在明显的 DoS 放大面：

1. **瞬时大分配**：`new byte[len]` 在峰值场景会造成短时间大量 heap 分配与 GC 抖动；
2. **复制放大**：payload 在 ByteBuf 与 heap buffer 间的复制放大了驻留与带宽成本；
3. **教学项目误用风险**：默认值偏松时，部署者可能在未理解风险的情况下直接暴露到公网或更弱隔离环境。

## Change Content

1. 将 `CustomRequestDecoder` 的 payload 读取从 “heap `byte[]` 全量拷贝” 改为 “基于 `ByteBuf` slice + `ByteBuffer` 解析”，避免在 decoder 中创建与 payload 等大的 heap buffer。
2. 为协议层 JSON 解析提供 `ByteBuffer` 输入重载（Netty-free），降低 transport 层与协议层之间的无谓拷贝。
3. 增强回归测试与文档说明，确保错误模型与 resync 语义保持一致，并明确 **输入上限仍需结合部署环境配置**（例如通过 `--protocolMaxBulkBytes` 收紧）。

## Impact Scope

- **Modules:** `yierdis-protocol-netty`, `yierdis-protocol`, `yierdis-server`（pipeline 装配复用 decoder 行为）
- **Files:**
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/json/JsonParser.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`
  - `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/json/JsonParserTest.java`
- **APIs:** 新增 JSON 解析的 `ByteBuffer` 重载方法（向后兼容）
- **Data:** 无

## Core Scenarios

### Requirement: Low-copy Request Decode（低拷贝请求解码）
**Module:** protocol-netty / protocol

在不改变现有 framing 与错误模型的前提下，避免 decoder 在读取 payload 阶段创建整帧 heap buffer，以降低“大包请求”对 heap/GC 的冲击。

#### Scenario: Single frame decode
条件：输入为单个合法帧 `<len>:<json>\n`，`cmd/args` schema 合法。
- 期望：正确解码为 `CustomCommand`
- 期望：decoder 不再按 `len` 分配整帧 heap `byte[]` 作为 payload 容器

#### Scenario: Pipelined frames
条件：同一个 inbound buffer 中包含多帧连续数据。
- 期望：逐帧解码并输出多条 `Command`

#### Scenario: Invalid JSON resync
条件：出现一帧 JSON 语法错误，后续紧跟合法帧。
- 期望：写回 `ok=false` 的 protocol error
- 期望：丢弃到下一个 `\n` 后继续解码后续帧

#### Scenario: Payload too large
条件：`len > maxPayloadBytes`。
- 期望：写回 protocol error 并进入丢弃/resync 流程（保持既有行为）

#### Scenario: Raw CR/LF inside payload
条件：payload 内包含原始 `\r` 或 `\n` 字节（非转义序列）。
- 期望：拒绝该帧并进入丢弃/resync（保持既有行为）

## Risk Assessment

- **Risk:** `ByteBuf.nioBuffer()` 在某些 composite/cumulation 场景可能退化（需要处理 “无法获得单段 ByteBuffer” 的情况），或引入边界条件 bug。
- **Mitigation:** 保持 framing/错误模型/丢弃策略不变；为 heap/direct buffer 输入添加回归测试；必要时为极端 ByteBuf 形态提供保守 fallback（受 `maxPayloadBytes` 约束）。

