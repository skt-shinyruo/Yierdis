<!-- migrated_from: history/2026-02/202602081104_protocol_v1_request_decoder_low_copy/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: Custom Protocol v1 request 解码低拷贝化

## Technical Solution

### Core Technologies

- Java（UTF-8 严格解码 `CharsetDecoder`）
- Netty (`ByteToMessageDecoder`, `ByteBuf` slice/nio buffer)
- 项目内置 JSON codec：`JsonParser` / `JsonLimits`（无三方依赖）

### Implementation Key Points

1. **Decoder 低拷贝读取 payload：**
   - 将 `CustomRequestDecoder` 的 `READ_PAYLOAD` 从 `new byte[len] + readBytes(byte[])` 改为 `in.readSlice(len)`（或等价 slice 方式）。
   - 保持 terminator（`\n`）校验与 “payload 内禁止原始 CR/LF” 的单行约束逻辑不变，但扫描逻辑改为基于 `ByteBuf` 读取（避免先拷贝到 byte[]）。

2. **协议层 JSON 解析支持 ByteBuffer 输入：**
   - 在 `JsonParser` 中新增 `parseStrictUtf8(ByteBuffer, JsonLimits)`（或等价重载），以便 decoder 使用 `payload.nioBuffer()` 直接解析 UTF-8 JSON，而不需要先拷贝到 heap `byte[]`。
   - 实现时需对 `ByteBuffer` 做 `duplicate()/slice()`，避免污染 caller 的 position/limit。

3. **兼容性与 fallback：**
   - 绝大多数 Netty inbound/cumulation 场景会提供单段 `ByteBuffer`；仍需考虑 composite buffer 形态。
   - 若 payload 区间无法提供单段 `ByteBuffer`（例如 `nioBufferCount > 1`），提供保守 fallback：
     - 方案 A：按 payload 长度分配临时 `byte[]` 并复制（行为与现状一致，但应仅在少数退化路径触发，且受 `maxPayloadBytes` 硬上限约束）。
     - 方案 B：将多段 `ByteBuffer[]` 聚合解码为 String（实现更复杂，收益不大）。
   - 默认优先 A（可预测、改动小），同时通过测试覆盖主要路径确保低拷贝收益。

4. **行为保持：**
   - framing：仍为 `<len>:<json>\n`
   - 错误模型：仍为 best-effort recoverable（写回 protocol error + resync）
   - DoS 护栏：仍由 `maxPayloadBytes/maxHeaderBytes/maxArgs/maxDiscardBytes` 约束

## Architecture Decision ADR

### ADR-20260208-01: request payload 从 heap-copy 改为 ByteBuf slice + ByteBuffer 解析

**Context:** decoder 以 `new byte[len]` 读取整帧 payload 会造成瞬时大分配与复制放大，在开放网络环境下会扩大 DoS 面。

**Decision:** decoder 读取 payload 改用 `ByteBuf` slice，并通过 `JsonParser` 的 `ByteBuffer` 重载直接解析 UTF-8 JSON；仅在极端 ByteBuf 形态下使用受上限约束的保守 fallback。

**Rationale:** 在不改协议帧格式与错误模型的前提下，以最小改动降低大包请求对 heap/GC 的冲击，并保持 `yierdis-protocol` 作为 Netty-free SSOT。

**Alternatives:**
- 继续使用 heap `byte[]` payload：实现简单，但 DoS 风险高（拒绝）
- 引入流式 JSON parser：收益更大，但实现复杂度显著提升且超出教学项目当前演进节奏（暂缓）

**Impact:** decoder 峰值 heap 分配降低（至少消除 “payload 全量拷贝” 这一次大分配）；需要通过测试覆盖与 fallback 处理来降低兼容性风险。

## Security and Performance

- **Security:**
  - 输入上限继续由 `ProtocolLimits` SSOT + server args 显式可配
  - 错误消息净化、discard/close 逻辑保持不变
  - 不在 `out` 中泄漏/持有 `ByteBuf` slice 引用（避免引用计数与驻留风险）
- **Performance:**
  - 消除 decoder 的整帧 heap payload 拷贝，减少一次大块分配与复制
  - 退化路径仅在少数 composite 场景触发，且仍受 `maxPayloadBytes` 约束

## Testing and Deployment

- **Testing:**
  - 为 `JsonParser` 的 ByteBuffer 重载增加单元测试
  - 为 `CustomRequestDecoder` 增加 direct buffer 场景测试（确保无数组依赖）
  - 回归覆盖：invalid JSON / invalid header / pipelined frames / CRLF payload
- **Deployment:**
  - 即便 decoder 低拷贝化，仍建议在开放网络环境下显式收紧 `--protocolMaxBulkBytes`，并结合 bytes-based 预算与背压参数做容量护栏

