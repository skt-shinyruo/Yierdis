# Technical Design: reply BytesSlice streaming encoder（Custom Protocol v1 NDJSON）

## Technical Solution

### Core Technologies
- Java 17
- `yierdis-bytes`: `BytesSource/BytesSlice/BytesSink`（Netty-free SSOT）
- `yierdis-bytes-netty`: `NettyByteBufSink`（`DirectBytesSink` adapter）
- `yierdis-protocol`: `CustomProtocolV1NdjsonEncoder`（reply encoder SSOT）+ `JsonWriter`（JSON escape SSOT）

### Implementation Key Points

- **统一入口：**在 `CustomProtocolV1NdjsonEncoder` 中实现 bytes value 的 streaming 编码，并让：
  - `writeBytesValue(BytesSink, byte[], off, len)` 走统一实现（消除 `strictUtf8ToStringOrNull(...)` + `String` 构造）
  - 新增 `writeBytesValue(BytesSink, BytesSlice)` / `writeBytesValue(BytesSink, BytesSource, off, len)` 入口供 reply writer 使用
- **输出策略（稳定语义，避免漂移）：**
  1. `null` → 输出 `null`
  2. `len==0` → 输出 `""`
  3. strict UTF-8 且无需 escape → 输出 `"` + 原始 bytes + `"`
     - 对 `BytesSlice`：使用 `slice.writeTo(out)` 以命中 off-heap/Netty fast-path
  4. strict UTF-8 但需要 escape → streaming 解码 code point，并按 `JsonWriter.writeString` 同等规则输出 escape（不构造整段 `String`）
  5. invalid UTF-8 → 输出 `{"$b64":"..."}`
     - base64 编码采用 streaming 输出（避免 `copyRange(...)` 与 `String` 分配）
- **缓冲与内存策略：**
  - 采用 `ThreadLocal` scratch buffer（例如 8KB）用于：
    - `BytesSlice/BytesSource` 的分段读取（`getBytes(...)`）
    - base64 streaming 输入缓冲（按块 feed encoder）
  - 禁止按 value 长度 `new byte[len]`，也避免 `new String(...)`/`CharsetDecoder.decode(...).toString()` 的整段分配

## Architecture Decision ADR

### ADR-20260209-02: bytes value 编码由“整段 decode + JsonWriter”改为“streaming 校验/escape + fast-path writeTo”
**Context:**
- 现状 `JsonLineReplyWriter.bulkString(BytesSlice)` 会全量拷贝到 heap `byte[]`，绕开 `BytesSlice.writeTo` fast-path
- encoder 的 bytes value 编码依赖整段 `String` 构造与 `copyRange(...)`，在大 value 下存在明显 heap 分配与拷贝

**Decision:**
- 在 `CustomProtocolV1NdjsonEncoder` 内提供统一 streaming bytes value 编码能力，同时支持 `byte[]` 与 `BytesSlice/BytesSource`
- `JsonLineReplyWriter.bulkString(BytesSlice)` 改为直接调用 encoder 的 slice/source 入口，消除按值大小 heap 分配

**Rationale:**
- 让 reply 写出阶段能实际利用 `BytesSlice.writeTo + DirectBytesSink/NettyByteBufSink`，使 off-heap slice 的收益在网络写出阶段体现
- 将 wire 语义（strict UTF-8 / b64 tagged）继续收敛在 encoder SSOT，避免多处实现导致漂移

**Alternatives:**
- 方案 1：仅覆盖 “valid UTF-8 & no-escape” 的 fast-path（更小改动，但对 escape/invalid 仍需按值拷贝/分配，收益不完整）
- 方案 3：仅对 `DirectBytesSink` 进行 writerIndex 回滚式单 pass（实现复杂、对非 Direct sink 退化明显）

**Impact:**
- 行为面更广（byte[] 与 slice 都改用 streaming），需要补齐更全面的回归测试
- 双遍扫描可能增加 CPU，但可通过 no-escape fast-path 与低分配实现抵消整体成本

## Security and Performance
- **Security:** 输出必须保持单行 JSON、无未转义控制字符；invalid UTF-8 仍采用 tagged base64（语义保真，避免信息丢失）
- **Performance:** 避免按值大小 heap `byte[]`/`String` 分配；在 Netty 输出中可触发 off-heap slice 的 direct-memory copy fast-path

## Testing and Deployment
- **Testing:** 扩展 `JsonLineReplyWriterTest` 覆盖 `BytesSlice` 的 UTF-8/no-escape、escape、invalid 三类场景；增加 encoder 级一致性对比测试（byte[] 与 slice 输出一致）
- **Deployment:** 无对外协议变更；仅性能与实现收敛，随常规发布流程上线

