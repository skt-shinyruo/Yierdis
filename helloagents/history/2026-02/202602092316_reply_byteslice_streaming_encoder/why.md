# Change Proposal: reply BytesSlice streaming encoder（贯通 off-heap fast-path）

## Requirement Background

当前 reply 写出链路中，`yierdis-protocol` 的 `JsonLineReplyWriter.bulkString(BytesSlice)` 会走：

- `new byte[len]`（按值大小分配 heap 数组）
- `slice.getBytes(0, data, 0, len)`（全量拷贝到 heap）
- `CustomProtocolV1NdjsonEncoder.writeBytesValue(out, data, 0, len)`（strict UTF-8 decode → JSON string escape / b64 tagged fallback）

这会绕开 `BytesSlice.writeTo(BytesSink)` + `DirectBytesSink/NettyByteBufSink` 的 fast-path，导致 off-heap slice 在 reply 写出阶段的意义被削弱，且在大 value 场景引入明显的 heap 分配与拷贝成本。

## Change Content

1. 将 bytes value 的 NDJSON 编码在 `CustomProtocolV1NdjsonEncoder` 收敛为 **streaming 统一实现**，同时支持 `byte[]` 与 `BytesSlice/BytesSource` 输入。
2. `JsonLineReplyWriter.bulkString(BytesSlice)` 改为直接委托 encoder 的 `BytesSlice/BytesSource` 入口，移除按值长度分配的 heap `byte[]`。
3. 对 **strict UTF-8 且无需 JSON escape** 的路径：输出 JSON 引号并直接写入原始 bytes（`byte[]` 直接写；`BytesSlice` 走 `writeTo(out)`，让 Netty sink/off-heap slice 能命中 fast-path）。
4. 对 **strict UTF-8 但需要 JSON escape** 的路径：按字节流进行 strict UTF-8 解码并 streaming 输出 escape（不构造整段 `String`，不按值大小分配 heap `byte[]`）。
5. 对 **invalid UTF-8 bytes**：保持 `{"$b64":"..."}` tagged value 语义不变，但 base64 编码改为 streaming，避免 `copyRange(...)` 产生的按值大小 heap 分配。

## Impact Scope

- **Modules:** `yierdis-protocol`（encoder/writer/json codec SSOT）
- **Files:**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/json/JsonWriter.java`（如需要新增 bytes streaming JSON string 写出能力）
  - `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterTest.java`
- **APIs:** 仅新增 encoder 内部/模块级的 overload（wire 语义不变）
- **Data:** None

## Core Scenarios

### Requirement: Bulk string bytes value streaming
**Module:** protocol
reply 的 bulk-string（bytes）值写出需满足以下稳定语义与性能目标。

#### Scenario: BytesSlice（off-heap）写出到 Netty sink
前置条件：
- value 来自 off-heap slice（实现 `BytesSlice`）
- transport 侧输出为 `NettyByteBufSink`（`DirectBytesSink`）

期望结果：
- 不按值大小分配 heap `byte[]`
- strict UTF-8 / b64 tagged 的 wire 语义与现有保持一致
- 在 “UTF-8 且无需 escape” 的常见路径中，可触发 `slice.writeTo(out)` fast-path

#### Scenario: UTF-8 bytes 需要 JSON escape（含引号/反斜杠/控制字符）
期望结果：
- escape 行为与 `JsonWriter.writeString(String)` 保持一致（无未转义控制字符）
- 不构造整段 `String`

#### Scenario: invalid UTF-8 bytes
期望结果：
- 输出 `{"$b64":"<base64>"}` tagged value（与现有一致）
- 不通过 `copyRange(...)` 创建按值大小的新 heap `byte[]`

## Risk Assessment

- **Risk:** streaming UTF-8 校验/解码与 escape 输出存在语义偏差（导致 JSON 不合法或内容漂移）
  - **Mitigation:** 以现有测试为基线新增 `BytesSlice` 覆盖；同时对 `byte[]` 与 `BytesSlice` 的输出做一致性回归，覆盖控制字符、引号/反斜杠、补充平面字符、invalid UTF-8 等边界用例
- **Risk:** 双遍扫描（先判定 valid/escape，再输出）带来 CPU 成本
  - **Mitigation:** 对 ASCII/无 escape 的快路径做一次扫描即可进入 `writeTo(out)`；实现上避免多余的对象分配与拷贝
- **Risk:** base64 streaming 输出与旧实现不一致
  - **Mitigation:** 引入对比测试（streaming 输出与 `Base64.getEncoder().encodeToString(...)` 结果一致）

