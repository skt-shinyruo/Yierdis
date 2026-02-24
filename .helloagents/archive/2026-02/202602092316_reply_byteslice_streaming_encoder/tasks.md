<!-- migrated_from: history/2026-02/202602092316_reply_byteslice_streaming_encoder/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: reply BytesSlice streaming encoder（Custom Protocol v1 NDJSON）

Directory: `helloagents/plan/202602092316_reply_byteslice_streaming_encoder/`

---

## 1. protocol（encoder SSOT）
- [√] 1.1 在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java` 引入 streaming bytes value 统一实现（byte[] 与 BytesSlice/BytesSource 共用），验证 why.md#requirement-bulk-string-bytes-value-streaming
- [-] 1.2 如需 JSON string 的 bytes streaming escape：在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/json/JsonWriter.java` 增加面向 UTF-8 bytes 的写出能力（保持与 `writeString(String)` 语义一致），验证 why.md#scenario-utf-8-bytes-需要-json-escape含引号反斜杠控制字符
> Note: bytes 的 strict UTF-8 校验与 JSON string escape 已在 encoder SSOT 内以 streaming 方式实现，避免引入 JsonWriter 的额外 API 分支。

## 2. protocol（reply writer）
- [√] 2.1 修改 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`：`bulkString(BytesSlice)` 不再 `new byte[len]` 全拷贝，改为直接委托 encoder 的 slice/source 入口，验证 why.md#scenario-bytesliceoff-heap写出到-netty-sink

## 3. Testing
- [√] 3.1 扩展 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterTest.java`：新增 `BytesSlice` 场景用例（UTF-8 no-escape / escape / invalid UTF-8 tagged b64），并引入“禁止全量拷贝”的测试 double（限制 `getBytes` 最大 chunk），验证 why.md 各场景
- [√] 3.2 增加 encoder 一致性测试：同一份 bytes 以 `byte[]` 与 `BytesSlice` 输入时输出一致（包含控制字符、补充平面字符、invalid UTF-8），验证 why.md#requirement-bulk-string-bytes-value-streaming

## 4. Security Check
- [√] 4.1 安全复核（G9）：确认输出无未转义控制字符、无 CR/LF 注入与 response splitting 风险；invalid UTF-8 不做 best-effort 变换，验证 why.md 风险缓解项

## 5. Documentation Update
- [√] 5.1 更新知识库：`helloagents/wiki/modules/protocol.md` 补充 bytes value streaming 编码与 `BytesSlice` fast-path 的实现说明，并链接到本变更 history 记录

## 6. Verification
- [√] 6.1 运行 `mvn test`（至少覆盖 `yierdis-protocol` 模块测试），记录结果并在实现阶段同步到 CHANGELOG/历史归档
