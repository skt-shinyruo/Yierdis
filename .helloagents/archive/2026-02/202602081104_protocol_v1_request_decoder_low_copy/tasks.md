<!-- migrated_from: history/2026-02/202602081104_protocol_v1_request_decoder_low_copy/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Custom Protocol v1 request 解码低拷贝化

Directory: `helloagents/plan/202602081104_protocol_v1_request_decoder_low_copy/`

---

## 1. yierdis-protocol（JSON 解析能力）
- [√] 1.1 为 `JsonParser` 增加 `ByteBuffer` 输入的 strict UTF-8 解析重载（不改变现有 byte[] API），修改 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/json/JsonParser.java`，verify why.md#requirement-low-copy-request-decode低拷贝请求解码-scenario-single-frame-decode
- [√] 1.2 为新增重载补齐单元测试（覆盖合法 JSON、非法 UTF-8 等），修改 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/json/JsonParserTest.java`，verify why.md#requirement-low-copy-request-decode低拷贝请求解码-scenario-single-frame-decode

## 2. yierdis-protocol-netty（decoder 低拷贝化）
- [√] 2.1 将 `CustomRequestDecoder` 的 payload 读取改为 `ByteBuf` slice + `ByteBuffer` 解析，移除整帧 heap `byte[]` payload 分配，修改 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`，verify why.md#requirement-low-copy-request-decode低拷贝请求解码-scenario-single-frame-decode
- [√] 2.2 扩展 `CustomRequestDecoderTest` 覆盖 direct buffer / CRLF payload 拒绝等路径，修改 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`，verify why.md#requirement-low-copy-request-decode低拷贝请求解码-scenario-raw-crlf-inside-payload

## 3. Security Check
- [√] 3.1 执行安全检查（输入上限、错误消息净化、discard/close 行为、ByteBuf slice 生命周期与引用计数），并记录发现与结论
  > Note: 输入上限：`maxHeaderBytes/maxPayloadBytes/maxArgs/maxDiscardBytes` 仍生效；length header 仅允许数字并做 int 溢出保护。
  > Note: 错误消息：`safeErrorMessage` 会去除 CR/LF、限制长度 256，并通过 `JsonWriter.writeString` JSON 转义后写回。
  > Note: discard/close：`DISCARD_TO_LF` 丢弃到下一次 `\\n`；若丢弃累计超 `maxDiscardBytes` 则 `ctx.close()`（DoS 护栏）。
  > Note: slice 生命周期：decoder 仅在当前 `decode()` 调用内使用 `ByteBuf` slice/`ByteBuffer` 视图，不向 `out` 泄漏/持有，避免引用计数与驻留风险。
  > Note: 退化路径：当 payload 无法暴露为单段 NIO buffer（`nioBufferCount>1`）时，会回退为受上限约束的临时拷贝解析（已在文档说明）。

## 4. Documentation Update
- [√] 4.1 更新 `helloagents/wiki/modules/protocol-netty.md`：补充 decoder 低拷贝化说明与退化路径说明
- [√] 4.2 更新 `README.md`：补充“开放网络环境建议收紧协议上限”的操作建议（如需要）
- [√] 4.3 更新 `helloagents/CHANGELOG.md`：记录 decoder 低拷贝化与相关风险说明

## 5. Testing
- [√] 5.1 运行 `mvn test`，确保全量测试通过，并记录关键结果
  > Note: `mvn test`：BUILD SUCCESS（Total time: 51.423 s；Finished at: 2026-02-09T16:34:25+08:00）
