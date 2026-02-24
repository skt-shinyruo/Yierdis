# Task List: RESP3 全覆盖（request + reply + streamed + push + attributes）

Directory: `helloagents/plan/202602031537_resp3_full_coverage/`

---

## 1. protocol（SSOT：streamed 语义解析）
- [√] 1.1 扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`：支持 `$?` streamed blob string（chunk `;len`…`;0`），verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s1_streamed-blob-string
- [√] 1.2 扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`：支持 `*?/%?/~?` streamed aggregates（直到 `.` END），verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s2_streamed-aggregatesarraymapset，depends on task 1.1
- [√] 1.3 新增单测：`yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespObjectParserStreamedTest.java`（新增）覆盖 streamed string/aggregate 与上限校验，verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s2_streamed-aggregatesarraymapset，depends on task 1.2

## 2. protocol-netty（framing：RespDecoder 支持 streamed）
- [√] 2.1 扩展 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`：支持 `$?` streamed blob string 的切帧跳过，verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s1_streamed-blob-string
- [√] 2.2 扩展 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`：支持 `*?/%?/~?` streamed aggregates 与 `.` END 的切帧跳过，verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s2_streamed-aggregatesarraymapset，depends on task 2.1
- [√] 2.3 新增单测：`yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespDecoderStreamedTest.java`（新增），覆盖半包/粘包/嵌套/上限，verify why.md#requirement-r1_resp3-streamed-strings--aggregates-全解析-s2_streamed-aggregatesarraymapset，depends on task 2.2

## 3. protocol-netty + server（request decoder：完整 RESP3 命令解码）
- [√] 3.1 设计并实现通用 request decoder（新类或重构现有）：`yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java` 支持 attributes 包裹 array + streamed blob 参数 + streamed array，严格命令约束（top-level 非 array/inline → protocol error），verify why.md#requirement-r2_request-侧完整-resp3-命令解码严格模式-s1_attributes-包裹命令--streamed-参数
- [√] 3.2 更新 server pipeline：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java` 使用新的 request decoder 行为（必要时保留兼容开关），verify why.md#requirement-r2_request-侧完整-resp3-命令解码严格模式-s1_attributes-包裹命令--streamed-参数，depends on task 3.1
  > Note: 本次无需修改 server initializer：pipeline 已使用 `RespCommandDecoder`，直接继承新行为；通过集成测试覆盖。
- [√] 3.3 新增 server 集成测试：`yierdis-server/src/test/java/yier/bubu/redis/Resp3StreamedRequestIntegrationTest.java`（新增），覆盖 streamed request 执行与 protocol error → close，verify why.md#requirement-r2_request-侧完整-resp3-命令解码严格模式-s1_attributes-包裹命令--streamed-参数，depends on task 3.2

## 4. client（push/attributes：避免 request/response 错配）
- [√] 4.1 扩展 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`：识别 push（含 attributes 包裹 push）并路由到独立 push handler/队列，保证 reply 配对不受影响，verify why.md#requirement-r3_pushattributes-不破坏-client-的-requestresponse-配对-s1_push-与-reply-乱序到达
- [√] 4.2 新增 client 测试：`yierdis-client/src/test/java/yier/bubu/redis/client/Resp3PushInterleaveTest.java`（新增），覆盖 push 与 reply 乱序输入不导致错配，verify why.md#requirement-r3_pushattributes-不破坏-client-的-requestresponse-配对-s1_push-与-reply-乱序到达，depends on task 4.1

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：streamed 累计长度/元素数/嵌套深度上限、异常路径 readerIndex 回滚、ByteBuf release 无泄漏、protocol error → close 一致性

## 6. Documentation Update（SSOT 同步）
- [√] 6.1 更新 `helloagents/wiki/overview.md`：RESP3 全覆盖与 streamed 支持声明、严格错误模型说明
- [√] 6.2 更新 `helloagents/wiki/modules/protocol.md` / `helloagents/wiki/modules/protocol-netty.md`：补齐 streamed 与 request decoder 策略说明
- [√] 6.3 更新 `README.md`：明确 RESP3 支持范围（含 streamed/push/attributes）、与 Redis 行为差异点、以及 client push 处理方式

## 7. Testing
- [√] 7.1 执行 `mvn test` 并确认通过（全量回归，含新增 streamed/push 用例）
