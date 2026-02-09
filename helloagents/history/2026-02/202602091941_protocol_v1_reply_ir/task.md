# Task List: Custom Protocol v1 Reply IR（协议语义中间层）与错误模型收敛

Directory: `helloagents/plan/202602091941_protocol_v1_reply_ir/`

---

## 1. yierdis-protocol（Reply IR + encoder SSOT）
- [√] 1.1 定义 Reply IR 数据模型（`ReplyValue/ReplyEnvelope/ReplyError` 等）与统一 sanitize helper，新增/修改 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/reply/*`，verify why.md#r1-s1
- [√] 1.2 实现 Custom Protocol v1 NDJSON encoder（bytes→`$b64`、map→`$map`、nested error→`$error`），新增 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/*Encoder*.java`，verify why.md#r2-s1, depends on task 1.1
- [√] 1.3 改造/替换 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`：移除 object-key fallback 与 UTF-8 best-effort；统一走 encoder SSOT，verify why.md#r1-s1, depends on task 1.2
- [√] 1.4 更新并扩展 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterTest.java` 为 wire-level golden tests（覆盖 bytes/map/nested error/sanitize），verify why.md#r4-s1, depends on task 1.3

## 2. yierdis-protocol-netty（decoder 职责收敛 + conformance tests）
- [√] 2.1 改造 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`：移除手写 JSON error reply，改为输出 `ProtocolError` 事件/消息并继续 resync，verify why.md#r3-s1
- [√] 2.2 更新 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`：断言 decoder 产出 protocol error 事件且后续帧可解码（不依赖 outbound JSON 字符串），verify why.md#r3-s1, depends on task 2.1

## 3. yierdis-server（pipeline 收敛 + 端到端黄金测试）
- [√] 3.1 在 `yierdis-server` Netty pipeline 中新增/调整 handler：捕获 decoder 的 protocol error 事件并调用统一 encoder 写回 NDJSON，修改 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java` 或新增专用 handler，verify why.md#r2-s1, depends on task 2.1
- [√] 3.2 统一 internal/protocol 错误的回包路径：handler 不再自拼/自净化 message（回包交给 encoder SSOT），修改 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`，verify why.md#r2-s2, depends on task 1.2
- [√] 3.3 更新并扩展 `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java` 为端到端 wire-level golden（invalid frame → protocol error；next frame → PONG），verify why.md#r4-s2, depends on task 3.1

## 4. yierdis-client / yierdis-bench（解析与展示对齐新 wire）
- [√] 4.1 更新 `yierdis-client`：解析 `$b64/$map/$error` tagged value，并提供可读输出（UTF-8 展示可 best-effort，但语义以 bytes 为准），修改 `yierdis-client/src/main/java/yier/bubu/redis/client/*`，verify why.md#r4-s1
- [√] 4.2 更新 `yierdis-bench`：错误统计/结果解析对齐新协议表示（尤其是 error 与 map/bytes），修改 `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`，verify why.md#r4-s2, depends on task 4.1
- [√] 4.3 更新 client 测试中依赖 map/object 的断言（如 `MEMORY STATS`），修改 `yierdis-client/src/test/java/yier/bubu/redis/client/*`，verify why.md#r1-s1, depends on task 4.1

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：输入校验、CRLF 注入防护、message 截断一致性、DoS 上限（payload/header/discard/line）验证

## 6. Documentation Update
- [√] 6.1 更新知识库协议规范：`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/api.md`、`helloagents/wiki/modules/protocol-netty.md`，记录新的 `$b64/$map/$error` wire 表示与 decoder 职责边界，verify why.md#r2
- [√] 6.2 更新 `helloagents/wiki/arch.md` ADR 索引（添加 ADR-20260209-01），并在 `helloagents/CHANGELOG.md` 记录 breaking change

## 7. Testing
- [√] 7.1 运行 `mvn test` 并修复本次改动引入的失败（仅限本次范围）；补充必要的黄金测试覆盖点
