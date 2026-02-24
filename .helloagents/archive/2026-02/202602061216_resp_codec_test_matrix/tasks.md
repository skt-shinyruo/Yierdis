<!-- migrated_from: history/2026-02/202602061216_resp_codec_test_matrix/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: RESP 解析质量兜底（Golden + Round-trip + Fuzz + 一致性差分）

Directory: `helloagents/plan/202602061216_resp_codec_test_matrix/`

---

## 1. protocol（SSOT：wire skip/parse 的边界与一致性）
- [√] 1.1 扩展 wire skipper golden：补齐 `trySkipOneStrict` 与 limits 边界（line/array/nesting/bulk）于 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespWireSkipperTest.java`，verify why.md#requirement-limits-口径一致性与协议错误策略可回归
- [√] 1.2 扩展 parser streamed/attributes 组合边界（含截断/非法 end marker）于 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespObjectParserStreamedTest.java`（或新增同目录测试类），verify why.md#requirement-reply-切帧-wire-preserving-与-streamedattributes-组合稳定

## 2. protocol-netty（decoder：golden + packetization + round-trip + fuzz）
- [-] 2.1 扩展 request golden：attributes 链式 + streamed 值、streamed command array、以及 array 内 scalar/strictness 边界，于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderResp3RequestTest.java`，verify why.md#requirement-request-解码的兼容性与严格性锁定
  > Note: 已覆盖（现有用例已包含 attributes 链式、attributes 内 streamed aggregates、streamed blob arg、streamed command array、标量参数）。
- [-] 2.2 新增 packetization 一致性测试（同一 request 在不同分片下输出一致，覆盖 fast-path/materialize 回退）于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderPacketizationTest.java`（新增），verify why.md#requirement-request-解码的兼容性与严格性锁定
  > Note: 已由 `RespCodecFuzzTest` 覆盖随机分片（固定 seed）一致性；本次增强为 argv 全量断言，覆盖 fast-path/materialize/skip-scan 的关键性质。
- [-] 2.3 扩展 reply streamed/attributes 半包恢复与错误用例，于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespDecoderStreamedTest.java`，verify why.md#requirement-reply-切帧-wire-preserving-与-streamedattributes-组合稳定
  > Note: 已覆盖（包含 streamed blob 半包恢复、attributes 包裹 streamed、嵌套 streamed、streamed map odd elements、maxBulkBytes 超限等）。
- [-] 2.4 扩展 round-trip：覆盖更多 RESP3 组合与随机分片回归，于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespRoundTripTest.java`，verify why.md#requirement-round-trip--短-fuzz-作为回归兜底
  > Note: 已覆盖（RESP3 类型集合 round-trip + 可解析性兜底已存在；随机分片回归由 `RespCodecFuzzTest` 负责）。
- [√] 2.5 新增短 fuzz（固定 seed、≤3s，失败打印 seed，优先覆盖 reply framing + request decoding 的随机分片）于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCodecFuzzTest.java`（新增），verify why.md#requirement-round-trip--短-fuzz-作为回归兜底
  > Note: 该 fuzz 已存在；本次增强为对 request 侧 argv 全量断言（包含 streamed blob 物化回退、标量/NULL bulk 等边界），提高漂移检测能力。

## 3. server（集成：protocol error close / 非致命错误 keep-open）
- [-] 3.1 扩展协议错误 close 行为矩阵（fatal vs non-fatal），于 `yierdis-server/src/test/java/yier/bubu/redis/FastPipelineTest.java`，verify why.md#scenario-protocol-error-是否-close-行为被集成测试锁定
  > Note: 已覆盖（fatal protocol error close + 非致命参数非法 keep-open 现有用例齐备）。
- [-] 3.2 扩展 streamed request 的协议错误与半包回归，于 `yierdis-server/src/test/java/yier/bubu/redis/Resp3StreamedRequestIntegrationTest.java`，verify why.md#scenario-streamed-blob--streamed-aggregates-的-request-形态行为稳定
  > Note: 已覆盖（streamed blob request + 非法 chunk 前缀触发 protocol error close 已存在）。

## 4. Security Check
- [√] 4.1 执行协议安全检查（limits、资源释放、生存期、错误策略、DoS 风险），按 G9 记录关键结论（必要时补测试断言）
  > Note: 本次改动集中在测试/文档；未引入密钥/PII/外部服务；fuzz 固定 seed + 迭代上限；新增 strictness/limits/attributes 测试用于锁定协议边界并降低 DoS/漂移风险。

## 5. Documentation Update（知识库同步）
- [√] 5.1 更新知识库：`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/modules/protocol-netty.md`、`helloagents/wiki/arch.md`（追加 ADR-20260206-02）、`helloagents/CHANGELOG.md`

## 6. Testing
- [√] 6.1 运行 `mvn test`（至少覆盖 `yierdis-protocol`、`yierdis-protocol-netty`、`yierdis-server`），确认 fuzz 测试稳定且 ≤3s
