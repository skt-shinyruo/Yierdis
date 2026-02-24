<!-- migrated_from: history/2026-02/202602061102_resp_parser_ssot_alignment/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: RESP 解析 SSOT 收敛与质量兜底

Directory: `helloagents/plan/202602061102_resp_parser_ssot_alignment/`

---

## 1. protocol（SSOT：wire scan/skip 语义）
- [√] 1.1 新增 SSOT wire support（CRLF 扫描、ASCII 数字解析、limits 统一）于 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWireSupport.java`，verify why.md#requirement-ssot-wire-skipper协议-scanskip-语义收敛
- [√] 1.2 新增 SSOT wire skipper（`trySkipOne`：RESP2/RESP3 + streamed + attributes）于 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWireSkipper.java`，verify why.md#requirement-ssot-wire-skipper协议-scanskip-语义收敛，depends on task 1.1
- [√] 1.3 让 `RespObjectParser` 复用 SSOT support（减少重复 `indexOfCrlf/parseInt` 语义实现）于 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`，verify why.md#requirement-ssot-wire-skipper协议-scanskip-语义收敛，depends on task 1.1
- [√] 1.4 新增/扩展协议层测试矩阵（streamed/attributes/limits/truncated）于 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/RespWireSkipperTest.java`（新增）与既有 parser tests，verify why.md#requirement-test-matrix-lock-ingoldenround-tripfuzz，depends on task 1.2

## 2. protocol-netty（adapter：decoder 复用 SSOT）
- [√] 2.1 引入 ByteBuf→BytesSource 的轻量适配（或等价封装）于 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/NettyBytesSource.java`（新增），verify why.md#requirement-netty-decoder-delegationadapter-复用-ssot
- [√] 2.2 `RespDecoder` 改为委托 SSOT skipper 计算 frame endIdx（保持 wire-preserving + 半包恢复）于 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`，verify why.md#scenario-reply-切帧respdecoderwire-preserving--半包恢复，depends on task 1.2, task 2.1
- [√] 2.3 `RespCommandDecoder` 将 attributes/streamed/skip-scan 分支委托 SSOT（保留 bulk-array fast-path 与 materialize 回退）于 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`，verify why.md#scenario-request-解码respcommanddecoder严格性--回退路径一致，depends on task 1.2, task 2.1

## 3. 测试矩阵（golden/round-trip/fuzz）
- [√] 3.1 扩展 golden cases：attributes 链式 + 内部复杂嵌套/streamed、截断/半包、skip/scan 回退边界，于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderResp3RequestTest.java` 与 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespDecoderStreamedTest.java`，verify why.md#requirement-test-matrix-lock-ingoldenround-tripfuzz
- [√] 3.2 扩展 round-trip：wire 字节级一致 + 语义可解析，于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespRoundTripTest.java`（扩展 RESP3 类型集合），verify why.md#requirement-test-matrix-lock-ingoldenround-tripfuzz
- [√] 3.3 新增短 fuzz（默认 `mvn test` ≤3s、固定 seed、失败打印 seed），于 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCodecFuzzTest.java`（新增），verify why.md#scenario-golden-cases--round-trip--短-fuzz默认-mvn-test
- [-] 3.4 回归 protocol error → close 行为（复用现有 server 测试，必要时补 case）于 `yierdis-server/src/test/java/yier/bubu/redis/FastPipelineTest.java` 或新增用例，verify why.md#scenario-request-解码respcommanddecoder严格性--回退路径一致，depends on task 2.3
  > Note: 已覆盖（server 集成测试已锁定 protocol error close 与非致命错误 keep-open 行为边界）。

## 4. Security Check
- [√] 4.1 执行协议安全检查（limits、生存期/释放、错误消息、拒绝服务风险）并补齐测试断言（如需要），按 G9 记录关键结论
  > Note: 本次变更不涉及外部服务/密钥/PII；DoS 防护依赖 `RespLimits` + skipper/parser 边界校验；资源释放由零拷贝 frame 约束与回归测试兜底。

## 5. Documentation Update（知识库同步）
- [√] 5.1 更新模块文档与 ADR 索引：`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/modules/protocol-netty.md`、`helloagents/wiki/arch.md`（新增 ADR-20260206-01 条目），并在 `helloagents/CHANGELOG.md` 记录变更

## 6. Testing
- [√] 6.1 运行 `mvn test`（至少覆盖 `yierdis-protocol`、`yierdis-protocol-netty`、`yierdis-server`），确保 fuzz 测试稳定且 ≤3s
