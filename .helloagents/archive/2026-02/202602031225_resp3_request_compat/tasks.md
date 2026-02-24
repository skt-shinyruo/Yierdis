<!-- migrated_from: history/2026-02/202602031225_resp3_request_compat/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: RESP3 Request 兼容性对齐（decoder 支持 attributes + scalar）

Directory: `helloagents/plan/202602031225_resp3_request_compat/`

---

## 1. protocol-netty（RESP3 request 解码）
- [√] 1.1 扩展 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`：支持跳过 `| attribute` 前缀并继续解析命令，verify why.md#requirement-r1_resp3-request-解码兼容-s1_attributes-前缀不再破坏连接
- [√] 1.2 扩展 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`：在 `* array` 内支持 RESP3 标量类型（`_ / + / : / # / , / ( / =`），verify why.md#requirement-r1_resp3-request-解码兼容-s2_数组元素支持-resp3-标量类型，depends on task 1.1
- [√] 1.3 更新/新增单测：`yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespCommandDecoderStrictnessTest.java` 与新增 `RespCommandDecoderResp3RequestTest.java`，verify why.md#requirement-r1_resp3-request-解码兼容-s1_attributes-前缀不再破坏连接，depends on task 1.2

## 2. Security Check
- [√] 2.1 执行安全检查（G9）：输入上限、attributes 跳过的嵌套深度限制、异常路径无 ByteBuf 泄漏

## 3. Documentation Update（SSOT 同步）
- [√] 3.1 更新 `helloagents/wiki/overview.md` 的“协议边界（RESP2/RESP3）”段落：反映 request 侧 RESP3 兼容范围与限制
- [√] 3.2 更新 `helloagents/wiki/modules/protocol-netty.md`：反映 `RespCommandDecoder` 新的兼容策略与边界
- [√] 3.3 更新 `helloagents/wiki/api.md` 的 RESP3 说明：反映 request 侧 RESP3 attributes + 标量兼容范围与限制

## 4. Testing
- [√] 4.1 执行 `mvn test` 并确认通过（覆盖新用例）
