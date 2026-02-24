<!-- migrated_from: history/2026-02/202602061216_resp_codec_test_matrix/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: RESP 解析质量兜底（Golden + Round-trip + Fuzz + 一致性差分）

## Technical Solution

### Core Technologies
- Java 17（Maven 多模块）
- Netty（`ByteToMessageDecoder` / `EmbeddedChannel`）
- JUnit4（延续现有测试体系）
- 现有 SSOT 组件（直接复用）：
  - `RespLimits`（默认上限 SSOT）
  - `RespWireSkipper` / `RespWireSupport`（wire scan/skip SSOT）
  - `RespObjectParser`（对象化语义解析，用于测试/CLI/调试）
  - `RespWriter` / `RespEncoder`（RESP2/RESP3 编码能力，用于 round-trip）

### Implementation Key Points

1. **黄金用例（Golden）按“形态维度”组织，而不是按“类/方法”组织**
   - 形态维度（重点覆盖）：
     - attributes 链式包裹（含 streamed 值）
     - streamed blob（含截断/半包）
     - streamed aggregates（数组/Map/Set 的 `?` + `.` end marker）
     - inline vs array 严格性（top-level inline，array 内只接收 bulk/scalar）
     - limits 一致性（bulk/array/nesting/line）
     - skip/scan 回退正确性（fast-path/materialize 结果等价）
   - 每个维度至少包含：
     - 1 个“完整包”用例
     - 1 个“半包/截断恢复”用例
     - 1 个“非法输入触发 protocol error”用例（尽量走 server 集成断言 close）

2. **随机分片（packetization）作为一等公民**
   - 对 decoder 的正确性而言，“同一 wire 在不同分片下输出一致”是最关键的性质之一。
   - 测试策略：
     - 提供通用的 `writeInboundInChunks(...)` 测试 helper（仅在 test 目录）
     - 对同一 payload 运行：
       - 单包写入（one-shot）
       - 固定分片（例如 1 byte、2 bytes、随机 chunk size）
     - 断言输出对象（frame bytes / command argv）一致

3. **Round-trip（encode→decode→parse）兜底**
   - reply 方向（最易做，也最可靠）：
     1) 使用 `RespEncoder` / `RespWriter` 输出 wire bytes
     2) `RespDecoder` 切出 `NettyRespFrame`
     3) 校验 frame bytes == 原 wire（wire-preserving）
     4) `RespObjectParser.parse(frame)` 成功，并对结构做语义断言（避免只测“能 parse”）
   - request 方向（适度覆盖）：
     - 对 command array（bulk/scalar/streamed blob）用 `RespWriter` 写出 array wire，喂给 `RespCommandDecoder`
     - 断言 `argc/arg bytes/null arg` 与生成器一致

4. **一致性差分（Differential consistency）**
   - 对一组固定 payload：
     - `RespWireSkipper.trySkipOne(...)` 返回值应等于 payload 长度（或在截断输入时返回 -1）
     - `RespDecoder` 切帧后 bytes 与 payload 相等
     - `RespObjectParser` 解析后不应出现 trailing bytes（或明确抛出 `Protocol error:`）
   - 目标：用最小断言锁定“协议语义是否一致”，降低实现漂移风险。

5. **轻量 fuzz（无第三方依赖，默认纳入 `mvn test`）**
   - 输入生成策略（优先稳定/可复现）：
     - 种子集合：来自 golden payload 的少量 wire 样本
     - 随机变异（mutation）：随机分片、随机截断、少量字节翻转（非法路径只要求不死循环/能被关闭/能报错）
     - 合法生成（generation）：随机生成小尺寸的 `RespObject` 树 → encode → decode（reply 方向）
   - 运行约束：
     - 固定 seed（可通过系统属性覆盖）
     - 迭代数与 payload 上限可配置（默认保证 ≤3s）
     - 失败输出 seed 与最小 payload（便于复现）

## Architecture Decision ADR

### ADR-20260206-02：以测试矩阵作为 RESP 解码语义的“最后护栏”
**Context:** `RespCommandDecoder`/`RespDecoder`/`RespObjectParser` 在 RESP3（attributes/streamed）扩展后分支复杂度提升；即使已有 SSOT 组件（`RespLimits`/`RespWireSkipper`），仍存在 fast-path/回退/适配层漂移风险。  
**Decision:** 优先补齐 golden + round-trip + differential + 轻量 fuzz 的测试矩阵，先锁定行为边界，再在测试护航下逐步收敛实现细节。  
**Rationale:** 测试是跨实现一致性的最低成本 SSOT；能让性能优化与可维护性改造在可控范围内演进。  
**Alternatives:** 先做大规模重构收敛所有分支 → Rejection reason: 风险大、难验证、回归成本高。  
**Impact:** 测试用例数量与执行时间略增（通过时间预算与固定 seed 控制）；未来重构可依赖测试网进行回归验证。

## Security and Performance

- **Security:**
  - 严格覆盖 `RespLimits` 相关边界（防止超长 line、过深嵌套、streamed 累积长度等 DoS 向量）
  - 通过集成测试锁定 protocol error → close（避免 malformed 输入占用连接资源）
  - 区分 fatal protocol error 与非 fatal 参数非法（避免误关连接影响可用性）
- **Performance:**
  - 本方案以测试为主，不主动改动 fast-path
  - 若后续重构/收敛实现，必须以 round-trip + differential 作为回归基线，避免行为漂移

## Testing and Deployment

- **Testing:**
  - 单测：`yierdis-protocol` / `yierdis-protocol-netty` 增补 golden/round-trip/fuzz
  - 集成：`yierdis-server` 锁定 close/keep-open 行为
  - 约束：fuzz 测试默认 ≤3s，固定 seed，可复现
- **Deployment:** 无运行时部署动作；以代码变更 + 测试变更交付

