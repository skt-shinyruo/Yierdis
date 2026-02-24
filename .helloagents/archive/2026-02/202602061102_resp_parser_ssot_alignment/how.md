<!-- migrated_from: history/2026-02/202602061102_resp_parser_ssot_alignment/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: RESP 解析 SSOT 收敛与质量兜底

## Technical Solution

### Core Technologies
- Java（Maven 多模块）
- Netty（`ByteToMessageDecoder` / `EmbeddedChannel`）
- JUnit4（现有测试体系延续）
- 现有 SSOT 抽象：`BytesSource` / `RespFrame` / `RespLimits` / `RespWriter` / `RespObjectParser`

### Implementation Key Points

1. **在 `yierdis-protocol` 新增 RESP wire 工具（SSOT，Netty-free）**
   - 目标：把“scan/skip 的协议语义”从多个 decoder/parser 中抽取出来，成为唯一来源。
   - 关键能力（以 `BytesSource + availableLimit` 为输入）：
     - CRLF 扫描（带 `maxLineBytes` 上限）
     - ASCII 数字解析（int/long），用于校验整数/长度
     - `trySkipOne(...)`：按 RESP 前缀跳过一个完整对象，支持 RESP2/RESP3（含 streamed blob / streamed aggregates / attributes / push / map / set / scalar）
     - 统一 limits：bulk/array/nesting/line（默认值来源 `RespLimits`，可注入自定义上限用于测试）
   - 返回约定：
     - 数据不足：返回 `-1`（让 streaming adapter 回滚 readerIndex 并等待更多数据）
     - 协议错误：抛出 `IllegalArgumentException("Protocol error: ...")`（文案尽量保持与现有一致，避免行为漂移）

2. **`yierdis-protocol-netty` 作为 adapter：只保留 Netty I/O 细节**
   - `RespDecoder`（reply framing）：
     - 通过 ByteBuf 的 `readerIndex()/writerIndex()` 将可用区间映射为 SSOT skipper 的 `availableLimit`
     - 用 SSOT skipper 计算 endIdx 后做 `retainedSlice(start, len)` 输出 `NettyRespFrame`
     - 保持 wire-preserving（frame bytes 必须与输入一致）
   - `RespCommandDecoder`（request decoding）：
     - 保留最常见命令形态的 fast-path（RESP2 array-of-bulk-strings / inline command），避免性能回退
     - 将高风险、分支复杂、易漂移的部分委托给 SSOT skipper：
       - attributes 链式包裹（`|...|...*...`）
       - streamed blob string / streamed aggregates 的 skip/scan
       - skip/scan 的递归与 limits 语义
     - materialize 回退路径保持不变，但由测试覆盖其触发条件与资源释放。

3. **`RespObjectParser` 与 decoder 共享“wire 语义”**
   - 目标：减少 parser 自己的一套 `indexOfCrlf/parseInt` 逻辑，降低 SSOT 漂移概率。
   - 策略：
     - 将 line 扫描、数字解析、streamed end marker 处理等基础能力复用 SSOT support
     - parser 的“对象分配/byte[] 物化”仍然保留（它的定位是 CLI/测试/调试）

4. **错误策略与连接关闭语义**
   - decoder 层保持 `IllegalArgumentException("Protocol error: ...")` 风格；
   - server pipeline 层（现有 `yierdis-server` 测试覆盖）继续要求：protocol error → `-ERR Protocol error: ...` + close connection；
   - 通过 golden/integration 用例锁定，防止文案/行为不一致。

## Architecture Decision ADR

### ADR-20260206-01：RESP wire scan/skip 语义下沉到 `yierdis-protocol` 作为 SSOT
**Context:** `RespCommandDecoder` / `RespDecoder` / `RespObjectParser` 之间存在相近但不完全一致的协议解析与 skip/scan 实现；随着 RESP3 attributes/streamed 扩展，分支爆炸与漂移风险上升。  
**Decision:** 在 `yierdis-protocol` 引入 Netty-free 的 RESP wire support + skipper，统一 scan/skip 的语义与 limits；Netty 模块仅做 streaming adapter 与 zero-copy 切片。  
**Rationale:** 降低重复实现带来的维护成本与行为漂移概率；通过统一 SSOT + 测试矩阵锁死边界，允许后续继续做 fast-path 优化而不破坏语义。  
**Alternatives:** 仅通过增加测试锁定行为，不抽取 SSOT helper → Rejection reason: 重复实现仍在，未来迭代更容易出现“修一处漏一处”的漂移。  
**Impact:** `yierdis-protocol` 增加 wire-level 解析/skip 代码；`protocol-netty` decoders 变薄；需要补齐/调整测试矩阵以确保兼容性与性能不退化。

## Security and Performance

- **Security:**
  - 严格执行 `RespLimits`（bulk/array/nesting/line），防止 DoS（超长 line、巨量嵌套、streamed 累积长度溢出等）
  - 确保错误回复不产生 CRLF 注入（回复写出侧已做净化，回归测试覆盖）
  - 对“数据不足”与“协议错误”严格区分，避免误判导致连接异常关闭或无穷等待
- **Performance:**
  - 保持 request decoder 的 bulk-array fast-path；
  - SSOT skipper 重点覆盖 attributes/streamed/skip-scan 等复杂分支，避免在常见路径引入额外 per-byte 成本；
  - 如发现热点回退，可在 adapter 层保留 ByteBuf 特化扫描，但必须由黄金用例锁死语义一致性。

## Testing and Deployment

- **Testing:**
  - Golden cases：补齐 “attributes 链式 + 内部复杂嵌套/streamed” 等目前覆盖不足的分支
  - Round-trip（双口径）：
    1) `RespObject` → encode → `RespDecoder` → frame bytes 与 wire 相等
    2) decode 后 `RespObjectParser.parse(frame)` 可成功解析并满足语义断言
  - 短 fuzz（默认 `mvn test`）：≤3s、固定 seed、失败打印 seed；随机分片输入覆盖半包/截断/噪声与 resource release
- **Deployment:** 无运行时部署动作；以库代码变更 + 测试变更交付

