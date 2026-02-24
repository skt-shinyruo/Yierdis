<!-- migrated_from: history/2026-02/202602031225_resp3_request_compat/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: RESP3 Request 兼容性对齐（decoder 支持 attributes + scalar）

## Technical Solution

### Core Technologies
- Java 17
- Netty `ByteToMessageDecoder`
- RESP 协议限制：`RespLimits`（maxBulkBytes/maxArgs/maxLineBytes/maxNestingDepth）

### Implementation Key Points

1. **保留 fast-path + 零拷贝**
   - 仍以 `* array` 为主路径解码为 `RespCommand(argv)`。
   - 通过 `ByteBuf.retainedSlice(start, len)` 创建 frame，argv 仅记录 offset/len（不拷贝 bulk payload）。

2. **支持 RESP3 attributes 前缀**
   - 在 request 解码入口处识别 `|`：
     - 解析并跳过 attributes map（pairs 结构，key/value 都是 RESP value）。
     - 忽略 attributes 内容，不向命令层透出（避免引入新 API 面）。
     - 继续解析紧随其后的命令（通常为 `* array`）。
   - 为避免实现漂移，attributes 的跳过逻辑复用与 `RespDecoder` 一致的“切帧/跳过”语义（实现上在 request decoder 内部提供等价的 `trySkipOne` 支持）。

3. **支持 `* array` 内的 RESP3 标量类型**
   - 在数组元素解析处允许以下类型，并映射为 argv 的 bytes slice：
     - `$` blob string（含 `$-1` → null）
     - `_` null（→ null）
     - `+` simple string（取行内容）
     - `:` integer（取行内容）
     - `,` double（取行内容）
     - `#` boolean（取 `t|f`）
     - `(` big number（取行内容）
     - `=` verbatim string（忽略 3-char format 前缀，仅取 `format:` 后的内容）
   - 对复杂/聚合类型（`%` map / `~` set / `>` push / `|` attribute 作为数组元素）保持 `Protocol error`，避免将结构体误当成参数。

4. **上限与错误模型**
   - `maxArgs/maxBulkBytes/maxLineBytes` 继续生效。
   - attributes map 的 pairs 数量也受限（避免 metadata 造成 DoS）。
   - `maxNestingDepth` 用于 attributes 内跳过嵌套结构（安全防护）。
   - 半包/粘包：当数据不足时必须回滚 readerIndex 并返回 null（保持 Netty decoder 正确行为）。

## Architecture Decision ADR

### ADR-001: request decoder 兼容 RESP3 request 采用“跳过 attributes + 标量 slice”方案
**Context:** 现有 request decoder 仅支持 RESP2 bulk array，且对 RESP3 前缀直接 protocol error，导致 RESP3 request 形态客户端/代理不兼容。  
**Decision:** 在 `RespCommandDecoder` 内新增对 `| attribute` 前缀的跳过能力，并在 `* array` 内支持 RESP3 标量类型，将其映射为 argv 的 bytes slice。  
**Rationale:** 兼容性提升同时保持 zero-copy；attributes 不进入命令层避免 API 破口；复杂结构体作为参数直接拒绝避免语义不确定。  
**Alternatives:** 直接引入通用对象树解析（`RespObjectParser`）并再转换为 argv → Rejection reason: 分配与拷贝显著增加，且会弱化 fast-path 的教学/性能价值。  
**Impact:** request decoder 代码复杂度略升；需要新增测试覆盖与文档同步。

## Security and Performance

- **Security：**
  - attributes/数组长度/行长度/递归深度均受限，降低 DoS 风险。
  - 不支持聚合类型作为参数，避免 parser 被构造出过深/过大的结构体输入。
- **Performance：**
  - bulk/string/line 类型均保持零拷贝 slice；
  - 仅在 attributes 跳过路径引入额外扫描（与 reply 侧 `RespDecoder` 同量级）。

## Testing and Deployment

- **Testing：**
  - 新增 `RespCommandDecoderResp3RequestTest`：
    - attributes 包裹 request 能正确解码为命令 argv
    - array 内 RESP3 标量能被映射为 argv bytes
  - 更新 `RespCommandDecoderStrictnessTest`：保留对 top-level 非命令形态的拒绝，但不再将 `|` 视为立即 protocol error（应等待完整 attributes frame 或成功解码）。
  - 运行 `mvn test` 全量回归。

