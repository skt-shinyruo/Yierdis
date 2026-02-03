# Technical Design: RESP3 全覆盖（request + reply + streamed + push + attributes）

## Technical Solution

### Core Technologies
- Java 17
- Netty `ByteToMessageDecoder` / `MessageToByteEncoder`
- RESP3 规范（含 streamed strings / streamed aggregates）

### Implementation Key Points

1. **SSOT 优先：先补齐 `yierdis-protocol` 的解析能力**
   - 扩展 `RespObjectParser`：支持 `$?` streamed blob string（chunked `;len`…`;0`）与 `*?/%?/~?` streamed aggregates（直到 `.` END）。
   - streamed 解析结果映射为现有类型（blob string / array / map / set），不引入“streamed 专用类型”，以降低上层 API 面。
   - 严格限制：sum(chunkLen) ≤ maxBulkBytes；streamed aggregate 元素数 ≤ maxArrayLen；嵌套深度 ≤ maxNestingDepth。

2. **Netty framing：`RespDecoder` 扩展 streamed skip**
   - `RespDecoder` 是“切帧 decoder”，必须能在不构建对象树的情况下定位 frame 的结束位置。
   - 新增：
     - `$?`：按 chunk 逐段跳过，直到 `;0\r\n`
     - `*?/%?/~?`：递归跳过内部元素，直到 `.\r\n`
   - 保持半包语义：数据不足返回 -1 并回滚 readerIndex。

3. **request decoder：从“fast-path argv”升级到“通用 RESP3 解析 + 严格命令约束”**
   - 引入“通用命令解码路径”：
     1) 使用（扩展后的）frame/skip 能力切出完整 request frame
     2) 使用 `RespObjectParser` 解析为 `RespObject`
     3) 仅接受“命令形态”：attributes 包裹的 array，或直接 array；inline command 仍保留
     4) 将 array 元素严格映射为 argv（blob string 允许 `$len` 与 `$?` streamed）
   - 对非命令形态（top-level 不是 array/inline）：返回 `ERR Protocol error: expected array` 并关闭连接（与 Redis 风格一致）。
   - streamed blob string 作为参数需要 materialize（拼接 chunk），因此该路径不再保证零拷贝；这是本方案明确接受的 trade-off。

4. **client push/attributes：避免 request/response 错配**
   - `YierdisClient` 的同步模型需要区分 push 与普通 reply：
     - frame 解析后若为 push（或 attributes 包裹 push），路由到 push handler/队列，不占用“等待 reply 的配额”
     - 仅非 push 的 frame 进入 responses 队列，用于 request/response 配对
   - CLI 输出层补齐 push/attributes 的展示（与 `redis-cli --resp3` 阅读习惯尽量接近）。

## Architecture Decision ADR

### ADR-001：以“SSOT 先补齐 streamed 解析 + Netty 切帧扩展”为主线
**Context:** streamed 需要同时出现在 framing 与语义解析层，否则会出现“能切帧但解析不了/能解析但切不出完整帧”的双轨漂移。  
**Decision:** 先在 `yierdis-protocol` 补齐 streamed 语义解析，再在 `yierdis-protocol-netty` 扩展 streamed 切帧。  
**Rationale:** 单一 SSOT 降低协议漂移风险，测试也更容易形成闭环。  
**Alternatives:** 只在 Netty 层实现 streamed 并绕过 SSOT → Rejection reason: 协议语义分裂，维护成本高。  
**Impact:** `RespObjectParser/RespDecoder` 变更较大，需要新增细粒度测试。

### ADR-002：request 解码采用“通用 RESP3 解析 + 严格命令约束”
**Context:** 现有 `RespCommandDecoder` 偏 fast-path，只覆盖 RESP2 + 小范围兼容；要做到 streamed + 全类型严格性，需要更通用的解析路径。  
**Decision:** request 侧允许引入通用解析：先切帧，再对象解析，再转换为 argv；并严格限制 top-level 必须是命令形态（array/inline）。  
**Rationale:** 正确性与规范对齐优先；性能可后置优化。  
**Alternatives:** 在现有 fast-path 上继续堆分支实现全类型 → Rejection reason: 复杂度爆炸且难以验证角落行为。  
**Impact:** request decode 可能引入更多分配；需要在 server 文档中明确该选择。

## Security and Performance

- **Security：**
  - 强制执行 `maxBulkBytes/maxArrayLen/maxNestingDepth/maxLineBytes`，并对 streamed 做累计上限，防止 DoS。
  - protocol error 统一走 “返回 error + close” 路径，避免连接进入未知解析状态。
- **Performance：**
  - streamed 与通用解析会增加分配；后续可选优化：为常见 RESP2/RESP3 bulk-array 保留 fast-path，遇到 streamed/复杂输入再 fallback。

## Testing and Deployment

- **Testing：**
  - 协议单测（protocol/protocol-netty）：覆盖 `$?`/`*?/%?/~?`、嵌套、半包、上限、END/CHUNK 终止符错误等。
  - server 集成测试：发送 raw streamed request，验证执行结果与 protocol error → close 行为。
  - client 测试：push 与 reply 乱序输入不导致错配。

