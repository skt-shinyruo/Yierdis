# Technical Design: 支持 RESP3 + Inline（提升 Redis 客户端兼容性）

## Technical Solution

### Core Technologies
- Java 17
- Netty（现有 TCP pipeline）
- 现有 fast-path：`RespCommandDecoder` → `RespCommand` → `YierdisFastCommandProcessor` → `RespWriter`

### Implementation Key Points

1. **连接级协议版本状态**
   - 默认 `RESP2`
   - 当收到 `HELLO 3`（或未来扩展的协商命令）后，将该连接切换为 `RESP3`
   - 使用 Netty `Channel.attr(...)` 保存协议版本（线程安全、跨 handler 可读）

2. **请求解码：兼容 RESP2 multi-bulk + inline**
   - 保留现有 RESP2 multi-bulk 解析作为首选快路径（首字节 `*`）
   - 新增 inline 解析分支（首字节非 `*`，读取单行到 CRLF，按空白拆分 argv）
   - 目标输出仍为 `RespCommand`，避免改动命令执行层与 DB 语义
   - inline 解析的约束：
     - 仅支持最小可用语法（空白分隔，不支持复杂引号/转义；如需再迭代）
     - 限制最大行长度与最大参数数量（与现有 maxLine/maxArgs 对齐）

3. **响应写出：RESP2/RESP3 双栈**
   - 引入“协议感知 writer”（RESP2 与 RESP3 的差异主要体现在 nil 与 HELLO 的结构）
   - RESP3 下关键差异点：
     - nil：使用 RESP3 的 `_`（null）类型
     - `HELLO`：使用 RESP3 的 `%` map 返回（至少包含 server/version/proto/mode/role）
   - 其余简单类型（`+/-/:/$/*`）尽量复用现有编码，降低改动面

## Architecture Decision ADR

### ADR-001: 使用 Channel Attribute 保存协议版本（RESP2/RESP3）
**Context:** 需要在同一连接上根据 `HELLO` 协商结果切换响应编码，同时命令执行在单独线程中运行（`CommandExecutor`）。  
**Decision:** 采用 `Channel.attr(AttributeKey<...>)` 保存当前连接协议版本，并在 handler/executor 写响应时读取。  
**Rationale:**  
- 线程安全、跨 pipeline 共享；
- 不引入全局状态；
- 与 Netty 常见做法一致。  
**Alternatives:**  
- 方案：在 `RespCommand` 上携带协议版本 → 拒绝原因：协议是连接级状态，绑在每条命令上容易错用/冗余。  
**Impact:** 需要在写响应路径（含 executor）能读取 channel attribute。

## Security and Performance

- **Security:**
  - inline 命令强制最大行长度、最大参数数；异常输入返回协议错误并关闭连接（与现有策略一致）。
  - 错误消息净化（CRLF 注入）保持现有 `RespWriter` 逻辑。
- **Performance:**
  - RESP2 multi-bulk 保持 fast-path，不引入额外对象树构建。
  - inline 仅作为备用路径，不针对极致性能优化。

## Testing and Deployment

- **Testing:**
  - 新增单测覆盖：inline PING、RESP3 HELLO 3、RESP3 nil（GET missing）等关键场景。
  - 回归：`mvn test` 确保现有命令语义不回退。
- **Deployment:** 仅影响协议兼容性，无部署流程变更。

