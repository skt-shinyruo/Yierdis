<!-- migrated_from: history/2026-02/202602061601_custom_protocol_v1/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: Custom Protocol v1（完全替换 RESP）

## Technical Solution

### Core Technologies

- Java 17
- Netty（transport）
- 自研最小 JSON codec（无第三方依赖；仅支持本协议需要的 JSON 子集）

### Implementation Key Points

#### 1) Wire Protocol（v1）

本协议面向“UTF-8 文本命令参数 + JSON reply”，并以**可恢复解析**为目标。

**Request framing（length-prefixed）**

采用单帧格式（文本 header + payload + 行结束符）：

```
<len>:<json-payload>\n
```

- `<len>`：十进制 ASCII，表示 `<json-payload>` 的 UTF-8 字节长度（不含结尾 `\n`）
- `:`：分隔符
- `<json-payload>`：JSON object，推荐单行（不包含实际 `\n`/`\r` 字符）
- 结尾：单个 `\n` 作为帧终止符（用于 resync；payload 内禁止出现未转义的真实换行）

**Reply framing（JSON line）**

服务端对每个命令返回一行 JSON（NDJSON 风格）：

```
<json-reply>\n
```

约束：writer 必须保证输出为**单行**（所有控制字符按 JSON 转义写出，禁止写出真实 `\n/\r`）。

#### 2) JSON Schema

**Request JSON（示例）**

```json
{"cmd":"PING","args":[]}
```

- `cmd`：命令名（大小写不敏感，server 内部按 upper 归一）
- `args`：UTF-8 文本参数数组（不支持二进制；需要二进制时作为后续版本扩展）

**Reply JSON（统一 envelope）**

成功：

```json
{"ok":true,"result":...}
```

失败：

```json
{"ok":false,"error":{"kind":"protocol|command|internal","message":"..."}}
```

其中 `result` 支持：
- `null`
- `string`
- `number`（整数优先；如未来需要浮点按 JSON number 输出）
- `array`（嵌套任意 `result` 结构）
- `object`（key 必须为 string）

#### 3) 语义映射（从现有输出到 JSON）

本次不要求保持 RESP 的“类型前缀细粒度区别”，而是保留可用语义：

- `simple string` / `bulk string` → JSON string（缺失值 → JSON null）
- `integer` → JSON number
- `array` / `set` → JSON array（保持元素顺序；set 语义仅在 server 逻辑层存在）
- `map` / `attribute` → JSON object（仅当 key 可稳定转为 string；否则退化为 array-of-pairs）
- `error` → `ok=false` envelope

#### 4) 可恢复解析（Protocol error 不断连）

目标：遇到以下问题尽量**返回错误 reply**，并继续从下一帧开始解析：
- header 不合法（非数字、缺少 `:`、len 为空等）
- len 超过上限或为负数
- payload 不是合法 JSON
- JSON schema 不满足（缺少 cmd/args 类型不对）

实现建议（Netty decoder 状态机）：

- **正常态**：从 cumulation 中解析 `<len>:`，校验 len 上限；若不足以读完 payload + `\n`，返回等待更多数据
- **错误态（可 resync）**：写出一条 protocol error reply，然后丢弃直到下一个 `\n`（帧终止符），回到正常态
- **安全兜底**：超过 `maxDiscardBytes` 或持续无法找到终止符时，允许断连（防 DoS）

#### 5) 内部抽象重构（Solution 2 的核心）

为达成“完全替换 RESP”，core 不再依赖 RESP 命名与类型：

- 引入协议无关的 `Command`/`CommandArgView` 抽象（UTF-8 参数视图）
- 引入协议无关的 `ReplyWriter`/`ReplyValue` 抽象（表达 string/number/null/array/object/error）
- 将 `yierdis-core` 的命令实现从 `RespCommand/RespWriter` 迁移到上述抽象
- 新协议实现提供：
  - `CustomCommand`（由新 decoder 构造）
  - `JsonLineReplyWriter`（写出 NDJSON）
- RESP 相关实现进入 `legacy`，在迁移完成后逐步删除（含 README/overview 口径）

## Architecture Design

```mermaid
flowchart LR
    C[Client/Bench] -->|custom request frames| N[Netty Channel]
    N --> D[CustomRequestDecoder]
    D --> H[Command Handler]
    H --> X[Single-thread Executor]
    X --> P[Core Command Processor]
    P --> W[JsonLineReplyWriter]
    W -->|NDJSON replies| N
```

## Architecture Decision ADR

### ADR-001: 完全替换 RESP，采用自定义 length-prefixed request + JSON line reply
**Context:** RESP2/RESP3 兼容性边界复杂，严格断连模型与部分客户端“宽容解析”不一致；协议扩展维护成本高。
**Decision:** 自定义协议 v1：request 采用 length-prefix，reply 采用 NDJSON；并将 core API 抽象为协议无关接口。
**Rationale:** 降低对外兼容压力，减少协议边界争议；JSON 提升可观测性与调试成本；length-prefix 便于 framing 与大包处理；resync 状态机可实现“错误不断连”。
**Alternatives:**
- 保持 RESP 严格模型 → 拒绝原因：仍受生态兼容预期影响，边界争议持续
- 仅做协议适配层（不改 core API） → 拒绝原因：长期仍保留 RESP 语义与命名，难以彻底清理与演进
**Impact:** 对外破坏性变更（redis-cli 不可用）；需要大量测试与文档同步；性能与安全需重新评估。

## Security and Performance

- **Security:**
  - `len`/header/payload 强上限（拒绝超大包与恶意拖拽）
  - JSON 输出必须做控制字符转义，避免换行注入破坏 framing
  - 错误消息限长、避免回显原始 payload
- **Performance:**
  - reply writer 采用 streaming 写出，避免构建大对象树
  - 复用编码缓冲区，避免频繁分配
  - 对大数组输出增加预算/限额（与现有 backpressure 口径对齐）

## Testing and Deployment

- **Testing:**
  - 单元测试：JSON codec、framing、resync、schema 校验
  - 集成测试：server/client 端到端，覆盖 pipelining、非法帧不掉线
  - bench 回归：更新 strictReplies 校验以适配新 schema
- **Deployment:**
  - 默认端口仍为 `6378`
  - README/overview 明确“非 RESP 协议”，避免误用 redis-cli
