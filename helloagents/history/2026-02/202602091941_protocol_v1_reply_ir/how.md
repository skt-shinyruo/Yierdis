# Technical Design: Custom Protocol v1 Reply IR（协议语义中间层）与错误模型收敛

## Technical Solution

### Core Technologies

- Java 17 + Maven 多模块
- Netty pipeline（server/client/bench）
- `yierdis-protocol`：JSON codec（`JsonParser/JsonWriter`）与协议抽象
- `yierdis-bytes` / `yierdis-bytes-netty`：`BytesSink` / `ByteBuf` 适配

### Implementation Key Points

1. **定义 Reply IR（语义中间层）作为 SSOT：**
   - 目标：用一组“可表达、可测试、无歧义”的值类型表示 reply 语义，而不是依赖 JSON/RESP 的隐式限制。
   - 形态：`ReplyValue`（标量/bytes/array/map/error）+ `ReplyEnvelope`（ok/error 顶层包裹）。
   - 注意：实现应支持 streaming-friendly 的写出路径（避免对大数组/大 map 全量缓冲），但语义必须一致。

2. **为 Custom Protocol v1 明确 wire 表示（breaking）：**
   - 顶层 envelope（保持风格一致但允许字段调整）：
     - success：`{"ok":true,"result":<value>}\\n`
     - error：`{"ok":false,"error":{"kind":"command|protocol|internal","message":"..."}}\\n`
   - 值编码（建议）：
     - `null` → JSON `null`
     - `boolean` → JSON `true/false`
     - `integer/double` → JSON number（double 必须 finite；超大整数可保留为 string 或显式 `{"$num":"..."}`）
     - `string` → JSON string
     - `bytes` → 显式 tagged object（例如 `{"$b64":"<base64>"}`），避免 UTF-8 best-effort 造成信息丢失
     - `array` → JSON array
     - `map` → 显式 tagged object（例如 `{"$map":[[k,v],...]}`），避免 JSON object key 只能为 string 的限制
     - `error value`（嵌套 error）→ 显式 tagged object（例如 `{"$error":{"kind":"...","message":"..."}}`），避免与普通 object 混淆

3. **统一错误 message 规则（SSOT helper）：**
   - message 空/blank 的默认值、CRLF→space、长度截断上限（当前代码为 256）集中在 `yierdis-protocol`。
   - decoder/handler/writer 均不得各自实现净化/限长；最多保留“日志用途”的独立逻辑，但回包必须走统一 helper。

4. **decoder 边界调整：**
   - `CustomRequestDecoder`：
     - 继续承担 framing/parse/schema 校验与 resync（discard-to-LF）；
     - 遇到协议错误时产出 **ProtocolError 事件/消息**（进入 pipeline），由后续 handler 调用统一 encoder 写回。

5. **测试体系补齐：**
   - `yierdis-protocol`：encoder 的纯单元 golden tests（覆盖值组合与 error sanitize）
   - `yierdis-protocol-netty`：decoder conformance tests（非法帧→protocol error 事件 + resync）
   - `yierdis-server`：EmbeddedChannel wire-level golden tests（端到端：bytes in → NDJSON out）
   - `yierdis-client`/`yierdis-bench`：解析与展示的兼容性（对齐新 wire spec）

## Architecture Design

```mermaid
flowchart TD
    Inbound[Netty ByteBuf] --> Decoder[CustomRequestDecoder\nframing/parse/resync]
    Decoder -->|Command| Handler[YierdisFastCommandHandler\nsubmit to executor]
    Decoder -->|ProtocolError event| PEH[ProtocolErrorHandler\nencode & write reply]
    Handler --> Executor[NettyCommandExecutor\nsingle-thread execute]
    Executor --> IRW[Reply IR writer\n(command layer)]
    IRW --> Enc[NDJSON encoder (SSOT)]
    Enc --> Outbound[Netty ByteBuf → write/flush]
```

## Architecture Decision ADR

### ADR-20260209-01: map/bytes 的 JSON 表示采用显式 tagged value（`$map`/`$b64`）
<a id="adr-20260209-01"></a>

**Context:**  
JSON object key 只能为 string；同时 bulk bytes/二进制 key 在当前实现中只能 UTF-8 best-effort 或 fallback，占位策略会掩盖错误并造成语义漂移。

**Decision:**  
在 Custom Protocol v1 的 reply 中引入显式 tagged value：
- bytes：`{"$b64":"..."}`（base64 编码，语义保真）
- map：`{"$map":[[k,v],...]}`（entries 结构，key/value 均为 value，可表达任意类型）
- nested error：`{"$error":{"kind":"...","message":"..."}}`

**Rationale:**  
该方案能以 JSON 原生能力表达完整语义，不依赖 best-effort，且易于写出 wire-level golden tests 锁定行为；同时保持 NDJSON framing 不变。

**Alternatives:**  
- JSON object map（string key only） → 拒绝原因：无法表达 binary key/非 string key，且会诱发 fallback/漂移。  
- bytes 继续用 UTF-8 string 表示 → 拒绝原因：语义不保真，且无法区分“字符串”与“任意 bytes”。  
- 引入 CBOR/MessagePack 等二进制编码 → 拒绝原因：违背“保持 JSON request + NDJSON reply”约束，且引入新复杂度。

**Impact:**  
- breaking change：client/bench/测试基线需要更新。
- 编码体积增加：base64 与 tagged object 会放大 payload；需要用测试与基准验证可接受性。

## Security and Performance

- **Security:**
  - 统一 error message sanitize（CRLF 注入防护、长度截断）并通过测试锁定。
  - 保持并验证 `maxPayloadBytes/maxHeaderBytes/maxDiscardBytes/maxArgs` 等 DoS 安全上限。
- **Performance:**
  - 设计上以“语义 IR + 单点 encoder”为 SSOT，但实现上尽量 streaming-friendly，避免对大结果集全量物化。
  - 为 encoder 热路径提供低分配实现（避免频繁创建中间 `String`/`List`）。

## Testing and Deployment

- **Testing:**
  - 新增 wire-level golden tests（端到端 NDJSON 输出逐字节锁定）。
  - 新增 error format consistency tests（覆盖 decoder/handler/command/internal 各类错误来源）。
  - 新增 decoder resync conformance tests（非法帧后紧跟合法帧仍可执行）。
- **Deployment:**
  - breaking change：无需兼容旧 client；发布时同步更新 CLI/bench，并在 README/wiki 中标注新的 wire 规范。
