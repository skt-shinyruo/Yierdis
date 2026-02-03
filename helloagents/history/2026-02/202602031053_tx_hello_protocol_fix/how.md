# Technical Design: TX HELLO 协议切换修复 + 文档 SSOT 同步

## Technical Solution

### Core Technologies
- Java 17 / Maven
- Netty（TCP server + pipeline）
- RESP2 request 主路径 + RESP3 reply 最小子集（`HELLO 3` 协商后切换）
- Fast path：`RespWriter` 直接写 RESP reply（避免构建对象树）
- 单线程执行模型：`NettyCommandExecutor` 串行执行命令（配套有界队列、bytes 预算与背压）

### Implementation Key Points

#### 1) Root Cause（协议破坏根因）
- `EXEC` 的实现会复用同一个 `RespWriter`，逐条执行队列中的命令并输出一个数组作为结果容器。
- `HELLO 3` 会在执行时立刻 `out.setProtocol(RESP3)`，随后写出 RESP3 `map`（`%...`）。
- 当连接当前为 RESP2 且 `HELLO 3` 出现在 `EXEC` 的数组元素里时，RESP2 客户端在解析数组元素时遇到 `%` 会失败，导致协议流被破坏。

#### 2) Fix Strategy（修复策略）
核心原则：**事务期间禁止“改变连接协议/改变外层容器语义”的命令执行**，以避免在同一 reply 中混入不同协议类型前缀。

本次选择对 `HELLO` 做“硬护栏”：
- MULTI 模式下禁止 `HELLO` 入队与执行（`HELLO 2/3` 均拒绝）
- 发生拒绝时将事务标记为 aborted（使 `EXEC` 返回 `EXECABORT` 并丢弃队列）
- 这样可保证：
  - `EXEC` 的输出不会混入 RESP3 `%/~/>/|/...` 类型前缀
  - 连接协议不会在 `EXEC` reply 中途切换
  - 语义更贴近 Redis 的“入队阶段出错 → EXECABORT”模型

#### 3) Transaction Aborted 的实现方式
为避免 core 直接依赖 server/Netty，同时保持可扩展性：
- 在 `yierdis-protocol` 的 `RespTransactionState` 增加 **default 方法**用于标记 aborted（保持对非 server 实现的兼容）
- server 侧的 `TransactionState` 实现该方法，将事务置为 aborted；队列是否继续接收入队保持与既有超限 aborted 语义一致，并由队列上限负责兜底（避免 OOM）
- command processor 在 MULTI 入队路径中识别 `HELLO` 并触发 aborted（不入队）

#### 4) Tests（回归测试策略）
采用 server 的 fast pipeline 回归测试形态（EmbeddedChannel）来真实覆盖解码→入队→执行→编码：
- 构造 RESP2 multi-bulk 请求：`MULTI` → `HELLO 3` → `EXEC`
- 断言：
  - `HELLO 3` 在 MULTI 下返回 `-ERR ...`（不是 `+QUEUED`）
  - `EXEC` 返回 `-EXECABORT ...`（并验证队列被丢弃）
  - 连接协议未被切换（例如 `GET missing` 在 RESP2 下仍返回 `$-1`，不出现 RESP3 `_\r\n`）
- 同时覆盖“连接已处于 RESP3 时的 `HELLO 2`”在 MULTI 下同样被拒绝，以避免 `EXEC` 中途切回 RESP2

## Architecture Decision ADR

### ADR-20260203-01: 在事务中禁止 HELLO，并以 EXECABORT 失败
**Context:** `HELLO` 是连接级协议协商命令；在事务 `EXEC` 的数组 reply 中执行会导致协议前缀混入，破坏 RESP2 客户端解析。

**Decision:** 在 MULTI 模式下禁止 `HELLO`（2/3）入队与执行；拒绝时将事务标记为 aborted，从而让 `EXEC` 返回 `EXECABORT` 并丢弃队列。

**Rationale:**
- 协议安全优先：避免产生“部分回复不可解析”的硬故障
- 语义可解释：与 Redis 的“入队阶段错误 → EXECABORT”接近，降低使用者困惑
- 改动集中：仅对 `HELLO` 增加护栏，不扩散到其他命令的行为变更

**Alternatives:**
- Alternative A：仅拒绝 `HELLO` 但不 aborted，允许继续入队并执行其他命令 → 拒绝原因：可能导致使用者误以为事务完整执行，且对内存驻留没有收益。
- Alternative B：延迟协议切换到 reply 完成后再生效 → 拒绝原因：`HELLO 3` 的 reply 仍需要 RESP3 `map`，放在 `EXEC` 的数组元素中依然会破坏 RESP2；本质矛盾无法通过“延迟提交 session.protocol”解决。

**Impact:**
- 行为变化：MULTI 下 `HELLO` 不再返回 `QUEUED`，而是 error + EXECABORT
- 文档需要明确该边界（同时也更符合教学目标：区分“连接级握手命令”与“事务内命令”）

## Security and Performance
- **Security:**
  - 不引入新的网络面与外部依赖
  - 错误信息保持简短、无 CRLF 注入风险（沿用 `RespWriter.error` 的净化/限长逻辑）
- **Performance:**
  - aborted 后可选择拒绝继续入队，避免无意义的 backlog/事务队列驻留
  - 文档明确单线程执行模型与大输出/O(N) 命令风险，配合 `STATS/INFO` 排障

## Testing and Deployment
- **Testing:** `mvn test` 必须通过；新增回归测试覆盖 HELLO-in-MULTI/EXEC
- **Deployment:** 无需额外发布步骤；完成后更新 `helloagents/CHANGELOG.md` 并迁移 solution package 到 `helloagents/history/`
