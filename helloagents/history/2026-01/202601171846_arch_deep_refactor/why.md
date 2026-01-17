# Change Proposal: 架构深度重构（协议/执行/客户端分层与 SSOT 收敛）

## Requirement Background

当前项目已经具备清晰的多模块拆分与“解码 → 入队 → 单线程执行 → 写回”的主链路，但在深入阅读与对齐长期演进目标时，仍存在一组会持续放大维护成本与行为漂移风险的问题：

1. **模块边界与抽象泄漏风险：**协议适配层与 server 运行时状态（连接级背压/统计/关闭语义）存在混杂空间，导致“看起来是协议模块，但实际上承载了 server 语义”的认知负担。
2. **默认值与 SSOT 漂移风险：**协议输入上限（bulk/args/line）、解析器/codec 的默认值分散在多处，容易出现“默认值不一致但编译能过、运行也不一定立刻暴露”的隐性问题。
3. **编码实现重复与不一致风险：**同一协议族存在多条写出路径（例如 object encoder vs fast-path writer），能力覆盖与行为净化策略可能不一致，长期会造成 subtle incompatibility。
4. **执行器复杂度与可验证性：**执行器承担了队列/预算/背压/调度/flush 合并/统计等多职责，虽然功能合理，但“单类过宽”的形态降低了可读性与单元可验证性。
5. **maxmemory 口径解释成本：**maxmemory/淘汰/诊断命令依赖“估算 + 真实 off-heap”组合口径，若缺少统一定义与文档护栏，会导致使用者误判（把估算当精确）。
6. **client/CLI 一致性与鲁棒性：**CLI 参数解析方式与 server/bench 不一致；client 的队列/异常路径对未来协议扩展（例如 RESP3 push）较敏感；同时 `System.exit` 的散落会降低可复用性。

本次改造选择“深度重构”路径：以更明确的层次边界与单点 SSOT 为第一目标，并以测试与回滚策略作为落地护栏，允许在不破坏教学属性的前提下进行小幅对外行为调整（例如帮助信息/错误边界/统计字段的更清晰表达）。

## Change Content

1. 将 **连接级协议会话（RESP2/RESP3）** 与 **server 连接运行时状态（背压/计数/closing）** 明确拆分为两个对象，分别归属到 protocol-netty 与 server。
2. 建立统一的 **协议上限默认值 SSOT**（bulk/args/line/nesting 等），让 args/codec/parser 共享同一组常量定义，并通过测试锁定一致性。
3. 统一 **编码输出 SSOT**：以 `RespWriter` 为唯一语义写出实现；保留/改造 object encoder 作为 adapter（或移除），确保输出行为与净化策略一致。
4. 对 `NettyCommandExecutor` 做职责拆分与可测试化重构：将队列调度、预算、背压控制、drain loop 等拆成可独立测试的组件，降低修改风险。
5. 明确 maxmemory 相关口径与输出：将“估算 vs 实占（off-heap）”的定义、字段含义与约束写入文档，并确保命令输出与文档一致。
6. client/CLI 深度收敛：统一 CLI 参数解析（与 server/bench 同栈）、为 response queue 引入边界与异常闭环、避免不必要的 `System.exit` 扩散。

## Impact Scope

- **Modules:**
  - yierdis-protocol
  - yierdis-protocol-netty
  - yierdis-server
  - yierdis-args
  - yierdis-client
  - yierdis-bench（可能仅测试与参数复用）
  - helloagents/wiki（同步架构与模块文档）
- **Files (representative, not exhaustive):**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/*`
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/*`
  - `yierdis-server/src/main/java/yier/bubu/redis/*`
  - `yierdis-args/src/main/java/yier/bubu/redis/args/*`
  - `yierdis-client/src/main/java/yier/bubu/redis/client/*`
- **APIs:** 对外命令行为保持兼容为主；允许少量“更一致/更清晰”的行为调整（例如帮助信息/错误边界/统计字段补充）
- **Data:** 无持久化数据结构变更；仅涉及内存估算与统计口径的解释与输出

## Core Scenarios

### Requirement: R1 Protocol Session Boundary
**Module:** protocol-netty / server
将“协议会话”与“server 运行时连接状态”完全解耦，避免模块语义混杂。

<a id="r1-s1-session-only"></a>
#### Scenario: R1-S1 Session only（ConnectionContext/Session 只承载 RESP2/RESP3 协商）
条件：连接建立 → 执行 `HELLO 3` → 后续执行基础命令
- 预期：协议协商状态仅由 `RespSession`（Netty 实现）承载；server 的 pending/backpressure/closing 等状态不再位于 protocol-netty
- 预期：RESP3 map/null 输出与 RESP2 bulk/null 输出保持一致，且切换可回退（`HELLO 2`）

<a id="r1-s2-server-state"></a>
#### Scenario: R1-S2 Server connection state（背压与 closing 在 server 内闭环）
条件：并发压测使队列触发条数或 bytes 水位线；或执行 `QUIT`
- 预期：背压状态机与 counters 由 server 模块的连接态对象维护；协议模块不感知调度细节
- 预期：`QUIT` 的 close-after-reply 语义不变，且 QUIT 后 backlog 命令仅回收不执行（无副作用）

### Requirement: R2 Protocol Limits SSOT
**Module:** protocol / args / protocol-netty
将协议上限默认值收敛为单点定义，并在各层引用同一来源。

<a id="r2-s1-defaults-consistent"></a>
#### Scenario: R2-S1 Defaults consistent（默认上限一致性锁定）
条件：未显式配置 maxBulk/maxArgs/maxLine 的情况下启动 server/client/bench 或运行 parser/codec 默认构造
- 预期：所有默认值来自同一处 SSOT 常量；任一处变更会在单测中被捕获（防漂移）

### Requirement: R3 Encode Path SSOT
**Module:** protocol / protocol-netty
统一写出路径，确保不同调用路径（server fast-path、测试 object 编码）行为一致。

<a id="r3-s1-one-writer"></a>
#### Scenario: R3-S1 One writer semantics（writer 语义统一 + 错误净化一致）
条件：写出 error/simple/integer/bulk/array/map/null 等回复
- 预期：所有路径使用同一套写出语义与安全净化策略（避免 CRLF 注入等 response splitting 风险）

### Requirement: R4 Executor Refactor for Verifiability
**Module:** server
降低执行器单类复杂度，提高可读性与单元可测试性。

<a id="r4-s1-invariants"></a>
#### Scenario: R4-S1 Invariants（pending/bytes/slots 与 autoRead 状态机可验证）
条件：高压提交/拒绝/关闭/异常路径交织
- 预期：预算与 counters 不出现负数/泄漏；autoRead disable/enable 具备明确的滞回与全局恢复策略；关键不变量有单元测试覆盖

### Requirement: R5 maxmemory Semantics Hardening
**Module:** core / command / wiki
将估算口径与 off-heap 实占口径明确化，避免误解与误用。

<a id="r5-s1-memory-stats-contract"></a>
#### Scenario: R5-S1 MEMORY STATS contract（字段含义与口径说明一致）
条件：执行 `MEMORY STATS` / `MEMORY USAGE` / 触发 maxmemory/淘汰
- 预期：输出字段与 wiki 说明一致；明确“估算 != JVM heap 实测”，并能解释淘汰/拒写触发原因

### Requirement: R6 Client/CLI Deep Harden
**Module:** client
提升 CLI 一致性与 client 资源边界，降低未来扩展风险。

<a id="r6-s1-cli-parsing"></a>
#### Scenario: R6-S1 CLI parsing unified（picocli 同栈 + 兼容现有参数）
条件：使用 `--host/--port/--timeoutMillis/--hex`、单次执行与 REPL 模式
- 预期：帮助信息一致且可扩展；参数错误的退出码与输出可预测；尽量保持现有 CLI 用法兼容

<a id="r6-s2-client-queue-bound"></a>
#### Scenario: R6-S2 Client queue bounded（异常/乱序/未来 push 的防御性策略）
条件：server 关闭/连接异常/超时；或未来出现“非请求-响应”的额外帧
- 预期：client 不出现无界积压；异常路径及时回收 frame 并唤醒等待；必要时关闭连接以保持 FIFO 配对正确性

## Risk Assessment

- **Risk: 行为细微漂移（RESP 输出/默认值/统计字段/CLI 帮助信息）**
  - Mitigation：以测试用例锁定关键行为；默认值 SSOT 单测；对外行为变更写入 CHANGELOG 与 wiki。
- **Risk: 模块拆分/重命名引入编译级联与引用破坏**
  - Mitigation：分阶段迁移（先引入新类并双写/适配，再删除旧实现）；每阶段保持 `mvn test` 绿灯。
- **Risk: ByteBuf/off-heap 生命周期回归导致泄漏**
  - Mitigation：补齐异常路径与资源所有权测试；对关键 release/close 路径增加针对性单测；压测脚本验证长期运行稳定性。
