# Change Proposal: CommandContext 全链路重构（拆分输入 Session 与输出 ReplyWriter）

## Requirement Background

当前命令执行链路把 `ReplyWriter` 同时当成“输出端口 + 输入侧执行上下文”的载体，导致边界不清晰：

1. **路由/事务/可观测等输入侧状态挂在输出端口上：**
   - `YierdisDbRouter#dbFor(ReplyWriter out)` 把“选择 DB”依赖绑定到 writer。
   - `YierdisInstance` 的 router 通过 `out.session()` 获取 `dbIndex` 决定路由。
   - `YierdisFastCommandProcessor` 在事务与 change event 中通过 `out.session()` 判断并读取连接态（`instanceof ServerSession`）。
2. **embedded / 单测难以“只提供输入侧状态”而不伪造 writer：**
   - 现状下要覆盖多 DB / SELECT / 事务语义，测试往往必须构造带 session 的 writer（即使只为了路由输入侧）。
3. **促成 `out == null` 与 `instanceof` 分支扩散：**
   - 逻辑上属于“输入侧判定”的分支，散落在 router、processor、command handlers 与 server info provider 中，维护成本与漂移风险增大。

目标是把“输入上下文（Session/连接态）”与“输出端口（ReplyWriter）”在 API 层面显式拆分，使路由/事务/鉴权等输入侧决策不再依赖 writer，并收敛 `instanceof` 到适配层。

## Change Content

1. **引入显式 `CommandContext`：**
   - 将命令执行所需的 *输入侧*（`Session`）与 *输出端口*（`ReplyWriter`）统一封装到上下文对象中；
   - 在命令层与执行器层统一以 `CommandContext` 作为“执行时上下文”的 SSOT 入口。
2. **`ReplyWriter` 纯输出化：**
   - 移除 `ReplyWriter.session()` 等连接态读取能力；
   - writer 的职责仅限于“按协议无关的 reply shape 语义写出结果”。
3. **路由依赖从 `ReplyWriter` 迁移到 `Session`/更窄接口：**
   - `YierdisDbRouter` 改为依赖 `Session`（或 `DbIndexProvider`）来选择 DB；
   - 多 DB/SELECT 的输入侧状态仅通过 session 进入路由。
4. **全链路签名一次性切换：**
   - `YierdisFastCommandProcessor`、`CommandRegistry` 与所有 `*Commands` handler 统一改为接收 `CommandContext`；
   - 事务、INFO/STATS provider、慢命令治理（KEYS）等扩展点同步改为基于 context。
5. **`instanceof` 收敛到适配层：**
   - 通过 `CommandContext` 的 helper（例如 `serverSessionOrNull()`）集中完成 “`Session` → `ServerSession`” 的判定；
   - core/server/test 不再散落直接 `instanceof` 分支。

## Impact Scope
- **Modules:**
  - `yierdis-protocol-model`（新增 `CommandContext`，调整 `ReplyWriter/ReplyWriterFactory` 与 provider 接口）
  - `yierdis-core`（router/processor/command handlers 全链路签名切换）
  - `yierdis-server`（executor/handler/info provider/boot 适配 context 与新 factory）
  - `yierdis-core` tests（`FastTestClient` 等测试辅助适配）
- **Files:** 涉及多处接口签名与调用点联动（以 task.md 为准）
- **APIs:** 内部 API 破坏性变更（跨模块联动），对外协议语义保持不变（Custom Protocol v1）
- **Data:** 无（纯执行链路与接口重构）

## Core Scenarios

### Requirement: 路由输入解耦（DB 路由不依赖 ReplyWriter）
**Module:** core(command/runtime) / protocol-model

#### Scenario: SELECT 切换 DB 后路由仍正确
条件：连接态具备 `dbIndex`（server 连接）；执行 `SELECT 1` 后执行 `SET/GET`。
- 预期：路由只读取 session 的 dbIndex，命中 DB1；reply 语义保持一致。

#### Scenario: embedded/单测缺少连接态时默认 DB0
条件：session 为空或不包含 dbIndex 能力；执行普通读写命令。
- 预期：路由默认 DB0；不需要伪造 writer 才能执行语义。

### Requirement: ReplyWriter 纯输出端口（不再暴露 Session）
**Module:** protocol-model / server / core

#### Scenario: 命令层不再出现 out.session() 读取
条件：完成迁移后扫描源码。
- 预期：core/server/test 不再出现 `out.session()`；输入侧状态仅来自 `CommandContext.session()`。

#### Scenario: writer 实现不再持有连接态
条件：协议实现（Custom Protocol v1 writer）仅负责编码输出。
- 预期：writer 不再保存/暴露 session；避免输入侧状态从输出端口“泄漏”。

### Requirement: 事务/可观测/慢命令治理统一基于上下文
**Module:** core(command) / server

#### Scenario: MULTI/EXEC 仅在 server 连接态可用
条件：`Session` 为 `ServerSession` 时允许事务；否则返回稳定错误。
- 预期：事务逻辑通过 `CommandContext` 读取连接态；错误 message 与现状一致。

#### Scenario: INFO/STATS provider 可基于 session 访问连接态
条件：server 注入的 provider 需要读取 `ServerConnectionState`。
- 预期：provider 通过 `CommandContext.session()` 获取连接态，不依赖 writer。

## Risk Assessment
- **Risk:** 破坏性接口变更导致联动面大、回归范围广  
  - **Mitigation:** 以编译为护栏（一次性切换签名，消除双轨）；分模块逐步改完后再跑全量 `mvn test`；用 `rg` 约束 “禁止 `out.session()`”。
- **Risk:** 性能回归（新增上下文对象/间接调用）  
  - **Mitigation:** context 设计允许复用（executor 侧可复用一个可变 context），避免 per-command 额外分配；热路径保持 writer 直写不引入中间聚合。
- **Risk:** 语义漂移（事务边界、SELECT 错误风格、busy/error 回包一致性）  
  - **Mitigation:** 保持命令层为 SSOT；补齐关键场景测试锚点（SELECT、MULTI/EXEC、INFO/STATS）。
