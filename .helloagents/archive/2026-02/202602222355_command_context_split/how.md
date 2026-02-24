<!-- migrated_from: history/2026-02/202602222355_command_context_split/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: CommandContext 全链路重构（ReplyWriter 纯输出化 + Router 依赖 Session）

## Technical Solution

### Core Technologies
- Java 17（Maven 多模块）
- Netty（`yierdis-server`：I/O + 单线程 executor）
- 协议抽象：`yierdis-protocol-model`（`Command/ReplyWriter/Session/ServerSession/...`）
- 对外协议：Custom Protocol v1（NDJSON reply）

### Implementation Key Points

1. **新增协议无关的 `CommandContext`（SSOT）：**
   - `CommandContext` 同时携带：
     - 输入侧：`Session session()`（可为空，或为 server-side `ServerSession`）
     - 输出端口：`ReplyWriter out()`
   - 提供最小 helper（集中 `instanceof`）：
     - `serverSessionOrNull()`：将 `Session` 转换为 `ServerSession`（无则返回 null）
     - `dbIndexProviderOrNull()`：若需要，收敛路由对 “dbIndex 能力” 的依赖
2. **ReplyWriter 纯输出化：**
   - 从 `ReplyWriter` 移除 `session()`，避免输入侧状态从输出端口读取；
   - `ReplyWriterFactory` 改为仅创建 writer（不再接收 session）。
3. **Router 输入侧改造：**
   - `YierdisDbRouter` 改为 `DbEngine dbFor(Session session)`（或 `DbIndexProvider`），不再接收 `ReplyWriter`；
   - `YierdisInstance` 默认 router 使用 `DbIndexProvider` 读取 `dbIndex`，缺失则落到 DB0。
4. **Command dispatch 全链路签名切换：**
   - `CommandRegistry.CommandHandler`：`(Command, CommandContext)`；
   - `YierdisFastCommandProcessor#execute`：`(Command, CommandContext)`；
   - 所有 `*Commands` handler 统一接收 context，并通过 `ctx.out()` 写 reply。
5. **扩展点同步迁移：**
   - `ServerInfoProvider`：`info/stats/memoryStats` 统一改为基于 `CommandContext`；
   - `SlowCommandGovernor`：预算与上限的输入改为 `CommandContext`（避免未来需要 session 时再回退依赖 writer）。
6. **server/executor 适配与性能策略：**
   - `NettyCommandExecutor` 构造 writer 后，与 `ServerConnectionState` 组装出 `CommandContext`；
   - 为避免 per-command 额外对象分配，可在 executor 线程内复用一个可变 context（reset 模式）。
7. **测试适配：**
   - `FastTestClient` 直接创建：
     - `ServerSession`（或自定义 session）
     - `CapturingReplyWriter`（纯输出）
     - `CommandContext`（组合二者）
   - 覆盖 SELECT/MULTI 等输入侧语义时，不再需要“把 session 塞进 writer”。

## Architecture Design

```mermaid
flowchart TD
  subgraph protocol_model[yierdis-protocol-model]
    CMD[Command]
    CTX[CommandContext<br/>session + out]
    SES[Session]
    SS[ServerSession]
    OUT[ReplyWriter]
    SES --> SS
    CTX --> SES
    CTX --> OUT
  end

  subgraph core[yierdis-core]
    ROUTER[YierdisDbRouter<br/>dbFor(Session)]
    PROC[YierdisFastCommandProcessor<br/>execute(Command, CommandContext)]
    CMDS[*Commands handlers<br/>execute(Command, CommandContext)]
    PROC --> CMDS
    PROC --> ROUTER
  end

  subgraph server[yierdis-server]
    CONN[ServerConnectionState<br/>implements ServerSession]
    EXEC[NettyCommandExecutor]
    FACT[ReplyWriterFactory]
    EXEC --> FACT
    EXEC --> PROC
    EXEC --> CONN
    EXEC --> CTX
  end
```

## Architecture Decision ADR

### ADR-20260222-01: 移除 ReplyWriter 的 session 能力，引入 CommandContext 作为执行上下文 SSOT

**Context:**  
现状 `ReplyWriter.session()` 被用作路由/事务/可观测等输入侧判定，导致：
- 输入侧状态被挂在输出端口上，embedded/单测需要伪造 writer；
- `instanceof/out==null` 分支扩散到多处；
- 扩展点（INFO provider 等）进一步加深依赖反转（server 连接态从 writer 读取）。

**Decision:**  
- 新增 `CommandContext`（`session + out`）作为命令执行时上下文的 SSOT；
- 将 router、事务、change event、INFO/慢命令治理等输入侧逻辑统一迁移为基于 `CommandContext.session()`；
- 从 `ReplyWriter` 移除 `session()`，使其回归“纯输出端口”职责。

**Rationale:**  
- 编译期强制依赖方向：输入侧只能来自 session/context，输出侧只能写 reply；
- `instanceof` 可收敛到 context helper，减少分支散落；
- 让 embedded/test 可以独立构造输入侧 session，不再被迫耦合到 writer 实现。

**Alternatives:**  
- 保留 `ReplyWriter.session()` 但仅约定不使用 → 拒绝原因：无法形成编译期护栏，长期仍易回退到 writer 读取。  
- 仅把 `dbFor(ReplyWriter)` 改为 `dbFor(Session)`（局部修补） → 拒绝原因：事务/INFO/慢命令治理仍会继续滥用 writer 读取输入侧状态。  
- 使用 `(Command, Session, ReplyWriter)` 三参签名 → 拒绝原因：上下文能力分散，扩展点演进更难统一收敛与复用。

**Impact:**  
- 涉及跨模块接口签名一次性迁移（core/server/test 同步修改）；  
- 对外协议不变，但内部 API 变更需要全量回归测试。

## API Design

### `CommandContext`（protocol-model）
- `Session session()`
- `ReplyWriter out()`
- helper：`ServerSession serverSessionOrNull()`（集中 `instanceof`）

### `DbIndexProvider`（protocol-model）
- `int dbIndex()`（路由读取能力）
- `ServerSession` 继承该接口，避免 router 依赖过宽的 server 连接态能力

### `ReplyWriter`（protocol-model）
- 移除 `Session session()`，保留 reply shape + `ReplySink` 语义

### `ReplyWriterFactory`（protocol-model）
- `ReplyWriter newWriter(BytesSink out)`（不再接收 session）

### `YierdisDbRouter`（core）
- `DbEngine dbFor(Session session)`（或 `DbIndexProvider`）

### `YierdisFastCommandProcessor`（core）
- `execute(Command cmd, CommandContext ctx)`

## Security and Performance

- **Security:**
  - 不新增任何外部连接/密钥处理；
  - context 不应跨线程共享（executor 单线程语义保持），避免把连接态误用到非 owner thread。
- **Performance:**
  - 推荐 executor 侧复用可变 context（reset），避免每条命令额外分配；
  - 路由从 `out.session()` 迁移到 `ctx.session()` 不增加额外拷贝与聚合分配；
  - `instanceof` 分支集中后更易做局部优化与审计。

## Testing and Deployment

- **Testing:**
  - 全量：`mvn test`
  - 约束检查：`rg "\\.session\\(\\)"` 应不再出现于 `ReplyWriter` 使用路径（只允许对 `CommandContext.session()` 的调用）
  - 增补关键锚点：SELECT 路由、MULTI/EXEC、INFO provider 连接态读取
- **Deployment:**
  - 内部重构，无需变更启动参数与对外协议；建议以单次 PR 合并，确保接口变更不会留下“双轨”。
