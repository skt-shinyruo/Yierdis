# Project Docs Code Logic Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a coverage-driven documentation system that inventories missing Yierdis code logic and fills the highest-risk gaps in `docs/project-docs`, with explicit navigation and verification entry points.

**Architecture:** Start by creating `docs/project-docs/code-logic-coverage.md` as the control-plane inventory. Then document the request path, transaction/dispatch behavior, DB lifecycle, TTL/maxmemory/change-event behavior, and native-memory boundaries in execution-order batches. Only create new topic docs when the logic has its own stable lifecycle or invariant set. Every batch updates the coverage matrix and the navigation/index layer together.

**Tech Stack:** Markdown, `rg`, `sed`, `find`, `git diff --check`, lightweight shell link/path audits.

---

## Execution Rules

- Preserve any unrelated user changes in the worktree.
- Do not use `git add -A`.
- Keep Chinese prose in `docs/project-docs`, with English identifiers such as `ExecutionRequest`, `ReplyWriter`, `TransactionState`, `EntryRecord`, and `NativeHandle` unchanged.
- Prefer extending existing topic manuals first. Create a new doc only when the plan explicitly calls for it.
- Treat the current code and tests as the factual source of truth. When existing docs disagree, write the implementation reality and mark ambiguity explicitly.

## Target File Responsibilities

- Create: `docs/project-docs/code-logic-coverage.md`  
  Maintainer-facing coverage inventory of core classes, key methods or logic blocks, invariants, tests, doc destinations, and status.

- Create: `docs/project-docs/command-parsing-and-dispatch.md`  
  Focused manual for command registry lookup, parse contracts, dispatch flow, error handling, and tests.

- Create: `docs/project-docs/transaction-and-replay.md`  
  Focused manual for `MULTI` queueing, `EXEC` replay, abort paths, and session state.

- Create: `docs/project-docs/ttl-and-expiration-lifecycle.md`  
  Focused manual for TTL command handling, lazy expiration, cleanup passes, and index consistency.

- Create: `docs/project-docs/maxmemory-and-eviction.md`  
  Focused manual for ledger reservation, eviction coordination, policies, and OOM behavior.

- Modify: `docs/project-docs/readme.md`  
  Add the coverage matrix and new topic manuals to the documentation map.

- Modify: `docs/project-docs/core-logic-index.md`  
  Add source-entry links for newly documented logic and point readers to the new manuals.

- Modify: `docs/project-docs/development-navigation.md`  
  Update “what file should I open first?” routes for transaction, TTL, maxmemory, dispatch, and change-event work.

- Modify: `docs/project-docs/request-execution-flow.md`  
  Deepen submit, reject, replay, error, and close-path coverage.

- Modify: `docs/project-docs/commands-and-data-model.md`  
  Keep topic-level command semantics here and link deeper parse/dispatch and transaction behavior to the new manuals.

- Modify: `docs/project-docs/executor-and-backpressure.md`  
  Make queueing, `autoRead`, drain, and close/recovery behavior explicit.

- Modify: `docs/project-docs/db-internals.md`  
  Keep the storage graph and DB-object overview here while linking deeper TTL and maxmemory behavior to the new manuals.

- Modify: `docs/project-docs/change-event-and-proxy-logic.md`  
  Make the change-event production and bridging lifecycle explicit without inventing guarantees the code does not implement.

- Modify: `docs/project-docs/native-memory-runtime.md`
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/offheap-copy-behavior.md`
- Modify: `docs/project-docs/bytes-and-fast-paths.md`  
  Deepen native handle, allocator lifecycle, materialization, and byte-boundary explanations where the coverage scan shows gaps.

- Modify: `docs/project-docs/testing-and-debugging.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`
- Modify: `docs/project-docs/glossary.md`  
  Add verification routes, tooling entry points, and term links needed by the new manuals.

---

### Task 1: Create The Coverage Matrix And Hook It Into Navigation

**Files:**
- Create: `docs/project-docs/code-logic-coverage.md`
- Modify: `docs/project-docs/readme.md`
- Modify: `docs/project-docs/core-logic-index.md`

- [ ] **Step 1: Inventory the current project-doc files and high-level source areas**

Run:

```bash
find docs/project-docs -maxdepth 1 -type f -name '*.md' | sort
rg -n "class |record |enum " yierdis-server yierdis-command yierdis-db yierdis-networking yierdis-common yierdis-cli -g '*.java'
```

Expected: the first command lists the current topic manuals; the second gives the class surface needed to seed the coverage matrix headings and first-row candidates.

- [ ] **Step 2: Create `code-logic-coverage.md` with the schema, subsystem headings, and starter rows**

Create this file structure:

```markdown
# Code Logic Coverage

本文是 Yierdis 代码逻辑文档覆盖矩阵。它不重复完整专题解释，只记录哪些核心逻辑已经有文档、哪些还只有源码和测试。

## 怎么使用

- 先按子系统找到类和关键方法。
- 看 `覆盖状态` 判断当前文档是否足够。
- 看 `文档归属` 决定补哪篇文档。
- 看 `相关测试` 决定改动后先验证哪里。

## 记录字段

| 字段 | 含义 |
| --- | --- |
| 子系统 | 逻辑所属模块或行为边界 |
| 类 | 主要实现入口 |
| 关键方法/逻辑块 | 需要解释的行为单元 |
| 行为职责 | 这段逻辑真正负责什么 |
| 关键分支/状态/不变量 | 维护时最容易破坏的事实 |
| 线程/内存边界 | owner thread、生命周期、materialization 等边界 |
| 相关测试 | 改这里先看哪些测试 |
| 文档归属 | 已有或计划中的文档 |
| 覆盖状态 | `covered` / `partial` / `missing` |
| 备注 | 实现现状、文档缺口或待确认点 |

## server-main / bootstrap / connection

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `YierdisServerBootstrap` | `start(...)`, `startInternal()` | 组装 runtime、engine、executor 和 Netty server | 启动顺序、资源关闭顺序、默认组件选择 | Netty 线程和 executor owner thread 分离 | 启动/集成测试 | `request-execution-flow.md`, `main-path-walkthrough.md` | `partial` | 需要补关闭路径和依赖装配细节 |

## networking-resp / networking-netty

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `RespRequestDecoder` | `decode(...)` | RESP array / inline command 解码 | 协议错误、inline 分支、limit 分支 | I/O 线程只做协议适配 | `RespRequestDecoderTest` | `request-execution-flow.md`, `protocol-reference.md` | `partial` | 需要补协议错误和关闭行为 |
| `RespExecutionAdapter` | request -> `ExecutionRequest` | 隔离 RESP DTO 与执行层请求模型 | 复制语义、事务 replay 复用 | materialize 到 `ByteArrayExecutionRequest` | `RespExecutionAdapterTest` | `request-execution-flow.md` | `partial` | 需要补复制边界 |

## executor

## engine / session / transaction

## command-api / command-core / builtin commands

## runtime / multi-db / maxmemory governor

## db-api / db-memory

## bytes / memory / ffm

## cli / benchmark / smoke

## tests / architecture guards / contract tests
```

- [ ] **Step 3: Add the coverage matrix to the docs map and source index**

Update `docs/project-docs/readme.md` to list the new file under reference or development-oriented navigation, and update `docs/project-docs/core-logic-index.md` so the top “怎么使用这份索引” section points to `code-logic-coverage.md`.

Use snippets like:

```markdown
- 覆盖追踪：[`code-logic-coverage.md`](./code-logic-coverage.md)
```

and:

```markdown
- 维护覆盖矩阵：[`code-logic-coverage.md`](./code-logic-coverage.md)
```

- [ ] **Step 4: Verify the new file and links**

Run:

```bash
test -f docs/project-docs/code-logic-coverage.md
rg -n "code-logic-coverage" docs/project-docs/readme.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git diff --check -- docs/project-docs/code-logic-coverage.md docs/project-docs/readme.md docs/project-docs/core-logic-index.md
```

Expected: `test -f` exits 0, `rg` finds the new references, and `git diff --check` prints nothing.

- [ ] **Step 5: Commit the coverage scaffold**

Run:

```bash
git add docs/project-docs/code-logic-coverage.md docs/project-docs/readme.md docs/project-docs/core-logic-index.md
git diff --cached --name-status
git commit -m "docs: add code logic coverage matrix" -- docs/project-docs/code-logic-coverage.md docs/project-docs/readme.md docs/project-docs/core-logic-index.md
```

Expected: the commit contains only the new matrix file and its navigation hooks.

---

### Task 2: Cover Request Dispatch, Command Parsing, And Transaction Replay

**Files:**
- Create: `docs/project-docs/command-parsing-and-dispatch.md`
- Create: `docs/project-docs/transaction-and-replay.md`
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `docs/project-docs/commands-and-data-model.md`
- Modify: `docs/project-docs/executor-and-backpressure.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/code-logic-coverage.md`

- [ ] **Step 1: Extend the coverage matrix for the request path and transaction surface**

Add rows for at least these logic owners:

```markdown
| `YierdisFastCommandHandler` | submit path | 把 `ExecutionRequest` 提交给 `CommandExecutor` | submit reject、closing、直接回错 | I/O 线程不执行命令 | handler / integration tests | `request-execution-flow.md`, `executor-and-backpressure.md` | `missing` | 需要补拒绝与回包路径 |
| `CommandExecutor` | `start()`, `submit(...)`, drain loop | owner thread 执行、排队、预算、关闭 | queue slot、queued bytes、drain budget、close-after-reply | DB 访问只能发生在 owner thread | executor tests | `executor-and-backpressure.md` | `partial` | 需要补关闭与恢复路径 |
| `DefaultYierdisEngine` | `execute(...)` | session/request/reply 到 command processor 的桥接 | session capability 收窄、reply writer 边界 | engine 不直接触 DB internal | engine contract tests | `request-execution-flow.md` | `partial` | 需要补 capability 边界 |
| `YierdisFastCommandProcessor` | `execute(...)` | 命令查表、解析、事务分支、执行和错误翻译 | empty/unknown、parse error、MULTI queue、observer gate | 只通过 API 访问 DB | command core tests | `command-parsing-and-dispatch.md` | `missing` | 需要完整分发说明 |
| `TransactionState` | queue / replay lifecycle | `MULTI/EXEC/DISCARD` 状态机 | queue 限制、abort、replay 顺序 | 事务保存的是请求快照 | transaction tests | `transaction-and-replay.md` | `missing` | 需要独立状态机文档 |
```

- [ ] **Step 2: Create `command-parsing-and-dispatch.md`**

Use this top-level structure:

```markdown
# 命令解析与分发

本文解释 Yierdis 如何从 `ExecutionRequest` 走到 `CommandSpec`、参数解析、事务分支、命令实现和错误回包。

## 入口和边界

## `CommandRegistry` 和 `CommandSpec`

## `YierdisFastCommandProcessor.execute(...)` 主流程

## 参数解析、arity 和 parse error

## 未知命令、空命令和错误翻译

## 事务排队前的复用规则

## change observer / mutation gate

## 相关测试
```

The body must explicitly mention `CommandRegistry`, `CommandSpec`, `ArgReader`, `CommandParseError`, `CommandSupport`, and the rule that command handlers do not build RESP bytes directly.

- [ ] **Step 3: Create `transaction-and-replay.md`**

Use this top-level structure:

```markdown
# 事务与重放

本文只解释 `MULTI/EXEC/DISCARD` 相关逻辑：session 状态、入队快照、重放、abort 和验证路径。

## 事务状态属于哪里

## 为什么队列里保存的是 `ExecutionRequest` 快照

## `MULTI` 之后的入队流程

## `EXEC` 的 replay 主链

## `DISCARD`、abort 和错误路径

## 队列限制和配置边界

## 相关测试
```

The text must explicitly describe that replay reuses the same command processor and command handlers rather than a separate IR or execution path.

- [ ] **Step 4: Deepen the existing request-path manuals**

Update the existing docs with focused additions:

```markdown
## 提交拒绝和直接回错

`YierdisFastCommandHandler` 只负责提交，不负责执行业务命令。提交失败时，错误在 handler / I/O 边界直接回写，不会进入 command 层。

## replay 仍然走同一条执行链

事务重放不会走另一套“内部命令执行器”。它仍然进入 `DefaultYierdisEngine`、`YierdisFastCommandProcessor` 和原始 command handler。
```

and:

```markdown
## `autoRead`、writability 和 close-after-reply

需要明确写出单连接 pending、queued bytes、channel writability 和 flush/close 之间的关系，不要只停留在“有背压”这一层。
```

Insert the first snippet into `request-execution-flow.md` and the second into `executor-and-backpressure.md`, then add short linking sections in `commands-and-data-model.md` that point readers to the two new manuals instead of duplicating their full detail.

- [ ] **Step 5: Verify links and commit the request-path batch**

Run:

```bash
test -f docs/project-docs/command-parsing-and-dispatch.md
test -f docs/project-docs/transaction-and-replay.md
rg -n "command-parsing-and-dispatch|transaction-and-replay" docs/project-docs
git diff --check -- docs/project-docs/command-parsing-and-dispatch.md docs/project-docs/transaction-and-replay.md docs/project-docs/request-execution-flow.md docs/project-docs/commands-and-data-model.md docs/project-docs/executor-and-backpressure.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git add docs/project-docs/command-parsing-and-dispatch.md docs/project-docs/transaction-and-replay.md docs/project-docs/request-execution-flow.md docs/project-docs/commands-and-data-model.md docs/project-docs/executor-and-backpressure.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git diff --cached --name-status
git commit -m "docs: cover command dispatch and transaction replay" -- docs/project-docs/command-parsing-and-dispatch.md docs/project-docs/transaction-and-replay.md docs/project-docs/request-execution-flow.md docs/project-docs/commands-and-data-model.md docs/project-docs/executor-and-backpressure.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
```

Expected: both new docs exist, references resolve, whitespace is clean, and the commit contains only the request-path batch.

---

### Task 3: Cover DB Lifecycle, TTL, Maxmemory, And Change Events

**Files:**
- Create: `docs/project-docs/ttl-and-expiration-lifecycle.md`
- Create: `docs/project-docs/maxmemory-and-eviction.md`
- Modify: `docs/project-docs/db-internals.md`
- Modify: `docs/project-docs/change-event-and-proxy-logic.md`
- Modify: `docs/project-docs/configuration-and-operations.md`
- Modify: `docs/project-docs/development-navigation.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/code-logic-coverage.md`

- [ ] **Step 1: Add DB, TTL, maxmemory, and change-event rows to the coverage matrix**

Add rows for at least these logic owners:

```markdown
| `YierdisDbKeyLifecycle` | `liveEntryRecord(...)`, `computeWithHandle(...)`, `removeEntry(...)` | 协调 key、entry、TTL metadata 和 payload 生命周期 | 惰性过期、删除顺序、ledger delta callback | owner thread only | DB lifecycle tests | `db-internals.md`, `ttl-and-expiration-lifecycle.md` | `partial` | TTL 和删除顺序需要更细说明 |
| `YierdisDbMutationExecutor` | `execute(plan)` | reserve / apply / commit / rollback | upper bound、actual delta、OOM 映射 | mutation 期间的 reservation 生命周期 | DB mutation tests | `db-internals.md`, `maxmemory-and-eviction.md` | `partial` | 需要补完整顺序 |
| `YierdisDbExpirationSupport` | cleanup path | 批量过期清理 | dirty index、stale entry、budget 限制 | owner thread cleanup | expiration tests | `ttl-and-expiration-lifecycle.md` | `missing` | 需要独立说明 |
| `YierdisDbMaxmemorySupport` | victim selection / eviction | 选择 victim 并通过 lifecycle 删除 | policy 分支、sample 行为、仍然 OOM 的路径 | eviction 仍在 owner thread 完成 | maxmemory tests | `maxmemory-and-eviction.md` | `missing` | 需要独立说明 |
| `CommandChangeEmitter` | mutation gate | 只在真实 mutation outcome 后产出 change event | no-op、observer 缺省、异常路径 | 不直接承诺 AOF/复制语义 | change-event tests | `change-event-and-proxy-logic.md` | `partial` | 需要补事件边界 |
```

- [ ] **Step 2: Create `ttl-and-expiration-lifecycle.md`**

Use this structure:

```markdown
# TTL 与过期生命周期

本文解释 TTL 命令、惰性过期、批量清理和 expire index 一致性。

## TTL 元数据到底存在哪里

## TTL 命令写路径

## `liveEntryRecord(...)` 的惰性删除语义

## `cleanupExpired(...)` 的扫描和 budget

## `EntryRecord.expireAtMillis` 与 expire index 的双写约束

## 相关测试
```

- [ ] **Step 3: Create `maxmemory-and-eviction.md`**

Use this structure:

```markdown
# Maxmemory 与淘汰

本文解释 ledger reservation、per-DB / global scope、eviction policy 和 OOM 路径。

## `usedBytes`、`reservedBytes` 和 effective usage

## `YierdisDbMutationExecutor` 为什么先 reserve 再 apply

## per-DB scope 的判断顺序

## global scope 与 governor 协调

## `allkeys-random` / `allkeys-lru` / `noeviction`

## 仍然无法写入时的错误路径

## 相关测试
```

- [ ] **Step 4: Deepen the existing DB and change-event manuals**

Add focused linking and behavior sections such as:

```markdown
## 更细的 TTL 行为

本页只保留 DB storage graph 和 lifecycle 总览。TTL 写路径、惰性过期和 cleanup 预算见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## 更细的 maxmemory 行为

ledger reservation、policy 选择和 global governor 协调见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。
```

and:

```markdown
## change event 不是完整持久化协议

这里要明确写出当前 change event 是最小可重放事件契约，不是 AOF、复制或跨进程 durability 保证。
```

Apply the first block to `db-internals.md`, the second to `change-event-and-proxy-logic.md`, and update `configuration-and-operations.md` plus `development-navigation.md` so readers can find the new TTL/maxmemory manuals from operational and maintenance perspectives.

- [ ] **Step 5: Verify and commit the DB/runtime batch**

Run:

```bash
test -f docs/project-docs/ttl-and-expiration-lifecycle.md
test -f docs/project-docs/maxmemory-and-eviction.md
rg -n "ttl-and-expiration-lifecycle|maxmemory-and-eviction" docs/project-docs
git diff --check -- docs/project-docs/ttl-and-expiration-lifecycle.md docs/project-docs/maxmemory-and-eviction.md docs/project-docs/db-internals.md docs/project-docs/change-event-and-proxy-logic.md docs/project-docs/configuration-and-operations.md docs/project-docs/development-navigation.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git add docs/project-docs/ttl-and-expiration-lifecycle.md docs/project-docs/maxmemory-and-eviction.md docs/project-docs/db-internals.md docs/project-docs/change-event-and-proxy-logic.md docs/project-docs/configuration-and-operations.md docs/project-docs/development-navigation.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git diff --cached --name-status
git commit -m "docs: cover db lifecycle and maxmemory paths" -- docs/project-docs/ttl-and-expiration-lifecycle.md docs/project-docs/maxmemory-and-eviction.md docs/project-docs/db-internals.md docs/project-docs/change-event-and-proxy-logic.md docs/project-docs/configuration-and-operations.md docs/project-docs/development-navigation.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
```

Expected: the new TTL and maxmemory manuals exist, their links resolve, and the commit contains only the DB/runtime batch.

---

### Task 4: Cover Native-Memory, Handle, And Byte-Boundary Logic

**Files:**
- Modify: `docs/project-docs/native-memory-runtime.md`
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/offheap-copy-behavior.md`
- Modify: `docs/project-docs/bytes-and-fast-paths.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/code-logic-coverage.md`
- Modify: `docs/project-docs/glossary.md`

- [ ] **Step 1: Extend the coverage matrix for native-memory and bytes logic**

Add rows for at least these logic owners:

```markdown
| `NativeHandle` | encode/decode contract | stable handle ABI | domain/kind/generation 校验 | raw long 不能当指针使用 | handle tests | `native-allocator-and-handles.md` | `partial` | 需要补失败语义和 DB wrapper 关系 |
| `YierdisStableNativeAllocator` | allocate / realloc / defrag lifecycle | 稳定分配、视图 pin、quarantine | generation、pin、epoch、active defrag | allocator 负责物理位置变化 | allocator tests | `native-allocator-and-handles.md`, `native-memory-runtime.md` | `partial` | 需要补跨文档一致性 |
| `EntryHandle` / `ValueHandle` | typed wrapper usage | DB 层类型化 stable handle | 不能缓存 physical address | DB hot path 只保存 handle | DB native tests | `native-memory-runtime.md` | `partial` | 需要补 wrapper contract |
| `BytesView` / `BytesSlice` / `BytesSink` | materialization boundaries | heap/off-heap/direct 边界 | 什么时候 copy、谁拥有生命周期 | 长生命周期不能泄漏短 view | bytes tests | `bytes-and-fast-paths.md`, `offheap-copy-behavior.md` | `partial` | 需要补 ownership 规则 |
```

- [ ] **Step 2: Deepen the handle and allocator docs**

Add sections like:

```markdown
## 为什么 DB hot path 只保存 stable handle

这里要把 stable identity、physical location、`resolve(...)` 短生命周期 view 和 active defrag 的关系讲清楚。

## domain / kind / generation 校验失败意味着什么

文档要明确这类校验是 ABI 和生命周期护栏，而不是可选的调试辅助。
```

Insert these ideas into `native-allocator-and-handles.md` and connect them to `EntryHandle` / `ValueHandle` usage in `native-memory-runtime.md`.

- [ ] **Step 3: Deepen byte and materialization boundary docs**

Add sections like:

```markdown
## 哪些路径仍然会 materialize heap copy

不要只写“不是零拷贝”。要具体列出协议适配、事务 replay、显式 introspection、reply 生成等典型 copy 点。

## `BytesView` 与 `BytesSlice` 的 ownership 约束

说明谁拥有底层数据、谁只能短期读取、什么时候必须复制。
```

Apply these additions to `bytes-and-fast-paths.md` and `offheap-copy-behavior.md`, then add any missing glossary terms such as `materialization`, `stable handle`, `generation`, `pin`, or `quarantine`.

- [ ] **Step 4: Update the index and coverage statuses**

Add index entries like:

```markdown
| [`NativeHandle`](../../yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java) | stable handle ABI 和 encode/decode contract | `encode/decode methods` | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) |
```

and move any rows that are now sufficiently documented from `partial` to `covered` in `code-logic-coverage.md`.

- [ ] **Step 5: Verify and commit the native-memory batch**

Run:

```bash
rg -n "materialization|stable handle|generation|quarantine|pin" docs/project-docs/native-memory-runtime.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md docs/project-docs/bytes-and-fast-paths.md docs/project-docs/glossary.md
git diff --check -- docs/project-docs/native-memory-runtime.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md docs/project-docs/bytes-and-fast-paths.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md docs/project-docs/glossary.md
git add docs/project-docs/native-memory-runtime.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md docs/project-docs/bytes-and-fast-paths.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md docs/project-docs/glossary.md
git diff --cached --name-status
git commit -m "docs: deepen native memory and bytes coverage" -- docs/project-docs/native-memory-runtime.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md docs/project-docs/bytes-and-fast-paths.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md docs/project-docs/glossary.md
```

Expected: the search finds the new terminology, whitespace is clean, and the commit contains only the native-memory batch.

---

### Task 5: Close Tooling, Testing, And Navigation Gaps, Then Run Final Consistency Checks

**Files:**
- Modify: `docs/project-docs/testing-and-debugging.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`
- Modify: `docs/project-docs/development-navigation.md`
- Modify: `docs/project-docs/readme.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/code-logic-coverage.md`

- [ ] **Step 1: Add verification routes for the newly documented logic**

Update `testing-and-debugging.md` with explicit subsections like:

```markdown
## 改 transaction / replay 先看什么

- `TransactionQueueLimitTest`
- `TransactionState` 相关 command / integration tests
- 涉及 replay 和 mutation 语义时，再看 request-path 和 DB 侧测试

## 改 TTL / expiration 先看什么

- TTL command tests
- expiration cleanup tests
- `db-internals.md` 和 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)

## 改 maxmemory / eviction 先看什么

- maxmemory / eviction tests
- `MEMORY STATS` 相关验证
- [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)
```

- [ ] **Step 2: Add CLI / bench / smoke entry points where they help validate behavior**

Update `client-and-bench-internals.md` with a short section such as:

```markdown
## 它们更适合验证什么

- CLI: 快速确认协议、回包和单命令行为
- smoke: 快速确认 server 启动、基础命令和 CLI/RESP 主链
- benchmark: 观察高并发请求、pipeline 和 backpressure 行为，但不是 correctness oracle
```

- [ ] **Step 3: Update the docs map and index for all new manuals**

Ensure `docs/project-docs/readme.md`, `development-navigation.md`, and `core-logic-index.md` all reference:

```markdown
- [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)
- [`transaction-and-replay.md`](./transaction-and-replay.md)
- [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)
- [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)
- [`code-logic-coverage.md`](./code-logic-coverage.md)
```

The readme should route readers by task, the development guide should route maintainers by change type, and the core index should route source readers by class or behavior.

- [ ] **Step 4: Re-check the matrix and run final doc verification**

Run:

```bash
rg -n "\| `missing` \|" docs/project-docs/code-logic-coverage.md
rg -n "command-parsing-and-dispatch|transaction-and-replay|ttl-and-expiration-lifecycle|maxmemory-and-eviction|code-logic-coverage" docs/project-docs
git diff --check -- docs/project-docs
```

Expected:

- `rg -n "\| \`missing\` \|" ...` may still return rows, but only for clearly deferred lower-priority areas; no request-path, transaction, TTL, maxmemory, change-event, or native-memory core rows should remain `missing`.
- the second `rg` finds all navigation references.
- `git diff --check -- docs/project-docs` prints nothing.

- [ ] **Step 5: Commit the final navigation and verification batch**

Run:

```bash
git add docs/project-docs/testing-and-debugging.md docs/project-docs/client-and-bench-internals.md docs/project-docs/development-navigation.md docs/project-docs/readme.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
git diff --cached --name-status
git commit -m "docs: finalize code logic coverage navigation" -- docs/project-docs/testing-and-debugging.md docs/project-docs/client-and-bench-internals.md docs/project-docs/development-navigation.md docs/project-docs/readme.md docs/project-docs/core-logic-index.md docs/project-docs/code-logic-coverage.md
```

Expected: the final commit closes the documentation routing and verification layer without mixing unrelated files.

---

## Self-Review Checklist

- The plan creates the coverage matrix before asking the implementer to fill doc gaps.
- Every new document has an explicit path and a clear reason to exist.
- Each batch updates both the topic prose and the matrix or navigation layer together.
- The highest-risk logic areas from the spec are all covered by at least one task.
- Verification commands stay lightweight and doc-focused.
