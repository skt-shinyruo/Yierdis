# Project Docs Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `docs/project-docs` into a comprehensive layered documentation system for both first-time readers and developers using the docs as a source index.

**Architecture:** Keep the existing high-value entry filenames so existing links remain mostly stable, while reshaping every project-doc Markdown file around one clear responsibility. Split the oversized FFM material by adding two focused native-memory docs, keep `operation-test-coverage-matrix.md` at its current path because tests parse it, and update root `README.md` so it points to the project-doc map instead of duplicating the internal manual.

**Tech Stack:** Markdown, Java 25 Maven verification, `rg`, `find`, `git diff --check`, lightweight Python link audit run from the shell.

---

## Execution Rules

- Use the current working tree as the baseline. There are existing user edits in `README.md`, several `docs/project-docs` files, and integration-test files; do not revert them.
- Do not use `git add -A`.
- Before each commit, run `git diff --cached --name-status` and then commit only the task paths with a path-limited `git commit -m "..." -- <paths>`.
- Do not edit Java, Maven, shell scripts, generated SVGs, or binary assets for this rewrite.
- Use Chinese prose for `docs/project-docs`, with English identifiers such as `ExecutionRequest`, `ReplyWriter`, `RespExecutionAdapter`, and `NativeHandle` left unchanged.
- For Maven commands, use JDK 25 explicitly:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## Target File Responsibilities

Keep and rewrite these existing project-doc files:

- `docs/project-docs/readme.md`: authoritative documentation map and reading-path selector.
- `docs/project-docs/project-introduction.md`: purpose, project positioning, learning mindset, and non-drop-in Redis boundary.
- `docs/project-docs/project-overview.md`: current capability boundary, module map, runtime features, and first source files to open.
- `docs/project-docs/request-execution-flow.md`: runtime request flow from RESP bytes to write-back.
- `docs/project-docs/main-path-walkthrough.md`: source-order walkthrough for startup, `PING`, and `SET`.
- `docs/project-docs/module-architecture.md`: Maven modules, ownership, dependency direction, and architecture guards.
- `docs/project-docs/protocol-reference.md`: RESP request/reply behavior, protocol limits, inline command support, `HELLO 3`, and protocol errors.
- `docs/project-docs/commands-and-data-model.md`: command registry, parsing, data families, logical types, encodings, and command semantics boundaries.
- `docs/project-docs/db-internals.md`: `YierdisDb`, key lifecycle, DB storage graph, TTL, maxmemory, memory ledger, and DB owner-thread rules.
- `docs/project-docs/executor-and-backpressure.md`: executor queues, submission, drain loop, scheduling, budgets, `autoRead`, output writability, and metrics.
- `docs/project-docs/bytes-and-fast-paths.md`: `BytesSource`, `BytesView`, `BytesSlice`, `BytesSink`, `DirectBytesSink`, Netty adapters, and materialization boundaries.
- `docs/project-docs/configuration-and-operations.md`: server args, runtime config flow, operations, observability, scenarios, and shutdown.
- `docs/project-docs/client-and-bench-internals.md`: CLI, Netty client, benchmark harness, smoke script, bench script, and strict reply validation.
- `docs/project-docs/development-navigation.md`: task-based "what file do I open first" guide.
- `docs/project-docs/testing-and-debugging.md`: change-type test guide and symptom-based debugging guide.
- `docs/project-docs/operation-test-coverage-matrix.md`: test-parsed command, DB API, native/internal coverage matrix.
- `docs/project-docs/core-logic-index.md`: compact index of classes, modules, responsibilities, and links into deeper docs.
- `docs/project-docs/glossary.md`: high-frequency term definitions and links back to manuals.
- `docs/project-docs/ffm-usage.md`: short Yierdis native-memory landing page that links to the split native-memory docs.
- `docs/project-docs/native-allocator-and-handles.md`: stable allocator, `NativeHandle`, object table, `realloc`, pin/epoch/quarantine, active defrag, and DB handle migration.
- `docs/project-docs/offheap-copy-behavior.md`: heap/off-heap/direct copy behavior and remaining materialization points.

Create these new project-doc files:

- `docs/project-docs/ffm-primer.md`: concise JDK FFM primer for readers who need `Arena`, `MemorySegment`, `ValueLayout`, slicing, lifetime, and native-call safety before reading Yierdis internals.
- `docs/project-docs/native-memory-runtime.md`: Yierdis-specific FFM runtime ownership, region/span/access model, DB scope modes, maxmemory accounting, and what is still heap-backed.

Modify this external entry point:

- `README.md`: keep quick start and command list, replace the long internal reading list with a short pointer to `docs/project-docs/readme.md`.

Do not edit these assets in this plan:

- `docs/project-docs/assets/db-internals-object-graph.svg`
- `docs/project-docs/assets/executor-backpressure.svg`
- `docs/project-docs/assets/module-architecture.svg`
- `docs/project-docs/assets/protocol-boundary.svg`
- `docs/project-docs/assets/request-execution-flow.svg`

---

### Task 1: Preflight Inventory And Constraints

**Files:**
- Read: `docs/superpowers/specs/2026-05-17-project-docs-rewrite-design.md`
- Read: `README.md`
- Read: `docs/project-docs/*.md`
- Read: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java`
- Read: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java`

- [ ] **Step 1: Record worktree state**

Run:

```bash
git status --short
git diff --cached --name-status
```

Expected: output may include existing user edits. Preserve them. If `docs/superpowers/plans/2026-05-17-project-docs-rewrite.md` is already present, treat it as this plan.

- [ ] **Step 2: Inventory project docs**

Run:

```bash
find docs/project-docs -maxdepth 1 -type f -name '*.md' | sort
find docs/project-docs/assets -maxdepth 1 -type f | sort
```

Expected: the Markdown list contains the existing project-doc files named in this plan, and the asset list contains SVGs that are not edited by this plan.

- [ ] **Step 3: Inventory Markdown references into project docs**

Run:

```bash
rg -n "docs/project-docs|project-docs/|project-introduction|project-overview|request-execution-flow|main-path-walkthrough|core-logic-index|ffm-usage|native-allocator-and-handles|offheap-copy-behavior" README.md docs -g '*.md'
```

Expected: output identifies links in `README.md`, `docs/project-docs`, and historical `docs/superpowers` files. Update current docs and `README.md` during this plan; leave historical specs and plans unchanged.

- [ ] **Step 4: Read matrix parser constraints**

Run:

```bash
sed -n '1,260p' yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java
sed -n '1,120p' yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java
```

Expected: confirm these requirements for `operation-test-coverage-matrix.md`:

- command sections are `### UPPERCASECOMMAND`
- status lines use `- **Command layer**: \`status\` - detail`
- status lines use `- **DB API**: \`status\` - detail`
- status lines use `- **Native internals**: \`status\` - detail`
- command variant lines use `- **Command variant**: \`variant\` - \`status\` - detail`
- DB and native inventory tables keep their required sections and rows

- [ ] **Step 5: No commit for this task**

This task only gathers context. Do not modify files or commit.

---

### Task 2: Rewrite Navigation Layer And Root README Linkage

**Files:**
- Modify: `README.md`
- Modify: `docs/project-docs/readme.md`

- [ ] **Step 1: Rewrite `docs/project-docs/readme.md` around reading paths**

Use this exact top-level structure:

```markdown
# Codebase Guide

本文是 Yierdis 的代码库文档地图。它不替代仓库根部的 `README.md`，而是帮助你根据目标选择阅读路径：先理解项目、跟一次请求、查某个专题、准备改代码，或定位核心类。

## 先选你的阅读路径

| 目标 | 建议路径 |
| --- | --- |
| 第一次了解项目 | `project-introduction.md` -> `project-overview.md` -> `request-execution-flow.md` |
| 跟一条请求读源码 | `request-execution-flow.md` -> `main-path-walkthrough.md` -> `core-logic-index.md` |
| 理解协议和命令 | `protocol-reference.md` -> `commands-and-data-model.md` -> `operation-test-coverage-matrix.md` |
| 理解 DB 和内存 | `db-internals.md` -> `native-memory-runtime.md` -> `native-allocator-and-handles.md` |
| 准备改代码 | `development-navigation.md` -> `testing-and-debugging.md` -> 对应专题文档 |

## 文档分层

## 推荐第一轮阅读

## 维护者提示
```

`## 文档分层` must include these five groups with links and one-sentence responsibilities:

- 入口导读: `readme.md`, `project-introduction.md`, `project-overview.md`
- 系统主线: `request-execution-flow.md`, `main-path-walkthrough.md`, `module-architecture.md`
- 专题手册: `protocol-reference.md`, `commands-and-data-model.md`, `db-internals.md`, `executor-and-backpressure.md`, `bytes-and-fast-paths.md`, `configuration-and-operations.md`, `client-and-bench-internals.md`, `ffm-usage.md`, `ffm-primer.md`, `native-memory-runtime.md`, `native-allocator-and-handles.md`, `offheap-copy-behavior.md`
- 开发导航: `development-navigation.md`, `testing-and-debugging.md`, `operation-test-coverage-matrix.md`
- 参考资料: `core-logic-index.md`, `glossary.md`

`## 推荐第一轮阅读` must list `project-introduction.md`, `project-overview.md`, `request-execution-flow.md`, `main-path-walkthrough.md`, `protocol-reference.md`, `commands-and-data-model.md`, `db-internals.md`, `executor-and-backpressure.md`, `testing-and-debugging.md`, and `development-navigation.md`.

`## 维护者提示` must say that `operation-test-coverage-matrix.md` is parsed by tests, native-memory truth should stay consistent across `native-memory-runtime.md`, `native-allocator-and-handles.md`, and `db-internals.md`, and root `README.md` should stay a quick-start page rather than an internal manual.

Keep every link relative to `docs/project-docs`, for example `./project-overview.md`.

- [ ] **Step 2: Shorten root `README.md` internal-doc section**

Replace the current long internal document list with a compact entry section:

```markdown
## 初学者导读

如果你是第一次进入这个仓库，先读 `docs/project-docs/readme.md`。那是完整的代码库文档地图，会按不同目标给出阅读路径：

- 先理解项目定位和能力边界
- 跟一条请求从 RESP 走到 DB 再写回
- 查协议、命令、DB、执行器、native memory 等专题
- 准备改代码并选择测试范围

推荐第一轮阅读：

1. `docs/project-docs/readme.md`
2. `docs/project-docs/project-introduction.md`
3. `docs/project-docs/project-overview.md`
4. `docs/project-docs/request-execution-flow.md`
5. `docs/project-docs/main-path-walkthrough.md`
```

Keep the rest of `README.md` focused on project positioning, environment, build, run, client usage, command list, native-memory headline, benchmark, and smoke commands.

- [ ] **Step 3: Verify navigation links exist**

Run:

```bash
rg -n "\]\(\./|docs/project-docs/" README.md docs/project-docs/readme.md
test -f docs/project-docs/project-introduction.md
test -f docs/project-docs/project-overview.md
test -f docs/project-docs/request-execution-flow.md
test -f docs/project-docs/main-path-walkthrough.md
```

Expected: `rg` shows only links to files that exist or will be created by later tasks. The `test -f` commands exit 0.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check -- README.md docs/project-docs/readme.md
```

Expected: no output and exit 0.

- [ ] **Step 5: Commit navigation layer**

Run:

```bash
git diff --cached --name-status
git add README.md docs/project-docs/readme.md
git commit -m "docs: rewrite project docs navigation" -- README.md docs/project-docs/readme.md
```

Expected: commit succeeds and includes only `README.md` and `docs/project-docs/readme.md`.

---

### Task 3: Rewrite Entry Guide Docs

**Files:**
- Modify: `docs/project-docs/project-introduction.md`
- Modify: `docs/project-docs/project-overview.md`

- [ ] **Step 1: Rewrite `project-introduction.md`**

Use this top-level structure:

```markdown
# Project Introduction

本文面向第一次阅读 Yierdis 的读者，回答三个问题：这个项目为什么存在、应该用什么预期理解它、为什么它比普通内存 KV 练习更值得读。

## 一句话定位

## 为什么做这个项目

## 它当前已经覆盖什么

## 它当前还没有什么

## 为什么不是普通 Map 服务

## 读代码前先建立的心智模型

## 接下来读什么
```

Required content:

- State that Yierdis is a Java 25 + Netty + JDK FFM Redis-style single-node in-memory KV server.
- State that RESP2 is the default wire target and `HELLO 3` negotiates basic RESP3 replies.
- State that it is not a Redis drop-in replacement.
- Current implemented areas: Redis-style data families, TTL, maxmemory, approximate eviction, minimal transactions, backpressure, observability, native-memory-backed paths.
- Current missing areas: AOF/RDB, replication/cluster, Lua, ACL/TLS, PubSub, full Redis ecosystem compatibility.
- End with links to `project-overview.md` and `request-execution-flow.md`.

- [ ] **Step 2: Rewrite `project-overview.md`**

Use this top-level structure:

```markdown
# Project Overview

本文从代码和运行时边界出发，说明 Yierdis 当前是什么、有哪些模块、一次请求会经过哪些层，以及读源码时先打开哪些文件。

## 当前定位

## 能力边界

## 技术栈和运行时特征

## 模块总览

## 请求主链概览

## 数据和内存主线

## 最先打开的源码文件

## 接下来读什么
```

Required content:

- Include a compact module map covering common bytes, memory API/FFM, networking RESP/Netty, server API/core/executor/main, command API/core/builtin, DB API/memory, CLI, benchmark, tests.
- Include the request path terms required by the spec: `RespCommandRequest`, `RespExecutionAdapter`, `ByteArrayExecutionRequest`, `ExecutionRequest`, `CommandExecutor`, engine, command processor, DB, `ReplyWriter`, `RespReplyWriter`, Netty write-back.
- Link to `module-architecture.md`, `request-execution-flow.md`, `db-internals.md`, and `native-memory-runtime.md`.

- [ ] **Step 3: Check entry docs for duplicate long reading lists**

Run:

```bash
rg -n "建议的第一轮阅读顺序|如果你是第一次进入仓库，推荐顺序|这组文档分别覆盖" docs/project-docs/project-introduction.md docs/project-docs/project-overview.md
```

Expected: no matches. The full reading map belongs in `docs/project-docs/readme.md`.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/project-introduction.md docs/project-docs/project-overview.md
```

Expected: no output and exit 0.

- [ ] **Step 5: Commit entry docs**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/project-introduction.md docs/project-docs/project-overview.md
git commit -m "docs: rewrite project entry guide" -- docs/project-docs/project-introduction.md docs/project-docs/project-overview.md
```

Expected: commit succeeds and includes only the two entry docs.

---

### Task 4: Rewrite System Main Path Docs

**Files:**
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `docs/project-docs/main-path-walkthrough.md`
- Modify: `docs/project-docs/module-architecture.md`

- [ ] **Step 1: Rewrite `request-execution-flow.md`**

Use this top-level structure:

```markdown
# Request Execution Flow

本文解释一条客户端请求在 Yierdis 里的运行路径：从 Netty 收到 RESP bytes，到命令进入 owner thread，再到 DB 读写和 RESP 回包写出。

## 一张主链图

## 启动和连接状态

## Netty pipeline

## RESP 到 ExecutionRequest

## 提交到 CommandExecutor

## owner thread 执行

## Engine 和命令分发

## PING 最短路径

## SET 写路径

## 事务和 replay

## 错误、关闭和背压

## 推荐源码和测试
```

Required flow:

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter
  -> RespExecutionAdapter
  -> ByteArrayExecutionRequest
  -> ExecutionRequest
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandSpec<ExecutionRequest>
  -> command implementation
  -> DbEngine / DbReads / DbWrites
  -> yierdis-db-memory
  -> ReplyWriter
  -> RespReplyWriter
  -> NettyExecutionIoAdapter
  -> transport flush
```

- [ ] **Step 2: Rewrite `main-path-walkthrough.md`**

Use this top-level structure:

```markdown
# Main Path Walkthrough

本文按源码阅读顺序串起 Yierdis 的主路径，适合一边打开文件一边跟读。

## 阅读前提

## 路线图

## 1. 进程入口

## 2. 组装中心

## 3. 实例和多 DB

## 4. 连接和 pipeline

## 5. RESP 适配

## 6. Executor 提交和 drain

## 7. Engine 到命令层

## 8. PING 路径

## 9. SET 路径

## 10. 回包写出

## 容易读错的边界

## 继续阅读
```

Required source anchors include:

- `YierdisServer`
- `YierdisServerBootstrap`
- `YierdisServerChannelInitializer`
- `RespRequestDecoder`
- `RespCommandAdapter`
- `RespExecutionAdapter`
- `YierdisFastCommandHandler`
- `CommandExecutor`
- `DefaultYierdisEngine`
- `YierdisFastCommandProcessor`
- `StringCommands`
- `YierdisStringOps`
- `YierdisDbMutationExecutor`
- `ReplyWriter`
- `RespReplyWriter`
- `NettyExecutionIoAdapter`

- [ ] **Step 3: Rewrite `module-architecture.md`**

Use this top-level structure:

```markdown
# Module Architecture

本文说明 Yierdis 的 Maven 模块和依赖方向。重点不是列目录，而是说明哪些模块拥有协议、命令、执行、DB、memory 和组装职责。

## 一眼看懂的依赖方向

## 聚合模块

## bytes 基础层

## memory 车道

## protocol 车道

## execution 和 server 车道

## command 车道

## DB 车道

## CLI 和 benchmark

## architecture tests

## 改模块边界前先看什么
```

Required boundary statements:

- protocol owns wire shape and reply encoding, not DB semantics.
- command owns parsing and command semantics, not Netty.
- DB owns storage behavior, not RESP.
- server-main owns final assembly.
- architecture tests protect dependency direction.

- [ ] **Step 4: Check main-path consistency**

Run:

```bash
rg -n "RespExecutionAdapter|ByteArrayExecutionRequest|RespReplyWriter|NettyExecutionIoAdapter|CommandExecutor|YierdisFastCommandProcessor" docs/project-docs/request-execution-flow.md docs/project-docs/main-path-walkthrough.md docs/project-docs/module-architecture.md
```

Expected: all required path terms appear in the relevant docs.

- [ ] **Step 5: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/request-execution-flow.md docs/project-docs/main-path-walkthrough.md docs/project-docs/module-architecture.md
```

Expected: no output and exit 0.

- [ ] **Step 6: Commit main-path docs**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/request-execution-flow.md docs/project-docs/main-path-walkthrough.md docs/project-docs/module-architecture.md
git commit -m "docs: rewrite system main path" -- docs/project-docs/request-execution-flow.md docs/project-docs/main-path-walkthrough.md docs/project-docs/module-architecture.md
```

Expected: commit succeeds and includes only the three main-path docs.

---

### Task 5: Rewrite Protocol And Command Manuals

**Files:**
- Modify: `docs/project-docs/protocol-reference.md`
- Modify: `docs/project-docs/commands-and-data-model.md`

- [ ] **Step 1: Rewrite `protocol-reference.md`**

Use this top-level structure:

```markdown
# Protocol Reference

本文解释 Yierdis 当前公开 TCP 协议的实现边界：RESP 请求如何进入系统、回包如何编码、协议错误如何处理，以及哪些 Redis 协议能力还只是基础兼容。

## 协议定位

## RESP2 请求

## inline command

## HELLO 2 / HELLO 3

## ReplyWriter 到 RESP 回包

## 协议错误和断连

## 协议上限

## 和 Redis 兼容性的边界

## 推荐源码和测试
```

Required content:

- RESP2 is the default.
- `HELLO 3` switches the connection to basic RESP3 reply encoding.
- malformed RESP returns an error and closes the connection.
- `ReplyWriter` is the semantic authority for replies; RESP writers encode those shapes to bytes.
- Full Redis client ecosystem compatibility is not claimed.

- [ ] **Step 2: Rewrite `commands-and-data-model.md`**

Use this top-level structure:

```markdown
# Commands And Data Model

本文说明命令层如何注册、解析和分发命令，以及命令语义如何映射到 DB 能力、逻辑类型和内部编码。

## 命令层职责

## CommandRegistry 和 CommandSpec

## 参数解析和错误

## CommandContext 和 ReplyWriter

## 命令家族总览

## 逻辑类型和内部编码

## HLL、bitmap 和 string 的关系

## 事务中的命令语义

## 新增命令时的路线

## 推荐源码和测试
```

Required content:

- Explain `CommandRegistry`, `CommandSpec<ExecutionRequest>`, `ArgReader`, `CommandArity`, `CommandParsers`, `CommandParseError`, `CommandContext`, and `CommandSupport`.
- Cover command families: connection/server, key/TTL, string/bitmap, HLL, list, hash, set, zset, transaction.
- State that command semantics are a Redis-style minimum subset where implemented.
- Link to `operation-test-coverage-matrix.md` for coverage status.

- [ ] **Step 3: Check protocol and command wording**

Run:

```bash
rg -n "drop-in replacement|HELLO 3|ReplyWriter|CommandSpec|CommandRegistry|operation-test-coverage-matrix" docs/project-docs/protocol-reference.md docs/project-docs/commands-and-data-model.md
```

Expected: output includes the protocol compatibility boundary, `HELLO 3`, reply authority, command registry/spec terms, and the coverage matrix link.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/protocol-reference.md docs/project-docs/commands-and-data-model.md
```

Expected: no output and exit 0.

- [ ] **Step 5: Commit protocol and command manuals**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/protocol-reference.md docs/project-docs/commands-and-data-model.md
git commit -m "docs: rewrite protocol and command manuals" -- docs/project-docs/protocol-reference.md docs/project-docs/commands-and-data-model.md
```

Expected: commit succeeds and includes only the two manuals.

---

### Task 6: Rewrite DB, Executor, And Bytes Manuals

**Files:**
- Modify: `docs/project-docs/db-internals.md`
- Modify: `docs/project-docs/executor-and-backpressure.md`
- Modify: `docs/project-docs/bytes-and-fast-paths.md`

- [ ] **Step 1: Rewrite `db-internals.md`**

Use this top-level structure:

```markdown
# DB Internals

本文解释单个 `YierdisDb` 内部如何组织 key、entry、value、TTL、maxmemory、memory ledger 和 introspection。

## 先记住一句话

## 从 YierdisInstance 到 YierdisDb

## DB storage graph

## Key lifecycle

## 读路径

## 写路径

## TTL 和过期清理

## maxmemory 和 memory ledger

## memory / object introspection

## owner thread 约束

## 改 DB 前先看什么

## 推荐测试
```

Required content:

- Describe `NativeKeyDirectory`, `EntryTable`, `EntryRecord`, `EntryHandle`, `ValueHandle`, type roots, expire index, key lifecycle, mutation executor, ledger, maxmemory support, and introspection.
- State current allocator-backed facts consistently with `native-allocator-and-handles.md`.

- [ ] **Step 2: Rewrite `executor-and-backpressure.md`**

Use this top-level structure:

```markdown
# Executor And Backpressure

本文解释命令为什么不直接在 I/O 线程里执行，以及 executor 如何用队列、预算、调度和 Netty 读写控制保护系统。

## 先记住一句话

## 主要对象

## 提交路径

## backlog budget

## GLOBAL 和 FAIR 调度

## drain loop

## 执行支持和回包写出

## 背压来源

## global recovery

## maintenance task

## 统计和观测

## 推荐源码和测试
```

Required content:

- Cover `CommandExecutor`, `CommandExecutorSubmitter`, `ExecutorBacklogBudget`, `ExecutorTaskQueue`, `CommandExecutorDrainLoop`, `CommandExecutorExecutionSupport`, `ExecutorBackpressureController`, `ExecutionConnectionContext`, and `NettyExecutionConnection`.
- Cover queue capacity, queued bytes, per-connection pending count, per-connection pending bytes, global backlog high/low watermarks, Netty output writability, and `autoRead`.

- [ ] **Step 3: Rewrite `bytes-and-fast-paths.md`**

Use this top-level structure:

```markdown
# Bytes And Fast Paths

本文解释 Yierdis 为什么有一套独立于 Netty 和 DB 的 bytes 抽象，以及这些抽象如何减少无意义复制。

## 先记住一句话

## 为什么不是直接传 byte[]

## 核心接口

## 协议层如何使用 bytes

## DB lookup 和写路径如何使用 bytes

## ReplyWriter 和 Netty 写回

## fast path 和 fallback

## 和 native memory 的关系

## 推荐源码和测试
```

Required content:

- Cover `BytesSource`, `BytesView`, `BytesSlice`, `BytesSink`, `DirectBytesSink`, `NettyByteBufSink`, `RespCommandRequest`, and `BulkStringSink`.
- State where heap materialization is intentional: protocol adaptation snapshots, transaction replay, explicit introspection, tests, and unavoidable fallback paths.

- [ ] **Step 4: Check DB/executor/bytes terms**

Run:

```bash
rg -n "NativeKeyDirectory|EntryTable|EntryHandle|ValueHandle|CommandExecutor|ExecutorBackpressureController|BytesView|BytesSlice|DirectBytesSink|NettyByteBufSink" docs/project-docs/db-internals.md docs/project-docs/executor-and-backpressure.md docs/project-docs/bytes-and-fast-paths.md
```

Expected: output includes all required core terms.

- [ ] **Step 5: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/db-internals.md docs/project-docs/executor-and-backpressure.md docs/project-docs/bytes-and-fast-paths.md
```

Expected: no output and exit 0.

- [ ] **Step 6: Commit DB/executor/bytes manuals**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/db-internals.md docs/project-docs/executor-and-backpressure.md docs/project-docs/bytes-and-fast-paths.md
git commit -m "docs: rewrite db executor and bytes manuals" -- docs/project-docs/db-internals.md docs/project-docs/executor-and-backpressure.md docs/project-docs/bytes-and-fast-paths.md
```

Expected: commit succeeds and includes only the three manuals.

---

### Task 7: Split And Rewrite Native-Memory Docs

**Files:**
- Create: `docs/project-docs/ffm-primer.md`
- Create: `docs/project-docs/native-memory-runtime.md`
- Modify: `docs/project-docs/ffm-usage.md`
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/offheap-copy-behavior.md`

- [ ] **Step 1: Create `ffm-primer.md`**

Use this top-level structure:

```markdown
# FFM Primer

本文是阅读 Yierdis native-memory 文档前的最小 JDK FFM 入门。它只解释后续文档会反复用到的概念。

## 先记住三句话

## Arena

## MemorySegment

## ValueLayout

## offset 和 index

## slice

## lifetime 和关闭后的访问

## MemoryLayout 和结构化内存

## native function 调用边界

## 接下来读什么
```

Required content:

- Keep examples short and explain concepts, not full tutorials.
- Link to `native-memory-runtime.md`, `native-allocator-and-handles.md`, and `offheap-copy-behavior.md`.

- [ ] **Step 2: Create `native-memory-runtime.md`**

Use this top-level structure:

```markdown
# Native Memory Runtime

本文解释 Yierdis 如何把 JDK FFM 接入运行时：runtime、region、span、access、DB scope、maxmemory 和仍然 heap-backed 的边界。

## 当前结论

## 启动和组装

## runtime / region / span / access

## global scope 和 per-db scope

## keyspace、expires 和 value roots

## maxmemory 和 memory stats

## 仍然会 materialize 到 heap 的地方

## 和 stable allocator 的关系

## 推荐源码和测试
```

Required content:

- State that Yierdis requires JDK 25 and uses `java.lang.foreign`.
- Explain `YierdisFfmMemoryRuntime`, FFM-backed storage paths, DB-local versus instance-level runtime ownership, and `maxmemory` as the native-memory budget entry.
- Link to `native-allocator-and-handles.md` for stable allocator details.

- [ ] **Step 3: Rewrite `ffm-usage.md` as the native-memory landing page**

Use this top-level structure:

```markdown
# FFM Usage

本文是 Yierdis native-memory 文档的入口页。JDK FFM 基础、Yierdis runtime 接入、stable allocator 和 copy 行为已经拆到独立文档中。

## 该先读哪一篇

## Yierdis 里的 native-memory 层次

## 当前生产路径摘要

## 不要误读的边界

## 文档索引
```

Required links:

- `ffm-primer.md`
- `native-memory-runtime.md`
- `native-allocator-and-handles.md`
- `offheap-copy-behavior.md`
- `db-internals.md`
- `bytes-and-fast-paths.md`

- [ ] **Step 4: Rewrite `native-allocator-and-handles.md`**

Use this top-level structure:

```markdown
# Native Allocator And Handles

本文记录当前 production stable native allocator、handle ABI、object table、pin/epoch/quarantine、active defrag 和 DB handle 语义。

## 为什么需要 stable handle

## NativeHandle 位布局

## Object table

## Page 和 size class

## Stable allocator API

## realloc 语义

## pin / epoch / quarantine

## active defrag

## DB memory handle 迁移

## metrics

## 推荐测试
```

Required content:

- Distinguish stable handles from raw physical addresses.
- Cover `KEY_BYTES`, `ENTRY_RECORD`, `STRING_BYTES`, collection root records, list quicklist metadata records, and remaining internal/payload boundaries.
- Avoid claiming every collection internal node and payload byte is fully nativeized when that is not current-state truth.

- [ ] **Step 5: Rewrite `offheap-copy-behavior.md`**

Use this top-level structure:

```markdown
# Off-Heap Copy Behavior

本文说明 Yierdis 在 heap、direct buffer 和 FFM/native memory 之间什么时候复制，什么时候只是传递 view 或 handle。

## 先记住一句话

## Heap -> off-heap

## Off-heap -> heap

## Off-heap -> off-heap / direct -> direct

## 同侧复制也存在

## 常见误读

## 推荐源码和测试
```

Required content:

- Explain that zero-copy is a goal for selected paths, not a universal guarantee.
- Link back to `bytes-and-fast-paths.md` and `native-memory-runtime.md`.

- [ ] **Step 6: Run native-memory stale phrase audit**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs
```

Expected: remaining matches are current-state accurate, clearly marked as boundaries, or appear in references to historical/future context.

- [ ] **Step 7: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/ffm-primer.md docs/project-docs/native-memory-runtime.md docs/project-docs/ffm-usage.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md
```

Expected: no output and exit 0.

- [ ] **Step 8: Commit native-memory docs**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/ffm-primer.md docs/project-docs/native-memory-runtime.md docs/project-docs/ffm-usage.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md
git commit -m "docs: split native memory manuals" -- docs/project-docs/ffm-primer.md docs/project-docs/native-memory-runtime.md docs/project-docs/ffm-usage.md docs/project-docs/native-allocator-and-handles.md docs/project-docs/offheap-copy-behavior.md
```

Expected: commit succeeds and includes only the native-memory docs.

---

### Task 8: Rewrite Operations, Client, And Benchmark Manuals

**Files:**
- Modify: `docs/project-docs/configuration-and-operations.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`

- [ ] **Step 1: Rewrite `configuration-and-operations.md`**

Use this top-level structure:

```markdown
# Configuration And Operations

本文解释启动参数如何进入运行时配置，以及本地运行、观测、调参和关闭时应该看哪些入口。

## 配置流向

## 网络和实例规模

## protocol limits

## executor 和 backpressure

## transaction 保护

## TTL 和 maintenance

## maxmemory 和 eviction

## 慢客户端和输出缓冲保护

## 可观测命令

## 常见运行场景

## 启动失败和关闭

## 推荐测试
```

Required content:

- Cover `INFO`, `INFO yierdis`, `STATS`, `MEMORY STATS`, `MEMORY USAGE`, and `OBJECT ENCODING`.
- Keep operational commands aligned with root `README.md`.

- [ ] **Step 2: Rewrite `client-and-bench-internals.md`**

Use this top-level structure:

```markdown
# Client And Bench Internals

本文解释项目内置 CLI、Netty client、benchmark 和 smoke/bench 脚本如何沿真实 RESP 路径工作。

## 先记住一句话

## yierdis-cli

## InlineCommandParser

## YierdisClient

## RespClientCodec

## yierdis-benchmark

## ServerProcess

## workload workers

## strict reply validation

## smoke.sh 和 bench.sh

## 推荐源码和测试
```

Required content:

- Explain single-command CLI and REPL behavior.
- Explain that benchmark uses the real server process and real protocol path.
- Preserve caveats about comparing benchmark runs with the same workload shape.

- [ ] **Step 3: Check operations/client terms**

Run:

```bash
rg -n "INFO yierdis|MEMORY STATS|OBJECT ENCODING|YierdisClient|RespClientCodec|strict reply|bench.sh|smoke.sh" docs/project-docs/configuration-and-operations.md docs/project-docs/client-and-bench-internals.md
```

Expected: output includes the required operations and tool terms.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/configuration-and-operations.md docs/project-docs/client-and-bench-internals.md
```

Expected: no output and exit 0.

- [ ] **Step 5: Commit operations/client manuals**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/configuration-and-operations.md docs/project-docs/client-and-bench-internals.md
git commit -m "docs: rewrite operations and client manuals" -- docs/project-docs/configuration-and-operations.md docs/project-docs/client-and-bench-internals.md
```

Expected: commit succeeds and includes only the two manuals.

---

### Task 9: Rewrite Development, Testing, Matrix, And Reference Docs

**Files:**
- Modify: `docs/project-docs/development-navigation.md`
- Modify: `docs/project-docs/testing-and-debugging.md`
- Modify: `docs/project-docs/operation-test-coverage-matrix.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/glossary.md`

- [ ] **Step 1: Rewrite `development-navigation.md`**

Use this top-level structure:

```markdown
# Development Navigation

本文按常见改动类型回答一个实际问题：我要改某类需求时，应该先打开哪些文件，沿哪条链继续追。

## 工作规则

## 改协议

## 新增或修改命令

## 改 string / bitmap / HLL

## 改 list / hash / set / zset

## 改 keyspace / TTL / maxmemory

## 改 native memory

## 改 executor / backpressure

## 改 INFO / STATS / observability

## 推荐最小工作流

## 新人先收藏的文件
```

Required content:

- Each change type must list first files to open, next layer to inspect, and tests to run.
- Link to `testing-and-debugging.md`, `operation-test-coverage-matrix.md`, and the relevant topic manual.

- [ ] **Step 2: Rewrite `testing-and-debugging.md`**

Use this top-level structure:

```markdown
# Testing And Debugging

本文说明 Yierdis 测试如何分层，以及不同改动和故障应该先跑哪些测试、先看哪一层。

## 测试分层

## 改协议时

## 改命令时

## 改 DB 或数据结构时

## 改 native memory 时

## 改 executor / backpressure 时

## 改 CLI / bench 时

## operation coverage matrix

## 常见故障入口

## 最小验证组合
```

Required content:

- Preserve the matrix guard guidance and mention `OperationCoverageMatrixTest` and `ServerOperationCoverageMatrixTest`.
- Include the JDK 25 Maven prefix for test commands.

- [ ] **Step 3: Rewrite `operation-test-coverage-matrix.md` without breaking parser rules**

Keep this required top-level structure:

```markdown
# Operation Test Coverage Matrix

## Command Layer Coverage

### AUTH

## Option And Subcommand Coverage

## Option And Subcommand Inventory

## DB API Inventory

## Native/Internal Inventory

## Current Gap Queue
```

Required row shapes:

```markdown
### SET

- **Command layer**: `covered` - `SomeTest#methodName` covers the command-layer behavior.
- **DB API**: `covered` - `SomeDbTest#methodName` covers the direct DB behavior.
- **Native internals**: `covered-by-shared-test` - `SomeNativeTest#methodName` covers shared native storage behavior.
- **Command variant**: `SET NX` - `covered` - `SomeTest#methodName` covers conditional set.
```

Rules:

- Every registered command keeps a `### COMMAND` heading.
- Every status is one of `covered`, `covered-by-shared-test`, `missing`, or `not-applicable`.
- Every `covered` and `covered-by-shared-test` row cites at least one `FileName#methodName`.
- Keep the DB API inventory rows required by `OperationCoverageMatrixTest`.
- Keep the native/internal inventory terms required by `OperationCoverageMatrixTest`.
- Keep option inventory rows for commands with variants.

- [ ] **Step 4: Rewrite `core-logic-index.md` as compact index**

Use this top-level structure:

```markdown
# Core Logic Index

本文是源码定位索引，不再重复完整架构解释。每个条目说明职责、入口、边界和应该继续阅读的专题文档。

## 怎么使用这份索引

## Server 启动与组装

## Runtime 和多 DB

## Executor 和背压

## Engine、session 和事务

## Command 注册、解析和分发

## DB API 和 DB 内核

## RESP 和回包

## Bytes 和 native memory

## CLI 和 benchmark

## 测试和架构护栏

## 边界清单
```

Required content:

- Keep entries compact: class or module, responsibility, key methods, read next.
- Link to topic manuals for detailed explanations.

- [ ] **Step 5: Rewrite `glossary.md`**

Use this top-level structure:

```markdown
# Glossary

本文解释 Yierdis 文档和源码里反复出现的术语。每个术语都尽量指向最相关的专题文档。

## Request / Reply Path

## Command Layer

## Engine / Runtime

## DB / Keyspace / TTL

## Data Model

## Executor / Backpressure

## Bytes

## FFM / Native Memory

## Testing
```

Required terms include:

- `ExecutionRequest`
- `ByteArrayExecutionRequest`
- `ReplyWriter`
- `RespReplyWriter`
- `CommandSpec`
- `CommandRegistry`
- `CommandContext`
- `DbEngine`
- `YierdisDb`
- `YierdisInstance`
- owner thread
- maintenance tick
- keyspace
- expire index
- maxmemory
- retained bytes
- `EntryHandle`
- `ValueHandle`
- `NativeHandle`
- object table
- stable native allocator
- pin
- quarantine
- active defrag
- backpressure
- operation coverage matrix

- [ ] **Step 6: Run matrix guard tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Check whitespace**

Run:

```bash
git diff --check -- docs/project-docs/development-navigation.md docs/project-docs/testing-and-debugging.md docs/project-docs/operation-test-coverage-matrix.md docs/project-docs/core-logic-index.md docs/project-docs/glossary.md
```

Expected: no output and exit 0.

- [ ] **Step 8: Commit development and reference docs**

Run:

```bash
git diff --cached --name-status
git add docs/project-docs/development-navigation.md docs/project-docs/testing-and-debugging.md docs/project-docs/operation-test-coverage-matrix.md docs/project-docs/core-logic-index.md docs/project-docs/glossary.md
git commit -m "docs: rewrite development and reference docs" -- docs/project-docs/development-navigation.md docs/project-docs/testing-and-debugging.md docs/project-docs/operation-test-coverage-matrix.md docs/project-docs/core-logic-index.md docs/project-docs/glossary.md
```

Expected: commit succeeds and includes only the five docs.

---

### Task 10: Cross-Link, Phrase, And Final Verification Pass

**Files:**
- Modify: `README.md`
- Modify: `docs/project-docs/*.md`

- [ ] **Step 1: Run Markdown link audit**

Run this from the repository root:

```bash
python3 - <<'PY'
from pathlib import Path
import re
import sys

roots = [Path("README.md"), Path("docs/project-docs")]
files = [roots[0]] + sorted(roots[1].glob("*.md"))
link = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
broken = []
for file in files:
    text = file.read_text(encoding="utf-8")
    for raw in link.findall(text):
        target = raw.split("#", 1)[0].strip()
        if not target or target.startswith(("http://", "https://", "mailto:")):
            continue
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1]
        candidate = (file.parent / target).resolve()
        try:
            candidate.relative_to(Path.cwd().resolve())
        except ValueError:
            broken.append(f"{file}: outside repo link {raw}")
            continue
        if not candidate.exists():
            broken.append(f"{file}: missing link {raw}")
if broken:
    print("\n".join(broken))
    sys.exit(1)
PY
```

Expected: no output and exit 0.

- [ ] **Step 2: Run current-state phrase audit**

Run:

```bash
rg -n "不打算|明确不做|out-of-scope|完整 Redis 生态兼容|drop-in replacement" README.md docs/project-docs
rg -n "RespCommandAdapter|RespExecutionAdapter|ByteArrayExecutionRequest|RespReplyWriter|ReplyWriter" docs/project-docs
rg -n "blob-store-owned|still.*key bytes|adapter-owned|physical address|zero-copy|零拷贝" docs/project-docs
```

Expected:

- Redis compatibility wording says unsupported capabilities are not currently implemented or are later roadmap directions.
- Request-path terms match the current RESP-to-execution path.
- Native-memory matches are either current-state accurate or clearly framed as boundaries.

- [ ] **Step 3: Run placeholder scan**

Run:

```bash
rg -n "T[B]D|T[O]DO|F[I]XME|P[L]ACEHOLDER|待[补]|稍后[补]" docs/project-docs README.md
```

Expected: no output and exit 1 because there are no matches.

- [ ] **Step 4: Run final whitespace check**

Run:

```bash
git diff --check
```

Expected: no output and exit 0.

- [ ] **Step 5: Run matrix guard tests again**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Confirm final documentation map**

Run:

```bash
find docs/project-docs -maxdepth 1 -type f -name '*.md' | sort
rg -n "ffm-primer|native-memory-runtime|operation-test-coverage-matrix|development-navigation|core-logic-index" docs/project-docs/readme.md README.md
```

Expected:

- The file list includes all existing rewritten Markdown files plus `ffm-primer.md` and `native-memory-runtime.md`.
- `docs/project-docs/readme.md` links to the new native-memory split docs.
- `README.md` points readers to `docs/project-docs/readme.md`.

- [ ] **Step 7: Commit final consistency pass**

Run:

```bash
git diff --cached --name-status
git add README.md docs/project-docs/*.md
git commit -m "docs: verify project docs rewrite" -- README.md docs/project-docs/*.md
```

Expected: commit succeeds and includes only final documentation consistency edits. If there are no changes after the verification pass, skip this commit and record that no final consistency commit was needed.

## Self-Review Notes

- Spec coverage: every acceptance criterion in `docs/superpowers/specs/2026-05-17-project-docs-rewrite-design.md` maps to Tasks 2 through 10.
- Scope control: Java, Maven, scripts, SVG assets, and historical `docs/superpowers` specs are not edited.
- Matrix safety: Task 9 preserves the exact parser-facing structure for `operation-test-coverage-matrix.md` and runs the matrix guard tests.
- Link safety: Task 10 runs a repository-local Markdown link audit for root `README.md` and every top-level project-doc Markdown file.
- Native-memory split: Task 7 creates `ffm-primer.md` and `native-memory-runtime.md`, keeps `ffm-usage.md` as the landing page, and preserves allocator/copy reference docs.
