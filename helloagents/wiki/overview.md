# Yierdis

> 项目级核心信息入口；更细的模块说明位于 `modules/`。

---

## 1. 项目概览

### 目标与背景

Yierdis 是一个“教学/演示导向”的简化版 Redis 兼容服务端：使用 Java 17 + Netty，实现 RESP2/RESP3（最小子集）over TCP 的核心命令子集，并提供 inline command 支持（调试用，支持引号/转义/`\\xHH`），便于学习网络编程与 Redis 协议/数据结构思路。

### 范围

- **包含（In scope）**：内存数据结构、基础命令集、TTL（惰性删除 + 可选后台清理）、maxmemory（教学简化版，支持 global/per-db 口径）、最小事务子集（`MULTI/EXEC/DISCARD`，含队列上限保护）、RESP2 + RESP3（reply 最小子集）、基础可观测性（`INFO/STATS/MEMORY STATS`）
- **不包含（Out of scope）**：AOF/RDB 持久化、复制、集群、Lua、ACL、TLS 等

---

## 2. 模块索引

| 模块 | 责任 | 状态 | 文档 |
|------|------|------|------|
| server | Netty 启动/管线/Handler | ✅Stable | [modules/server.md](modules/server.md) |
| protocol | RESP 对象模型 + `RespWriter`（Netty-free SSOT） | ✅Stable | [modules/protocol.md](modules/protocol.md) |
| protocol-netty | Netty codec/adapters（decoder/encoder/frame/session） | ✅Stable | [modules/protocol-netty.md](modules/protocol-netty.md) |
| command | 命令路由与参数解析 | ✅Stable | [modules/command.md](modules/command.md) |
| db | 内存存储、编码、TTL、淘汰 | ✅Stable | [modules/db.md](modules/db.md) |
| offheap | 堆外内存抽象与后端 | 🚧In Development | [modules/offheap.md](modules/offheap.md) |
| client | 内置 RESP2 CLI 客户端（用于调试） | ✅Stable | [modules/client.md](modules/client.md) |

---

## 3. 快速链接

- [技术约定（SSOT）](../project.md)
- [架构设计](arch.md)
- [命令/API 手册](api.md)
- [数据模型](data.md)
- [压测与基准测试](bench.md)
- [变更历史](../history/index.md)

---

## 4. 重要边界与常见踩坑（务必阅读）

### 协议边界（RESP2/RESP3）

- RESP3 支持覆盖 request + reply：连接默认 RESP2；执行 `HELLO 3` 后服务端切换为 RESP3 回复，并尽量使用 RESP3 类型表达集合与标量；同时 reply 侧 decoder/parser 支持 streamed strings（`$? ... ;0`）与 streamed aggregates（`*?/%?/~? ... .`）。
- request 侧仍坚持“命令形态”的严格约束（top-level 仅允许 `*` array/inline command）；在此基础上增强兼容性：
  - 允许命令前携带 RESP3 `|` attributes（忽略 attributes map）
  - 允许 `*` 命令数组内使用 RESP3 标量类型（例如 `+`/`:`/`_`/`#`/`,`/`(`/`=`）作为参数
  - 支持 `$?` streamed blob string 作为参数（必要时 materialize 为连续 argv bytes）
  - 支持 `*?` streamed array 作为命令容器（直到 `.` END）
- 仍保持明确边界：不支持将 map/set/push/attribute 等聚合类型作为“命令参数”传入（避免语义不确定）；遇到不支持的类型会返回 protocol error，并关闭连接（Redis 风格严格错误模型）。

### 执行模型边界（单线程命令语义）

- server 采用 I/O 线程解码 + **单线程命令执行器** 串行执行（教学取向，贴近 Redis “单线程命令语义”）。
- 大输出/O(N) 命令会拉长该执行器的 drain 时间，导致 tail latency 放大、背压触发与 `-ERR busy <reason>` 增多（例如 `KEYS`、大范围 `SCAN`、`HGETALL`、`SMEMBERS`、大范围 `ZRANGE` 等）。
- 排障建议：优先用 `STATS`/`INFO` 观察 backlog、busy 原因与背压计数（详见 `helloagents/wiki/modules/server.md`）。

### 事务与连接级命令（MULTI/EXEC）

- 事务为连接级队列，并有条数/bytes 硬上限保护；触发入队错误会进入 aborted，后续 `EXEC` 返回 `EXECABORT` 并丢弃队列。
- `HELLO` 属于连接级协议协商命令（RESP2/RESP3 握手）。为避免在 `EXEC` reply 中混入不同协议类型前缀导致客户端解析失败，MULTI 模式下禁止 `HELLO`（返回错误并触发 `EXECABORT`）。

### maxmemory 与 off-heap（预算口径）

- `--maxmemoryBytes` 是教学取向的 best-effort 预算口径，用于解释“为什么拒写/为什么淘汰”，并不等同于 JVM/GC 视角的真实 heap 使用量。
- 启用 off-heap 后端时务必关注 `--offheapMaxBytes`：若启用 off-heap 但保持 `offheapMaxBytes=0`，则 off-heap 没有硬上限；此时即使配置了 `--maxmemoryBytes` 也可能出现“以为有限制但 off-heap 仍增长”的误解。

---

## 5. 兼容性路线图（Roadmap）

本项目的核心定位是教学/演示；路线图用于说明“哪些能力可能做、做的深度到哪里、为什么当前不做/暂缓”，避免被误认为 Redis 的 drop-in replacement。

> 说明：以下为阶段性规划，并不承诺全部实现；当某个能力需要引入复杂的分布式一致性或持久化语义时，会优先选择“可解释、可验证”的最小实现。

| 能力 | 当前状态 | 里程碑（阶段性） | 风险/备注 |
|------|----------|------------------|----------|
| AOF/RDB 持久化 | ❌ out-of-scope | M1：手动 RDB snapshot（SAVE/LOAD，单线程一致性）→ M2：AOF 最小子集（append + rewrite） | 正确性与恢复语义复杂；易与 maxmemory/过期/事务交织 |
| PubSub | ❌ out-of-scope | M1：最小 `SUBSCRIBE/PUBLISH`（不保证顺序与完整 Redis 语义）→ M2：push 分流与 client subscribe API | RESP3 push 会打破“单请求-单响应”模型，需要重构 client/bench |
| TLS/ACL | ❌ out-of-scope | M1：TLS（仅 server 侧）→ M2：最小 ACL（命令白名单/连接级） | 需要引入证书管理与更严格的错误模型；教学价值与维护成本需平衡 |
| Lua / EVAL | ❌ out-of-scope | M1：脚本执行框架（只读脚本）→ M2：最小 `EVAL`（限制资源/指令） | 安全风险高（EHRB），需要沙箱与资源限制；兼容性测试成本大 |
| 复制/集群 | ❌ out-of-scope | M1：只读副本（单主）→ M2：最小复制（PSYNC/offset）→ M3：集群（slot/migrate） | 分布式一致性与故障恢复复杂度高；对项目定位可能不划算 |
