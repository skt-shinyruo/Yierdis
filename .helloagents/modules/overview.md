<!-- migrated_from: wiki/overview.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Yierdis

> 项目级核心信息入口；更细的模块说明位于本目录下各模块文档（如 `server.md`）。

---

## 1. 项目概览

### 目标与背景

Yierdis 是一个“教学/演示导向”的内存 KV 服务端：使用 Java 17 + Netty，实现一套 **自定义协议 Custom Protocol v1**（length-prefixed JSON request + NDJSON reply）来承载 Redis 风格的命令语义子集，便于学习网络编程、单线程命令执行、背压与淘汰等思路。

说明：对外不兼容 Redis 原生协议（Redis 生态客户端不可直接使用）；项目仅承诺 Custom Protocol v1。

### 范围

- **包含（In scope）**：内存数据结构、基础命令集、TTL（惰性删除 + 可选后台清理）、maxmemory（教学简化版，支持 global/per-db 口径）、最小事务子集（`MULTI/EXEC/DISCARD`，含队列上限保护）、自定义协议（Custom Protocol v1；协议错误尽量可恢复）、基础可观测性（`INFO/STATS/MEMORY STATS`）
- **不包含（Out of scope）**：AOF/RDB 持久化、复制、集群、Lua、ACL、TLS 等

---

## 2. 模块索引

| 模块 | 责任 | 状态 | 文档 |
|------|------|------|------|
| server | Netty 启动/管线/Handler | ✅Stable | [server.md](./server.md) |
| protocol | 协议层（`protocol-model` 端口/模型 + `protocol-codec` JSON/v1 codec；`yierdis-protocol` 为兼容聚合层） | ✅Stable | [protocol.md](./protocol.md) |
| protocol-netty | Netty codec/adapters（Custom Protocol v1 decoder/line decoder + 连接态适配） | ✅Stable | [protocol-netty.md](./protocol-netty.md) |
| command | 命令路由与参数解析 | ✅Stable | [command.md](./command.md) |
| db | 内存存储、编码、TTL、淘汰 | ✅Stable | [db.md](./db.md) |
| offheap | 堆外内存抽象与后端 | 🚧In Development | [offheap.md](./offheap.md) |
| client | 内置 Custom Protocol v1 CLI 客户端（用于调试） | ✅Stable | [client.md](./client.md) |

---

## 3. 快速链接

- [项目上下文](../context.md)
- [架构设计](arch.md)
- [命令/API 手册](api.md)
- [数据模型](data.md)
- [压测与基准测试](bench.md)
- [历史方案索引](../archive/_index.md)

---

## 4. 重要边界与常见踩坑（务必阅读）

### 协议边界（Custom Protocol v1）

- Wire framing：
  - request：`<len>:<json>\\n`（`len` 为 JSON payload 的 UTF-8 字节长度）
  - reply：NDJSON（每个 reply 一行 JSON，以 `\\n` 结尾）
- request schema（严格最小集合）：
  - payload 必须是 JSON object，至少包含 `cmd`（string）
  - `args` 为可选字段：允许省略或为 array；元素仅允许 `string|null`
  - 为保持可恢复 resync，payload 在 wire 上必须是 **单行**（字符串内的换行会被 JSON 转义为 `\\n`，wire 上不出现原始 CR/LF）
- 错误模型（尽量可恢复）：
  - 解析/校验失败：返回 `ok=false` 且 `error.kind=\"protocol\"`，并尽量丢弃到下一帧边界继续读取后续帧
  - 超出安全上限（例如长时间找不到帧边界）：作为 DoS 兜底允许断连

### 执行模型边界（单线程命令语义）

- server 采用 I/O 线程解码 + **单线程命令执行器** 串行执行（教学取向，贴近 Redis “单线程命令语义”）。
- 大输出/O(N) 命令会拉长该执行器的 drain 时间，导致 tail latency 放大、背压触发与 `ERR busy <reason>` 增多（例如 `KEYS`、大范围 `SCAN`、`HGETALL`、`SMEMBERS`、大范围 `ZRANGE` 等）。
- 排障建议：优先用 `STATS`/`INFO` 观察 backlog、busy 原因与背压计数（详见 `server.md`）。

### 事务与连接级命令（MULTI/EXEC）

- 事务为连接级队列，并有条数/bytes 硬上限保护；触发入队错误会进入 aborted，后续 `EXEC` 返回 `EXECABORT` 并丢弃队列。
- `HELLO` 属于连接级信息命令（用于输出 `server/version/proto/mode/role`）。为避免事务语义被“连接态类命令”干扰，MULTI 模式下禁止 `HELLO`（返回错误并触发 `EXECABORT`）。

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
| PubSub | ❌ out-of-scope | M1：最小 `SUBSCRIBE/PUBLISH`（不保证顺序与完整 Redis 语义）→ M2：out-of-band 通知分流与 client subscribe API | out-of-band 通知会打破“单请求-单响应”模型，需要重构 client/bench |
| TLS/ACL | ❌ out-of-scope | M1：TLS（仅 server 侧）→ M2：最小 ACL（命令白名单/连接级） | 需要引入证书管理与更严格的错误模型；教学价值与维护成本需平衡 |
| Lua / EVAL | ❌ out-of-scope | M1：脚本执行框架（只读脚本）→ M2：最小 `EVAL`（限制资源/指令） | 安全风险高（EHRB），需要沙箱与资源限制；兼容性测试成本大 |
| 复制/集群 | ❌ out-of-scope | M1：只读副本（单主）→ M2：最小复制（PSYNC/offset）→ M3：集群（slot/migrate） | 分布式一致性与故障恢复复杂度高；对项目定位可能不划算 |
