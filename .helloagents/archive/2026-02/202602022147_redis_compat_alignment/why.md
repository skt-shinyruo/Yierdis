<!-- migrated_from: history/2026-02/202602022147_redis_compat_alignment/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Redis 兼容性对齐与功能扩展（RESP3 / 事务 / 全局 maxmemory）

## Requirement Background

本项目当前定位为“教学/演示取向的 Redis 兼容服务端（单机内存版）”，核心链路已具备（Netty + RESP + 命令执行 + 数据结构 + TTL + maxmemory + off-heap 实验）。但如果目标升级为“更贴近 Redis（可作为更可信的兼容实现）”，现有实现存在以下问题与风险：

1. **兼容面较窄**：缺少持久化（AOF/RDB）、复制/集群、Lua、ACL/TLS、PubSub 等；已实现命令也存在“最小子集”语义差异（例如 TTL 清理、KEYS/glob）。
2. **RESP3 最小子集已存在但不一致**：`HELLO 3` 可切换 RESP3，且部分命令会输出 RESP3 map/set；但 Netty codec 的 `RespEncoder` 覆盖不完整，未来扩展容易出现“写出语义漂移”。
3. **事务 MULTI 队列无上限**：MULTI 模式下命令 argv 会被复制并缓存在连接态队列中，缺少条数/bytes 的硬限制，存在堆 OOM 风险。
4. **maxmemory 口径与 Redis 不一致**：当前 server 多 DB 场景下会把 `--maxmemoryBytes` 按 DB 分摊；与 Redis 实例级全局预算不同。且 off-heap 与 maxmemory 是两套约束，容易误配置。
5. **busy/背压诊断不够直观**：执行器 backpressure/预算机制较完整，但 `-ERR busy` 的原因对客户端不透明，定位依赖读日志/看统计。

## Change Content

1. **扩展 RESP3**：补齐 `RespEncoder` / CLI 的 RESP3 类型支持，统一写出语义，并新增一致性测试（encode→decode→parse）。
2. **事务安全**：为 MULTI 队列引入 Redis 风格的“入队错误→EXECABORT”语义，并增加可配置的条数/bytes 上限，防止 OOM。
3. **内存预算对齐**：将 `--maxmemoryBytes` 口径调整为“实例级全局预算”，支持跨 DB 统一淘汰（allkeys-random/allkeys-lru），并明确 off-heap 与 maxmemory 的关系（默认纳入全局预算）。
4. **可观测性**：提升 `-ERR busy` 的可诊断性（返回原因或可在 STATS 中定位），并补齐关键文档与 wiki。
5. **兼容性路线图**：为缺失的大特性（AOF/RDB、PubSub、TLS/ACL、Lua、复制/集群）制定分阶段计划与边界声明，避免误用。

## Impact Scope

- **Modules:**
  - yierdis-protocol
  - yierdis-protocol-netty
  - yierdis-core
  - yierdis-server
  - yierdis-args
  - yierdis-client
  - helloagents/wiki（文档同步）
- **Files（重点，非穷尽）：**
  - yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespEncoder.java
  - yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java
  - yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObject.java
  - yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java
  - yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java
  - yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java
  - yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java（可能引入全局预算协调接口）
- **APIs（广义）：**
  - TCP/RESP 协议行为（RESP3 类型输出）
  - Server CLI args（新增 transaction/memory 相关参数）
  - INFO/STATS/MEMORY 命令输出口径（全局化）
- **Data:**
  - 内存内数据结构与淘汰行为（可能改变既有脚本/测试预期）

## Core Scenarios

### Requirement: R1_RESP3 编码一致性与扩展
**Module:** protocol / protocol-netty / client

补齐 RESP3 类型的编码与显示，确保 server fast-path 与 codec adapter 行为一致，避免“同一 RespObject 在不同写出路径下协议不一致”。

#### Scenario: S1_RespEncoder 覆盖 RESP3 全类型
条件：通过 Netty pipeline 使用 `RespEncoder` 写出 `RespObject`（含 set/boolean/double/bignum/verbatim/blob error/push/attribute/null）。

- 期望结果：
  - 输出字节符合 RESP3 规范（与 `RespWriter` 语义一致）
  - 可被 `RespDecoder` + `RespObjectParser` 正确解析
  - 保持与 `RespWriter.error()` 一致的 CRLF 注入净化策略

#### Scenario: S2_CLI 对 RESP3 类型友好展示
条件：CLI 执行 `HELLO 3` 后读取 map/set/null/boolean/double 等回复。

- 期望结果：
  - CLI 输出可读、稳定，不出现“未知类型”或难以理解的 raw dump
  - 输出结构与 `redis-cli` 的阅读习惯尽量接近（例如 map/set 的逐项展示）

### Requirement: R2_事务队列上限与 EXECABORT 语义
**Module:** server / core / protocol

为 MULTI 入队引入资源上限与 Redis 风格的“入队错误导致 EXECABORT”语义，避免大事务造成 OOM 或长时间驻留。

#### Scenario: S1_MULTI 队列超限保护
条件：MULTI 状态下持续发送大量/大参数命令，超过配置的条数或 bytes 上限。

- 期望结果：
  - 触发入队错误，返回明确错误
  - 事务被标记为 aborted；EXEC 返回 EXECABORT 并清理队列
  - 连接关闭时能 best-effort 清理（避免悬挂引用）

### Requirement: R3_maxmemory 全局口径与跨 DB 淘汰
**Module:** server / core / db

让 maxmemory 行为更贴近 Redis（实例级预算），并在多 DB 场景下保证口径一致。

#### Scenario: S1_跨 DB 统一预算
条件：开启 `--maxmemoryBytes`，在 DB0/DB1 分别写入数据直到触发淘汰。

- 期望结果：
  - 全局 used > maxmemory 时触发淘汰，而不是按 DB 固定切分
  - 淘汰策略（allkeys-random/allkeys-lru）在全局维度生效（近似采样允许）
  - `MEMORY STATS` 与 `INFO MEMORY` 体现全局口径

#### Scenario: S2_off-heap 与 maxmemory 关系清晰且安全
条件：启用 off-heap backend，配置 maxmemory 但未配置 `offheapMaxBytes`（或配置不合理）。

- 期望结果：
  - 启动/INFO 输出明确提示风险或给出安全默认策略
  - 全局预算统计不会双计或漏计（shared allocator 只计一次）

### Requirement: R4_busy/背压可诊断性
**Module:** server / protocol / docs

让用户在遇到 `-ERR busy` 时可快速定位原因并调参。

#### Scenario: S1_busy 返回原因或可通过 STATS 定位
条件：压测导致队列满或 bytes 预算触发，server 返回 busy。

- 期望结果：
  - busy 错误包含原因（queue_full/bytes_budget/not_running 等）或提供稳定诊断路径（STATS 字段/INFO yierdis）
  - 不破坏 `redis-cli` 基本兼容（至少保持 RESP error 形态）

### Requirement: R5_兼容性路线图与边界声明
**Module:** docs / wiki / README

对缺失特性给出明确路线图与边界声明，避免误用（尤其是“误当成 drop-in replacement”）。

#### Scenario: S1_用户理解一致
条件：新用户阅读 README/wiki 并尝试把 Yierdis 当作 Redis 替代。

- 期望结果：
  - 文档清晰声明 in-scope/out-of-scope
  - 给出未来扩展计划与优先级（非承诺，明确风险与工作量）

## Risk Assessment

- **风险：maxmemory 口径变化导致行为变化（多 DB 场景尤甚）。**
  - 缓解：引入兼容开关（例如 `maxmemoryScope=global|per-db`），默认 global，并在 README/wiki 明确说明。
- **风险：RESP3 扩展带来协议兼容性与测试覆盖压力。**
  - 缓解：以 `RespWriter` 作为 SSOT；增加 round-trip 测试（encode→decode→parse）；覆盖关键类型与嵌套结构。
- **风险：事务队列限制与 Redis 细节可能不完全一致。**
  - 缓解：对齐 Redis 的 EXECABORT 语义；文档注明差异点；增加回归测试与压力场景（防 OOM）。
- **风险：全局淘汰策略实现复杂，可能引入性能回退。**
  - 缓解：保持采样近似、时间预算；必要时提供 `global`/`per-db` 两种模式供教学与兼容性对比。

