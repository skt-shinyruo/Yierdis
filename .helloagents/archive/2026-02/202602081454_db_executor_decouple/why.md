<!-- migrated_from: history/2026-02/202602081454_db_executor_decouple/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: DB/Executor 分层解耦与组件化（ReplySink + 巨型类拆分）

## Requirement Background

当前实现中，核心实现层次的耦合偏重，主要体现在两个“巨型类”上：

1. `YierdisDb`（`yierdis-core`）单文件体量大（约 3321 行），既承载 keyspace/数据结构/TTL/内存记账/淘汰，也在对外 API 形态上出现“为了回包优化的 API”（例如 `*ReplyCount/*ReplyInto`）与 `Command` 相关的低拷贝写入路径。
2. `NettyCommandExecutor`（`yierdis-server`）将背压、调度策略、队列预算、flush 合并、回包写出等编排集中在同一处，导致维护门槛高、边界 bug 的定位与修复成本上升。

这类耦合在短期内能带来更快的迭代，但长期会造成：
- 存储层/命令语义/协议编码/性能优化互相渗透，后续替换协议、补齐语义、引入持久化/复制、做多核运行时改造都会被迫在“核心热路径”里做高风险改动；
- 单点类持续增胖，导致变更评审难、回归范围难以界定；
- 复杂编排逻辑难以单测与复用，回归锚点不足时容易引入隐性退化。

本提案目标是以“渐进式分层重构（Strangler + Facade）”的方式，把“协议无关抽象/命令语义/存储引擎/执行器编排”重新切边，降低未来演进成本，并在每一步都有可验证的回归锚点。

## Change Content

1. **引入 `ReplySink`（窄接口）并收敛 streaming 输出边界：**
   - `ReplyWriter` 仍是命令层的协议无关写出 SSOT；
   - 新增 `ReplySink` 作为 `ReplyWriter` 的子集能力，用于 value 侧的“流式 bulk value 写出”，避免 DB/数据结构层感知协议细节或暴露“reply 形态 API”。
2. **迁移/改造 `*ReplyCount/*ReplyInto`：**
   - 从 `YierdisDb` 的公开 API 中移除这些“回包优化形态”的方法；
   - 将同类能力收敛到更明确的边界（`ops`/命令层），并逐步把内部实现从“reply-oriented”重命名为“data-oriented”（例如 `rangeCount/rangeInto` 等）。
3. **拆分 `YierdisDb` 的职责边界：**
   - 让 `YierdisDb` 更像引擎/状态容器（keyspace/expires/ledger/thread-guard/协调器），value 类型操作实现下沉到可组合组件（按 String/Hash/List/Set/ZSet/HLL 分拆）。
   - 逐步移除 `db` 包对 `protocol.Command` 的直接依赖：将写入热路径从 “接收 `Command`” 改为 “接收中立的 bytes view/slice”。
4. **拆分执行器编排：**
   - 以 `NettyCommandExecutor` 作为门面，抽出 drain loop、submit/预算、调度策略、回包写出与 flush 合并等组件，提升可测试性与可维护性。

## Impact Scope

- **Modules:** `yierdis-core`（db/ops/command）、`yierdis-protocol`（ReplySink 抽象）、`yierdis-server`（executor 组件化）
- **Files (representative):**
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/ReplyWriter.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/CommandSupport.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/*Ops.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutor*.java`
- **APIs:** 内部 API 允许任意调整（包名/类名/方法签名）；对外协议仍保持 Custom Protocol v1（现阶段不扩展对外协议承诺）
- **Data:** 无（纯内存结构重构，不引入新的持久化格式）

## Core Scenarios

### Requirement: ReplySink 分层（将 streaming bulk 输出从 db 包迁出）
**Module:** protocol / core(command+ops) / core(db)

建立一个窄接口 `ReplySink`，将“写出 bulk string 值”的能力与 `ReplyWriter` 的“形状语义（array/map/header/error 等）”解耦。

#### Scenario: LRANGE / SMEMBERS 结果以 streaming 方式写出
条件：命令返回集合类结果（list/set/zset 等），且结果量较大。
- 期望：命令层仍可先写出 header（array/map）再 streaming 写出元素
- 期望：不构建中间 `List<byte[]>` 作为聚合结果（保持现有低分配路径）
- 期望：db/数据结构层不再暴露“reply 优化形态”的 API（`*ReplyCount/*ReplyInto` 从 `YierdisDb` 对外移除）

#### Scenario: GET/ECHO 等 bulk string 输出保持一致
条件：读取 string 并写回 bulk string（含 `null` bulk string）。
- 期望：返回 shape 与空值语义保持不变
- 期望：off-heap slice/heap byte[] 路径对 `ReplySink` 透明

### Requirement: db 层与 protocol.Command 解耦（为未来协议/复制/持久化铺路）
**Module:** core(db) / core(command)

移除 `db` 包对 `protocol.Command` 的直接依赖：db 只消费中立的 bytes view/slice（可来自 command/frame 或 heap copy），并保留现有低拷贝写入能力。

#### Scenario: SET/APPEND 等写命令在 off-heap 后端下仍保持低拷贝特性
条件：off-heap 后端启用；命令参数来自协议层 frame（可提供 `BytesSource + offset`）。
- 期望：写入路径可直接从 frame slice 拷贝到 off-heap（避免先 `toByteArray` 再拷贝）
- 期望：错误语义与 maxmemory 语义不回归（不引入双 reply）

### Requirement: YierdisDb 组件化（降低单类复杂度与变更爆炸半径）
**Module:** core(db)

将 value 类型操作与引擎状态分离，使 `YierdisDb` 的职责更清晰、可测试。

#### Scenario: 过期清理/淘汰/内存记账行为保持一致
条件：在高压写入 + TTL + maxmemory 场景下运行既有回归测试。
- 期望：`MemoryLedger` 两阶段语义与统计口径不变
- 期望：过期惰性删除、后台维护 tick 的行为不变

### Requirement: NettyCommandExecutor 拆分（可测试、可演进）
**Module:** server

将执行器编排拆解为更小的组件，降低维护成本，并保留现有行为与可观测性。

#### Scenario: Backpressure 与公平调度回归
条件：启用 FAIR scheduling；压测单连接/多连接混合 pipeline。
- 期望：连接级 `autoRead` 背压滞回行为不变
- 期望：fair scheduling 的 round-robin 语义不变
- 期望：`ERR busy <reason>` 与 `STATS` 计数器口径不变

## Risk Assessment

- **Risk:** 语义回归（聚合 shape、空值语义、错误模型、事务/MULTI 边界）
  - **Mitigation:** 先补齐针对 streaming reply 的单测/集成测锚点；每次阶段性迁移后运行 `mvn test`；保持命令层写出形状的 SSOT 不变。
- **Risk:** 性能回归（双遍历、额外分配、adapter 造成的额外间接调用）
  - **Mitigation:** 渐进迁移：先引入窄接口与适配层，再逐步去除 adapter；用 `yierdis-bench` 做 A/B；对热点路径（SET/GET/聚合读）设定可解释指标。
- **Risk:** off-heap slice 生命周期/引用语义被错误延长（泄漏或越界）
  - **Mitigation:** 将 sink 的职责限制为“同步写出、不得缓存引用”；保留并扩展泄漏回归测试；对 retainedBytes/queued-bytes 口径做一致性审计。
- **Risk:** 执行器拆分引入竞态（drainScheduled/running/closing）或改变 flush 合并边界
  - **Mitigation:** 复用现有 executor 测试（backpressure/fair scheduling）；拆分以“移动代码不改逻辑”为主，先提取组件、后微调结构。

