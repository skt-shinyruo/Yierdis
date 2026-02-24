# 变更提案：架构收敛（移除 Deprecated Alias / 协议栈统一 / ConnectionContext / 可观测性 / 命令路由加速）

## 需求背景

当前项目已具备较完整的 server/client/bench 多模块形态，但在“协议/bytes/连接态/执行器/命令路由”交界处仍存在多处架构层面的摩擦点，主要表现为：

1. **兼容层与 Deprecated alias 仍在扩散**：同一语义存在多套接口（例如 off-heap api 的 bytes alias 与 `yierdis-bytes` 并存），使依赖边界变得不清晰，增加维护成本与误用概率。
2. **client/bench 与 server 存在协议实现“双轨”**：server 侧走更偏性能与零拷贝的 fast-path（`RespCommandDecoder` + `RespWriter`），而 client/bench 的回复解析仍走 “正确性优先、分配较多” 的实现路径，导致行为与性能优化难以统一。
3. **连接态散落在多个 `Channel.attr`**：协议协商状态与执行器的 pending/backpressure/closing 等状态分别以多个 `AttributeKey` 分散存储，形成隐式耦合；当状态机演进时容易出现遗漏与不一致。
4. **执行器复杂度提升但缺少可观测性支撑**：`NettyCommandExecutor` 已具备队列、背压、调度策略、compaction 等能力，但缺乏统一的统计与诊断入口，线上/压测问题定位成本高。
5. **命令路由线性扫描扩展性不足**：`CommandRegistry` 线性扫描在命令数量增长时不可避免地带来额外 CPU 消耗，也不利于更精细的可观测性（如 per-command 命中统计、未知命令诊断）。

用户约束与偏好（必须遵守）：
- 全部落地、没有优先级（但允许按依赖关系排序执行）
- 兼容层目标：**A. 彻底移除（Breaking change）**
- 协议双轨目标：**A. client/bench 收敛到与 server 同一套 fast-path/codec**
- 连接态：**直接迁移**到单一 `ConnectionContext`（不做最小整理）
- **性能与可观测性优先**

## 变更内容

1. **移除 Deprecated bytes alias（Breaking）**：以 `yierdis-bytes` 作为唯一 bytes 抽象（`BytesSink/BytesSource/DirectBytesSink/BytesSlice`），彻底删除 off-heap api 中的 alias 类型并完成全仓迁移。
2. **协议栈统一（client/bench ←→ server）**：将 client/bench 的回复解码迁移到与 server 同源的 fast-path/codec 体系，收敛协议解析逻辑与 DoS 限制策略，减少分配与拷贝。
3. **连接态 SSOT：ConnectionContext**：建立单一 `ConnectionContext` 承载协议协商 + 执行器连接态 + 连接级统计，替换散落的 `Channel.attr` 状态。
4. **可观测性优先**：补齐 executor/connection/command 的关键指标与诊断输出（低开销、可按需开启），并提供 server 侧可查询入口（例如 `INFO`/`STATS`）。
5. **命令路由加速**：将 `CommandRegistry` 从线性扫描改为 O(1) 的索引结构（保持运行时零分配约束），并为观测（命中/未知命令）预留挂点。

## 影响范围

- **Modules：**
  - `yierdis-bytes`
  - `yierdis-bytes-netty`
  - `yierdis-offheap/api`
  - `yierdis-offheap/netty`
  - `yierdis-offheap/unsafe`
  - `yierdis-offheap/foreign`
  - `yierdis-protocol`
  - `yierdis-protocol-netty`
  - `yierdis-server`
  - `yierdis-client`
  - `yierdis-bench`
  - `yierdis-core`
- **Files（非穷举，列关键入口）：**
  - `yierdis-offheap/api/.../YierdisOffHeapSlice.java`
  - `yierdis-offheap/api/.../YierdisBytesSink.java`（计划删除）
  - `yierdis-protocol-netty/.../RespCommandDecoder.java`
  - `yierdis-protocol-netty/.../RespDecoder.java`（计划收敛/替换）
  - `yierdis-protocol-netty/.../NettyRespSession.java`（计划收敛/替换）
  - `yierdis-server/.../NettyCommandExecutor.java`
  - `yierdis-server/.../YierdisServerBootstrap.java`
  - `yierdis-client/.../YierdisClient.java`
  - `yierdis-bench/.../YierdisBench.java`
  - `yierdis-core/.../command/CommandRegistry.java`
- **APIs（Breaking / 新增）：**
  - 删除：`yier.bubu.redis.db.offheap.api.YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink`（以及相关 alias）
  - 变更：`YierdisOffHeapSlice` 的读写接口签名（收敛到 `yier.bubu.redis.bytes.*`）
  - 新增：`yier.bubu.redis.protocol.netty.ConnectionContext`（SSOT）
  - 新增：server 可观测性命令（`INFO`/`STATS` 等）
- **Data：** 无数据结构/持久化层迁移（仅运行时行为与接口变更）

## 核心场景

### Requirement: bytes-alias-removal
**Module:** `yierdis-offheap/*`、`yierdis-protocol`、`yierdis-server`

彻底移除 bytes alias，保证 bytes 抽象的 SSOT 为 `yierdis-bytes`。

#### Scenario: compile-time-enforcement
- 任何模块不再引用 `YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink`（编译期强制失败）
- off-heap slice 的读写统一使用 `yier.bubu.redis.bytes.*`，并保持 Netty/Direct 的 fast-path 能力

### Requirement: protocol-unification
**Module:** `yierdis-protocol-netty`、`yierdis-client`、`yierdis-bench`

client/bench 的回复解析与 server 侧 codec 同源，避免“双轨”长期分叉。

#### Scenario: client-bench-use-same-fast-codec
- client/bench 使用 frame/zero-copy 取向的回复 decoder（同一套解析核心与限制参数）
- 行为一致性：错误信息与边界条件（bulk/array/line 上限）与 server 侧对齐
- 性能导向：减少字符串/byte[] 分配与拷贝

### Requirement: connection-context
**Module:** `yierdis-protocol-netty`、`yierdis-server`

连接态以 `ConnectionContext` 为唯一事实来源（SSOT），替换多组 `Channel.attr`。

#### Scenario: single-context-for-protocol-and-executor
- 协议协商状态、pending/backpressure/closing 等状态统一放入 `ConnectionContext`
- server pipeline 与 executor 不再直接维护多组 `AttributeKey`（只维护一个 context key）

### Requirement: observability
**Module:** `yierdis-server`、`yierdis-core`

对关键路径提供“能看见”的诊断入口与指标，降低排障成本。

#### Scenario: debug-and-stats
- 可查询：队列深度/字节、背压进入/退出次数、命令处理计数、慢命令/超预算 drain 等
- 低开销：常态不引入大规模分配；可通过配置/命令按需开启更详细输出

### Requirement: command-routing-index
**Module:** `yierdis-core`

命令路由结构升级以提升扩展性与观测能力。

#### Scenario: o1-command-lookup
- 命令查找由线性扫描变为 O(1) 索引结构（保持运行时零分配）
- 为 per-command 统计与未知命令诊断提供统一挂点

## 风险评估

- **Risk：Breaking change（接口删除/签名变更）**
  - **Mitigation：** 在任务中加入“全仓编译/全量测试/bench 冒烟”，并同步更新知识库与 CHANGELOG，明确迁移指南。
- **Risk：性能回退（codec/执行器改造）**
  - **Mitigation：** 逐段加入 micro/bench 对比（bench 现有工具可复用），并在关键路径保留 fast-path（zero-copy、voidPromise 等）。
- **Risk：状态机迁移引入竞态/泄漏**
  - **Mitigation：** ConnectionContext 封装状态迁移，补齐 executor/backpressure/closing 的集成测试与资源回收断言。
