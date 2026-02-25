# server

## Purpose

负责 Netty 服务端启动、管线组装与连接生命周期管理。

## Module Overview

- **Responsibility:** 端口监听、Pipeline 组装、定时任务（如 TTL 清理）的调度入口
- **ArtifactId:** `yierdis-server`
- **Status:** ✅Stable
- **Last Updated:** 2026-02-25

## Specifications

### Requirement: 自定义协议 v1 TCP 服务端（仅 Custom Protocol v1）
**Module:** server
启动一个基于 Netty 的 TCP 服务端，使用**自定义协议 v1** 承载“argv 风格命令请求/响应”，对外仅承诺 Custom Protocol v1。

协议要点：
- request framing：`<len>:<json-payload>\\n`（`len` 为 JSON payload 的 UTF-8 字节长度）
- request schema：JSON object：`{"cmd":"PING","args":["a","b"]}`（`args` 可省略；元素仅允许 `string|null`）
- reply framing：NDJSON（每条回复一个 JSON object，以 `\\n` 结尾）
- reply envelope：
  - 成功：`{"ok":true,"result":...}\\n`
  - 错误：`{"ok":false,"error":{"kind":"protocol|command|internal","message":"..."}}\\n`

#### Scenario: 基本连通性
条件：服务端启动并监听端口
- 预期：客户端发送 `14:{"cmd":"PING"}\\n`，服务端返回 `{"ok":true,"result":"PONG"}\\n`
- 预期：客户端发送 `30:{"cmd":"SET","args":["k","v"]}\\n`，服务端返回 `{"ok":true,"result":"OK"}\\n`
- 预期：建议优先使用项目自带 `yierdis-client`/`yierdis-cli` 做调试与回归（避免手写 len 计算出错）

#### Scenario: 协议错误（非法帧 / 非法 JSON / 非法 schema）
条件：客户端发送非法请求（解码阶段触发 protocol error）
- 预期：服务端返回 `{"ok":false,"error":{"kind":"protocol","message":"Protocol error: ..."}}\\n`
- 预期：服务端**尝试继续读取下一帧**（resync：丢弃到下一处 `\\n` 边界），连接保持可用
- 说明：这是为了匹配“宽容解析”的客户端直觉（错误可恢复）；仅在无法找到帧边界超过上限时才会断连（DoS 防护）

### Requirement: 多 DB（DB0..N-1）与连接级 DB 路由
**Module:** server
为提升 Redis 生态兼容性，server 支持多逻辑 DB：
- 通过 `--databases <n>` 配置 DB 数量（默认 16）
- 连接级维护 `dbIndex`（默认 0），由 `SELECT <index>` 修改
- server 通过 core 的 `YierdisInstance` 装配多个 DB 引擎（`DbEngine`，当前实现为 `YierdisDb`，DB0..N-1）与路由（`YierdisDbRouter`），命令执行时按连接态路由到目标 DB
- 执行器线程在启动阶段调用 `instance.bindToCurrentThread()` 完成 owner-thread 绑定（保持单线程命令语义）
- 边界约束：server 侧不直接依赖 `YierdisDb` 类型（通过 `DbEngine`/binder/ops 交互），避免执行器/调度逻辑被具体 DB 实现细节污染

说明：多 DB 仍保持“单线程命令语义”（所有 DB 绑定同一 executor 线程），避免引入跨线程并发复杂度。

扩展边界（冻结约定）：
- 路由扩展点在 core 的 `YierdisInstance`/router；server 不实现 shard 逻辑。
- 若未来引入 key-hash shard：跨 shard 的多 key 命令默认应返回确定性的错误（类似 CROSSSLOT），避免部分成功造成一致性不可解释。

### Requirement: I/O 与命令执行解耦（单线程命令语义）
**Module:** yierdis-server
执行模型已升级为 Netty 体系内的单线程执行器（`DefaultEventExecutorGroup(1)` + `NettyCommandExecutor`）：
- 目标：保持 Redis 风格“全局单线程命令语义”，同时减少 per-command `flush` 并引入连接级背压闭环
- 执行：I/O 线程负责解码与投递；命令在执行器线程串行执行；同一轮 drain 内对同一连接 `write` 聚合并在末尾 `flush`
- 协议写出解耦：执行器与 handler 通过 `ReplyWriterFactory` 注入获取 `ReplyWriter`；默认使用 Custom Protocol v1 的 `JsonLineReplyWriterFactory`，避免在执行器侧直接依赖具体协议实现
- 执行上下文收敛：执行器为每条命令组装 `CommandContext`（`session + out`），并调用 core 的 `YierdisFastCommandProcessor.execute(cmd, ctx)`；路由/事务/可观测等输入侧状态从 `ctx.session()` 获取，避免通过输出端口旁路读取
- 背压：采用“双约束”：per-connection pending **条数** + pending **bytes** 两套水位线（带滞回阈值 high/low），避免“少量大包积压”导致内存驻留不可解释
- 公平性：支持连接级公平调度（per-channel queue + round-robin），避免热点连接长期挤占全局 backlog（可配置）
- 连接关闭语义：`QUIT` 由命令层请求 close-after-reply，执行器在 flush 后关闭连接，并跳过该连接后续已入队命令（仅回收，不执行 DB），保证 pipeline 顺序与无副作用
- 连接态拆分：`ServerSessionState`（dbIndex/事务队列/AUTH/name；实现 `ServerSession`）与 `ServerRuntimeState`（pending/backpressure/counters/closing）分别通过 `Channel.attr` 绑定，显式区分单线程 session 与跨线程 runtime 的访问边界
- 组件化实现：队列/预算/调度/autoRead tracking 等 Netty-free 决策下沉到 `yierdis-executor-core`（`ExecutorTaskQueue` / `ExecutorBacklogBudget` / `ExecutorBackpressureController`）；server 侧 `NettyCommandExecutor` 负责 Netty adapter（`Channel`/writability/autoRead）与写回编排；per-channel 调度 state 仍收敛到 server 私有 `NettyExecutorChannelState`（`Channel.attr`）
- 配置收敛：执行器的队列/背压/drain/调度策略参数收敛到 `NettyCommandExecutorConfig`（由 `ServerConfig` 派生），降低装配与测试的维护成本
- 可观测性：提供 `INFO`/`STATS` 命令输出执行器/连接级统计摘要（队列、背压 enter/exit、reject、drain budget、close-after-reply 等），用于排障与容量评估
  - `INFO`：返回一个 JSON string（内部仍按“文本分节”拼接）
  - `INFO YIERDIS` / `STATS`：返回结构化 JSON（array/object），用于教学与排障

#### Scenario: 高压 pipeline 下的 flush 合并与背压恢复
- 当 backlog ≥ high watermark：服务端对该连接 `autoRead=false`，并可能返回 `ok=false` 的 `ERR busy <reason>`
- 当 backlog ≤ low watermark：服务端恢复 `autoRead=true`，连接继续读入并保持响应顺序一致

#### Configuration: 相关启动参数
- `--executorQueueCapacity <n>`：全局执行队列容量（有界）
- `--executorQueueMaxBytes <bytes>`：全局执行队列 bytes 上限（0 表示禁用；用于防止大 bulk 积压）
- `--executorSchedulingPolicy global|fair`：执行队列调度策略（全局 FIFO / 连接级公平）
- `--backpressureHigh <n>` / `--backpressureLow <n>`：连接级背压滞回阈值
- `--backpressureBytesHigh <bytes>` / `--backpressureBytesLow <bytes>`：连接级 bytes 背压滞回阈值（0 表示禁用）
- `--executorMaxDrain <n>` / `--executorDrainMillis <ms>`：单次 drain 批量/时间预算（避免维护任务饥饿）
- `--protocolMaxBulkBytes <bytes>` / `--protocolMaxArgs <n>` / `--protocolMaxLineBytes <bytes>`：协议输入上限（DoS 防护；与 protocol-netty decoder 对齐）
- `--databases <n>`：逻辑 DB 数量（`SELECT 0..n-1`；默认 16）
- `--maxmemoryScope global|per-db`：maxmemory 预算口径（`global` 更贴近 Redis；`per-db` 为兼容模式）
- `--transactionQueueMaxCommands <n>` / `--transactionQueueMaxBytes <bytes>`：事务队列硬上限（连接级，避免 OOM）
- 启动参数错误（解析失败/校验失败）会输出错误信息 + usage，并使用稳定退出码（exit=2），便于脚本集成与排障

#### Scenario: 多 worker I/O + 单线程执行
条件：`--ioThreads > 1` 且多个连接并发请求
- 预期：命令仍由同一个执行器线程串行执行（`DefaultEventExecutorGroup(1)`）
- 预期：DB 仅绑定到执行器线程；I/O 线程不直接访问 DB（避免线程安全问题）

#### Scenario: 执行队列满（背压）
条件：全局执行队列达到 `--executorQueueCapacity`
- 预期：服务端立即返回 `ok=false` 的 `ERR busy <reason>`，避免请求无界堆积导致 OOM/延迟雪崩

#### Diagnostic: busy 原因码与 STATS 计数器映射（排障）

当投递被拒绝时，server 会返回 `ok=false` 的 `ERR busy <reason>`；同时 `STATS` 会输出对应的全局计数器，便于定位主因：

| busy reason | 含义（典型） | `STATS` 计数器 |
|------------|--------------|----------------|
| `not_running` | 执行器未启动/正在关闭 | `submit_rejected_not_running_total` |
| `queue_full` | 全局队列已满（条数） | `submit_rejected_queue_full_total` |
| `bytes_budget` | 全局 queued-bytes 预算耗尽 | `submit_rejected_bytes_budget_total` |
| `offer_failed` | 入队失败（通常是竞态/关闭路径） | `submit_rejected_offer_failed_total` |

补充：
- `INFO`（JSON string 形态）只会输出 `yierdis_queued_tasks/yierdis_queued_bytes` 的即时快照；若需要“拒绝原因”与累计值，请使用 `STATS`。
- `STATS` 还会输出连接级 `conn_commands_rejected`（总拒绝次数，不区分原因），用于判断是否为单连接热点导致。

示例（排障）：
- 当客户端看到 `ERR busy queue_full`：执行 `STATS`，观察 `submit_rejected_queue_full_total` 是否持续增长（表明全局队列条数是主因）。
- 若只需要查看“当前 backlog 即时快照”：执行 `INFO stats`，观察 `yierdis_queued_tasks/yierdis_queued_bytes`。

### Requirement: 慢命令治理（KEYS / 全表扫描隔离）
**Module:** yierdis-core-command（SSOT） + yierdis-server（配置注入）

`KEYS` 属于“潜在全表扫描”命令：在大数据集/rehash/过期清理叠加时，容易长时间占用 executor，导致整体 tail latency 飙升甚至触发 backpressure。

本项目的治理策略是“保守预算 + 结果截断 + 推荐 SCAN”：
- core 提供 `SlowCommandGovernor` 作为治理 SSOT：定义 `KEYS` 的时间预算与结果上限（两者都可配置）。
- server 仅负责将启动参数注入到 command processor（embedded 场景也可直接注入自定义 governor）。
- 语义选择：当预算耗尽或结果超过上限时，**返回已收集的部分结果（可能被截断）**，不再直接报错（提升脚本/客户端的兼容直觉）；若需要可证明的完整遍历，请使用 `SCAN`。

#### Configuration: 相关启动参数
- `--keysTimeBudgetMillis <ms>`：`KEYS` 的时间预算（0 表示不限制；默认 20ms）
- `--keysMaxResults <n>`：`KEYS` 结果上限（0 表示禁用 `KEYS`；默认无限制）

#### Scenario: KEYS 超时/超量时返回部分结果
- 条件：大数据集下执行 `KEYS *`
- 预期：当超出 `--keysTimeBudgetMillis` 时仍返回 bulk string array（可能为空/不完整），不再返回错误
- 预期：当达到 `--keysMaxResults` 但扫描未结束时返回前 N 个匹配 key（不完整），不再返回错误

### Requirement: 优雅关停（Graceful Shutdown）
**Module:** server

服务端关闭时需避免竞态：
- 不出现“执行器仍在处理命令，但 DB/off-heap 已关闭”的情况
- backlog 中的命令在关闭时可被 drain（释放 retained bytes/资源），避免泄漏
- DB/instance 的 `close()/shutdown()` 固定在执行器线程内执行（保持 owner-thread 语义）

## Dependencies

- `yierdis-protocol-netty`（Netty codec/adapters；通过其依赖引入 `yierdis-protocol-codec` / `yierdis-protocol-model`）
- `yierdis-core-runtime`
- `yierdis-executor-core`（Netty-free 执行器调度/背压决策内核）
- `yierdis-args`

## Change History

- 2026-01-03：补充协议错误的连接生命周期处理约定（返回 protocol error；必要时断连）。
- 2026-01-04：引入单线程 `CommandExecutor` 解耦 I/O 与执行，并增加队列上限与 `ERR busy` 背压策略。
- 2026-01-08：执行模型升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：flush 合并 + 连接级 `autoRead` 背压闭环。
- 2026-01-16：执行器加固：引入 backlog bytes 预算（`Command.retainedBytes()` 口径）与滞回反压（与条数阈值并存），并提供连接级公平调度。
- 2026-01-16：执行模型硬化：DB owner-thread 语义 fail-fast；server 侧仅保留“走执行器”的 handler 入口，避免绕过 executor 直接访问 DB。
- 2026-01-16：连接生命周期收敛：`QUIT` 纳入 core 命令；执行器支持 close-after-reply，并在 QUIT 后丢弃该连接后续 backlog 命令（仅回收，不执行）。
- 2026-01-17：server 装配与边界收敛：Pipeline 装配下沉到 `YierdisServerChannelInitializer`；连接态收敛为 server 私有 `ServerConnectionState`；执行器调度 state 下沉为 `NettyExecutorChannelState`。
- 2026-02-01：多 DB 支持：新增 `--databases`，bootstrap 装配 DB0..N-1 并按连接态路由；INFO 输出为 JSON string，保留 `INFO YIERDIS`/`STATS` 结构化指标；连接关闭语义加固：连接关闭/协议错误会标记 closing，执行器跳过该连接后续 backlog（仅回收不执行），避免副作用与资源浪费。
- 2026-02-05：兼容性语义调整：`KEYS` 在时间预算/结果上限触达时改为返回部分结果（不再 fail-fast 抛错）；Custom Protocol v1 request 解码为严格 schema + 可恢复 resync（返回 error 并尽量继续读下一帧）。
- 2026-02-04：bootstrap 装配收敛：server 改为使用 core 的 `YierdisInstance` 统一 DB/路由/生命周期装配语义，为 bench/工具/嵌入式用法提供可复用基座。
- 2026-02-09：协议边界收敛：引入 `ReplyWriterFactory` 注入点，执行器/handler 不再直接 new 具体协议 writer；flush coalescing 抽离为独立组件以降低执行器维护复杂度。
- 2026-02-24：executor-core/连接态重构：排队/预算/调度/背压 tracking 迁移到 Netty-free `yierdis-executor-core`；连接态从 `ServerConnectionState` 拆分为 `ServerSessionState` + `ServerRuntimeState`（分别绑定 `Channel.attr`），并补齐 internal error→closing→skip-side-effects 回归测试。
