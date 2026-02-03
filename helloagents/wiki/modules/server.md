# server

## Purpose

负责 Netty 服务端启动、管线组装与连接生命周期管理。

## Module Overview

- **Responsibility:** 端口监听、Pipeline 组装、定时任务（如 TTL 清理）的调度入口
- **Status:** ✅Stable
- **Last Updated:** 2026-02-02

## Specifications

### Requirement: RESP2/RESP3 TCP 服务端（含 inline command，调试用）
**Module:** server
启动一个基于 Netty 的 TCP 服务端，支持 Redis 风格命令请求与响应写回：默认 RESP2，并支持最小 RESP3（`HELLO 3` 协商后切换），同时支持 inline command（便于调试，支持引号/转义/`\\xHH`）。

#### Scenario: 基本连通性
条件：服务端启动并监听端口
- 预期：`redis-cli --resp2` 可连接并执行 `PING` 得到响应
- 预期：`redis-cli --resp3` 在执行 `HELLO 3` 后可继续执行基础命令
- 预期：`nc/telnet` 可通过 inline `PING\\r\\n` / `ECHO \"hello world\"\\r\\n` 做最小交互调试

#### Scenario: 协议错误（非法 RESP）
条件：客户端发送非法请求（解码阶段抛出 `Protocol error: ...`）
- 预期：服务端返回 `ERR Protocol error: ...` 并关闭连接
- 说明：连接关闭是“协议层错误”的默认策略，避免解码状态不一致影响后续请求；同时 server 会将该连接标记为 closing，确保已入队 backlog 命令不会继续执行产生副作用（仅回收 frame/预算）

### Requirement: 多 DB（DB0..N-1）与连接级 DB 路由
**Module:** server
为提升 Redis 生态兼容性，server 支持多逻辑 DB：
- 通过 `--databases <n>` 配置 DB 数量（默认 16）
- 连接级维护 `dbIndex`（默认 0），由 `SELECT <index>` 修改
- 执行器线程绑定多个 `YierdisDb` 实例（DB0..N-1），命令执行时按连接态路由到目标 DB

说明：多 DB 仍保持“单线程命令语义”（所有 DB 绑定同一 executor 线程），避免引入跨线程并发复杂度。

### Requirement: I/O 与命令执行解耦（单线程命令语义）
**Module:** yierdis-server
执行模型已升级为 Netty 体系内的单线程执行器（`DefaultEventExecutorGroup(1)` + `NettyCommandExecutor`）：
- 目标：保持 Redis 风格“全局单线程命令语义”，同时减少 per-command `flush` 并引入连接级背压闭环
- 执行：I/O 线程负责解码与投递；命令在执行器线程串行执行；同一轮 drain 内对同一连接 `write` 聚合并在末尾 `flush`
- 背压：采用“双约束”：per-connection pending **条数** + pending **bytes** 两套水位线（带滞回阈值 high/low），避免“少量大包积压”导致内存驻留不可解释
- 公平性：支持连接级公平调度（per-channel queue + round-robin），避免热点连接长期挤占全局 backlog（可配置）
- 连接关闭语义：`QUIT` 由命令层请求 close-after-reply，执行器在 flush 后关闭连接，并跳过该连接后续已入队命令（仅回收，不执行 DB），保证 pipeline 顺序与无副作用
- 连接态二分：`ConnectionContext`（protocol-netty）仅承载连接级协议会话（RESP2/RESP3，`Channel.attr` 绑定）；`ServerConnectionState`（server 私有）承载 pending/backpressure/closing 与低开销统计；执行器调度 state（per-channel queue + scheduled 标志）收敛到 server 私有 `NettyExecutorChannelState`（`Channel.attr` 绑定），避免 protocol 模块携带 server 语义与调度细节
- 可观测性：提供 `INFO`/`STATS` 命令输出执行器/连接级统计摘要（队列、背压 enter/exit、reject、drain budget、close-after-reply 等），用于排障与容量评估
  - `INFO`：Redis 兼容 bulk string（文本分节）
  - `INFO YIERDIS` / `STATS`：保留结构化输出（RESP2 array / RESP3 map），用于教学与排障

#### Scenario: 高压 pipeline 下的 flush 合并与背压恢复
- 当 backlog ≥ high watermark：服务端对该连接 `autoRead=false`，并可能返回 `-ERR busy <reason>`
- 当 backlog ≤ low watermark：服务端恢复 `autoRead=true`，连接继续读入并保持响应顺序一致

#### Configuration: 相关启动参数
- `--executorQueueCapacity <n>`：全局执行队列容量（有界）
- `--executorQueueMaxBytes <bytes>`：全局执行队列 bytes 上限（0 表示禁用；用于防止大 bulk 积压）
- `--executorSchedulingPolicy global|fair`：执行队列调度策略（全局 FIFO / 连接级公平）
- `--frameCompactionThresholdBytes <bytes>`：当单个 frame 驻留体积显著大于其逻辑长度时，允许 compact（0 禁用）
- `--frameCompactionRatio <n>`：compaction 触发比率（retainedBytes / length，默认 2.0）
- `--frameCompactionMaxCopyBytes <bytes>`：compaction 单次最大拷贝上限（避免对大 payload 复制）
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
- 预期：服务端立即返回 `-ERR busy <reason>`，避免请求无界堆积导致 OOM/延迟雪崩

#### Diagnostic: busy 原因码与 STATS 计数器映射（排障）

当投递被拒绝时，server 会返回 `-ERR busy <reason>`；同时 `STATS` 会输出对应的全局计数器，便于定位主因：

| busy reason | 含义（典型） | `STATS` 计数器 |
|------------|--------------|----------------|
| `not_running` | 执行器未启动/正在关闭 | `submit_rejected_not_running_total` |
| `queue_full` | 全局队列已满（条数） | `submit_rejected_queue_full_total` |
| `bytes_budget` | 全局 queued-bytes 预算耗尽 | `submit_rejected_bytes_budget_total` |
| `offer_failed` | 入队失败（通常是竞态/关闭路径） | `submit_rejected_offer_failed_total` |

补充：
- `INFO`（Redis bulk string 形态）只会输出 `yierdis_queued_tasks/yierdis_queued_bytes` 的即时快照；若需要“拒绝原因”与累计值，请使用 `STATS`。
- `STATS` 还会输出连接级 `conn_commands_rejected`（总拒绝次数，不区分原因），用于判断是否为单连接热点导致。

示例（redis-cli 排障）：
- 当客户端看到 `-ERR busy queue_full`：执行 `redis-cli -p 6378 --resp3 STATS`，观察 `submit_rejected_queue_full_total` 是否持续增长（表明全局队列条数是主因）。
- 若只需要查看“当前 backlog 即时快照”：执行 `redis-cli -p 6378 INFO stats`，观察 `yierdis_queued_tasks/yierdis_queued_bytes`。

### Requirement: 优雅关停（Graceful Shutdown）
**Module:** server

服务端关闭时需避免竞态：
- 不出现“执行器仍在处理命令，但 DB/off-heap 已关闭”的情况
- backlog 中的命令在关闭时可被 drain（释放 `RespFrame/ByteBuf`），避免泄漏
- DB 的 `shutdown()` 固定在执行器线程内执行（保持 owner-thread 语义）

## Dependencies

- `yierdis-protocol-netty`（Netty codec/adapters；`yierdis-protocol` 由其传递依赖提供）
- `yierdis-core`
- `yierdis-args`

## Change History

- 2026-01-03：补充协议错误的连接生命周期处理约定（返回 `ERR` 并关闭连接）。
- 2026-01-04：引入单线程 `CommandExecutor` 解耦 I/O 与执行，并增加队列上限与 `ERR busy` 背压策略。
- 2026-01-07：补充 RESP3（`HELLO 3` 协商）与 inline command（调试用）支持，提高常见客户端兼容性。
- 2026-01-08：执行模型升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：flush 合并 + 连接级 `autoRead` 背压闭环。
- 2026-01-08：inline command 解析增强：支持单/双引号、反斜杠转义与 `\\xHH` 十六进制转义。
- 2026-01-15：依赖切换：RESP codec 下沉到 `yierdis-protocol-netty`；`RespWriter` 写出路径改为 bytes sink + session，降低协议层与 Netty 的耦合。
- 2026-01-16：执行器加固：引入 backlog bytes 预算（`RespFrame.retainedBytes()` 口径）与滞回反压（与条数阈值并存），并提供可配置 frame compaction 与连接级公平调度。
- 2026-01-16：执行模型硬化：DB owner-thread 语义 fail-fast；server 侧仅保留“走执行器”的 handler 入口，避免绕过 executor 直接访问 DB。
- 2026-01-16：连接生命周期收敛：`QUIT` 纳入 core 命令；执行器支持 close-after-reply，并在 QUIT 后丢弃该连接后续 backlog 命令（仅回收，不执行）。
- 2026-01-17：server 装配与边界收敛：Pipeline 装配下沉到 `YierdisServerChannelInitializer`；连接态二分（`ConnectionContext` 仅协议会话，运行时连接状态迁移到 `ServerConnectionState`）；执行器调度 state 下沉为 server 私有 `NettyExecutorChannelState`；协议 request 解码严格化（reply/非法前缀判为 protocol error 并关闭连接）。
- 2026-02-01：多 DB 支持：新增 `--databases`，bootstrap 装配 DB0..N-1 并按连接态路由；INFO 形态对齐 Redis（bulk string），保留 `INFO YIERDIS`/`STATS` 结构化指标；连接关闭语义加固：连接关闭/协议错误会标记 closing，执行器跳过该连接后续 backlog（仅回收不执行），避免副作用与资源浪费。
