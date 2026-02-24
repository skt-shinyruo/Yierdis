# command

## Purpose

负责命令路由、参数校验、调用 DB engine 边界（`DbEngine`），并通过协议无关的 `ReplyWriter` 写出结果（由协议层编码为 Custom Protocol v1 的 NDJSON reply）。

归属：`yierdis-core-command`（`yier.bubu.redis.command.*`），作为命令语义 SSOT（`yierdis-core` 为迁移期聚合层）；`yierdis-server` 仅负责 Netty 适配与调度。
补充：为保持分层，命令层负责 reply 形状（array/map header、count 等）；value/db/off-heap 层通过 domain result（`BulkStringValue/BulkStringSequence/BulkStringMapPairs`）与 `BulkStringSink` 表达“可 streaming 的 bulk 值输出”，命令层通过 adapter 将其写入 `ReplyWriter`。
边界约束：命令层通过 `YierdisDbRouter`（依赖 `DbIndexProvider`）选择 `DbEngine`；路由的输入侧状态来自 `CommandContext.session()`，输出通过 `CommandContext.out()` 写回。

## Module Overview

- **Responsibility:** 命令分发、参数解析、错误映射、性能优化（低分配写出路径）
- **Status:** ✅Stable
- **Last Updated:** 2026-02-24

## Specifications

### Requirement: 命令执行路径收敛（SSOT）
**Module:** command
同一条命令仅保留单一权威实现：以写出式 `YierdisFastCommandProcessor` 为 SSOT，避免双实现带来的行为漂移。

#### Scenario: 参数错误与类型错误
条件：用户传入错误参数或对错误类型 key 操作
- 预期：返回 `ERR ...` 或 `WRONGTYPE ...`，并尽量与 Redis 的错误风格一致
  - 变更：整数解析错误统一为 Redis 风格（`ERR value is not an integer or out of range`），避免携带参数 label 导致生态工具/测试字符串匹配失败

#### Scenario: null 参数（Custom Protocol v1）
条件：客户端发送包含 null 参数的命令参数（`args` 元素为 `null`）
- 预期：除 `PING/ECHO` 的单参数消息外，其余命令统一返回 `ERR Protocol error: null bulk string`（避免 NPE/断线）
- 说明：该规则主要用于保证服务端鲁棒性与错误可观测性（错误可恢复；请求侧仍强约束 argv 形态）

### Requirement: 命令表驱动路由
**Module:** command
命令路由通过 `CommandRegistry`（command name → handler）统一注册点完成，并按 domain 拆分为多个 `*Commands`：
- `ServerCommands`：连接/通用命令（PING/ECHO/HELLO/INFO/STATS/SELECT/QUIT/FLUSHDB/COMMAND）
- `KeyCommands`：key/TTL/诊断命令（TYPE/MEMORY/OBJECT/KEYS/DEL/EXISTS/EXPIRE/TTL）
- `StringCommands` / `HllCommands` / `ListCommands` / `HashCommands` / `SetCommands` / `ZSetCommands`

目的：扩展新命令/新类型时局部改动，不再“牵一发动全身”。
补充：`CommandRegistry` 的查找已从线性扫描升级为开放寻址哈希索引（期望 O(1)），并保持运行时零分配。

### Requirement: BITMAP / HLL 命令族
**Module:** command
新增 BITMAP（`SETBIT/GETBIT/BITCOUNT`）与 HLL（`PFADD/PFCOUNT/PFMERGE`）命令族，并确保：
- 参数校验与错误输出风格与 Redis 尽量一致
- 写入命令接入 `maxmemory`（noeviction/eviction）防止不可控内存增长

### Requirement: 写命令 reply 顺序与 maxmemory 语义（避免双 reply）
**Module:** command/db
写命令必须保证：任何可能抛错的 maxmemory 逻辑都发生在写 reply 之前（避免同一命令输出“正常 reply + error reply”导致协议损坏）。

实现要点：
- 写命令统一走 `engine.eviction().prepareWrite(estimatedExtraBytes)` 做 preflight（含 cleanupExpired + 预淘汰/预检查）
- DB 写入实现必须在 mutate 成功/失败路径完成 reservation 的 commit/rollback；处理器在 finally 做防御性 rollback，避免“上一个命令泄漏 reservation 影响下一个命令”
- `enforceMaxmemory()` 仅作为**后台/周期性维护**的 best-effort 手段（server 侧维护 tick 触发），命令 handler 不再在写入后显式调用

#### Scenario: maxmemory 触发错误时不产生双 reply
条件：写命令在 maxmemory 压力下触发 `-ERR OOM ...`
- 预期：客户端只收到 **单条** error reply（不会出现 reply 拼接/response splitting）

### Requirement: 结构化集合回复（map/set）
**Module:** command/protocol
为降低“平铺 key/value array”带来的解析负担并提升可观测性，命令层对部分集合结果采用结构化写出：
- `HGETALL`：field -> value（Custom Protocol v1 下表现为 JSON object）
- `MEMORY STATS`：key -> value（字段集合保持稳定；数值字段使用 integer 类型；Custom Protocol v1 下表现为 JSON object）
- `SMEMBERS`：集合成员（Custom Protocol v1 下表现为 JSON array）
补充：`OBJECT ENCODING` 返回 string，缺失 key 返回 `null`。

### Requirement: Transaction（MULTI/EXEC/DISCARD）边界与上限（对齐 Redis 行为）
**Module:** command/server

事务实现为“最小子集”，但需要在 **资源安全** 与 **Redis 生态兼容性** 上对齐关键边界：

- 事务队列为连接级状态（MULTI 模式下入队，EXEC 重放执行）
- 事务队列必须是 **有界** 的：避免大事务/大参数在入队阶段导致 JVM OOM
- 当入队阶段发生错误（例如触达队列上限）时，后续 `EXEC` 必须返回 Redis 风格 `EXECABORT` 并丢弃事务队列（对齐 Redis “入队阶段出错 → EXEC 终止” 语义）
- MULTI 模式下禁止 `HELLO`（连接级信息命令）：作为护栏简化事务边界，避免混入连接级状态变更语义

相关 server 启动参数（硬上限）：
- `--transactionQueueMaxCommands <n>`：最大入队命令数（0 表示不限制）
- `--transactionQueueMaxBytes <bytes>`：最大入队参数 bytes（按入队拷贝估算；0 表示不限制）

已知限制（仍属于 out-of-scope）：
- 不支持 `WATCH/UNWATCH` 与乐观锁语义
- 不保证与 Redis 在所有边界行为上完全一致（以教学/可解释性为优先）

## Dependencies

- 外部：`yierdis-protocol-model`（`Command`/`CommandContext`/`ReplyWriter`/`Session`/`DbIndexProvider` 等协议无关接口）
- 内部：`yierdis-core-api` 的 `ops` 边界（`DbEngine`/`ValueOps`/`KeyspaceOps`/`TtlOps`/`MemoryOps` 等）

## Change History

- 2026-01-03：补充 null 参数的统一校验与错误输出约定。
- 2026-01-04：移除对象式 `CommandProcessor` 线上路径，收敛为单一写出式实现，并引入 command table 驱动路由。
- 2026-01-07：新增 `HELLO` 信息命令（输出 server/version/proto/mode/role 等），用于调试与诊断。
- 2026-01-08：写命令热路径减少不必要的 `toByteArray`：支持从 `Command.frame()` + `argOffset/len` 直接写入/追加到最终 payload（heap/off-heap）。
- 2026-01-14：新增 BITMAP/HLL 命令族（`SETBIT/GETBIT/BITCOUNT/PFADD/PFCOUNT/PFMERGE`），并补齐相关参数校验与 `maxmemory` 接入。
- 2026-01-17：命令路由加速：`CommandRegistry` 从线性扫描升级为 O(1) 哈希索引；新增 `INFO/STATS` 作为可观测性入口（输出由 server 注入 provider）。
- 2026-01-23：写命令统一引入 write preflight（`prepareWrite`）并调整顺序为 preflight→执行→enforce→reply（避免双 reply）；集合类命令（如 `HGETALL/MEMORY STATS/SMEMBERS`）按 map/set 形状写出；`KEYS` glob 兼容范围补齐（`[]`/否定/范围/转义）。
- 2026-02-01：多 DB 路由接入命令层（连接态维护 `dbIndex`）；`INFO` 输出形态对齐 Redis（bulk string，结构化指标迁移到 `INFO YIERDIS`/`STATS`）；`MEMORY STATS` 数值字段类型对齐为 integer；`OBJECT ENCODING` 回复类型对齐为 bulk string。
- 2026-02-03：事务护栏：MULTI 模式下禁止 `HELLO`，避免连接级命令干扰事务边界（触发 `EXECABORT` 并清理队列）。
- 2026-02-04：maxmemory 语义收敛：写命令不再在 handler 中调用 enforce（以 `ledger.reserve` 作为拒写点），server 维护 tick 做 best-effort enforce；`MEMORY STATS` 字段升级为 `ledger_*` 口径。
- 2026-02-06：对外协议切换：命令入口改为协议无关 `Command/ReplyWriter`；集合回复改为结构化写出（map/set）以便 Custom Protocol v1 直接映射为 JSON object/array；协议错误策略调整为可恢复（返回 error 并继续读下一帧）。
- 2026-02-09：命令层依赖收口：路由返回 `DbEngine`，命令实现移除对 `YierdisDb` 的直接依赖；写 preflight 通过 `DbEngine.eviction()` 统一访问。
- 2026-02-23：执行上下文边界收敛：引入 `CommandContext`，将路由/事务/可观测等输入侧状态从 `ReplyWriter` 迁移到 `CommandContext.session()`，并移除 `ReplyWriter.session()`。
