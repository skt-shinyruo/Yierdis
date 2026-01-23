# command

## Purpose

负责命令路由、参数校验、调用 DB，并按连接协商的协议版本输出 RESP2/RESP3 回复（最小子集）。

归属：`yierdis-core`（`yier.bubu.redis.command.*`），作为命令语义 SSOT；`yierdis-server` 仅负责 Netty 适配与调度。

## Module Overview

- **Responsibility:** 命令分发、参数解析、错误映射、性能优化（低分配写出路径）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-23

## Specifications

### Requirement: 命令执行路径收敛（SSOT）
**Module:** command
同一条命令仅保留单一权威实现：以写出式 `YierdisFastCommandProcessor` 为 SSOT，避免双实现带来的行为漂移。

#### Scenario: 参数错误与类型错误
条件：用户传入错误参数或对错误类型 key 操作
- 预期：返回 `ERR ...` 或 `WRONGTYPE ...`，并尽量与 Redis 的错误风格一致
  - 说明：当连接处于 RESP3 时，nil 使用 RESP3 null（`_`）返回；其余基础类型尽量保持 RESP2 可解析子集

#### Scenario: null bulk string 参数（`$-1`）
条件：客户端发送包含 `$-1`（null bulk string）的命令参数
- 预期：除 `PING/ECHO` 的单参数消息外，其余命令统一返回 `ERR Protocol error: null bulk string`（避免 NPE/断线）
- 说明：该规则主要用于保证服务端鲁棒性与错误可观测性，不改变正常 `redis-cli` 使用路径

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
- 写命令统一走 `db.prepareWrite(estimatedExtraBytes)` 做 preflight（含 cleanupExpired + 预淘汰/预检查）
- 写入执行后仍调用 `db.enforceMaxmemory()`，但必须在写 reply 之前完成（确保错误只会产生单条 reply）

#### Scenario: maxmemory 触发错误时不产生双 reply
条件：写命令在 maxmemory 压力下触发 `-ERR OOM ...`
- 预期：客户端只收到 **单条** error reply（不会出现 reply 拼接/response splitting）

### Requirement: RESP3 友好集合回复（map/set）
**Module:** command/protocol
当连接协议为 RESP3（`HELLO 3`）时：
- `HGETALL` 输出 RESP3 map（field -> value）
- `MEMORY STATS` 输出 RESP3 map（key -> value；字段集合保持稳定）
- `SMEMBERS` 输出 RESP3 set

## Dependencies

- 外部：`yierdis-protocol`（`RespCommand`/`RespWriter` 等）
- 内部：`yierdis-core` 的 `db` 包（同模块内部依赖）

## Change History

- 2026-01-03：补充 `$-1`（null bulk string）参数的统一校验与错误输出约定。
- 2026-01-04：移除对象式 `CommandProcessor` 线上路径，收敛为单一写出式实现，并引入 command table 驱动路由。
- 2026-01-07：支持 `HELLO 3` 切换 RESP3（最小子集），并在 RESP3 模式下按协议输出 null（`_`）。
- 2026-01-08：写命令热路径减少不必要的 `toByteArray`：支持从 `RespCommand.frame()` 的参数 slice 直接写入/追加到最终 payload（heap/off-heap）。
- 2026-01-14：新增 BITMAP/HLL 命令族（`SETBIT/GETBIT/BITCOUNT/PFADD/PFCOUNT/PFMERGE`），并补齐相关参数校验与 `maxmemory` 接入。
- 2026-01-17：命令路由加速：`CommandRegistry` 从线性扫描升级为 O(1) 哈希索引；新增 `INFO/STATS` 作为可观测性入口（输出由 server 注入 provider）。
- 2026-01-23：写命令统一引入 write preflight（`prepareWrite`）并调整顺序为 preflight→执行→enforce→reply（避免双 reply）；RESP3 下 `HGETALL/MEMORY STATS/SMEMBERS` 改为 map/set；`KEYS` glob 兼容范围补齐（`[]`/否定/范围/转义）。
