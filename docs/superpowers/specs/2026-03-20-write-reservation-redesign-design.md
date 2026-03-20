# Write Reservation Redesign Design

**Date:** 2026-03-20

## Goal

彻底移除命令层对 write reservation / maxmemory 预留协议的感知，把预算判断、reservation、commit、rollback 全部收回 DB 内部，并以新的公开契约表达“只读能力”和“语义化写能力”。

## Problem Summary

当前设计里，写入路径同时暴露了两层协议：

1. `core-command` 先在命令层估算 `extra bytes`，再显式调用 `engine.eviction().prepareWrite(...)`。
2. 具体 mutation 在 DB/value 实现里自行 `commitWrite(...)` / `rollbackWrite(...)`。
3. `YierdisFastCommandProcessor` 还需要在 `finally` 中执行 `rollbackWriteReservationIfAny()` 做全局兜底。

这导致写入边界没有被真正封装：

- 命令层必须知道 maxmemory reservation 生命周期。
- 写命令必须遵守一套隐式协议，否则可能把脏 reservation 泄漏到后续命令。
- `DbEngine` 的公开契约把基础设施细节暴露给上层，而不是只暴露业务语义。
- 读写都挂在 `values()` 下，强化了“命令层直接摸到底层 mutation API”的耦合模式。

## Scope

### In Scope

- 对 `yierdis-core-api` 做 breaking change，重塑 DB 能力边界。
- 移除 `DbEngine.values()` 和 `DbEngine.eviction()` 这类旧入口。
- 用新的 `DbReads` / `DbWrites` 公开契约替代当前读写混合接口。
- 让所有写命令一次性迁移到新的语义化写 API。
- 删除命令层中的 `prepareWrite(...)` 调用、预算估算逻辑和 processor `finally` reservation 兜底。
- 在 `YierdisDb` 内部引入统一 mutation executor，收敛 reservation、commit、rollback、OOM / noeviction 处理。
- 补行为回归测试和架构护栏，确保旧协议不会回流。

### Out of Scope

- `Session` / `ServerSession` / `Channel.attr(...)` 连接态模型重构。
- Netty executor 调度结构重构。
- 协议格式改动。
- 为兼容旧 `core-api` 保留长期适配层。

## Non-Goals

- 不追求兼容旧的 `core-api` 调用方式。
- 不把这次重构限制为“内部重命名”或“最小修补”。
- 不保留命令层提供 `estimated bytes hint` 的机制。

## Design Overview

### 1. Public API Split: `DbReads` and `DbWrites`

`DbEngine` 从当前的能力拼装形式：

- `values()`
- `eviction()`
- `keyspace()`
- `ttl()`

改为显式的读写分离：

- `reads()`
- `writes()`
- `expiration()`
- `memory()`
- `lifecycle()`

新的设计目标是：

- 命令层中的读命令只能依赖 `reads()`
- 命令层中的写命令只能依赖 `writes()`
- maxmemory / reservation 不再是 command-facing capability

### 2. Remove Old Mixed Read/Write Interfaces

下面这些旧公开契约直接退场，而不是保留长期兼容层：

- `ValueOps`
- `EvictionCoordinator`
- `StringOps`
- `HashOps`
- `ListOps`
- `SetOps`
- `ZSetOps`
- `HllOps`
- 读写混合版 `KeyspaceOps`
- 读写混合版 `TtlOps`

替代方案是成对引入新的接口：

- `StringReadOps` / `StringWriteOps`
- `HashReadOps` / `HashWriteOps`
- `ListReadOps` / `ListWriteOps`
- `SetReadOps` / `SetWriteOps`
- `ZSetReadOps` / `ZSetWriteOps`
- `HllReadOps` / `HllWriteOps`
- `KeyspaceReadOps` / `KeyspaceWriteOps`
- `TtlReadOps` / `TtlWriteOps`

`DbLifecycleOps` 保留独立边界，`FLUSHDB` 不并入 `DbWrites`。此外，当前由 runtime maintenance 通过
`engine.eviction().enforceMaxmemory()` 触发的后台 maxmemory enforcement，将迁移到 `DbLifecycleOps`
这一类管理边界，例如 `lifecycle().enforceMaxmemoryMaintenance()`，避免在删除 `eviction()` 后让 runtime
失去明确入口。

### 3. Semantic Writes Instead of Infrastructure Protocol

新的写 API 直接表达业务语义，而不是表达底层 reservation 协议。例如：

- string 写入口直接表达 `SET` / `APPEND` / `SETBIT` / `INCRBY`
- list 写入口直接表达 `LPUSH` / `RPUSH`
- hash 写入口直接表达 `HSET` / `HDEL`
- keyspace 写入口直接表达 `DEL`
- ttl 写入口直接表达 `EXPIRE` / `PERSIST`

命令层不再负责：

- 判断是否是 growth write
- 估算额外字节
- 显式 prepare / rollback
- 为预算服务做额外 DB 预读

### 4. Internal Mutation Executor

`YierdisDb` 内部引入统一 mutation executor，作为所有写入路径唯一允许的 reservation 生命周期入口。

这个执行器负责：

- 写前预算判断
- reservation 获取
- 具体 mutation 执行
- 成功路径 `commit`
- 异常路径 `rollback`
- `noeviction`、OOM、淘汰失败等异常的统一收敛

结构型 mutation 逻辑依然分布在 string/list/hash/set/zset/hll/keyspace/ttl 对应实现中，但这些实现降级为私有 mutator：

- 它们只负责结构语义和实际 delta 计算
- 它们不再直接调用 `commitWrite(...)` / `rollbackWrite(...)`
- 它们不再假设调用者已经提前完成 reservation

### 5. Reservation State Becomes Strictly Internal

`activeReservation`、`commitWrite(...)`、`rollbackWrite(...)` 不再是 DB 内部任何 mutation 都可随意调用的通用 helper。

目标状态是：

- reservation 状态只由 mutation executor 持有和结束
- command 层、processor 层、公开 API 都拿不到 reservation 生命周期手柄
- 一条命令是否成功结束，由 DB 内部写边界保证“不泄漏脏 reservation”

## Command Migration

所有写命令一次性迁移，不保留旧路径：

### String

- `SET`
- `APPEND`
- `SETBIT`
- `INCR`
- `DECR`

`SET` 相关的 `NX` / `XX` / `GET` / `KEEPTTL` / `EX` / `PX` / `EXAT` / `PXAT` 组合判断全部下沉进新的 string write API。

### Hash

- `HSET`
- `HDEL`

### List

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`

### Set

- `SADD`
- `SREM`

### ZSet

- `ZADD`
- `ZREM`
- `ZREMRANGEBYRANK`
- `ZREMRANGEBYSCORE`

### HLL

- `PFADD`
- `PFMERGE`

### Keyspace / TTL

- `DEL`
- `EXPIRE`
- `PEXPIRE`
- `EXPIREAT`
- `PEXPIREAT`
- `PERSIST`

### Lifecycle

- `FLUSHDB` 继续走 `DbLifecycleOps`
- runtime 后台 maintenance 的 maxmemory enforcement 也继续走 `DbLifecycleOps`，而不是 `DbWrites`
  或任何 command-facing 能力

## Allowed Behavioral Corrections

这次重构允许对旧 reservation 机制带来的不一致行为进行纠偏，但必须满足两个约束：

1. 对外仍保持同一协议和同一命令集合。
2. 纠偏后的行为必须更一致、更可解释，不能引入新的“成功写入后再 OOM”或“命令异常后影响下一条命令”这类问题。

重点允许纠偏的方向：

- 不再依赖 processor `finally` 兜底来保证 reservation 清理
- 把同类写命令的 OOM / noeviction 行为收敛为同一模型
- 把 `SET`、`SETBIT`、`PFMERGE`、TTL 写入等混合语义路径的预算处理统一进 DB 内部

## Testing Strategy

### Core DB

新增围绕统一 mutation executor 的回归测试，覆盖：

- 成功提交会更新 ledger / memory accounting
- mutation 抛异常时 reservation 自动回滚
- `noeviction` / OOM 在 mutation 边界统一失败
- TTL 相关 mutation 不泄漏 reservation
- `DEL` / `PERSIST` / `PFMERGE` / `SETBIT` 这类边角写路径
- 命令失败后下一条写命令不会继承脏 reservation

### Core Command

命令测试继续验证语义，但需要确保：

- 命令实现不再调用 `prepareWrite(...)`
- `SET` 组合路径通过新的语义化写 API 仍然正确
- 读命令只依赖 `reads()`
- 写命令只依赖 `writes()`

### Integration

保留并继续运行现有 runtime / server / client 集成测试，确保新的 DB 写边界经过 server/bootstrap 后仍然成立。

## Architecture Guardrails

新增或加强这些护栏：

- `core-command` 禁止再 import / 调用：
  - `EvictionCoordinator`
  - `prepareWrite`
  - `rollbackWriteReservationIfAny`
  - 任何新的 mutation executor 私有实现
- `core-api` 禁止保留旧入口：
  - `DbEngine.values()`
  - `DbEngine.eviction()`
  - `ValueOps`
  - 旧的混合 `StringOps/HashOps/ListOps/SetOps/ZSetOps/HllOps`
- `YierdisFastCommandProcessor` 禁止再出现 reservation 兜底 `finally`
- `core-command` 中不应再出现仅为预算估算服务的 `DbMemoryConstants` 使用

## Risks and Tradeoffs

### Pros

- 写入边界真正收敛成单点机制
- 命令层彻底摆脱 reservation 协议
- `core-api` 的读写边界更清晰
- 后续再做 connection context 重构时，不会继续和 maxmemory reservation 协议纠缠

### Cons

- 这是一次明确的 breaking change
- `core-api`、`core-db`、`core-command` 会有大面积迁移
- `SET` / TTL / bit operations / HLL 这类混合语义路径需要更细的迁移测试

## Recommended Delivery Strategy

虽然实现范围大，但这个子项目本身仍然是单一主题：`write reservation` 彻底下沉与读写契约重塑。它应该通过一个单独 implementation plan 落地，并在该计划中分任务处理：

1. `core-api` 契约重塑
2. `core-db` mutation executor 落地
3. 所有写命令迁移
4. 读命令切换到 `reads()`
5. 架构护栏与回归测试补齐

## Follow-Up

`connection context` 重构保留为独立后续子项目，不与本设计混做一个 plan。
