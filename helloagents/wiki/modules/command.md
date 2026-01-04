# command

## Purpose

负责命令路由、参数校验、调用 DB 并输出 RESP2 回复。

## Module Overview

- **Responsibility:** 命令分发、参数解析、错误映射、性能优化（低分配写出路径）
- **Status:** ✅Stable
- **Last Updated:** 2026-01-04

## Specifications

### Requirement: 命令执行路径收敛（SSOT）
**Module:** command
同一条命令仅保留单一权威实现：以写出式 `YierdisFastCommandProcessor` 为 SSOT，避免双实现带来的行为漂移。

#### Scenario: 参数错误与类型错误
条件：用户传入错误参数或对错误类型 key 操作
- 预期：返回 `ERR ...` 或 `WRONGTYPE ...`，并尽量与 Redis 的错误风格一致

#### Scenario: null bulk string 参数（`$-1`）
条件：客户端发送包含 `$-1`（null bulk string）的命令参数
- 预期：除 `PING/ECHO` 的单参数消息外，其余命令统一返回 `ERR Protocol error: null bulk string`（避免 NPE/断线）
- 说明：该规则主要用于保证服务端鲁棒性与错误可观测性，不改变正常 `redis-cli` 使用路径

### Requirement: 命令表驱动路由
**Module:** command
命令路由通过 command table（command name → handler）统一注册点完成，减少长 if/else 链，提高可维护性与可测试性。

## Dependencies

- protocol
- db

## Change History

- 2026-01-03：补充 `$-1`（null bulk string）参数的统一校验与错误输出约定。
- 2026-01-04：移除对象式 `CommandProcessor` 线上路径，收敛为单一写出式实现，并引入 command table 驱动路由。
