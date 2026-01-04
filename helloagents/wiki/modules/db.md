# db

## Purpose

实现内存数据结构、编码策略、TTL/过期清理、内存估算与淘汰策略。

## Module Overview

- **Responsibility:** Keyspace + 过期索引 + 值编码（string/list/set/hash/zset）+ maxmemory
- **Status:** ✅Stable
- **Last Updated:** 2026-01-04

## Specifications

### Requirement: 二进制安全 Keyspace
**Module:** db
key 以 `byte[]` 存储并按内容比较，支持增量 rehash 以减少延迟抖动。

#### Scenario: key 复用与 canonicalKey
条件：调用方以 `BytesView` 或不同 `byte[]` 传入同内容 key
- 预期：能找到同一条 key，并可获得 canonical key（避免重复分配）

### Requirement: TTL 惰性删除
**Module:** db
访问 key 时检查是否过期，过期则删除并返回“不存在”语义。

#### Scenario: GET/TYPE/EXISTS 触发删除
条件：key 已过期
- 预期：读取类命令返回 key 不存在；并清理过期索引与存储对象

### Requirement: maxmemory 统计口径可解释（heap + off-heap）
**Module:** db
`maxmemoryBytes` 的触发依据统一为“heap 侧元数据估算 + off-heap allocator 实占”的总和，避免 off-heap 模式下漏计或明显双计数。

#### Scenario: off-heap 启用时触顶行为可预测
条件：启用 off-heap 并持续写入直至达到 `maxmemoryBytes`
- 预期：淘汰/拒写触发时机可解释（与 allocator.usedBytes 变化趋势一致）
- 预期：当字符串 payload 存放在 off-heap 时，heap 估算不重复计入该 payload 长度（避免双计数）

## Dependencies

- offheap（可选）

## Change History

- 2026-01-04：统一 maxmemory 统计口径（heap 估算 + off-heap usedBytes），并补齐相关回归测试与泄漏验证。
