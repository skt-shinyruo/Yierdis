# db

## Purpose

实现内存数据结构、编码策略、TTL/过期清理、内存估算与淘汰策略。

## Module Overview

- **Responsibility:** Keyspace + 过期索引 + 值编码（string/list/set/hash/zset）+ maxmemory
- **Status:** ✅Stable
- **Last Updated:** 2026-01-01

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

## Dependencies

- offheap（可选）

## Change History

- （暂无）

