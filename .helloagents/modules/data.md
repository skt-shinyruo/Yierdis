<!-- migrated_from: wiki/data.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 数据模型与内部结构

## 1. 顶层 Keyspace

- Key 以 **二进制安全** 的 `byte[]` 表示，按内容比较（而非引用）。
- 顶层 keyspace 使用开地址哈希表，并带 **增量 rehash**，尽量避免 rehash 带来的延迟尖刺。

## 2. 值对象（Redis 风格）

键空间 value 存储为 `YierdisObject`（逻辑类型 + 内部编码 + payload），逻辑类型目前包括：

- `STRING`
- `LIST`
- `SET`
- `HASH`
- `ZSET`

### 2.1 STRING 编码

- `STRING_INT`：可解析为整数的字符串，使用 `long` 存储并按需序列化
- `STRING_EMBSTR`：小字符串（教学近似）
- `STRING_RAW`：一般字符串（可扩容）

### 2.2 复合类型编码（教学近似）

- `HASH_PACKED` / `HASH_HT`
- `LIST_PACKED` / `LIST_QUICKLIST`
- `SET_INTSET` / `SET_HT`
- `ZSET_PACKED` / `ZSET_SKIPLIST`

## 3. TTL 与过期索引

- TTL 采用“访问时惰性删除”，并可由 Netty event loop 定期触发后台清理（可配置关闭）。
- 过期索引与 keyspace 解耦，删除 key 时需同步清理对应过期项。

## 4. 内存与淘汰（教学简化）

- 支持 `maxmemoryBytes` + `maxmemoryPolicy`（`noeviction` / `allkeys-random` / `allkeys-lru`）。
- LRU 为采样近似，且仅在配置启用时更新 `lruClock`。

## 5. Off-heap（可选）

- 通过 `yierdis-offheap-*` 模块提供堆外内存抽象与后端实现。
- 启用 off-heap 后，部分字符串值与索引结构会迁移到堆外以降低 GC 压力并演示不同实现策略。
- 后端类型（由启动参数选择）：`none` / `netty` / `unsafe` / `foreign`（不同后端强调的目标不同：易用性/性能/教学演示）。
- 内存口径建议以 `heap 估算 + allocator.usedBytes()` 为统一观测锚点，并在压测/回归测试中用 `usedBytes` 作为“泄漏回归”的硬指标。
