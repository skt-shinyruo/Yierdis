# db

## Purpose

实现内存数据结构、编码策略、TTL/过期清理、内存估算与淘汰策略。

归属：`yierdis-core`（`yier.bubu.redis.db.*`），作为数据结构与内存语义 SSOT。

## Module Overview

- **Responsibility:** Keyspace + 过期索引 + 值编码（string/list/set/hash/zset）+ maxmemory
- **Status:** ✅Stable
- **Last Updated:** 2026-01-23

## Specifications

### Requirement: DB 单线程语义硬化（fail-fast）
**Module:** db
`YierdisDb` 明确为 **非线程安全**，并通过 owner-thread 绑定 + fail-fast 机制将“约定”变为“硬约束”：
- 必须在唯一线程调用 `bindToCurrentThread()` 完成绑定（通常由 server executor 在 `start()` 阶段执行）
- 未绑定即访问、或跨线程访问：立即抛出异常（测试可覆盖），避免静默竞态/一致性风险

### Requirement: 二进制安全 Keyspace
**Module:** db
key 以 `byte[]` 存储并按内容比较，支持增量 rehash 以减少延迟抖动。

在 off-heap 启用时：
- 当 allocator 具备 `YierdisOffHeapAddressAllocator` capability 时，key bytes 与 expires 索引 **可选** 使用 off-heap（行为保持一致，差异仅体现在内存路径与性能）。
- 为降低 Unsafe/raw address 的误用风险：keys/expires 的 off-heap 路径默认关闭，需显式启用（server 参数 `--offheapKeysEnabled`，且仅允许 `--offheapBackend=unsafe`）。

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

为提升“为什么拒写/为什么淘汰”的可解释性，补充预算分解输出：
- `YierdisDb.memoryStats()`：输出 heap/off-heap/结构开销等分解估算
- `MEMORY STATS`：命令侧暴露预算分解（用于排障/教学；明确为估算）

#### Contract: `MEMORY STATS` 输出字段（稳定性约束）
`MEMORY STATS` 输出字段集合保持稳定，按连接协议版本返回不同容器类型：
- RESP2：输出为 **扁平 key/value 数组**（总元素数固定为 **34**，即 17 对 key/value）
- RESP3：输出为 **map**（总 pair 数固定为 **17**）

字段编码约束：
- key 均为 ASCII bulk string
- value 均为十进制 ASCII bulk string（布尔值用 `0/1` 表示）

字段含义（口径：估算/预算优先，可解释性优先）：
- `maxmemory_bytes`：配置的 maxmemory 上限（0 表示不启用淘汰/拒写）
- `used_bytes_for_maxmemory`：用于 maxmemory 判定的“统一口径已用 bytes”（heap 估算 + off-heap usedBytes 实占）
- `heap_data_bytes_estimate`：heap 侧数据与元数据的估算值（不等同于 JVM/GC 视角真实 heap）
- `offheap_used_bytes`：off-heap allocator 的实占 `usedBytes()`（可作为泄漏回归锚点）
- `keyspace_table_overhead_bytes_estimate`：keyspace hash table 结构开销估算
- `expire_table_overhead_bytes_estimate`：expire index hash table 结构开销估算
- `expire_value_objects_bytes_estimate`：TTL 元数据/对象估算（不含 key/value payload）
- `total_estimated_bytes`：上述各项汇总（用于解释触顶/拒写/淘汰行为）
- `keys_stored_offheap`：keys/expires 是否存储在 off-heap（`0/1`；仅 unsafe 后端可启用）
- `key_count`：keyspace key 数量
- `expire_count`：expire index 数量
- `keyspace_rehashing`：keyspace 是否处于渐进 rehash
- `keyspace_table0_capacity` / `keyspace_table1_capacity`：rehash 双表容量（教学口径）
- `expire_rehashing`：expire index 是否处于渐进 rehash
- `expire_table0_capacity` / `expire_table1_capacity`：rehash 双表容量（教学口径）

> 注意：以上统计口径属于“可解释的估算/预算”，并不等同于 JVM `Runtime.totalMemory()` 或 GC 视角的真实 heap 使用量；其目标是让 maxmemory/淘汰/拒写行为在不同后端下保持一致且可推导。

#### Change: 淘汰与过期清理的时间预算可配置
- `enforceMaxmemory()`：新增 eviction 时间预算（避免高压下长时间同步淘汰导致 tail latency 放大）
- `cleanupExpired()`：时间预算从固定值改为可配置（避免不同部署/负载下出现过期清理不稳定）

#### Change: 写入 preflight（预淘汰/预检查）
为降低“写入后才触发 OOM/淘汰失败”的概率，并从根源避免命令层双 reply：
- `prepareWrite(estimatedExtraBytes)`：写入前进行 cleanupExpired + 预淘汰/预检查（noeviction 下严格拒写）
- 命令层在写 reply 前必须完成 maxmemory 相关的可抛错逻辑（`prepareWrite/enforceMaxmemory`）

#### Configuration: 相关启动参数
- `--evictionTimeLimitMillis <ms>`：单次 maxmemory 淘汰循环的时间预算
- `--expireCleanupTimeLimitMillis <ms>`：单次过期清理的时间预算

#### Scenario: off-heap 启用时触顶行为可预测
条件：启用 off-heap 并持续写入直至达到 `maxmemoryBytes`
- 预期：淘汰/拒写触发时机可解释（与 allocator.usedBytes 变化趋势一致）
- 预期：当字符串 payload 存放在 off-heap 时，heap 估算不重复计入该 payload 长度（避免双计数）

### Requirement: `KEYS` glob 语义（byte 级 Redis 风格）
**Module:** db
`KEYS` 的 glob 匹配必须按 raw bytes 执行（不进行 UTF-8 语义解码），并支持 Redis 风格的最小子集：
- `*` 任意长度
- `?` 单字节
- `[]` 字符集合、范围（`a-z`）、否定（`^`/`!`）
- `\\` 转义特殊字符（例如 `\\*`/`\\?`/`\\[`）

对不完整 `[]`（未闭合）采取兼容策略：按字面量 `[` 匹配，避免越界与不可控解析复杂度。

## Dependencies

- `yierdis-offheap-api`（可选：用于 off-heap allocator/buf/slice 抽象）

## Change History

- 2026-01-04：统一 maxmemory 统计口径（heap 估算 + off-heap usedBytes），并补齐相关回归测试与泄漏验证。
- 2026-01-08：淘汰与过期清理增加“时间预算”并支持配置（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），降低高压下维护任务放大 tail latency 的风险。
- 2026-01-16：off-heap capabilities：core 通过 `YierdisOffHeapAddressAllocator` 显式判断 raw address 能力，避免对具体后端类型的 `instanceof` 耦合，并让依赖方向更符合“SSOT 仅依赖 API”的边界约束。
- 2026-01-16：线程模型硬化：DB 未绑定或跨线程访问 fail-fast；keys/expires 的 off-heap 使用改为显式开关（默认安全）。
- 2026-01-23：新增写入 preflight（`prepareWrite`）与可复用淘汰逻辑（evictUntilUnder），并补齐 `KEYS` glob（`[]`/范围/否定/转义）与 RESP3 `MEMORY STATS` map 输出约定。
