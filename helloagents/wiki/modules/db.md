# db

## Purpose

实现内存数据结构、编码策略、TTL/过期清理、内存估算与淘汰策略。

归属：`yierdis-core`（`yier.bubu.redis.db.*`），作为数据结构与内存语义 SSOT。

## Module Overview

- **Responsibility:** Keyspace + 过期索引 + 值编码（string/list/set/hash/zset）+ maxmemory
- **Status:** ✅Stable
- **Last Updated:** 2026-01-16

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

> 注意：以上统计口径属于“可解释的估算/预算”，并不等同于 JVM `Runtime.totalMemory()` 或 GC 视角的真实 heap 使用量；其目标是让 maxmemory/淘汰/拒写行为在不同后端下保持一致且可推导。

#### Change: 淘汰与过期清理的时间预算可配置
- `enforceMaxmemory()`：新增 eviction 时间预算（避免高压下长时间同步淘汰导致 tail latency 放大）
- `cleanupExpired()`：时间预算从固定值改为可配置（避免不同部署/负载下出现过期清理不稳定）

#### Configuration: 相关启动参数
- `--evictionTimeLimitMillis <ms>`：单次 maxmemory 淘汰循环的时间预算
- `--expireCleanupTimeLimitMillis <ms>`：单次过期清理的时间预算

#### Scenario: off-heap 启用时触顶行为可预测
条件：启用 off-heap 并持续写入直至达到 `maxmemoryBytes`
- 预期：淘汰/拒写触发时机可解释（与 allocator.usedBytes 变化趋势一致）
- 预期：当字符串 payload 存放在 off-heap 时，heap 估算不重复计入该 payload 长度（避免双计数）

## Dependencies

- `yierdis-offheap-api`（可选：用于 off-heap allocator/buf/slice 抽象）

## Change History

- 2026-01-04：统一 maxmemory 统计口径（heap 估算 + off-heap usedBytes），并补齐相关回归测试与泄漏验证。
- 2026-01-08：淘汰与过期清理增加“时间预算”并支持配置（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），降低高压下维护任务放大 tail latency 的风险。
- 2026-01-16：off-heap capabilities：core 通过 `YierdisOffHeapAddressAllocator` 显式判断 raw address 能力，避免对具体后端类型的 `instanceof` 耦合，并让依赖方向更符合“SSOT 仅依赖 API”的边界约束。
- 2026-01-16：线程模型硬化：DB 未绑定或跨线程访问 fail-fast；keys/expires 的 off-heap 使用改为显式开关（默认安全）。
