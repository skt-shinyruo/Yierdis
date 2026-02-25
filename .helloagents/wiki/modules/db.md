# db

## Purpose

实现内存数据结构、编码策略、TTL/过期清理、内存估算与淘汰策略。

归属：`yierdis-core-db`（`yier.bubu.redis.db.*`），作为数据结构与内存语义 SSOT（`yierdis-core` 为父 POM/聚合层）。

## Module Overview

- **Responsibility:** Keyspace + 过期索引 + 值编码（string/list/set/hash/zset）+ maxmemory
- **Status:** ✅Stable
- **Last Updated:** 2026-02-25

## Specifications

### Requirement: 存储层与协议写出解耦（domain result / BulkStringSink）
**Module:** db

为避免 “存储层/协议层/性能优化” 相互渗透，db 层遵循以下边界约束：

- db 不对外暴露 `*ReplyCount/*ReplyInto` 等“为了回包优化的 API”（回包形态属于命令/协议层职责）；对外统一使用数据语义命名（例如 `*Count/*WriteTo`）
- value/数据结构层如需 streaming 输出 bulk string 值，只依赖 core 内协议无关的 domain result / sink（例如 `BulkStringValue/BulkStringSequence/BulkStringMapPairs` + `BulkStringSink`），不得依赖 `yierdis-protocol-model`（`ReplyWriter/ReplySink`）
- 命令层负责 reply 形状（array/map header、count 计算、错误语义等），并通过 adapter 将 domain result 写入 `ReplyWriter`（协议编码留在边界层）

补充：db 对命令层暴露的稳定边界为 `DbEngine`（`yier.bubu.redis.ops.DbEngine`），通过 `values()/expiration()/eviction()/keyspace()/ttl()/memory()/lifecycle()` 组合子能力；命令层/协议层不得直接依赖具体 DB 实现（当前实现为 `YierdisDb`）。

### Contracts（SSOT 冻结契约）
**Module:** db

为避免后续补齐生产能力（持久化/复制/ACL/模块/多核扩展）时倒逼“推倒重写”，db 模块冻结以下内部契约作为 SSOT：

#### Contract: KeyHandle（key identity SSOT）
- SSOT 类型：`yier.bubu.redis.db.key.KeyHandle`
- 不可变语义：
  - 提供 `len + byteAt` 的只读 bytes view（统一 heap/off-heap/bytesview 来源）
  - 可携带 `dictHash`（用于 keyspace 索引，可能包含 per-dict seed；不要求跨实例一致）
  - equality/hashCode 语义以 bytes 内容为准（跨后端一致）
- 现状：
  - keyspace / expire index / scan 已全链路落地 handle；热路径禁止隐式 canonical heap copy（以回归与 diagnostics 计数锚定）

#### Contract: MemoryLedger（maxmemory SSOT）
- SSOT 类型：`yier.bubu.redis.db.memory.MemoryLedger`
- 不可变语义：
  - `reserve → commit/rollback` 两阶段写入语义（异常路径可 rollback）
  - 预算判定的拒写点可复现、可回归；不允许出现负数/下溢
  - OOM message 对齐 Redis：`OOM command not allowed when used memory > 'maxmemory'.`
- 口径（SSOT）：
  - `usedBytes`/`ledger_used_bytes`：DB 数据集的 best-effort heap 估算（包含 entry overhead；heap key bytes 在 keys-on-heap 时计入；off-heap payload 不重复计入）
  - `offheap_used_bytes`：allocator 的 best-effort used bytes（用于解释与泄漏锚点）
  - `used_bytes_for_maxmemory`：用于 maxmemory 判定的统一口径（是否包含 off-heap 由 `offheap_included_in_maxmemory` 明确）
  - `reservedBytes`：本次命令 reserve 尚未 commit 的预留值（用于 explain/防漂移；异常路径必须 rollback）
- 现状：
  - 写路径已落地 `prepareWrite → ledger.reserve`；mutate 内部负责 commit/rollback；`enforceMaxmemory()` 作为后台维护入口（server 维护 tick 触发）

#### Contract: ScanCursorV2（SCAN cursor SSOT）
- SSOT 类型：`yier.bubu.redis.ops.ScanCursorV2`
- 不可变语义：
  - cursor 为数字字符串（ASCII 十进制）；`0` 表示扫描结束
  - best-effort：不保证强一致，但必须“可推进、可终止”；rehash/插入/删除/过期并发下仍可 make progress
  - `COUNT` 为 hint，不得导致死循环或单次扫描跑完整个 keyspace
- 现状：
  - cursor v1 已移除；`SCAN` 统一基于 keyspace iterator + cursor v2（rehash-aware）

#### Contract: Snapshot / ChangeEvent（生产扩展护栏）
- SSOT 接口：
  - 快照：`yier.bubu.redis.db.YierdisSnapshot` / `YierdisSnapshotEntry`
  - 事件：`yier.bubu.redis.runtime.api.YierdisChangeSink` / `YierdisChangeEvent`
- 不可变语义：
  - snapshot 基于 `ScanCursorV2` 做 time-slice（`count + cursor` 分批推进），不得暴露/依赖 keyspace 内部结构
  - change event 不携带 DB 内部对象引用或 raw address（避免消费者误用导致泄漏/越界）
  - 消费者不得跨线程直接触达 DB 引擎实现实例（`DbEngine` 的实现，如 `YierdisDb`）；需要执行命令必须回到 owner thread（instance/executor 调度）
- 现状：
  - core 已提供最小接口与回归测试锚点，用于后续 RDB/AOF/replication/审计能力接入

### Requirement: DB 单线程语义硬化（fail-fast）
**Module:** db
DB 引擎实现（当前为 `YierdisDb`）明确为 **非线程安全**，并通过 owner-thread 绑定 + fail-fast 机制将“约定”变为“硬约束”：
- 必须在唯一线程完成 owner-thread 绑定（通常由 server 在 executor owner thread 上通过 binder/`YierdisInstance#bindToCurrentThread` 触发）
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
`maxmemoryBytes` 的触发依据以“可解释的 best-effort 预算口径”为主：heap 侧元数据估算为基础，并在可行时叠加 off-heap allocator 的实占 used bytes。

#### Compatibility: maxmemory scope（global vs per-db）

Redis 的 `maxmemory` 口径是“全实例预算”；在多 DB（`SELECT`）场景下，淘汰/拒写的决策不应被硬拆分到每个 DB。

Yierdis 提供 `--maxmemoryScope` 来明确预算口径：

- `--maxmemoryScope global`（默认，更贴近 Redis）：
  - `maxmemoryBytes` 视为全实例预算；淘汰可跨 DB 进行（allkeys-*）
  - `MEMORY STATS` 输出 **全局口径**（汇总所有 DB）
- `--maxmemoryScope per-db`（兼容模式）：
  - 将 `maxmemoryBytes` 按 DB 数量做硬分摊（`maxmemoryBytes / databases`），各 DB 独立执行淘汰/拒写
  - 行为更像“每个逻辑 DB 一个预算”，与 Redis 的全实例口径不同，但便于教学/隔离

实现约束（SSOT）：
- global 口径的“实例级协调”由 core 内部协调器承担（`YierdisGlobalMaxmemoryCoordinator`），避免将实例职责耦合进单 DB 引擎逻辑。
- core 提供 Netty-free 的 embedded instance API：`yier.bubu.redis.runtime.YierdisInstance`，可在 server/bench/工具/测试中复用“多 DB 装配 + 路由 + 生命周期”语义，减少装配重复与行为漂移。

⚠️ 兼容性说明（多 DB + shared allocator）：
- 当 off-heap allocator 由单个 DB 所拥有（`ownsOffHeapAllocator=true`）时：`usedBytesForMaxmemory = heap_estimate + offheap_used`，更接近“总预算”的直觉。
- 当 server 多 DB 共享同一个 off-heap allocator（`ownsOffHeapAllocator=false`）时：为避免同一 `allocator.usedBytes()` 被每个 DB 重复计入导致过度淘汰，DB 侧 `usedBytesForMaxmemory` 仅使用 heap 估算；off-heap 总量由 `--offheapMaxBytes` 单独约束，并通过 `MEMORY STATS`/`INFO` 输出可观测。

为提升“为什么拒写/为什么淘汰”的可解释性，补充预算分解输出：
- `DbEngine.memory().memoryStats()`（当前实现委派到 `YierdisDb#memoryStats()`）：输出 heap/off-heap/结构开销等分解估算
- `MEMORY STATS`：命令侧暴露预算分解（用于排障/教学；明确为估算）

#### Contract: `MEMORY STATS` 输出字段（稳定性约束）
`MEMORY STATS` 输出字段集合保持稳定，输出为结构化 object（key/value）：
- key：ASCII string
- value：integer（布尔值用 `0/1` 表示）

字段含义（口径：估算/预算优先，可解释性优先）：
- `maxmemory_bytes`：配置的 maxmemory 上限（0 表示不启用淘汰/拒写）
- `used_bytes_for_maxmemory`：用于 maxmemory 判定的“统一口径已用 bytes”（是否包含 off-heap 由 `offheap_included_in_maxmemory` 明确）
- `effective_used_bytes_for_maxmemory`：`used_bytes_for_maxmemory + ledger_reserved_bytes`（用于 explain reserve 阶段的瞬时压力）
- `ledger_used_bytes`：DB 数据集的 heap 侧估算（不等同于 JVM/GC 视角真实 heap）
- `offheap_used_bytes`：off-heap allocator 的实占 `usedBytes()`（可作为泄漏回归锚点）
- `ledger_reserved_bytes`：当前命令 reserve 尚未 commit 的预留值（异常路径必须 rollback）
- `offheap_included_in_maxmemory`：off-heap used bytes 是否计入 maxmemory 判定（`0/1`）
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
- maxmemory eviction：提供 eviction 时间预算（避免高压下长时间同步淘汰导致 tail latency 放大）
- `cleanupExpired()`：时间预算从固定值改为可配置（避免不同部署/负载下出现过期清理不稳定）

#### Change: 写入 preflight（预淘汰/预检查）
为降低“写入后才触发 OOM/淘汰失败”的概率，并从根源避免命令层双 reply：
- `prepareWrite(estimatedExtraBytes)`：写入前进行 cleanupExpired + 预淘汰/预检查（noeviction 下严格拒写）
- 命令层在写 reply 前必须完成可抛错的 maxmemory preflight（即 `prepareWrite`）；后续淘汰作为 best-effort 维护，不参与命令错误语义

#### Configuration: 相关启动参数
- `--evictionTimeLimitMillis <ms>`：单次 maxmemory 淘汰循环的时间预算
- `--expireCleanupTimeLimitMillis <ms>`：单次过期清理的时间预算
- `--maxmemoryScope global|per-db`：maxmemory 预算口径（默认 `global`）

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
- 2026-01-23：新增写入 preflight（`prepareWrite`）与可复用淘汰逻辑（evictUntilUnder），并补齐 `KEYS` glob（`[]`/范围/否定/转义）与 `MEMORY STATS` 输出字段约定。
- 2026-02-01：`EXPIRE seconds<=0` 对齐为“立即删除”；`MEMORY STATS` 的数值字段对齐为 integer；entry overhead 估算常量收敛为单点定义（避免命令层/DB 双口径漂移），并对多 DB + shared allocator 场景明确 maxmemory/off-heap 的口径边界。
- 2026-02-04：实例语义收敛：抽离 global maxmemory 协调器并引入 core 的 `YierdisInstance`（embedded instance API），为 server/bench/测试复用 instance 装配与生命周期语义提供基座。
- 2026-02-09：分层边界收口：引入/扩展 `DbEngine` 子 ops（keyspace/ttl/memory/lifecycle）并适配 `YierdisDb`；集合 streaming 写出接口命名收敛为 `*Count/*WriteTo`，避免 reply 语义渗透到存储层 API 命名。
