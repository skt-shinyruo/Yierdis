# Technical Design: DB Production-Grade Architecture Contracts (Redis Alignment)

## Technical Solution

### Core Technologies
- Java 17
- Maven multi-module
- 现有 SSOT 模块边界保持：`yierdis-core`（Netty-free）+ `yierdis-server`（装配/运行时）
- off-heap 抽象保持：`yierdis-offheap-api`（capability + allocator/buf/slice）

### Implementation Key Points

1. **冻结 3 个内部契约（SSOT）：**
   - `KeyHandle`：统一 key identity（heap/off-heap/bytesview）与 hash/len/bytes 访问能力
   - `MemoryLedger`：统一 maxmemory 预算口径、预留/提交流程、淘汰触发与拒写语义
   - `ScanCursor`（新语义）：稳定可推进的 keyspace cursor（可表达 rehash 双表状态）

2. **实例级职责抽离：**
   - 新增 Instance Runtime（建议放在 `yierdis-server` 或 `yierdis-core` 的 runtime 子包，取决于是否希望 core 具备“多 DB 实例”能力）
   - 将全局 LRU clock、global maxmemory coordinator、共享 off-heap allocator 口径收敛到 instance 层
   - `YierdisDb` 只保留单 DB 状态（store/expires/value/memory delta），不直接承载实例级协调

3. **KeyHandle 落地迁移：**
   - `YierdisKeyspace` 增加/迁移为基于 `KeyHandle` 的查找与迭代接口（保留 byte[] 兼容入口但在内部统一转 KeyHandle）
   - `YierdisExpireIndex` 迁移为 key-handle 友好：heap 版避免额外 key copy；off-heap 版引用 keyspace 的 handle/address
   - 命令层尽量以 `YierdisBytesView`/slice 直通 DB（不强制 materialize `byte[]`）

4. **Ledger 落地迁移：**
   - 写入前：`ledger.reserve(estimatedExtra)`，内部完成 `expire cleanup + eviction`（策略可插拔）
   - 写入后：`ledger.commit(delta)` 或 `ledger.rollback(reservation)`（确保异常路径口径不漂移）
   - `MEMORY STATS` / `INFO`：输出 ledger 口径与可解释拆分（保留估算字段，但标注其用途）

5. **测试锁定与可观测性：**
   - 锁定 3 类行为：TTL 返回码、maxmemory 拒写点（preflight）、SCAN cursor 可推进性
   - 保留/扩展 off-heap 泄漏回归锚点：`allocator.usedBytes()` + 强制释放路径覆盖

### Detailed Component Boundaries (Proposed)

> 目标：把当前 `YierdisDb` 的“超大聚合体”拆成可演进组件，同时保证命令层与对外语义尽量不受影响。

| Component | Responsibility | Proposed Location | Notes |
|---|---|---|---|
| `DbEngine`（或保留名 `YierdisDb` 但内部组件化） | 单 DB 编排：线程语义、跨组件编排、暴露对命令层的稳定入口 | `yierdis-core` (`yier.bubu.redis.db.*`) | 对外命令层尽量仍通过 `YierdisDb` 调用，内部逐步搬家 |
| `KeyHandle` | key identity SSOT（heap/off-heap/bytesview），用于 keyspace/expires/scan | `yierdis-core` (`yier.bubu.redis.db.key.*` 或 `db.*`) | 支持“热路径零 heap copy”（off-heap keys） |
| `Keyspace` | key → value 容器（hash table + rehash），提供迭代/采样/删除 | `yierdis-core` (`yier.bubu.redis.db.keyspace.*`) | 现有 `ByteArrayKeyspace`/`UnsafeOffHeapKeyspace` 迁移适配 |
| `ExpireIndex` | key → expireAtMillis 索引，支持快速 TTL 查询与采样清理 | `yierdis-core` (`yier.bubu.redis.db.expire.*`) | 逐步去掉 `Long` 装箱（生产化） |
| `ExpirationManager` | 惰性删除 + 主动清理策略（时间片/采样） | `yierdis-core` (`yier.bubu.redis.db.expire.*`) | 与 scan 解耦，避免遍历副作用 |
| `MemoryLedger` | maxmemory SSOT：预算判定、reserve/commit/rollback、观测口径输出 | `yierdis-core` (`yier.bubu.redis.db.memory.*`) | 逐步替换 `usedBytes` 的手工增减 |
| `EvictionCoordinator` | eviction 策略（random/LRU）与 victim 选择；支持 global/per-db | `yierdis-core` 或 `yierdis-server` | 建议实例层持有 coordinator，DB 提供可操作接口 |
| `ValueOps/*Ops` | 各类型操作：String/Hash/List/Set/ZSet/HLL（只关注类型语义与编码升级） | `yierdis-core` (`yier.bubu.redis.db.ops.*`) | 将 `YierdisDb` 中的大量“命令语义”搬迁出去 |

### Migration & Compatibility Strategy (Feature Flags)

为了避免“一次性大改”带来的不可控回归，所有核心抽象落地应支持灰度与快速回滚：

- `--scanCursorV2`：启用新的 `ScanCursor` 语义（默认关闭）；关闭时保留旧 offset cursor（用于兼容/排障）
- `--ledgerMode legacy|ledger`：legacy 使用当前 `usedBytes/prepareWrite` 逻辑；ledger 模式启用 `MemoryLedger`（默认 legacy，逐步切换）
- `--keyHandleMode legacy|handle`：legacy 允许 canonicalKey heap copy；handle 模式要求 off-heap keys 热路径零 heap copy（默认 legacy，完成后切换）
- 所有开关必须在 `INFO`/`STATS`/`MEMORY STATS` 中可观测（输出当前模式），并能通过测试覆盖锁定行为

### Migration Plan (Detailed, Execution-Oriented)

> 本节用于把“大方向”拆成可执行的迁移步骤，确保每一步都能被测试与可观测性覆盖，并且可回滚。

1. **先锁契约，再动代码：**
   - 先补齐知识库与 ADR 的 SSOT 定义（KeyHandle/Ledger/Cursor/执行模型），再开始迁移实现
   - 先增加回归测试（TTL/maxmemory/SCAN），再开始拆分/重构
2. **先建立“新路径 + feature flag”，再替换“旧路径”：**
   - 每个大改动都必须先提供新实现的旁路路径，并能通过 flag 切换
   - 默认保持 legacy 行为，确保线上/压测可控
3. **按“最小闭环”推进：**
   - KeyHandle：先让 keyspace/expires/scan 在某个后端完整跑通，再扩大到所有后端
   - Ledger：先让单个命令族（String）全链路 reserve/commit 跑通，再扩展到其它类型
   - DB 拆分：先拆 StringOps，再按类型逐步迁移，避免一次性把 `YierdisDb` 拆碎
4. **每一步的验收条件（必须满足才进入下一步）：**
   - 单元测试/集成测试通过
   - INFO/MEMORY STATS 可观测当前模式与关键数值
   - off-heap usedBytes 泄漏回归（allocator.usedBytes）不回退
   - 关键延迟/分配行为不显著变差（至少具备基准 smoke）

## Architecture Design

```mermaid
flowchart TD
    subgraph Instance[Instance Runtime]
      Alloc[OffHeapAllocator]
      Ledger[MemoryLedger]
      Evict[EvictionCoordinator]
      LRU[Global LRU Clock]
      Dbs[DB Array]
    end

    subgraph DB0[YierdisDb (per DB)]
      KS[Keyspace]
      EXP[ExpireIndex]
      VAL[Values / Encodings]
    end

    subgraph DB1[YierdisDb (per DB)]
      KS1[Keyspace]
      EXP1[ExpireIndex]
      VAL1[Values / Encodings]
    end

    Instance --> DB0
    Instance --> DB1
    Alloc --> DB0
    Alloc --> DB1
    Ledger --> Evict
    Evict --> Dbs
    LRU --> DB0
    LRU --> DB1
```

> 说明：DB 仍保持单线程语义（executor 绑定），Instance Runtime 的职责是“共享预算/协调策略/全局时钟”，不引入跨线程并发访问 DB。

## Architecture Decision ADR

### ADR-001: Introduce KeyHandle as key identity SSOT
**Context:** off-heap keys 模式下 `canonicalKey()` 可能返回 heap copy，导致热点读路径分配与语义耦合。  
**Decision:** 引入 `KeyHandle`（或 `KeyRef`），作为 key identity 与访问方式的唯一抽象；keyspace/expires/scan 统一以 handle 交互。  
**Rationale:** 避免“为了 canonical 而 copy”，同时为 scan/ttl/eviction/persistence 提供稳定 key 表示。  
**Alternatives:** 继续使用 `byte[] canonicalKey` → Rejection reason: off-heap keys 读路径无法稳定零拷贝，后续演进会被迫重构。  
**Impact:** 修改 keyspace/expires/scan 的内部 API；命令层可保持外观不变但会减少 `toByteArray()` 的使用。

**Contract (v1, immutable boundary):**
- SSOT 类型：`yier.bubu.redis.db.key.KeyHandle`（提供 `len + byteAt + dictHash`）
- equality/hashCode：以 bytes 内容为准（不以 dictHash 为准），确保 heap/off-heap 之间语义一致
- 兼容策略：通过 `--keyHandleMode legacy|handle` 灰度；legacy 仍保留 `canonicalKey()` 入口但不得作为热路径依赖

### ADR-002: Extract instance-level responsibilities from YierdisDb
**Context:** 全局 maxmemory/LRU/多 DB 协调目前与单 DB 实现耦合。  
**Decision:** 新增 Instance Runtime（实例级上下文），承载 global budget、global LRU、eviction coordinator 与共享 allocator 口径。  
**Rationale:** 清晰划分“实例级 vs DB 级”；降低 DB 内核耦合与测试复杂度。  
**Alternatives:** 继续把 coordinator 放在 `YierdisDb` 内部 → Rejection reason: 未来引入持久化/复制/模块时会扩大耦合面。  
**Impact:** server bootstrap 负责创建 instance 并注入 db；`YierdisDb` 变为更纯粹的 per-db 存储引擎。

### ADR-003: Unified memory accounting with MemoryLedger
**Context:** `usedBytes` + 估算常量 + allocator.usedBytes() + global/per-db 多口径并存，生产上难以保证可复现行为。  
**Decision:** 引入 `MemoryLedger`，作为预算判定的唯一权威；写路径统一 reserve/commit，淘汰与拒写统一由 ledger 驱动。  
**Rationale:** 将“容量规划/拒写点/淘汰策略/可观测输出”收敛到单点，避免口径漂移。  
**Alternatives:** 继续在 DB 内手工维护 usedBytes → Rejection reason: 随类型/命令增长容易遗漏，回归面不可控。  
**Impact:** 命令层 `prepareWrite()` 与 DB 内 delta 计算需要适配 ledger；测试需锁行为。

**Contract (v1, immutable boundary):**
- SSOT 类型：`yier.bubu.redis.db.memory.MemoryLedger`（`reserve/commit/rollback` 两阶段写入语义）
- OOM message 对齐 Redis：`OOM command not allowed when used memory > 'maxmemory'.`
- 兼容策略：通过 `--ledgerMode legacy|ledger` 灰度迁移写路径（默认 legacy）

### ADR-004: Redefine ScanCursor semantics for progress and rehash-awareness
**Context:** 以“遍历偏移量”作为 cursor 在 rehash/变化数据集下不稳定，且易与副作用（过期清理）耦合。  
**Decision:** 重新定义 `ScanCursor`：可表达 table0/table1 与 bucket 位置（以及 rehash 阶段），并确保 best-effort 可推进。  
**Rationale:** 对标 Redis 的 SCAN 客户端使用方式，减少因实现细节导致的不可解释行为。  
**Alternatives:** 保持 offset cursor → Rejection reason: 后续对齐 Redis 行为时会强制重写迭代器。  
**Impact:** keyspace 需要提供迭代 API；scan 不应默认带副作用（过期清理走单独路径）。

**Contract (v1/v2, immutable boundary):**
- v1（默认）：cursor 为数字字符串，`0` 表示结束；best-effort 但必须可推进/可终止（当前实现）
- v2（规划）：rehash-aware cursor（table/bucket），通过 `--scanCursorV2` 灰度启用并保持 bulk string 数字兼容

### ADR-005: Execution model and scaling strategy (single-thread DB + shard/router)
**Context:** 生产化需要明确“单线程一致性语义”与“扩展/隔离策略”。若不提前冻结，后续引入多核扩展、慢命令治理、大 key 防护会倒逼 DB 内核重构。  
**Decision:** 固化以下策略为 SSOT：
- DB 内核（`YierdisDb`）保持“单线程语义 + fail-fast”（不引入并发访问 DB）
- 扩展走 instance 层的 shard/router（默认单 shard；可选多 shard，每个 shard 独立单线程 executor）
- 跨 shard 的命令能力明确限制/策略（优先保持与 Redis 单实例语义一致，避免隐式分布式语义）
- 慢命令/大 key 的隔离治理在 server/instance 层实现（时间预算、分页/COUNT、限流、slowlog/metrics），不侵入 KeyHandle/Ledger/Cursor 契约
**Rationale:** 既对齐 Redis 的单线程语义与生态预期，又为多核利用与生产隔离提供可演进空间，同时避免改动 DB 内核契约。  
**Alternatives:** 直接让 `YierdisDb` 支持多线程并发 → Rejection reason: 与 Redis 语义不一致，且会导致 keyspace/ttl/eviction/iterator 全面重构。  
**Impact:** server 引入 shard/router 与可插拔治理策略；DB 内核继续保持简单一致，扩展与隔离通过上层实现。

### ADR-006: Decompose YierdisDb into engine components (reduce coupling)
**Context:** 当前 `YierdisDb` 同时承担数据结构、命令语义、过期、淘汰、记账等职责，形成“god object”。生产化后新增能力（持久化/复制/ACL/更多编码/策略）会导致跨路径联动改动与回归面扩大。  
**Decision:** 保留 `YierdisDb` 作为对命令层的入口（减少外观变更），但将内部职责拆分为 `ValueOps`/`ExpirationManager`/`EvictionCoordinator`/`MemoryLedger` 等组件，并以清晰接口连接；命令语义逐步从 `YierdisDb` 搬迁到类型 ops。  
**Rationale:** 用“内部组件化 + 渐进迁移”替代“推倒重写”，把未来扩展的变化隔离到局部组件，显著降低演进成本与回归面。  
**Alternatives:** 继续在 `YierdisDb` 中堆叠逻辑 → Rejection reason: 功能越多，耦合越深，最终不可避免重写。  
**Impact:** `yierdis-core` 内部新增 package 与接口；`YierdisDb` 方法逐步变薄；测试以行为锁定确保语义不变。

## Security and Performance

- **Security:**
  - 继续遵守现有硬上限（协议输入/事务队列/错误净化）策略，避免 DoS 与 response splitting 风险扩散到 DB 层
  - off-heap path 必须保持“唯一 owner + 全路径释放”，并以 `allocator.usedBytes()` 做回归锚点
  - 任何新的 instance/ledger 对外可观测输出不得包含客户端原始输入（避免日志注入/信息泄漏）
- **Performance:**
  - KeyHandle 目标：off-heap keys 热路径零 heap 分配（除非必须返回 key bytes）
  - ledger reserve/commit 必须是 O(1) 或近似 O(1)；淘汰循环需保持时间预算（避免 tail latency 放大）
  - scan/iter 必须支持 time-slicing（或 COUNT hint）并避免单次遍历导致长阻塞
  - 慢命令/大 key：通过 server/instance 层策略限制单次执行时间与返回体积，并提供 slowlog/metrics 观测，避免拖垮全实例

## Testing and Deployment

- **Testing:**
  - 单元测试：KeyHandle/hash/equals、cursor 推进、ledger 口径一致性、off-heap 泄漏回归
  - 集成测试：基于命令层验证 TTL 返回码（-2/-1/正数）、maxmemory preflight 拒写点、SCAN cursor 可推进性
  - 压测/回归：bench 对比（heap vs off-heap、global vs per-db），确保 GC/延迟曲线稳定
- **Deployment:**
  - 先以 feature flag/配置项逐步启用（例如：`--scanCursorV2`、`--ledgerEnabled`），允许灰度与回滚
  - 每次演进必须更新 `MEMORY STATS/INFO` 字段解释与 wiki，保持 SSOT 文档同步
