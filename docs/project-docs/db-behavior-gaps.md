# DB 行为缺口与审计发现

对 `yierdis-db`（含 server 侧装配）的源码审计发现如下：**运行期行为缺口、观测口径偏差、健壮性边界和可疑实现**。设计意图与层间契约见 [`db-design-analysis.md`](./db-design-analysis.md)；机制参考见 [`db-internals.md`](./db-internals.md)。

每条都按同一模板写：**触发条件 → 可观测现象 → 影响范围 → 测试覆盖 → 状态 → 建议**。测试覆盖一栏是实际的检索结论，不是印象。

状态图例：

- **已文档化**：现有专题文档已明确说明，属有意行为，此处只做集中索引与运维提示。
- **未文档化**：行为正确性未受质疑，但没有文档说明，建议补文档或代码注释。
- **待确认**：需要作者判断是缺陷还是有意取舍。
- **实现观察**：不影响正确性，但命名/冗余/口径有清理价值。

**验证方式**：A/B/C 节的关键事实（`requireWritable` 早于 `admissionMode`、`reconcileAccounting` 全仓调用点、`expireCount` 下溢位置、`maxAttempts` 表达式、`INFO` 字段映射、`PER_DB` 额度分配）已用 `grep` 与定点阅读回源确认。D/E 节已逐条回源核对分配器与编码源码，并据此删除了此前一条未能证实的"`slotRef` 隐性前置"观察。

**测试覆盖一栏的检索范围**：本仓 `**/src/test/**/*.java`（`grep` 类名与方法名）。"未发现"指的是本次检索范围内没有命中，不等于绝对不存在。

---

## A. 运行期行为

### A1. degraded 会阻断"回收类"写入，且恢复入口未接入 server/command

**触发条件**

任一 degraded 事故后：commit 之后抛出的 `NativeMemoryException`/`IllegalStateException`（经 `postCommitFailure` → `recordInvariantFailure`），或其它显式 `recordInvariantFailure(...)` 调用点。

**现象**

`YierdisDbMutationExecutor.execute` 在读取 `plan.admissionMode()` **之前**无条件调用 `health.requireWritable()`。因此 degraded 时连 `AdmissionMode.RECLAMATION` 也被拒绝：

- 过期 key 回收（active expiration、`reclaimExpiredBeforeMutation`）；
- `DEL`、`PERSIST`、`FLUSHDB`；
- **读路径的惰性过期**——degraded 时读到一个已过期 key 会抛 `YierdisCommandException("MISCONF DB is in a degraded state; writes are disabled")`，而不是返回 nil。

维护侧同理：`YierdisDbDataMaintenance.runMaintenance` 在第 3 步 `requireWritable()` 之后才排空过期、推进 rehash、执行 maxmemory enforcement，因此 degraded 时只有 detached-entry 回收照常进行，tick 的其余部分全部跳过。

**恢复入口**

`RuntimeDbEngine.reconcileAccounting()` 是唯一恢复路径（`YierdisDb` → `YierdisDbDataMaintenance.reconcileAccounting`，绕过 mutation executor，把逻辑账本对齐到物理重算值）。但审计确认：**`yierdis-command`、`yierdis-server`、`yierdis-cli`、`yierdis-benchmark` 的 main 源码中没有任何调用点**，只有 `yierdis-db` 内部与测试引用。

**影响范围**

stock server 部署进入 degraded 后，没有任何协议/命令入口能把实例恢复到可写状态，必须由嵌入式宿主在 owner 线程显式调用 runtime API。影响面是**整实例该 DB 的写入能力**，且会连带阻断 `DEL`/`FLUSHDB` 这类"用户以为总能执行"的命令。这与"恢复只能显式触发"的设计意图一致，但"显式"目前只对嵌入式宿主成立。

**测试覆盖**

- `YierdisDbHealthTest`、`YierdisDbReconcileAccountingTest`（覆盖清除 degraded、恢复写入、owner 线程约束、failed reconciliation、physical read error 等场景）、`MutationExecutorReservationTest`、`ReplyResultUnknownTest`（`yierdis-tests` 集成层）覆盖了 degraded 机制与恢复语义。
- "stock server 没有触发路径"这一事实本身**不可能有测试**——缺的就是那条路径。

**状态**：未文档化（恢复语义已文档化，缺的是"没有 server 触发路径"这一事实）。

**建议**：要么补一个运维命令/管理入口，要么在 [`production-hardening-operations.md`](./production-hardening-operations.md) 明确写出 stock server 无恢复入口。

### A2. `expireCount` 下溢发生在 commit 阶段，会升级为 degraded

**触发条件**

`YierdisDbKeyLifecycle` 的派生计数在 publish/replace/release 中被错误地多减一次（例如同一 entry 被 release 两次、或 TTL 状态判定与计数更新不一致）。

**现象**

一旦下溢就抛 `IllegalStateException("derived expire count underflow")`。这些调用都发生在 `prepared.commit()` **内部**，因此会被 `YierdisDbMutationExecutor.postCommitFailure` 捕获 → `recordInvariantFailure` → DB degraded + result-unknown。

**影响范围**

一个纯簿记（派生计数）错误会升级为需要人工对账的事故；而 A1 的恢复入口又未接入 server。触发后果不止"计数不对"，而是整个 DB 停止接受写入。

**测试覆盖**

`expireCount` 的派生计数行为在 `YierdisDbKeyLifecycleTest` 等测试中有覆盖（`ActiveExpirationTest`、`PhysicalMemoryAccountingTest`、`MutationFaultInjectionTest` 也涉及该字段）。但"下溢 → commit 内异常 → degraded + result-unknown"这一组合，本次检索未发现专门的端到端用例。

**状态**：未文档化。

**建议**：确认派生计数下溢是否应在 commit 阶段被降级为可恢复错误——现在它是"一个计数 bug 换一次停写"。

### A3. `noeviction` 下 admission 不回收过期占用（已文档化）

**触发条件**

配置 `noeviction` + keyspace 中存在大量已过期但尚未被维护节拍回收的 TTL key。

**现象**

`YierdisDbMaxmemorySupport.pickEvictionKey` 对 `noeviction` 直接返回 null，`evictUntilUnder` 立即退出；`ledger.reserve` / `enforceLocalLimit` 也不内联触发 expires 索引清理。因此 keyspace 只剩可回收过期 key 时，增长型写入仍可能 OOM。

`allkeys-random` / `allkeys-lru` 不受影响：候选选择抽到或扫描到过期 key 时按最优候选上报（`lruClock = 0`），`evict(...)` 先走 expiration reclamation。

**影响范围**

写入可用性——OOM 会发生在两个 maintenance tick 之间（默认 1 s），degraded 时维护 tick 还被跳过（见 A1），此时过期占用只能靠读路径惰性过期少量释放。

**测试覆盖**

`MaxmemoryEvictionTest.noevictionRejectsWritesWhenFull` 直接覆盖该行为；`TtlMaxmemoryTest`、`YierdisInstanceTest`、`YierdisDbMaintenanceContractTest`、`KeysBudgetTest` 覆盖相关口径。

**状态**：已文档化（[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) 均明确说明，属有意策略边界）。

### A4. `ExpiresIndex` 及其 stale 项不进入任何内存账（已文档化）

**触发条件**

高频改 TTL、`PERSIST`、`KEEPTTL` 或 re-`SET`：每次 deadline 真的变化就写一条新索引项，旧项不主动清除。

**现象**

`ExpiresIndex` 是纯堆派生结构：既不计入 ledger 逐 mutation 账，也不计入 `componentRetainedHeapBytes`，因此 `MEMORY STATS` / `INFO used_memory` 会系统性低估这部分堆。队列规模可长期超过存活 TTL key 数。

**影响范围**

观测口径（低估）与堆压（真实存在但不可见）。极端情况下队列的堆占用可以显著超过用户感知的"TTL key 数量"。

**测试覆盖**

`ActiveExpirationTest`、`TtlMaxmemoryTest` 覆盖过期消费与 TTL 行为；"索引规模 > 存活 TTL key 数"这一不变量本身未被断言。

**状态**：已文档化（[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) 明确为有意的记账边界）。

---

## B. 观测口径

### B1. `ledger_used_bytes` 名不符实（已文档化）

**现象**

`MEMORY STATS` 的 `ledger_used_bytes` 与 `INFO memory` 的 `yierdis_ledger_used_bytes` 都绑定到 `heapDataBytesEstimate`（组件保留堆估算）——在 `KeyCommands` 里就是 `memoryStat("ledger_used_bytes", YierdisMemoryStats::heapDataBytesEstimate)`，**不是** `YierdisDbMemoryLedger.usedBytes()`。`usedBytes()` 全仓只有 `prepareFlushDb` 与 `reconcileAccounting` 会用到，不出现在任何对外字段中。

**影响范围**

运维误判：看到"ledger"字样会以为它是准入阈值用的那本账，实际两者可以长期不等。

**测试覆盖**

字段映射由 `YierdisDbHealthTest`/`PhysicalMemoryAccountingTest` 一类测试间接覆盖；命名本身无法测试。

**状态**：已文档化（[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)、[`db-internals.md`](./db-internals.md)）。**建议**：命名改为 `heap_estimate_bytes`，或补一个真正的 ledger 逻辑账字段。

### B2. `used_bytes_for_maxmemory` 与 `effective_used_bytes_for_maxmemory` 的关系

- `used_bytes_for_maxmemory = heap + native metadata committed + native data committed`；
- `effective_used_bytes_for_maxmemory = used + reservedBytes`（在途 reservation）。

**admission 实际比较的是不含 reservation 的值**；`effective` 是报告值，不是准入阈值本身。两者差值 = 在途 reservation。

**触发条件**：任何时刻只要有一次 mutation 处于"已准入未 settle"的窗口，两个字段就会不相等。

**影响范围**：报告解读。把 `effective` 当成阈值会得出"已经超了但没被拒"的错误结论。

**测试覆盖**：`MutationExecutorReservationTest` 覆盖 reservation 语义。

**状态**：已文档化。

### B3. `INFO` 的 per-db 额度用整除，与实际分配可能差 1 字节

**现象**

`NettyServerInfoProvider` 输出 `yierdis_maxmemory_per_db_bytes = maxmemoryBytes / max(1, databases)`（整数除法）；而 `YierdisInstance.create` 在 `PER_DB` scope 下把余数分给前几个 DB（每个 +1）。分到余数的 DB，实际额度比 INFO 字段多 1 字节。

**触发条件**：`maxmemoryBytes` 不能被 `databases` 整除，且该 DB 落在余数分配范围内。

**影响范围**：纯观测偏差（1 字节量级），不影响准入行为。

**测试覆盖**：`YierdisInstanceTest` 覆盖 PER_DB 装配；该 1 字节差异未被断言。

**状态**：未文档化。

---

## C. 边界与健壮性

### C1. 单 DB 淘汰尝试次数无 int 溢出保护

**触发条件**

`YierdisDbMaxmemorySupport.evictUntilUnder` 用 `int maxAttempts = Math.max(64, keyLifecycle.keyCount() * 2)`。`keyCount()` 接近 `Integer.MAX_VALUE / 2` 时 `* 2` 会回绕为负。

**现象**

`Math.max` 得到 64——尝试上限**静默降级为 64**，不会崩溃，但极端 keyspace 下的淘汰收敛会变慢（每轮只能看 64 个候选）。

**影响范围**

只影响极端规模（约 10 亿 key）下的淘汰效率；正确性不受影响。GLOBAL governor 的同类计算（`maxAttemptsFromKeys`）有显式溢出保护，两者行为不一致。

**测试覆盖**：未发现针对溢出边界的用例（这类输入在测试里无法真实构造）。

**状态**：未文档化。

**建议**：单 DB 的 `maxAttempts` 加溢出保护，与 governor 对齐。

### C2. GLOBAL scope 下每个 DB 的本地 `maxmemoryBytes` 仍是整份全局额度

**现象**

`YierdisInstance.create` 在 GLOBAL scope 把完整全局额度传给每个 DB，并挂同一个 coordinator。admission 走 coordinator 分支、本地 limit 不参与；但维护期 `enforceLocalMaintenance()` 仍以该全局值作为本地阈值（只在"单个 DB 单独超过全局 maxmemory"时触发，方向一致）。

**影响范围**

不会误拒（阈值比实际更宽松），只会在"单 DB 独占全局额度"时才生效——是保守而非激进。

**测试覆盖**：`YierdisInstanceTest` / `YierdisDbMaintenanceContractTest` 覆盖 GLOBAL 装配与维护路径。

**状态**：已文档化（[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)）。

---

## D. Native allocator 实现观察

以下观察来自 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) 对应源码的复核（本次已逐条回源），都不影响已文档化的不变量，只是命名与实际语义有偏差，或者存在冗余：

| 观察 | 位置 | 说明 |
|---|---|---|
| `stats()` 字段语义偏差 | `YierdisNativePageAllocator` | `freePages` 复用 `emptySmallPages`（构造 stats 时同一值传了两次）；`mediumFreeBytes`/`largeFreeBytes` 硬编码为 `0L`；`liveMediumSpanPages`/`liveLargeSpanPages` 实际累加 `span.pageCount`（**页数**）而非 span 描述符数（后者是 `liveSpanDescriptors`） |
| `defragReclaimedPages` 混用两种口径 | `YierdisFfmStableMemoryBackend.moveLiveObject` 与 `trimEmptyPages` | 搬迁时按 `retiredBytes / PAGE_BYTES` 累加（统计的是**退役 block 覆盖页数**，不是真正回收的页数）；`trimEmptyPages(...)` 又把本次 trim 回收量累加到同一字段 |
| `skippedBudgetObjects` 偏窄 | `YierdisFfmStableMemoryBackend.defragCycle` | 预算检查顺序为 object → time → pinned → byte；`skippedBudgetObjects` 只在 byte 预算停止（`meta.size() > options.maxMoveBytes() - movedBytes`）时自增，object/time 预算停止时为 0 |
| `STATE_CORRUPT` 未使用 | `YierdisNativeObjectTable` | 状态常量定义但从未写入或匹配 |
| handle `flags` 恒 0 | `YierdisLocalHandleCodec` / object table | flags 会被解出并由 `localHandleFor` 回填，但所有写入路径都传 0，且无任何校验 |
| `NativeReallocPolicy` 单值 | `YierdisFfmStableMemoryBackend.reallocateLocal` | 唯一值 `PRESERVE_PREFIX`，实现未按 policy 分支（行为上仍保留 prefix） |
| `doubleFreeDetections` 语义偏宽 | `YierdisFfmStableMemoryBackend.requireLiveMetaForFree` | 实为"free 时命中 stale 句柄"，包含非 double-free 场景 |
| defrag 无收益判定 | `YierdisFfmStableMemoryBackend.defragCycle` | 对每个合格对象都会分配新 block 并复制，没有"已足够紧凑就跳过"的启发式 |
| skiplist 层数确定性 | `ZSkipList.levelFor` | `P = 0.25` 声明未用；层数由 `mix64(Double.doubleToLongBits(score) ^ memberStore.hashBytes(member))` 起步、低 2 位为 0 就升层（继续概率 1/4，与 `P` 等价）确定性推导，使 prepared insert/delete 无需随机状态 |

**测试覆盖**：这些行为落在 `YierdisFfmStableMemoryBackendTest`、`StableMemoryBackendContractTest`、`YierdisFfmStableMemoryBackendOwnershipTest`、`YierdisNativePageAllocatorTest`、`YierdisNativeObjectTableTest`、`YierdisLocalHandleCodecTest`、`NativeAllocationScopeTest`、`YierdisDbLifecycleContractTest` 的覆盖范围内。**统计字段的语义偏差本身没有断言**——测试断言的是不变量（回收安全性、scope 回滚、handle 校验），不是这些名字。

**状态**：实现观察。

---

## E. 编码与数据表示的偏离

完整对照表见 [`db-design-analysis.md`](./db-design-analysis.md) 的"刻意偏离 Redis 的地方"。要点：

- **STRING `int`/`embstr` 与 `raw` 物理表示相同**：都是 native `STRING_BYTES` blob，`StringRoot.encoding()` 恒返回 `STRING_RAW`；`STRING_INT` 只影响 `EntryRecord` 标签与 entry 元数据估算，不省内存。
- **`SET_INTSET` 在 heap**（`short[]`/`int[]`/`long[]`），不受 off-heap 口径影响。
- **ZSET `listpack` 的 score 在 heap `double[]`**，native listpack 只存 member。
- **listpack 块是自定义 varint 格式**，不是 Redis listpack 二进制；HLL 是唯一字节级 Redis 兼容的编码。
- **hash packed 阈值 512** 对齐旧 `hash-max-ziplist-entries`，与 Redis 7.x 默认 128 不同。
- 死代码/冗余字段：`SetValue.LONG_MIN_VALUE_BYTES`；quicklist node 的 `payloadRef`（offset 48 恒写 `NativeHandle.NULL`、无读取者）；`ZSkipList.P`（层数改用确定性 `mix64(...) & 0x3`）；`NativeObjectKind.SCORE_BYTES`（声明但全仓无使用）。

**影响范围**：与真实 Redis 客户端交互时，除了 HLL 之外的序列化内容都不保证二进制互通；`TYPE`/`OBJECT ENCODING` 的输出语义与 Redis 不完全对应（例如 string 永远不是真 int 编码）。

**测试覆盖**：`OffHeapStringStorageTest`、`NativeStorageRegressionTest`、`PhysicalMemoryAccountingTest` 覆盖存储与口径回归；HLL 字节兼容有专门用例。

**状态**：未文档化（[`db-design-analysis.md`](./db-design-analysis.md) 现已记录）。

---

## 建议优先级

1. **A1**：明确 degraded 的运维恢复路径（命令/管理层），或文档化"stock server 无恢复入口"。
2. **A2**：确认派生计数下溢是否应在 commit 阶段被降级为可恢复错误。
3. **B1/B3**：观测字段命名与 per-db 额度口径。
4. **C1**：单 DB `maxAttempts` 加溢出保护，与 governor 对齐。
5. **D/E**：清理冗余常量/字段与修正 stats 命名。