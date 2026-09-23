# DB 行为缺口与审计发现

本文记录一次对 `yierdis-db`（含 server 侧装配）的源码审计结果：**运行期行为缺口、观测口径偏差、健壮性边界和可疑实现**。设计意图与层间契约见 [`db-design-analysis.md`](./db-design-analysis.md)；机制参考见 [`db-internals.md`](./db-internals.md)。

状态图例：

- **已文档化**：现有专题文档已明确说明，属有意行为，此处只做集中索引与运维提示。
- **未文档化**：行为正确性未受质疑，但没有文档说明，建议补文档或代码注释。
- **待确认**：需要作者判断是缺陷还是有意取舍。
- **实现观察**：不影响正确性，但命名/冗余/口径有清理价值。

**验证方式**：A/B/C 节的关键事实（`requireWritable` 早于 `admissionMode`、`reconcileAccounting` 全仓调用点、`expireCount` 下溢位置、`maxAttempts` 表达式、`INFO` 字段映射、`PER_DB` 额度分配）已用 `grep` 与定点阅读回源确认；D/E 节已逐条回源核对分配器与编码源码，并据此删除了此前一条未能证实的"`slotRef` 隐性前置"观察。仍未经逐行回读的文件清单见 [`db-design-analysis.md`](./db-design-analysis.md) 的"验证状态"。

---

## A. 运行期行为

### A1. degraded 会阻断"回收类"写入，且恢复入口未接入 server/command

**现象**

`YierdisDbMutationExecutor.execute` 在读取 `plan.admissionMode()` **之前**无条件调用 `health.requireWritable()`。因此 degraded 时连 `AdmissionMode.RECLAMATION` 也被拒绝：

- 过期 key 回收（active expiration、`reclaimExpiredBeforeMutation`）；
- `DEL`、`PERSIST`、`FLUSHDB`；
- **读路径的惰性过期**——degraded 时读到一个已过期 key 会抛 `YierdisCommandException("MISCONF ...")`，而不是返回 nil。

维护侧同理：`YierdisDbDataMaintenance.runMaintenance` 在第 3 步 `requireWritable()` 之后才排空过期、推进 rehash、执行 maxmemory enforcement，因此 degraded 时除 detached-entry 回收外的整个 tick 被跳过。

**恢复入口**

`RuntimeDbEngine.reconcileAccounting()` 是唯一恢复路径（`YierdisDb` → `YierdisDbDataMaintenance.reconcileAccounting`，绕过 mutation executor，把逻辑账本对齐到物理重算值）。但审计确认：**`yierdis-command`、`yierdis-server`、`yierdis-cli`、`yierdis-benchmark` 的 main 源码中没有任何调用点**，只有 `yierdis-db` 内部与测试引用。

**影响**

stock server 部署进入 degraded 后，没有任何协议/命令入口可以把实例恢复到可写状态，必须由嵌入式宿主在 owner 线程显式调用 runtime API。这与"恢复只能显式触发"的设计意图一致，但"显式"目前只对嵌入式宿主成立。

**状态**：未文档化（恢复语义已文档化，缺的是"没有 server 触发路径"这一事实）。

**建议**：要么补一个运维命令/管理入口，要么在 [`production-hardening-operations.md`](./production-hardening-operations.md) 明确写出 stock server 无恢复入口。

### A2. `expireCount` 下溢发生在 commit 阶段，会升级为 degraded

**现象**

`YierdisDbKeyLifecycle` 的派生计数在 publish/replace/release 中维护；一旦下溢抛 `IllegalStateException("derived expire count underflow")`。这些调用发生在 `prepared.commit()` **内部**，因此会被 `YierdisDbMutationExecutor.postCommitFailure` 捕获 → `recordInvariantFailure` → DB degraded + result-unknown。

**影响**

一个纯簿记（派生计数）错误会升级为需要人工对账的事故；而 A1 的恢复入口又未接入 server。

**状态**：未文档化。

### A3. `noeviction` 下 admission 不回收过期占用（已文档化）

**现象**

`YierdisDbMaxmemorySupport.pickEvictionKey` 对 `noeviction` 直接返回 null，`evictUntilUnder` 立即退出；`ledger.reserve` / `enforceLocalLimit` 也不内联触发 expires 索引清理。因此 keyspace 只剩可回收过期 key 时，增长型写入仍可能 OOM。

`allkeys-random` / `allkeys-lru` 不受影响：候选选择抽到或扫描到过期 key 时按最优候选上报，`evict(...)` 先走 expiration reclamation。

**状态**：已文档化（[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) 均明确说明，属有意策略边界）。

**运维提示**：`noeviction` + 大量 TTL key 时，OOM 可能发生在两个 maintenance tick（默认 1 s）之间；degraded 时维护 tick 还被跳过（见 A1）。

### A4. `ExpiresIndex` 及其 stale 项不进入任何内存账（已文档化）

**现象**

`ExpiresIndex` 是纯堆派生结构：既不计入 ledger 逐 mutation 账，也不计入 `componentRetainedHeapBytes`，因此 `MEMORY STATS` / `INFO used_memory` 会系统性低估这部分堆。高频改 TTL / re-SET 会累积 stale 项。

**状态**：已文档化（[`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) 明确为有意的记账边界）。

---

## B. 观测口径

### B1. `ledger_used_bytes` 名不符实（已文档化）

`MEMORY STATS` 的 `ledger_used_bytes` 与 `INFO memory` 的 `yierdis_ledger_used_bytes` 都绑定到 `heapDataBytesEstimate`（组件保留堆估算），**不是** `YierdisDbMemoryLedger.usedBytes()`。`usedBytes()` 全仓只被 `prepareFlushDb` 与 `reconcileAccounting` 使用，不出现在任何对外字段中。

**状态**：已文档化（[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)、[`db-internals.md`](./db-internals.md)）。**建议**：命名改为 `heap_estimate_bytes`，或补一个真正的 ledger 逻辑账字段。

### B2. `used_bytes_for_maxmemory` 与 `effective_used_bytes_for_maxmemory` 的关系

- `used_bytes_for_maxmemory = heap + native metadata committed + native data committed`；
- `effective_used_bytes_for_maxmemory = used + reservedBytes`（在途 reservation）。

**admission 实际比较的是不含 reservation 的值**；`effective` 是报告值，不是准入阈值本身。两者差值 = 在途 reservation。

**状态**：已文档化。

### B3. `INFO` 的 per-db 额度用整除，与实际分配可能差 1 字节

`NettyServerInfoProvider` 输出 `yierdis_maxmemory_per_db_bytes = maxmemoryBytes / databases`（整数除法）；而 `YierdisInstance.create` 在 `PER_DB` scope 下把余数分给前几个 DB（每个 +1）。被分配到余数的 DB 实际额度会比 INFO 字段多 1 字节。纯观测偏差。

**状态**：未文档化。

---

## C. 边界与健壮性

### C1. 单 DB 淘汰尝试次数无 int 溢出保护

`YierdisDbMaxmemorySupport.evictUntilUnder` 用 `int maxAttempts = Math.max(64, keyLifecycle.keyCount() * 2)`。`keyCount()` 接近 `Integer.MAX_VALUE / 2` 时 `* 2` 会回绕为负，`Math.max` 得到 64——即尝试上限**静默降级为 64**，不会崩溃，但极端 keyspace 下的淘汰收敛会变慢。GLOBAL governor 的同类计算有显式溢出保护。

**状态**：未文档化。

### C2. GLOBAL scope 下每个 DB 的本地 `maxmemoryBytes` 仍是整份全局额度

`YierdisInstance.create` 在 GLOBAL scope 把完整全局额度传给每个 DB，并挂同一个 coordinator。admission 走 coordinator 分支、本地 limit 不参与；但维护期 `enforceLocalMaintenance()` 仍以该全局值作为本地阈值（只在"单个 DB 单独超过全局 maxmemory"时触发，方向一致）。

**状态**：已文档化（[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)）。

---

## D. Native allocator 实现观察

来自 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) 对应源码的复核（本次已逐条回源）。均不影响已文档化的不变量，但命名与实际语义有偏差或存在冗余：

| 观察 | 位置 | 说明 |
|---|---|---|
| `stats()` 字段语义偏差 | `YierdisNativePageAllocator` | `freePages` 复用 `emptySmallPages`；`mediumFreeBytes`/`largeFreeBytes` 硬编码为 `0L`；`liveMediumSpanPages`/`liveLargeSpanPages` 实际累加 `span.pageCount`（**页数**）而非 span 描述符数（后者是 `liveSpanDescriptors`） |
| `skippedBudgetObjects` 偏窄 | `YierdisFfmStableMemoryBackend.defragCycle` | 只在 byte 预算停止时自增；object/time 预算停止时为 0 |
| `defragReclaimedPages` 名不符实 | `YierdisFfmStableMemoryBackend.moveLiveObject` | 按 `retiredBytes / PAGE_BYTES` 在搬迁时累加，统计的是退役 block 覆盖页数，不是真正回收的页数 |
| `STATE_CORRUPT` 未使用 | `YierdisNativeObjectTable` | 状态常量定义但从未写入或匹配 |
| handle `flags` 恒 0 | `YierdisLocalHandleCodec` / object table | flags 会被解出并由 `localHandleFor` 回填，但所有写入路径都传 0，且无任何校验 |
| `NativeReallocPolicy` 单值 | `YierdisFfmStableMemoryBackend.reallocateLocal` | 唯一值 `PRESERVE_PREFIX`，实现未按 policy 分支（行为上仍保留 prefix） |
| `doubleFreeDetections` 语义偏宽 | `YierdisFfmStableMemoryBackend.requireLiveMetaForFree` | 实为"free 时命中 stale 句柄"，包含非 double-free 场景 |
| defrag 无收益判定 | `YierdisFfmStableMemoryBackend.defragCycle` | 对每个合格对象都会分配新 block 并复制，没有"已足够紧凑就跳过"的启发式 |
| skiplist 层数确定性 | `ZSkipList.levelFor` | `P = 0.25` 声明未用，层数由 `mix64(scoreBits ^ memberHash) & 0x3` 确定性推导（概率等价），使 prepared insert/delete 无需随机状态 |

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

**状态**：未文档化（`db-design-analysis.md` 现已记录）。

---

## 建议优先级

1. **A1**：明确 degraded 的运维恢复路径（命令/管理层），或文档化"stock server 无恢复入口"。
2. **A2**：确认派生计数下溢是否应在 commit 阶段被降级为可恢复错误。
3. **B1/B3**：观测字段命名与 per-db 额度口径。
4. **C1**：单 DB `maxAttempts` 加溢出保护，与 governor 对齐。
5. **D/E**：清理冗余常量/字段与修正 stats 命名。
