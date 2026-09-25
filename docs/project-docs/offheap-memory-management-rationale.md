# 堆外内存管理机制与 C Redis 的对照

## 一、这篇文档回答什么

选定堆外存储之后，内存管理（分配、碎片、监控、回收、与 JVM 协同）具体要怎么做，以及各点与 C 版 Redis（jemalloc / 引用计数 / 淘汰策略）的差异和可借鉴方案。本文是背景性结论，源码实现见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) 与 [`native-memory-runtime.md`](./native-memory-runtime.md)；“为什么用堆外”见 [`jvm-constraints-and-offheap-rationale.md`](./jvm-constraints-and-offheap-rationale.md)。文中比例数字为经验估算，采样参数是 Redis 公开实现的既有口径。

## 二、四层职责：由谁提供

内存管理的职责分层两边相同，差别在分配器层与 OS 层的提供方：

```text
层职责      C Redis（jemalloc 生态）              Java 堆外（FFM 生态）
策略层      maxmemory 淘汰 · TTL 主动过期          自记账 maxmemory · 采样淘汰
生命周期层  robj 引用计数 · lazy free 后台线程      handle 引用计数 / Arena scope · 后台回收
分配器层    jemalloc：size class + arena + tcache  自建 slab / size class on Arena（核心分水岭）
OS 层       mmap · fork COW · MADV                 fork 不可用 · 页归还与快照都要自建
```

C 侧第 3、4 层由 jemalloc 与 fork 免费提供；Java 侧全部自建。

## 三、分配与释放机制设计

C 侧 `zmalloc()` 包一层 jemalloc 即全部；Java 侧拿到的只有“一大块裸内存”（`Arena.allocate` / `Unsafe.allocateMemory`），分配器要自建，可选路线：

| 路线 | 思路 | 适用 |
| --- | --- | --- |
| slab + size class | 按固定档位（8/16/32…字节）切 chunk，空闲链串联 | 小对象为主，正是 Redis 场景；Memcached 与 Netty `PooledByteBufAllocator` 同思路 |
| buddy 分配 | 2 的幂拆分合并 | 变长大 value，合并时碎片回收干脆 |
| segregated free list | 几档自由链表混合 | 折中，实现成本最高 |

设计要点：

1. **分配元数据放堆内**：free list 等元数据量级小（每页几字节），放堆内 `long[]` 换 `jmap`/heap dump 可见性；纯 C 风格放堆外也行，但排障更盲。
2. **句柄而不是裸 offset**：对外只发句柄（`id = 索引 + 代次`），代次识别释放后的悬垂引用（Java 版 ABA 防护）。
3. **并发分配**：jemalloc 用 arena-per-thread 抗竞争；若采用单 owner 线程模型（DB 操作绑定固定线程），天然无竞争，不需要 tcache 这层复杂度。
4. **确定析构**：`DirectByteBuffer` 的 Cleaner 要等 GC 回收 wrapper，时序不可控；一律用 FFM `Arena.close()` 或自建引用计数，把释放变成显式动作。

## 四、内存碎片：可见性、分级化与搬迁

C Redis 的碎片由 jemalloc 兜着（`INFO memory` 的 `mem_fragmentation_ratio` 可观测，超阈值可开 activedefrag 增量搬迁）；Java 侧底层是裸 malloc，碎片**不可见也不可搬**，必须主动设计，三层组合拳：

1. **变长 → size class 化**：任意大小归入最近档位，用可控的内部碎片（档位取整浪费，估算约 10~15%）换外部碎片归零。
2. **间接句柄层 → 可搬迁**：上层拿句柄不拿地址，碎片整理就只是“复制块 + 改表项”：

   ```text
   堆内 handle（引用 = long id） -> 句柄表（id -> 地址） -> 堆外块
   搬迁 = 复制块 + 只改句柄表中的地址；上层引用（id）不变，逐 tick 增量执行避免停顿
   ```

   这是 Java 版 activedefrag 的前提：C 的 activedefrag 要反向修正所有指针，句柄间接层把这件事变成表单更新。
3. **页归还 OS**：jemalloc 自动把释放页分级（dirty/muzzy）并 `MADV_FREE`；Java 侧要自建 trim——分配器定期扫描全空 span 调 `madvise` 归还，RSS 与自身记账的差距就是碎片信号。

## 五、监控与统计

C 侧是“一条计数 + 一个比率”（`zmalloc` 全局计数得 `used_memory`，除以 RSS 得碎片率，`mallctl` 另给分配器内部统计）。Java 侧自建同等体系：

| 口径 | 实现 |
| --- | --- |
| 自身记账 | `LongAdder` 按 owner 分段计数，区分 committed（已占页）/ live（逻辑存活）/ reclaimable（回收候选），比 C 的单一口径更细 |
| 对账 | accounted（堆估计 + native committed）vs `/proc/self/status` 的 RSS；gap 持续增长 = 碎片或泄漏警报 |
| 暴露 | JMX MBean / Micrometer；兼容 Redis 运维习惯就再包一层 `INFO memory` 语义输出 |
| JVM 兜底 | NMT（只到堆外总量）、`-XX:MaxDirectMemorySize` 硬顶 |

关键纪律：**堆外压力 JVM 不会替你感知**——不触发 GC、不传回信号。监控必须反过来驱动策略：分配速率上涨 → 提前启动淘汰、收紧 ingress 背压，而不是等 OOM。

## 六、回收策略的实现

四层回收，节奏各管各的：

1. **对象级**：句柄引用计数（Netty 风格，适合共享 slice，如跨 `EXEC` replay 保留的请求）与 Arena scope（per-DB 整片释放）混合：热路径 refcount，搬迁/重建用 scope。
2. **延迟回收**：借鉴 lazy free——大 key 删除、`FLUSHALL` 等重释放放后台线程，避免在 owner 线程上手动制造一次“STW”。
3. **内存淘汰（maxmemory）**：
   - C：精确字节计数 + 采样近似 LRU/LFU（每轮随机采样、默认 5 个进 eviction pool；LRU 为 24-bit 时钟，LFU 为 8-bit 对数计数 + 时间衰减）。
   - Java：采样算法原样可搬（开放寻址表里随机采 N 个槽位比较 idle 时间），不需要 O(1) 全量 LRU 链表；代价是精确记账要自己做，没有 zmalloc 代劳。实现口径见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。
4. **TTL 主动过期**：`hz` 轮询 + 每轮时间预算（Redis 默认 10hz、单轮约 25% CPU）；过期索引可按 Redis 7 的基数树思路做按过期时间排序的堆外索引。实现口径见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

贯穿原则：所有重回收动作拆成每 tick 固定预算的增量步——与 activedefrag、active expire 同一思想，也是 GC 之外的第二道 P99 防线。

## 七、与 JVM 内存管理的协同

| 主题 | 协同方式 |
| --- | --- |
| 堆划分 | 小堆（`-Xmx` 只服务协议层、连接缓冲、句柄表）+ 大堆外（数据） |
| GC 选型 | ZGC / Shenandoah；数据离堆后堆内以短寿对象为主，基本只剩 Young GC |
| 限额对账 | `MaxDirectMemorySize` 作硬顶，自建限额留 guard band，先于 JVM 触发淘汰 |
| 释放兜底 | 数据不走 `DirectByteBuffer`，避免 `-XX:+DisableExplicitGC` 与其 `System.gc()` 兜底机制的冲突 |
| JIT | 堆外访问走 `VarHandle` 热路径可内联；上线前预热让分配/访问路径编译到位 |
| 线程交接 | 后台回收线程与 owner 线程间用 acquire/release 语义（`VarHandle`），别把锁带进热路径 |

## 八、与 C 三大机制的对照与借鉴

| 机制 | C Redis 做法 | Java 差异 | 借鉴方案 |
| --- | --- | --- | --- |
| 分配器 | jemalloc：size class + arena + tcache，碎片可控可观测 | 裸 malloc，碎片化零 | Netty `PooledByteBufAllocator` 即 jemalloc 的 Java 移植；自建则抄 slab / size class 分级 |
| 引用计数 | `robj.refcount` + 0~9999 共享整数 | 无内建，Cleaner 时序不可控 | 句柄 refcount + 代次；共享小对象改放堆内缓存 |
| 淘汰策略 | 精确计数 + 采样近似 LRU/LFU + 主动过期 | 算法可直接照搬，计数要自建 | 采样淘汰 + hz 过期轮询原样实现；记账细于 C（committed/live 分开） |

## 九、Java 特有挑战与解决思路清单

1. 无确定析构 → 显式 `close()` / 引用计数为主，Cleaner 只作兜底保险。
2. 越界与悬垂 → 默认走 FFM 检查路径；句柄代次识别 stale；`Unsafe` 仅限验证过的热路径。
3. 排障失明 → 分配元数据放堆内（heap dump 可见）+ 自建 native dump 工具。
4. fork 不可用 → 快照逻辑化：增量快照或 epoch 一致切点，同时影响 BGSAVE 与主从全量同步设计。
5. 计数器竞争与假共享 → `LongAdder` 分段、采样代替全局锁。
6. 跨线程内存序 → acquire/release 语义的句柄发布，不进锁。
7. RSS 对账 → 定期跑，gap 趋势比单点数值更有诊断价值。

## 十、结论

三道分水岭：**分配器质量**（size class + 句柄间接层）、**回收节奏**（lazy free 思想 + tick 增量预算）、**监控对账**（accounted vs RSS）。现成借鉴对象：Netty `PooledByteBufAllocator`（jemalloc 移植）、Memcached slab、Redis 自身的采样淘汰与 active expire cycle。Yierdis 的对应实现入口见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)、[`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。
