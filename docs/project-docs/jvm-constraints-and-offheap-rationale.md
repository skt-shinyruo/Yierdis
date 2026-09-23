# JVM 约束与堆外内存取舍

## 一、这篇文档回答什么

两个背景问题：

1. 用 Java 实现 Redis 这类内存数据库，JVM 会在哪些方面拖后腿？
2. 选择堆外（native）存储之后，与 C 版 Redis 的内存管理差距还剩多少？

本文只给"为什么"层面的结论，不描述源码实现。Yierdis 的具体实现见 [`ffm-primer.md`](./ffm-primer.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`offheap-copy-behavior.md`](./offheap-copy-behavior.md)、[`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。文中停顿与开销数字均为经验估算值，用于量级判断，不作精确依据。

## 二、JVM 对内存数据库的影响

核心结论：**JVM 不拖累吞吐**——JIT 编译后热路径性能接近 C，NIO/epoll/Netty 做事件循环没有瓶颈，JDK 21 虚拟线程还能简化并发模型。真正的问题集中在内存模型上。

### 1. GC 停顿 → 尾延迟毛刺（最致命）

Redis 的卖点是亚毫秒级 P99，而 JVM GC 会 Stop-The-World：

| 停顿来源 | 量级（估算） |
| --- | --- |
| G1 Full GC（大堆最差情况） | 数百 ms |
| G1 Young GC（常规回收） | 几十 ms |
| ZGC / Shenandoah（并发回收） | < 1 ms |
| 对照：Redis 单命令 P99 预算 | ~0.2 ms |

C 版 Redis 没有 GC 问题，其停顿主要来自 fork RDB，可控得多。Java 版若不处理，客户端会周期性看到延迟尖刺。

### 2. 对象头开销 → 内存膨胀

每个 Java 对象带 12~16B 对象头（取决于是否启用压缩指针）外加 8B 对齐填充。一个 `Entry + String key + byte[] value` 的小 KV，堆内实际占用是原始数据的 3~5 倍。C 版 Redis 用 listpack、intset、0~9999 共享整数等定制编码把内存压到极致，堆内 Java 对象模型无法追平。

### 3. 堆内存不可精确计量 → 淘汰策略难做

`maxmemory` 与 LRU/LFU 淘汰要求精确的字节级记账，这对堆内对象几乎不可行（Instrumentation 或估算都不精确）；且数据压在堆上会使 GC 扫描成本随数据量线性增长。

### 4. 大堆惩罚

堆超过 32GB 会丢失压缩指针（CompressedOops），指针从 4B 变 8B 进一步加剧膨胀；堆越大 GC 越难调。正确姿势是**小堆 + 大堆外**。

### 5. 次要问题与无影响的部分

- JIT 预热：冷启动后前几千次请求偏慢，上线/压测前需要预热，或考虑 GraalVM Native Image。
- Safepoint 停顿：即使不 GC，线程到达安全点也有毫秒级抖动。
- 线程栈内存：每线程约 1MB，不能用一连接一线程模型。
- 无影响：吞吐与网络 IO（见本节开头）。

## 三、工程对策

| 问题 | 对策 |
| --- | --- |
| GC 停顿 | 用 ZGC / Shenandoah（停顿 < 1ms）；数据整体移出堆，使 GC 扫描面不再随数据量增长 |
| 内存膨胀 | 堆外存储：`DirectByteBuffer` 或 FFM（`Arena` + `MemorySegment`），slab 分配器 + 开放寻址哈希表（大 `byte[]`/`MemorySegment`，避免逐 key 对象） |
| 内存控制 | 堆外自行记账，`-XX:MaxDirectMemorySize` 限额，淘汰在堆外实现 |
| 手动内存泄漏风险 | 借鉴 Netty `ByteBuf` 引用计数、FFM Arena 显式关闭，把析构时机变确定 |

Yierdis 走的就是"小堆 + FFM 堆外"路线：region 分配入口与 Arena 选型见 [`ffm-primer.md`](./ffm-primer.md)，分配器与句柄见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)，maxmemory 记账与淘汰见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 四、堆外内存 vs C Redis 内存管理

Java 堆外 ≈ 把 C 的 `malloc/free` 手动内存管理搬回 JVM。**能力上基本等价**（C 的连续字节编码，Java 堆外都能逐字节复刻），但有几项 C 里的"免费午餐"需要自己造：

| 维度 | C Redis（malloc / jemalloc） | Java 堆外（DirectBuffer / FFM / Unsafe） |
| --- | --- | --- |
| 分配器 | jemalloc：size class、arena、线程缓存，碎片可控且 `INFO memory` 直接暴露碎片率 | JDK 只记字节总数，底层是裸 malloc，碎片不可见；要自己写 slab/buddy，或用 Netty `PooledByteBufAllocator`（jemalloc 思路的 Java 移植） |
| 寻址与结构 | 64 位裸指针，struct 内嵌指针随意互相指向 | `base + long 偏移`，"指针"是偏移量；struct 靠手工编码字段偏移（FFM `MemoryLayout`/`VarHandle` 可规范，但仍绕） |
| 释放时机 | `free()` 精确即时 | `DirectByteBuffer` 靠 Cleaner，要等 wrapper 被 GC 回收，时机不确定（堆内没压力时堆外可能先 OOM）；FFM `Arena.close()` 或 Netty 引用计数才是确定析构 |
| 限额与记账 | `zmalloc` 全局计数 + RSS | `-XX:MaxDirectMemorySize` 只管 DirectBuffer；Unsafe/FFM 不受它约束，淘汰策略要全程自己记账 |
| 安全检查 | 无，野指针直接 segfault 或悄悄写坏数据 | `ByteBuffer`/`MemorySegment` 默认有边界+生命周期检查（JIT 可消除一部分）；只有 `Unsafe` 和 C 一样裸奔 |
| 持久化快照 | **fork() + Copy-On-Write** 白捡 BGSAVE 与主从同步 | JVM 进程几乎不能 fork（大地址空间 + 线程/锁状态），必须自建增量快照或 COW 数据结构 |
| 排障工具 | gdb + core dump + jemalloc profile，内存现场全可见 | `jmap`/heap dump 对堆外完全失明，只有 NMT 粗粒度统计或 `pmap`，泄漏定位更难 |

风险形态也不同：堆外泄漏不会触发普通堆 OOM 的报警路径，`DirectBuffer` 的 GC 依赖释放还引入了时序不确定性。生产纪律是引用计数或 Arena 把析构变确定（对应 Yierdis 的 region 生命周期管理，见 [`native-memory-runtime.md`](./native-memory-runtime.md)）。

## 五、结论

- **教学/中等规模**：Java 实现 Redis 完全可行；堆外方案 + ZGC 即可拿到可用延迟。
- **追平 C 版 Redis**：难点不在 CPU 而在内存效率与 P99 稳定性，需要大量堆外工程。业界 Java 系 KV（Hazelcast、Apache Geode、Chronicle Map）走的都是同一条路。
- 工程分水岭就两件事：**分配器质量**和**快照方案**，其余是体力活。
