# Java Redis 级引擎 V1 设计

**日期：** 2026-03-23

## 目标

设计一个纯内存的 Java 引擎，在中等 value、混合读写负载下，能够在内存密度和尾延迟上具备与 Redis 竞争的可能性，同时把第一阶段实现范围控制在可构建、可验证的尺度内。

这里的 V1 目标不是“一个基于标准库集合实现的 Redis-like Java 服务”，而是“一个使用 Java 编写、但具备紧凑内存布局、低延迟、自主管理内存能力的内存引擎”，并对一组核心 Redis 语义提供支持。

## 问题总结

如果采用传统 JVM 设计，例如围绕 `HashMap<String, Object>`、装箱元数据和大量按元素分配的堆对象来组织数据，虽然可以做出 Redis 风格的行为语义，但在这个项目真正关心的两个指标上通常会失败：

1. 内存密度：对象头、引用、节点包装和重复字节拷贝会显著抬高单 key 开销。
2. 尾延迟：庞大的对象图和频繁分配会让 GC 成为 p99/p999 延迟上的显式参与者。

对这个项目来说，这些代价是不可接受的。因此，数据面不能依赖普通 Java 对象图作为稳态存储模型。

## 范围

### V1 范围内

- 单进程、单线程事件循环执行模型。
- 纯内存引擎。
- 以 RESP2 作为 V1 的规范协议，便于使用现有 Redis 工具做基础互操作验证和 benchmark 对比。
- Redis 风格核心类型：
  - `String`
  - `Hash`
  - `Set`
  - `List`
  - `ZSet`
- Redis 风格 TTL 支持：
  - lazy expiration
  - active expiration
- 面向 RESP 的 request/response 执行路径。
- 紧凑存储编码与单向编码升级。
- 面向数据面的自定义内存管理。
- 足以与 Redis 基线对比内存占用和尾延迟的 benchmark 与观测能力。

### 同一主线的后续阶段

以下内容不属于当前第一波落地范围，但明确属于同一主线下的后续 phase，而不是独立项目或主线之外内容：

- `maxmemory / eviction`
- `defragment`

### V1 范围外

- 持久化（`AOF`、`RDB`）
- 复制、集群或分片
- 多线程共享状态执行
- Lua / scripting
- ACL / TLS / PubSub
- 完整 Redis 命令覆盖
- 自动编码降级

## 非目标

- 不为了最快交付速度而牺牲内存布局质量。
- 不保留“主要依赖 Java 集合，只在局部补一点 off-heap”的热路径架构。
- 不在存储核心、allocator 和压测基础设施还没稳定时就追逐所有 Redis 特性。

## 设计约束

本设计明确建立在以下约束上：

1. 主要负载画像：
   - 中等大小 value
   - 混合读写
   - 更强调通用性，而不是极度特化的 microbenchmark 场景
2. 第一波数据结构范围：
   - `String / Hash / Set / ZSet / List`
3. 运行模型：
   - 单进程
   - 单线程事件循环
4. 持久化边界：
   - V1 不做持久化
5. 性能目标：
   - 内存密度与 p99/p999 延迟是第一等公民指标，而不是附属诊断项

## 协议与命令子集

V1 需要刻意收窄，但边界必须明确。后续 planning 不应该重新猜测哪些命令在范围内。

### 协议

- RESP2 是 V1 唯一要求支持的协议。
- 对于支持的命令子集，引擎应能被常见 Redis 兼容客户端使用。
- RESP3 与非 Redis 协议格式不在本设计范围内。

### 最小命令子集

#### Core / Keyspace / TTL

- `PING`
- `DEL`
- `TYPE`
- `EXPIRE`
- `PEXPIRE`
- `TTL`
- `PTTL`

#### `String`

- `GET`
- `SET`

#### `Hash`

- `HSET`
- `HGET`
- `HDEL`

#### `Set`

- `SADD`
- `SREM`
- `SISMEMBER`

#### `List`

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `LRANGE`

#### `ZSet`

- `ZADD`
- `ZREM`
- `ZRANGE`

超出这一子集的内容，除非在实现计划中被显式加入，否则一律视为后续范围。

## 推荐方案

采用一套拆分式架构：

- 控制面驻留在 heap
- 数据面按 off-heap-first 的紧凑模型设计

在不会主导稳态内存和尾延迟表现的地方，控制面可以放心使用普通 Java 对象，例如：

- 协议编解码状态
- 命令注册表
- 连接状态
- 日志
- 指标接线
- 测试工具链

数据面则不能依赖 Java 标准集合作为稳态存储表示：

- 主 keyspace
- TTL 索引
- 各类型负载数据
- 对象元数据
- 紧凑编码
- 升级后的 hash/set/zset/list 结构

这是能让项目继续朝“Redis 级内存密度与延迟”方向推进的最小正确架构，而不是过度设计。

## 架构概览

### 1. 执行模型

引擎运行在单线程事件循环上：

- 所有命令都在同一个 owner 线程上串行执行
- 热路径保持无锁
- 维护任务以增量方式挂靠在命令执行过程中推进

这让 V1 的正确性和尾延迟分析保持可控，也避免过早把存储引擎问题和并发控制问题混在一起。

### 2. 控制面与数据面

架构必须清晰分离这两类职责：

- 控制面：
  - 网络 I/O
  - 协议状态
  - 命令分发
  - 连接 / 会话状态
  - 诊断与观测接线
- 数据面：
  - keyspace 查找
  - TTL 所有权
  - 紧凑负载存储
  - 编码升级
  - 内存记账
  - 删除与回收

命令层只能通过语义化 API 与引擎交互，不直接操作 allocator、裸地址或底层存储布局。

## 核心组件

### `RedisServer`

拥有单线程事件循环，并负责驱动：

- 协议读取与解码
- 命令分发
- 每条命令附带的有界维护任务
- 响应写回

### `ConnectionContext`

驻留在 heap 上的连接级状态：

- 协议解析状态
- 输出缓冲
- 若后续加入事务，则承载事务 / 会话状态

### `CommandRegistry`

维护命令名到处理器的映射。这一层属于控制面。

### `DbEngine`

唯一的公开数据面入口。它暴露语义化操作，例如：

- `get/set/del`
- `expire/ttl`
- `hset/hget/hdel`
- `sadd/srem/smembers`
- `lpush/rpush/lpop/rpop/lrange`
- `zadd/zrem/zrange`

`DbEngine` 负责协调：

- `KeyspaceTable`
- `ExpireIndex`
- `MemoryManager`
- 按类型拆分的 `*Ops`

### `KeyspaceTable`

拥有主 key 字典：

- off-heap 开放寻址哈希表
- 面向 scan locality 优化的 slot 元数据
- 稳定的 `entryHandle` 引用
- 增量 rehash

### `ExpireIndex`

拥有 active expiration 的调度职责：

- 二级时间索引
- 不独立拥有 key
- 只解析到 entry handle

### `MemoryManager`

拥有 allocator 组合和内存统计：

- `EntryArena`
- `SmallObjectArena`
- `LargeObjectAllocator`

### `EntryLayout`

定义固定大小 entry header 的内存布局，并暴露基于静态偏移的读写助手。它更像一个 layout/access 模块，而不是传统 heap 对象模型。

### 类型专属 `Ops`

按类型拆分负载解释逻辑：

- `StringOps`
- `HashOps`
- `SetOps`
- `ListOps`
- `ZSetOps`

这些组件共享统一的 entry 生命周期协议和内存所有权规则。

## 推荐模块边界与类职责

为了避免后续实现再次退化成“一个超大 `DbEngine` + 若干辅助类”的结构，本设计建议在实现阶段遵守以下模块边界。这里的“模块”不强制对应 Maven module，更主要表达代码职责边界与依赖方向。

### 1. `server/runtime`

职责：

- 单线程事件循环
- RESP2 解码与回包
- 命令分发
- 每条命令后的 maintenance 调度
- 运行期参数与指标暴露

允许依赖：

- `engine/api`
- 少量 runtime-local 配置对象

不应直接依赖：

- allocator 具体实现
- keyspace slot 布局
- packed/large encoding 内部结构

说明：

`server/runtime` 是主线的最外层 owner。它驱动命令，但不拥有数据结构细节，也不绕过 facade 直接去操作 storage 或 memory。

### 2. `engine/api`

职责：

- 对命令层暴露语义化读写接口
- 屏蔽底层 handle、page、slot、extent
- 固定命令语义与失败语义边界

建议类：

- `DbEngine`
- `DbReadFacade`
- `DbWriteFacade`
- `DbMaintenanceFacade`

说明：

命令层应只看 facade，不应该直接拿到 `KeyspaceTable`、`MemoryManager`、`ExpireHeap` 或任何 `*Access` 类。这样后续调 layout、rehash、eviction、defragment 时，不需要回头改命令层。

### 3. `engine/core`

职责：

- 编排一次命令在数据面的完整执行
- 串联 keyspace、TTL、allocator、各 type ops
- 驱动 `Plan -> Build -> Commit -> Cleanup`

建议类：

- `DbCommandExecutor`
- `MutationPlanner`
- `MutationBuilder`
- `MutationCommitter`
- `DeletionCoordinator`
- `MaintenanceScheduler`

说明：

`engine/core` 是整个引擎的控制流中心，但它不直接决定字节布局，也不直接承担命令解析。它负责把“语义操作”落到“稳定的数据面执行序列”。

### 4. `engine/storage`

职责：

- 主 entry
- keyspace
- TTL heap
- rehash
- 随机候选与遍历基础能力

建议类：

- `EntryLayout`
- `EntryAccess`
- `KeyspaceTable`
- `KeyspaceRehasher`
- `ExpireHeap`
- `ExpireCoordinator`

说明：

`engine/storage` 只关心 entry、slot、handle、索引和删除一致性，不应该理解 `Hash`、`List`、`ZSet` 的高层命令语义。

### 5. `engine/memory`

职责：

- arena / allocator / extent
- handle 编码与校验
- page 利用率与碎片统计
- debug 检查与 dump

建议类：

- `MemoryManager`
- `HandleCodec`
- `EntryArena`
- `SmallObjectArena`
- `LargeObjectAllocator`
- `MemoryStatsTracker`
- `AllocatorDebugInspector`

说明：

`engine/memory` 不应该理解 Redis 命令，也不应该做任何命令级语义判断。它只对分配、释放、统计、校验负责。

### 6. `engine/types`

职责：

- `String / Hash / Set / List / ZSet` 的 encoding 解释
- packed -> large 升级
- 类型级 free / 复制 / streaming
- 类型级 memory usage 估算

建议类：

- `StringOps`
- `HashOps`
- `SetOps`
- `ListOps`
- `ZSetOps`

每个类型下进一步建议拆为：

- `XxxEncoding`
- `XxxPackedAccess`
- `XxxLargeAccess`
- `XxxUpgrade`
- `XxxFree`

说明：

不要把 packed 解析、large 结构维护、升级逻辑、删除逻辑、回包 streaming 全塞进一个 `XxxOps` 超大类。类型模块内部也必须继续分层。

### 推荐依赖方向

推荐维持以下依赖关系：

`server/runtime -> engine/api -> engine/core -> engine/storage + engine/memory + engine/types`

其中：

- `engine/types` 可以依赖 `engine/storage` 与 `engine/memory`
- `engine/storage` 可以依赖 `engine/memory`
- `engine/memory` 不反向依赖任何上层模块

### 推荐类图草案

下面这段不是最终 Java API，而是实现阶段建议遵守的骨架草案：

```java
interface DbEngine {
    DbReadFacade reads();
    DbWriteFacade writes();
    DbMaintenanceFacade maintenance();
}

final class DbCommandExecutor {
    private final KeyspaceTable keyspace;
    private final ExpireCoordinator expire;
    private final MemoryManager memory;
    private final StringOps strings;
    private final HashOps hashes;
    private final SetOps sets;
    private final ListOps lists;
    private final ZSetOps zsets;
}

final class KeyspaceTable { }
final class KeyspaceRehasher { }
final class ExpireHeap { }
final class ExpireCoordinator { }

final class MemoryManager { }
final class EntryArena { }
final class SmallObjectArena { }
final class LargeObjectAllocator { }

final class StringOps { }
final class HashOps { }
final class SetOps { }
final class ListOps { }
final class ZSetOps { }
```

这套类图的价值在于，它能直接支撑后续 implementation plan 的任务切分，不会让实现阶段又回到“所有逻辑都先塞到 `DbEngine` 里再说”。

## 内存模型

### 基于 Handle 的访问

引擎统一使用稳定的 64 位 handle，而不是把裸地址泄漏到高层。对外 API、keyspace、TTL heap、skiplist、各类 packed/large encoding 全部依赖 handle 作为统一引用形态。

推荐的 V1 handle 逻辑布局如下：

- 高 8 位：arena kind
- 接下来的 24 位：page id / extent id
- 接下来的 16 位：slot id 或 chunk id
- 低 16 位：generation

这不是唯一可能的编码方式，但它满足 V1 的几个硬要求：

- 能在 debug 模式下校验 handle 是否属于合法 arena/page
- 能在 free/reuse 后通过 generation 检测 stale handle
- 能在 rehash、编码升级和 value 替换时保持 entry handle 稳定
- 不要求上层知道真实地址，也不要求所有 arena 共享同一种物理布局

热路径内部可以把 handle 解码为地址，但系统边界必须保持为“基于 handle 的访问模型”。

### 对齐、页大小与基础分层

V1 先采用简单但可扩展的分层内存模型：

- 所有分配默认按 8 字节对齐。
- `EntryArena` 和 `SmallObjectArena` 使用固定 page 大小。
- `LargeObjectAllocator` 使用 extent 或大块区间管理。

推荐默认值：

- `EntryArena.pageSize = 16 KiB`
- `SmallObjectArena.pageSize = 16 KiB`
- `LargeObjectAllocator.extentSize = 64 KiB`

选择这组默认值的原因是：

- `EntryArena` 中一个 64B entry page 能容纳 256 个 entry，足够形成稳定局部性。
- `SmallObjectArena` 16 KiB page 能覆盖 packed hash/set/zset、list segment、小字符串块，而不会过早放大内部碎片。
- `LargeObjectAllocator` 用更大的 extent，有利于处理中大字符串和大 blob，同时把 page metadata 开销摊薄。

### Allocator 分工

V1 不应使用一套单一通用 allocator 策略来承载所有对象形状，而应显式拆分职责。

#### `EntryArena`

职责：

- 只分配固定大小主 entry
- page 内槽位密集排列
- free-list 回收
- 为 keyspace、TTL、value 索引提供稳定 entry handle

推荐规则：

- entry 固定目标大小为 64B
- page 内使用位图或空闲单链表管理空槽
- entry 不做变长扩容；任何变长数据都通过 `keyRef` / `valueRef` 指向其他 block

#### `SmallObjectArena`

职责：

- 存放 packed blob
- 存放小字符串块
- 存放 list segment
- 存放小型 skiplist node 或 member record

推荐 size class：

- `32 / 48 / 64 / 80 / 96 / 128 / 160 / 192 / 256 / 320 / 384 / 512 / 768 / 1024 / 1536 / 2048 / 4096`

这些值不要求第一次就完全固定，但 V1 至少要有：

- 对小对象分级
- 同 size class 复用
- page 内块回收
- 对每个 size class 单独统计利用率

#### `LargeObjectAllocator`

职责：

- 存储大字符串
- 存储大 packed blob
- 存储超出 `SmallObjectArena` 上限的 segment 或大型结构块

推荐规则：

- `> 4 KiB` 的对象默认转入 `LargeObjectAllocator`
- 维护空闲区间表
- 邻接 free block 合并
- 记录 extent 使用率与碎片率

V1 不必在第一天实现最复杂的 slab/buddy 组合，但必须做到：

- 大块不会反复挤占 small-object page
- 大块释放后可以被后续复用
- 大块碎片率可观测

### 内存块头与所有权

V1 的通用原则是：`entry` 拥有 key 与 value 的逻辑所有权，但不一定物理内嵌它们。

推荐基础块头规则：

- 不可变字节块（如 key、set member、部分 packed record）使用只读块头：
  - `len`
  - `kind`
  - `hash/cache`
- 可重写字符串块使用可扩容块头：
  - `len`
  - `cap`
  - `kind`
  - `bytes...`

V1 应避免为每一种数据结构定义完全无关的 block header。块头可以分为少数几类，而不是几十类。

### 分配与释放纪律

V1 的 allocator 必须支持以下写路径纪律：

1. 先分配新对象或新表示
2. 构建完成后再切换 entry 引用
3. 切换成功后释放旧对象
4. 删除路径必须 deterministic free，不依赖 GC

这意味着 allocator 除了 `alloc/free` 外，还要支持：

- debug generation 检查
- 大对象和小对象不同释放路径
- page-level 活跃对象计数
- shutdown 前统一泄漏扫描

### 必要的 Allocator 诊断能力

内存系统至少要暴露这些指标：

- `allocatedBytes`
- `activeBytes`
- `fragmentBytes`
- `residentPages`
- `objectCountByEncoding`
- 各 size class 利用率
- debug/test 模式下的退出时泄漏报告

除此之外，建议增加：

- `entryArenaPages`
- `smallArenaPagesByClass`
- `largeObjectCount`
- `largestFreeExtentBytes`
- `allocatorInternalOverheadBytes`

这些指标决定后续是否值得进入 `maxmemory/eviction` 阶段。

## Entry 布局

### 主 Entry Header

每个 primary entry 都是固定大小，V1 以 64B 为目标，先把热字段收紧在这一层：

```text
offset  size  field
0       1     type
1       1     encoding
2       2     flags
4       4     ttlVersion
8       4     keyLen
12      4     payloadLen
16      8     keyRef
24      8     valueRef
32      8     expireAtMillis
40      8     accessMeta
48      8     aux
56      8     reserved
```

字段语义建议如下：

- `type`：`STRING/HASH/SET/LIST/ZSET`
- `encoding`：该 type 当前的具体编码
- `flags`：删除标志、rehash 辅助位、统计位等
- `ttlVersion`：TTL heap 去重与惰性失效校验
- `keyLen`：key 长度，避免为常见路径额外解引用
- `payloadLen`：value 当前逻辑长度或 packed blob 总字节数
- `keyRef`：不可变 key 字节块句柄
- `valueRef`：值结构根句柄
- `expireAtMillis`：绝对时间戳，`0` 表示无 TTL
- `accessMeta`：用于后续 `LRU/LFU`、最近触碰时间、采样统计
- `aux`：根据 type/encoding 复用，例如元素个数、segment 数、score 计数

### Key Block 布局

key 应视为不可变字节块，建议布局：

```text
[u32 len][u64 hash64][bytes...][padding]
```

原因：

- key 比 value 更偏向只读
- key 常被多次比较
- key hash 可以缓存，减少重复计算

主字典、TTL、value 结构都不能再持有第二份 key bytes。

## 对象层级与内存所有权模型

为了让删除路径、编码升级、TTL 回收和后续 `defragment` 都能保持可推理，本设计建议明确把数据面对象划分为四层，而不是把“entry / blob / 节点 / handle”混在一起。

### 第一层：`Entry`

`Entry` 是逻辑根对象，也是主 key 生命周期的唯一 owner。

它负责：

- 拥有 key 的逻辑所有权
- 拥有 value 的逻辑所有权
- 持有 TTL 与 access metadata
- 作为 keyspace、TTL、eviction、defragment 的统一根引用

重要约束：

- `Entry` 自身固定大小
- `Entry` 不直接嵌入变长 key/value 字节
- `Entry` 在 rehash 期间必须保持稳定 handle

### 第二层：`KeyBlock`

`KeyBlock` 是只读变长对象，保存：

- key bytes
- 缓存 hash
- 长度

语义上由 `Entry` 唯一拥有，但可被以下组件共享读取：

- keyspace 查找
- TTL / eviction 候选比较
- 各 type ops 中的 debug/inspection 路径

重要约束：

- 第一波设计中，`KeyBlock` 视为不可变
- 不允许存在第二份同 key 字节副本
- 删除 `Entry` 时必须统一释放其 `KeyBlock`

### 第三层：`ValueRoot`

`ValueRoot` 是 `entry.valueRef` 指向的二级根对象。

它可能是：

- 小字符串块
- `PACKED_HASH` blob
- `PACKED_SET` blob
- `PACKED_ZSET` blob
- `List` 的头部根对象
- large `Hash/Set/ZSet` 的根结构

重要约束：

- `ValueRoot` 由 `Entry` 唯一拥有
- 编码升级的本质是 `Entry` 从旧 `ValueRoot` 切换到新 `ValueRoot`
- 删除路径总是从 `ValueRoot` 开始向下递归 free

### 第四层：`Node / Segment / Record`

这是具体编码内部的三级对象，例如：

- `Hash` 的 `fieldRecord`
- `Set` 的 `memberRecord`
- `List` 的 `segment`
- `ZSet` 的 `zsetRecord`
- `skiplist node`

这些对象不应被命令层、TTL、keyspace 直接感知，只由所属 `ValueRoot` 或对应 `*Ops` 管理。

### 所有权规则

建议统一采用以下规则：

- `Entry` 逻辑拥有 `KeyBlock` 与 `ValueRoot`
- `ValueRoot` 逻辑拥有其下的 `Node / Segment / Record`
- 索引结构持有“可达引用”，但不持有“释放所有权”

例如：

- keyspace slot 持有 `entryHandle`，但不拥有 entry
- TTL heap node 持有 `entryHandle`，但不拥有 entry
- large `ZSet` 中的 dict 和 skiplist 都引用同一个 `zsetRecord`，但不应各自都把自己视为 payload owner

### 删除顺序

统一删除顺序建议写死为：

1. 从外部索引摘除可见性
2. 释放 `ValueRoot` 及其子对象
3. 释放 `KeyBlock`
4. 释放 `Entry`

这里的“外部索引”包括：

- keyspace
- TTL heap 的逻辑可见性
- `Hash/Set/ZSet/List` 内部子索引

### 可移动性规则

为了给后续 `defragment` 留出稳定边界，建议现在就把对象的默认可移动性写清楚：

- `Entry`：默认不可移动，至少在 `Phase 0 ~ Phase 5` 保持稳定
- `KeyBlock`：第一波默认不可移动
- `ValueRoot`：部分可移动，视编码而定
- `Node / Segment / Record`：只在各自结构实现明确支持的情况下可移动

这意味着：

- 第一波 defragment 优先移动 `ValueRoot` 和 packed/segment 对象
- 不要求一开始就支持“全对象可移动”
- 任何对象是否可移动，都必须由所属 `*Ops` 显式声明

## 核心状态机

本设计建议把当前主线中最关键的四条状态机写死在设计文档里，而不是留到实现阶段靠隐式约定维持。

### `Entry` 生命周期状态机

建议的最小状态集合：

- `EMPTY`
- `LIVE`
- `EXPIRED_PENDING_DELETE`
- `DELETING`
- `DELETED`

含义：

- `EMPTY`：arena 槽位尚未被分配为有效 entry
- `LIVE`：正常对外可见
- `EXPIRED_PENDING_DELETE`：已确认过期，但尚未完成统一删除
- `DELETING`：正在执行删除路径，防止重复 free
- `DELETED`：已完成逻辑删除，仅用于 debug/校验，随后回收到 arena

允许的迁移：

- `EMPTY -> LIVE`
- `LIVE -> EXPIRED_PENDING_DELETE`
- `LIVE -> DELETING`
- `EXPIRED_PENDING_DELETE -> DELETING`
- `DELETING -> DELETED`
- `DELETED -> EMPTY`

不允许的迁移：

- `EXPIRED_PENDING_DELETE -> LIVE`
- `DELETING -> LIVE`
- `DELETED -> LIVE`

#### `Entry` 状态约束表

| 状态 | 进入条件 | 允许的出边 | 负责方 | 必须保持的约束 |
|------|----------|------------|--------|----------------|
| `EMPTY` | arena 槽位尚未被分配 | `LIVE` | `EntryArena` / `DbCommandExecutor` | 槽位不能被 keyspace、TTL、value 结构引用 |
| `LIVE` | entry 已完成构建并对外可见 | `EXPIRED_PENDING_DELETE`、`DELETING` | `DbCommandExecutor` / `ExpireCoordinator` | `keyRef`、`valueRef`、`type`、`encoding` 必须自洽；对命令路径可见 |
| `EXPIRED_PENDING_DELETE` | TTL 检查确认过期但尚未完成删除 | `DELETING` | `ExpireCoordinator` / `DeletionCoordinator` | 不再作为正常命中结果返回给读写命令 |
| `DELETING` | 已进入统一删除路径 | `DELETED` | `DeletionCoordinator` | 不能再次进入删除；所有外部索引必须先摘可见性 |
| `DELETED` | 逻辑删除已完成 | `EMPTY` | `EntryArena` | 仅允许 debug/校验短暂可见，随后回收 |

#### `Entry` 删除收口规则

- 从 `LIVE` 或 `EXPIRED_PENDING_DELETE` 进入删除路径时，必须先摘除 keyspace 可见性。
- 只有在 `ValueRoot` 与 `KeyBlock` 都成功释放后，entry 才能进入 `DELETED`。
- `DELETED -> EMPTY` 的回收动作不得保留任何可达引用，否则 debug 校验必须失败。

### `Mutation` 状态机

建议的最小状态集合：

- `PLAN`
- `RESERVE`
- `BUILD`
- `COMMIT`
- `RELEASE_OLD`
- `DONE`
- `ABORT`

含义：

- `PLAN`：查找、类型判断、升级判断、预算估算
- `RESERVE`：为增长写预留预算
- `BUILD`：构建新 value 或新 encoding
- `COMMIT`：切换 entry 引用
- `RELEASE_OLD`：释放旧对象
- `DONE`：完成
- `ABORT`：失败收口

关键约束：

- `COMMIT` 之前旧值必须仍然可读
- `RELEASE_OLD` 只能发生在 `COMMIT` 成功后
- `ABORT` 必须能从 `PLAN/RESERVE/BUILD` 任一点收口
- 不允许在“半提交”状态下回退成旧语义

#### `Mutation` 状态转移表

| 状态 | 入口条件 | 成功出边 | 失败出边 | 负责方 | 资源责任 |
|------|----------|----------|----------|--------|----------|
| `PLAN` | 命令已完成语义解析并进入数据面 | `RESERVE` 或直接 `BUILD` | `ABORT` | `MutationPlanner` | 只允许读取与估算，不允许分配不可回滚资源 |
| `RESERVE` | 该 mutation 可能增长内存或进入 `Phase 6` 预算路径 | `BUILD` | `ABORT` | `MutationPlanner` / `MemoryManager` | 只能持有 reservation，不允许修改 entry 引用 |
| `BUILD` | 预算已通过，允许构建新表示 | `COMMIT` | `ABORT` | `MutationBuilder` / `*Ops` | 可以分配新对象，但旧对象仍必须保持可读 |
| `COMMIT` | 新表示已经完整可用 | `RELEASE_OLD` | 无正常回滚出边 | `MutationCommitter` | 负责切换 `entry.valueRef`、`encoding`、`payloadLen`、`aux` 等字段 |
| `RELEASE_OLD` | `COMMIT` 已完成 | `DONE` | `DONE`（记录降级告警并进入一致性补救） | `DeletionCoordinator` / `*Free` | 释放旧对象、结束 reservation、更新统计 |
| `DONE` | mutation 逻辑完成 | 无 | 无 | `DbCommandExecutor` | 返回结果，允许下条命令进入 |
| `ABORT` | `PLAN/RESERVE/BUILD` 任一阶段失败 | 无 | 无 | `DbCommandExecutor` | 统一 rollback reservation、释放新分配但未提交对象 |

#### `Mutation` 失败收口表

| 失败点 | 必须执行的收口动作 | 绝对禁止 |
|--------|-------------------|----------|
| `PLAN` | 返回错误，不保留任何部分状态 | 修改 entry、写入 keyspace、写入 TTL |
| `RESERVE` | 释放 reservation，返回稳定失败 | 已持有 reservation 却进入 `BUILD` |
| `BUILD` | 释放新分配对象、rollback reservation、保留旧值 | 修改旧 `valueRef`、部分提交新 `encoding` |
| `COMMIT` 前的最后校验 | 若校验失败则回到 `ABORT` | 在校验失败后继续进入 `RELEASE_OLD` |
| `RELEASE_OLD` | 允许记录告警并进入补救，但逻辑提交不能回滚 | 因旧对象释放失败而把 entry 回退到旧值 |

#### `Mutation` 幂等要求

- `ABORT` 必须幂等。重复调用不得重复 free 或重复 rollback reservation。
- `RELEASE_OLD` 对同一旧对象只能成功一次；若被重复调用，必须在 debug 模式下被发现。
- `COMMIT` 必须具备“只切换一次”的保护，防止同一 mutation plan 被重复提交。

### `Rehash` 状态机

建议的最小状态集合：

- `IDLE`
- `PREPARE_NEW_TABLE`
- `MIGRATING`
- `CUTOVER`
- `CLEANUP`

含义：

- `PREPARE_NEW_TABLE`：分配新表并初始化
- `MIGRATING`：每条命令推进少量 bucket 迁移
- `CUTOVER`：old table 已迁完，切换主表引用
- `CLEANUP`：释放 old table

关键约束：

- `MIGRATING` 期间 lookup 必须对新旧表一致可见
- insert 一律进入新表
- delete 必须能从新旧表一致删除
- TTL、eviction 和 defragment 都不能依赖 slot 稳定，只能依赖 `entryHandle`

#### `Rehash` 状态转移表

| 状态 | 入口条件 | 成功出边 | 失败出边 | 负责方 | 资源责任 |
|------|----------|----------|----------|--------|----------|
| `IDLE` | 当前没有 rehash 任务 | `PREPARE_NEW_TABLE` | 无 | `KeyspaceRehasher` | 旧表是唯一可见表 |
| `PREPARE_NEW_TABLE` | 装载因子或 probe 分布触发扩容 | `MIGRATING` | `IDLE` | `KeyspaceRehasher` / `MemoryManager` | 分配新表，但旧表仍保持完整可用 |
| `MIGRATING` | 新表已可用 | `CUTOVER` | 保持 `MIGRATING` 并在下轮重试 | `KeyspaceRehasher` | 每轮只迁移预算内 bucket；lookup 要查双表 |
| `CUTOVER` | oldTable 已迁移完 | `CLEANUP` | 保持 `CUTOVER` 并重试切换 | `KeyspaceRehasher` | 切换主表引用，但 oldTable 仍暂存，直到 cleanup |
| `CLEANUP` | 主表已切换成功 | `IDLE` | 保持 `CLEANUP` 并重试释放 | `KeyspaceRehasher` / `MemoryManager` | 释放 oldTable，清空 rehash 元数据 |

#### `Rehash` 运行期约束

- `MIGRATING` 期间：
  - lookup 顺序固定为“先新表，后旧表”
  - insert 固定进入新表
  - delete 必须能命中新旧表中的任一位置
- 任意时刻都不能要求 TTL、eviction、defragment 通过 slot 地址识别对象；它们只能使用 `entryHandle`。
- rehash 预算耗尽不是失败，而是正常的 `PAUSE/CONTINUE` 型行为，下条命令继续推进。

#### `Rehash` 失败收口规则

- `PREPARE_NEW_TABLE` 分配失败时，必须原地回到 `IDLE`，旧表保持完全可用。
- `MIGRATING` 中单个 bucket 迁移失败时，不允许丢失 oldTable 内容；必须保持当前双表状态，等待下一轮重试。
- `CUTOVER` 成功前，不允许释放 oldTable。
- `CLEANUP` 失败时，可以延后释放 oldTable，但不允许再次把主表引用切回去。

### `Defragment` 状态机

建议的最小状态集合：

- `IDLE`
- `SELECT_SOURCE_PAGE`
- `MOVE_OBJECT`
- `REWRITE_REF`
- `FREE_OLD`
- `RELEASE_PAGE`
- `PAUSE`

含义：

- `SELECT_SOURCE_PAGE`：选择低利用率 page
- `MOVE_OBJECT`：复制活对象到新位置
- `REWRITE_REF`：更新 owning reference
- `FREE_OLD`：释放旧对象
- `RELEASE_PAGE`：当 page 被清空后回收
- `PAUSE`：预算耗尽，等待下一轮 maintenance

关键约束：

- `REWRITE_REF` 成功前不能 free old object
- source page 只有在活对象数归零后才能 `RELEASE_PAGE`
- defragment 与 split/merge/rehash 的交错必须通过显式状态检查保护，而不是靠“应该不会同时发生”的隐式假设

#### `Defragment` 状态转移表

| 状态 | 入口条件 | 成功出边 | 失败出边 | 负责方 | 资源责任 |
|------|----------|----------|----------|--------|----------|
| `IDLE` | 当前没有可运行 defragment 任务 | `SELECT_SOURCE_PAGE` | 无 | `MaintenanceScheduler` / `AllocatorDebugInspector` | 不持有任何迁移上下文 |
| `SELECT_SOURCE_PAGE` | 触发信号满足且预算允许 | `MOVE_OBJECT` 或 `PAUSE` | `IDLE` | `MaintenanceScheduler` | 只选 page，不移动对象 |
| `MOVE_OBJECT` | 已锁定 source/target page | `REWRITE_REF` | `PAUSE` 或 `IDLE` | `MemoryManager` / `*Ops` | 复制新对象，但旧对象仍保持有效 |
| `REWRITE_REF` | 新对象已构建完成 | `FREE_OLD` | `PAUSE` 或 `IDLE` | `*Ops` / owning container | 更新 owning entry 或 container 引用 |
| `FREE_OLD` | 引用已重写成功 | `RELEASE_PAGE` 或 `SELECT_SOURCE_PAGE` | `PAUSE` | `MemoryManager` | 释放旧对象，更新 page 活跃计数 |
| `RELEASE_PAGE` | source page 活对象数已归零 | `SELECT_SOURCE_PAGE` 或 `IDLE` | `PAUSE` | `MemoryManager` | 回收整页并更新碎片统计 |
| `PAUSE` | 达到对象数/字节数/时间预算上限 | `SELECT_SOURCE_PAGE` 或恢复到先前状态 | `IDLE` | `MaintenanceScheduler` | 保存足够上下文，保证下轮可继续或放弃 |

#### `Defragment` 失败与中断收口表

| 失败点 | 必须执行的收口动作 | 绝对禁止 |
|--------|-------------------|----------|
| `SELECT_SOURCE_PAGE` | 丢弃本轮候选，回到 `IDLE` 或 `PAUSE` | 记录了 source page 却不校验其当前状态 |
| `MOVE_OBJECT` | 释放新分配位置，保留旧对象 | 已复制部分数据却把旧对象标为无效 |
| `REWRITE_REF` | 保留旧引用，释放新对象或进入 `PAUSE` | 更新了一半 owning reference 后继续 free old |
| `FREE_OLD` | 若旧对象释放失败，记录告警并保留后续补救上下文 | 因释放失败把引用回写到旧对象 |
| `RELEASE_PAGE` | 若 page 仍有活对象，必须放弃释放并回到 `SELECT_SOURCE_PAGE` 或 `PAUSE` | 活对象未清零却回收整页 |

#### `Defragment` 并发交错保护

虽然主线是单线程事件循环，但 defragment 仍然会与其他维护行为交错出现，因此必须定义显式禁止条件：

- 正在参与 `split/merge` 的 `List segment` 不进入 `MOVE_OBJECT`
- 正在被 rehash 改写的结构根对象不进入迁移
- 正在 `COMMIT/RELEASE_OLD` 的 mutation 对象不进入 defragment
- 对同一 owning entry，不允许同时存在“mutation builder 持有的新对象”和“defragment 持有的新对象”

这些保护必须由显式状态位或上下文登记实现，而不是依赖“代码路径通常不会撞上”的经验假设。

## Keyspace 设计

### 哈希表策略

主字典采用 off-heap、开放寻址、`Robin Hood` probing。

选择理由：

- 相比链地址法，更节省指针和节点对象
- 更有利于 cache locality
- 探测长度分布更平滑，减少 lookup 长尾

### Slot 布局

推荐每个 slot 采用 16B 级别布局：

```text
[u64 entryHandle][u32 hashFingerprint][u16 probeDistance][u16 flags]
```

解释：

- `entryHandle`：主引用
- `hashFingerprint`：快速过滤，减少无意义 key bytes 比较
- `probeDistance`：支持 Robin Hood 交换与诊断
- `flags`：空槽、rehash 标记、保留位

如果后续 benchmark 证明 `u32 fingerprint` 不够，可以扩大；V1 不应过度压缩到影响实现清晰度。

### 查找路径

lookup 路径应固定为：

1. 计算 `hash64`
2. 定位 home slot
3. 按 Robin Hood 规则顺序探测
4. 先比 `hashFingerprint`
5. 再解引用 `entryHandle`
6. 比较 `keyLen + key bytes`

遇到以下条件即可提前停止：

- 空槽
- 当前 slot 的 `probeDistance` 小于当前查找距离

这能避免不必要的尾部长探测。

### 插入路径

插入规则：

1. 先做查找，确认 key 是否已存在
2. 若不存在，分配新 entry 与 key block
3. 通过 Robin Hood 插入
4. 若发生冲突，则比较 `probeDistance`，必要时交换 slot 内容

插入前不应直接分配大型 value。应先尽可能确定 keyspace 插入是否能成功，再进入完整 mutation。

### 删除路径

V1 推荐使用 `backward-shift deletion`，而不是长期保留 tombstone。

原因：

- 单线程下实现可控
- 避免 tombstone 持续累积
- 减少对 lookup 方差的长期污染

删除步骤：

1. 找到目标 slot
2. 从 keyspace 摘掉该 slot
3. backward shift 后续 cluster
4. 释放 entry 持有的 key/value

### 负载因子与扩容

推荐阈值：

- 装载因子 `> 0.80` 时触发增长 rehash
- 平均 probe distance 或 p99 probe distance 明显恶化时也允许提前 rehash
- V1 不强制做收缩；收缩可作为后续优化

### 增量 Rehash

V1 必须是“两张表 + 增量迁移”，不能全表阻塞式 rehash。

推荐模型：

- 保持 `oldTable` 和 `newTable`
- 每条命令执行时推进固定 bucket 预算，例如 `1~64` 个 bucket
- lookup 在 rehash 期间先查新表，再查旧表
- insert 一律进入新表
- oldTable bucket 迁移完成后释放 oldTable

建议把 rehash 预算做成 runtime 可调参数，例如：

- `rehashBucketsPerCommand`
- `rehashNanosBudgetPerCommand`

### Rehash 与删除、TTL 的交互

- entry handle 在 rehash 期间必须保持稳定
- TTL heap 只认识 entry handle，不认识 slot
- 任何删除都先从 keyspace 摘 entry，再让 TTL heap 节点自然 stale
- rehash 期间的 lookup 与 delete 都必须对新旧表一致可见

这意味着 TTL 与 keyspace 可以独立演进，不会因为 rehash 引入额外 key 拷贝。

## TTL 设计

### Lazy Expiration

`expireAt` 直接内联在 primary entry 中。读写命令在命中 entry 后，优先做一次轻量 TTL 判断：

1. `expireAt == 0`，直接返回未过期
2. `now < expireAt`，直接继续
3. `now >= expireAt`，执行同步删除路径

lazy expiration 应发生在：

- `GET`
- `HGET`
- `SISMEMBER`
- `LRANGE`
- `ZRANGE`
- 覆盖写前的旧值检查

### Active Expiration

active expiration 使用 off-heap min-heap 作为二级时间索引。

heap node 建议布局：

```text
[i64 expireAtMillis][u64 entryHandle][u32 ttlVersion][u32 reserved]
```

TTL 写入路径：

1. 更新 `entry.expireAtMillis`
2. `entry.ttlVersion++`
3. 压入新 heap node

旧 heap node 不做原地删除，只在 pop 时校验：

- handle 是否仍有效
- `ttlVersion` 是否匹配
- entry 当前 `expireAtMillis` 是否等于该 node

只要有任一条件不匹配，就把该 heap node 视为 stale 节点丢弃。

### Active Expire 执行预算

V1 不使用单独的高复杂度过期线程，而由事件循环推进：

- 每条命令后推进少量 active expire
- 在空闲 tick 中补额外预算

推荐默认预算：

- 每条命令最多处理 `8~64` 个 heap node
- 或限制为 `50~200` 微秒 CPU 预算

这样做的目的不是“尽快清完 TTL”，而是“不让 TTL 清理成为尾延迟尖刺来源”。

### 删除路径与 TTL 统计

真正的过期删除必须：

1. 从 keyspace 移除
2. 释放 value
3. 释放 key block
4. 释放 entry
5. 更新 `expiredKeys` 等统计

TTL heap 本身不直接拥有 key 或 value，因此不会参与最终 free，只负责调度。

## 类型与编码策略

### 通用原则

所有 value 都统一表示为：

- `type`
- `encoding`
- `valueRef`

通用编码原则：

- 小对象优先压缩内存密度
- 大对象优先稳定读写复杂度
- 编码升级只做单向，不自动降级
- 删除路径必须从 encoding 根句柄开始，递归释放

### 类型级编码状态机总原则

为了让不同类型的实现策略保持一致，本设计建议所有类型都遵守统一的编码级状态约束：

- 每个 `Entry` 在任一时刻只能有一个当前 `encoding`
- `encoding` 的切换只能通过 `Build -> Commit -> Release Old` 完成
- packed -> large 是单向升级
- 删除路径总是从当前 `encoding` 对应的 `ValueRoot` 开始
- 升级失败时必须保留旧 `encoding` 完整可读

可以把类型级编码状态统一理解为：

- `ABSENT`
- `PACKED`
- `LARGE`
- `DELETING`
- `DELETED`

其中：

- `ABSENT` 表示 key 不存在
- `PACKED` 表示当前 key 采用紧凑编码
- `LARGE` 表示当前 key 采用大对象编码
- `DELETING` / `DELETED` 复用前面 `Entry` 生命周期规则

统一允许的迁移：

- `ABSENT -> PACKED`
- `ABSENT -> LARGE`
- `PACKED -> LARGE`
- `PACKED -> DELETING`
- `LARGE -> DELETING`
- `DELETING -> DELETED`

统一禁止的迁移：

- `LARGE -> PACKED`
- `DELETED -> PACKED`
- `DELETED -> LARGE`

### `String`

#### 编码 1：`EMBSTR-like`

这里的 `EMBSTR-like` 不是指“值字节真正嵌进 64B entry header”，而是指：

- 值块很小
- 值块从 `SmallObjectArena` 分配
- 生命周期与 entry 强绑定
- 不预留过大 capacity

建议默认阈值：

- `len <= 64B` 时优先 `EMBSTR-like`

推荐布局：

```text
[u16 len][u16 flags][bytes...][padding]
```

适用场景：

- 高频 `GET`
- 稳定小值
- 不做反复追加

#### 编码 2：`RAW`

当 value 较大，或后续命令子集引入 `APPEND/SETRANGE` 等增长写入时，使用 `RAW`。

推荐布局：

```text
[u32 len][u32 cap][bytes...][padding]
```

建议规则：

- `len > 64B` 时默认进入 `RAW`
- 若未来引入可增长字符串操作，小字符串第一次增长时可从 `EMBSTR-like` 升级为 `RAW`

#### `String` 写路径细节

- `SET` 覆盖时总是先构建新块，再切换 `valueRef`
- `GET` 不复制到长期 heap 对象；尽量直接流式写回
- `DEL` / 过期删除直接 free string block

#### `String` 编码状态表

| 当前状态 | 触发条件 | 目标状态 | 负责方 | 备注 |
|----------|----------|----------|--------|------|
| `ABSENT` | `SET` 写入且 `len <= 64B` | `EMBSTR-like` | `StringOps` | 默认走小块分配 |
| `ABSENT` | `SET` 写入且 `len > 64B` | `RAW` | `StringOps` | 直接走连续块 |
| `EMBSTR-like` | 覆盖写且新值仍小 | `EMBSTR-like` | `StringOps` | 构建新小块后替换 |
| `EMBSTR-like` | 覆盖写或增长写导致超阈值 | `RAW` | `StringOps` | 单向升级 |
| `RAW` | 覆盖写 | `RAW` | `StringOps` | 不做自动降级 |
| `EMBSTR-like/RAW` | `DEL`、过期删除 | `DELETING` | `DeletionCoordinator` | 随后进入统一删除 |

#### `String` 删除与回滚表

| 场景 | 必须执行的动作 | 绝对禁止 |
|------|----------------|----------|
| `EMBSTR-like` 覆盖失败 | 释放新小块，保留旧 `valueRef` | 部分覆盖旧块内容 |
| `EMBSTR-like -> RAW` 升级失败 | 释放新 `RAW` 块，保留旧小块 | 已切换 `encoding` 后再回滚 |
| `RAW` 覆盖失败 | 释放新块，旧 `RAW` 保持完整可读 | 原地修改旧 `RAW.cap/len` 后失败 |
| `String` 删除 | 从 entry 摘 `valueRef` 后 free 块 | free 后仍把旧块作为可读结果返回 |

### `Hash`

#### 编码 1：`PACKED_HASH`

小 hash 使用 listpack-like 顺序 blob：

```text
[u16 count][u16 bytesUsed][u32 reserved][fieldLen][fieldBytes][valueLen][valueBytes]...
```

设计选择：

- 按顺序存储，不再为每个 field 建立单独节点
- `HGET/HDEL` 线性扫描
- `HSET` 可能重写整块或重建新块

推荐升级阈值：

- `count > 64`
- 任一 `field` 或 `value` 长度 `> 128B`
- packed blob 总大小 `> 4 KiB`

#### 编码 2：`HT_HASH`

大 hash 升级到 off-heap 哈希表。

建议结构：

- 主 table：`fieldHash -> fieldRecordHandle`
- `fieldRecord`：
  - field bytes
  - value ref
  - value len

这样可以做到：

- field bytes 只存一份
- 大 value 仍可复用 string-like block
- `HSET/HGET/HDEL` 复杂度稳定

#### 升级路径

升级必须是原子性的：

1. 从 packed blob 读取全部 field/value
2. 构建新 hash table
3. 切换 entry 的 `encoding/valueRef`
4. 释放旧 packed blob

#### `Hash` 编码状态表

| 当前状态 | 触发条件 | 目标状态 | 负责方 | 备注 |
|----------|----------|----------|--------|------|
| `ABSENT` | `HSET` 首次写入且对象仍小 | `PACKED_HASH` | `HashOps` | 默认先走 packed |
| `ABSENT` | 首次写入即超阈值 | `HT_HASH` | `HashOps` | 直接 large |
| `PACKED_HASH` | `HSET/HDEL` 后仍在阈值内 | `PACKED_HASH` | `HashOps` | 允许整块重建 |
| `PACKED_HASH` | 触发升级阈值 | `HT_HASH` | `HashUpgrade` | 单向升级 |
| `HT_HASH` | 后续增删改 | `HT_HASH` | `HashOps` | 不自动降级 |
| `PACKED_HASH/HT_HASH` | 删除或过期 | `DELETING` | `DeletionCoordinator` | 统一走释放路径 |

#### `Hash` 删除与回滚表

| 场景 | 必须执行的动作 | 绝对禁止 |
|------|----------------|----------|
| `PACKED_HASH` 重建失败 | 释放新 blob，保留旧 blob | 原地改旧 blob 再失败 |
| `PACKED_HASH -> HT_HASH` 升级失败 | 释放新 hash table，保留旧 blob | 切换 `encoding` 后再尝试回退 |
| `HT_HASH` 写入失败 | 释放新 `fieldRecord/value`，旧 table 保持一致 | 只插入 dict 或只插入 payload 的半状态 |
| `Hash` 删除 | 先释放 field/value 记录，再释放根结构 | 先 free 根结构再访问子记录 |

### `Set`

#### 编码 1：`PACKED_SET`

小 set 使用紧凑 member blob。

建议布局：

```text
[u16 count][u16 bytesUsed][u32 reserved][memberLen][memberBytes]...
```

V1 推荐把 members 按字典序排序存放：

- membership 判断可二分或半二分 + 顺序比较
- 行为顺序依然不承诺对外稳定，只是内部优化

推荐升级阈值：

- `count > 128`
- 任一 member 长度 `> 64B`
- packed blob 总大小 `> 4 KiB`

#### 编码 2：`HT_SET`

大 set 升级到 off-heap 哈希表。

建议结构：

- table slot 存 `memberHandle`
- member record 只保存一份 member bytes

`SADD/SREM/SISMEMBER` 都走哈希查找，不需要为 set 额外引入树结构。

#### `Set` 编码状态表

| 当前状态 | 触发条件 | 目标状态 | 负责方 | 备注 |
|----------|----------|----------|--------|------|
| `ABSENT` | `SADD` 首次写入且对象仍小 | `PACKED_SET` | `SetOps` | 默认 packed |
| `ABSENT` | 首次写入即超阈值 | `HT_SET` | `SetOps` | 直接 large |
| `PACKED_SET` | `SADD/SREM` 后仍在阈值内 | `PACKED_SET` | `SetOps` | 允许整块重建 |
| `PACKED_SET` | 触发升级阈值 | `HT_SET` | `SetUpgrade` | 单向升级 |
| `HT_SET` | 后续增删改 | `HT_SET` | `SetOps` | 不自动降级 |
| `PACKED_SET/HT_SET` | 删除或过期 | `DELETING` | `DeletionCoordinator` | 统一释放 |

#### `Set` 删除与回滚表

| 场景 | 必须执行的动作 | 绝对禁止 |
|------|----------------|----------|
| `PACKED_SET` 重建失败 | 释放新 member blob，保留旧 blob | 原地修改旧 blob 后失败 |
| `PACKED_SET -> HT_SET` 升级失败 | 释放新 table / member record，保留旧 blob | 出现一半 member 在新表、一半还在旧 blob |
| `HT_SET` 写入失败 | 释放新 member record，旧 table 保持可读 | 写入 slot 后找不到对应 member bytes |
| `Set` 删除 | 先释放 members，再释放根结构 | free 根结构后继续枚举 members |

### `List`

#### 编码策略：`QUICKLIST-like`

V1 不使用传统链表，也不直接使用单块超大 packed array。推荐使用 quicklist-like 分段链表。

每个 segment 维护：

- `prev`
- `next`
- `count`
- `usedBytes`
- `headOffset`
- `tailOffset`
- payload block

segment payload 是一个“小型双端块”，而不是只能一端写入的纯数组。这样 `LPUSH/RPUSH/LPOP/RPOP` 不需要每次整体搬移。

#### Segment 默认策略

推荐默认目标：

- 单个 segment 目标负载 `8 KiB`
- 硬上限 `16 KiB`
- 元素个数上限 `128`

推荐 split / merge 规则：

- 超过 `8 KiB` 目标值且增长明显时，优先 split
- 相邻两个 segment 合并后仍小于 `8 KiB` 时，可在后台 merge
- merge 不要求每次命令都执行，可延后到维护阶段

#### `LRANGE` 路径

`LRANGE` 应按 segment 顺序遍历：

- 先跳过整个 segment 的元素计数
- 命中范围后再解析 segment 内 payload

不要把 list 先完全 materialize 到 heap 再回包。

#### `List` 编码状态表

V1 主线下 `List` 只定义一种主编码：`QUICKLIST-like`。它内部仍然存在 segment 级局部状态机。

| 当前状态 | 触发条件 | 目标状态 | 负责方 | 备注 |
|----------|----------|----------|--------|------|
| `ABSENT` | 首次 `LPUSH/RPUSH` | `QUICKLIST-like` | `ListOps` | 构建首个 root + segment |
| `QUICKLIST-like` | `LPUSH/RPUSH` 未超 segment 阈值 | `QUICKLIST-like` | `ListOps` | 原 segment 内写入 |
| `QUICKLIST-like` | segment 超阈值 | `QUICKLIST-like` | `ListOps` | 触发 split，但 encoding 不变 |
| `QUICKLIST-like` | 相邻 segment 可合并 | `QUICKLIST-like` | `ListOps` / `MaintenanceScheduler` | merge 是内部整理，不算编码切换 |
| `QUICKLIST-like` | 删除或过期 | `DELETING` | `DeletionCoordinator` | 统一释放 segment 链 |

#### `List` segment 局部状态

每个 segment 建议再有一条局部状态机：

- `ACTIVE`
- `SPLITTING`
- `MERGING`
- `DETACHED`
- `FREED`

关键约束：

- `SPLITTING` 中不能进入 defragment
- `MERGING` 中不能再次被选为 merge 目标
- `DETACHED` 后才允许真正 free

#### `List` 删除与回滚表

| 场景 | 必须执行的动作 | 绝对禁止 |
|------|----------------|----------|
| push 导致 split 失败 | 释放新 segment，保留旧链结构 | 改坏前后指针后再中断 |
| merge 失败 | 保持两段原状，回滚临时连接 | 一端已摘链，另一端未恢复 |
| `LRANGE` 流式输出中遇到并发维护 | 由于单线程语义，应保证结构在本次命令内稳定可遍历 | 一边遍历一边改 segment 拓扑 |
| `List` 删除 | 从头到尾释放 segment，再释放 root | free 一半 segment 后丢失剩余链引用 |

### `ZSet`

#### 编码 1：`PACKED_ZSET`

小 zset 使用按 `(score, member)` 排序的顺序 blob。

建议布局：

```text
[u16 count][u16 bytesUsed][u32 reserved][f64 score][u16 memberLen][memberBytes]...
```

选择按 `(score, member)` 排序的原因：

- `ZRANGE` 直接顺序读取
- score 相同时可以用 member 字节序做稳定 tie-break

代价是：

- `ZADD/ZREM` 的 member 定位对小对象仍需线性扫描

这在“小对象”假设下是可接受的。

推荐升级阈值：

- `count > 128`
- 任一 member 长度 `> 64B`
- score 更新频率高到明显拖垮 packed 重写成本

#### 编码 2：`DICT + SKIPLIST`

大 zset 采用双结构：

- `DICT`：`member -> zsetRecordHandle`
- `SKIPLIST`：按 `(score, member)` 排序，node 指向同一个 `zsetRecordHandle`

`zsetRecord` 建议拥有：

- member bytes
- 当前 score
- dict node 相关引用
- skiplist node 引用或句柄

必须保证 member bytes 只存一份，不能在 dict 和 skiplist 中各存一份 payload。

#### Skiplist 细节

V1 推荐：

- 随机层数概率 `p = 1/4`
- 最大层高先取 `16`

如果 benchmark 表明 rank/range tail 明显偏大，再评估是否升到 `32` 层上限。V1 不需要一开始就把 skiplist 参数抠到极限。

#### `ZSet` 编码状态表

| 当前状态 | 触发条件 | 目标状态 | 负责方 | 备注 |
|----------|----------|----------|--------|------|
| `ABSENT` | `ZADD` 首次写入且对象仍小 | `PACKED_ZSET` | `ZSetOps` | 默认 packed |
| `ABSENT` | 首次写入即超阈值 | `DICT + SKIPLIST` | `ZSetOps` | 直接 large |
| `PACKED_ZSET` | `ZADD/ZREM` 后仍在阈值内 | `PACKED_ZSET` | `ZSetOps` | 允许整块重建 |
| `PACKED_ZSET` | 触发升级阈值 | `DICT + SKIPLIST` | `ZSetUpgrade` | 单向升级 |
| `DICT + SKIPLIST` | 后续增删改 | `DICT + SKIPLIST` | `ZSetOps` | 不自动降级 |
| `PACKED_ZSET/DICT + SKIPLIST` | 删除或过期 | `DELETING` | `DeletionCoordinator` | 统一释放 |

#### `ZSet` 删除与回滚表

| 场景 | 必须执行的动作 | 绝对禁止 |
|------|----------------|----------|
| `PACKED_ZSET` 重建失败 | 释放新 blob，保留旧 blob | 原地改旧 score/member 顺序后失败 |
| `PACKED_ZSET -> DICT + SKIPLIST` 升级失败 | 释放新 dict/skiplist/zsetRecord，保留旧 blob | member bytes 在新旧结构中同时形成活引用 |
| `DICT + SKIPLIST` 写入失败 | 回收新 node / record，旧双结构保持一致 | 只插入 dict 或只插入 skiplist 的半状态 |
| `ZSet` 删除 | 先按记录释放 dict/skiplist 子节点，再释放根结构 | 先 free 根结构再尝试遍历 skiplist |

#### `ZSet` 双结构一致性要求

进入 `DICT + SKIPLIST` 后，必须保持以下恒等式：

- 每个 `member` 恰好对应一个 `zsetRecord`
- 每个 `zsetRecord` 恰好被 dict 和 skiplist 各引用一次
- score 的真源数据只能在一个位置定义，其他位置只能缓存或索引

任何写入失败都不能让系统落到以下非法状态：

- dict 有记录但 skiplist 没有
- skiplist 有 node 但 dict 没有
- 同一个 member 对应多个 `zsetRecord`

## 命令路径

稳态命令路径应固定为：

`decode -> argv views -> dispatch -> expire check -> lookup/mutate -> encode`

关键规则：

- 请求参数尽可能长时间保持为 view
- 命令处理器只调用语义化 engine API
- 大回复在可行时直接流式写给 reply writer
- 每条命令推进有限预算的维护任务

建议把维护预算显式拆开：

- `expireBudget`
- `rehashBudget`
- `optionalCompactBudget`

这样可以分别调参，而不是把所有 maintenance 混成一个黑箱。

## Mutation 纪律

所有写入都必须遵守同一条不变式：

1. 校验并定位
2. 分配 / 构建新表示
3. 只有当新 value 完整可用后，才切换 entry 引用
4. 最后再释放旧表示

这条纪律适用于：

- overwrite 写入
- type change
- expiration delete
- 显式删除
- 编码升级

推荐把 mutation 分成三层：

- `Plan`：只做查找、阈值判断、预算估算
- `Build`：分配和构建新结构
- `Commit`：切换引用并释放旧结构

目标不是事务隔离，而是保证失败场景下的确定性、无泄漏状态迁移。

## 命令语义与失败矩阵

本节把前面的结构设计、状态机、TTL 和编码策略收敛为“命令级行为”。目标不是穷举所有 Redis 命令，而是把当前最小命令子集在命中、未命中、过期、wrongtype、升级失败、预算失败等场景下的行为固定下来，避免实现阶段出现同类命令语义漂移。

### 通用命令语义规则

所有命令都应遵守以下前置规则：

1. **先做 lazy expiration**
   - 命中 key 后，先检查 TTL。
   - 若 key 已过期，则先执行统一删除路径，再按“key 不存在”继续语义判断。

2. **再做类型判断**
   - key 未过期且存在时，才进入类型分派。
   - key 不存在时，不返回 wrongtype。

3. **增长写必须在提交前失败**
   - 任何分配失败、升级失败、`Phase 6` 预算失败，都必须发生在 `COMMIT` 前。
   - 不允许对客户端表现为“部分成功”。

4. **容器变空时自动删 key**
   - `Hash / Set / List / ZSet` 在最后一个元素被删除后，应自动删除整个 key，并释放其 TTL。
   - 不保留“空容器但 key 仍存在”的中间态对外可见。

5. **rehash / defragment 不改变语义**
   - 无论 keyspace 是否处于 rehash、对象是否处于 defragment 预算周期中，命令对外结果必须与静态布局一致。

### 统一失败类别

实现阶段建议把对外可见失败统一归入以下几类：

- `WRONGTYPE`
  - key 存在、未过期，但其 `type` 与命令要求不符
- `OOM_OR_NOEVICTION`
  - `Phase 6` 后，增长写因预算不足而被拒绝
- `INTERNAL_ERROR`
  - 理论上不应发生；若发生，必须保证命令不产生部分提交

在 `Phase 0 ~ Phase 5` 中，`OOM_OR_NOEVICTION` 还不会对外生效，但命令语义需要为此保留稳定入口。

### Core / Keyspace / TTL 语义矩阵

| 命令 | key 不存在 | key 存在且未过期 | key 存在但类型不符 | 备注 |
|------|------------|------------------|--------------------|------|
| `PING` | 返回 `PONG` | 返回 `PONG` | 不适用 | 与 DB 状态无关 |
| `DEL` | 返回 `0` | 删除成功返回 `1`，多 key 未来按删除个数返回 | 不适用 | `DEL` 不做类型校验 |
| `TYPE` | 返回 `none` | 返回实际类型名 | 不适用 | 过期 key 先删后返回 `none` |
| `EXPIRE` / `PEXPIRE` | 返回 `0` | 设置成功返回 `1` | 不适用 | 只要求 key 存在；不关心类型 |
| `TTL` / `PTTL` | 返回 `-2` | 有 TTL 返回剩余时间；无 TTL 返回 `-1` | 不适用 | 语义跟随 Redis 风格 |

### `String` 命令语义矩阵

| 命令 | key 不存在 | key 存在且为 `String` | key 存在但不是 `String` | 失败收口要求 |
|------|------------|-----------------------|--------------------------|--------------|
| `GET` | 返回 `nil` | 返回 bulk string | `WRONGTYPE` | 只读，不分配新对象 |
| `SET` | 创建新 key，返回 `OK` | 覆盖旧值，返回 `OK` | `WRONGTYPE` | 新块构建失败必须保留旧值或空状态 |

补充规则：

- `SET` 的最小子集默认是“无条件覆盖写”，不包含 `NX/XX/GET/KEEPTTL` 等组合语义。
- `SET` 成功后默认清除旧 TTL，除非后续另加命令语义明确要求保留。
- `SET` 触发 `EMBSTR-like -> RAW` 升级时，客户端只能看到一次成功覆盖或一次失败，不能观察到中间编码状态。

### `Hash` 命令语义矩阵

| 命令 | key 不存在 | key 存在且为 `Hash` | key 存在但不是 `Hash` | 失败收口要求 |
|------|------------|---------------------|-----------------------|--------------|
| `HSET` | 创建新 hash，新增 field 返回 `1` | 新 field 返回 `1`，覆盖旧 field 返回 `0` | `WRONGTYPE` | packed 重建或 packed->large 升级失败必须保留旧 hash |
| `HGET` | 返回 `nil` | field 存在返回 bulk string；不存在返回 `nil` | `WRONGTYPE` | 只读，不修改结构 |
| `HDEL` | 返回 `0` | 返回实际删除 field 个数 | `WRONGTYPE` | 删空后自动删 key |

补充规则：

- `HSET` 返回值采用 Redis 风格：只统计新插入 field 数，不统计覆盖数。
- `HDEL` 删掉最后一个 field 后，应删除整个 key 和其 TTL，而不是留下空 hash。

### `Set` 命令语义矩阵

| 命令 | key 不存在 | key 存在且为 `Set` | key 存在但不是 `Set` | 失败收口要求 |
|------|------------|--------------------|----------------------|--------------|
| `SADD` | 创建新 set，返回新增 member 数 | 返回本次真正新增 member 数 | `WRONGTYPE` | packed 重建 / 升级失败必须保留旧 set |
| `SREM` | 返回 `0` | 返回实际删除 member 数 | `WRONGTYPE` | 删空后自动删 key |
| `SISMEMBER` | 返回 `0` | member 存在返回 `1`，否则 `0` | `WRONGTYPE` | 只读，不修改结构 |

补充规则：

- `SADD` 和 `SREM` 都采用“去重后计数”的 Redis 风格，不因为重复 member 产生错误。
- `PACKED_SET` 内部即使按字典序排序，对外也不承诺集合顺序。

### `List` 命令语义矩阵

| 命令 | key 不存在 | key 存在且为 `List` | key 存在但不是 `List` | 失败收口要求 |
|------|------------|---------------------|-----------------------|--------------|
| `LPUSH` / `RPUSH` | 创建新 list，返回新长度 | 头/尾插入后返回新长度 | `WRONGTYPE` | split 失败必须保留旧链结构 |
| `LPOP` / `RPOP` | 返回 `nil` | 返回被弹出的元素 | `WRONGTYPE` | 弹空后自动删 key |
| `LRANGE` | 返回空数组 | 返回指定区间数组 | `WRONGTYPE` | 只读遍历，不 materialize 整表到 heap |

补充规则：

- `LRANGE` 超界应返回合法裁剪后的结果，而不是错误。
- `LPOP/RPOP` 删除最后一个元素后，应同步移除 TTL。

### `ZSet` 命令语义矩阵

| 命令 | key 不存在 | key 存在且为 `ZSet` | key 存在但不是 `ZSet` | 失败收口要求 |
|------|------------|---------------------|-----------------------|--------------|
| `ZADD` | 创建新 zset，返回新增 member 数 | 新增 member 计入返回值，更新已有 member 的 score 不计入 | `WRONGTYPE` | packed 重建 / packed->large 升级失败必须保留旧 zset |
| `ZREM` | 返回 `0` | 返回实际删除 member 数 | `WRONGTYPE` | 删空后自动删 key |
| `ZRANGE` | 返回空数组 | 返回按 `(score, member)` 排序后的范围结果 | `WRONGTYPE` | 只读，不修改结构 |

补充规则：

- `ZADD` 对已存在 member 的 score 更新不增加返回计数。
- `ZRANGE` 的稳定性依赖 `(score, member)` 排序规则，因此 score 相同情况下必须以 member 字节序作 tie-break。

### 统一失败矩阵

| 失败场景 | 读命令语义 | 写命令语义 | 绝对禁止 |
|----------|------------|------------|----------|
| lazy expiration 发现 key 已过期 | 按 miss 处理 | 先删旧 key，再按 miss 或新建语义继续 | 返回过期前旧值 |
| `WRONGTYPE` | 返回错误，不做写入 | 返回错误，不做写入 | 在 wrongtype 后部分修改结构 |
| packed 重建失败 | 不适用 | 保留旧 packed 结构，返回失败 | 返回成功但结构未完整更新 |
| packed -> large 升级失败 | 不适用 | 保留旧 packed 结构，返回失败 | 切换 `encoding` 后再尝试补救 |
| `Phase 6` 预算失败 | 不适用 | 在 `COMMIT` 前返回 `OOM_OR_NOEVICTION` | 先提交再淘汰或先提交再报错 |
| rehash 进行中 | 结果与静态表一致 | 结果与静态表一致 | 因新旧表切换返回不一致结果 |
| defragment 进行中 | 结果与静态布局一致 | 不得观察到搬迁中间态 | 读到旧对象已 free、新对象未接管的中间态 |

### 命令级一致性要求

当前最小命令子集必须满足以下一致性要求：

- 同一个命令的 miss 语义要稳定：
  - `GET/HGET/LPOP/RPOP` 走 `nil`
  - `SISMEMBER` 走 `0`
  - `LRANGE/ZRANGE` 走空数组
  - `DEL/HDEL/SREM/ZREM` 走 `0`
- 同一类容器写命令在删空后必须统一触发“自动删 key”
- 同一类增长写命令在失败时必须统一表现为“旧值保持不变”
- 同一类 read-only 命令都不能因为内部 layout 优化而改变回复语义

## 失败场景与调试不变式

数据面应把以下约束视为 debug 期的强制不变式：

- 不允许 double free
- 不允许通过 stale handle 发生 use-after-free
- 不允许 silent handle forgery
- 不允许 clean shutdown 后仍残留 leaked page
- 不允许 entry 删除后仍悬挂负载引用

推荐调试能力：

- handle generation/version checks
- free 后 poison memory
- allocator dump
- test 模式下的启动 / 退出一致性检查
- 定向的 keyspace / TTL / skiplist / packed blob 完整性校验

V1 至少要支持在 debug 模式下跑以下一致性检查：

- keyspace slot 可达的 entry 数 == active entry 数
- TTL heap 的 live 节点数与带 TTL entry 数在同一数量级
- `allocatedBytes >= activeBytes`
- 每种 encoding 的逻辑对象数与 arena 中实际对象数对得上

## Benchmark 与可观测性

V1 的 benchmark 不能只看吞吐。

### 基准环境约束

为了让 Redis 基线比较有意义，benchmark 环境应固定：

- 单机单核或固定核绑核
- 相同协议路径
- 持久化关闭
- 客户端连接数、pipeline 深度、value 分布固定
- JVM `-Xms/-Xmx` 固定，off-heap 上限明确

如果这些条件不固定，任何“赢 Redis / 输 Redis”的结论都没有参考价值。

### 必要指标

- `ops/s`
- `p50/p95/p99/p999`
- 每个 key 的常驻字节数
- 每个负载字节对应的总字节开销
- 碎片率
- encoding 分布
- TTL 和 rehash 活动下的维护成本
- 每条命令附带的平均维护步数

### 必要的 Benchmark Profile

1. `String` 混合负载：
   - `64B key / 256B value`
   - 大约 `70/30` 读写比例
   - 不带 TTL
2. `String + TTL` 负载：
   - `64B key / 256B value`
   - 短 TTL churn
3. 混合结构负载：
   - `Hash/Set/List/ZSet`
   - 混合读写
   - packed 与 large encoding 都要覆盖
4. 编码升级负载：
   - 持续把对象从 packed 推到 large
   - 验证升级尖刺和回收路径
5. Rehash 压力负载：
   - 持续插入 / 删除
   - 刻意维持在装载因子阈值附近

### 必要的稳定性运行

必须执行长时间 soak test，而不仅仅是短时间冲分，以观察：

- 延迟漂移
- 内存增长
- 碎片率爬升
- allocator 在持续 churn 下的健康度
- rehash 与 TTL 清理是否形成周期性尖刺

建议默认 soak 时长：

- Phase 1 以后至少 `30 min`
- Phase 5 以后至少 `2~4 h`

### 阶段性验收目标带

这些数值是 planning 用的目标带，不是对外发布承诺，但它们必须足够具体，能指导实现取舍。

#### Phase 1 目标带

- `String 70/30` 混合负载下，30 分钟内无内存持续漂移
- `activeBytes` 与逻辑对象估算偏差稳定，不出现单调失真
- p99 不因 TTL 或 rehash 出现固定周期的巨大尖刺

#### Phase 2 ~ Phase 4 目标带

- packed -> large 升级不会造成对象丢失、重复释放或重复持有
- 单次升级尖刺必须受控，不能把事件循环拖成毫秒级停顿常态
- 各结构的 large encoding 内存放大量级必须明显优于“Java 集合 + 对象包装”版本

#### Phase 5 Redis 对比目标带

- `String` 主负载的内存放大倍数不应超过 Redis 基线的 `2.0x`
- 复杂结构主负载的内存放大倍数不应超过 Redis 基线的 `2.5x`
- 单线程吞吐至少达到 Redis 基线的 `50%~70%` 区间
- 目标负载下 p99 不应长期劣于 Redis 基线的 `2x`

这些目标不意味着 V1 必须“全面超过 Redis”，但至少要进入值得继续深挖的区间，而不是完全偏离目标。

## 交付策略

V1 应按明确阶段推进，并在阶段之间设置硬门槛。

### Phase 0：骨架与观测

实现：

- 事件循环骨架
- RESP 路径
- 压测工具链
- allocator debug 基础设施
- 基线内存统计
- handle generation 检查

显式验证：

- 启动 / 退出 leak scan
- 单元级 allocator 压测
- 基础 `PING/SET/GET` 闭环

主要风险：

- allocator 没有统一所有权模型
- debug 模式和 release 模式行为不一致

门槛：

- 启动与退出无泄漏
- 压测工具链可重复运行
- allocator 状态可检查

### Phase 1：`String + TTL + Keyspace`

实现：

- `MemoryManager`
- `EntryArena`
- `KeyspaceTable`
- `EntryLayout`
- expire heap
- `GET/SET/DEL/EXPIRE/TTL`
- 增量 rehash
- lazy/active expire

显式验证：

- 覆盖写
- 删除
- 过期删除
- rehash 中读写
- stale TTL heap node 丢弃

主要风险：

- backward-shift deletion 实现错误
- TTL heap stale 节点过多导致尾延迟抖动
- rehash 与删除交错引入悬挂 handle

门槛：

- 语义正确
- overwrite / delete / expire 场景无泄漏
- 不出现明显 TTL 维护尖刺

### Phase 2：`Hash` 与 `Set`

实现：

- `PACKED_HASH`
- `HT_HASH`
- `PACKED_SET`
- `HT_SET`
- 升级路径与删除路径

显式验证：

- packed 读取 / 删除
- large 读取 / 删除
- packed 升级为 large 后行为一致
- 升级过程中异常回滚

主要风险：

- packed blob 重建成本过高
- hash/set 大对象路径在 field/member 所有权上重复持有字节

门槛：

- packed 形态与升级后形态的行为保持一致
- 升级路径稳定且单向

### Phase 3：`List`

实现：

- quicklist-like 分段链表
- segment split / merge
- push/pop/range 路径

显式验证：

- 双端高频 churn
- 大范围 `LRANGE`
- segment merge/split 的一致性

主要风险：

- segment 过小导致指针开销过大
- segment 过大导致 `LRANGE` 和 split 尖刺明显

门槛：

- churn 下 allocator 不出现明显病理行为
- range 读取延迟可接受

### Phase 4：`ZSet`

实现：

- `PACKED_ZSET`
- `DICT + SKIPLIST`
- score/member 稳定排序
- member 单份所有权

显式验证：

- `ZADD/ZREM/ZRANGE`
- packed -> large 升级
- score tie-break 正确性

主要风险：

- skiplist node 开销过高
- dict 与 skiplist 双结构同步不一致
- member bytes 被双份持有

门槛：

- member 查找与有序 range 语义稳定
- 不出现明显负载重复持有回归

### Phase 5：加固

实现：

- memory/encoding 检查命令
- 遍历支持
- fuzzing
- 长稳 soak 验证
- Redis 基线对比
- allocator / keyspace / TTL 完整性检查

显式验证：

- 多小时 soak
- benchmark 基线记录
- 指标导出
- 常见失败注入

主要风险：

- 逻辑正确但内存放大倍数过高
- 吞吐尚可但 p99/p999 长尾不可接受
- packed/large 共存时统计口径不一致

门槛：

- 多小时运行稳定
- allocator / accounting 一致
- benchmark 结果足以支撑继续进入 `Phase 6`

### Phase 6：`maxmemory / eviction`

本节属于同一主线下的后续 phase 设计。它不是独立 spec，也不是主线之外内容；它表达的是“在前五个 phase 把 allocator、keyspace、TTL 与核心类型跑稳后，主线继续推进到内存预算与淘汰机制”。

### 目标

`maxmemory / eviction` 要解决的是“活数据总量超过预算时如何保持引擎语义稳定、内存可控、尾延迟可接受”。

对本引擎而言，这一能力不是“JVM 快没内存时临时删几个 key”，而是：

- 引擎自己维护一套可信的数据面内存预算
- 每个增长写入都在提交前经过预算判定
- 当预算不足时，要么拒绝写入，要么在可控预算内淘汰足够多的 key
- 整个过程不能把事件循环拖成明显尖刺

### 前置条件

只有在以下前置条件成立后，才应进入 `Phase 6`：

- allocator 统计已经稳定，`allocatedBytes / activeBytes / fragmentBytes` 可信
- keyspace、TTL、删除路径、编码升级路径已经无明显泄漏
- entry header 中已经有稳定的 `accessMeta`
- `payloadLen` 与各 encoding 的对象计数可以回推出单 key 的 memory usage
- 事件循环已经具备“每条命令推进有限 maintenance 预算”的基础框架

如果这些前提不成立，`maxmemory` 只会变成“不精确地删东西”，而不是稳定的预算系统。

### 预算口径

`maxmemory` 统计口径应明确为“数据面内存预算”，而不是整个 JVM 进程的总内存占用。

建议计入预算的内容：

- `EntryArena`
- `SmallObjectArena`
- `LargeObjectAllocator`
- 主 keyspace table
- TTL heap
- 各类 value block
- allocator 元数据本身

建议暂不计入预算的内容：

- Netty / 协议层缓冲
- 连接状态
- 命令注册表与控制面对象
- 日志与监控接线对象

原因：

- 这些对象不由数据面单 key 生命周期控制
- 如果把控制面内存混入预算，写入是否成功就会依赖连接数、日志行为等非数据语义因素
- 这种行为很难测试，也很难向用户解释

建议在观测上区分两个指标：

- `usedMemoryForMaxmemory`
- `processResidentMemory`

前者用于策略决策，后者用于运维观测。

### 策略范围

`Phase 6` 推荐按如下顺序实现，而不是一口气做全量 Redis 策略：

1. `noeviction`
2. `allkeys-random`
3. 采样式 `allkeys-lru`

不建议在第一波进入以下策略：

- `volatile-random`
- `volatile-lru`
- `allkeys-lfu`
- `volatile-lfu`
- `volatile-ttl`

这些策略并非永远不做，而是它们会引入额外候选池、频率衰减、TTL 口径差异和更复杂的测试矩阵。对当前路线来说，优先级不如先把统一预算系统做稳。

### 写路径与预算判定

增长写入必须在真正提交前完成预算判定。推荐沿用当前文档中已经定义的 mutation 分层：

- `Plan`
- `Build`
- `Commit`

对于 `maxmemory`，需要在 `Plan` 阶段额外提供：

- `upperBoundDeltaBytes`
- `expectedReleaseBytes`
- `mayTriggerEncodingUpgrade`
- `mayRequireEviction`

推荐写路径顺序如下：

1. 命令进入 `Plan`
2. 计算本次 mutation 的上界内存增量
3. 若 `usedMemoryForMaxmemory + upperBoundDeltaBytes <= maxmemory`，直接进入 `Build`
4. 若超限，则先执行一小段 active expire
5. 若 active expire 后仍超限，则进入 eviction 流程
6. 若 eviction 在预算内释放出足够空间，则继续 `Build`
7. 若到达尝试上限或时间预算后仍无法腾出空间，则按策略失败

关键要求：

- 不允许“先写进去，再补救”
- 不允许“写了一半后才发现超限”
- `noeviction` 下所有增长写都必须在提交前稳定失败

### 淘汰候选选择

本引擎是单线程事件循环，不适合第一阶段实现精确全局 LRU 树或全局优先队列。更合理的路线是采样式候选选择。

#### `allkeys-random`

策略非常直接：

- 从 keyspace 随机抽若干 key
- 直接删除其中一个

适合作为第一批验证预算系统和删除路径的策略，因为它不依赖复杂元数据。

#### 采样式 `allkeys-lru`

推荐做法：

- 随机抽取 `N` 个候选
- 优先回收已经过期但尚未被清理的 key
- 若候选都未过期，则比较 `accessMeta`
- 选择“最旧”的那个 key 进行淘汰

建议默认采样数从 `5` 或 `8` 开始。

原因：

- 足够逼近“近似 LRU”
- 不会把单次候选选择成本抬太高
- 更接近 Redis sampled eviction 的工程思路

### `accessMeta` 设计要求

为了支持后续采样式 `LRU/LFU`，entry 中的 `accessMeta` 不能只是一个模糊的保留字段，而应具备明确可扩展语义。

推荐第一阶段先按近似 LRU 设计：

- 保存压缩后的最近访问时钟
- 时钟精度以“够区分冷热”为准，而不是追求纳秒级

后续若需要演进到 LFU，可把该字段扩展为：

- 访问频率计数
- 衰减时间戳

但在采样式 `LRU` 没跑稳之前，不建议提前引入 LFU。

### 淘汰循环预算

eviction 本身必须是有预算的。不能为了让一条写命令成功，就无限循环删除 key。

每次写入前的淘汰循环都应至少受两个条件约束：

- 最大淘汰 key 数
- 最大 CPU 时间预算

推荐默认控制项：

- `evictionMaxKeysPerWrite = 16 ~ 64`
- `evictionNanosBudgetPerWrite = 50us ~ 300us`

执行顺序建议固定为：

1. 先做少量 active expire
2. 再进入 eviction sample loop
3. 每删除一个 key，就立刻更新 `usedMemoryForMaxmemory`
4. 一旦回到预算内，立即结束 eviction
5. 若超过预算仍无法回落，则失败返回

### 与 TTL、rehash、编码升级的交互

`maxmemory / eviction` 不能孤立实现，必须与现有数据面路径对齐。

#### 与 TTL 的关系

- eviction 前应先尝试回收已过期 key
- 已过期 key 不应与普通活 key 在候选排序上等价对待
- TTL heap 负责发现可回收对象，但真正 free 仍走统一删除路径

#### 与 rehash 的关系

- rehash 期间随机采样必须能从新旧表中获得一致可见的候选
- eviction 删除的 key 不能破坏 rehash 状态机
- entry handle 稳定性必须保持，eviction 只能删除 entry，不能依赖 slot 稳定

#### 与编码升级的关系

- packed -> large 的升级是典型增长写
- 如果升级会显著增大内存占用，必须在升级前参与预算判定
- 不能“先升级成功，再触发 OOM”

### 观测与命令面

一旦实现 `maxmemory / eviction`，需要新增最少一组稳定观测指标：

- `maxmemoryBytes`
- `usedMemoryForMaxmemory`
- `evictedKeys`
- `evictionAttempts`
- `evictionRejectedWrites`
- `evictionCandidateSamples`
- `expiredKeysReclaimedBeforeEviction`

建议同时支持最小观测命令输出：

- 当前策略名
- 当前预算与使用量
- 累计淘汰 key 数
- 最近一段时间的淘汰失败 / 拒写数

### 验收标准

进入 `Phase 6` 时，建议以以下标准验收：

- `noeviction` 下，所有增长写都在提交前稳定失败
- `allkeys-random` 下，删除路径无泄漏，内存能回落
- 采样式 `allkeys-lru` 下，热点 key 不会像随机淘汰一样被频繁误删
- 在 `95% ~ 105% maxmemory` 压边负载下，p99 不出现明显灾难性抖动
- 开启 eviction 后，吞吐下降和尾延迟上升都应处于可接受区间，而不是说明实现本身失衡

### Phase 7：`defragment`

本节同样属于同一主线下的后续 phase 设计。它建立在 `Phase 6` 或至少 `Phase 5` 后的稳定 allocator 与引用模型之上，目标是解决长期运行下的碎片与内存形态问题，而不是作为独立项目另起一条线。

### 目标

`defragment` 解决的问题不是“数据太多”，而是“活数据没超预算，但布局太碎，导致 resident memory、分配效率和尾延迟都变差”。

它的目标是：

- 逐步压实低利用率 page
- 回收可腾空的 page 或 extent
- 提升 `largestFreeExtentBytes`
- 降低 `fragmentBytes`
- 在不改变逻辑语义的前提下改善长期内存形态

这与 `eviction` 完全不同。`eviction` 是删数据，`defragment` 是搬数据。

### 前置条件

`defragment` 比 `maxmemory / eviction` 更晚进入实现阶段，因为它对底层稳定性要求更高。

必须满足以下前置条件后，才应进入 `Phase 7`：

- handle 间接层已经稳定
- delete / overwrite / encoding upgrade 路径已无明显泄漏
- allocator 已经有 page/extent 级利用率统计
- 明确知道哪些对象可移动，哪些对象暂时不移动
- 事件循环已支持“有预算的后台维护工作”

如果这些条件不满足，defragment 几乎等于主动制造悬挂引用和 use-after-free 风险。

### 第一波范围

建议把 `Phase 7` 内部也拆成多波，而不是“一开始全对象可移动”。

第一波优先支持：

- 小字符串块
- `PACKED_HASH` blob
- `PACKED_SET` blob
- `PACKED_ZSET` blob
- `List` segment

第一波暂不主动迁移：

- primary entry
- key block
- `LargeObjectAllocator` 中的大对象
- `DICT + SKIPLIST` 里的复杂双向引用节点

理由：

- 第一波对象通常只有少量 owner 引用
- 迁移后引用更新路径较短
- 它们对小对象 arena 的碎片率影响最大

### 可移动对象与引用更新模型

对本引擎来说，defragment 不是“把页复制一下”这么简单。关键在于：对象搬迁后，所有引用都必须保持一致。

建议采用以下原则：

- `entryHandle` 保持稳定
- 第一波 defragment 优先搬迁 entry 指向的 value block，而不是搬迁 entry 本身
- 迁移完成后，只更新 owning entry 或 owning container 的字段

这样可以避免全系统都通过一张重量级全局 indirection table 解析对象位置，也能显著降低 defragment 的实现面。

### 触发条件

不建议让 defragment 高频常驻运行，而应基于“信号触发 + 小预算执行”。

推荐触发信号：

- 某个 `SmallObjectArena` size class 的 page 平均利用率长期低于阈值
- `fragmentBytes / activeBytes` 高于阈值
- 总空闲很多，但 `largestFreeExtentBytes` 很小
- soak test 中 resident memory 长期明显高于 active memory

推荐保守默认阈值：

- page 平均利用率 `< 50%`
- 碎片率 `> 20%`
- 大对象总空闲充足但最大连续 extent 不足以满足常见大块申请

这些阈值不是最终真理，但足够支撑第一版 maintenance 设计。

### Small Object 压实策略

第一波 defragment 的最主要工作流应是“page 级压实”。

推荐步骤：

1. 选择一个低利用率 source page
2. 为它选择一个或多个 target page
3. 从 source page 中逐个选择活对象
4. 为每个对象分配新位置并复制数据
5. 更新 owning reference
6. 释放旧对象
7. 当 source page 被清空后，整页归还 page pool

要点：

- 一次 maintenance 不要求处理完整个 page
- 可以中断、恢复、继续
- source page 只有在完全清空后才真正回收

### `List` Segment 的特殊处理

`List` segment 不是单纯 blob，因为它还处于链结构中。

因此第一波 `List` defragment 至少要支持：

- 更新 segment 前驱 / 后继引用
- 更新 owning entry 指向的头尾 segment
- 保证 split / merge 与 defragment 不交错破坏结构

建议规则：

- 正在参与 split/merge 的 segment 暂不加入 defragment
- segment 搬迁完成后，统一做一次局部链一致性校验

### `DICT + SKIPLIST` 的后续波次

对于大 `ZSet` 的双结构，推荐放到 defragment 后续波次再做，而不是第一波进入。

原因：

- `dict` 与 `skiplist` 存在双重引用关系
- `member bytes` 通常还要维持单份所有权
- 迁移步骤更像“局部重建”，而不是简单块复制

第二波若要支持，建议优先迁移：

- `zsetRecord`
- skiplist node

但前提是已经有足够强的引用更新与一致性校验框架。

### 大对象碎片与大对象搬迁

不建议第一波把 `LargeObjectAllocator` 的 relocation 当作 defragment 核心能力。

原因：

- 大对象复制成本高
- 单次搬迁更容易形成尾延迟尖刺
- 大对象常常更适合通过 free extent 合并和分配策略优化来缓解碎片，而不是主动迁移

因此推荐顺序是：

1. 先做 small-object compaction
2. 再做好 large allocator 的 free extent 合并
3. 只有 benchmark 明确证明“大对象碎片是主瓶颈”时，才设计 large relocation

### 执行预算

defragment 和 TTL、rehash 一样，必须是“渐进式、可中断、有预算”的后台维护任务。

推荐每轮限制至少包含：

- 最多迁移对象数
- 最多迁移字节数
- 最大 CPU 时间预算

推荐初始预算：

- 每轮迁移 `1 ~ 4` 个 small object
- 或最多迁移 `4 ~ 16 KiB`
- 或最多消耗 `50 ~ 100us`

重点不是“尽快整理完”，而是“不让整理本身变成新的尾延迟源”。

### 观测指标

一旦实现 defragment，至少要新增这些可观测指标：

- `defragAttempts`
- `defragMoves`
- `defragBytesMoved`
- `defragPagesFreed`
- `defragSkips`
- `fragmentationBefore`
- `fragmentationAfter`
- 各 arena 的 page 利用率分布

没有这些指标，就无法判断 defragment 是真的在改善内存形态，还是只是在白白复制数据。

### 验收标准

进入 `Phase 7` 时，建议至少满足以下验收标准：

- 压实后 `fragmentBytes` 能稳定回落
- `residentPages` 或 page 利用率分布有可观改善
- `largestFreeExtentBytes` 不再长期停留在极小值
- soak test 下 resident memory 曲线更平稳
- defragment 开启后 p99/p999 不会显著恶化

如果 defragment 只能轻微降碎片，却明显恶化尾延迟，那这套设计就不值得继续推进。

## 风险与权衡

### 收益

- 从第一天起就让项目沿着 Redis 级内存密度目标前进。
- 避免后期从 heap-object 数据结构重写到紧凑布局。
- 更早暴露尾延迟问题，而不是等到后期才发现。

### 代价

- 实现复杂度远高于 Java-collections-first 设计。
- 调试和测试基础设施要求明显更高。
- 在很多用户可见特性加入之前，必须先把 allocator discipline 做扎实。

### 拒绝的替代方案

本设计明确拒绝“主要把稳态数据存放在 Java 集合里，只在局部机会式使用 off-heap”的基线方案。那条路线适合做一个 Redis-like JVM 服务，但不适合一个明确以 Redis 级内存密度和低延迟为目标的引擎项目。

## 后续

当这份设计被接受后，下一份产物应是实现计划，并明确：

- 把每个 phase 展开成可执行任务
- 指定第一批具体文件 / 模块边界
- 定义第一批 benchmark 命令与预期失败模式
- 以当前文档中的 `Phase 0 ~ Phase 7` 作为同一主线的总体边界
- 明确 `Phase 6` 与 `Phase 7` 的进入条件，避免在前五个 phase 未稳定时强行并行推进
