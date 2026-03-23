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

### V1 范围外

- 持久化（`AOF`、`RDB`）
- 复制、集群或分片
- 多线程共享状态执行
- Lua / scripting
- ACL / TLS / PubSub
- 完整 Redis 命令覆盖
- 自动编码降级
- 最终版 `maxmemory` 淘汰策略

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

## 内存模型

### 基于 Handle 的访问

引擎应统一使用稳定的 64 位 handle，而不是把裸地址泄漏到高层。

推荐性质：

- handle 编码 arena / page / type 等身份信息
- debug 模式下可以做 handle 合法性校验
- hash table rehash 后 handle 仍保持稳定
- 上层不需要知道原始地址

热路径内部可以把 handle 解码为地址，但系统边界必须保持为基于 handle 的访问模型。

### Allocator 策略

V1 不应使用一套单一的通用 allocator 策略来承载所有对象形状，而应显式拆分职责：

#### `EntryArena`

- 固定大小块
- 只存储主 entry
- 以 page 为单位管理
- 通过 free-list 回收

#### `SmallObjectArena`

- 基于 size class
- 存储 packed blob、小字符串块、list segment、小节点
- 目标是低额外开销和可预测回收

#### `LargeObjectAllocator`

- 存储大字符串和大块 blob
- 使用更粗粒度的分配策略
- 必须暴露碎片可见性

这样可以避免把固定宽度元数据和变长负载的生命周期混在同一套分配路径里。

### 必要的 Allocator 诊断能力

内存系统至少要暴露这些指标：

- `allocatedBytes`
- `activeBytes`
- `fragmentBytes`
- `residentPages`
- `objectCountByEncoding`
- 各 size class 利用率
- debug/test 模式下的退出时泄漏报告

## Keyspace 设计

### 哈希表策略

主字典应满足以下特征：

- off-heap
- 开放寻址
- 为 locality 优化
- 支持增量 rehash

`Robin Hood` probing 是一个很强的默认方案，因为它有助于降低 lookup 方差和 probe tail。

每个 slot 只存储最小必要元数据，例如：

- hash fingerprint
- entry handle
- probe metadata

slot 本身不拥有完整对象负载。真正的 key/value 状态存放在 handle 指向的稳定 entry 中。

### 稳定的 Entry 所有权

每个 key 只能有一个 owning entry。这个 entry 负责拥有：

- key bytes
- value 引用
- expiration 元数据
- type / encoding 元数据

设计上必须避免在以下位置重复复制 key bytes：

- primary keyspace
- TTL structures
- command-layer 临时副本

## Entry 布局

每个 primary entry 都是固定大小，包含常见操作所需的最小热元数据。

推荐字段：

- `type`
- `encoding`
- `flags`
- `ttlVersion`
- `keyLen`
- `payloadLen`
- `keyRef`
- `valueRef`
- `expireAt`
- `accessMeta`
- `aux/count`

具体字节布局后续可以继续细化，但 V1 应优先建立一个稳定、可检查的 header，而不是一层层小 heap wrapper 对象。

## TTL 设计

### Lazy Expiration

`expireAt` 直接内联在 primary entry 中。这样 lazy expiration 只需要读取一次 entry，而不是再做一次独立哈希查找。

### Active Expiration

active expiration 使用一个 off-heap min-heap 作为二级时间索引。

每个 heap node 存储：

- `expireAt`
- `entryHandle`
- `ttlVersion`

当 TTL 被覆盖时：

1. 更新 `entry.expireAt`
2. 递增 `entry.ttlVersion`
3. 压入一个新的 heap node

旧 heap node 在后续弹出时按过期版本节点惰性丢弃。

这套模型能保持所有权简单，并避免为了 TTL 单独复制 key。

## 类型与编码策略

所有 value 都统一表示为：

- `type`
- `encoding`
- `valueRef`

编码只做单向升级，V1 不做自动降级。

### `String`

两种编码：

- `INLINE/EMBSTR`：用于小字符串
- `RAW`：用于中大字符串的连续块

### `Hash`

两种编码：

- `PACKED`：用于小对象的顺序 blob
- off-heap 哈希表：用于较大对象

升级阈值应至少考虑：

- field 数量
- 单个 field/value 的最大尺寸

### `Set`

两种编码：

- `PACKED`：用于小对象的 member blob
- off-heap 哈希表：用于较大对象

### `List`

不要使用传统链表。

建议采用 `quicklist-like` 设计：

- 顶层 segment 链
- segment 内部使用 packed entries
- 两端 push/pop 高效
- range 读取具备可接受的 locality

### `ZSet`

两种编码：

- `PACKED`：用于小对象的顺序 member/score 表示
- `DICT + SKIPLIST`：用于较大对象

大 `ZSet` 模式不能产生不必要的负载重复所有权。

## 命令路径

稳态命令路径应固定为：

`decode -> argv views -> dispatch -> expire check -> lookup/mutate -> encode`

关键规则：

- 请求参数尽可能长时间保持为 view
- 命令处理器只调用语义化 engine API
- 大回复在可行时直接流式写给 reply writer
- 每条命令推进有限预算的维护任务

这样可以同时避免大规模临时 heap 对象和维护尖刺。

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

目标不是事务隔离，而是保证失败场景下的确定性、无泄漏状态迁移。

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

## Benchmark 与可观测性

V1 的 benchmark 不能只看吞吐。

### 必要指标

- `ops/s`
- `p50/p95/p99/p999`
- 每个 key 的常驻字节数
- 每个负载字节对应的总字节开销
- 碎片率
- encoding 分布
- TTL 和 rehash 活动下的维护成本

### 必要的 Benchmark Profile

1. `String` 混合负载：
   - 中等大小 value
   - 大约 `70/30` 读写比例
2. 混合结构负载：
   - `Hash/Set/List/ZSet`
   - 混合读写
3. TTL-heavy 负载：
   - 持续的短 TTL key churn

### 必要的稳定性运行

必须执行长时间 soak test，而不仅仅是短时间冲分，以观察：

- 延迟漂移
- 内存增长
- 碎片率爬升
- allocator 在持续 churn 下的健康度

## 交付策略

V1 应按明确阶段推进，并在阶段之间设置硬门槛。

### Phase 0：骨架与观测

实现：

- 事件循环骨架
- RESP 路径
- 压测工具链
- allocator debug 基础设施
- 基线内存统计

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

门槛：

- 语义正确
- overwrite / delete / expire 场景无泄漏
- 不出现明显 TTL 维护尖刺

### Phase 2：`Hash` 与 `Set`

实现：

- 紧凑编码
- 升级到 off-heap 哈希表的路径

门槛：

- packed 形态与升级后形态的行为保持一致
- 升级路径稳定且单向

### Phase 3：`List`

实现：

- quicklist-like 分段链表
- push/pop/range 路径

门槛：

- churn 下 allocator 不出现明显病理行为
- range 读取延迟可接受

### Phase 4：`ZSet`

实现：

- packed `ZSet`
- `DICT + SKIPLIST` 升级路径

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

门槛：

- 多小时运行稳定
- allocator / accounting 一致
- benchmark 结果足以支撑继续进入 maxmemory / eviction 后续工作

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
- 明确把 `maxmemory/eviction` 继续延后，直到 allocator、keyspace、TTL 与内存记账路径稳定
