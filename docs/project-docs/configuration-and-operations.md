# 配置与运行

本文回答三件事：启动参数怎么变成运行时配置、每个配置项的默认值和取值边界是什么、改完之后在运行中的 server 上能看到什么变化。读者读完后应能自己启动、调参、压测和排障。

## 配置流向与信任边界

启动配置从文件到组合根的主路径是：

```text
yierdis.conf（--config <path> 指定，缺省读取 ./yierdis.conf）
  -> ServerConfig.fromArgs(...)              // 边界：定位/读取文件，失败只打一行错误
  -> YierdisServerFileConfig.fromProperties(...)  // 应用键值，未知键报错
  -> normalizeAndValidate()
  -> YierdisServerRuntimeConfig               // 唯一信任边界，构造器集中校验
  -> YierdisServerBootstrap.startInternal()
```

`YierdisServerFileConfig` 用公开字段承载每个配置键：默认值就是字段初值，`fromProperties(...)` 应用文件里出现的键。字段的默认值、范围校验分散在两个阶段，理解分工很重要：

- `normalizeAndValidate()` 只做归一化和派生：`noCleanup` 把 `cleanupIntervalMillis` 归零；`bind` 去掉首尾空白；三个枚举（`executorSchedulingPolicy`、`maxmemoryScope`、`maxmemoryPolicy`）在这里解析一次并缓存实例，再把文件里的字符串改写成稳定值，不再走字符串 round-trip。归一化后 `maxmemoryScope` 只会是 `global` 或 `per-db`，`maxmemoryPolicy` 只会是它的 `redisName()`。
- `YierdisServerRuntimeConfig` 的紧凑构造器是真正的校验点：网络、协议、reply、内存、maintenance、executor 队列与背压约束都在这里检查，越界直接抛 `IllegalArgumentException`。这是启动配置唯一的校验边界；下游的 `CommandExecutorConfig` 只承载已经校验过的值，不再重复校验（见该 record 的注释）。

归一化接受的等价写法：`maxmemoryScope` 大小写不敏感，并允许 `perdb` / `per_db` / `per-db` 三种拼法（`parseMaxmemoryScope` 内部把 `_` 换成 `-` 再匹配）；`maxmemoryPolicy` 同样大小写不敏感，`_` 归一成 `-`，只接受 `noeviction`、`allkeys-random`、`allkeys-lru`。

`toRuntimeConfig()` 把已归一化配置转成 `YierdisServerRuntimeConfig` record：首次调用触发 `normalizeAndValidate()` 并缓存，后续调用返回同一实例。这个 record 字段已经是 enum、number 和 boolean，不再携带原始字符串；`executorConfig()` 直接生成 executor 领域配置。

`ServerConfig.fromArgs(String...)` 是启动入口到组合根的边界，顺序如下：

1. argv 只接受一个 `--config <path>`（或 `--config=<path>`；重复给出或任何其他参数都报错）。未给出时读 `./yierdis.conf`；文件不存在时报"找不到配置文件"。
2. 文件按 UTF-8 `Properties` 读入，`YierdisServerFileConfig.fromProperties(...)` 应用键值：未知键、类型错误都抛 `IllegalArgumentException`，把原因打一行到 stderr，包成 `YierdisCliException.invalidArguments(...)`（调用方据此 `exit(2)`）。
3. 必须显式给出 `maxmemoryBytes` 键（`wasSpecified(...)` 检查），否则抛 `IllegalArgumentException("maxmemoryBytes must be specified explicitly in <path> (use 0 to acknowledge unlimited memory)")`。这是唯一的"必须显式传"配置。
4. `normalizeAndValidate()` 抛 `IllegalArgumentException`（校验失败）同样打一行原因并包成 `invalidArguments`。
5. 成功则返回 `toRuntimeConfig()`。

这个 jar **不提供** `help`，也没有任何 usage 输出：它只有"启动服务"一个用途，配置键的默认值与约束就是下面那张总览表。

源码入口：

- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/args/YierdisServerFileConfig.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/ServerConfig.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

## 启动过程

真正启动发生在 `YierdisServerBootstrap.startInternal()`，顺序如下：

1. 把 `YierdisServerRuntimeConfig` 映射成 `YierdisInstanceConfig`：databases、maxmemory、defrag 等。
2. `YierdisInstance.create(config)` 组装唯一的 FFM DB backend，并取得 runtime access、maintenance 入口和 observability。
3. 创建 `NettyServerInfoProvider`，绑定 lifecycle state 和 instance observability，再绑定 inbound/outbound budget、child registry、reply egress stats 和 executor。
4. 由 bootstrap 调用 `CommandRegistries.dispatcher(...)` 注册默认命令模块和 server-only 模块（`ServerCommandModule`）。
5. 创建单线程 `DefaultEventExecutorGroup(1)` 与 `CommandExecutor`；owner executor 是 `NettySerialOwnerExecutor`，背后就是 `commandGroup.next()`。
6. `executor.start()` 在 owner thread 上执行 `bindToCurrentThread`，把该线程标成 DB owner。
7. 按需在 Netty worker event loop 上调度 maintenance tick（见下文 TTL 与 maintenance），但 DB 逻辑仍通过 `executeMaintenance(...)` 回到 owner thread。
8. 创建 boss/worker Netty group，并由 `YierdisServerChannelInitializer` 装配连接 pipeline。
9. `bind(bind, port).sync()`，成功后 `lifecycleState = RUNNING`。

`YierdisInstance` 不是“随手可用的 DB 容器”：`runtimeAccess().bindToCurrentThread()` 先把当前线程标成 owner，后续 DB access 才被允许；跨线程访问会 fail-fast。bootstrap 用 `ok` 标记包住整段启动，任一步失败都会 `close()`，避免留下半初始化实例。

benchmark 不持有 server 参数或生命周期模型，只连接由操作者单独管理的 Yierdis；client idle / output-buffer 这类 server-only 参数必须在启动目标 Yierdis 时直接配置。

## 配置项总览

下面按域列出全部配置键。`默认` 一列来自 `YierdisServerFileConfig` 的字段初值；`范围/约束` 一列来自 `YierdisServerRuntimeConfig` 构造器或 `normalizeAndValidate()`；`生效阶段` 说明它在启动流程的哪一步被消费。"必须显式指定"的项已单独标注。

### 网络与实例规模

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `bind` | `127.0.0.1` | trim 后非空 | `bind(...)` 监听地址 |
| `port` | `6378` | `0..65535`（0 由内核分配） | `bind(...)` |
| `maxClients` | `1024` | `> 0` | `ChildChannelRegistry` 准入 |
| `databases` | `16` | `1..1024` | 创建 `YierdisInstance` 的 DB 数组 |
| `ioThreads` | `1` | `> 0` | Netty worker `MultiThreadIoEventLoopGroup` |
| `maxmemoryBytes` | `0` | `>= 0`，**必须在配置文件中显式出现** | `YierdisInstanceConfig` 预算 |

### 协议入口限制

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `protocolMaxBulkBytes` | `536870912`（512 MiB） | `1..536870912` | `RespRequestDecoder` bulk body |
| `protocolMaxArgs` | `1048576` | `1..1048576` | 单命令参数个数 |
| `protocolMaxLineBytes` | `1048576` | `> 0` | header/inline 行长度 |
| `protocolMaxCommandBytes` | `67108864`（64 MiB） | `1..536870912` | 单命令 heap footprint 估算 |
| `protocolGlobalInFlightBytes` | `0` | `>= 0` | ingress 全局在途预算，见下 |

`protocolGlobalInFlightBytes` 是 `InboundMemoryBudget` 的容量：正值按字面使用；`0` 不是“无限”，而是派生为 `max(134217728, 2 × executorQueueMaxBytes)`。默认 `executorQueueMaxBytes=64 MiB` 时派生结果是 `134217728`（128 MiB），即 `MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES`。想收紧不可信网络的入站内存占用，可以直接写正值。

### executor 与背压

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `executorQueueCapacity` | `1024` | `> 0` | 全局 backlog 任务数硬上限 |
| `executorQueueMaxBytes` | `67108864` | `>= 0`；`0` 禁用 bytes 预算 | 全局 backlog retained bytes 硬上限 |
| `executorSchedulingPolicy` | `fair` | `global`/`fair`（大小写不敏感） | `ExecutorTaskQueue` 调度 |
| `backpressureHigh` | `256` | `> 0` | 单连接 pending 高水位 |
| `backpressureLow` | `128` | `>= 0` 且 `< backpressureHigh` | 单连接 pending 低水位 |
| `backpressureBytesHigh` | `16777216` | `>= 0`；`0` 禁用 bytes 水位 | 单连接 pending bytes 高水位 |
| `backpressureBytesLow` | `8388608` | `>= 0`；high 为 `0` 时必须为 `0`，否则 `< high` | 单连接 pending bytes 低水位 |
| `executorMaxDrain` | `512` | `> 0` | 每轮 drain 命令数上限 |
| `executorDrainMillis` | `2` | `> 0` | 每轮 drain 时间预算 |

以上 bytes 类配置都按 `HeapRequestFootprint` 的 heap footprint 口径计量：请求对象、argv 槽位与每参数数组头和对齐 payload 之和，而非纯 payload 求和。每个非空参数计 `ARRAY_HEADER_BYTES`（16）加上按 8 对齐的 payload；空 `byte[]` 只计 16。机制细节、拒绝形态和中转线程见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### transaction 队列

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `transactionQueueMaxCommands` | `1024` | `>= 0`；`0` 禁用 | 连接建立时传给 `EngineSession` |
| `transactionQueueMaxBytes` | `67108864` | `>= 0`；`0` 禁用 | 同上 |

### 连接保护与空闲

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `client-idle-timeout-millis` | `0` | `>= 0`；`0` 不主动断开 | pipeline `IdleStateHandler` |
| `client-output-buffer-limit-bytes` | `67108864` | `>= 0`；`0` 不设自定义 watermark | Netty `WriteBufferWaterMark` |
| `client-output-buffer-over-limit-millis` | `10000` | `>= 0`；limit 大于 `0` 时必须大于 `0` | 慢客户端宽限关闭 |

### responder reply 准入（硬容量）

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `replyGlobalCapacityBytes` | `268435456` | `> 0` | 全局 reply 容量 |
| `replyPerConnectionCapacityBytes` | `134217728` | `> 0` | 单连接 reply 容量 |
| `replyMaxTotalBytes` | `67108864` | `> 0` | 单条顶层 reply 计费上限 |
| `replyChunkPayloadBytes` | `65536` | `> 0` | reply chunk payload 容量 |
| `replyControlReservationBytes` | `4096` | `> 0` 且 `>= 1539` | 每槽位控制错误预留 |
| `replyDrainTimeoutMillis` | `5000` | `> 0` | graceful shutdown 排空上限 |

这些值彼此有顺序约束，启动时会一并校验：`replyControlReservationBytes >= 1024 + 515 = 1539`；`control <= replyMaxTotalBytes`；`replyMaxTotalBytes <= replyPerConnectionCapacityBytes`；`replyPerConnectionCapacityBytes <= replyGlobalCapacityBytes`；并且 `control + chunk + 1024 <= replyMaxTotalBytes`。任何一条不满足都算配置错误，不是运行时背压信号。reply 所有权、result-unknown 与关闭语义见本文 §七（生产环境加固与验收操作）。

### TTL、maintenance 与 defrag

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `cleanupIntervalMillis` | `1000` | `>= 0`；`0` 切换为纯 deferred reclamation | maintenance tick 周期 |
| `noCleanup` | 关 | flag，归一化为 `cleanupIntervalMillis=0` | 同上 |
| `expireCleanupTimeLimitMillis` | `5` | `> 0` | 单次 expire cleanup 时间预算 |
| `keysTimeBudgetMillis` | `0` | `>= 0`；`0` 不设时间预算 | `KEYS` 扫描预算 |
| `keysMaxResults` | `Integer.MAX_VALUE` | `>= 0`；`0` 禁用 `KEYS` | `KEYS` 最大返回条数 |
| `nativeDefragEnabled` | 关 | flag | maintenance tick 里开 defrag |
| `nativeDefragMaxMoveBytes` | `65536` | `>= 0` | 每次 tick 最大移动字节 |
| `nativeDefragMaxObjects` | `64` | `>= 0` | 每次 tick 最大检查对象数 |
| `nativeDefragTimeLimitMillis` | `1` | `>= 0` | defrag 时间预算 |
| `nativeSlotCapacity` | `0` | `>= 0`；`0` 保留默认 slot 容量 | native object slot 容量覆盖 |

### maxmemory 与 eviction

| 配置键 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `maxmemoryScope` | `global` | `global`/`per-db`（接受 `perdb`/`per_db`） | instance 预算协调范围 |
| `maxmemoryPolicy` | `noeviction` | `noeviction`/`allkeys-random`/`allkeys-lru` | eviction 策略 |
| `maxmemorySamples` | `5` | `> 0` | 采样数量 |
| `evictionTimeLimitMillis` | `5` | `> 0` | 单次 eviction 时间预算 |

## 网络和实例规模

`ioThreads` 是最容易误解的一项。它们是 Netty worker，只处理 socket I/O、pipeline decode/encode 事件和定时器触发，与 DB mutation 并行度无关。DB 读写和 maintenance 里的 DB 访问都经 `CommandExecutor` 的 owner thread 进入；`YierdisInstance` 同样要求 DB 访问先绑定到 owner thread，跨线程访问会 fail-fast。

单 owner 是有意保留的执行模型，并非“调大 `CommandExecutor` 线程数就能消除”的临时限制。`DefaultEventExecutorGroup(1)` 的线程数写死在 bootstrap 里，它让 keyspace、TTL、stable backend、mutation ledger 和连接会话在同一条命令序列中推进，DB state 无需在每个结构内部再实现并发写入协议。

真正的 shard-per-core 必须作为一套完整执行架构实现：每个 shard 拥有独立 DB、allocator 和 runtime；提交前按命令 key 规划路由；同一连接仍保持顺序执行，并正确携带 `SELECT`、RESP 协商和 `MULTI/EXEC` 状态；跨 key 命令还需要明确单 shard 限制或跨 shard 协调协议。global maxmemory、maintenance 和 shutdown 也必须覆盖全部 shard。在这些契约同时落地前，增加 DB owner 数会破坏现有语义，因此当前配置不提供伪并行的 storage-shard 开关。

本地运行命令应和根 `README.md` 保持一致：

```bash
mvn -q -DskipTests package
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar
```

仓库根目录自带一份 `yierdis.conf` 模板：全部配置键都在里面，非必填键以注释形式给出默认值，取消注释即可覆盖。注意配置文件里必须显式带 `maxmemoryBytes` 键（模板中已默认 `=0`）。省略它会在 stderr 打一行原因并以退出码 2 结束，这是有意的安全护栏：把"忘记配置容量上限"和"明确选择无限制"区分开。

启动后可以用 `redis-cli` 或项目 CLI：

```bash
redis-cli -p 6378 PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS
```

## 协议入口限制

`protocolMaxBulkBytes`、`protocolMaxArgs`、`protocolMaxLineBytes` 和 `protocolMaxCommandBytes` 会直接传给 `RespRequestDecoder`，分别约束 bulk body、参数个数、header/inline 行长度，以及单条命令的 heap footprint 估算字节数（`HeapRequestFootprint` 口径，含请求对象、argv 槽位和每个参数的数组头与对齐 payload，不等于纯 payload 求和）。`protocolGlobalInFlightBytes` 则约束 ingress 全局在途内存，见上文。暴露在不可信网络里时，优先收紧这几个入口上限，再考虑更深层的内存调参。

解析失败会走 RESP protocol error 路径：`RespRequestDecoder` 做 RESP 解析、入口限制和 ingress admission，出错时把 `RespProtocolError` 放进已注册 reply slot；协议错误由 `NettyExecutionRequestIngress` 统一回复并关闭连接，避免请求和回包错位。这个路径不进入 command executor。

## executor 和背压

executor 参数分两类：全局队列预算（`executorQueueCapacity`、`executorQueueMaxBytes`、`executorSchedulingPolicy`、`executorMaxDrain`、`executorDrainMillis`）和单连接背压（`backpressureHigh/Low`、`backpressureBytesHigh/Low`）。默认值见上表。

`YierdisServerRuntimeConfig.executorConfig()` 把已经校验的 runtime 字段映射成 `CommandExecutorConfig`。`CommandExecutor` 只有一个 owner executor，启动时 `executor.start()` 在 owner 上调用 `bindToCurrentThread`，之后通过 `tryAcquire(...)` 和 `ExecutorAdmission.publish(...)` 接收 Netty pipeline 交来的请求。

可以这样理解“改一个值会怎样”：

- 调大 `executorQueueCapacity`：全局 backlog 能容纳更多未执行任务，高并发下更少触发 `Unavailable`（输入暂停），但队列积压延迟上限也更高。它同时抬高了全局背压高水位（约为容量的 75%）。
- 把 `executorQueueMaxBytes` 设为 `0`：完全关闭 queued bytes 预算，只按任务数限制 backlog；此时任何单请求都不会因为 bytes 超限被拒，也观测不到 `submit_rejected_bytes_budget_total`。
- 收紧 `backpressureHigh/Low`：单连接更早停收，`conn_autoread_disabled_by_executor` 更频繁为 `1`，但低水位滞回避免 `autoRead` 抖动。
- 调大 `executorMaxDrain` / `executorDrainMillis`：单轮 drain 处理更多命令再让出，吞吐更平滑但单轮延迟更长。

queue slot 或 bytes budget 暂时不足时，ingress 暂停输入并等待容量（`onAdmissionAvailable` 注册一次性回调），不会立即生成 busy reply。单个请求本身超过 bytes hard limit 时返回：

```text
ERR request exceeds executor queue byte limit
```

连接已经进入 closing 时，提交层会在预留 queue slot / bytes budget 前拒绝，计入 `submit_rejected_closing_total`；该路径不会再额外写 `ERR busy`。

改配置后要验证就查 `STATS`。重点看 `queued_tasks`、`queued_bytes`、`submit_rejected_queue_full_total`、`submit_rejected_bytes_budget_total`、`submit_rejected_closing_total`、`backpressure_enter_total`、`backpressure_exit_total`，以及当前连接的 `conn_pending`、`conn_pending_bytes`、`conn_autoread_disabled_by_executor` 和 `conn_commands_rejected`。完整机制、线程切换点和 result-unknown 来源见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## transaction 保护

事务队列是连接级状态，创建连接时 `NettyExecutionConnection.getOrCreate(...)` 会把：

- `transactionQueueMaxCommands`
- `transactionQueueMaxBytes`

传给 `EngineSession` 的 `DefaultTransactionState`。默认值分别是 `1024` 和 `67108864`；`0` 表示对应限制禁用。

在 `MULTI` 状态下，命令入队会用 `ExecutionRequest.retain()` 取得事务自己的所有权并累计 retained bytes（`HeapRequestFootprint` 口径）；网络请求共享不可变 argv 和 request-memory lease。超过命令数或 bytes 上限时，事务标记为 aborted，入队返回 `ERR Transaction queue is full`；后续 `EXEC` 会返回 Redis 风格 `EXECABORT Transaction discarded because of previous errors.` 并丢弃队列（字符串见 `TransactionCommands.EXEC_ABORT`）。这样可防止大事务或大参数在连接状态里无界驻留。

推荐看 `TransactionQueueLimitTest` 和 `EngineSession`。

## TTL 和 maintenance

TTL 语义是“访问时惰性删除 + 轻量后台清理”。相关参数见上表，这里说清默认行为：

- `cleanupIntervalMillis` 默认 `1000` ms。它决定 maintenance tick 的周期，也决定 tick 里跑什么。
- 设为 `0`（或 `noCleanup`）**不是完全停掉定时器**：bootstrap 会把周期改成固定 `DEFERRED_RECLAMATION_INTERVAL_MILLIS = 1000` ms，并把 tick 内容从 `maintenanceTick` 换成 `deferredReclamationTick`。换句话说，周期性 expire cleanup 被关掉，但延迟回收仍需周期性推进。

bootstrap 使用 Netty worker event loop 做定时器，但定时器只提交 `executor.executeMaintenance(...)`。真正的 DB cleanup、global maxmemory maintenance 和 native defrag 都在 DB owner thread 上执行。调度侧用一个 `AtomicBoolean maintenancePending` 做 coalesce：上一轮还没结束就跳过本轮，避免高压下堆积追赶式 maintenance 任务。

`nativeDefragEnabled` 只是给 `YierdisDb.defragMaintenance()` 提供预算闸门；更细的移动、pin、quarantine 和 object table 语义看 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

`KEYS` 的时间和结果数预算由 bootstrap 转成 `SlowCommandLimits`，再由 `DefaultCommandModules.create(...)` 注入命令模块。大 keyspace 运行时优先使用 `SCAN`，把 `KEYS` 当成受限诊断工具；`keysMaxResults=0` 会直接禁用 `KEYS`。

连接空闲超时 `client-idle-timeout-millis` 默认 `0`，表示不因空闲主动断开；不可信或资源紧张的部署可以显式设置正值。

当前 native-memory 路径统一使用 JDK 25 FFM。更细的 runtime、region、arena 和 copy 边界见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

TTL 命令写路径、lazy expire、cleanup sample/budget 和 expiration reclamation 见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md#一ttl-与过期生命周期)。这里的配置章节只保留参数和 runtime 调度顺序。

## maxmemory 和 eviction

`YierdisServerBootstrap` 把 server runtime scope 和 native slot capacity 映射进 `YierdisInstanceConfig`。`YierdisInstance.create(config)` 使用唯一的 FFM backend 组合，不接受可替换的 DB factory。

- `global`（默认）：每个 DB 仍有独立的 keyspace、entry table、roots、ledger 和 FFM backend/runtime，但 maxmemory 由 `YierdisGlobalMaxmemoryGovernor` 跨 DB 协调。governor 汇总每个 participant 报告的 owned `MemoryUsageSnapshot`，不另加 runtime 级 usage source。
- `per-db`：兼容模式。`YierdisInstance` 把 `maxmemoryBytes` 按 DB 数硬分摊，整数除法后的余数按 DB 创建顺序每个 DB 多给 1 byte。每个 DB 都创建独立 backend/runtime，evict/reserve/memory stats 按单 DB 预算运行。

`global` 与 `per-db` 的区别在预算协调范围，与 FFM runtime ownership 无关。别把 `ioThreads`、Netty 连接数或 DB 数当成 maxmemory 的并发写入模型，mutation 仍经 owner thread。

`MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph；native memory 是否纳入 maxmemory 要看字段口径。global scope 下 `NettyServerInfoProvider.memoryStats(...)` 会优先返回 instance 聚合视角；`per-db` scope 下它返回 `null`，命令层回退到当前 DB 的 `memoryStats()`。

更细的 reservation 顺序、`usedBytes` / `reservedBytes` 口径、victim 选择、global governor 协调和 eviction reclamation 见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 慢客户端和输出缓冲保护

`YierdisServerChannelInitializer.initChannel(...)` 在连接建立时按顺序做这些事：

1. 连接准入：`childChannelRegistry.admit(ch)`，超过 `maxClients` 直接不装 pipeline。
2. 当 `client-output-buffer-limit-bytes` 大于 `0` 时，把 Netty channel 的 `WriteBufferWaterMark` 设成 `low = max(1, high/2)`、`high = limit`；为 `0` 时不覆盖 channel 原有 watermark。
3. 创建 `NettyExecutionConnection`（含 `EngineSession` 和事务队列上限）并绑定 owner task executor；注册 `closeFuture` 回调做 `markClosing`。
4. 装配 inbound budget、`InboundReadCreditHandler`、`ConnectionReplySequencer`、reply gate 和 `RespRequestDecoder`。
5. 始终安装 `WriteBufferBackpressureHandler`。channel 不可写时调用 `executor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；恢复可写时由 owner executor 调用 `recoverInputIfPossible(...)`。
6. 如果 channel 持续不可写超过宽限期，经 `NettyExecutionConnection.initiateClose()` 统一关闭：先标记 closing、回收事务状态，再关闭 transport；`client-output-buffer-limit-bytes` 为 `0` 时 handler 的 grace 为 `0`，不会调度这类慢客户端宽限关闭。
7. 当 `client-idle-timeout-millis` 大于 `0` 时安装 `IdleStateHandler` 和 `CloseOnReadIdleHandler`，读空闲超时后同样经 `initiateClose()` 关闭连接。
8. 最后按序追加 `inboundReadCredit`、`inboundByteAccounting`、`respRequestDecoder`、`executionRequestIngress`。

这层保护处理的是慢读客户端和闲置连接，和 executor queue/backpressure 互补：前者看 Netty outbound buffer 和读空闲，后者看入站请求积压。即使关闭 Yierdis 自定义 output-buffer limit，Netty channel 仍然有自身的 writability 状态；如果 channel 按当前 watermark 变为不可写，transport backpressure 仍会暂停 `autoRead`。

## 可观测命令

`INFO` 返回 Redis 风格文本块。section 固定按 `# Server`、`# Health`、`# Clients`、`# Memory`、`# Stats`、`# Keyspace` 输出。不带 section，或带上 `default`/`all`，会输出全部。可以同时给出多个 section：同名只保留一次，未知名字忽略，输出仍是这个固定顺序，不按请求顺序。单独的 `INFO health` 和 `INFO yierdis` 仍是结构化 map；`health` 一旦和其他 section 写在一起，就回到文本块。文本适合人工排查，也便于与 Redis 经验对照。

- `# Server`：`redis_version`、`tcp_port`、`uptime_in_seconds`、`uptime_in_milliseconds`。
- `# Health`：`lifecycle_state`、`ready`、`writable`、`databases`、`degraded_databases`、`connected_clients`、`total_connections_received`、`rejected_connections`、`max_clients`，出故障时再加 `first_failure_type` / `first_failure_message`。
- `# Memory`：`used_memory`、`used_memory_dataset`、`maxmemory`、`maxmemory_policy`、`yierdis_maxmemory_scope`、`yierdis_ledger_used_bytes`、`yierdis_ledger_reserved_bytes`、`yierdis_maxmemory_used_bytes`、`yierdis_maxmemory_effective_used_bytes`、`yierdis_offheap_included_in_maxmemory`、`yierdis_offheap_used_bytes`、native metadata/data committed 摘要、`yierdis_native_live_objects`、`yierdis_native_live_regions` 等。per-db scope 且 maxmemory 大于 0 时额外输出 `yierdis_maxmemory_per_db_bytes`。
- `# Stats`：`total_commands_processed`、`rejected_connections`、`total_connections_received`、`instantaneous_ops_per_sec`，以及带 `yierdis_` 前缀的 queue/inbound/reply/outbound/egress/deferred 组字段。

`INFO yierdis` 返回结构化 map（bulk string 键值对，更适合脚本和测试）。它的 server 组字段是：`server`、`version`、`port`、`io_threads`、`executor_policy`、`executor_queue_capacity`、`executor_queue_max_bytes`、`backpressure_high`、`backpressure_low`、`backpressure_bytes_high`、`backpressure_bytes_low`、`executor_max_drain`、`executor_drain_millis`、`started_millis`、`uptime_millis`。除 server 组外，结构化输出还包含 deferred、inbound、reply capacity、outbound、egress、live channel、health、databases、max_clients 等组。

`INFO health` 返回 health 结构化 map。`STATS` 返回结构化 map，聚焦 executor 和当前连接统计（`conn_*` 字段），遇到输入被暂停或吞吐抖动时先看它。

每次 `INFO`、`INFO yierdis`、`INFO health` 或 `STATS` 执行时，`NettyServerInfoProvider` 都先构造一份请求级 `ServerStatsSnapshot`。executor、ingress、egress、child channels、runtime health 和 uptime 只采样一次，文本与结构化 writer 共享这份快照，避免同一个回复里的字段来自不同采样时刻。`INFO memory` 和 `INFO keyspace` 的 DB 聚合仍按 section 按需读取，不让轻量 health 探针承担全库聚合成本。

`MEMORY STATS` 返回内存估算 map，字段（按声明顺序）为：`maxmemory_bytes`、`used_bytes_for_maxmemory`、`effective_used_bytes_for_maxmemory`、`ledger_used_bytes`、`offheap_used_bytes`、`ledger_reserved_bytes`、`offheap_included_in_maxmemory`、`total_estimated_bytes`、`keys_stored_offheap`、`key_count`、`expire_count`。`ledger_used_bytes` 是 heap 估算 `heapDataBytesEstimate`，`ledger_reserved_bytes` 才是 ledger `reservedBytes`。

`MEMORY USAGE key` 返回某个 key 的估算字节数，用于定位大 key。`OBJECT ENCODING key` 返回内部编码名，例如 string 的 `int` / `embstr` / `raw`，collection 的 `listpack` / `hashtable` / `intset` / `quicklist` / `skiplist` 等，用于理解数据结构升级和存储形态。

常用检查命令：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO health
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar MEMORY STATS
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar MEMORY USAGE mykey
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar OBJECT ENCODING mykey
```

## 常见运行场景

**本地开发**：先按 `README.md` 跑默认 server，再用 CLI 或 `redis-cli` 执行 `PING`、`SET`、`GET`、`INFO yierdis`、`STATS`。需要看数据结构时加 `OBJECT ENCODING`；需要看预算口径时加 `MEMORY STATS` 和 `MEMORY USAGE`。

**弱隔离或不可信客户端**：优先收紧 `protocolMaxBulkBytes`、`protocolMaxArgs`、`protocolMaxLineBytes`、`protocolMaxCommandBytes` 和 `protocolGlobalInFlightBytes`，再设置 `client-idle-timeout-millis`、`client-output-buffer-limit-bytes` 和 `client-output-buffer-over-limit-millis`。随后根据 `STATS` 中的 reject 和 backpressure 计数调整 executor queue/backpressure。

**高并发压测**：不要只增加 `ioThreads`。Netty worker 只扩大 I/O 处理能力，DB mutation 仍经 executor owner thread。更关键的是固定 workload shape，用相同的 `REQUESTS`、`CLIENTS`、`PIPELINE`、`DATA_SIZE` 和 server 参数比较结果：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

benchmark 只连已运行的 server，脚本不会替你启动或停止 Yierdis；`scripts/bench.sh` 默认目标是 `127.0.0.1:16378`，而 server 默认监听 `6378`，所以压测要么给 server 传 `port 16378`，要么给脚本设 `PORT=6378`。

**大 keyspace 或慢扫描**：优先用 `SCAN`，并用 `keysTimeBudgetMillis`、`keysMaxResults` 控制 `KEYS` 风险。TTL 或 native defrag 压力明显时，检查 `cleanupIntervalMillis`、`expireCleanupTimeLimitMillis` 和 native defrag budget；用 `MEMORY STATS` 观察 rehash/reserved，用 `INFO` memory section 观察 native defrag 摘要。

**maxmemory 调试**：先决定 scope。想模拟实例级 Redis 风格预算，用默认 `maxmemoryScope global`；想验证每个 DB 独立预算，用 `maxmemoryScope per-db`。例如：

```bash
cat > /tmp/yierdis-maxmemory.conf <<'EOF'
port=6378
maxmemoryBytes=10485760
maxmemoryScope=global
maxmemoryPolicy=allkeys-lru
maxmemorySamples=5
EOF
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --config /tmp/yierdis-maxmemory.conf
```

## 启动失败和关闭

启动失败常见位置：

- 配置文件缺失或未知键：`YierdisServerFileConfig.fromProperties(...)` 抛 `IllegalArgumentException`，`ServerConfig.fromArgs(...)` 只在 stderr 打一行错误（不打印 usage，退出码 2）。
- 缺少 `maxmemoryBytes` 键：同样打一行错误，提示必须显式指定。
- 配置校验失败：`normalizeAndValidate()` / `YierdisServerRuntimeConfig` 抛 `IllegalArgumentException`，例如端口越界、watermark 非法、output buffer grace 为 `0`、reply 容量顺序不满足。
- JDK 不满足要求：启动前使用 JDK 25 编译/运行环境；直接 FFM imports 会在不兼容环境中失败。
- 端口绑定失败：Netty `bind(...)` 报错。
- DB/native runtime 初始化失败：`YierdisInstance.create(...)` 会 best-effort 关闭已创建 DB 和 factory-owned resources 再抛出启动失败。

`YierdisServerBootstrap.start(config)` 使用 `ok` 标记，启动任一步失败都会调用 `close()` 做清理；`close()` 用 `closeAttempt` 保证幂等，状态从 `CLOSING` 推进到 `CLOSED`，失败则 `FAILED`。

关闭是 best-effort，`closeInternal()` 顺序大致是：server channel → child input（`beginShutdown` + `markClosing`）→ cleanup future → executor graceful shutdown → child reply drain（受 `replyDrainTimeoutMillis` 限制，超时则 force-close）→ inbound/outbound budget → instance runtime access → command group → boss group → worker group。runtime access 的关闭会经 `executor.executeOwnerTask(runtimeAccess::close)` 回到 owner thread，避免在错误线程释放已绑定 DB runtime。某一步失败会记进聚合的 failure 并继续关闭后续资源，最后一起抛出。

脚本层关闭逻辑也要按真实进程处理：谁启动 server，谁负责用 `trap … EXIT` 把它停掉；connect-only benchmark 不拥有也不停止目标 Yierdis。

## 生产环境加固与验收操作

本节是单节点生产加固方案（Production Hardening）的运行时契约与运维手册，定义了服务端强约束容量边界、容量估算推导、Result-Unknown 判定条件、优雅停机所有权时序以及发布验收测试规范。

### 运行时基线 (Runtime Baseline)

全仓构建、测试、打包和压测统一使用 JDK 25。在自动化脚本与故障复现时需显式导出环境：

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
export PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH
java -version
mvn -version
```

验证打包产物时，确保环境处于 JDK 25：

```bash
mvn -DskipTests package
printf 'port=6378\nmaxmemoryBytes=0\n' > /tmp/yierdis-check.conf
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --config /tmp/yierdis-check.conf
```

排查运行时实例使用 `INFO`、`INFO stats`、`STATS` 和 `MEMORY STATS`。**切勿仅凭 JVM heap 占用推断系统限制**：请求入站（Ingress）、堆外/Maxmemory 以及回复出站（Outbound Reply）属于三个完全独立的有界所有权域。

### Reply 与 Ingress 准入容量约束

以下 Reply 限制是在启动时进行静态严格校验的硬性容量（`YierdisServerRuntimeConfig.normalizeAndValidate`），任何请求都无法通过直接写或可扩容 Buffer 绕过：

| 配置项 | 默认值 | 含义 |
| --- | ---: | --- |
| `replyGlobalCapacityBytes` | `268435456` | 所有连接允许准入的 RESP Reply 内存总硬上限（256 MiB） |
| `replyPerConnectionCapacityBytes` | `134217728` | 单条连接允许准入的 RESP Reply 内存硬上限（128 MiB） |
| `replyMaxTotalBytes` | `67108864` | 单个顶级 Reply 允许计费的最大字节数，含持有的底层源数据（64 MiB） |
| `replyChunkPayloadBytes` | `65536` | 回复分块（Chunk）的固定有效载荷大小（64 KiB） |
| `replyControlReservationBytes` | `4096` | 每个 Reply Slot 预留的控制/错误回复配额（必须先于业务回复写入） |
| `replyDrainTimeoutMillis` | `5000` | 优雅停机期间 Reply 排空的等待超时时间 |

启动时会验证容量大小关系的合法性：控制预留必须能够容纳固定 Reply 开销加上最大的标量错误帧。常量定义为 `REPLY_FIXED_OVERHEAD_BYTES = 1024`，`REPLY_MAX_CONTROL_ERROR_FRAME_BYTES = 515`，因此 `MIN_REPLY_CONTROL_RESERVATION_BYTES = 1539`。

校验链要求：
1. `replyControlReservationBytes` 不得超过单回复容量 `replyMaxTotalBytes`；
2. 单回复容量 `replyMaxTotalBytes` 不得超过单连接容量 `replyPerConnectionCapacityBytes`；
3. 单连接容量不得超过全局容量 `replyGlobalCapacityBytes`；
4. `controlReservation + chunkPayload + fixedOverhead` 必须能装入单回复容量。

若不满足上述条件，启动阶段直接抛出配置异常（fail-fast），绝不作为运行时背压降级。

#### 容量估算推导

容量配比是一个严格的算术问题，不可凭经验猜测：
1. **Ingress（入站）**：`protocolGlobalInFlightBytes` 限制已解析请求的总在途字节。正值按字面限制；为 `0` 时不是无限制，而是派生为 `max(128 MiB, 2 × executorQueueMaxBytes)`（下限 `MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES = 128 MiB`）。协议解码器在请求抵达 executor 之前还会强校验 `protocolMaxBulkBytes`、`protocolMaxArgs`、`protocolMaxLineBytes` 和 `protocolMaxCommandBytes`。
2. **Reply（出站）**：单个 Reply 最大计费为 `replyMaxTotalBytes`。由于控制预留和首个分块必须计入配额，实际有效业务回复预算约为 `replyMaxTotalBytes - (replyControlReservationBytes + replyChunkPayloadBytes) - fixedOverhead`。并发连接的总 Reply 内存严格受控于 `replyGlobalCapacityBytes`。
3. `client-output-buffer-limit-bytes` 与 `client-output-buffer-over-limit-millis` 属于针对慢客户端的策略性保护，绝不替代上述硬性准入容量限制。

#### `OutboundMemoryBudget` 的两套计量

- `reserved`：向 Reply Slot 收取的容量配额（包含编码输出与持有的底层数据源字节）；DB 数据源对象在同步渲染期间由 `PreparedCommand` 持有。
- `allocated`：当前从该预留中实际物化出来的 Chunk Buffer 物理容量。

两组数值均受统一的硬限制层级约束。DB 流式源在同步渲染完成后关闭，但其计费保留在 Reply Slot 租约中，直到该 Slot 达到终态被清理（`ReplySlot`, `BoundedChunkedReplySink`）。

### 调度策略与预检机制

每个客户端输入都会按接收顺序分配一个 Reply Slot（包括普通命令、BUSY 拒绝、协议错误、内部故障及 `QUIT` 这类 reply-and-close 命令）。后续就绪的回复必须排在先前的 Slot 之后。有界 egress 负责人负责 chunk 分片，命令处理器本身不直接向 socket 写字节。

在执行 mutation 之前，只要回复规模可以安全预估，命令均会预先声明 Reply Plan。如果发生超限预检失败，mutation 根本不会提交，数据库保持原状。聚合回复数据源在 planning 和同步渲染全程由 `PreparedCommand` 持有，禁止复制到无界 detached 列表中。

当 Reply 容量阻塞队列头部时，由 executor 调度策略决定处理行为：
- `FAIR`：轮转可运行连接，防止某条因自身 Reply 耗尽而等待的连接阻塞其他有可用容量的独立连接；
- `GLOBAL`：维护全局严格 FIFO 头，后续任务不得跨越阻塞的队头。

无论何种策略均不放宽容量限制。当遇到 `ReplyTooLargeException` 时，表明单个回复无法用当前限制表示，服务端直接关闭该传输通道，绝不虚构替换错误信息。

### Result-Unknown 行为契约

系统严格区分两类故障：
1. **确定性失败（Deterministic）**：在 mutation 或可见 reply 输出之前触发的预检拒绝。安全返回容量或命令错误，DB 状态未发生变更。
2. **结果未知失败（Result-Unknown）**：在 mutation 可能已经提交之后、reply 字节已经部分发出之后、或者写回结果产生歧义时发生的故障（例如提交后 mutation 异常、底层 write 失败、source/chunk 长度不匹配、输出中断开连接等）。

对于 Result-Unknown 失败，服务端立即取消该 Reply Slot 并**直接关闭底层连接**，绝不捏造 `-ERR internal error`，因为伪造错误会与客户端可能已观察到的变更产生矛盾。每次此类关闭均递增计数器 `yierdis_result_unknown_closes`。

### 可观测指标与泄漏排查矩阵

`INFO stats` 输出运行指标。文本格式中各字段带有 `yierdis_` 前缀；在 `STATS` 命令的结构化 Map 中则不带该前缀：

| 领域 | 核心排查字段 |
| --- | --- |
| Ingress（入站） | `inbound_capacity_bytes`, `inbound_reserved_bytes`, `inbound_peak_reserved_bytes`, `inbound_waiting_connections`, `inbound_backpressured`, `inbound_rejected_connections`, `inbound_closed` |
| Reply 容量 | `reply_global_capacity_bytes`, `reply_per_connection_capacity_bytes`, `reply_max_total_bytes`, `reply_chunk_payload_bytes`, `reply_control_reservation_bytes`, `reply_drain_timeout_millis`, `outbound_reserved_bytes`, `outbound_allocated_bytes`, `outbound_peak_reserved_bytes`, `outbound_peak_allocated_bytes` |
| Reply 所有权 | `outbound_active_connections`, `outbound_active_slots`, `outbound_active_chunks`, `outbound_active_sources`, `live_child_channels` |
| 异常与调度 | `outbound_capacity_rejects`, `outbound_oversized_replies`, `outbound_cancelled_slots`, `outbound_failed_slots`, `outbound_write_failures`, `result_unknown_closes`, `reply_shutdown_timeouts`, `deferred_fair_reply_heads`, `deferred_global_reply_heads` |
| Shutdown 停机 | `reply_shutdown_timeouts`, `inbound_closed`, 以及最终归零的所有权仪表盘 |
| 内存与堆外 | `INFO memory` 中的 `yierdis_maxmemory_used_bytes`, `yierdis_maxmemory_effective_used_bytes`, `yierdis_ledger_used_bytes`, `yierdis_ledger_reserved_bytes`, `yierdis_offheap_used_bytes`, `native_metadata_committed_bytes`, `native_data_committed_bytes`, `native_data_live_bytes`, `native_live_objects`, `native_live_regions` 等 |

**泄漏判定标准**：在平稳运行中，峰值（Peak）可以非零，但当前 `reserved`/`allocated` 会归零。在测试夹具执行完毕或成功优雅停机后，`active_slots`、`active_chunks`、`active_sources`、`live_child_channels` 以及入站预留必须全部**严格收敛为 0**。客户端断开后若仪表盘非零即表明存在泄漏。

### 优雅停机所有权时序 (Graceful Shutdown)

优雅停机是一个严格的所有权交接协议，而非简单的关闭监听。`YierdisServerBootstrap.closeInternal` 按照以下绝对顺序执行：

1. 关闭 Server Channel，停止接收任何新的入站连接；
2. 关闭子通道注册表（`ChildChannelRegistry.beginShutdown()`）并把所有已建连子通道置为 closing（`NettyExecutionConnection.markClosing`），彻底停用子通道输入读取；
3. 取消维护与清理定时任务（Maintenance / cleanup future）；
4. 命令执行器优雅退出（`CommandExecutor.shutdownGracefully()`），拒绝新任务、取消未开始或等待容量的回复，排空已经开始执行的任期所有者；
5. 各连接排序器（`ConnectionReplySequencer`）按接收顺序刷新 READY 队头，受 `replyDrainTimeoutMillis` 硬超时限制。若超时，则强制关闭剩余子通道（`ChildChannelRegistry.forceClose()`），记录 `yierdis_reply_shutdown_timeouts` 并汇报停机失败；
6. 仅在子通道所有权彻底排空后，依次关闭 Ingress 和 Outbound 内存预算、实例运行时资源（DB 与 Native 堆外），最后按序释放 Netty Command Group、Boss Group 和 Worker Group。

停机超时不视为成功关闭。超时异常信息会明确附带 `liveChildren`, `reservedBytes`, `allocatedBytes`, `activeConnections`, `activeSlots` 等现场上下文。

### 自动化验收校验与基准压测对比

全部测试使用统一的 JDK 25 环境。

#### 核心回复与双重写入回归校验

用于快速验证接收顺序、容量边界与 Result-Unknown 所有权：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=OrderedReplyIntegrationTest,MaxmemoryDoubleReplyRegressionTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsurefire.rerunFailingTestsCount=3 test
```

#### DB 架构守卫测试

在修改公开工厂或接口可见性后执行：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=YierdisDbArchitectureGuardTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

#### 真实 Jar 启动与验证

```bash
printf 'port=16379\nmaxmemoryBytes=0\n' > /tmp/yierdis-release-check.conf
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --config /tmp/yierdis-release-check.conf
redis-cli -p 16379 PING
```

#### 基准性能对比规则

压测脚本 `scripts/bench.sh` 仅连接已启动的 Yierdis 实例（不带认证，仅在 `database != 0` 时发 `SELECT`）：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 FORMAT=csv HOST=127.0.0.1 PORT=16378 \
  REQUESTS=100000 CLIENTS=50 DATA_SIZE=3 PIPELINE=1 \
  ./scripts/bench.sh > target/yierdis-benchmark.csv
```

CSV 输出格式定义为（`BenchmarkOutputRenderer.CSV_HEADER`）：

```text
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms","status","reason"
```

前 8 个字段与 Redis 官方基准 CSV 完全对齐，是跨系统性能对比的标准表头；`status` 与 `reason` 为 Yierdis 专有扩展字段。标准场景包含 17 项 `SUCCESS` 测试以及 4 项目前为 `UNSUPPORTED` 的项目（`SPOP`, `ZPOPMIN`, `MSET (10 keys)`, `XADD`）。对比双方性能时必须使用完全相同的参数（requests, clients, pipeline, data-size, keyspace, keep-alive, database 等）。