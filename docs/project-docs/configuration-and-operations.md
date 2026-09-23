# 配置与运行

本文回答三件事：启动参数怎么变成运行时配置、每个配置项的默认值和取值边界是什么、改完之后在运行中的 server 上能看到什么变化。读者读完后应能自己启动、调参、压测和排障。

## 配置流向与信任边界

启动参数从命令行到组合根的主路径是：

```text
argv
  -> YierdisServerArgs (picocli)
  -> normalizeAndValidate()
  -> YierdisServerRuntimeConfig   // 唯一信任边界，构造器集中校验
  -> ServerConfig.fromArgs(...)   // CLI 边界，失败打 usage
  -> YierdisServerBootstrap.startInternal()
```

`YierdisServerArgs` 用 picocli 声明每个 `@Option`：默认值写在 `defaultValue` 和字段初值里（两处需要一致），usage 文案也来自这里。字段的默认值、范围校验分散在两个阶段，理解分工很重要：

- `normalizeAndValidate()` 只做 CLI 归一化和派生：`--noCleanup` 把 `cleanupIntervalMillis` 归零；`bind` 去掉首尾空白；三个枚举（`executorSchedulingPolicy`、`maxmemoryScope`、`maxmemoryPolicy`）在这里解析一次并缓存实例，再把 argv 里的字符串改写成稳定值，不再走字符串 round-trip。归一化后 `maxmemoryScope` 只会是 `global` 或 `per-db`，`maxmemoryPolicy` 只会是它的 `redisName()`。
- `YierdisServerRuntimeConfig` 的紧凑构造器是真正的校验点：网络、协议、reply、内存、maintenance、executor 队列与背压约束都在这里检查，越界直接抛 `IllegalArgumentException`。这是启动参数唯一的校验边界；下游的 `CommandExecutorConfig` 只承载已经校验过的值，不再重复校验（见该 record 的注释）。

归一化接受的等价写法：`--maxmemoryScope` 大小写不敏感，并允许 `perdb` / `per_db` / `per-db` 三种拼法（`parseMaxmemoryScope` 内部把 `_` 换成 `-` 再匹配）；`--maxmemoryPolicy` 同样大小写不敏感，`_` 归一成 `-`，只接受 `noeviction`、`allkeys-random`、`allkeys-lru`。

`toRuntimeConfig()` 把已归一化参数转成 `YierdisServerRuntimeConfig` record：首次调用触发 `normalizeAndValidate()` 并缓存，后续调用返回同一实例。这个 record 字段已经是 enum、number 和 boolean，不再携带原始 CLI 字符串；`executorConfig()` 直接生成 executor 领域配置。

`ServerConfig.fromArgs(String...)` 是 CLI 到组合根的边界，顺序如下：

1. `cmd.parseArgs(args)` 解析失败（如未知选项、类型错误）抛 `ParameterException`，打印错误和 usage，包成 `YierdisCliException.usageError(...)`。
2. 除非带 `--help`，必须显式匹配到 `--maxmemoryBytes` 选项，否则抛 `ParameterException("--maxmemoryBytes must be specified explicitly (use 0 to acknowledge unlimited memory)")`。这是唯一的“必须显式传”参数。
3. `--help` 只打 usage 并返回 `null`；`YierdisServerBootstrap.start(String...)` 收到 `null` 会视为“没有可启动配置”。
4. `normalizeAndValidate()` 抛 `IllegalArgumentException`（校验失败）同样打印 usage 并包成 `usageError`。
5. 成功则返回 `toRuntimeConfig()`。

源码入口：

- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/ServerConfig.java`
- `yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

## 启动过程

真正启动发生在 `YierdisServerBootstrap.startInternal()`，顺序如下：

1. 打一行日志 `native memory backend: foreign (JDK 25 FFM)`，后续是 JDK 25 编译目标和直接 FFM imports 提供的 native-memory 运行前提。
2. 把 `YierdisServerRuntimeConfig` 映射成 `YierdisInstanceConfig`：databases、maxmemory、defrag 等。
3. `YierdisInstance.create(config)` 组装唯一的 FFM DB backend，并取得 runtime access、maintenance 入口和 observability。
4. 创建 `NettyServerInfoProvider`，绑定 lifecycle state 和 instance observability，再绑定 inbound/outbound budget、child registry、reply egress stats 和 executor。
5. 由 bootstrap 调用 `CommandRegistries.dispatcher(...)` 注册默认命令模块和 server-only 模块（`ServerCommandModule`）。
6. 创建单线程 `DefaultEventExecutorGroup(1)` 与 `CommandExecutor`；owner executor 是 `NettySerialOwnerExecutor`，背后就是 `commandGroup.next()`。
7. `executor.start()` 在 owner thread 上执行 `bindToCurrentThread`，把该线程标成 DB owner。
8. 按需在 Netty worker event loop 上调度 maintenance tick（见下文 TTL 与 maintenance），但 DB 逻辑仍通过 `executeMaintenance(...)` 回到 owner thread。
9. 创建 boss/worker Netty group，并由 `YierdisServerChannelInitializer` 装配连接 pipeline。
10. `bind(bind, port).sync()`，成功后 `lifecycleState = RUNNING`。

`YierdisInstance` 不是“随手可用的 DB 容器”：`runtimeAccess().bindToCurrentThread()` 先把当前线程标成 owner，后续 DB access 才被允许；跨线程访问会 fail-fast。bootstrap 用 `ok` 标记包住整段启动，任一步失败都会 `close()`，避免留下半初始化实例。

benchmark 不持有 server 参数或生命周期模型，只连接由操作者单独管理的 Yierdis；client idle / output-buffer 这类 server-only 参数必须在启动目标 Yierdis 时直接配置。

## 配置项总览

下面按域列出全部启动选项。`默认` 一列来自 `YierdisServerArgs` 的 `@Option(defaultValue=...)`；`范围/约束` 一列来自 `YierdisServerRuntimeConfig` 构造器或 `normalizeAndValidate()`；`生效阶段` 说明它在启动流程的哪一步被消费。“必须显式指定”的项已单独标注。

### 网络与实例规模

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--bind` | `127.0.0.1` | trim 后非空 | `bind(...)` 监听地址 |
| `--port` | `6378` | `0..65535`（0 由内核分配） | `bind(...)` |
| `--maxClients` | `1024` | `> 0` | `ChildChannelRegistry` 准入 |
| `--databases` | `16` | `1..1024` | 创建 `YierdisInstance` 的 DB 数组 |
| `--ioThreads` | `1` | `> 0` | Netty worker `NioEventLoopGroup` |
| `--maxmemoryBytes` | `0` | `>= 0`，**必须命令行显式出现** | `YierdisInstanceConfig` 预算 |

### 协议入口限制

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--protocolMaxBulkBytes` | `536870912`（512 MiB） | `1..536870912` | `RespRequestDecoder` bulk body |
| `--protocolMaxArgs` | `1048576` | `1..1048576` | 单命令参数个数 |
| `--protocolMaxLineBytes` | `1048576` | `> 0` | header/inline 行长度 |
| `--protocolMaxCommandBytes` | `67108864`（64 MiB） | `1..536870912` | 单命令 heap footprint 估算 |
| `--protocolGlobalInFlightBytes` | `0` | `>= 0` | ingress 全局在途预算，见下 |

`--protocolGlobalInFlightBytes` 是 `InboundMemoryBudget` 的容量：正值按字面使用；`0` 不是“无限”，而是派生为 `max(134217728, 2 × executorQueueMaxBytes)`。默认 `executorQueueMaxBytes=64 MiB` 时派生结果是 `134217728`（128 MiB），即 `MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES`。想收紧不可信网络的入站内存占用，可以直接写正值。

### executor 与背压

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--executorQueueCapacity` | `1024` | `> 0` | 全局 backlog 任务数硬上限 |
| `--executorQueueMaxBytes` | `67108864` | `>= 0`；`0` 禁用 bytes 预算 | 全局 backlog retained bytes 硬上限 |
| `--executorSchedulingPolicy` | `fair` | `global`/`fair`（大小写不敏感） | `ExecutorTaskQueue` 调度 |
| `--backpressureHigh` | `256` | `> 0` | 单连接 pending 高水位 |
| `--backpressureLow` | `128` | `>= 0` 且 `< backpressureHigh` | 单连接 pending 低水位 |
| `--backpressureBytesHigh` | `16777216` | `>= 0`；`0` 禁用 bytes 水位 | 单连接 pending bytes 高水位 |
| `--backpressureBytesLow` | `8388608` | `>= 0`；high 为 `0` 时必须为 `0`，否则 `< high` | 单连接 pending bytes 低水位 |
| `--executorMaxDrain` | `512` | `> 0` | 每轮 drain 命令数上限 |
| `--executorDrainMillis` | `2` | `> 0` | 每轮 drain 时间预算 |

以上 bytes 类配置都按 `HeapRequestFootprint` 的 heap footprint 口径计量：请求对象、argv 槽位与每参数数组头和对齐 payload 之和，而非纯 payload 求和。每个非空参数计 `ARRAY_HEADER_BYTES`（16）加上按 8 对齐的 payload；空 `byte[]` 只计 16。机制细节、拒绝形态和中转线程见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

### transaction 队列

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--transactionQueueMaxCommands` | `1024` | `>= 0`；`0` 禁用 | 连接建立时传给 `EngineSession` |
| `--transactionQueueMaxBytes` | `67108864` | `>= 0`；`0` 禁用 | 同上 |

### 连接保护与空闲

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--client-idle-timeout-millis` | `0` | `>= 0`；`0` 不主动断开 | pipeline `IdleStateHandler` |
| `--client-output-buffer-limit-bytes` | `67108864` | `>= 0`；`0` 不设自定义 watermark | Netty `WriteBufferWaterMark` |
| `--client-output-buffer-over-limit-millis` | `10000` | `>= 0`；limit 大于 `0` 时必须大于 `0` | 慢客户端宽限关闭 |

### responder reply 准入（硬容量）

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--replyGlobalCapacityBytes` | `268435456` | `> 0` | 全局 reply 容量 |
| `--replyPerConnectionCapacityBytes` | `134217728` | `> 0` | 单连接 reply 容量 |
| `--replyMaxTotalBytes` | `67108864` | `> 0` | 单条顶层 reply 计费上限 |
| `--replyChunkPayloadBytes` | `65536` | `> 0` | reply chunk payload 容量 |
| `--replyControlReservationBytes` | `4096` | `> 0` 且 `>= 1539` | 每槽位控制错误预留 |
| `--replyDrainTimeoutMillis` | `5000` | `> 0` | graceful shutdown 排空上限 |

这些值彼此有顺序约束，启动时会一并校验：`replyControlReservationBytes >= 1024 + 515 = 1539`；`control <= replyMaxTotalBytes`；`replyMaxTotalBytes <= replyPerConnectionCapacityBytes`；`replyPerConnectionCapacityBytes <= replyGlobalCapacityBytes`；并且 `control + chunk + 1024 <= replyMaxTotalBytes`。任何一条不满足都算配置错误，不是运行时背压信号。reply 所有权、result-unknown 与关闭语义以 [`production-hardening-operations.md`](./production-hardening-operations.md) 为准。

### TTL、maintenance 与 defrag

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--cleanupIntervalMillis` | `1000` | `>= 0`；`0` 切换为纯 deferred reclamation | maintenance tick 周期 |
| `--noCleanup` | 关 | flag，归一化为 `cleanupIntervalMillis=0` | 同上 |
| `--expireCleanupTimeLimitMillis` | `5` | `> 0` | 单次 expire cleanup 时间预算 |
| `--keysTimeBudgetMillis` | `0` | `>= 0`；`0` 不设时间预算 | `KEYS` 扫描预算 |
| `--keysMaxResults` | `Integer.MAX_VALUE` | `>= 0`；`0` 禁用 `KEYS` | `KEYS` 最大返回条数 |
| `--nativeDefragEnabled` | 关 | flag | maintenance tick 里开 defrag |
| `--nativeDefragMaxMoveBytes` | `65536` | `>= 0` | 每次 tick 最大移动字节 |
| `--nativeDefragMaxObjects` | `64` | `>= 0` | 每次 tick 最大检查对象数 |
| `--nativeDefragTimeLimitMillis` | `1` | `>= 0` | defrag 时间预算 |
| `--nativeSlotCapacity` | `0` | `>= 0`；`0` 保留默认 slot 容量 | native object slot 容量覆盖 |

### maxmemory 与 eviction

| 选项 | 默认 | 范围/约束 | 生效阶段 |
| --- | ---: | --- | --- |
| `--maxmemoryScope` | `global` | `global`/`per-db`（接受 `perdb`/`per_db`） | instance 预算协调范围 |
| `--maxmemoryPolicy` | `noeviction` | `noeviction`/`allkeys-random`/`allkeys-lru` | eviction 策略 |
| `--maxmemorySamples` | `5` | `> 0` | 采样数量 |
| `--evictionTimeLimitMillis` | `5` | `> 0` | 单次 eviction 时间预算 |

## 网络和实例规模

`--ioThreads` 是最容易误解的一项。它们是 Netty worker，只处理 socket I/O、pipeline decode/encode 事件和定时器触发，与 DB mutation 并行度无关。DB 读写和 maintenance 里的 DB 访问都经 `CommandExecutor` 的 owner thread 进入；`YierdisInstance` 同样要求 DB 访问先绑定到 owner thread，跨线程访问会 fail-fast。

单 owner 是有意保留的执行模型，并非“调大 `CommandExecutor` 线程数就能消除”的临时限制。`DefaultEventExecutorGroup(1)` 的线程数写死在 bootstrap 里，它让 keyspace、TTL、stable backend、mutation ledger 和连接会话在同一条命令序列中推进，DB state 无需在每个结构内部再实现并发写入协议。

真正的 shard-per-core 必须作为一套完整执行架构实现：每个 shard 拥有独立 DB、allocator 和 runtime；提交前按命令 key 规划路由；同一连接仍保持顺序执行，并正确携带 `SELECT`、RESP 协商和 `MULTI/EXEC` 状态；跨 key 命令还需要明确单 shard 限制或跨 shard 协调协议。global maxmemory、maintenance 和 shutdown 也必须覆盖全部 shard。在这些契约同时落地前，增加 DB owner 数会破坏现有语义，因此当前配置不提供伪并行的 storage-shard 开关。

本地运行命令应和根 `README.md` 保持一致：

```bash
mvn -q -DskipTests package
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 6378 --maxmemoryBytes 0
```

注意这里必须带上 `--maxmemoryBytes`。省略它会直接启动失败并打印 usage，这是有意的安全护栏：把“忘记配置容量上限”和“明确选择无限制”区分开。

启动后可以用 `redis-cli` 或项目 CLI：

```bash
redis-cli -p 6378 PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS
```

## 协议入口限制

`--protocolMaxBulkBytes`、`--protocolMaxArgs`、`--protocolMaxLineBytes` 和 `--protocolMaxCommandBytes` 会直接传给 `RespRequestDecoder`，分别约束 bulk body、参数个数、header/inline 行长度，以及单条命令的 heap footprint 估算字节数（`HeapRequestFootprint` 口径，含请求对象、argv 槽位和每个参数的数组头与对齐 payload，不等于纯 payload 求和）。`--protocolGlobalInFlightBytes` 则约束 ingress 全局在途内存，见上文。暴露在不可信网络里时，优先收紧这几个入口上限，再考虑更深层的内存调参。

解析失败会走 RESP protocol error 路径：`RespRequestDecoder` 做 RESP 解析、入口限制和 ingress admission，出错时把 `RespProtocolError` 放进已注册 reply slot；协议错误由 `NettyExecutionRequestIngress` 统一回复并关闭连接，避免请求和回包错位。这个路径不进入 command executor。

## executor 和背压

executor 参数分两类：全局队列预算（`--executorQueueCapacity`、`--executorQueueMaxBytes`、`--executorSchedulingPolicy`、`--executorMaxDrain`、`--executorDrainMillis`）和单连接背压（`--backpressureHigh/Low`、`--backpressureBytesHigh/Low`）。默认值见上表。

`YierdisServerRuntimeConfig.executorConfig()` 把已经校验的 runtime 字段映射成 `CommandExecutorConfig`。`CommandExecutor` 只有一个 owner executor，启动时 `executor.start()` 在 owner 上调用 `bindToCurrentThread`，之后通过 `tryAcquire(...)` 和 `ExecutorAdmission.publish(...)` 接收 Netty pipeline 交来的请求。

可以这样理解“改一个值会怎样”：

- 调大 `--executorQueueCapacity`：全局 backlog 能容纳更多未执行任务，高并发下更少触发 `Unavailable`（输入暂停），但队列积压延迟上限也更高。它同时抬高了全局背压高水位（约为容量的 75%）。
- 把 `--executorQueueMaxBytes` 设为 `0`：完全关闭 queued bytes 预算，只按任务数限制 backlog；此时任何单请求都不会因为 bytes 超限被拒，也观测不到 `submit_rejected_bytes_budget_total`。
- 收紧 `--backpressureHigh/Low`：单连接更早停收，`conn_autoread_disabled_by_executor` 更频繁为 `1`，但低水位滞回避免 `autoRead` 抖动。
- 调大 `--executorMaxDrain` / `--executorDrainMillis`：单轮 drain 处理更多命令再让出，吞吐更平滑但单轮延迟更长。

queue slot 或 bytes budget 暂时不足时，ingress 暂停输入并等待容量（`onAdmissionAvailable` 注册一次性回调），不会立即生成 busy reply。单个请求本身超过 bytes hard limit 时返回：

```text
ERR request exceeds executor queue byte limit
```

连接已经进入 closing 时，提交层会在预留 queue slot / bytes budget 前拒绝，计入 `submit_rejected_closing_total`；该路径不会再额外写 `ERR busy`。

改配置后要验证就查 `STATS`。重点看 `queued_tasks`、`queued_bytes`、`submit_rejected_queue_full_total`、`submit_rejected_bytes_budget_total`、`submit_rejected_closing_total`、`backpressure_enter_total`、`backpressure_exit_total`，以及当前连接的 `conn_pending`、`conn_pending_bytes`、`conn_autoread_disabled_by_executor` 和 `conn_commands_rejected`。完整机制、线程切换点和 result-unknown 来源见 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## transaction 保护

事务队列是连接级状态，创建连接时 `NettyExecutionConnection.getOrCreate(...)` 会把：

- `--transactionQueueMaxCommands`
- `--transactionQueueMaxBytes`

传给 `EngineSession` 的 `DefaultTransactionState`。默认值分别是 `1024` 和 `67108864`；`0` 表示对应限制禁用。

在 `MULTI` 状态下，命令入队会用 `ExecutionRequest.retain()` 取得事务自己的所有权并累计 retained bytes（`HeapRequestFootprint` 口径）；网络请求共享不可变 argv 和 request-memory lease。超过命令数或 bytes 上限时，事务标记为 aborted，入队返回 `ERR Transaction queue is full`；后续 `EXEC` 会返回 Redis 风格 `EXECABORT Transaction discarded because of previous errors.` 并丢弃队列（字符串见 `TransactionCommands.EXEC_ABORT`）。这样可防止大事务或大参数在连接状态里无界驻留。

推荐看 `TransactionQueueLimitTest` 和 `EngineSession`。

## TTL 和 maintenance

TTL 语义是“访问时惰性删除 + 轻量后台清理”。相关参数见上表，这里说清默认行为：

- `--cleanupIntervalMillis` 默认 `1000` ms。它决定 maintenance tick 的周期，也决定 tick 里跑什么。
- 设为 `0`（或 `--noCleanup`）**不是完全停掉定时器**：bootstrap 会把周期改成固定 `DEFERRED_RECLAMATION_INTERVAL_MILLIS = 1000` ms，并把 tick 内容从 `maintenanceTick` 换成 `deferredReclamationTick`。换句话说，周期性 expire cleanup 被关掉，但延迟回收仍需周期性推进。

bootstrap 使用 Netty worker event loop 做定时器，但定时器只提交 `executor.executeMaintenance(...)`。真正的 DB cleanup、global maxmemory maintenance 和 native defrag 都在 DB owner thread 上执行。调度侧用一个 `AtomicBoolean maintenancePending` 做 coalesce：上一轮还没结束就跳过本轮，避免高压下堆积追赶式 maintenance 任务。

`--nativeDefragEnabled` 只是给 `YierdisDb.defragMaintenance()` 提供预算闸门；更细的移动、pin、quarantine 和 object table 语义看 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

`KEYS` 的时间和结果数预算由 bootstrap 转成 `SlowCommandLimits`，再由 `DefaultCommandModules.create(...)` 注入命令模块。大 keyspace 运行时优先使用 `SCAN`，把 `KEYS` 当成受限诊断工具；`--keysMaxResults=0` 会直接禁用 `KEYS`。

连接空闲超时 `--client-idle-timeout-millis` 默认 `0`，表示不因空闲主动断开；不可信或资源紧张的部署可以显式设置正值。

当前 native-memory 路径统一使用 JDK 25 FFM。更细的 runtime、region、arena 和 copy 边界见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

TTL 命令写路径、lazy expire、cleanup sample/budget 和 expiration reclamation 见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。这里的配置章节只保留参数和 runtime 调度顺序。

## maxmemory 和 eviction

`YierdisServerBootstrap` 把 server runtime scope 和 native slot capacity 映射进 `YierdisInstanceConfig`。`YierdisInstance.create(config)` 使用唯一的 FFM backend 组合，不接受可替换的 DB factory。

- `global`（默认）：每个 DB 仍有独立的 keyspace、entry table、roots、ledger 和 FFM backend/runtime，但 maxmemory 由 `YierdisGlobalMaxmemoryGovernor` 跨 DB 协调。governor 汇总每个 participant 报告的 owned `MemoryUsageSnapshot`，不另加 runtime 级 usage source。
- `per-db`：兼容模式。`YierdisInstance` 把 `maxmemoryBytes` 按 DB 数硬分摊，整数除法后的余数按 DB 创建顺序每个 DB 多给 1 byte。每个 DB 都创建独立 backend/runtime，evict/reserve/memory stats 按单 DB 预算运行。

`global` 与 `per-db` 的区别在预算协调范围，与 FFM runtime ownership 无关。别把 `ioThreads`、Netty 连接数或 DB 数当成 maxmemory 的并发写入模型，mutation 仍经 owner thread。

`MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph；native memory 是否纳入 maxmemory 要看字段口径。global scope 下 `NettyServerInfoProvider.memoryStats(...)` 会优先返回 instance 聚合视角；`per-db` scope 下它返回 `null`，命令层回退到当前 DB 的 `memoryStats()`。

更细的 reservation 顺序、`usedBytes` / `reservedBytes` 口径、victim 选择、global governor 协调和 eviction reclamation 见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 慢客户端和输出缓冲保护

`YierdisServerChannelInitializer.initChannel(...)` 在连接建立时按顺序做这些事：

1. 连接准入：`childChannelRegistry.admit(ch)`，超过 `--maxClients` 直接不装 pipeline。
2. 当 `--client-output-buffer-limit-bytes` 大于 `0` 时，把 Netty channel 的 `WriteBufferWaterMark` 设成 `low = max(1, high/2)`、`high = limit`；为 `0` 时不覆盖 channel 原有 watermark。
3. 创建 `NettyExecutionConnection`（含 `EngineSession` 和事务队列上限）并绑定 owner task executor；注册 `closeFuture` 回调做 `markClosing`。
4. 装配 inbound budget、`InboundReadCreditHandler`、`ConnectionReplySequencer`、reply gate 和 `RespRequestDecoder`。
5. 始终安装 `WriteBufferBackpressureHandler`。channel 不可写时调用 `executor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；恢复可写时由 owner executor 调用 `recoverInputIfPossible(...)`。
6. 如果 channel 持续不可写超过宽限期，经 `NettyExecutionConnection.initiateClose()` 统一关闭：先标记 closing、回收事务状态，再关闭 transport；`--client-output-buffer-limit-bytes` 为 `0` 时 handler 的 grace 为 `0`，不会调度这类慢客户端宽限关闭。
7. 当 `--client-idle-timeout-millis` 大于 `0` 时安装 `IdleStateHandler` 和 `CloseOnReadIdleHandler`，读空闲超时后同样经 `initiateClose()` 关闭连接。
8. 最后按序追加 `inboundReadCredit`、`inboundByteAccounting`、`respRequestDecoder`、`executionRequestIngress`。

这层保护处理的是慢读客户端和闲置连接，和 executor queue/backpressure 互补：前者看 Netty outbound buffer 和读空闲，后者看入站请求积压。即使关闭 Yierdis 自定义 output-buffer limit，Netty channel 仍然有自身的 writability 状态；如果 channel 按当前 watermark 变为不可写，transport backpressure 仍会暂停 `autoRead`。

## 可观测命令

`INFO` 返回 Redis 风格文本块，按 section 输出 `# Server`、`# Health`、`# Clients`、`# Memory`、`# Stats`、`# Keyspace`。不带 section（或 `default`/`all`）时全部输出；指定 `server`/`health`/`clients`/`memory`/`stats`/`keyspace` 之一只输出该段。文本适合人工排查，也便于与 Redis 经验对照。

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

**弱隔离或不可信客户端**：优先收紧 `--protocolMaxBulkBytes`、`--protocolMaxArgs`、`--protocolMaxLineBytes`、`--protocolMaxCommandBytes` 和 `--protocolGlobalInFlightBytes`，再设置 `--client-idle-timeout-millis`、`--client-output-buffer-limit-bytes` 和 `--client-output-buffer-over-limit-millis`。随后根据 `STATS` 中的 reject 和 backpressure 计数调整 executor queue/backpressure。

**高并发压测**：不要只增加 `--ioThreads`。Netty worker 只扩大 I/O 处理能力，DB mutation 仍经 executor owner thread。更关键的是固定 workload shape，用相同的 `REQUESTS`、`CLIENTS`、`PIPELINE`、`DATA_SIZE` 和 server 参数比较结果：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

benchmark 只连已运行的 server，脚本不会替你启动或停止 Yierdis；`scripts/bench.sh` 默认目标是 `127.0.0.1:16378`，而 server 默认监听 `6378`，所以压测要么给 server 传 `--port 16378`，要么给脚本设 `PORT=6378`。

**大 keyspace 或慢扫描**：优先用 `SCAN`，并用 `--keysTimeBudgetMillis`、`--keysMaxResults` 控制 `KEYS` 风险。TTL 或 native defrag 压力明显时，检查 `--cleanupIntervalMillis`、`--expireCleanupTimeLimitMillis` 和 native defrag budget；用 `MEMORY STATS` 观察 rehash/reserved，用 `INFO` memory section 观察 native defrag 摘要。

**maxmemory 调试**：先决定 scope。想模拟实例级 Redis 风格预算，用默认 `--maxmemoryScope global`；想验证每个 DB 独立预算，用 `--maxmemoryScope per-db`。例如：

```bash
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar \
  --port 6378 \
  --maxmemoryBytes 10485760 \
  --maxmemoryScope global \
  --maxmemoryPolicy allkeys-lru \
  --maxmemorySamples 5
```

## 启动失败和关闭

启动失败常见位置：

- 参数解析失败：picocli 抛 `ParameterException`，`ServerConfig.fromArgs(...)` 打 usage。
- 缺少 `--maxmemoryBytes`：同样是 `ParameterException`，提示必须显式指定。
- 参数校验失败：`normalizeAndValidate()` / `YierdisServerRuntimeConfig` 抛 `IllegalArgumentException`，例如端口越界、watermark 非法、output buffer grace 为 `0`、reply 容量顺序不满足。
- JDK 不满足要求：启动前使用 JDK 25 编译/运行环境；直接 FFM imports 会在不兼容环境中失败。
- 端口绑定失败：Netty `bind(...)` 报错。
- DB/native runtime 初始化失败：`YierdisInstance.create(...)` 会 best-effort 关闭已创建 DB 和 factory-owned resources 再抛出启动失败。

`YierdisServerBootstrap.start(config)` 使用 `ok` 标记，启动任一步失败都会调用 `close()` 做清理；`close()` 用 `closeAttempt` 保证幂等，状态从 `CLOSING` 推进到 `CLOSED`，失败则 `FAILED`。

关闭是 best-effort，`closeInternal()` 顺序大致是：server channel → child input（`beginShutdown` + `markClosing`）→ cleanup future → executor graceful shutdown → child reply drain（受 `--replyDrainTimeoutMillis` 限制，超时则 force-close）→ inbound/outbound budget → instance runtime access → command group → boss group → worker group。runtime access 的关闭会经 `executor.executeOwnerTask(runtimeAccess::close)` 回到 owner thread，避免在错误线程释放已绑定 DB runtime。某一步失败会记进聚合的 failure 并继续关闭后续资源，最后一起抛出。

脚本层关闭逻辑也要按真实进程处理：只有 `scripts/smoke.sh` 拥有它启动的临时 server，并用 `trap cleanup EXIT` 清理；connect-only benchmark 不拥有也不停止目标 Yierdis。

## Production Hardening Operations

reply global/per-connection/single limits、ingress admission、maxmemory、result-unknown 和 graceful shutdown 共同构成运行时容量边界。精确默认值、启动校验、INFO/STATS 字段、漏账排查和发布命令以 [`production-hardening-operations.md`](./production-hardening-operations.md) 为准；不要只用 `--client-output-buffer-limit-bytes` 或 JVM heap 来判断这些硬限制是否生效。