# 配置与运行

本文解释启动参数如何进入运行时配置，以及本地运行、观测、调参和关闭时应该看哪些入口。

## 配置流向

启动参数的主路径是：

```text
argv
  -> YierdisServerArgs
  -> normalizeAndValidate()
  -> YierdisServerRuntimeConfig
  -> ServerConfig
  -> YierdisServerBootstrap
```

`YierdisServerArgs` 用 picocli 声明 server 参数、默认值和 usage。`normalizeAndValidate()` 会先处理派生语义，例如 `--noCleanup` 会把 `cleanupIntervalMillis` 归零，再校验端口、DB 数量、executor 队列、协议上限、事务队列、maxmemory、native defrag 和慢客户端参数。字符串枚举也在这里归一化：`executorSchedulingPolicy` 只接受 `global|fair`，`maxmemoryScope` 接受 `global|per-db`，并兼容 `perdb` / `per_db` 写法。

`toRuntimeConfig()` 把已归一化参数转成 `YierdisServerRuntimeConfig` record。这个 record 是 server-main 内部后续组装的稳定配置对象，字段已经是 enum、number 和 boolean，不再携带原始 CLI 字符串。

`ServerConfig.fromArgs(...)` 是 CLI 到组合根的边界：解析失败或校验失败时打印 usage，并抛 `YierdisCliException.usageError(...)`；`--help` 打印 usage 并返回 `null`。`YierdisServerBootstrap.start(String... args)` 收到 `null` 会视为没有可启动配置。

真正启动发生在 `YierdisServerBootstrap.startInternal()`：

1. `ForeignMemoryAutoModules.ensureFfmAvailable()` 检查 JDK 25 FFM。
2. 将 `YierdisServerRuntimeConfig` 映射成 `YierdisInstanceConfig`。
3. 创建 `YierdisInstance`，并取得 runtime access、maintenance 和 observability。
4. 创建 `NettyServerInfoProvider`，绑定 instance observability。
5. 创建 `DefaultYierdisEngine` 和命令模块。
6. 创建单线程 `DefaultEventExecutorGroup(1)` 与 `CommandExecutor`。
7. `executor.start()` 在 owner thread 上绑定 runtime。
8. 按需在 Netty worker event loop 上调度 maintenance tick，但 DB 逻辑仍通过 `executeMaintenance(...)` 回到 owner thread。
9. 创建 boss/worker Netty group，并由 `YierdisServerChannelInitializer` 装配连接 pipeline。
10. `bind(port)`。

`YierdisInstance` 在这个过程中不是“随手可用的 DB 容器”。`YierdisInstance.create(config)` 要求 `engineFactory` 已经注入；`bindToCurrentThread()` 要先把当前线程标成 owner thread，后续 DB access 才会被允许；`close()` 负责按拥有关系关闭 runtime、allocator 和 DB resources。bootstrap 失败时会 best-effort 清掉已经创建的对象，避免留下半初始化实例。

注意：benchmark 有自己的 `YierdisBenchServerArgs`，只负责生成子进程 server argv。它不是完整 server 参数模型，尤其没有 server-only 的 client idle/output-buffer 慢客户端参数。

源码入口：

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

## 网络和实例规模

`--port` 决定 Netty bind 端口，默认 `6378`；`--databases` 决定 `SELECT 0..N-1` 的逻辑 DB 数，默认 `16`，校验范围是 `1..1024`；`--ioThreads` 决定 Netty worker 线程数，默认 `1`。

这里最容易误解的是 `ioThreads`。它们是 Netty worker，负责 socket I/O、pipeline decode/encode 事件和定时器触发，不是 DB mutation 并行度。DB 读写和 maintenance 里的 DB 访问都通过 `CommandExecutor` 的 owner thread 进入；`YierdisInstance` 也要求 DB 访问先绑定到 owner thread，跨线程访问会 fail-fast。

本地运行命令应和根 `README.md` 保持一致：

```bash
mvn -q -DskipTests package
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 6378
```

然后可以用 `redis-cli` 或项目 CLI：

```bash
redis-cli -p 6378 PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS
```

## protocol limits

`--protocolMaxBulkBytes`、`--protocolMaxArgs`、`--protocolMaxLineBytes` 和 `--protocolMaxCommandBytes` 会直接传给 `RespRequestDecoder`。它们分别约束 bulk body、参数个数、header/inline 行长度，以及单条命令累计字节数。暴露在不可信网络里时，优先收紧这四个入口上限，再考虑更深层的内存调参。

解析失败会走 RESP protocol error 路径：`RespRequestDecoder` 负责 RESP 解析、入口限制和 ingress admission，出错时产出 `RespProtocolError`；`RespProtocolErrorReplyHandler` 统一回协议错误并关闭连接，避免请求和回包错位。这个路径不会进入 `YierdisFastCommandHandler` 的命令提交主链。

## executor 和 backpressure

executor 参数分两类：全局队列预算和单连接背压。

- `--executorQueueCapacity`：executor 可积压任务数，默认 `1024`。
- `--executorQueueMaxBytes`：executor 已接收请求的 retained bytes 预算，默认 `67108864`，`0` 表示禁用 bytes budget。
- `--executorSchedulingPolicy`：`fair` 或 `global`，默认 `fair`。
- `--executorMaxDrain`：每轮 drain 最多执行多少命令，默认 `512`。
- `--executorDrainMillis`：每轮 drain 时间预算，默认 `2` ms。
- `--backpressureHigh` / `--backpressureLow`：单连接 pending 命令高低水位，默认 `256/128`。
- `--backpressureBytesHigh` / `--backpressureBytesLow`：单连接 pending bytes 高低水位，默认 `16777216/8388608`；high 为 `0` 时 bytes 背压禁用，low 也必须为 `0`。

`CommandExecutorConfigs.from(...)` 把 runtime config 映射成 `CommandExecutorConfig`。`CommandExecutor` 只有一个 owner executor，启动时调用 `runtimeAccess.bindToCurrentThread`，之后通过 `trySubmit(...)` 接收 Netty pipeline 交来的请求。队列满或 bytes budget 超限时，客户端会收到类似：

```text
ERR busy queue_full
ERR busy bytes_budget
ERR busy not_running
```

连接已经进入 closing 时，提交层会在预留 queue slot / bytes budget 前拒绝，计入 `submit_rejected_closing_total`；该路径不会再额外写 `ERR busy`。`STATS` 是排查入口。重点看 `queued_tasks`、`queued_bytes`、`submit_rejected_queue_full_total`、`submit_rejected_bytes_budget_total`、`submit_rejected_closing_total`、`backpressure_enter_total`、`backpressure_exit_total`，以及当前连接的 `conn_pending`、`conn_pending_bytes`、`conn_autoread_disabled_by_executor` 和 `conn_commands_rejected`。

## transaction 保护

事务队列是连接级状态，创建连接时 `NettyExecutionConnection.getOrCreate(...)` 会把：

- `--transactionQueueMaxCommands`
- `--transactionQueueMaxBytes`

传给 `EngineSession` 的 `DefaultTransactionState`。默认值分别是 `1024` 和 `67108864`；`0` 表示对应限制禁用。

在 `MULTI` 状态下，命令入队会复制 `ExecutionRequest` 快照并累计 retained bytes。超过命令数或 bytes 上限时，事务被标记为 aborted，入队返回 `ERR Transaction queue is full`；后续 `EXEC` 会返回 Redis 风格 `EXECABORT Transaction discarded because of previous errors.` 并丢弃队列。这是为了防止大事务或大参数在连接状态里无界驻留。

推荐看 `TransactionQueueLimitTest` 和 `EngineSession`。

## TTL 和 maintenance

TTL 语义是“访问时惰性删除 + 轻量后台清理”。相关参数：

- `--cleanupIntervalMillis`：后台 maintenance 间隔，默认 `1000` ms，`0` 禁用。
- `--noCleanup`：归一化为 `cleanupIntervalMillis=0`。
- `--expireCleanupTimeLimitMillis`：单次 expire cleanup 时间预算，默认 `5` ms。
- `--keysTimeBudgetMillis`：`KEYS` 扫描时间预算，默认 `20` ms；`0` 禁用预算。
- `--keysMaxResults`：`KEYS` 最大返回条数，默认 `Integer.MAX_VALUE`；`0` 禁用 `KEYS`。
- `--nativeDefragEnabled` 及 `--nativeDefragMaxMoveBytes`、`--nativeDefragMaxObjects`、`--nativeDefragTimeLimitMillis`：maintenance tick 里可选 native allocator defrag 预算。

bootstrap 使用 Netty worker event loop 做定时器，但定时器只提交 `executor.executeMaintenance(...)`。真正的 DB cleanup、global maxmemory maintenance 和 native defrag 都在 DB owner thread 上执行。`cleanupPending` 会 coalesce 尚未完成的 cleanup，避免高压下堆积追赶式 maintenance 任务。

`nativeDefragEnabled` 只是给 `YierdisDb.defragMaintenance()` 提供预算闸门；更细的移动、pin、quarantine 和 object table 语义看 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

`KEYS` 的预算来自 `SlowCommandGovernor`，由 `DefaultCommandModules.create(...)` 注入命令模块。大 keyspace 运行时优先使用 `SCAN`，把 `KEYS` 当成受限诊断工具。

当前 native-memory 路径统一使用 JDK 25 FFM。更细的 runtime、region、arena 和 copy 边界见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

TTL 命令写路径、lazy expire、cleanup sample/budget 和 synthetic `EXPIRED` delete 见 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。这里的配置章节只保留参数和 runtime 调度顺序。

## maxmemory 和 eviction

maxmemory 参数：

- `--maxmemoryBytes`：总预算，默认 `0` 表示不限制。
- `--maxmemoryScope`：`global|per-db`，server 默认 `global`。
- `--maxmemoryPolicy`：`noeviction|allkeys-random|allkeys-lru`，默认 `noeviction`。
- `--maxmemorySamples`：采样数量，默认 `5`。
- `--evictionTimeLimitMillis`：单次 eviction 时间预算，默认 `5` ms。

`YierdisServerBootstrap` 把 server runtime scope 映射成 `YierdisInstanceConfig.MaxmemoryScope`，并在生产启动路径里选择默认 DB factory。`YierdisInstance.create(config)` 是 strict 入口，要求 `YierdisInstanceConfig` 已经注入 `engineFactory` 或 `EngineFactoryBinding`；embedded/test 调用方也必须显式提供相同的 factory 依赖，runtime 不再隐式选择默认 DB backend。

- `global`：server-main 默认 factory 下所有 DB 共享一个 instance-level `YierdisFfmMemoryRuntime("instance")`。每个 DB 仍有自己的 keyspace、entry table、roots、ledger 和 allocator 视图，但 maxmemory 由 `YierdisGlobalMaxmemoryGovernor` 跨 DB 协调。governor 汇总每个 participant 报告的 owned `MemoryUsageSnapshot`，不另加一个 runtime 级 usage source。
- `per-db`：兼容模式。`YierdisInstance` 把 `maxmemoryBytes` 按 DB 数硬分摊，整数除法后的余数按 DB 创建顺序每个 DB 多给 1 byte。server-main 默认 factory 下每个 DB 创建自己的 DB-owned `YierdisFfmMemoryRuntime("db")`，evict/reserve/memory stats 都按单 DB 预算运行。

`global` 是共享实例级 runtime/governor；`per-db` 是拆分预算和 runtime ownership。不要把 `ioThreads`、Netty 连接数或 DB 数误解成 maxmemory 的并发写入模型，mutation 仍经 owner thread。

`MEMORY STATS` 是 explainable estimate，不是 JVM instrumentation object graph；native memory 是否纳入 maxmemory 要看字段口径。global scope 下 `NettyServerInfoProvider.memoryStats(...)` 会优先返回 instance 聚合视角。

更细的 reservation 顺序、`usedBytes` / `reservedBytes` 口径、victim 选择、global governor 协调和 synthetic `EVICTED` delete 见 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 慢客户端和输出缓冲保护

连接保护参数：

- `--client-idle-timeout-millis`：读空闲关闭时间，默认 `300000` ms；`0` 禁用。
- `--client-output-buffer-limit-bytes`：Yierdis 自定义慢客户端输出缓冲上限，默认 `67108864`；`0` 不设置自定义 `WriteBufferWaterMark`，并禁用 Yierdis 的宽限关闭。
- `--client-output-buffer-over-limit-millis`：输出缓冲持续超过上限后的宽限期，默认 `10000` ms；启用 output buffer limit 时必须大于 `0`。

`YierdisServerChannelInitializer` 在连接初始化时：

1. 当 output buffer limit 大于 `0` 时设置 Yierdis 自定义的 Netty `WriteBufferWaterMark`，low 为 high 的一半；为 `0` 时不覆盖 channel 原有 watermark。
2. 始终安装 `WriteBufferBackpressureHandler`。channel 不可写时调用 `executor.onTransportUnwritable(...)`，executor 关闭该连接 `autoRead`；恢复可写时通过 owner executor 调用 `recoverInputIfPossible(...)`。
3. 如果 channel 持续不可写超过 grace，则关闭连接；output buffer limit 为 `0` 时 handler 的 grace 为 `0`，不会调度这类慢客户端宽限关闭。
4. 当 idle timeout 大于 `0` 时安装 `IdleStateHandler` 和 `CloseOnReadIdleHandler`，读空闲超时后关闭连接。

这层保护处理的是慢读客户端和闲置连接，和 executor queue/backpressure 互补：前者看 Netty outbound buffer 和读空闲，后者看入站请求积压。即使关闭 Yierdis 自定义 output-buffer limit，Netty channel 仍然有自身的 writability 状态；如果 channel 按当前 watermark 变为不可写，transport backpressure 仍会暂停 `autoRead`。

## 可观测命令

`INFO` 返回 Redis 风格文本块，支持 `server`、`clients`、`memory`、`stats`、`keyspace` 等 section。它适合人工排查和与 Redis 经验对照。memory section 会包含 `maxmemory`、`maxmemory_policy`、`yierdis_maxmemory_scope`、ledger、offheap 和 native defrag 摘要；stats section 会包含 executor queue 摘要。

`INFO yierdis` 返回结构化 map，更适合脚本和测试。它暴露 `server`、`version`、`port`、`io_threads`、`executor_policy`、`executor_queue_capacity`、`executor_queue_max_bytes`、`backpressure_high`、`backpressure_low`、`backpressure_bytes_high`、`backpressure_bytes_low`、`executor_max_drain`、`executor_drain_millis`、`started_millis`、`uptime_millis`。

`STATS` 返回结构化 map，专注 executor 和当前连接统计。遇到 `ERR busy ...`、输入被暂停、吞吐抖动时先看它。

`MEMORY STATS` 返回内存估算 map。常用字段包括 `maxmemory_bytes`、`used_bytes_for_maxmemory`、`effective_used_bytes_for_maxmemory`、`ledger_used_bytes`、`ledger_reserved_bytes`、`offheap_used_bytes`、`offheap_included_in_maxmemory`、`key_count`、`expire_count`、`keyspace_rehashing`、`expire_rehashing` 和 table capacity。global scope 下优先读聚合视角；per-db scope 下更贴近当前 DB。native defrag 摘要当前在 `INFO` memory section 中输出。

`MEMORY USAGE key` 返回某个 key 的估算字节数，用于定位大 key。`OBJECT ENCODING key` 返回内部编码名，例如 string 的 `int` / `embstr` / `raw`，collection 的 `listpack` / `hashtable` / `intset` / `quicklist` / `skiplist` 等，用于理解数据结构升级和存储形态。

常用检查命令：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar MEMORY STATS
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar MEMORY USAGE mykey
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar OBJECT ENCODING mykey
```

## 常见运行场景

本地开发先按 `README.md` 跑默认 server，再用 CLI 或 `redis-cli` 执行 `PING`、`SET`、`GET`、`INFO yierdis`、`STATS`。需要看数据结构时加 `OBJECT ENCODING`；需要看预算口径时加 `MEMORY STATS` 和 `MEMORY USAGE`。

弱隔离或不可信客户端场景，优先收紧 `--protocolMaxBulkBytes`、`--protocolMaxArgs`、`--protocolMaxLineBytes`，再设置 `--client-idle-timeout-millis`、`--client-output-buffer-limit-bytes` 和 `--client-output-buffer-over-limit-millis`。随后根据 `STATS` 中的 reject 和 backpressure 计数调整 executor queue/backpressure。

高并发压测场景，不要只增加 `--ioThreads`。Netty worker 只扩大 I/O 处理能力，DB mutation 仍经 executor owner thread。更关键的是固定 workload shape，用相同的 `REQUESTS`、`CLIENTS`、`PIPELINE`、`DATA_SIZE` 和 server 参数比较结果：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

大 keyspace 或慢扫描场景，优先用 `SCAN`，并用 `--keysTimeBudgetMillis`、`--keysMaxResults` 控制 `KEYS` 风险。TTL 或 native defrag 压力明显时，检查 `--cleanupIntervalMillis`、`--expireCleanupTimeLimitMillis` 和 native defrag budget；用 `MEMORY STATS` 观察 rehash/reserved，用 `INFO` memory section 观察 native defrag 摘要。

maxmemory 调试场景，先决定 scope。想模拟实例级 Redis 风格预算，用默认 `--maxmemoryScope global`；想验证每个 DB 独立预算，用 `--maxmemoryScope per-db`。例如：

```bash
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --port 6378 \
  --maxmemoryBytes 10485760 \
  --maxmemoryScope global \
  --maxmemoryPolicy allkeys-lru \
  --maxmemorySamples 5
```

## 启动失败和关闭

启动失败常见位置：

- 参数解析失败：picocli 抛 `ParameterException`，`ServerConfig.fromArgs(...)` 打 usage。
- 参数校验失败：`normalizeAndValidate()` 抛 `IllegalArgumentException`，例如端口越界、watermark 非法、output buffer grace 为 `0`。
- JDK 不满足要求：`ForeignMemoryAutoModules.ensureFfmAvailable()` 检查不到 JDK 25 FFM。
- 端口绑定失败：Netty `bind(...)` 报错。
- DB/native runtime 初始化失败：`YierdisInstance.create(...)` 会 best-effort 关闭已创建 DB 和 factory-owned resources 再抛出启动失败。

`YierdisServerBootstrap.start(config)` 使用 `ok` 标记，启动任一步失败都会调用 `close()` 做清理。

关闭是 best-effort，顺序大致是：server channel、cleanup future、executor graceful shutdown、engine、instance runtime access、command group、boss group、worker group。runtime access 的关闭会通过 `executor.executeOwnerTask(runtimeAccess::close)` 回到 owner thread，避免在错误线程释放已绑定 DB runtime。

脚本层关闭逻辑也要按真实进程处理：`scripts/smoke.sh` 用 trap 杀掉临时 server；benchmark 的 `ServerProcess.stop()` 先 `destroy()`，超时后 `destroyForcibly()`。

## Production Hardening Operations

reply global/per-connection/single limits、ingress admission、commit-stream、maxmemory、result-unknown 和 graceful shutdown 共同构成运行时容量边界。精确默认值、启动校验、INFO/STATS 字段、漏账排查和发布命令以 [`production-hardening-operations.md`](./production-hardening-operations.md) 为准；不要只用 `clientOutputBufferLimitBytes` 或 JVM heap 来判断这些硬限制是否生效。
