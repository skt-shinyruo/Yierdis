# Configuration And Operations

本文面向两类读者：

- 想知道“server 启动参数最后落到哪里”的初学者
- 想知道“遇到背压、淘汰、清理和观测问题时该看什么”的维护者

如果你只想先跑起来，`README.md` 里的启动示例已经够用；这篇文档更关心“这些参数在代码里怎么工作”。

## 配置是怎么流动的

启动参数不是直接散落在 Netty 或 DB 里，而是经过一条很清晰的转换链：

```text
CLI args
  -> YierdisServerArgs
  -> normalizeAndValidate()
  -> YierdisServerRuntimeConfig
  -> ServerConfig
  -> YierdisServerBootstrap
```

这条链的分工是：

- `YierdisServerArgs`
  负责 picocli 参数定义、默认值、归一化和校验，源码位于 `yierdis-server-main`
- `YierdisServerRuntimeConfig`
  负责把字符串参数变成稳定的 record 和 enum，仍属于 server runtime config
- `ServerConfig`
  负责把参数层错误转换成 CLI 级错误
- `YierdisServerBootstrap`
  负责真正把 runtime config 装配进 server

bench 不再复用 server runtime config；它在 `yierdis-benchmark` 内维护自己的 server launch argv 模型，只负责生成子进程启动参数。

## 参数分组

### 1. 网络和实例规模

- `--port`
- `--databases`
- `--ioThreads`

这组参数决定：

- server 监听哪个端口
- 暴露多少个逻辑 DB
- Netty worker 线程数是多少

要注意的一点是：`ioThreads` 不是 DB 执行线程数。DB 读写仍然由 command executor owner thread 串行访问。

### 2. 过期清理和慢命令预算

- `--cleanupIntervalMillis`
- `--noCleanup`
- `--expireCleanupTimeLimitMillis`
- `--keysTimeBudgetMillis`
- `--keysMaxResults`

这组参数控制两件事：

- 后台 maintenance tick 多久触发一次
- 单次清理和慢扫描最多可以花多少预算

`KEYS` 之所以有单独预算，是因为它天然可能成为慢命令。项目用 `SlowCommandGovernor` 把这个预算显式收口，而不是让 `KEYS` 在大 keyspace 上无界跑。

### 3. 执行队列和背压

- `--executorQueueCapacity`
- `--executorQueueMaxBytes`
- `--executorSchedulingPolicy`
- `--backpressureHigh`
- `--backpressureLow`
- `--backpressureBytesHigh`
- `--backpressureBytesLow`
- `--executorMaxDrain`
- `--executorDrainMillis`

这组参数是 server 运行时最值得关注的部分之一。

它们共同控制：

- command executor 全局队列能积压多少任务
- 单连接 pending 到什么程度时禁用 `autoRead`
- drain loop 每一轮最多处理多少命令、最多花多少时间
- 多连接竞争时按 `global` 还是 `fair` 调度

如果你看到客户端收到：

```text
ERR busy queue_full
ERR busy bytes_budget
ERR busy not_running
```

基本就要回到这一组参数和 `STATS` 计数器来看。

### 4. 事务队列保护

- `--transactionQueueMaxCommands`
- `--transactionQueueMaxBytes`

这组参数只作用于 `MULTI` 状态下的连接级事务队列。

目的很明确：

- 避免一次事务塞入过多命令
- 避免少量大参数事务长期驻留内存

当事务入队阶段触发上限时：

- 当前事务会被标记为 aborted
- 后续 `EXEC` 返回 `EXECABORT Transaction discarded because of previous errors.`

### 5. 协议安全上限

- `--protocolMaxBulkBytes`
- `--protocolMaxArgs`
- `--protocolMaxLineBytes`

这组参数会被 `YierdisServerChannelInitializer` 直接传给 `CustomRequestDecoder`。

它们主要负责：

- 限制单条请求 payload 大小
- 限制 argv 元素数量
- 限制 length header 长度

如果你把 Yierdis 暴露到公网或弱隔离环境，首先应该收紧这一组参数，而不是先去调整更深层的内存细节。

### 6. maxmemory 和淘汰

- `--maxmemoryBytes`
- `--maxmemoryScope`
- `--maxmemoryPolicy`
- `--maxmemorySamples`
- `--evictionTimeLimitMillis`

这组参数控制的是“预算”和“超过预算时怎么处理”。

当前 scope 有两种：

- `global`
- `per-db`

policy 有三种：

- `noeviction`
- `allkeys-random`
- `allkeys-lru`

如果预算为 `0`，表示不限制。

项目当前没有独立的 `--offheapBackend` 或 `--offheapMaxBytes`。native memory 路径默认统一走 JDK 25 FFM，真正的预算入口就是 `maxmemory`。

## 启动时真正发生了什么

`YierdisServerBootstrap.startInternal()` 大致按下面顺序工作：

1. 检查 FFM 是否可用
2. 用 runtime config 组出 `YierdisInstanceConfig`
3. 创建 `YierdisInstance`
4. 创建 `NettyServerInfoProvider`
5. 创建 `DefaultYierdisEngine`
6. 创建 `CommandExecutor`
7. 启动 executor，并在 owner thread 绑定 runtime
8. 如果开启清理任务，则调度 maintenance tick
9. 装配 Netty pipeline
10. `bind(port)`

这里最容易误解的一点是：

- `ioThreads` 负责收包
- command executor 负责真正访问 DB
- 清理任务也不是单独直接碰 DB，而是回到 executor owner thread 执行

所以配置调优时，不能把它想成一个“完全多线程并发改 DB”的服务。

## Maintenance Tick 是怎么跑的

过期清理并不是直接在 worker 线程里跑 DB 逻辑。

真实流程是：

1. worker event loop 只负责定时
2. 定时器触发后调用 `executor.executeMaintenance(...)`
3. 真正的 cleanup 在 DB owner thread 中执行

并且 bootstrap 还做了一个 `cleanupPending` 的 coalesce 开关，避免高压下积累一串追赶式 cleanup 任务。

这意味着：

- 你不会因为 cleanup 额外引入第二个 DB 写线程
- 也不会因为 fixed-rate 定时器追赶而把 executor 压爆

## 可观测性：先看哪些命令

### `INFO`

`INFO` 默认返回 Redis 风格的文本块，包含：

- server
- clients
- memory
- stats
- keyspace

它更适合：

- 人工浏览
- 和 Redis 经验做类比

### `INFO yierdis`

`INFO yierdis` 返回的是结构化 map，字段更贴近本项目自己的 runtime config 和 executor 状态，例如：

- `executor_policy`
- `executor_queue_capacity`
- `backpressure_high`
- `executor_max_drain`
- `uptime_millis`

如果你在做自动化脚本、集成测试或排障，优先看这个版本。

### `STATS`

`STATS` 专注于执行器和连接统计，比较关键的字段包括：

- `queued_tasks`
- `queued_bytes`
- `submit_accepted_total`
- `submit_rejected_queue_full_total`
- `submit_rejected_bytes_budget_total`
- `backpressure_enter_total`
- `backpressure_exit_total`
- 当前连接的 `conn_pending`、`conn_commands_rejected` 等

遇到 `ERR busy ...` 时，第一反应应该是：

1. 复现问题
2. 立刻跑一次 `STATS`
3. 看 reject counters 和 pending 指标

### `MEMORY STATS`

这是结构化 map，适合看：

- `used_bytes_for_maxmemory`
- `effective_used_bytes_for_maxmemory`
- `ledger_used_bytes`
- `ledger_reserved_bytes`
- `offheap_used_bytes`
- `key_count`
- `expire_count`

如果 `maxmemoryScope=global`，server 的 `NettyServerInfoProvider` 会优先返回聚合视角；否则更接近单 DB 视角。

### `MEMORY USAGE key`

适合回答：

- 某个 key 大致占了多少字节

### `OBJECT ENCODING key`

适合回答：

- 这个 key 当前是 `int` / `embstr` / `raw`，还是 `listpack` / `skiplist`

这对理解数据结构升级路径特别有帮助。

## 新手最常见的运行场景

### 本地开发

目标通常是“先跑通，再看行为”。

这时建议：

- 先用默认参数启动
- 用 CLI 跑 `PING`、`SET/GET`、`INFO yierdis`、`STATS`
- 再用 `OBJECT ENCODING` 和 `MEMORY STATS` 观察内部状态

### 公网或弱隔离环境

目标通常是“先收紧输入，再考虑吞吐”。

优先级建议：

1. 先收紧 `--protocolMaxBulkBytes`
2. 再收紧 `--protocolMaxArgs` 和 `--protocolMaxLineBytes`
3. 根据负载调小 `--executorQueueCapacity` 和 `--executorQueueMaxBytes`
4. 通过 `STATS` 验证是否开始出现过量 reject

### 大 keyspace 或慢扫描场景

重点看：

- `--keysTimeBudgetMillis`
- `--keysMaxResults`
- `--cleanupIntervalMillis`
- `--expireCleanupTimeLimitMillis`

这些参数决定了：

- `KEYS` 会不会拖太久
- 后台过期清理会不会吃掉太多 owner thread 时间

## 启动和关闭时要知道的事

### 启动失败通常发生在哪

最常见的是三类：

- CLI 参数非法，`ServerConfig.fromArgs(...)` 打 usage 并失败
- JVM 不支持 JDK 25 FFM，`ForeignMemoryAutoModules.ensureFfmAvailable()` 失败
- 端口绑定失败，Netty `bind(...)` 报错

### 关闭时发生什么

`YierdisServerBootstrap.close()` 会按反向顺序 best-effort 关闭：

- server channel
- cleanup future
- executor
- instance runtime access
- command group
- boss / worker group

这也是为什么 bootstrap 很适合被集成测试复用，它把生命周期收在了一个对象里。

## 初学者最值得看的测试

想理解配置和运行时，不建议直接盯着参数定义看。推荐对照下面这些测试：

1. `ServerConfigArgsTest`
   看参数解析、归一化和失败路径
2. `YierdisServerBootstrapCommandWiringTest`
   看 runtime config 如何真正装进 pipeline 和 observability
3. `CommandExecutorBackpressureTest`
   看队列满、背压进入和恢复
4. `CommandExecutorTest`
   看执行器主流程
5. `CommandExecutorFairSchedulingTest`
   看 `fair` 调度语义
6. `MemoryStatsCommandTest`
   看内存观测字段是否稳定

## 一句话总结

把这层记成：

“配置先被规范化成 runtime config，再被 bootstrap 分发到 protocol、executor、maintenance 和 DB；运维时主要盯 `INFO yierdis`、`STATS`、`MEMORY STATS` 三组观测面。”
