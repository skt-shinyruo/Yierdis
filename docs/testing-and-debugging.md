# Testing And Debugging

本文不是简单列一串测试文件，而是想回答一个更实用的问题：

- 改某块代码以后，到底应该跑哪些测试？
- 出问题时，应该先从哪一层往下排？

## 测试分层心智模型

这个仓库的测试大致可以按“离线上协议有多近”分成 6 层。

### 1. 参数与边界层

目标是保证 CLI、契约和架构护栏不漂移。

代表测试：

- `yierdis-args/.../YierdisServerArgsTest.java`
- `yierdis-server/.../ServerConfigArgsTest.java`
- `yierdis-core-runtime/.../ArchitectureBoundaryTest.java`
- `yierdis-core-runtime/.../ReplySsoTGuardTest.java`

这层很适合抓：

- 参数名变更
- README / 文档契约漂移
- 模块边界被偷偷破坏

### 2. 协议 codec / parser 层

目标是保证 request/reply 的编码、解析和 tagged value 语义稳定。

代表测试：

- `CustomProtocolV1RequestEncoderTest`
- `CustomProtocolV1RequestPayloadParserTest`
- `CustomProtocolV1ReplyParserTest`
- `JsonLineReplyWriterTest`
- `CustomRequestDecoderTest`

如果你改的是：

- request frame
- NDJSON reply
- tagged value
- decoder 恢复策略

这层是第一站。

### 3. 命令处理器层

目标是验证命令注册、参数校验、错误语义和事务队列行为。

代表测试：

- `CommandProcessorTest`
- `CommandErrorTest`
- `TransactionCommandTest`
- `YierdisFastCommandProcessorModuleTest`
- `YierdisFastCommandProcessorRegistrationTest`

这层最适合命令开发者，因为它不需要起 Netty server 就能把大部分行为跑清楚。

### 4. DB / 数据结构层

目标是验证值对象、TTL、maxmemory 和内存估算。

代表测试：

- `SetCommandTest`
- `HashCommandTest`
- `ListCommandTest`
- `ZSetCommandTest`
- `HllCommandTest`
- `ExpireSemanticsTest`
- `MaxmemoryEvictionTest`
- `OffHeapStringStorageTest`
- `HashValueTest`
- `ListValueTest`
- `ZSetValueTest`

如果你改的是：

- 内部编码升级
- TTL 语义
- maxmemory / eviction
- FFM/off-heap 行为

这一层最关键。

### 5. Server 集成层

目标是验证 pipeline、执行器、背压和真正的 socket 行为。

代表测试：

- `YierdisServerBootstrapCommandWiringTest`
- `CustomProtocolResyncIntegrationTest`
- `NettyCommandExecutorTest`
- `NettyCommandExecutorBackpressureTest`
- `NettyCommandExecutorFairSchedulingTest`
- `TransactionQueueCleanupTest`

如果你改的是：

- Netty pipeline
- `ServerConnectionContext`
- backpressure / queue reject
- protocol error 回包

一定要回到这一层。

### 6. Client / Bench / 工具层

目标是保证仓库附带的工具也跟着协议和参数一起保持一致。

代表测试：

- `YierdisClientTest`
- `TransactionQueueLimitTest`
- `BenchServerArgsReuseTest`
- `CustomCommandWriterTest`

这层经常被忽略，但如果你改了协议或参数，工具层其实很容易先坏。

## 常见改动应该跑什么

### 改协议

至少看这些：

- `CustomProtocolV1RequestEncoderTest`
- `CustomRequestDecoderTest`
- `JsonLineReplyWriterTest`
- `CustomProtocolV1ReplyParserTest`
- `CustomProtocolResyncIntegrationTest`

如果你想单跑某一个模块，最稳妥的方式是进入对应模块目录：

```bash
cd yierdis-protocol/yierdis-custom-v1-wire
mvn -Dtest=CustomProtocolV1RequestEncoderTest,CustomProtocolV1ReplyParserTest test
```

```bash
cd yierdis-protocol/yierdis-custom-v1-execution-adapter
mvn -Dtest=CustomProtocolV1ExecutionAdapterTest,JsonLineReplyWriterTest test
```

```bash
cd yierdis-protocol/yierdis-custom-v1-netty
mvn -Dtest=CustomRequestDecoderTest test
```

```bash
cd yierdis-server
mvn -Dtest=CustomProtocolResyncIntegrationTest test
```

### 改某个命令家族

建议顺序：

1. 先跑对应 `*CommandTest`
2. 再跑 `CommandErrorTest`
3. 如果命令跟事务或连接态有关，再跑 `TransactionCommandTest`

例如改 zset：

```bash
cd yierdis-core/yierdis-core-runtime
mvn -Dtest=ZSetCommandTest,CommandErrorTest test
```

### 改内部编码或内存逻辑

建议至少跑：

- 对应值类测试
- `MemoryStatsCommandTest`
- `MaxmemoryEvictionTest`
- 相关 off-heap / FFM 测试

例如改 string / off-heap：

```bash
cd yierdis-core/yierdis-core-runtime
mvn -Dtest=OffHeapStringStorageTest,MemoryStatsCommandTest,MaxmemoryEvictionTest test
```

### 改启动参数、背压、pipeline

建议至少跑：

- `ServerConfigArgsTest`
- `YierdisServerBootstrapCommandWiringTest`
- `NettyCommandExecutorBackpressureTest`
- `NettyCommandExecutorTest`

```bash
cd yierdis-server
mvn -Dtest=ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorTest test
```

### 不确定改动影响面时

最简单的保底方式仍然是：

```bash
mvn test
```

仓库要求 JDK 25。如果你本机默认不是 JDK 25，优先按 `README.md` 里的方式切换后再跑。

## 最实用的两个脚本

### `scripts/smoke.sh`

这个脚本适合做“最小真实路径回归”：

1. 构建 server / client / bench jar
2. 拉起一个真实 server
3. 用 CLI 发一个 `PING`
4. 再用 bench 工具做一个很小的 correctness smoke

默认日志会写到：

```text
.tmp-smoke-server.log
```

常见用法：

```bash
./scripts/smoke.sh
```

如果你刚刚已经打好了包，可以跳过构建：

```bash
SKIP_BUILD=1 ./scripts/smoke.sh
```

### `scripts/bench.sh`

这个脚本适合做“可重复压测”或 request-path 回归对比。

最简单的运行方式：

```bash
./scripts/bench.sh
```

常见调参项：

- `REQUESTS`
- `CLIENTS`
- `PIPELINE`
- `DATA_SIZE`
- `KEYSPACE`
- `SKIP_PREFILL`
- `SKIP_LATENCY`
- `MAXMEMORY_BYTES`

例如：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

## 排障时应该先看哪里

### 症状 1：收到 `kind=protocol` 错误

先看：

- `CustomRequestDecoder`
- `CustomProtocolV1RequestPayloadParser`
- `ProtocolErrorReplyHandler`
- `CustomProtocolResyncIntegrationTest`

重点确认：

- `len` 是否按 UTF-8 字节数计算
- payload 是否包含原始换行
- JSON schema 是否只包含 `cmd/args`

### 症状 2：收到 `ERR busy queue_full` 或 `ERR busy bytes_budget`

先看：

- `YierdisFastCommandHandler`
- `NettyCommandExecutor`
- `NettyCommandExecutorBackpressureTest`
- `STATS`

重点确认：

- 全局队列是不是已经满了
- queued-bytes 预算是不是耗尽了
- 连接是否已经被 executor 关闭 `autoRead`

### 症状 3：`EXECABORT` 或连接关闭后事务内存不释放

先看：

- `TransactionCommands`
- `ServerConnectionContext`
- `TransactionQueueCleanupTest`
- `TransactionCommandTest`

重点确认：

- 入队阶段是否触发命令数或 bytes 上限
- 连接关闭时是否正确丢弃了事务状态

### 症状 4：`WRONGTYPE`、`ERR syntax error`、`value is not an integer`

先看：

- 对应 `*Commands.java`
- `CommandErrorTest`
- 对应命令家族测试

这类问题大多数不是 executor 或协议问题，而是：

- arity 校验
- option 解析
- 类型断言

### 症状 5：编码和内存观测不符合预期

先看：

- `YierdisDbIntrospection`
- `YierdisObject`
- `HashValue` / `ListValue` / `SetValue` / `ZSetValue`
- `MemoryStatsCommandTest`
- 对应值类测试

然后配合实际命令观察：

- `OBJECT ENCODING key`
- `MEMORY USAGE key`
- `MEMORY STATS`

### 症状 6：server 启动阶段直接失败

先看：

- `YierdisServer`
- `ServerConfig`
- `ForeignMemoryAutoModules.ensureFfmAvailable()`
- `YierdisServerBootstrap`

最常见原因是：

- 参数非法
- 当前 JVM 不是 JDK 25
- 端口不可绑定

## 代码阅读顺序建议

如果你准备一边排障一边读代码，建议顺序是：

1. 先确认问题属于 protocol、command、db 还是 server runtime
2. 先看对应测试，明确“项目期望的行为是什么”
3. 再看实现类
4. 最后再跑 smoke 或 bench 验证整体路径

这样通常比一上来从实现类里瞎翻更快。

## 一句话总结

这个仓库最有效的调试方式不是“先打很多日志”，而是“先定位所在层，再跑那一层最有代表性的测试和脚本”。
