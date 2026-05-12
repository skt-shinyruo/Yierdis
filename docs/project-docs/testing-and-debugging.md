# Testing And Debugging

本文不是简单列一串测试文件，而是想回答一个更实用的问题：

- 改某块代码以后，到底应该跑哪些测试？
- 出问题时，应该先从哪一层往下排？

## 测试分层心智模型

这个仓库的测试大致可以按“离线上协议有多近”分成 6 层。

### 1. 参数与边界层

目标是保证 CLI、契约和架构护栏不漂移。

代表测试：

- `YierdisServerArgsTest`
- `ServerConfigArgsTest`
- `ArchitectureBoundaryTest`
- `ArchitecturePolicyResourceTest`
- `RespBoundaryGuardTest`

这层很适合抓：

- 参数名变更
- README / 文档契约漂移
- 模块边界被偷偷破坏
- retired protocol 依赖回到 production source

### 2. 协议 codec / parser 层

目标是保证 RESP request/reply 的编码、解析和协议上限稳定。

代表测试：

- `RespClientCodecTest`
- `RespRequestDecoderTest`
- `RespReplyWriterTest`
- `RespReplyWriterFactoryTest`
- `RespProtocolVersionTest`
- `RespProtocolLimitsTest`

如果你改的是：

- RESP request frame
- inline command 解析
- RESP2 / RESP3 reply
- decoder 错误和断连策略

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

#### 操作覆盖矩阵

`docs/project-docs/operation-test-coverage-matrix.md` 是命令、DB API、native 内部结构三层测试覆盖的索引。新增命令或新增 server-only 命令时，先补矩阵行，再补对应测试；否则 `OperationCoverageMatrixTest` 或 `ServerOperationCoverageMatrixTest` 会失败。

常用 guard：

```bash
mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am \
  -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest,StringBitmapOperationCoverageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

### 4. DB / 数据结构层

目标是验证 native entry/root、类型 payload adapter、TTL、maxmemory 和内存估算。

代表测试：

- `NativeKeyDirectoryTest`
- `EntryTableContractTest`
- `StringRootTest`
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
- `RespProtocolIntegrationTest`
- `RespHandshakeIntegrationTest`
- `RespProtocolErrorIntegrationTest`
- `CommandExecutorTest`
- `CommandExecutorBackpressureTest`
- `CommandExecutorFairSchedulingTest`
- `TransactionQueueCleanupTest`

如果你改的是：

- Netty pipeline
- `NettyExecutionConnection`
- backpressure / queue reject
- protocol error 回包

一定要回到这一层。

### 6. Client / Bench / 工具层

目标是保证仓库附带的工具也跟着协议和参数一起保持一致。

代表测试：

- `YierdisClientTest`
- `TransactionQueueLimitTest`
- `BenchServerArgsReuseTest`
- `RespCommandWriterTest`
- `SmokeScriptContractTest`
- `BenchScriptContractTest`

这层经常被忽略，但如果你改了协议或参数，工具层其实很容易先坏。

## 常见改动应该跑什么

### 改协议

至少看这些：

- `RespClientCodecTest`
- `RespRequestDecoderTest`
- `RespReplyWriterTest`
- `RespHandshakeIntegrationTest`
- `RespProtocolErrorIntegrationTest`

常用命令：

```bash
mvn -pl yierdis-networking/yierdis-networking-resp,yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespClientCodecTest,RespRequestDecoderTest,RespReplyWriterTest,RespHandshakeIntegrationTest,RespProtocolErrorIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

### 改某个命令家族

建议顺序：

1. 先跑对应 `*CommandTest`
2. 再跑 `CommandErrorTest`
3. 如果命令跟事务或连接态有关，再跑 `TransactionCommandTest`

例如改 zset：

```bash
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=ZSetCommandTest,CommandErrorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

### 改内部编码或内存逻辑

建议至少跑：

- 对应值类测试
- `MemoryStatsCommandTest`
- `MaxmemoryEvictionTest`
- 相关 off-heap / FFM 测试

例如改 string / off-heap：

```bash
mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=OffHeapStringStorageTest,MemoryStatsCommandTest,MaxmemoryEvictionTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

### 改启动参数、背压、pipeline

建议至少跑：

- `ServerConfigArgsTest`
- `YierdisServerBootstrapCommandWiringTest`
- `CommandExecutorBackpressureTest`
- `CommandExecutorTest`

```bash
mvn -pl yierdis-server/yierdis-server-main,yierdis-server/yierdis-server-executor -am \
  -Dtest=ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,CommandExecutorBackpressureTest,CommandExecutorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

### 不确定改动影响面时

最简单的保底方式仍然是：

```bash
mvn test
```

仓库要求 JDK 25。

## 外部客户端兼容 smoke

Yierdis 的公开协议是 RESP，所以可以用常见 Redis 客户端做基础 smoke：

```bash
redis-cli -p 6378 PING
redis-cli -p 6378 SET smoke:key smoke:value
redis-cli -p 6378 GET smoke:key
redis-cli -p 6378 HELLO 3
```

也可以用 Jedis、Lettuce、go-redis 做最小连接检查，重点放在基础 RESP2 命令和 `HELLO 3` 协商上。这个项目不是 Redis drop-in replacement，因此 smoke 应聚焦已实现命令，不应把完整 Redis 客户端测试套件当作必须通过的目标。

## 最实用的两个脚本

### `scripts/smoke.sh`

这个脚本适合做“最小真实路径回归”：

1. 构建 server / client / bench jar
2. 拉起一个真实 server
3. 优先用 `redis-cli` 发 `PING` / `SET` / `GET`
4. 如果本机没有 `redis-cli`，回退到 Java CLI
5. 再用 bench 工具做一个很小的 correctness smoke

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

常见调参项：

- `HOST` / `PORT`
- `SERVER_LOG`
- `READY_TIMEOUT_SEC`
- `REQUESTS` / `CLIENTS` / `PIPELINE`

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
- `LATENCY_REQUESTS`
- `LATENCY_CLIENTS`
- `XMS` / `XMX` / `MAX_DIRECT_MEMORY`
- `SKIP_PREFILL`
- `SKIP_LATENCY`
- `MAXMEMORY_BYTES`
- `MAXMEMORY_POLICY`
- `MAXMEMORY_SAMPLES`
- `SERVER_ARGS_EXTRA`
- `BENCH_ARGS_EXTRA`

`SERVER_ARGS_EXTRA` 先经过 `YierdisBenchServerArgs` 解析，只能使用 bench launch 模型已声明的 server 参数；client idle/output-buffer 慢客户端保护这类 server-only 参数需要直接启动 server 验证。

例如：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

## 排障时应该先看哪里

### 症状 1：收到 `-ERR Protocol error`

先看：

- `RespRequestDecoder`
- `RespProtocolErrorReplyHandler`
- `RespProtocolErrorIntegrationTest`
- `RespRequestDecoderTest`

重点确认：

- RESP array / bulk length 是否正确
- inline command 是否超过上限
- bulk string 是否完整带 `\r\n`
- 错误后连接是否按预期关闭

### 症状 2：收到 `ERR busy queue_full` 或 `ERR busy bytes_budget`

先看：

- `YierdisFastCommandHandler`
- `CommandExecutor`
- `CommandExecutorBackpressureTest`
- `STATS`

重点确认：

- 全局队列是不是已经满了
- queued-bytes 预算是不是耗尽了
- 连接是否已经被 executor 关闭 `autoRead`

### 症状 3：`EXECABORT` 或连接关闭后事务内存不释放

先看：

- `TransactionCommands`
- `NettyExecutionConnection`
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
- `EntryRecord`
- `StringRoot` / `HashRoot` / `ListRoot` / `SetRoot` / `ZSetRoot`
- `HashValue` / `ListValue` / `SetValue` / `ZSetValue`
- `NativeKeyDirectoryTest` / `EntryTableContractTest` / `StringRootTest`
- `MemoryStatsCommandTest`
- 对应值类测试

然后配合实际命令观察：

- `OBJECT ENCODING key`
- `MEMORY USAGE key`
- `MEMORY STATS`
