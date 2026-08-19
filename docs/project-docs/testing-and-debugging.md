# 测试与排障

本文说明 Yierdis 测试如何分层，以及不同改动和故障应该先跑哪些测试、先看哪一层。

所有 Maven/Java 命令都使用 JDK 25。非交互 shell 中使用这个前缀：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## 测试分层

Yierdis 的测试大致分成七层：

| 层 | 主要目的 | 常见测试 |
| --- | --- | --- |
| API contract | 稳定接口和边界语义 | `ExecutionRequestContractTest`, `CommandContractTest`, `YierdisInstanceConfigTest` |
| command / integration | 命令注册、parse 隔离、参数、回包和 Redis 兼容语义 | `CommandDispatcherTest`, `CommandParseIsolationTest`, `StringCommandTest`, `CommandErrorTest` |
| DB direct ops | 绕开 command 层验证 DB API | `StringDirectOpsTest`, `CollectionDirectOpsTest`, `TtlLifecycleDirectOpsTest` |
| native/internal | handle、backend、keyspace、root/value、ledger | `NativeHandleTest`, `YierdisFfmStableMemoryBackendTest`, `StringRootTest`, `MemoryLedgerContractTest` |
| executor / server | owner thread、队列、背压、Netty 适配 | `CommandExecutorTest`, `CommandExecutorBackpressureTest`, `RespProtocolIntegrationTest` |
| CLI / bench | 客户端、catalog、NIO runner、storage footprint、脚本和输出契约 | `YierdisClientTest`, `RedisBenchmarkCatalogTest`, `NioBenchmarkRunnerTest`, `BenchmarkOutputRendererTest`, `StorageBenchmarkRunnerTest`, `BenchScriptContractTest` |
| architecture guard | command、DB 和 runtime 边界 | `CommandParseIsolationTest`, `YierdisDbArchitectureGuardTest` |

查找入口：开发路径看 [`development-navigation.md`](./development-navigation.md)，模块职责看 [`module-architecture.md`](./module-architecture.md)。

## 改协议时

先跑最窄协议测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server,yierdis-networking/yierdis-networking-resp -am -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest,RespReplyWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑 server 协议集成：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server -am -Dtest=RespProtocolIntegrationTest,RespProtocolErrorIntegrationTest,RespHandshakeIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`RespRequestDecoder` 看线上 bytes 是否在 admission 后正确转成 `ByteArrayExecutionRequest` 并移交 argv 与 lease，`InboundMemoryBudget` 看 lease 是否在最后一个消费者释放，`RespReplyWriter` 看 reply 语义是否被正确编码。

## 改命令时

先跑命令家族测试和错误测试。例如 string / bitmap：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=StringCommandTest,BitmapCommandTest,CommandErrorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

新增命令或新增 option/subcommand 时，优先补最窄的命令家族测试和错误测试；server-only 命令还要补
`yierdis-server` 组装或协议集成测试。

最终命令流固定为：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

新增注册名还要补 parse-isolation fixture。默认命令由 `CommandParseIsolationTest` 保证 fixture 名称集合与
`DefaultCommandModules` 的全部注册完全相等，并用会抛异常的 router/provider 证明 parse 不访问服务；
`ServerCommandParseIsolationTest` 对 `HELLO`、`INFO`、`STATS` 做同样检查。排障顺序：先看
`CommandRegistry` 是否注册，再看 `CommandSpec.syntax()` 的 arity/key metadata，然后区分 handler parse、
准备函数的 `apply(session)` 和 `PreparedCommand.execute(session)` 三个阶段。

## 改 DB 或数据结构时

先跑 direct ops，确认不依赖命令解析也能复现：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db -am -Dtest=StringDirectOpsTest,CollectionDirectOpsTest,TtlLifecycleDirectOpsTest,NativeStorageRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑相关命令家族，确认回包语义没有偏移：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=StringCommandTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`DbEngine` direct ops -> family ops -> `YierdisDbMutationExecutor` -> key lifecycle -> root/value 结构。DB 读写细节看 [`db-internals.md`](./db-internals.md)。

## 改 transaction / replay 时

先跑事务状态和 replay 相关测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=CommandDispatcherTest,EngineSessionTest,TransactionCommandTest,TransactionQueueCleanupTest,ReplyPreflightCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`CommandDispatcher` 的 handler-parse preflight ->
`EngineSession.DefaultTransactionState.tryEnqueue(...)` -> retained `ExecutionRequest` ->
`TransactionCommands` drain -> `CommandDispatcher.prepareExecReplay(...)` -> child execute 和聚合结果。入队前不应用
handler 返回的准备函数；drain 后的 `PreparedExec` 拥有并最终关闭队列 request 与 child
`PreparedCommand`。事务与 replay 的完整主线看 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 改 TTL / expiration 时

先跑 TTL 和过期清理测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=TtlLifecycleDirectOpsTest,ActiveExpirationTest,ExpireSemanticsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`YierdisTtlOps` -> `YierdisDbKeyLifecycle` -> `YierdisDbExpirationSupport` -> reclamation mutation -> `MEMORY STATS` / `INFO memory` 口径。TTL 细节看 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## 改 maxmemory / eviction 时

先跑 maxmemory 和 eviction 测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=MutationExecutorReservationTest,MaxmemoryEvictionTest,TtlMaxmemoryTest,YierdisGlobalMaxmemoryGovernorTest,GlobalMaxmemoryLruAcrossDbsTest,MemoryStatsAccountingConsistencyTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`YierdisDbMemoryLedger` -> `YierdisDbMutationExecutor` -> `YierdisDbMaxmemorySupport` / `YierdisGlobalMaxmemoryGovernor` -> eviction reclamation -> `MEMORY STATS` 校验。maxmemory 的完整语义看 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 改 native memory 时

先跑 allocator / handle contract：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db,yierdis-db/yierdis-db -am -Dtest=NativeHandleTest,YierdisNativeObjectTableTest,YierdisFfmStableMemoryBackendTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑 DB native path：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=EntryHandleContractTest,ValueHandleContractTest,KeyHandleContractTest,NativeStorageRegressionTest,OffHeapLeakRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`NativeHandle` backend identity / private localRaw -> object table generation -> stable backend pin/quarantine/epoch -> DB handle wrappers -> keyspace/root/value release。详细背景看 [`ffm-primer.md`](./ffm-primer.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)。

## 改 executor / backpressure 时

先跑 executor 单元测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server -am -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest,ExecutionConnectionContextTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑 server 组装和 Netty 适配：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server -am -Dtest=YierdisServerBootstrapCommandWiringTest,NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：submitter 接收请求 -> backlog budget -> scheduling policy -> drain loop -> dispatcher prepare -> reply
reserve -> validate -> execute -> `CommandResult` -> `RedisReplyRenderer` -> IO adapter 写回。语义 bulk / sequence /
map source 必须在 renderer 消费完成后、`PreparedCommand` 关闭前仍然有效；`QUIT` 关闭来自
`CommandResult.closeAfterReply`。详细模型看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## 改 CLI / bench 时

CLI 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=YierdisClientTest,MaxmemoryScopeTest,TransactionQueueLimitTest -Dsurefire.failIfNoSpecifiedTests=false test
```

RESP bench 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkCommandTemplateTest,NioBenchmarkRunnerTest,BenchmarkOutputRendererTest,RedisBenchmarkCommandTest,BenchScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

这些 focused tests 分别保护 canonical catalog/selection、wire template/random placeholder、单 `Selector` scheduling、human/quiet/CSV、CLI validation/exit code 和 connect-only shell contract。排障顺序：`RedisBenchmarkOptions` -> `BenchmarkConfig` -> `RedisBenchmarkCatalog` -> `RedisBenchmarkCommandTemplate` -> `NioBenchmarkRunner` / incremental reply decoder -> `BenchmarkLatencyRecorder` -> `BenchmarkOutputRenderer` -> `BenchScriptContractTest`。详细入口看 [`client-and-bench-internals.md`](./client-and-bench-internals.md)。

storage bench 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchEntrypointTest,StorageBenchmarkConfigTest,ProcessRssReaderTest,StorageBenchmarkRendererTest,StorageBenchmarkRunnerTest,StorageBenchScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

这组测试保护 1M 默认/10M 上限、固定宽度 key、RSS unavailable、21 列 CSV、rehash 稳定 snapshot、empty baseline/loaded accounting、真实小规模 DB 生命周期和专用脚本契约。排障顺序：`StorageBenchmarkOptions` -> `StorageBenchmarkConfig` -> `StorageBenchmarkRunner` -> `StorageMemorySnapshot` -> `StorageBenchmarkResult` -> `StorageBenchmarkRenderer`。

## 改架构护栏时

当改动可能触碰协议边界、command/internal 边界或 runtime 访问约束时，优先补护栏测试：

- `CommandParseIsolationTest` / `ServerCommandParseIsolationTest`：检查全部生产注册都有 parse-only fixture，parse 不访问 DB router 或 provider。
- `YierdisDbArchitectureGuardTest`：检查 DB 保持单一公开 factory，并且实现类型不公开；必要时连同 `DbEngineReadWriteBoundaryTest` 一起跑。

排障顺序：先确认是 boundary regression 还是功能 regression，再决定是去 protocol / command / DB 文档还是直接补 guard 测试。

## 常见故障入口

| 现象 | 先看哪里 | 常用测试 |
| --- | --- | --- |
| unknown command 或 arity 不对 | `CommandRegistry`, `CommandSpec`, `CommandArgs`, `CommandDispatcher` | `CommandRegistryTest`, `CommandDispatcherTest`, `CommandErrorTest` |
| parse 阶段意外访问 DB/provider | command handler、`CommandArgs` | `CommandParseIsolationTest`, `ServerCommandParseIsolationTest` |
| 事务里行为不同 | `CommandDispatcher`, `TransactionCommands`, `TransactionState` | `CommandDispatcherTest`, `TransactionCommandTest`, `TransactionQueueCleanupTest` |
| RESP 回包形状不对 | `CommandResult`, `RedisReplyRenderer`, `RespReplyWriter` | `RedisReplyRendererTest`, `RespReplyWriterTest`, `RespProtocolIntegrationTest` |
| `QUIT` 回复后未关闭 | `CommandResult.closeAfterReply`, reply slot / sequencer | `CommandExecutorTest`, `RespProtocolIntegrationTest` |
| TTL 不准或过期 key 仍可见 | `EntryRecord.expireAtMillis`, expiration scan/reclaim | `TtlLifecycleDirectOpsTest`, `ActiveExpirationTest` |
| maxmemory 多回包或错误回包 | `YierdisDbMemoryLedger`, mutation executor | `MaxmemoryEvictionTest`, `MaxmemoryDoubleReplyRegressionTest` |
| off-heap 泄漏 | root/value release, blob store, native handle graph | `OffHeapLeakRegressionTest`, `NativeStorageRegressionTest` |
| executor 卡住或背压不恢复 | submitter、drain loop、connection context | `CommandExecutorBackpressureTest`, `CommandExecutorFairSchedulingTest` |

## 最小验证组合

小文档改动：

```bash
git diff --check -- docs/project-docs README.md
```

DB/native 语义改动：目标 direct ops 测试 + 相关命令家族测试。

executor/server 改动：executor 单元测试 + server main 集成测试 + 相关协议测试。

## Production Hardening Gates

有界 ingress、maxmemory、ordered reply 和 shutdown 改动都要运行与影响面相符的 focused tests，并用 JDK 25 运行架构守卫。性能证据由操作者分别管理的 Yierdis benchmark 与官方 Redis benchmark 原始结果组成，两边必须使用等价 workload 设置；项目 benchmark 不计算阈值或 artifact ratio，任何通过/失败判定都属于外部 policy。完整的 reply matrix、smoke、deterministic soak、最终 ownership counter 和候选证据要求见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
