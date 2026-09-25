# 测试与排障

Yierdis 的测试按层组织。改动或故障先跑哪些测试、先看哪一层，取决于它落在哪一层。本页给出：**测试分层与所在模块 → 每类改动的可运行命令 → 排障该打开的观测点（真实字段） → 最小复现步骤**。

所有 Maven/Java 命令都在 JDK 25 下运行。非交互 shell 使用这个前缀：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## 测试分层与所在模块

Yierdis 的测试按「隔离粒度」分成四类，再按「测什么」分成若干主题层。**测试类所在模块决定 `-pl`**，这一点最容易搞错。

四类隔离粒度：

- **单元测试**：不启动 server、不建 DB，直接测一个类或一个值对象。例：`RespReplyWriterTest`、`CommandSpecTest`、`HeapRequestFootprintTest`。
- **模块测试**：在单个模块内起 DB 或 kernel，绕开网络。例：`StringDirectOpsTest`、`CommandDispatcherTest`、`NativeHandleTest`。
- **跨模块集成测试**：走真实 TCP/RESP 或真实 `YierdisInstance`，验证端到端语义。集中在 `yierdis-tests`（如 `MaxmemoryEvictionTest`）与 `yierdis-server`（如 `RespProtocolIntegrationTest`、`NettyExecutionAdapterIntegrationTest`）。
- **架构护栏测试**：检查编译器无法表达的 API 形状与边界。例：`CommandParseIsolationTest`、`ServerCommandParseIsolationTest`、`YierdisDbArchitectureGuardTest`、`DbEngineReadWriteBoundaryTest`。

按主题看，常见测试如下（括号内为模块）：

| 层 | 主要目的 | 常见测试（模块） |
| --- | --- | --- |
| API contract | 稳定接口和边界语义 | `ExecutionRequestContractTest`、`PreparedCommandsTest`（server-api）、`CommandContractTest`（command）、`YierdisInstanceConfigTest`（server） |
| command / integration | 命令注册、parse 隔离、参数、回包和 Redis 兼容语义 | `StringCommandTest`、`CommandErrorTest`、`DefaultCommandRegistrationTest`（tests）、`CommandDispatcherTest`、`CommandRegistryTest`（command） |
| DB direct ops | 绕开 command 层验证 DB API | `StringDirectOpsTest`、`CollectionDirectOpsTest`、`TtlLifecycleDirectOpsTest`（db） |
| DB internal contract | lifecycle、ledger、mutation、root/value、hash 表 | `YierdisDbKeyLifecycleTest`、`MemoryLedgerContractTest`、`PreparedEntryMutationTest`、`StringRootTest`、`HashTableMaintenanceTest`（db） |
| native/internal | handle、backend、keyspace、object table | `NativeHandleTest`、`YierdisFfmStableMemoryBackendTest`、`YierdisNativeObjectTableTest`（db） |
| executor / server | owner thread、队列、背压、Netty 适配、回复渲染 | `CommandExecutorTest`、`CommandExecutorBackpressureTest`（server）、`RespProtocolIntegrationTest`（server）、`RedisReplyRendererTest`（server-api） |
| CLI / bench | 客户端、catalog、NIO runner、storage footprint、脚本与输出契约 | `YierdisClientTest`（cli）、`RedisBenchmarkCatalogTest`、`NioBenchmarkRunnerTest`、`BenchmarkOutputRendererTest`、`StorageBenchmarkRunnerTest`、`BenchScriptContractTest`（benchmark） |
| architecture guard | command、DB 和 runtime 边界 | `CommandParseIsolationTest`、`ServerCommandParseIsolationTest`、`YierdisDbArchitectureGuardTest`、`DbEngineReadWriteBoundaryTest` |

查找入口：开发路径看 [`development-navigation.md`](./development-navigation.md)，模块职责看 [`module-architecture.md`](./module-architecture.md)。

## 改协议时

先跑最窄协议测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server,yierdis-networking-resp -am -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest,RespReplyWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑 server 协议集成：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server -am -Dtest=RespProtocolIntegrationTest,RespProtocolErrorIntegrationTest,RespHandshakeIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：先确认 `RespRequestDecoder` 把 admission 后的线上 bytes 正确转成 `ByteArrayExecutionRequest`，并移交 argv 与 lease；再看 `InboundMemoryBudget` 的 lease 是否在最后一个消费者处释放（`inbound_reserved_bytes` 回到 0）；最后确认 `RespReplyWriter` 的 reply 语义编码是否正确。

## 改命令时

先跑命令家族测试和错误测试。例如 string / bitmap：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=StringCommandTest,BitmapCommandTest,CommandErrorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

新增命令或 option/subcommand 时，优先补最窄的命令家族测试和错误测试；server-only 命令还要补 `yierdis-server` 组装或协议集成测试。

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

新增注册名还要补 parse-isolation fixture。`CommandParseIsolationTest` 保证默认命令的 fixture 名称集合与 `DefaultCommandModules` 的全部注册完全相等，并用会抛异常的 router/provider 确认 parse 不访问服务；`ServerCommandParseIsolationTest` 对 `HELLO`、`INFO`、`STATS` 做同样检查。排障顺序：先查 `CommandRegistry` 有没有注册，再核对 `CommandSpec.syntax()` 的 arity/key metadata，最后区分 handler parse、准备函数的 `apply(session)` 和 `PreparedCommand.execute(session)` 三个阶段。

## 改 DB 或数据结构时

先跑 direct ops，确认不依赖命令解析也能复现：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db -am -Dtest=StringDirectOpsTest,CollectionDirectOpsTest,TtlLifecycleDirectOpsTest,NativeStorageRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

再跑相关命令家族，确认回包语义没有偏移：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=StringCommandTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`DbEngine` direct ops -> family ops -> `YierdisDbMutationExecutor` -> key lifecycle -> root/value 结构。DB 读写细节看 [`db-internals.md`](./db-internals.md)。

## 改 transaction / replay 时

先跑事务状态和 replay 相关测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=CommandDispatcherTest,EngineSessionTest,TransactionCommandTest,TransactionQueueCleanupTest,TransactionQueueLimitTest,ReplyPreflightCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`CommandDispatcher` 的 handler-parse preflight -> `EngineSession.DefaultTransactionState.tryEnqueue(...)` -> retained `ExecutionRequest` -> `TransactionCommands` drain -> `CommandDispatcher.prepareExecReplay(...)` -> child execute 和聚合结果。入队前不应用 handler 返回的准备函数；drain 后的 `PreparedExec` 拥有并最终关闭队列 request 与 child `PreparedCommand`。事务与 replay 的完整主线看 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 改 TTL / expiration 时

先跑 TTL 和过期清理测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=TtlLifecycleDirectOpsTest,ActiveExpirationTest,ExpireSemanticsTest,ExpireConditionFlagsTest,TtlConditionDirectOpsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`YierdisTtlOps` -> `YierdisDbKeyLifecycle` -> `YierdisDbExpirationSupport` -> reclamation mutation -> `MEMORY STATS` / `INFO memory` 口径。TTL 细节看 [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)。

## 改 maxmemory / eviction 时

先跑 maxmemory 和 eviction 测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am -Dtest=MutationExecutorReservationTest,MaxmemoryEvictionTest,MaxmemoryScopeTest,TtlMaxmemoryTest,YierdisGlobalMaxmemoryGovernorTest,GlobalMaxmemoryLruAcrossDbsTest,MemoryStatsAccountingConsistencyTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：`YierdisDbMemoryLedger` -> `YierdisDbMutationExecutor` -> `YierdisDbMaxmemorySupport` / `YierdisGlobalMaxmemoryGovernor` -> eviction reclamation -> `MEMORY STATS` 校验。maxmemory 的完整语义看 [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)。

## 改 native memory 时

先跑 allocator / handle contract：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db -am -Dtest=NativeHandleTest,YierdisNativeObjectTableTest,YierdisFfmStableMemoryBackendTest -Dsurefire.failIfNoSpecifiedTests=false test
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

排障顺序：submitter 接收请求 -> backlog budget -> scheduling policy -> drain loop -> dispatcher prepare -> reply reserve -> validate -> execute -> `CommandResult` -> `RedisReplyRenderer` -> IO adapter 写回。语义 bulk / sequence / map source 必须在 renderer 消费完成后、`PreparedCommand` 关闭前仍然有效；`QUIT` 关闭来自 `CommandResult.closeAfterReply`。详细模型看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## 改 CLI / bench 时

CLI 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=YierdisClientTest -Dsurefire.failIfNoSpecifiedTests=false test
```

RESP bench 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkCommandTemplateTest,NioBenchmarkRunnerTest,BenchmarkOutputRendererTest,RedisBenchmarkCommandTest,BenchScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

这些 focused tests 分别保护 canonical catalog/selection、wire template/random placeholder、单 `Selector` scheduling、human/quiet/CSV、CLI validation/exit code 和 connect-only shell contract。排障顺序：`RedisBenchmarkOptions` -> `BenchmarkConfig` -> `RedisBenchmarkCatalog` -> `RedisBenchmarkCommandTemplate` -> `NioBenchmarkRunner` / incremental reply decoder -> `LatencyRecorder` -> `BenchmarkOutputRenderer` -> `BenchScriptContractTest`。详细入口看 [`client-and-bench-internals.md`](./client-and-bench-internals.md)。

storage bench 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchEntrypointTest,StorageBenchmarkConfigTest,ProcessRssReaderTest,StorageBenchmarkRendererTest,StorageBenchmarkRunnerTest,StorageBenchScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

这组测试覆盖 1M 默认/10M 上限、固定宽度 key、RSS unavailable、29 列 CSV、rehash 稳定 snapshot、empty baseline/loaded accounting、真实小规模 DB 生命周期和专用脚本契约。排障顺序：`StorageBenchmarkOptions` -> `StorageBenchmarkConfig` -> `StorageBenchmarkRunner` -> `StorageMemorySnapshot` -> `StorageBenchmarkResult` -> `StorageBenchmarkRenderer`。

## 改架构护栏时

改动可能触碰协议边界、command/internal 边界或 runtime 访问约束时，优先补护栏测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests,yierdis-server/yierdis-server -am -Dtest=CommandParseIsolationTest,ServerCommandParseIsolationTest,YierdisDbArchitectureGuardTest,DbEngineReadWriteBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- `CommandParseIsolationTest` / `ServerCommandParseIsolationTest`：检查全部生产注册都有 parse-only fixture，parse 不访问 DB router 或 provider。
- `YierdisDbArchitectureGuardTest`：检查 DB 保持单一公开 factory，并且实现类型不公开；必要时连同 `DbEngineReadWriteBoundaryTest` 一起跑。

排障顺序：先确认是 boundary regression 还是功能 regression，再决定是去 protocol / command / DB 文档还是直接补 guard 测试。

## 排障该打开的观测点

排障不要在 JVM 堆占用上猜结论：request、maxmemory/native、reply 是三个独立的有界域。优先读下面这些**真实存在**的观测面（字段名按源码声明）。

**`INFO stats`（文本段给字段统一加 `yierdis_` 前缀）**，分组口径：

- ingress 准入：`inbound_capacity_bytes`、`inbound_reserved_bytes`、`inbound_peak_reserved_bytes`、`inbound_waiting_connections`、`inbound_backpressured`、`inbound_rejected_connections`、`inbound_closed`。
- reply 容量：`reply_global_capacity_bytes`、`reply_per_connection_capacity_bytes`、`reply_max_total_bytes`、`reply_chunk_payload_bytes`、`reply_control_reservation_bytes`、`reply_drain_timeout_millis`。
- outbound 账目：`outbound_reserved_bytes`、`outbound_allocated_bytes`、`outbound_peak_reserved_bytes`、`outbound_peak_allocated_bytes`、`outbound_active_connections`、`outbound_active_slots`、`outbound_active_chunks`、`outbound_active_sources`、`live_child_channels`。
- outbound 失败与调度：`outbound_capacity_rejects`、`outbound_oversized_replies`、`outbound_cancelled_slots`、`outbound_failed_slots`、`outbound_write_failures`、`result_unknown_closes`、`reply_shutdown_timeouts`、`deferred_fair_reply_heads`、`deferred_global_reply_heads`。
- executor：`queued_tasks`、`queued_bytes`、`submit_accepted_total`、`submit_rejected_*_total`、`commands_executed_total`、`close_after_reply_total`、`backpressure_enter_total`、`backpressure_exit_total`、`channels_autoread_disabled`。

**`INFO memory`**：`used_memory`、`used_memory_dataset`、`maxmemory`、`maxmemory_policy`、`yierdis_maxmemory_scope`、`yierdis_ledger_used_bytes`、`yierdis_ledger_reserved_bytes`、`yierdis_ledger_effective_used_bytes`、`yierdis_maxmemory_used_bytes`、`yierdis_maxmemory_effective_used_bytes`、`yierdis_offheap_used_bytes`、`yierdis_offheap_included_in_maxmemory`、`yierdis_native_metadata_committed_bytes`、`yierdis_native_data_committed_bytes`、`yierdis_native_data_live_bytes`、`yierdis_native_live_objects`、`yierdis_native_live_regions`。

**`MEMORY STATS`**（explainable estimate，不是 JVM object graph）：`maxmemory_bytes`、`used_bytes_for_maxmemory`、`effective_used_bytes_for_maxmemory`、`ledger_used_bytes`、`offheap_used_bytes`、`ledger_reserved_bytes`、`offheap_included_in_maxmemory`、`total_estimated_bytes`、`keys_stored_offheap`、`key_count`、`expire_count`。注意 `ledger_used_bytes` 绑定 `heapDataBytesEstimate`，`ledger_reserved_bytes` 才是 ledger `reservedBytes`。

判断泄漏的判据：稳态下 peak 可以非零，但**当前** `outbound_reserved_bytes` / `outbound_allocated_bytes` / `outbound_active_slots` / `outbound_active_chunks` / `outbound_active_sources` / `live_child_channels` 以及 `inbound_reserved_bytes` 在客户端断开后必须收敛到 0。非零就是泄漏信号。对应测试见 `OffHeapLeakRegressionTest`、`NativeStorageRegressionTest`。完整口径与 shutdown 顺序见 [`production-hardening-operations.md`](./production-hardening-operations.md)。

## 最小复现步骤

1. 固定环境：用 JDK 25 起 server（写一份最小 `yierdis.conf` 后 `java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --config <path>`），或用最小 fixture 直接建 DB。
2. 记录输入：命令序列、并发与 pipeline、`maxmemoryBytes`/reply 上限等非默认配置。
3. 先在**隔离层**复现：能不用网络就不用（`yierdis-db` direct ops），能不用 DB 就不用（kernel/单元）。隔离层复现成功说明问题在底层。
4. 采集证据：`INFO stats`、`INFO memory`、`MEMORY STATS`、复现命令序列。
5. 修复后回到最窄测试，再逐步上移到集成层，确认没有把问题推到另一端。

## 常见故障入口

| 现象 | 先看哪里 | 常用测试 |
| --- | --- | --- |
| unknown command 或 arity 不对 | `CommandRegistry`、`CommandSpec`、`CommandArgs`、`CommandDispatcher` | `CommandRegistryTest`、`CommandDispatcherTest`、`CommandErrorTest` |
| parse 阶段意外访问 DB/provider | command handler、`CommandArgs` | `CommandParseIsolationTest`、`ServerCommandParseIsolationTest` |
| 事务里行为不同 | `CommandDispatcher`、`TransactionCommands`、`TransactionState` | `CommandDispatcherTest`、`TransactionCommandTest`、`TransactionQueueCleanupTest` |
| RESP 回包形状不对 | `CommandResult`、`RedisReplyRenderer`、`RespReplyWriter` | `RedisReplyRendererTest`、`RespReplyWriterTest`、`RespProtocolIntegrationTest` |
| `QUIT` 回复后未关闭 | `CommandResult.closeAfterReply`、reply slot / sequencer | `CommandExecutorTest`、`RespProtocolIntegrationTest` |
| TTL 不准或过期 key 仍可见 | `EntryRecord.expireAtMillis`、expiration scan/reclaim | `TtlLifecycleDirectOpsTest`、`ActiveExpirationTest` |
| maxmemory 多回包或错误回包 | `YierdisDbMemoryLedger`、mutation executor | `MaxmemoryEvictionTest`、`MaxmemoryDoubleReplyRegressionTest` |
| off-heap 泄漏 | root/value release、blob store、native handle graph | `OffHeapLeakRegressionTest`、`NativeStorageRegressionTest` |
| executor 卡住或背压不恢复 | submitter、drain loop、connection context | `CommandExecutorBackpressureTest`、`CommandExecutorFairSchedulingTest` |

## 最小验证组合

小文档改动：

```bash
git diff --check -- docs/project-docs README.md
```

DB/native 语义改动：目标 direct ops 测试 + 相关命令家族测试。

executor/server 改动：executor 单元测试 + server main 集成测试 + 相关协议测试。

## Production Hardening Gates

有界 ingress、maxmemory、ordered reply 和 shutdown 的改动都要跑与影响面相符的 focused tests，并用 JDK 25 运行架构守卫。性能证据由操作者分别管理的 Yierdis benchmark 与官方 Redis benchmark 原始结果组成，两边必须使用等价 workload 设置。项目 benchmark 不计算阈值或 artifact ratio，通过/失败判定一律属于外部 policy。完整的 reply matrix、最终 ownership counter 和候选证据要求见 [`production-hardening-operations.md`](./production-hardening-operations.md)。