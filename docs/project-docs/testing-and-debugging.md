# Testing And Debugging

本文说明 Yierdis 测试如何分层，以及不同改动和故障应该先跑哪些测试、先看哪一层。

所有 Maven/Java 命令都使用 JDK 25。非交互 shell 中使用这个前缀：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## 测试分层

Yierdis 的测试大致分成七层：

| 层 | 主要目的 | 常见测试 |
| --- | --- | --- |
| API contract | 稳定接口和边界语义 | `ExecutionRequestContractTest`, `CommandContractTest`, `DbEngineFactoryPolicyContractTest` |
| command / integration | 命令参数、回包、Redis 兼容语义 | `CommandProcessorTest`, `StringCommandTest`, `CommandErrorTest`, `CommandVariantCoverageTest` |
| DB direct ops | 绕开 command 层验证 DB API | `StringDirectOpsTest`, `CollectionDirectOpsTest`, `TtlLifecycleDirectOpsTest` |
| native/internal | handle、allocator、keyspace、root/value、ledger | `NativeHandleTest`, `YierdisStableNativeAllocatorTest`, `StringRootTest`, `MemoryLedgerContractTest` |
| executor / server | owner thread、队列、背压、Netty 适配 | `CommandExecutorTest`, `CommandExecutorBackpressureTest`, `RespProtocolIntegrationTest` |
| CLI / bench | 客户端、脚本和 benchmark 输出契约 | `YierdisClientTest`, `BenchScriptContractTest`, `YierdisBenchSummaryFormatTest` |
| architecture / docs guard | 模块边界和覆盖矩阵 | `ArchitectureDependencyRuleTest`, `RespBoundaryGuardTest`, `OperationCoverageMatrixTest` |

查找入口：开发路径看 [`development-navigation.md`](./development-navigation.md)，源码职责看 [`core-logic-index.md`](./core-logic-index.md)，覆盖状态看 [`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md)。

## 改协议时

先跑最窄协议测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-networking/yierdis-networking-resp -am -Dtest=RespRequestDecoderTest,RespExecutionAdapterTest,RespReplyWriterTest test
```

再跑 server 协议集成：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=RespProtocolIntegrationTest,RespProtocolErrorIntegrationTest,RespHandshakeIntegrationTest test
```

排障顺序：`RespRequestDecoder` 看线上 bytes 是否被正确切成 request，`RespCommandAdapter` / `RespExecutionAdapter` 看是否正确变成 `ExecutionRequest`，`RespReplyWriter` 看 reply 语义是否被正确编码。

## 改命令时

先跑命令家族测试和错误测试。例如 string / bitmap：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=StringCommandTest,BitmapCommandTest,CommandErrorTest,CommandVariantCoverageTest test
```

新增命令或新增 option/subcommand 时，还要跑矩阵 guard：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest -Dsurefire.failIfNoSpecifiedTests=false test
```

排障顺序：先看 `CommandRegistry` 是否注册，`CommandSpec` arity/key metadata 是否正确，再看 `YierdisFastCommandProcessor` 是否进入事务队列、错误路径或实际 handler。

## 改 DB 或数据结构时

先跑 direct ops，确认不依赖命令解析也能复现：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=StringDirectOpsTest,CollectionDirectOpsTest,TtlLifecycleDirectOpsTest,NativeStorageRegressionTest test
```

再跑相关命令家族，确认回包语义没有偏移：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=StringCommandTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest test
```

排障顺序：`DbEngine` capability view -> family ops -> `YierdisDbMutationExecutor` -> key lifecycle -> root/value 结构。DB 读写细节看 [`db-internals.md`](./db-internals.md)。

## 改 native memory 时

先跑 allocator / handle contract：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeHandleTest,YierdisNativeObjectTableTest,YierdisStableNativeAllocatorTest test
```

再跑 DB native path：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=EntryHandleContractTest,ValueHandleContractTest,KeyHandleContractTest,NativeStorageRegressionTest,OffHeapLeakRegressionTest test
```

排障顺序：`NativeHandle` bit layout -> object table generation -> stable allocator pin/quarantine/epoch -> DB handle wrappers -> keyspace/root/value release。详细背景看 [`ffm-primer.md`](./ffm-primer.md)、[`native-allocator-and-handles.md`](./native-allocator-and-handles.md)、[`native-memory-runtime.md`](./native-memory-runtime.md)。

## 改 executor / backpressure 时

先跑 executor 单元测试：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor -am -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest,ExecutionConnectionContextTest test
```

再跑 server 组装和 Netty 适配：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerBootstrapCommandWiringTest,NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest test
```

排障顺序：submitter 接收请求 -> backlog budget -> scheduling policy -> drain loop -> execution support -> IO adapter 写回。详细模型看 [`executor-and-backpressure.md`](./executor-and-backpressure.md)。

## 改 CLI / bench 时

CLI 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=YierdisClientTest,MaxmemoryScopeTest,TransactionQueueLimitTest test
```

bench 先跑：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchScriptContractTest,SmokeScriptContractTest,YierdisBenchSummaryFormatTest,YierdisBenchComparisonRenderTest test
```

排障顺序：`InlineCommandParser` -> client codec -> script contract -> benchmark args -> summary/comparison renderer。详细入口看 [`client-and-bench-internals.md`](./client-and-bench-internals.md)。

## operation coverage matrix

[`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md) 是测试覆盖索引，也是 guard tests 的输入文件。它被 `OperationCoverageMatrixTest` 和 `ServerOperationCoverageMatrixTest` 解析，修改时必须保持这些规则：

- command heading 必须是 `### UPPERCASECOMMAND`。
- 三层状态行必须保持 ``- **Command layer**: `status` - detail``、``- **DB API**: `status` - detail``、``- **Native internals**: `status` - detail``。
- option/subcommand 覆盖行必须保持 ``- **Command variant**: `variant` - `status` - detail``。
- 状态只能是 `covered`、`covered-by-shared-test`、`missing`、`not-applicable`。
- `covered` 和 `covered-by-shared-test` 必须包含 `FileName#methodName` 证据。
- `## Option And Subcommand Inventory`、`## DB API Inventory`、`## Native/Internal Inventory`、`## Current Gap Queue` 这些 heading 不能改名。

矩阵 guard 命令：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## 常见故障入口

| 现象 | 先看哪里 | 常用测试 |
| --- | --- | --- |
| unknown command 或 arity 不对 | `CommandRegistry`, `CommandSpec`, command handler | `CommandRegistryGuardTest`, `CommandErrorTest` |
| 事务里行为不同 | `YierdisFastCommandProcessor`, `TransactionState` | `TransactionCommandTest`, `TransactionQueueCleanupTest` |
| RESP 回包形状不对 | `ReplyWriter`, `RespReplyWriter` | `RespReplyWriterTest`, `RespProtocolIntegrationTest` |
| TTL 不准或过期 key 仍可见 | `YierdisExpireIndex`, lifecycle cleanup | `TtlLifecycleDirectOpsTest`, `ExpireIndexTest` |
| maxmemory 多回包或错误回包 | `YierdisDbMemoryLedger`, mutation executor | `MaxmemoryEvictionTest`, `MaxmemoryDoubleReplyRegressionTest` |
| off-heap 泄漏 | root/value release, blob store, native handle graph | `OffHeapLeakRegressionTest`, `NativeStorageRegressionTest` |
| executor 卡住或背压不恢复 | submitter、drain loop、connection context | `CommandExecutorBackpressureTest`, `CommandExecutorFairSchedulingTest` |
| docs matrix guard 失败 | matrix heading、row shape、evidence | `OperationCoverageMatrixTest`, `ServerOperationCoverageMatrixTest` |

## 最小验证组合

小文档改动：

```bash
git diff --check -- docs/project-docs/development-navigation.md docs/project-docs/testing-and-debugging.md docs/project-docs/operation-test-coverage-matrix.md docs/project-docs/core-logic-index.md docs/project-docs/glossary.md
```

命令或矩阵改动：

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest -Dsurefire.failIfNoSpecifiedTests=false test
```

DB/native 语义改动：目标 direct ops 测试 + 相关命令家族测试 + 矩阵 guard。

executor/server 改动：executor 单元测试 + server main 集成测试 + 相关协议测试。
