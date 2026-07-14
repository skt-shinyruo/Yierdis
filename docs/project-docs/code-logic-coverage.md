# Code Logic Coverage

本文是 Yierdis 代码逻辑文档覆盖矩阵。它不重复完整专题解释，只记录哪些核心逻辑已经有文档、哪些还只有源码和测试。

## 怎么使用

- 先按子系统找到类和关键方法。
- 看 `覆盖状态` 判断当前文档是否足够。
- 看 `文档归属` 决定补哪篇文档。
- 看 `相关测试` 决定改动后先验证哪里。

## 记录字段

| 字段 | 含义 |
| --- | --- |
| 子系统 | 逻辑所属模块或行为边界 |
| 类 | 主要实现入口 |
| 关键方法/逻辑块 | 需要解释的行为单元 |
| 行为职责 | 这段逻辑真正负责什么 |
| 关键分支/状态/不变量 | 维护时最容易破坏的事实 |
| 线程/内存边界 | owner thread、生命周期、materialization 等边界 |
| 相关测试 | 改这里先看哪些测试 |
| 文档归属 | 已有或计划中的文档 |
| 覆盖状态 | `covered` / `partial` / `missing` |
| 备注 | 实现现状、文档缺口或待确认点 |

## server-main / bootstrap / connection

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `YierdisServerBootstrap` | `start(...)`, `startInternal()`, `close()` | 组装 runtime、engine、executor 和 Netty server | 启动顺序、资源关闭顺序、默认组件选择 | Netty 线程和 executor owner thread 分离 | `YierdisServerBootstrapCommandWiringTest`, `YierdisServerBootstrapCloseTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`configuration-and-operations.md`](./configuration-and-operations.md), [`main-path-walkthrough.md`](./main-path-walkthrough.md) | `covered` | startInternal 顺序、pipeline 装配和 reverse close 已补齐 |
| `YierdisServerChannelInitializer` | `initChannel(...)`, backpressure / idle handler 装配 | 为单连接挂载协议、执行和关闭保护 handler | handler 顺序、协议错误回包、read-idle 关闭 | 每条连接只在 Netty channel 生命周期内持有上下文 | `NettyExecutionAdapterIntegrationTest`, `RespProtocolErrorIntegrationTest` | [`protocol-reference.md`](./protocol-reference.md), [`request-execution-flow.md`](./request-execution-flow.md), [`configuration-and-operations.md`](./configuration-and-operations.md) | `covered` | pipeline 顺序、protocol error 和 close-after-reply 边界已补齐 |
| `YierdisFastCommandHandler` | submit path | 把 `ExecutionRequest` 提交给 `CommandExecutor` | submit reject、closing、直接回错 | I/O 线程不执行命令 | `ClosingSkipSideEffectsIntegrationTest`, `NettyExecutionAdapterIntegrationTest`, `RespProtocolErrorIntegrationTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) | `covered` | handler 提交/拒绝/直接回错边界已补齐 |
| `NettyExecutionConnection` | request/reply/close 适配块 | 把 Netty channel 状态收敛成 executor 可消费的连接抽象 | 关闭后跳过副作用、reply 写回失败处理 | Channel 生命周期与 executor connection state 对接 | `ClosingSkipSideEffectsIntegrationTest`, `NettyExecutionAdapterIntegrationTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md) | `covered` | Channel root、closing 和事务丢弃边界已补齐 |

## networking-resp / networking-netty

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `RespRequestDecoder` | decode/admission path | RESP array / inline command 解码并直接构造 `ExecutionRequest` | 协议错误、inline 分支、limit 分支、argv/payload admission | I/O 线程只做协议适配；lease 脱离 Netty 生命周期 | `RespRequestDecoderTest`, `RespIngressAdmissionTest`, `RespProtocolErrorIntegrationTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`protocol-reference.md`](./protocol-reference.md) | `covered` | 协议错误、inline 解析、admission 和超限关闭行为已补齐 |
| `RetainedRespExecutionRequest` | retain/close path | 网络主链的 lease-backed `ExecutionRequest` | retain 共享 argv、最终 close 只归还一次 budget | 不保留 Channel/ByteBuf，可跨线程关闭 | `RespRequestDecoderTest`, `RespIngressLifecycleIntegrationTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md) | `covered` | decoder 直接边界和 detached lease 释放已覆盖 |
| `RespReplyWriter` | reply encode methods | 把 command 层回包语义编码成 RESP2/RESP3 | RESP2/RESP3 类型差异、null/map/set 编码、error 形态 | 编码层不泄漏 command/internal 对象 | `RespReplyWriterTest`, `RespHandshakeIntegrationTest` | [`protocol-reference.md`](./protocol-reference.md) | `covered` | 协议输出形态已有专门文档和测试入口 |

## executor

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CommandExecutor` | `start()`, `trySubmit(...)`, drain loop | owner thread 执行、排队、预算、关闭 | queue slot、queued bytes、drain budget、close-after-reply | DB 访问只能发生在 owner thread | `CommandExecutorTest`, `CommandExecutorBackpressureTest`, `CommandExecutorFairSchedulingTest` | [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`request-execution-flow.md`](./request-execution-flow.md) | `covered` | 关闭、恢复和 flush/close 关系已补到专题文档 |
| `CommandExecutorSubmitter` | submit / budget accounting path | 处理 backlog budget、bytes budget 和连接背压 | 超预算拒绝、背压恢复、队列状态切换 | Netty submitter 线程只能入队，不能执行命令 | `CommandExecutorBackpressureTest`, `ExecutionConnectionContextTest` | [`executor-and-backpressure.md`](./executor-and-backpressure.md) | `covered` | submit 顺序、budget 回滚和 backpressure 恢复已补齐 |
| `CommandExecutorDrainLoop` | GLOBAL / FAIR drain loop | 决定 drain 次序、公平调度和单轮预算 | scheduling policy、per-connection budget、空队列退出 | owner thread 独占 drain 和 session 执行 | `CommandExecutorFairSchedulingTest`, `CommandExecutorTest` | [`executor-and-backpressure.md`](./executor-and-backpressure.md) | `covered` | FAIR / GLOBAL 差异和 drain budget 已补齐 |

## engine / session / transaction

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `DefaultYierdisEngine` | `execute(...)` | session/request/reply 到 command processor 的桥接，并建立当前命令 record scope | session capability 收窄、record scope、reply writer 边界 | engine 不直接触 DB internal | `DefaultYierdisEngineTest`, `DbEngineReadWriteBoundaryTest` | [`request-execution-flow.md`](./request-execution-flow.md), [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md), [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md) | `covered` | capability 边界、command record 和 processor 入口已补齐 |
| `EngineSession` | session accessors, `DefaultTransactionState` | 维护 DB index、protocol version、transaction queue 和 client metadata | `MULTI/EXEC/DISCARD` 状态机、abort 标记、selected DB 持续性 | session 状态只在单连接执行上下文中读写 | `EngineSessionTest`, `TransactionQueueCleanupTest` | [`transaction-and-replay.md`](./transaction-and-replay.md), [`commands-and-data-model.md`](./commands-and-data-model.md) | `covered` | 事务状态机、snapshot queue 和 cleanup 路线已有独立文档 |
| `TransactionState` | queue / replay lifecycle | `MULTI/EXEC/DISCARD` 状态机 | queue 限制、abort、replay 顺序 | 事务保存的是请求快照 | `TransactionCommandTest`, `EngineSessionTest`, `TransactionQueueCleanupTest` | [`transaction-and-replay.md`](./transaction-and-replay.md) | `covered` | 已补独立状态机文档 |
| `ExecutionRecord` | transaction replay snapshot / runtime event payload | 保存可重放命令和变更事件所需的最小事实 | queue 时校验一次、replay 时复用参数快照 | 必须与原始请求生命周期解耦 | `TransactionQueueCleanupTest`, `CommitStreamTest` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md), [`transaction-and-replay.md`](./transaction-and-replay.md), [`glossary.md`](./glossary.md) | `covered` | dbIndex 归一化和 copied request snapshot 已补齐 |

## command-api / command-core / builtin commands

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CommandSpec` | spec factory / accessor methods | 定义命令名、arity、handler 和事务限制 | arity 规则、MULTI 限制、只读/写语义 | command 元数据与 transport 无关 | `CommandContractTest`, `CommandRegistryGuardTest` | [`commands-and-data-model.md`](./commands-and-data-model.md) | `covered` | 命令抽象和约束已有较完整说明 |
| `YierdisFastCommandProcessor` | `execute(...)` | 命令查表、解析、事务分支、执行和错误翻译 | empty/unknown、parse error、MULTI queue | 只通过 API 访问 DB | `YierdisFastCommandProcessorPolicyTest`, `YierdisFastCommandProcessorRegistrationTest`, `YierdisFastCommandProcessorModuleTest`, `CommandVariantCoverageTest` | [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md), [`request-execution-flow.md`](./request-execution-flow.md) | `covered` | DB commit publication 不在 command processor 内推断 |
| `DbCommitPublisher` / `CommitStream` | reserve / publish / callback drain | DB commit 记录预留、无分配发布和有界异步 delivery | slot/byte hard limit、post-commit failure、borrowed callback、shutdown drain | DB 只依赖 API port；runtime worker 独占 sink callback | `DbCommitPublisherTest`, `CommitStreamTest`, `CommitStreamShutdownTest`, `CommitStreamIntegrationTest` | [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md), [`production-hardening-operations.md`](./production-hardening-operations.md) | `covered` | 不承诺 durable AOF 或 replication |
| `StringCommands` | GET/SET/INCR 族 handler | 代表内建命令如何经 `CommandSupport` 路由到 DB API | wrong-type、NX/XX/EX/PX 语义、整数分支 | handler 不直接碰 internal root/value | `StringCommandTest`, `CommandErrorTest` | [`commands-and-data-model.md`](./commands-and-data-model.md), [`request-execution-flow.md`](./request-execution-flow.md) | `covered` | string/bitmap 主路线、wrong-type 和 TTL 语义已串起来 |

## runtime / multi-db / maxmemory governor

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `YierdisInstance` | `create(...)`, `engine(int)`, `bindToCurrentThread()`, `close()` | 管理多 DB 生命周期、owner thread 绑定和 runtime 资源 | strict create 注入要求、DB index 边界、关闭幂等性 | runtime access 绑定 executor owner thread | `YierdisInstanceTest`, `YierdisInstanceBoundaryTest` | [`db-internals.md`](./db-internals.md), [`configuration-and-operations.md`](./configuration-and-operations.md), [`request-execution-flow.md`](./request-execution-flow.md) | `covered` | strict create、owner-thread binding 和 reverse close 已补齐 |
| `YierdisInstanceMaintenance` | maintenance tick path | 驱动 expire cleanup、defrag 和 maintenance 协调 | tick coalescing、逐 DB 扫描顺序、per-db 与 global enforce 时机 | maintenance 在 runtime owner 上下文运行 | `YierdisDbDefragMaintenanceTest`, `ExpireIndexTest` | [`configuration-and-operations.md`](./configuration-and-operations.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) | `covered` | tick 顺序、coalescing 和 native defrag budget cross-link 已补齐 |
| `YierdisGlobalMaxmemoryGovernor` | cross-db arbitration / eviction loop | 在多 DB 之间协调全局 maxmemory 压力和淘汰 | cleanup-first、owned physical snapshots、victim 选择、全局限制收敛 | 协调器跨 DB 观察 memory participant，但不直接越过 DB API | `YierdisGlobalMaxmemoryGovernorTest`, `GlobalMaxmemoryLruAcrossDbsTest`, `TtlMaxmemoryTest` | [`configuration-and-operations.md`](./configuration-and-operations.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) | `covered` | global scope 的 prepareWrite、maintenance 和跨 DB LRU 已有专门说明 |

## db-api / db-memory

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `YierdisDb` | capability accessors / lifecycle methods | 暴露单 DB 的 reads/writes/expiration/memory/lifecycle 视图 | capability 聚合边界、关闭后行为、internal 不外泄 | DB 实现只应在 runtime/command 约定边界内可见 | `YierdisDbConstructionTest`, `DbEngineReadWriteBoundaryTest` | [`db-internals.md`](./db-internals.md) | `covered` | 单 DB capability 视图已有专题文档 |
| `YierdisDbMutationExecutor` | mutation reservation / commit / rollback path | 串联 mutation plan、memory reservation 和真实提交 | no-op accounting、rollback 一致性、TTL/ledger 同步提交 | owner thread 写路径统一入口，不能绕过 ledger | `MutationExecutorReservationTest`, `TtlMaxmemoryTest` | [`db-internals.md`](./db-internals.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) | `covered` | reservation 先于 apply、失败回滚和 OOM 映射已补齐 |
| `YierdisDbKeyLifecycle` | `liveEntryRecord(...)`, `computeWithHandleResult(...)`, `removeEntry(...)` | 维护 key、entry、TTL metadata 和 payload 生命周期 | lazy expire、删除顺序、synthetic delete、replacement release | key record / value handle 生命周期必须成对收敛 | `ExpireIndexTest`, `ExpireKeySharingTest`, `OffHeapBytesViewTtlRegressionTest` | [`db-internals.md`](./db-internals.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) | `covered` | live lookup、TTL 双写和删除副作用已有专门说明 |
| `YierdisDbExpirationSupport` | `cleanupExpired(...)` | 批量过期清理和脏索引自愈 | sample 数、命中率阈值、循环上限、时间 budget | owner thread cleanup；maintenance 下可发 synthetic `EXPIRED` delete | `ExpireIndexTest`, `TtlLifecycleDirectOpsTest`, `ExpireSemanticsTest` | [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md) | `covered` | cleanup 策略、budget 和 maintenance 调度已补齐 |
| `YierdisDbMaxmemorySupport` | `evictUntilUnder(...)`, victim helpers | 提供单 DB 视角的 victim 选择、回收和 memory 统计 | victim record 失效重试、淘汰与 TTL 清理协同、synthetic `EVICTED` delete | participant 受全局 governor 调用，但仍保留单 DB 边界 | `MaxmemoryEvictionTest`, `TtlMaxmemoryTest`, `GlobalMaxmemoryLruAcrossDbsTest` | [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md) | `covered` | policy 分支、删除顺序和 change-event 边界已有专门说明 |

## bytes / memory / ffm

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ByteArrayExecutionRequest` | constructor / copy / read-only access path | 提供 heap byte[] backed `ExecutionRequest` 视图 | copy vs read-only 边界、数组复用、空参数表示 | 用于 heap 输入和 retained snapshot；网络主链使用 detached lease 请求 | `ExecutionRequestContractTest`, `EngineSessionTest`, `DefaultYierdisEngineTest` | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`protocol-reference.md`](./protocol-reference.md) | `covered` | 视图族选择规则和 read-only fast path 已补齐 |
| `NativeHandle` | encode/decode contract | stable handle ABI | domain/kind/generation 校验、null sentinel、raw long 不能当指针 | handle tests | `NativeHandleTest`, `YierdisNativeObjectTableTest` | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | `covered` | stable handle ABI 和 fail-fast 语义已补齐 |
| `YierdisNativeObjectTable` | allocate / free / resolve / quarantine path | 管理 native object metadata、generation、pin 和隔离回收 | generation 校验、pin 泄漏、防止 use-after-free | native handle 只能经 table 解析，不能把 raw handle 当指针 | `YierdisNativeObjectTableTest`, `KeyHandleContractTest` | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | `covered` | object table、pin、quarantine 已有相对完整说明 |
| `YierdisStableNativeAllocator` | allocate / realloc / defrag methods | 提供稳定地址分配、重定位和 active defrag 能力 | stable address 不变量、epoch、defrag 可见性 | FFM/native 生命周期独立于 Java object，但受 owner/runtime 管理 | `YierdisStableNativeAllocatorTest`, `UnsafeOffHeapDbSmokeTest`, `NativeStorageRegressionTest` | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`native-memory-runtime.md`](./native-memory-runtime.md) | `covered` | allocator 与 DB handle graph 以及 maintenance 调度已连起来 |
| `EntryHandle` / `ValueHandle` | typed wrapper usage | DB 层类型化 stable handle | 不能缓存 physical address、domain/kind 校验、resolve view 短生命周期 | DB hot path 只保存 handle | `EntryHandleContractTest`, `ValueHandleContractTest`, `KeyHandleContractTest`, `YierdisDbNativeHandleGraphTest` | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | `covered` | wrapper contract 和 DB handle graph 已明确 |
| `NativeBytesSlice` | `writeTo(...)`, random read methods | 把 allocator-backed bytes 作为 `BytesSlice` 流式输出 | write 期间 pin/unpin、offset/length 校验、同步写出边界 | slice 不拥有 handle；长生命周期保存必须复制 | `NativeBytesSliceTest`, `NativeCollectionReadStreamingTest` | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | `covered` | collection/string streaming 的 native-backed 输出入口 |
| `NativeByteStore` / `NativeByteMap` / `NativeListpack` | collection internal byte ownership | 为 hash/set/zset/list internals 分配 type-specific native handles | field/value/member kind 区分、release 顺序、streaming slice 不复制 | internals 属于 shared native allocator，root/value adapter 只保存 stable handle | `HashValueTest`, `SetValueTest`, `ZSetValueTest`, `ListValueTest`, `NativeStorageRegressionTest` | [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md) | `covered` | legacy collection byte helpers 已由 native byte helpers 替代 |
| `YierdisDbNativeHandleGraph` | reachable handle traversal | 枚举 key、entry、string、collection root 和 collection internal handles | traversal 必须解析 live handle 并包含 collection internals | graph 是 defrag/validation 视角，不暴露 physical address | `YierdisDbNativeHandleGraphTest`, `NativeStorageRegressionTest` | [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`native-memory-runtime.md`](./native-memory-runtime.md) | `covered` | graph coverage 已覆盖 list/hash/set/zset internal handles |
| `BytesView` / `BytesSlice` / `BytesSink` | materialization / streaming contract | heap/off-heap/direct 边界与流式写出 | short-lived view、copy 时机、sink ownership、direct fast path | 长生命周期不能泄漏短 view | `RespRequestDecoderTest`, `RespReplyWriterTest`, `OffHeapBytesViewTtlRegressionTest` | [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) | `covered` | ownership 和 materialization 边界已单列 |

## cli / benchmark / smoke

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `InlineCommandParser` | parse methods | CLI inline 命令切词、引号和转义解析 | quote/escape 规则、空 token、错误输入 | 纯 heap 解析，不共享 server 请求对象 | `InlineCommandParserTest`, `YierdisClientTest`, `MaxmemoryScopeTest` | [`client-and-bench-internals.md`](./client-and-bench-internals.md), [`protocol-reference.md`](./protocol-reference.md) | `covered` | CLI / server inline 共享规则和差异边界已补齐 |
| `YierdisClient` | connect / execute / reply decode methods | 提供测试和 CLI 共用的 RESP client | 读超时、半包、server close、flooding reply | socket / buffer 生命周期只在 client 侧封装 | `YierdisClientTest`, `TransactionQueueLimitTest` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) | `covered` | 客户端行为、测试用法和边界已有较完整入口 |
| `YierdisBench` | `main(...)`, workload execution | benchmark 启动/复用 server 并执行 workload | 本地 server vs 外部 server、结果汇总、参数模式切换 | bench 作为外部使用者，不能绕过 RESP/CLI 边界 | `YierdisBenchComparisonExecutionTest`, `YierdisBenchSummaryFormatTest` | [`client-and-bench-internals.md`](./client-and-bench-internals.md) | `covered` | workload 选择、strictReplies 和比较结果边界已补齐 |

## tests / architecture guards / contract tests

| 类 | 关键方法/逻辑块 | 行为职责 | 关键分支/状态/不变量 | 线程/内存边界 | 相关测试 | 文档归属 | 覆盖状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ArchitectureDependencyRuleTest` | module dependency assertions | 防止 Maven/module 依赖方向回退 | forbidden edge、allowed edge、测试模块例外 | 架构护栏，不承载运行时状态 | [`ArchitectureDependencyRuleTest`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java) | [`module-architecture.md`](./module-architecture.md), [`testing-and-debugging.md`](./testing-and-debugging.md) | `covered` | 导航和护栏关系已经较清楚 |
| `ArchitectureBoundaryTest` | protocol boundary assertions | 防止网络实现泄漏进 command 层 | `ExecutionRequest` 边界、已删除 adapter 路径回退 | 保护 transport-neutral 输入边界 | [`ArchitectureBoundaryTest`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java), `RespRequestDecoderTest` | [`protocol-reference.md`](./protocol-reference.md), [`testing-and-debugging.md`](./testing-and-debugging.md) | `covered` | 直接 decoder-to-request 边界已有护栏 |
| `YierdisDbArchitectureGuardTest` | DB internal boundary assertions | 防止 command/runtime 越界依赖 DB internal | internal package 访问、owner-thread 假设、handle 泄漏 | 保护 DB API 与 internal 的编译期边界 | [`YierdisDbArchitectureGuardTest`](../../yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java), `DbEngineReadWriteBoundaryTest` | [`db-internals.md`](./db-internals.md), [`testing-and-debugging.md`](./testing-and-debugging.md) | `covered` | DB internal guard 的测试导航已补齐 |
