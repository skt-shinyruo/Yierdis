# Task List: architecture_refactor

> 目标：用“可执行、可验收”的任务清单解决当前已识别的 5 类架构/设计问题（模块边界、参数体系、executor 一致性、协议一致性、off-heap 解耦）。
>
> 任务约定：
> - 每条任务尽量包含：**落点文件/目录** + **验收命令/测试** + **依赖关系**。
> - 重构按阶段推进：每完成一个阶段，必须保证对应模块 `mvn -pl ... test` 通过后再进入下一阶段。
> - 默认保持包名不变（`yier.bubu.redis.*`），通过“跨模块迁移 + pom 依赖调整”达成边界治理。
>
> 任务状态符号：`[ ]` Pending / `[√]` Completed / `[X]` Failed / `[-]` Skipped / `[?]` To be confirmed。

## 0. 基线与安全网（Baseline）

- [√] 0.1 记录环境基线：执行 `java -version`、`mvn -version`，将输出摘要写入 `helloagents/history/2026-01/202601142007_architecture_refactor/how.md` 的 Testing and Deployment（verify why.md#value-proposition-and-success-metrics）
- [√] 0.2 记录构建基线（当前工作区）：执行 `mvn test`，记录通过/失败情况与耗时（verify why.md#value-proposition-and-success-metrics）
- [√] 0.3 固化“必须保持通过”的测试清单（先列出，后续阶段逐一确保仍通过）（verify why.md#value-proposition-and-success-metrics）
- [√] 0.3.1 协议/RESP 测试：`RespDecoderTest`、`RespEncoderTest`、`RespRoundTripTest`、`RespCommandDecoderZeroCopyTest`、`RespWriterSliceTest`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）
- [√] 0.3.2 命令语义测试：`CommandProcessorTest`、`CommandErrorTest`、`ExpireSemanticsTest`、`MaxmemoryEvictionTest`、`BitmapCommandTest`、`HllCommandTest`、`HashCommandTest`、`ListCommandTest`、`SetCommandTest`、`ZSetCommandTest`（verify why.md#requirement-executor-ssot-scenario-single-executor-impl）
- [√] 0.3.3 DB/数据结构测试：`ByteArrayHashMapTest`、`ByteArrayKeyspaceTest`、`ExpireIndexTest`、`ExpireKeySharingTest`、`HashValueTest`、`ListValueTest`、`YierdisListpackTest`、`ZSetValueTest` 等（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 0.3.4 off-heap 回归测试：`YierdisOffHeapAllocatorContractTest`、`OffHeapLeakRegressionTest`、`UnsafeOffHeapDbSmokeTest`、`OffHeapStringStorageTest`、`UnsafeOffHeapDictLongFuzzTest`（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional）
- [√] 0.4 定义最小 smoke（不追求性能，仅验证链路连通）：server 启动/关闭 + CLI 执行 1 个命令 + bench `--strictReplies` 运行 1 轮（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）
- [√] 0.5 阶段性回归回填（将实际执行记录到 how.md）：
  - `mvn -pl yierdis-protocol -am test`
  - `mvn -pl yierdis-core -am test`
  - `mvn -pl yierdis-server -am test`
  - `mvn -pl yierdis-args -am test`
  - `mvn -pl yierdis-bench -am test`

## 1. 模块化与依赖方向（Module Boundary / SSOT）

### 1A. 新增模块骨架（Maven multi-module）

- [√] 1.1 更新根 `pom.xml` 的 `<modules>`：新增 `yierdis-protocol`、`yierdis-core`、`yierdis-args`（可选：`yierdis-client`）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.2 创建 `yierdis-protocol/`：补齐 `pom.xml` + `src/main/java` + `src/test/java`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.1）
- [√] 1.3 创建 `yierdis-core/`：补齐 `pom.xml` + `src/main/java` + `src/test/java`（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.1）
- [√] 1.4 创建 `yierdis-args/`：补齐 `pom.xml` + `src/main/java` + `src/test/java`（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 1.1）

### 1B. 抽取 RESP SSOT：迁移 protocol 代码到 yierdis-protocol

- [√] 1.5 在 `yierdis-protocol/pom.xml` 中声明依赖：
  - `io.netty:netty-all`（或拆分到 buffer/codec）
  - `yierdis-offheap-api`（提供 slice/buf）
  - `junit:junit`（test）
  - `yierdis-offheap-netty`（test：仅用于 slice -> ByteBuf 的断言）
  并确保继承根版本管理（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.2）
- [√] 1.6 迁移 RESP 源码目录：将 `yierdis-server/src/main/java/yier/bubu/redis/protocol/**` 移动到 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/**`（保持包名不变）（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.5）
- [√] 1.6.1 迁移文件清单（必须全部迁移）：`RespArray.java`、`RespBulkString.java`、`RespCommand.java`、`RespCommandDecoder.java`、`RespDecoder.java`、`RespEncoder.java`、`RespError.java`、`RespInteger.java`、`RespNull.java`、`RespObject.java`、`RespProtocol.java`、`RespSimpleString.java`、`RespType.java`、`RespWriter.java`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）
- [√] 1.7 迁移 RESP 测试目录：将 `yierdis-server/src/test/java/yier/bubu/redis/protocol/**` 移动到 `yierdis-protocol/src/test/java/yier/bubu/redis/protocol/**`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.6）
- [√] 1.8 调整 `yierdis-server/pom.xml`：增加对 `yierdis-protocol` 的依赖；确保 server 不再编译包含 protocol 的源码（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.7）
- [√] 1.9（延后到 5.x）bench 是否需要依赖 `yierdis-protocol`：只有当 bench 复用 `RespWriter/RespDecoder` 时才添加（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 5.1）
- [√] 1.10 验收：执行 `mvn -pl yierdis-protocol -am test` 与 `mvn -pl yierdis-server -am test` 必须通过（verify why.md#value-proposition-and-success-metrics, depends on task 1.8）

### 1C. 抽取 core：迁移 DB/command 到 yierdis-core

- [√] 1.11 在 `yierdis-core/pom.xml` 中声明依赖：`yierdis-protocol`、`yierdis-offheap-api`、`yierdis-offheap-unsafe`（以及需要的后端模块）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.3）
- [√] 1.12 迁移 DB 源码：将 `yierdis-server/src/main/java/yier/bubu/redis/db/**` 移动到 `yierdis-core/src/main/java/yier/bubu/redis/db/**`（保持包名不变）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.11）
- [√] 1.12.1 迁移文件清单（db 根目录 21 个）：`ByteArrayHashMap.java`、`ByteArrayHashSet.java`、`ByteArrayKeyspace.java`、`HashValue.java`、`ListValue.java`、`SetValue.java`、`ValueEncoding.java`、`ValueType.java`、`YierdisBulkStringOutput.java`、`YierdisBytesView.java`、`YierdisDb.java`、`YierdisEncodingThresholds.java`、`YierdisExpireIndex.java`、`YierdisHeapExpireIndex.java`、`YierdisHyperLogLog.java`、`YierdisKeyspace.java`、`YierdisListpack.java`、`YierdisObject.java`、`YierdisValue.java`、`ZSetValue.java`、`ZSkipList.java`（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.12.2 迁移文件清单（db/offheap 8 个）：`YierdisUnsafeOffHeapDictLong.java`、`YierdisUnsafeOffHeapExpireIndex.java`、`YierdisUnsafeOffHeapKeyspace.java`、`YierdisUnsafeOffHeapListpack.java`、`YierdisUnsafeOffHeapRawSlice.java`、`YierdisUnsafeOffHeapSds.java`、`YierdisUnsafeOffHeapString.java`、`YierdisUnsafeOffHeapZSet.java`（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.13 迁移 command 源码：将 `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` 移动到 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.11）
- [√] 1.14 调整 `yierdis-server/pom.xml`：增加对 `yierdis-core` 的依赖；将“仅 DB/命令需要”的依赖迁移到 core（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.12,1.13）
- [√] 1.15 server wiring 最小修改：保证 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`、`YierdisFastCommandHandler.java` 仍可编译并运行（只通过依赖变化完成，不引入包名重写）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.14）
- [√] 1.16 迁移核心测试（第一批：纯 DB）：将 `yierdis-server/src/test/java/yier/bubu/redis/db/**` 迁移到 `yierdis-core/src/test/java/yier/bubu/redis/db/**`（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core, depends on task 1.12）
- [√] 1.16.1 迁移原则：仅迁移“纯算法/数据结构/内存语义”测试；任何依赖 Netty/网络 IO 的测试必须保留在 server（depends on task 1.16）
- [√] 1.16.2 `yierdis-core/pom.xml` 补齐 test 依赖：JUnit + 必要的 test utils（避免引入 netty 依赖）（depends on task 1.16）
- [√] 1.16.3 验收：`mvn -pl yierdis-core -am test` 通过（depends on task 1.16.2）
- [√] 1.17 迁移核心测试（第二批：纯命令语义）：将 `yierdis-server/src/test/java/yier/bubu/redis/command/**` 迁移到 `yierdis-core/src/test/java/yier/bubu/redis/command/**`（verify why.md#requirement-executor-ssot-scenario-single-executor-impl, depends on task 1.13）
- [√] 1.17.1 迁移原则：command 测试中若使用 `FastTestClient`（Netty client）则保持在 server；否则迁移到 core 并改为直接调用 processor/db（depends on task 1.17）
- [√] 1.17.2 验收：`mvn -pl yierdis-core -am test` 通过（depends on task 1.17.1）
- [√] 1.18 测试工具归属审计：评估 `yierdis-server/src/test/java/yier/bubu/redis/testutil/FastTestClient.java` 是否依赖 Netty；若仅用于命令/DB 测试则迁移到 core，否则保留在 server（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.19 验收：执行 `mvn -pl yierdis-core -am test` 与 `mvn -pl yierdis-server -am test` 必须通过（verify why.md#value-proposition-and-success-metrics）

### 1D.（可选但推荐）抽取客户端模块 yierdis-client

- [√] 1.20 创建 `yierdis-client/` 模块骨架与 `pom.xml`（依赖 `yierdis-protocol` + Netty client），并在根 `pom.xml` 注册模块（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.21 迁移客户端实现：将 `yierdis-server/src/main/java/yier/bubu/redis/client/**` 迁移到 `yierdis-client/src/main/java/yier/bubu/redis/client/**`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.20）
- [√] 1.22 迁移客户端测试：将 `yierdis-server/src/test/java/yier/bubu/redis/client/**` 迁移到 `yierdis-client/src/test/java/yier/bubu/redis/client/**`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 1.21）
- [√] 1.23 server 对 client 的反向依赖清理：确保 `yierdis-server` 不依赖 `yierdis-client`（只能反向：client/bench 依赖 server 是禁止的）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）
- [√] 1.24 验收：`mvn -pl yierdis-client test` 通过，并且全仓 `mvn test` 仍全绿（verify why.md#value-proposition-and-success-metrics）

## 2. 参数体系 SSOT（Server/Bench Args Alignment）

### 2A. 统一参数模型（yierdis-args）

- [√] 2.1 在 `yierdis-args/pom.xml` 引入 picocli 并提供稳定版本管理（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 1.4）
- [√] 2.2 在 `yierdis-args/src/main/java/yier/bubu/redis/args/` 定义共享参数 SSOT：
  - `YierdisServerArgNames`（flag 名称常量）
  - `YierdisServerArgs`（picocli 模型 + 默认值 + normalizeAndValidate）
  （verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.2.1 共享参数：端口与线程（`--port`、`--ioThreads`）（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.2.2 共享参数：maxmemory（`--maxmemoryBytes`、`--maxmemoryPolicy`、`--maxmemorySamples`）（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.2.3 共享参数：cleanup/预算（`--cleanupIntervalMillis`、`--expireCleanupTimeLimitMillis`、`--evictionTimeLimitMillis`、`--noCleanup`）（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.2.4 共享参数：executor/backpressure（`--executorQueueCapacity`、`--executorDrainMillis`、`--executorMaxDrain`、`--backpressureHigh`、`--backpressureLow`）（verify why.md#requirement-executor-ssot-scenario-single-executor-impl）
- [√] 2.2.5 共享参数：off-heap（`--offheapBackend`、`--offheapMaxBytes`）（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional）
- [√] 2.3 在 yierdis-args 中实现 fail-fast 校验：low/high watermark、容量/预算必须为非负、maxmemorySamples 基础校验、bytes 单位解释一致（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.3.1 maxmemoryPolicy 校验前置：在 `YierdisServerArgs.normalizeAndValidate()` 中校验 `noeviction|allkeys-random|allkeys-lru`（避免错误延迟到 db 层）（depends on task 2.3）
- [√] 2.4 为 yierdis-args 添加单测：覆盖默认值、合法组合、非法组合（verify why.md#value-proposition-and-success-metrics）
- [√] 2.4.1 单测范围：`--help`、水位线逆序、offheapBackend 非法值、offheapBackend=none 时 offheapMaxBytes 非 0、port 超界（depends on task 2.4）
- [√] 2.4.2 验收：`mvn -pl yierdis-args -am test` 通过（depends on task 2.4.1）

### 2B. server 参数落地（替换 ServerConfig 手写解析）

- [√] 2.5（收尾）重构 `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`：消除重复校验逻辑，确保校验 SSOT 只存在于 `yierdis-args`（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 2.2）
- [√] 2.5.1 决策：`ServerConfig` 是否保留 `fromArgs`（如果保留，仅负责调用 yierdis-args + 映射；不得再校验）（depends on task 2.5）
- [√] 2.6 `yierdis-server` 启动解析入口：使用 picocli 将 args 解析为共享参数模型，再转换为 `ServerConfig`（当前实现为 `ServerConfig.fromArgs(String[])`）（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.7 兼容性：保留现有 flag 名称与语义；如需改名，必须提供 alias（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned）
- [√] 2.8 server args 回归：至少新增/更新 1 个测试覆盖“非法参数 fail-fast”（例如 watermark 逆序）并确保 `mvn -pl yierdis-server -am test` 通过（verify why.md#value-proposition-and-success-metrics）
- [√] 2.8.1 建议新增测试类：`yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`（覆盖 --help 与非法参数）（depends on task 2.8）

### 2C. bench 参数落地（bench 与 server 语义一致）

- [√] 2.9 bench 解析 SSOT：为 `yierdis-bench` 增加 picocli 参数模型（建议命名 `YierdisBenchArgs`），并明确哪些参数属于 bench 自身、哪些是 server 透传参数（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 2.2）
- [√] 2.9.1 bench 自身参数：并发/请求数/keyspace/strictReplies/是否启动内置 server（depends on task 2.9）
- [√] 2.9.2 bench 透传参数：直接复用 `YierdisServerArgs`（作为 mixin/嵌套对象），避免再维护一套默认值（depends on task 2.9）
- [√] 2.10 bench 启动 server 的参数生成：将 `ServerProcess.start()` 的拼参逻辑改为从 `YierdisServerArgs` 生成（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 2.9.2）
- [√] 2.10.1（阶段性）bench 拼参使用 `YierdisServerArgNames` 常量生成 flag（避免 flag 名称漂移）（depends on task 2.10）
- [√] 2.10.2（收尾）bench 不再持有 server 参数默认值：默认值只在 `YierdisServerArgs` 中定义（depends on task 2.10）
- [√] 2.11 更新 `scripts/bench.sh`：只保留“组合/透传/重复执行”，不再承担参数语义定义；`--help` 以 bench 自身输出为准（verify why.md#requirement-config-ssot-scenario-server-and-bench-args-aligned, depends on task 2.9）
- [√] 2.12 验收：`mvn -pl yierdis-bench -am test`（若 bench 暂无测试则至少 compile）通过，并能用最小 smoke 参数跑通 1 轮（verify why.md#value-proposition-and-success-metrics）

## 3. 命令执行/反压 SSOT（Executor Convergence）

- [√] 3.1 写清 executor 行为契约：在 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 增补说明（队列、drain、预算、水位线、拒绝与恢复、维护任务优先级）（verify why.md#requirement-executor-ssot-scenario-single-executor-impl）
- [√] 3.1.1 文档必须包含：参数含义 + 默认值来源（`yierdis-args`）+ 生效边界（例如 drainMillis=2 表示“每 tick 最多消耗 2ms”而不是 sleep）（depends on task 3.1）
- [√] 3.1.2 文档必须包含：backpressure 状态机（进入/退出条件：high/low watermark）与对读/写/flush 的影响（depends on task 3.1）
- [√] 3.2 收敛实现：审计 `yierdis-server/src/main/java/yier/bubu/redis/CommandExecutor.java` 的使用点；将其替换为 canonical 路径或仅保留为薄封装（verify why.md#requirement-executor-ssot-scenario-single-executor-impl, depends on task 3.1）
- [√] 3.2.1 收敛目标：全仓只有 1 套“执行 + 反压”实现；其他类必须是 adapter（例如为了测试或不同 IO 入口）（depends on task 3.2）
- [√] 3.2.2 验收：`mvn -pl yierdis-server -am test` 通过（depends on task 3.2.1）
- [√] 3.3 明确 handler 行为：在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java` 统一处理“队列满/反压中”的回复策略（例如返回错误、限速或断开连接，选择其一并写入文档）（verify why.md#requirement-executor-ssot-scenario-single-executor-impl, depends on task 3.1）
- [√] 3.3.1 决策：backpressure 时对客户端的行为（推荐：返回明确错误 + 限制 read，而不是静默丢弃）（depends on task 3.3）
- [√] 3.3.2 增补测试：覆盖“触发 backpressure → 回复策略生效 → 低水位恢复”（depends on task 3.3.1）
- [√] 3.4 增强测试：补齐 `CommandExecutorBackpressureTest`/`NettyCommandExecutorTest` 对 high/low watermark 与 drain 上限的覆盖（verify why.md#requirement-executor-ssot-scenario-single-executor-impl, depends on task 3.2）
- [√] 3.4.1 必测用例：queue 接近容量时，high watermark 触发与 low watermark 恢复都可重复稳定复现（depends on task 3.4）
- [√] 3.4.2 必测用例：`executorMaxDrainCommands` 与 `executorDrainMillis` 的优先级/短路关系（谁先达到谁停止）（depends on task 3.4）
- [√] 3.5 增强测试：补齐维护任务（expire/eviction budget）与普通命令的时序/公平性测试（verify why.md#requirement-executor-ssot-scenario-single-executor-impl, depends on task 3.4）
- [√] 3.5.1 必测用例：维护任务不会饿死普通命令；普通命令也不会饿死维护任务（至少在有限时间窗口内有上界）（depends on task 3.5）
- [√] 3.6 验收：`mvn -pl yierdis-server -am test` 必须通过，且 backpressure 测试不 flaky（建议重复运行 5 次）（verify why.md#value-proposition-and-success-metrics）

## 4. Off-heap API 去 Netty 依赖（Decouple Netty from offheap-api）

### 4A. 设计并落地 Netty 无关 API

- [√] 4.1 在 `yierdis-offheap/api` 新增 Netty 无关的写入抽象（例如 `YierdisBytesSink`），用于替代 `ByteBuf`（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional）
- [√] 4.1.1 接口最小化：只包含“顺序写入字节”的能力（例如 `writeByte`/`writeBytes`），不引入 Netty/IO 概念（depends on task 4.1）
- [√] 4.1.2 提供基础实现：`byte[]`/`ByteBuffer` sink（用于单测与非 Netty 后端）（depends on task 4.1.1）
- [√] 4.2 修改 `yierdis-offheap/api/.../YierdisOffHeapSlice.java`：将 `writeTo(ByteBuf out)` 改为 `writeTo(<sink>)`（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.1）
- [√] 4.2.1 扫描实现点：定位所有 `writeTo(ByteBuf)` 调用/实现并逐一替换（depends on task 4.2）
- [√] 4.3 修改 `yierdis-offheap/api/.../YierdisOffHeapBuf.java`：将 `setBytes(..., ByteBuf, ...)` 改为 Netty 无关签名（例如 `byte[]/ByteBuffer/<view>`）（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.1）
- [√] 4.3.1 决策：新签名优先使用 `byte[]` + offset/len 或 `ByteBuffer`（避免引入额外 view 类型）（depends on task 4.3）
- [√] 4.3.2 扫描实现点：定位所有 `setBytes(..., ByteBuf, ...)` 调用/实现并逐一替换（depends on task 4.3.1）
- [√] 4.4 更新 `yierdis-offheap/api/pom.xml`：移除对 `io.netty:netty-all` 的依赖（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）

### 4B. 适配与实现更新（netty/unsafe/foreign 后端）

- [√] 4.5 在 `yierdis-offheap/netty` 新增 `ByteBuf` -> `<sink>` 的 adapter（或提供 `<sink>` 的 Netty 实现），并确保 netty 依赖只存在于 netty 模块（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.1）
- [√] 4.5.1 adapter 验收：slice/buf 写入行为与旧 ByteBuf 路径逐字节一致（可复用/迁移 `RespWriterSliceTest` 的断言）（depends on task 4.5）
- [√] 4.6 更新 `yierdis-offheap/netty/.../YierdisNettyOffHeapAllocator.java` 以适配新 API（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）
- [√] 4.7 更新 `yierdis-offheap/unsafe/.../YierdisUnsafeOffHeapAllocator.java` 以适配新 API（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）
- [√] 4.8 更新 `yierdis-offheap/foreign/.../YierdisForeignOffHeapAllocator.java` 以适配新 API（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）
- [√] 4.9 更新 `yierdis-offheap/api/src/test/java/.../YierdisOffHeapAllocatorContractTest.java`：替换 ByteBuf 相关断言为 `<sink>`/新签名（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）
- [√] 4.9.1 增补回归：覆盖至少 1 个“写入后读取一致”的用例（避免 sink 实现漏写/写错顺序）（depends on task 4.9）
- [√] 4.10 验收：`mvn -pl yierdis-offheap -am test` 通过；`mvn -pl yierdis-offheap,yierdis-server -am test` 通过（verify why.md#value-proposition-and-success-metrics）

### 4C. core/server 使用点更新

- [√] 4.11 审计并更新使用点：在 core/server 中替换对旧 off-heap API 的调用（尤其是 slice 写出与 setBytes 输入路径）（verify why.md#requirement-offheap-decouple-netty-scenario-netty-adapter-is-optional, depends on task 4.2,4.3）
- [√] 4.11.1 重点路径：`yierdis-protocol` 的 `RespWriter`（slice 写出）必须迁移到新 sink API（depends on task 4.11）
- [√] 4.11.2 重点路径：`yierdis-core` 的 offheap 读写（buf setBytes / slice 复制）必须迁移（depends on task 4.11）
- [√] 4.11.3 验收：`mvn -pl yierdis-protocol,yierdis-core,yierdis-server -am test` 通过（depends on task 4.11.2）
- [√] 4.12 保持可解释的内存统计：确保 maxmemory 统计口径在 heap + off-heap 下仍可解释（必要时补齐/更新 `MaxmemoryEvictionTest`）（verify why.md#requirement-module-boundary-ssot-scenario-extract-protocol-and-core）

## 5. 协议一致性（Server/Client/Bench Codec SSOT）

- [√] 5.1 bench 发送端复用 codec：将 `yierdis-bench` 中 RESP 写入逻辑替换为 `yierdis-protocol` 的 `RespWriter`（允许 bench 依赖 Netty buffer/codec 但不要引入 Netty 事件循环）（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）
- [√] 5.1.1 增加依赖：`yierdis-bench/pom.xml` 引入 `yierdis-protocol`（depends on task 5.1）
- [√] 5.1.2 抽 adapter：在 bench 内部提供“OutputStream/byte[]/ByteBuf”到 `RespWriter` 的桥接（depends on task 5.1.1）
- [√] 5.1.3 删除/封存旧实现：bench 自实现 RESP writer 只允许保留在历史目录或测试对比中（depends on task 5.1.2）
- [√] 5.2 bench 接收端 strictReplies 复用 codec：用 `RespDecoder` 做最小语义校验（resp3 基础类型可跳过，但必须与 `helloagents/wiki/bench.md` 描述一致）（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec, depends on task 5.1）
- [√] 5.2.1 定义 strictReplies 的“最小语义”：至少验证 RESP type + bulk length + error string（depends on task 5.2）
- [√] 5.2.2 增补测试/回归：bench strictReplies 路径跑 1 轮最小 smoke（depends on task 5.2.1）
- [√] 5.3 协议 round-trip 回归：确保迁移后的 `RespRoundTripTest` 仍覆盖 RESP2 + RESP3 最小子集关键路径（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）
- [-] 5.4（可选）bench 复用 yierdis-client：若后续需要更强一致性/复用连接管理，可让 bench 在非吞吐关键路径使用 `yierdis-client`（verify why.md#requirement-resp-codec-ssot-scenario-bench-reuses-codec）

## 6. 文档/验收（Docs & Acceptance）

- [√] 6.1 更新 `helloagents/wiki/arch.md`：补充模块图、依赖方向、核心调用链（server -> protocol -> core -> protocol -> reply）（verify why.md#impact-scope）
- [√] 6.2 更新 `helloagents/wiki/modules/command.md` 与 `helloagents/wiki/modules/db.md`：同步“SSOT 位置/依赖关系/行为契约”（verify why.md#impact-scope）
- [√] 6.3 更新 `helloagents/wiki/bench.md`：同步 bench 参数 SSOT、strictReplies 语义与一键压测方式（verify why.md#impact-scope）
- [√] 6.4 更新 `README.md`：同步启动方式、RESP2/RESP3 切换说明、off-heap 后端说明与 bench 入口（verify why.md#impact-scope）
- [√] 6.5 最终验收：根目录执行 `mvn test` 必须全绿（verify why.md#value-proposition-and-success-metrics）
- [√] 6.6 一致性审计：删除/标记弃用旧实现（bench 自实现 RESP、旧 ServerConfig 手写解析、旧 off-heap ByteBuf API），确保只有一套 SSOT（verify why.md#value-proposition-and-success-metrics）
- [√] 6.6.1 bench：删除/封存旧 RESP writer/decoder（与 5.x 对齐）（depends on task 6.6）
- [√] 6.6.2 server：确认 `ServerConfig` 不再存在“手写 args parse”逻辑（与 2.5 对齐）（depends on task 6.6）
- [√] 6.6.3 offheap：确认 `yierdis-offheap/api` 不再依赖任何 Netty class（与 4.x 对齐）（depends on task 6.6）
- [√] 6.7 更新 `helloagents/CHANGELOG.md`：记录本次模块拆分与参数体系 SSOT（以及后续 executor/offheap/bench codec 变更）（verify why.md#impact-scope）

## 7. 方案包生命周期（Plan → History）

- [√] 7.1 任务状态回写：在本 `task.md` 中将已完成任务标记为 `[√]`，失败标记为 `[X]`，并补充失败原因（depends on task 6.5）
- [√] 7.2 迁移方案包到历史：将 `helloagents/plan/202601142007_architecture_refactor/` 移动到 `helloagents/history/2026-01/`（保留目录名不变）（depends on task 7.1）
- [√] 7.3 更新 `helloagents/history/index.md`：追加本方案包的条目（包含时间、目标、关键改动、验收命令）（depends on task 7.2）
