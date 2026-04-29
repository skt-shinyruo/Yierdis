# Development Navigation

本文不是再讲一遍架构，而是直接回答一个更实际的问题：

“如果我要改某一类需求，应该先打开哪些文件，沿着哪条链往下追？”

## 先记住 4 条工作规则

### 1. 改命令语义，先看命令层，再追到 DB 能力边界

不要一上来就冲进 `YierdisDb` 大类。更高效的顺序通常是：

1. 命令注册和参数解析
2. `DbReads` / `DbWrites`
3. 对应的 `*Ops`
4. 必要时再看 `YierdisDb` 和底层 value 结构

### 2. 改 server 行为，优先停留在 `yierdis-server`

除非你确认要变的是 transport-agnostic 的 core 能力，否则不要为了省事把 server 细节塞进 core。

### 3. 改协议，只走 protocol 车道和 server 适配层

如果问题是 request / reply 格式、编解码、decoder 行为，不要去改 `core-command` 或 `core-db`。

### 4. 先找最接近的测试，再改实现

Yierdis 有不少针对行为回归、边界和架构的测试。先找最接近的测试，往往比先看实现更省时间。

## 任务 1：改 `SET` 语义

这是最典型的一类需求。

### 先看哪里

- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`

### 如果只是加一个 `SET` 选项

先判断这个选项属于哪一类：

- 模式类：类似 `NX/XX`
- 过期类：类似 `EX/PX/EXAT/PXAT/KEEPTTL`
- 返回值类：类似 `GET`
- 真正影响 DB 写语义的类

对应的改法一般是：

1. 先改测试
2. 在 `StringCommands.set(...)` 里解析新选项
3. 必要时扩 `SetMode` 或 `ExpireOption`
4. 如果会改变 DB 写入语义，再改 `YierdisStringOps.set(...)`
5. 如果会影响 TTL / 生命周期，再继续看 `YierdisDbKeyLifecycle`

### 初学者最好先建立的 `SET` 代码心智模型

可以先把 `SET` 看成 5 层协作：

1. `StringCommands.set(...)`
   负责“读懂用户输入”
2. `CommandSupport`
   负责“按当前连接路由到正确 DB”
3. `StringWriteOps`
   负责“定义命令层眼里的 string 写能力”
4. `YierdisStringOps.set(...)`
   负责“把 string 写语义落成真正 mutation”
5. `YierdisDbMutationExecutor + YierdisDbKeyLifecycle`
   负责“让 mutation 在内存记账、TTL 和 key 生命周期保护下执行”

如果你先把这 5 层分清，再去改 `SET` 选项，通常不会一下子掉进 `YierdisDb` 的大类里迷路。

### 建议先看的测试

- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/Milestone1CompatTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java`

读测试时推荐的顺序是：

1. 先看正常路径
2. 再看冲突选项和错误信息
3. 最后看 TTL / maxmemory / wrong-type 的边界行为

## 任务 2：新增一个简单命令

### 先判断命令属于哪一层

如果命令是 transport-agnostic 的，通常放 `core-command`。

例如：

- 连接类命令
- 纯命令层 / DB 层命令
- 不依赖 server 运行时观测和构建信息的命令

如果命令明显依赖 server 观测或构建信息，通常放 `server`。

例如：

- `HELLO`
- `INFO`
- `STATS`

### 新增 transport-agnostic 命令的常见入口

- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- 对应的 `*Commands.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`

对初学者来说，一个“简单命令”的最小逻辑通常只有三步：

1. 在对应 `*Commands.java` 的 `register(...)` 里注册命令名和 descriptor
2. 写一个 handler，参数从 `ExecutionRequest` 里读
3. 用 `ctx.out()` 回包

### 新增 server-facing 命令的常见入口

- `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`

如果你拿不准命令该放哪层，可以先问自己：

- 这个命令离开 Netty server / runtime observability 还能成立吗？

如果答案是“能”，通常优先放 `core-command`。
如果答案是“不能”，通常放 `server`。

### 如果命令需要读 DB

先看当前 `core-api` 是否已经有需要的能力接口。

最常用入口：

- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbReads.java`
- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/KeyspaceReadOps.java`
- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MemoryOps.java`

如果接口已经有能力，就直接从命令层通过 `support.dbReads(ctx)` 调。

如果没有，就按下面顺序加：

1. 先加 `core-api` 接口
2. 再加 `core-db` 实现
3. 再回到命令层调用

不要在 `core-command` 里直接 import `YierdisDb`。

初学者这里最容易犯的错是：

- 看到 `YierdisDb` 里已经有方法，就直接去命令层调 `YierdisDb`

但这个项目故意要求你先经过 `core-api`，原因是：

- 命令层依赖的是能力边界
- 不是某个具体实现类的私有方法

### 建议先看的测试

- `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`
- `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

## 任务 3：改协议

### request 侧

先看：

- `yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
- `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestEncoder.java`

### protocol -> execution bridge

只看：

- `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`

这里是 protocol DTO 转成 `ExecutionRequest` 的唯一桥。

### reply 侧

先看：

- `yierdis-server/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`
- `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1ReplyParser.java`

### 需要特别注意

- 不要让 server/core 直接构造 protocol reply model
- 回包语义 authority 仍然应该是 `ReplyWriter`

相关护栏测试：

- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`

## 任务 4：改多 DB、连接态或事务

### 会话状态入口

- `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/CommandContext.java`

`yierdis-core-contract` 目前只是临时兼容桥；新的执行契约源码都在 `yierdis-execution-api`。

### 路由入口

- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

### 事务判定入口

- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`

如果你要改的是：

- `SELECT`
- `MULTI/EXEC/DISCARD`
- 事务队列容量和快照行为
- close-after-reply 之后的命令跳过策略

这些文件基本都绕不过去。

### 初学者理解事务的最短路径

如果你只想先搞懂事务是怎么工作的，建议只盯住这 3 个点：

1. `ServerSessionState`
   保存事务队列和 `dbIndex`
2. `YierdisFastCommandProcessor`
   决定命令是立刻执行还是 `QUEUED`
3. `TransactionCommandTest`
   验证 `MULTI/EXEC/DISCARD` 的真实行为

先把这三点读通，再回头看其它连接态细节。

## 任务 5：改 TTL、maxmemory、off-heap

### TTL / 生命周期

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationManager.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`

### memory accounting / eviction

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java`

### off-heap / FFM

- `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmKeyspace.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmExpireIndex.java`
- `docs/ffm-usage.md`
- `docs/offheap-copy-behavior.md`

### 建议先看的测试

- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`

## 任务 6：改执行器、队列、背压

### 总入口

- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`

### 提交路径

- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`

### drain 路径

- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`

### 通用背压算法

- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureController.java`

### 建议先看的测试

- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`

### 初学者理解背压的最短路径

如果你第一次看这部分，建议不要一上来同时读 5 个类。

更好的顺序是：

1. `NettyCommandExecutor`
   先看有哪些协作者被组装起来
2. `NettyCommandSubmitter`
   再看入队失败和进入背压的条件
3. `NettyCommandDrainLoop`
   再看执行后如何释放预算和恢复连接
4. `ExecutorBackpressureController`
   最后再看通用的 enter / exit / recovery 逻辑

这样更容易看清“发现压力”和“恢复压力”分别发生在哪。

## 任务 7：改 INFO / STATS / 可观测性

### server-facing 命令入口

- `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`

### 数据汇总和输出

- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java`

如果你改的是：

- `INFO`
- `INFO yierdis`
- `STATS`
- keyspace / memory 统计

通常就是从这组文件开始。

## 推荐的最小工作流

如果你已经明确要改一个需求，建议用下面的最小工作流：

1. 先找到最贴近的测试
2. 确认它属于 protocol / command / db / server 哪一层
3. 只沿这条链往下追到第一个真正改状态的点
4. 修改实现
5. 先跑最窄测试，再跑更大范围

如果你是初学者，建议在第 3 步里再加一个小动作：

- 先写一句话描述“真正改状态的第一个点在哪”

例如：

- `SET`：真正改状态的第一个点是 `YierdisStringOps.set(...)`
- 协议长度头：真正改状态的第一个点是 `CustomRequestDecoder.tryReadLengthHeader(...)`
- 事务排队：真正改状态的第一个点是 `ConnectionTransactionState.tryEnqueue(...)`

这会强迫你先找到正确入口，再开始改。

## 新人最值得先收藏的文件

- `README.md`
- `docs/request-execution-flow.md`
- `docs/module-architecture.md`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

如果你能对这几个文件建立稳定地图，后续改需求就不会再靠猜。
