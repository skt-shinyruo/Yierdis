# 命令解析与分发

本文解释 Yierdis 如何从 `ExecutionRequest` 走到 `CommandDefinition`、参数解析、事务分支、命令准备和错误回包。

## 入口和边界

命令分发真正开始之前，请求已经经过了三层收敛：

```text
RESP bytes
  -> RespRequestDecoder
  -> RetainedRespExecutionRequest / ExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
```

这里最重要的边界是：

- `NettyExecutionRequestIngress` 只负责 reply slot 对齐和 executor admission：容量暂时不足时暂停输入并等待回调，request 永远不可能装入 bytes budget 时直接回错；它不执行命令语义。
- `CommandExecutor` 只负责 owner-thread 调度、budget 和关闭保护，不解释命令语义。
- `DefaultYierdisEngine` 把 `CommandSession` 和 `ExecutionRequest` 交给 processor，返回 `PreparedCommand`；reply writer 在容量预留成功后才创建。
- `YierdisFastCommandProcessor` 只消费 transport-neutral 的 `ExecutionRequest`，不接触 RESP DTO，也不拼协议字节。
- command handler 只通过 `RedisReplyWriter` 写 Redis reply 语义，不直接写 `+OK\r\n` 这类 wire bytes。

所以这条链里“提交失败回错”和“命令解析执行”是两段不同责任：前者属于 server/executor 边界，后者才属于 command-kernel。

## `CommandRegistry` 和 `CommandDefinition`

`CommandRegistry` 是命令名到 `CommandDefinition` 的 SSOT。composition root 先创建 registry 和 `YierdisFastCommandProcessor`，再注册 `TransactionCommands` 与额外 `CommandModule`。

lookup 不是用 `Map<String, ...>` 在热路径里做字符串分配。`CommandRegistry` 在注册阶段把命令名标准化成 ASCII upper-case bytes，并构造 open-addressed hash table；运行时直接对 `ExecutionRequest argv[0]` 做 ASCII case-insensitive 比较。

`CommandDefinition<T>` 是单条命令的最终注册形状，固定包含三件东西：

- `CommandSyntax`：保存规范化命令名、`CommandArity`、`CommandKeySpec` 和 `TransactionPolicy`
- `CommandParser<T>`：接收 `ArgReader`，返回 typed value 或 `CommandParseError`
- `CommandPreparer<T>`：在 reply capacity 预留前读取 DB、计算回复形状，并返回 `PreparedCommand`

因此 `CommandDefinition` 同时控制：

- 命令如何被找到
- 命令如何校验参数
- 命令如何准备后续执行单元
- 命令在事务中是允许入队还是直接报错

`CommandSupport` 不是 registry 的一部分，但它是内置命令的公共工具箱：DB routing、scratch buffer 和常用 prepared reply/error helper 都在这里收敛；参数读取由 `ArgReader` 负责。

## `YierdisFastCommandProcessor.prepare(...)` 主流程

`YierdisFastCommandProcessor.prepare(...)` 的顺序很短，但每一步都带着边界含义：

1. 检查 `argc <= 0` 或 `argv[0]` 为空，直接回 `ERR empty command`
2. 提前拒绝非法 null bulk string，只有 `PING` / `ECHO` 的单 message 参数允许为 null
3. 把事务排队逻辑委托给 `TransactionQueuePolicy.queueIfNeeded(...)`
4. 进入 `CommandExceptionTranslator.prepare(...)`
5. 用 `registry.definition(request)` 查表
6. 找不到命令时回 `ERR unknown command ...`
7. `CommandDefinition.parse(...)` 先做 arity 校验再调用 parser
8. 解析成功后调用 `CommandPreparer.prepare(...)`，返回带 `ReplyShape` 的 `PreparedCommand`

可以简化成：

```text
sanity checks
  -> transaction queue policy
  -> exception translator
  -> registry lookup
  -> parse
  -> prepare command
  -> reserve reply capacity
  -> validate and execute prepared command
```

这个主流程刻意保持薄：

- 事务排队不写进 processor 主体里
- 异常翻译不写进各个 handler 里

这样 command-kernel 小组件的职责边界清楚，测试也能直接针对每个策略层写。

## 参数解析、arity 和 parse error

参数解析不是 handler 里零散 `if/else` 的集合，而是 `CommandDefinition.parse(...)` 的固定前置阶段。

解析期常见组件有：

- `ArgReader`：基于 `ExecutionRequest` 读取参数、比较 literal、解析 long
- `CommandArity`：表达 exact、min、range、one-of 和 pair-tail 等参数个数规则
- `CommandParsers.args()`：不做额外转换的 identity parser，把同一个 `ArgReader` 交给 preparer
- `CommandParseError`：统一 wrong arity、syntax、integer out of range 和 custom message

`CommandParseError.toReplyMessage()` 是命令层错误文案的集中出口，例如：

- `ERR wrong number of arguments for '<cmd>' command`
- `ERR syntax error`
- `ERR value is not an integer or out of range`

这意味着：

- parse error 在 handler 执行前就结束
- 事务入队前也复用同一套 parser
- 命令实现不需要各自再维护一套 arity 文案

对于 typed command，preparer 拿到的是 parse 后的对象；简单命令使用 `CommandDefinition<ArgReader>`。只有需要保留参数切片、遍历 bulk payload 或创建 request 快照时，才通过 `args.request()` 取得底层 `ExecutionRequest`。

## 未知命令、空命令和错误翻译

命令分发链上的错误不是同一种错误。

### 空命令和 null bulk string

这两类错误在 `YierdisFastCommandProcessor` 最前面直接处理：

- `argc <= 0` 或 `argv[0]` 为空：`ERR empty command`
- RESP array 带进来的非法 null bulk string：`ERR Protocol error: null bulk string`

RESP 协议层现在会把 array 里的 `$-1` 忠实解成 `ExecutionRequest` 里的 null argv 元素；真正决定这些 null 是否合法的是 command-kernel。当前只有 `PING` / `ECHO` 的单 message 参数允许为 null，其余命令都会在 processor 入口被拒绝。

### 未知命令

查表失败时通过 `CommandRequestSupport.unknownCommandMessage(request)` 构造 Redis 风格错误。

### handler / DB 抛出的运行时异常

这类异常不由每个 handler 自己翻译，而是统一交给 `CommandExceptionTranslator`。所以 handler 的职责是表达命令语义，而不是自己拼一整套错误恢复协议。

### 协议错误不在这里处理

RESP frame 级别的 protocol error 由 `RespRequestDecoder` 产出，再由 `NettyExecutionRequestIngress` 使用已注册的 reply slot 回错并关闭连接；它不会进入 command-kernel。

## 事务排队前的复用规则

`MULTI` 状态下，普通命令不会立刻执行，但也不是“原样塞进队列以后再说”。`TransactionQueuePolicy` 会复用与即时执行几乎同一套前置校验。

顺序是：

1. 取 `ctx.session().transaction()`
2. 如果当前不在事务里，或命令本身是 `MULTI/EXEC/DISCARD`，则不走排队策略
3. 用 `registry.definition(request)` 查表
4. 找不到 definition：标记 transaction aborted，直接回 unknown command
5. 根据 `definition.syntax().transactionPolicy()` 处理 transaction-control 或 disallowed 分支
6. `definition.parse(request)` 预解析；parse error 同样会标记 aborted
7. `tx.tryEnqueue(request)` 真正入队；成功回 `QUEUED`

因此“事务排队前的复用规则”是：

- lookup 复用同一个 `CommandRegistry`
- 参数校验复用同一个 `CommandDefinition.parse(...)`
- 事务策略由同一个 `CommandSyntax` 描述

`EngineSession.DefaultTransactionState.tryEnqueue(...)` 通过 `request.retain()` 取得队列独立拥有的请求视图。生产网络请求共享不可变 argv 和 request-memory lease，不做第二次逐参数复制；队列保存的是后续可 replay 且最终必须关闭的 `ExecutionRequest`，不是另一套内部 IR。

## 命令记录与 DB 提交边界

命令层只负责解析、准备、执行和写回 Redis 语义，不根据 preparer 返回值推断或发布变更事件。reply capacity 预留成功后，executor 为当前请求创建 `CommandExecutionContext`；该作用域持有显式 `MutationContext`，执行结束时关闭，DB view 不从隐式 `ThreadLocal` 读取命令记录。

真正的变更发布由 DB 持有。`YierdisDbMutationExecutor` 只会在 prepared mutation 确认实际发生变化后，先向 `DbCommitPublisher` 预留记录容量，再开始可见性提交；storage 和 ledger 都提交后才把预留转换为已发布事件。这样读命令、parse error、unknown command、条件写入的 no-op 以及 `MULTI` 的 `QUEUED` 阶段都不会产生 commit-stream 事件。

`EXEC` replay 仍经 engine 和同一条 DB mutation 路径执行。每条真正提交的 mutation 都有自己的命令记录和 commit reservation；不能把“入队成功”或“handler 已返回”误当成已经提交。

## 相关测试

- `YierdisFastCommandProcessorPolicyTest`：事务排队、abort 和 replay
- `YierdisFastCommandProcessorRegistrationTest`：registry / module 注册面
- `YierdisFastCommandProcessorModuleTest`：命令模块装配面
- `YierdisFastCommandProcessorArchitectureTest`：processor 不越界拥有 transaction / exception 细节
- `CommandSupportFastPathTest`：常用 helper 的 fast path 约束
- `DefaultYierdisEngineTest`：engine 到 processor 的桥接
- `DbCommitPublisherTest`、`CommitStreamTest`：DB commit reservation、发布和 callback 生命周期
- `NettyExecutionAdapterIntegrationTest`：handler submit reject、close-after-reply 等 Netty 边界

事务状态机和 replay 主链的完整展开见 [`transaction-and-replay.md`](./transaction-and-replay.md)。
