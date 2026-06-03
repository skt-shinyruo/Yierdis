# 命令解析与分发

本文解释 Yierdis 如何从 `ExecutionRequest` 走到 `CommandSpec`、参数解析、事务分支、命令实现和错误回包。

## 入口和边界

命令分发真正开始之前，请求已经经过了三层收敛：

```text
RESP bytes
  -> RespCommandRequest
  -> RespExecutionAdapter
  -> ExecutionRequest
  -> YierdisFastCommandHandler
  -> CommandExecutor
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
```

这里最重要的边界是：

- `YierdisFastCommandHandler` 只负责提交，提交失败时在 I/O 边界直接回 `ERR busy <reason>`，不会进入命令层。
- `CommandExecutor` 只负责 owner-thread 调度、budget 和关闭保护，不解释命令语义。
- `DefaultYierdisEngine` 把 `Session` 收窄成命令层需要的 capability，并把 `ExecutionRequest` 和 `ReplyWriter` 交给 processor。
- `YierdisFastCommandProcessor` 只消费 transport-neutral 的 `ExecutionRequest`，不接触 RESP DTO，也不拼协议字节。
- command handler 只通过 `ReplyWriter` 写 Redis reply 语义，不直接写 `+OK\r\n` 这类 wire bytes。

所以这条链里“提交失败回错”和“命令解析执行”是两段不同责任：前者属于 server/executor 边界，后者才属于 command-kernel。

## `CommandRegistry` 和 `CommandSpec`

`CommandRegistry` 是命令名到 `CommandSpec` 的 SSOT。构造 `YierdisFastCommandProcessor` 时会先注册 `TransactionCommands`，再注册额外 `CommandModule`。

lookup 不是用 `Map<String, ...>` 在热路径里做字符串分配。`CommandRegistry` 在注册阶段把命令名标准化成 ASCII upper-case bytes，并构造 open-addressed hash table；运行时直接对 `ExecutionRequest argv[0]` 做 ASCII case-insensitive 比较。

`CommandSpec<T>` 是单条命令的统一形状，固定包含四件东西：

- parser：把 `ExecutionRequest` 解析成 typed command，或保留为 `ExecutionRequest`
- handler：执行 parsed command
- `CommandDescriptor`：记录 arity 和 key 位置信息
- `disallowedInMultiError`：声明这个命令在 `MULTI` 中是否禁止

因此 `CommandSpec` 同时控制：

- 命令如何被找到
- 命令如何校验参数
- 命令如何执行业务逻辑
- 命令在事务中是允许入队还是直接报错

`CommandSupport` 不是 registry 的一部分，但它是内置命令的公共工具箱：参数读取、DB routing、常用 reply/error helper 都在这里收敛。

## `YierdisFastCommandProcessor.execute(...)` 主流程

`YierdisFastCommandProcessor.execute(...)` 的顺序很短，但每一步都带着边界含义：

1. 检查 `argc <= 0` 或 `argv[0]` 为空，直接回 `ERR empty command`
2. 提前拒绝非法 null bulk string，只有 `PING` / `ECHO` 的单 message 参数允许为 null
3. 把事务排队逻辑委托给 `TransactionQueuePolicy.queueIfNeeded(...)`
4. 进入 `CommandExceptionTranslator.run(...)`
5. 用 `registry.spec(request)` 查表
6. 找不到命令时回 `ERR unknown command ...`
7. 通过 `CommandChangeEmitter.execute(...)` 包住真正的 `executeSpec(...)`
8. `executeSpec(...)` 先 `spec.parse(request)`，解析成功后再 `spec.executeParsed(...)`

可以简化成：

```text
sanity checks
  -> transaction queue policy
  -> exception translator
  -> registry lookup
  -> change emitter gate
  -> parse
  -> execute handler
```

这个主流程刻意保持薄：

- 事务排队不写进 processor 主体里
- 异常翻译不写进各个 handler 里
- change event gate 不写进各个命令实现里

这样 command-kernel 小组件的职责边界清楚，测试也能直接针对每个策略层写。

## 参数解析、arity 和 parse error

参数解析不是 handler 里零散 `if/else` 的集合，而是 `CommandSpec.parse(...)` 的固定前置阶段。

解析期常见组件有：

- `ArgReader`：基于 `ExecutionRequest` 读取参数、比较 literal、解析 long
- `CommandParsers`：把常见 arity rule 和 mapper 收敛成 `CommandParser<T>`
- `CommandParseError`：统一 wrong arity、syntax、integer out of range 和 custom message

`CommandParseError.toReplyMessage()` 是命令层错误文案的集中出口，例如：

- `ERR wrong number of arguments for '<cmd>' command`
- `ERR syntax error`
- `ERR value is not an integer or out of range`

这意味着：

- parse error 在 handler 执行前就结束
- 事务入队前也复用同一套 parser
- 命令实现不需要各自再维护一套 arity 文案

对于 typed command，handler 拿到的是 parse 后的对象；对于简单命令，`CommandSpec<ExecutionRequest>` 仍然可以直接把原始 argv 视图传下去。

## 未知命令、空命令和错误翻译

命令分发链上的错误不是同一种错误。

### 空命令和 null bulk string

这两类错误在 `YierdisFastCommandProcessor` 最前面直接处理：

- `argc <= 0` 或 `argv[0]` 为空：`ERR empty command`
- 非法 null bulk string：`ERR Protocol error: null bulk string`

这里的 null bulk string 特判只给 `PING` / `ECHO` 的单 message 参数开口子，避免更深层的 DB 或命令逻辑看到意外 null。

### 未知命令

查表失败时通过 `CommandRequestSupport.unknownCommandMessage(request)` 构造 Redis 风格错误。

### handler / DB 抛出的运行时异常

这类异常不由每个 handler 自己翻译，而是统一交给 `CommandExceptionTranslator`。所以 handler 的职责是表达命令语义，而不是自己拼一整套错误恢复协议。

### 协议错误不在这里处理

RESP frame 级别的 protocol error 发生在 decoder / Netty handler 边界，由协议层回错并关闭连接；它不是 command-kernel 的责任。

## 事务排队前的复用规则

`MULTI` 状态下，普通命令不会立刻执行，但也不是“原样塞进队列以后再说”。`TransactionQueuePolicy` 会复用与即时执行几乎同一套前置校验。

顺序是：

1. 取 `ctx.transactionSession().transaction()`
2. 如果当前不在事务里，或命令本身是 `MULTI/EXEC/DISCARD`，则不走排队策略
3. 用 `registry.spec(request)` 查表
4. 找不到 spec：标记 transaction aborted，直接回 unknown command
5. `spec.disallowedInMultiError()` 非空：标记 aborted，直接回该错误
6. `spec.parse(request)` 预解析；parse error 同样会标记 aborted
7. `tx.tryEnqueue(request)` 真正入队；成功回 `QUEUED`

因此“事务排队前的复用规则”是：

- lookup 复用同一个 `CommandRegistry`
- 参数校验复用同一个 `CommandSpec.parse(...)`
- 事务里禁止的命令仍由 `CommandSpec` 描述

真正保存进队列的也不是原始请求引用，而是 `EngineSession.DefaultTransactionState.tryEnqueue(...)` 里的 `ByteArrayExecutionRequest.copyOf(request)` 快照。队列保存的是后续可 replay 的执行请求，不是另一套内部 IR。

## change observer / mutation gate

`YierdisFastCommandProcessor` 不直接在执行后无条件发 change event。它通过 `CommandChangeEmitter.execute(...)` 包住 `executeSpec(...)`，只在真实 mutation outcome 出现后才通知 observer。

这条规则有两个直接后果：

- read-only command 即使执行成功，也不会产生用户变更事件
- 事务里 `MUTATE` 命令在 `QUEUED` 阶段不会产生事件，只有 `EXEC` 真正 replay 到 handler 时才会产出事件

这也是 `YierdisFastCommandProcessorPolicyTest` 里 replay 事件测试保护的重点：不能把“入队成功”误当成“已经发生真实变更”。

## 相关测试

- `YierdisFastCommandProcessorPolicyTest`：事务排队、abort、replay、change event gate
- `YierdisFastCommandProcessorRegistrationTest`：registry / module 注册面
- `YierdisFastCommandProcessorModuleTest`：命令模块装配面
- `YierdisFastCommandProcessorArchitectureTest`：processor 不越界拥有 transaction / exception 细节
- `CommandSupportFastPathTest`：常用 helper 的 fast path 约束
- `DefaultYierdisEngineTest`：engine 到 processor 的桥接
- `NettyExecutionAdapterIntegrationTest`：handler submit reject、close-after-reply 等 Netty 边界

事务状态机和 replay 主链的完整展开见 [`transaction-and-replay.md`](./transaction-and-replay.md)。
