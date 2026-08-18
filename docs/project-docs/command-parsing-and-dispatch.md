# 命令解析与分发

本文解释 Yierdis 如何从 transport-neutral 的 `ExecutionRequest` 完成查表、参数解析、事务 preflight、命令准备、执行和语义结果渲染。

## 入口和边界

请求进入 command-kernel 时已经完成 RESP 解码、reply slot 注册和 executor admission：

```text
RESP bytes
  -> RespRequestDecoder
  -> RetainedRespExecutionRequest / ExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

这段链路的责任划分是：

- `NettyExecutionRequestIngress` 只负责 reply slot 对齐、admission 和提交所有权；
- `CommandExecutor` 负责 owner-thread 调度、reply capacity、prepared validation、执行、渲染和关闭保护；
- `CommandDispatcher` 负责命令查表、统一前置校验、事务分支和预期 command error；
- `CommandHandler.parse(CommandArgs)` 只解释 argv，返回 `Function<CommandSession, PreparedCommand>`，不读取 session、DB 或 server provider；
- dispatcher 对返回的函数调用 `apply(session)`，此时才能读取连接态、访问 DB、准备 mutation 或获取 streamed reply source；
- `PreparedCommand.execute(session)` 在预留成功后完成可见动作并返回 `CommandResult`；
- `RedisReplyRenderer` 是语义 `RedisReply` 到 RESP-facing `RedisReplyWriter` 的唯一普通命令出口。

command package 不依赖 `RedisReplyWriter`。命令通过 `RedisReply` 描述标量、聚合或延迟 payload；writer 只是 renderer 的协议端口。

## `CommandRegistry` 和 `CommandSpec`

`CommandRegistries.dispatcher(...)` 创建 registry 与 dispatcher，先注册 `MULTI/EXEC/DISCARD`，再注册注入的 `CommandModule`，最后 seal registry。生产 composition root 传入 `DefaultCommandModules` 和 `ServerCommandModule`。

`CommandRegistry` 保存命令名到 `CommandSpec` 的单一映射。注册阶段拒绝重复名称；seal 后查表只读，继续注册会失败。dispatcher 从 `argv[0]` 生成精确的 upper-case ASCII 名称并做主路径 lookup；`COMMAND` 等 metadata 查询使用 registry 提供的规范化查询 API。

每个 `CommandSpec` 只有两部分：

- `CommandSyntax`：规范化命令名、`CommandArity`、`CommandKeySpec` 和 `TransactionPolicy`；
- `CommandHandler`：接收 `CommandArgs`，返回 `Function<CommandSession, PreparedCommand>`，解析失败时抛 `CommandParseException`。

这个函数是解析结果与 session/DB 准备之间的边界。调用 `apply(CommandSession)` 后，返回带 reservation shape、validation、execution 和可选 owner 的 `PreparedCommand`。

## `CommandDispatcher.prepare(...)` 主流程

dispatcher 的实际顺序是：

1. 拒绝零参数、null command name 或空 command name，准备 `ERR empty command`；
2. 把 `argv[0]` 严格转换为 upper-case ASCII；非 ASCII 名称按 unknown command 处理；
3. 检查 null bulk argument；当前只有二参数 `PING` 和 `ECHO` 的 message 可以为 null；
4. 用 upper-case name 从 `CommandRegistry` 取得 `CommandSpec`；
5. 创建 `CommandArgs`，先调用 `spec.syntax().arity().validate(...)`；
6. 若 transaction active，根据 `TransactionPolicy` 选择禁止、排队或 transaction-control 分支；
7. 普通执行调用 `spec.handler().parse(args)` 得到准备函数；
8. 调用 `prepareFunction.apply(session)` 得到 `PreparedCommand`；
9. executor 根据其 reservation shape 完成 `reserve -> validate -> execute(session)`；
10. execute 返回 `CommandResult`，executor 用 `RedisReplyRenderer` 统一渲染。

`CommandParseException` 被转换成语义 error reply；prepare 阶段抛出的 `WrongTypeException` 与 `YierdisCommandException` 也会变成 command error。其他未预期异常继续交给 executor 的 terminal failure 路径，不能被误报为确定的业务失败。

空命令、unknown command、illegal null 和 parse error 在 transaction active 时都使用 aborting prepared action：只有该错误回复获得容量并进入执行后，transaction 才被标记 aborted。这保持了所有 session mutation 都发生在预留之后。

## 参数解析集中在 `CommandArgs`

`CommandArgs` 是 argv、ASCII 和整数解析的统一 helper：

- `argc()`、`isNull(...)`、`length(...)` 暴露请求形状；
- `slice(...)` 提供不复制的 `BytesSlice`；
- `bytes(...)`、`byteArraysFrom(...)` 取得稳定参数视图；
- `is(index, literal)` 做 ASCII case-insensitive literal 比较；
- `longAt(...)`、`nonNegativeLongAt(...)`、`positiveLongAt(...)`、`intClampedAt(...)` 集中整数规则；
- `request()` 只在需要 retain 原请求或构造延迟参数 reply 时使用。

`CommandArity` 在 handler 前表达 exact、min、range、one-of 和 pair-tail 规则。命令自身更细的 option、subcommand、score 或 cursor 语法由 handler 解析，并通过 `CommandParseException(replyMessage)` 返回准确的 Redis 风格错误。

常见错误文案包括：

- `ERR wrong number of arguments for '<cmd>' command`；
- `ERR syntax error`；
- `ERR value is not an integer or out of range`；
- 命令家族定义的专用 parse error。

parse 阶段不得调用 session、DB router、server info provider 或 slow-command governor。`CommandParseIsolationTest` 用所有默认命令的 fixture 集合验证这一点，`ServerCommandParseIsolationTest` 覆盖 `HELLO/INFO/STATS`，事务控制命令也有独立 parse-isolation 覆盖。

## parse、prepare 和 execute 为什么分开

三个阶段分别回答不同问题：

```text
parse(CommandArgs)
  -> argv 是否可解释，并返回 Function<CommandSession, PreparedCommand>

apply(CommandSession)
  -> 当前连接态和 DB 状态下需要什么 ReplyShape、mutation 或 source owner

execute(CommandSession)
  -> 容量已预留且 prepared state 仍有效时，提交什么动作并返回什么 CommandResult
```

只读命令可以在 prepare 时取得 DB source，并由 `PreparedCommand` 持有到渲染完成。需要 optimistic preview 的写命令可以准备 `PreparedMutation`，将 `isCurrent()` 接到 `validateBeforeExecute()`，把真正的 commit 留到 execute。无需状态预读的写命令也可以返回带上界 shape 的 action，在 execute 时直接调用 DB capability。

executor 将当前 `CommandSession` 直接传给 `PreparedCommand.execute(...)`。command API 不提供 writer，command implementation 因而无法绕过 `CommandResult` 直接写协议输出。

## reply reservation 与统一渲染

`PreparedCommand.reservationShape()` 描述执行前可知的回复上界和 retained source bytes。executor 使用当前 session 的 RESP version 生成 reply plan，并向 reply slot 预留容量。

预留成功后：

1. executor 调用 `validateBeforeExecute()`；
2. `STALE` 会关闭 prepared object 并从 dispatcher 重新准备，尚未发生 mutation；
3. `VALID` 才调用 `execute(session)`；
4. execute 返回 `CommandResult(RedisReply, closeAfterReply)`；
5. executor 创建 `RedisReplyWriter`，调用 `RedisReplyRenderer.render(result.reply(), writer)`；
6. 渲染完成后再关闭 prepared owner 与 request。

`RedisReplyWriter` 不出现在 command API。正常命令不会调用 writer 的 close method；例如 `QUIT` 返回 `CommandResult.closeAfterReply(...)`，executor 根据 result flag 标记 connection 和 ordered reply slot。

## 未知命令、null argument 和运行时错误

unknown command 文案只在名称长度不超过 64 且全部为安全 printable ASCII 时回显原名；否则返回不带原始内容的 `ERR unknown command`。这避免把控制字符或转义字符带入错误流。

RESP decoder 会忠实保留 array 中的 null bulk string。dispatcher 统一执行命令级合法性判断：二参数 `PING` / `ECHO` 可以返回 null reply，其余位置出现 null 时返回 `ERR Protocol error: null bulk string`。frame 本身非法的 protocol error 仍由 decoder/ingress 处理，不进入 dispatcher。

执行期的 expected DB error 由命令 action 转成 `CommandResult.controlError(...)`。它使用预留的顶层 control capacity 替换尚未写出的结果；在 `EXEC` 数组中会降为普通 child error。执行路径若在可能产生可见 side effect 后失败，必须用 `ResultUnknownException` 标记；executor 识别该标记后关闭连接。renderer 已开始写出后失败也会进入结果未知的 terminal close，不能补发确定的业务错误。

## 事务排队 preflight

transaction active 时，dispatcher 仍先完成命令名检查、registry lookup 和 arity validation。之后按 `CommandSyntax.transactionPolicy()` 分支：

- `TRANSACTION_CONTROL`：`MULTI/EXEC/DISCARD` 立即应用自己的准备函数，不重新排队；
- `DISALLOWED_IN_MULTI`：准备 error action，并在执行时标记 transaction aborted；
- `QUEUEABLE`：调用同一个 `handler.parse(CommandArgs)` 做完整参数 preflight，但不应用其返回的准备函数。

queueable preflight 成功后，dispatcher 返回一个 maximum-shape prepared action。executor 先预留回复容量，再执行 `TransactionState.tryEnqueue(request)`：成功返回 `QUEUED`，条数或字节限制失败则返回 `ERR Transaction queue is full` 并标记 aborted。

因此排队阶段的 owner 分工很清楚：

- dispatcher 拥有查表、arity、policy 与 handler parse；
- `TransactionState` 拥有 retained request 和 queue limits；
- 准备函数应用、DB read、mutation preparation 与 execution 全部推迟到 `EXEC` replay；
- executor 仍拥有 reply reservation、execution scope 和 renderer。

详细的 drain、child ownership 和 replay 见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## streamed reply source

DB read API 可以返回 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。`DbReplies` 将它们转换成带 payload length、retained memory charge 和 synchronous emitter 的 `RedisReply`；命令用 `PreparedCommands.owned(...)` 把 source 生命周期挂到 prepared command。

renderer 调用 emitter 时 source 仍然有效，渲染后 executor 才关闭 prepared owner。`EXEC` 会保留每个 child prepared command，直到外层 aggregate 已经渲染，再按逆序关闭 child owner 与 drained request。命令结果本身不负责异步写 transport，也不把 native pin 转交给 Netty event loop。

## DB 提交边界

reply capacity 成功后，executor 把当前 `CommandSession` 直接传给 `PreparedCommand.execute(...)`。写入和 flush 操作直接来自本次路由得到的 `DbEngine`；只读路径不会提前访问未使用的 mutation capability。

真正的 storage 与 ledger 提交由 `YierdisDbMutationExecutor` 持有。parse error、unknown command 和 `QUEUED` 不进入 mutation executor；条件写 no-op 则由 prepared mutation 返回 unchanged outcome。

`EXEC` 的每个 child 都把当前 `CommandSession` 直接传给 `execute(...)`，继续走相同的 DB mutation path。事务入队成功不代表 mutation 已提交。

## 相关测试

- `CommandDispatcherTest`：查表、arity、parse/prepare 顺序、事务 preflight、replay、result 与 owner 清理；
- `CommandRegistryTest`：注册、seal、重复命令和 metadata lookup；
- `CommandParseIsolationTest`、`ServerCommandParseIsolationTest`：所有生产 handler 的 parse isolation；
- `ReplyPreflightCommandTest`：reply shape、容量拒绝、state-dependent reply 和 semantic source；
- `RedisReplyRendererTest`：所有语义 reply variant 到 writer 的集中映射；
- `CommandExecutorTest`：reserve、validate、execute、render、close-after-reply 和 terminal cleanup。
