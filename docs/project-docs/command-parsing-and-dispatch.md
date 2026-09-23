# 命令解析与分发

Yierdis 从 transport-neutral 的 `ExecutionRequest` 出发，依次完成查表、参数解析、事务 preflight、命令准备、执行和语义结果渲染。这一层不碰 Netty，也不直接写协议字节。

## 请求从哪来、到哪去

请求进入 command-kernel 时已经完成 RESP 解码、reply slot 注册和 executor admission：

```text
RESP bytes
  -> RespRequestDecoder
  -> ByteArrayExecutionRequest
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

各段的责任是这样切的：

| 组件 | 拥有什么 |
| --- | --- |
| `NettyExecutionRequestIngress` | reply slot 对齐、admission、提交所有权 |
| `CommandExecutor`（内部 `CommandExecutorExecutionSupport`） | owner-thread 调度、reply capacity、prepared validation、执行、渲染、关闭保护 |
| `CommandDispatcher` | 命令查表、统一前置校验、事务分支、预期 command error |
| `CommandHandler.parse(CommandArgs)` | 只解释 argv，返回 `Function<CommandSession, PreparedCommand>` |
| `PreparedCommand` | reservation shape、validation、execution、可选 owner |

dispatcher 对 handler 返回的函数调用 `apply(session)`，这时才能读取连接态、访问 DB、准备 mutation 或获取 streamed reply source。`prepare` 返回后，executor 按 reservation shape 走 `reserve -> validate -> execute(session)`，再用 `RedisReplyRenderer` 渲染 `CommandResult`。

command package 不依赖 `RedisReplyWriter`。命令通过 `RedisReply` 描述标量、聚合或延迟 payload；writer 只是 renderer 的协议端口。command implementation 拿到的只有 `CommandSession`，没有 writer，因此绕不开 `CommandResult` 直接写协议输出。

## `CommandRegistry` 和 `CommandSpec`

`CommandRegistries.dispatcher(...)` 创建 registry 与 dispatcher，先注册 `MULTI/EXEC/DISCARD`（`new TransactionCommands(dispatcher).register(registry)`），再注册注入的 `CommandModule`，最后 `registry.seal()`。生产 composition root 传入 `DefaultCommandModules` 和 `ServerCommandModule`。

`CommandRegistry` 维护命令名到 `CommandSpec` 的一对一映射，底层是 `LinkedHashMap`，`seal()` 时 `Map.copyOf` 成不可变快照。生命周期约束都由异常表达：

- `register(...)` 遇到重复名抛 `IllegalArgumentException("duplicate command registration: " + nameUpper)`；
- seal 之后再注册抛 `IllegalStateException("command registry is sealed")`；
- seal 之前查表抛 `IllegalStateException("command registry must be sealed before lookup")`。

查表有两套入口，区别很重要：

- `specByExactUpperName(name)` 精确匹配。dispatcher 主路径用它，所以 `argv[0]` 必须与注册名逐字节相等（已转成大写 ASCII）；
- `specByUpperName(name)` 先 `trim()` 再 `toUpperCase(Locale.ROOT)`，供 `COMMAND` 这类 metadata 查询使用，允许大小写和空白宽松匹配。

`upperNamesSorted()` 返回排序后的全部命令名，`commandCount()` 返回数量。

每个 `CommandSpec` 只有两部分：

- `CommandSyntax`：`nameUpper`、`CommandArity`、`CommandKeySpec`、`TransactionPolicy`、`ReplyAdmissionRequirement`。构造时把名字 trim + 大写，并拒绝空名或非 ASCII 名（`IllegalArgumentException("command name must be non-empty ASCII")`）；`nameLower()` 提供小写形式（arity 错误文案用它）。
- `CommandHandler`：接收 `CommandArgs`，返回 `Function<CommandSession, PreparedCommand>`，解析失败时抛 `CommandParseException`。

`CommandDispatcher.replyAdmissionRequirement(request)` 单独提供一条查询：空命令名、非 ASCII 名或未注册名一律返回 `ReplyAdmissionRequirement.PIPELINED`；已注册命令返回注册时声明的约束。它**不校验 arity**，参数数量错误的已识别命令仍返回其注册约束。`EXEC` 声明 `BARRIER_UNTIL_CLEANUP`，其余命令默认 `PIPELINED`。

## `CommandDispatcher.prepare(...)` 的真实分支

`prepare(session, request)` 等价于 `prepare(session, request, true)`；事务 replay 走 `prepareExecReplay(session, request)`，即 `prepare(session, request, false)`，唯一差别是跳过事务策略分支（队列已经 drain，不必再次排队）。私有 `prepare` 的顺序是：

1. **空命令**：`argc() <= 0`、`request.isNull(0)` 或 `request.len(0) == 0` 三者任一成立，返回 `abortingError(session, "ERR empty command")`。空 RESP array（`*0\r\n`）和空字符串命令名（`*1\r\n$0\r\n\r\n`）都落在这里。
2. **名称规范化**：`exactUpperAsciiName(request)` 逐字节把 `argv[0]` 转成大写 ASCII；任一字节能值 `> 0x7f` 就返回 `null`。
3. **非法 null**：`hasIllegalNullArgument(request, nameUpper)` 逐参数检查。只有 `argc == 2` 且名称是 `PING` 或 `ECHO` 时，`argv[1]` 才允许是 null bulk string。命中则返回 `abortingError(session, "ERR Protocol error: null bulk string")`。注意非 ASCII 名称会让 `nameUpper` 为 `null`，`"PING".equals(null)` 为假，所以非法 null 在名称检查之前就报出来，不会先报 unknown command。
4. **查表**：`registry.specByExactUpperName(nameUpper)`。未命中返回 `abortingError(session, unknownCommandMessage(request))`。
5. **arity 校验**：`spec.syntax().arity().validate(spec.syntax().nameLower(), args)`，不满足抛 `CommandParseException("ERR wrong number of arguments for '<nameLower>' command")`。
6. **事务策略**：仅当 `applyTransactionPolicy && transaction.active()` 时生效，见下文「事务排队 preflight」。
7. **handler parse**：`spec.handler().parse(args)` 得到准备函数，`null` 会触发 `NullPointerException("command handler returned null")`。
8. **apply**：`invocation.apply(session)` 得到 `PreparedCommand`，`null` 触发 `NullPointerException("command invocation returned null")`。

异常分类决定了回复是否 abort 事务：

- `CommandParseException` → `abortingError(session, failure.getMessage())`。错误在 transaction active 时会 abort；
- `WrongTypeException` 与 `YierdisCommandException` → `error(failure.getMessage())`，是普通 ready error，**不** abort 事务；
- 其他未预期异常不在这里吞掉，继续交给 executor 的 terminal failure 路径，不能误报为确定的业务失败。

`abortingError` 的行为值得单独看：

```java
PreparedCommands.action(
    ReplyShapes.error(message),                 // 预留形状按该错误的实际字节算
    context -> {
        if (context.transaction().active()) {   // session mutation 推迟到执行期
            transaction.markAborted();
        }
        return CommandResult.error(message);
    });
```

空命令、unknown command、非法 null、arity/parse error 都走这条路径。只有这样，`markAborted()` 才发生在该错误回复拿到容量并进入执行之后；prepare 阶段本身不碰 session。

`error(message)` 则返回 `PreparedCommands.ready(RedisReplies.error(message))`，不注册任何 abort 动作。

## `CommandArgs` 与 `CommandArity`

`CommandArgs` 只包一个 `ExecutionRequest`，是 argv、ASCII 和整数解析的共用 helper：

| 方法 | 语义 |
| --- | --- |
| `argc()`、`isNull(i)`、`length(i)` | 暴露请求形状 |
| `slice(i)` | 返回不复制的 `BytesSlice`（`RequestSlice`，`writeTo` 时才取 bytes） |
| `bytes(i)` | `request.readOnlyByteArray(i)`，零拷贝读路径 |
| `utf8(i)` | 把 `bytes(i)` 解成 UTF-8 字符串，null 透传 |
| `is(i, literal)` | ASCII case-insensitive 长度与逐字节比较，null 参数返回 false |
| `longAt(i)`、`nonNegativeLongAt(i)`、`positiveLongAt(i)`、`intClampedAt(i)` | 集中整数规则 |
| `byteArraysFrom(i)` | 从 i 起收集 `List<byte[]>`（不可变） |
| `request()` | 只在需要 retain 原请求或构造延迟参数 reply 时用 |

整数解析走 `String2ll.parse(...)`，方言与 Redis `string2ll` 一致：拒绝 `+` 前缀、前导零等非规范写法，失败统一抛 `CommandParseException("ERR value is not an integer or out of range")`。`nonNegativeLongAt` / `positiveLongAt` 在解析成功但符号不符时复用同一文案。`intClampedAt` 用 `Math.clamp` 把越界 long 夹到 `int` 范围，供 count/limit 一类参数使用。

`CommandArity` 在 handler 之前表达参数数量规则，内部五种量词：

| 工厂 | `accepts(argc)` | 典型命令 |
| --- | --- | --- |
| `exact(n)` | `argc == n` | `MULTI`/`EXEC`/`DISCARD`、`GET` |
| `min(n)` | `argc >= n` | `SET`、`HSET` |
| `range(min, max)` | `min <= argc <= max` | `GETRANGE` 一类 |
| `oneOf(a, b, ...)` | 命中允许集合（构造时排序去重） | 参数个数只能是几个定值 |
| `pairTail(min, tailStart)` | `argc >= min && (argc - tailStart)` 为偶数 | 尾部必须成对 |

构造阶段就拒绝非法定义：`exact`/`min` 要求正数，`range` 要求 `max >= min`，`oneOf` 不允许空或重复，`pairTail` 要求 `0 <= tailStart <= min`。`redisMetadataArity()` 把量词映射成 Redis `COMMAND` 元数据约定：`EXACT` 返回正数 `n`，其余返回 `-first`（负数表示“至少”）。命令自身更细的 option、subcommand、score 或 cursor 语法由 handler 解析。

## 常见错误文案来自哪里

parse 阶段的错误文案按来源分三类：

- arity：`ERR wrong number of arguments for '<nameLower>' command`（`CommandArity.validate`）；
- 参数方言：`ERR syntax error`（handler 自己）、`ERR value is not an integer or out of range`（`CommandArgs`）、`ERR value is out of range, must be positive`（LPOP/RPOP 负 count）、`ERR bit offset is not an integer or out of range`（SETBIT/GETBIT 非法 offset）；
- 命令家族定义的专用 parse error（`CommandParseException(replyMessage)`）。

不是所有错误都在 parse 期抛出。`ERR increment or decrement would overflow` 由 `YierdisStringOps` 在执行期抛出；`WRONGTYPE Key is not a valid HyperLogLog string value.` 来自 `YierdisHyperLogLog`，也不是 handler parse。

所有 error 文字最终都过 `ReplyShapes.normalizeError`：`\r`/`\n` 替换成空格、缺少 Redis 错误前缀时补 `ERR `、超 512 字节按 UTF-8 码点边界截断。前缀判定 `hasRedisErrorPrefix` 要求首个空白分隔 token 只由 `-`、`_`、数字、大写字母组成，所以 `NOPROTO ...` 这类自带前缀的错误不会被重复加 `ERR `。

parse 阶段不得调用 session、DB router、server info provider 或 slow-command governor。`CommandParseIsolationTest` 用 `DefaultCommandModules` 的默认命令验证这一点，`ServerCommandParseIsolationTest` 覆盖 `HELLO`、`INFO`、`STATS`。

## parse、apply、execute 为什么分开

三个阶段分别回答不同问题：

```text
parse(CommandArgs)
  -> argv 是否可解释，并返回 Function<CommandSession, PreparedCommand>

apply(CommandSession)
  -> 当前连接态和 DB 状态下需要什么 ReplyShape、mutation 或 source owner

execute(CommandSession)
  -> 容量已预留且 prepared state 仍有效时，提交什么动作并返回什么 CommandResult
```

只读命令可以在 apply 时取得 DB source，并由 `PreparedCommand` 持有到渲染完成。需要 optimistic preview 的写命令可以准备 `PreparedMutation`，把 `isCurrent()` 接到 `validateBeforeExecute()`，真正的 commit 留到 execute。无需状态预读的写命令也可以返回带上界 shape 的 action，在 execute 时直接调用 DB capability。

`PreparedCommand.replyProtocolVersion()` 是给“execute 期才切换协议版本”的命令（`HELLO`）用的：默认空表示按 prepare/预留时刻捕获的 session 版本算容量；`HELLO` 在 prepare 时就声明协商后的目标版本，让容量预留和实际写出用同一个版本。

`PreparedCommands` 提供四种现成形状：`ready(result)`（结果已定，shape 取 result 的 shape）、`action(shape, fn)`、`action(shape, version, fn)`、`owned(result, owner)` / `ownedAction(shape, owner, validation, fn)`。owner 的 `close()` 由 prepared command 的 `close()` 幂等触发一次，`null` owner 时 `close()` 是空操作。

## reserve / validate / execute / render：每一步失败的后果

`CommandExecutorExecutionSupport.execute(task)` 是这条链的落地实现。它先看 `context.isClosing() || !ioAdapter.isActive(connection)`，命中就 `CONNECTION_CLOSED`：取消容量注册、关闭 prepared/request/reply、归还 budget，命令不产生任何 side effect。

之后的每一步失败后果都不同：

| 阶段 | 失败/分支 | 结果 |
| --- | --- | --- |
| prepare（`commandProcessor.apply` 抛异常） | 尚未 execute | `handleReplyExecutionFailure`：写 `-ERR internal error`、标记 close-after-reply，连接随后关闭 |
| reserve | `WAITING` | `REPLY_CAPACITY_BLOCKED`，`disableAutoRead`，命令留在队列等容量释放；**不**走清理，`prepared` 保留 |
| reserve | `CLOSED` | `CONNECTION_CLOSED`，进入 terminal 清理 |
| reserve | `TOO_LARGE` | `closeOversizedReply`：`markClosing` + 取消 reply + 关闭 transport，**不**复用槽位补错误 |
| validate | `STALE` | `closePrepared()` 后返回 `REPREPARE`；request 不关闭，由外层用同一 request 重新 prepare |
| execute | 已能确定失败（无可见 side effect） | `handleReplyExecutionFailure` |
| execute | 可能已有可见 side effect | 必须抛 `ResultUnknownException`；executor 走 `closeResultUnknown` |
| render | 尚未写出字节 | `controlError("ERR internal error")` 后关闭 |
| render | 已写出字节（`reply.hasWrittenBytes()`） | 取消 reply + 关闭 transport，不补发确定的业务错误 |

两个细节容易踩坑：

- `REPREPARE` 分支直接 `return` 而不设 `terminal`，所以 `finally` 不回收 request；外层循环会对同一 request 重新 prepare，避免把 stale 当成终态。
- `closeResultUnknown` 会按顺序做 `markClosing`、`reply.markResultUnknown()`、`reply.cancel()`、`closeConnection`，每一步的清理异常以 suppressed 方式挂在主异常上，不中断其余清理。
- 容量预留成功后 executor 先 `task.cancelCapacityRegistration()`，再进 validate/execute，避免同一 slot 被重复监听。

渲染阶段的 writer 由 `replyWriterFactory.apply(replyPlan.protocolVersion(), reply.sink())` 创建，协议版本取的是 prepare 时捕获进 `replyPlan` 的值，不是执行时再读 session——`HELLO` 因此能在一次请求内完成版本切换而不让预留和写出用错版本。

`RedisReplyWriter` 不出现在 command API。正常命令不会调用 writer 的 close method；例如 `QUIT` 返回 `CommandResult.closeAfterReply(...)`，executor 根据 result flag 标记 `connection.markClosing()` 并让有序 reply slot 处理 close。

## 未知命令、null argument 和运行时错误

unknown command 文案由 `unknownCommandMessage` 决定：名称长度 `<= 64` 且每个字节都在 `0x20..0x7e` 之间、且不是 `'` 或 `\` 时回显原名（`ERR unknown command '<name>'`）；否则返回不带原始内容的 `ERR unknown command`。这样控制字符、引号和反斜杠不会进入错误流。

RESP decoder 会忠实保留 array 中的 null bulk string。命令级合法性判断都在 dispatcher 里：二参数 `PING` / `ECHO` 可以带 null，其余位置出现 null 时返回 `ERR Protocol error: null bulk string`。frame 本身非法的 protocol error（非法长度、缺 `$`、缺终止符等）仍由 decoder/ingress 处理，不进入 dispatcher。

执行期的 expected DB error 由命令 action 转成 `CommandResult.controlError(...)`。它用预留的顶层 control capacity 替换尚未写出的结果；在 `EXEC` 数组中会降为普通 child error。执行路径若在可能产生可见 side effect 后失败，必须用 `ResultUnknownException` 标记；executor 识别该标记后关闭连接，不伪造确定结果。

## 事务排队 preflight

transaction active 时，dispatcher 仍先完成命令名检查、registry lookup 和 arity validation。之后按 `CommandSyntax.transactionPolicy()` 分支：

- `TRANSACTION_CONTROL`（`MULTI`/`EXEC`/`DISCARD`）：立即应用各自的准备函数，不重新排队；
- `DISALLOWED_IN_MULTI`：准备 `abortingError`，文案 `ERR <NAME> is not allowed in MULTI`，执行时标记 aborted；
- `QUEUEABLE`：调用同一个 `handler.parse(CommandArgs)` 做完整参数 preflight，但不应用其返回的准备函数。

queueable preflight 成功后，dispatcher 返回 `prepareRetainedRequestEnqueue` 构造的 action，预留形状是 `ReplyShapes.errorUpperBound()`（错误上限 512 字节，足以覆盖 `QUEUED` 和 queue-full 错误）。该 action 在执行期调用 `transaction.tryEnqueue(request)`：

```java
String enqueueError = transaction.tryEnqueue(request);
return enqueueError == null
        ? CommandResult.reply(RedisReplies.simpleString("QUEUED"))
        : CommandResult.error(enqueueError);   // "ERR Transaction queue is full"
```

也就是说 `tryEnqueue` 是 session mutation，必须等 executor 预留回复容量后才在 queued action 里发生；parse 阶段不读 DB、不准备 mutation、不创建 reply source。

排队阶段各 owner 的分工：

- dispatcher 拥有查表、arity、policy 与 handler parse；
- `TransactionState` 拥有 retained request 和 queue limits；
- 准备函数应用、DB read、mutation preparation 与 execution 全部推迟到 `EXEC` replay；
- executor 仍拥有 reply reservation、execution scope 和 renderer。

空队列的 `EXEC` 用 `ReplyShapes.array(List.of())` 精确预留，非空队列用 `ReplyShapes.maximum()`。详细的 drain、child ownership 和 replay 见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## streamed reply source

DB read API 可以返回 `ByteValue`、`ByteSequenceSource`、`ByteMapSource` 或 `CollectionScanWindow`。`DbReplies` 把它们转换成 `RedisReply`，带 payload length、retained memory charge 和 synchronous emitter；命令用 `PreparedCommands.owned(...)` 把 source 生命周期挂到 prepared command。

renderer 调用 emitter 时 source 仍然有效，渲染后 executor 才关闭 prepared owner。`EXEC` 会保留每个 child prepared command，直到外层 aggregate 已经渲染，再按逆序关闭 child owner 与 drained request。命令结果本身不负责异步写 transport，也不把 native pin 转交给 Netty event loop。

## DB 提交边界

reply capacity 成功后，executor 把当前 `CommandSession` 直接传给 `PreparedCommand.execute(...)`。写入和 flush 操作直接来自本次路由得到的 `DbEngine`；只读路径不会提前访问未使用的 mutation capability。

真正的 storage 与 ledger 提交由 `YierdisDbMutationExecutor` 持有。parse error、unknown command 和 `QUEUED` 不进入 mutation executor；条件写 no-op 则由 prepared mutation 返回 unchanged outcome。

`EXEC` 的每个 child 都把当前 `CommandSession` 直接传给 `execute(...)`，继续走相同的 DB mutation path。事务入队成功不代表 mutation 已提交。

## 加一个命令要动哪里

一条能跑通的新命令通常只改三处：

1. 在所属 module 的 `register(...)` 里 `registry.register(new CommandSpec(syntax, handler))`，`CommandSyntax` 声明 `nameUpper`、arity、key spec、`TransactionPolicy` 和 `ReplyAdmissionRequirement`；
2. 写 `CommandHandler` 的 `parse(CommandArgs)`，只解释 argv，返回 `Function<CommandSession, PreparedCommand>`；
3. 在 apply 阶段调 DB capability，选合适的 `ReplyShape`/`PreparedCommands` 形状。

不在 `DefaultCommandModules` 或 `ServerCommandModule` 里注册的命令，客户端查不到也执行不了。各数据族到 ops 的映射见 [`commands-and-data-model.md`](./commands-and-data-model.md)。

## 相关测试

- `CommandDispatcherTest`：查表、arity、parse/prepare 顺序、事务 preflight、replay、result 与 owner 清理；
- `CommandRegistryTest`：注册、seal、重复命令和 metadata lookup；
- `CommandParseIsolationTest`、`ServerCommandParseIsolationTest`：分别覆盖 `DefaultCommandModules` 和 `HELLO`/`INFO`/`STATS`。注意 `CommandRegistries.dispatcher` 一定会注册 `TransactionCommands`，这两份测试不加载 `MULTI`/`EXEC`/`DISCARD`；
- `ReplyPreflightCommandTest`：reply shape、容量拒绝、state-dependent reply 和 semantic source；
- `RedisReplyRendererTest`：所有语义 reply variant 到 writer 的集中映射；
- `CommandExecutorTest`：reserve、validate、execute、render、close-after-reply 和 terminal cleanup。