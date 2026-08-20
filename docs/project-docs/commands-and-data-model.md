# 命令层与数据模型

本文说明命令层如何注册、解析和执行命令，以及命令语义如何映射到 DB capability、逻辑类型和内部编码。

## 命令层职责

命令层位于协议和 DB 之间。它接收 transport-neutral 的 `ExecutionRequest`，选择 `CommandSpec`，用 `CommandArgs` 解析参数，通过 handler 返回的准备函数准备 DB 操作，最后返回语义 `CommandResult`。

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

命令层不解析 RESP bytes，不直接管理 allocator、entry table 或 value root，也不写 reply sink。`RedisReplyWriter` 只存在于 executor 调用的 `RedisReplyRenderer` 一侧，是 renderer 的 RESP-facing port；command implementation 只构造 `RedisReply`。

## 分发与事务专题入口

本页保留命令抽象、命令家族、streamed result 和逻辑类型模型。

- 查表、`CommandArgs`、parse error、unknown command 和 transaction preflight 见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)；
- `MULTI/EXEC/DISCARD`、retained request、abort、replay 和 queue limit 见 [`transaction-and-replay.md`](./transaction-and-replay.md)；
- 从 Netty 到 renderer 的完整主路径见 [`request-execution-flow.md`](./request-execution-flow.md)。

## `CommandRegistry`、`CommandSpec` 和 handler

`YierdisServerBootstrap` 通过 `CommandRegistries.dispatcher(...)` 创建 `CommandRegistry` 与 `CommandDispatcher`。registry 依次接收 `DefaultCommandModules` 和 `ServerCommandModule`，其中事务控制命令由 registry helper 先注册，最后 seal。

`CommandRegistry` 是 upper-case command name 到 `CommandSpec` 的单一映射。`CommandSpec` 包含：

- `CommandSyntax`：name、`CommandArity`、`CommandKeySpec`、`TransactionPolicy` 和 `ReplyAdmissionRequirement`；
- `CommandHandler`：`parse(CommandArgs)`，成功时返回 `Function<CommandSession, PreparedCommand>`。

dispatcher 先做命令名、null argument、lookup 和 arity 检查，再调用 handler。handler 只读取 argv 并生成不可变的解析结果；它不能读取 session、路由 DB 或调用 server provider。这个限制让普通执行与 `MULTI` 入队 preflight 复用同一个 parse 行为。

handler 返回的准备函数是参数解析与状态访问之间的边界。它在 `apply(CommandSession)` 时可以根据当前 DB index 和连接状态：

- 准备带 validation 的 mutation；
- 执行只读查询并取得有所有权的 result source；
- 返回固定语义回复；
- 构造在执行阶段调用 DB capability 的 action。

无论哪种分支，最终都返回 `PreparedCommand`，由 executor 负责容量预留、validation、execution、render 和 close。

## 参数解析和错误

参数处理集中在两个层次：

- `CommandArity` 表达 exact、min、range、one-of 和 pair-tail 等 argc 规则；
- `CommandArgs` 提供 argv shape、`BytesSlice`、byte array、ASCII literal、UTF-8 和整数读取。

wrong arity 由 dispatcher 在 handler 前统一生成。命令特有的 option、subcommand、cursor、score 和 integer 约束由 handler 检查，并抛出带最终 Redis error message 的 `CommandParseException`。

parse 阶段只产生 `Function<CommandSession, PreparedCommand>`，不会访问 DB，也不会创建 reply source。对于 `MULTI` 中的 queueable command，dispatcher 会运行同一个 handler parse；成功后只 retain request 并返回 `QUEUED`，准备函数要到 `EXEC` replay 才会应用。

## 准备、执行和语义结果

`PreparedCommand` 把执行前和执行后的责任分开：

- `reservationShape()` 给出 encoded reply 与 retained source 的容量上界；
- `validateBeforeExecute()` 检查 prepare 时观察的状态是否仍可执行；
- `execute(CommandSession)` 在容量已预留时提交动作，返回 `CommandResult`；
- `close()` 归还 mutation、DB source、retained request 或其他 owner。

executor 把当前 `CommandSession` 直接传给 `PreparedCommand.execute(...)`。command API 没有 reply writer；`CommandResult` 包含语义 `RedisReply` 和 `closeAfterReply` flag。

`RedisReply.shape()` 是 sealed reply hierarchy 到 `ReplyShape` 的唯一投影权威：根接口用穷尽 switch 覆盖全部 variant，各 variant 只保存语义数据，不再各自重复 shape 映射。`ReplyShapes` 负责 shape 的构造与规范化；`RedisReplyRenderer` 负责遍历语义 reply；RESP sizer 只消费 `ReplyShape`。新增 reply variant 时，这三个职责仍应分别演进。

executor 的固定顺序是：

```text
PreparedCommand
  -> reserve reservationShape
  -> validateBeforeExecute
  -> execute(CommandSession)
  -> CommandResult
  -> RedisReplyRenderer.render(reply, RedisReplyWriter)
  -> close PreparedCommand
```

RESP2 / RESP3 的标量与 aggregate 编码由协议 writer 根据 session version 处理。`QUIT` 也不调用 writer；它返回 `CommandResult.closeAfterReply(...)`，由 executor 和 ordered reply slot 完成关闭。

## `CommandSupport` 与 DB capability

`CommandSupport` 是 built-in command 的公共边界。它持有 `YierdisDbRouter`、可选 `ServerInfoProvider` 和不可变的 `SlowCommandLimits`，并提供 DB 选择入口：

- `commandDb(CommandSession)`：prepare 和 execute 阶段都按 session 的 DB index 选择数据库。

`CommandSupport.preparedMutation(...)` 把 `PreparedMutation.isCurrent()` 接到 validation，把 mutation owner 交给 `PreparedCommand`，并在 execute 中把 expected DB error 转成 control result。命令家族通过 `DbEngine` 直接访问 typed ops、memory 查询和 flush 操作，不触碰 native handle 或 RESP bytes。

## semantic streamed reply source

只读 DB API 不必先复制完整 payload。它们可以返回：

- `ByteValue`：单个 bulk-string 或 null；
- `ByteSequenceSource`：bulk-string sequence；
- `ByteMapSource`：field/value pairs；
- `CollectionScanWindow`：带 cursor 的一次 scan window。

`DbReplies` 把这些 source 转成 `RedisReply.BulkString`、`ByteSequence`、`ByteSet` 或 `ByteMap`。语义 reply 记录 element count、payload lengths、retained source bytes 和同步 emitter；`PreparedCommands.owned(...)` 让 prepared command 持有 source。

executor 先把 source 的 retained memory 纳入 reply preflight，再执行并交给 renderer。renderer 在 command owner thread 同步调用 emitter；渲染完成后 executor 关闭 prepared command，source 才 unpin 或释放。source ownership 不会转移给 `RedisReplyWriter` 或 Netty event loop。

`EXEC` 的 streamed child 也遵守这一规则：child prepared command 持有 source，外层 transaction prepared command 持有 child，直到整个 aggregate 被 renderer 消费完才逆序关闭。

## 命令家族总览

当前命令语义是已实现范围内的 Redis-style minimum subset。准确支持面以生产注册和测试为准，表格不承诺完整 Redis command set。

| 家族 | 主要模块 | 代表命令 |
| --- | --- | --- |
| connection/server | `CoreConnectionCommands`、`ServerCommandModule` | `PING`、`ECHO`、`COMMAND`、`SELECT`、`QUIT`、`CLIENT SETINFO`、`CLIENT SETNAME`、`CLIENT GETNAME`、`AUTH`、`HELLO`、`INFO`、`STATS`、`FLUSHDB` |
| key/TTL | `KeyCommands` | `TYPE`、`MEMORY USAGE`、`MEMORY STATS`、`OBJECT ENCODING`、`KEYS`、`SCAN`、`DEL`、`EXISTS`、`EXPIRE`、`PEXPIRE`、`EXPIREAT`、`PEXPIREAT`、`PERSIST`、`TTL`、`PTTL` |
| string/bitmap | `StringCommands` | `SET`、`GET`、`STRLEN`、`APPEND`、`SETBIT`、`GETBIT`、`BITCOUNT`、`INCR`、`DECR` |
| HLL | `HllCommands` | `PFADD`、`PFCOUNT`、`PFMERGE` |
| list | `ListCommands` | `LPUSH`、`RPUSH`、`LRANGE`、`LPOP`、`RPOP` |
| hash | `HashCommands` | `HSET`、`HGET`、`HGETALL`、`HLEN`、`HDEL`、`HSCAN` |
| set | `SetCommands` | `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`、`SSCAN` |
| zset | `ZSetCommands` | `ZADD`、`ZRANGE`、`ZREVRANGE`、`ZRANGEBYSCORE`、`ZREVRANGEBYSCORE`、`ZREMRANGEBYSCORE`、`ZREMRANGEBYRANK`、`ZREM`、`ZSCAN` |
| transaction | `TransactionCommands` | `MULTI`、`EXEC`、`DISCARD` |

connection/server 命令主要操作连接态、协议协商和 runtime 信息；数据结构命令通过 DB capability 读取或修改逻辑类型。

## HSCAN、SSCAN 和 ZSCAN

集合扫描命令复用 `CollectionScanCommandSupport` 的参数规则：

```text
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
SSCAN key cursor [MATCH pattern] [COUNT count]
ZSCAN key cursor [MATCH pattern] [COUNT count]
```

`cursor` 是非负十进制值；`0` 开始迭代，返回 `0` 表示本轮结束。`MATCH` 只匹配 field/member，不匹配 hash value 或 zset score。`COUNT` 默认为 10，范围是 `1..Integer.MAX_VALUE`。option 可以换序；`NOVALUES` 只适用于 `HSCAN`。

三条命令都返回两元素 array：下一 cursor 和元素 sequence。元素形状分别为：

- `HSCAN`：`field, value, ...`，`NOVALUES` 时只有 field；
- `SSCAN`：`member, ...`；
- `ZSCAN`：`member, score, ...`。

不存在的 key 返回 cursor `0` 与空 sequence，类型不匹配返回 `WRONGTYPE`。`COUNT` 是工作量 hint，不是精确页大小。

- `HASH_PACKED`、`SET_INTSET`、`ZSET_PACKED` 的 compact 分支在初始 cursor 上过滤并物化当前稳定 heap window，通常一次返回并终止；
- `HASH_HT`、`SET_HT`、`ZSET_SKIPLIST` member table 的 dictionary 分支按物理 slot 有界扫描，空槽与 `MATCH` 过滤会使实际返回少于 count，甚至为空但 cursor 非零。

dictionary window 只 pin 被选中的 native payload，并把延迟回收量放进 `retainedSourceBytes`。renderer 发射 window 后，executor 关闭它并 unpin。compact window 使用稳定 heap byte arrays，也参与 reply admission。

这些 cursor 提供 Redis 风格弱一致迭代，不是快照或稳定顺序。并发新增、删除、rehash 和编码切换会影响可见元素，调用方应允许重复并自行去重；cursor 不能跨 DB、key 删除重建或集合生命周期长期保存。

## `StringCommands` 主路线

`SET`、`GET`、`APPEND`、`INCR` 等命令先由 handler 解析 argv，再由准备函数或 prepared action 使用 typed string ops。

- `SET` 在 prepare 阶段创建 `PreparedMutation` 与 preview reply，executor 预留和 validation 后才 commit；
- `GET` 在 prepare 阶段取得 `ByteValue`，用 semantic bulk reply 引用 source，渲染后释放；
- `APPEND`、`SETBIT` 等已知 reply 上界的动作在 execute 阶段访问 string typed ops，并返回 integer/control result。

bitmap 是 string bytes 的一种视图，因此 `SETBIT`、`GETBIT`、`BITCOUNT` 与普通 string 命令共享 `ValueType.STRING` 和 wrong-type 约束。写路径仍经过 DB mutation、TTL 和 memory ledger；读路径返回 `RedisReply`，不直接编码 RESP。

## 逻辑类型和内部编码

用户看到逻辑类型，DB 记录逻辑类型与内部编码。`ValueType` 当前包括：

- `STRING`；
- `LIST`；
- `SET`；
- `HASH`；
- `ZSET`。

`ValueEncoding` 的常见映射是：

| 逻辑类型 | 内部编码 |
| --- | --- |
| string | `STRING_INT`、`STRING_EMBSTR`、`STRING_RAW` |
| hash | `HASH_PACKED`、`HASH_HT` |
| list | `LIST_PACKED`、`LIST_QUICKLIST` |
| set | `SET_INTSET`、`SET_HT` |
| zset | `ZSET_PACKED`、`ZSET_SKIPLIST` |

`OBJECT ENCODING key` 把内部编码格式化为 Redis 风格名称，如 `int`、`embstr`、`raw`、`listpack`、`hashtable`、`intset`、`quicklist` 和 `skiplist`。

编码选择与升级由 value/DB 层拥有：

- hash 根据 entry 数或 field/value 长度从 packed 升为 hashtable；
- list 根据 compact block 约束从 packed 升为 quicklist；
- set 遇到非整数 member 或超过阈值时从 intset 升为 hashtable；
- zset 根据 entry 数或 member 长度从 packed 升为 skiplist；
- string 根据整数可解析性和长度选择 int、embstr 或 raw。

命令 handler 不手工选择编码。allocator handle、entry table、native object kind 和 off-heap payload 都属于 DB/value 层。

## HLL、bitmap 和 string

bitmap 没有独立逻辑类型，始终操作 string bytes。

HLL 也没有独立 `ValueType`。命令层由 `HllCommands` 表达语义，DB 层由 HLL typed ops 处理，但底层对象仍是 `ValueType.STRING`；payload 是否为有效 HLL 由其格式约定判断。

这允许命令家族独立演进，同时避免在主类型系统里为 bitmap 和 HLL 增加额外逻辑类型。

## 事务中的命令语义

事务状态属于每连接 `CommandSession`。生产中的 `EngineSession` 只作为该连接 session 的具体 owner，并在其 `TransactionState` 中保存 active、aborted、retained request queue 和 queue limits。

`MULTI` 后，queueable command 仍经过 registry、arity、transaction policy 和 handler parse；通过后才在 reply reservation 后 retain `ExecutionRequest` 并返回 `QUEUED`。DB preparation 与 execution 不发生在排队阶段。

`EXEC` 对 retained requests 调用同一个 dispatcher replay path，逐条得到 child `CommandResult`，聚合 `RedisReply` 后交回 executor 的单一 renderer。`DISCARD` 关闭队列并退出 transaction。

该实现是 Redis-style 的连接级排队与顺序重放，不隐含 `WATCH`、Lua 或 cluster transaction 语义。

## 新增命令时的路线

1. 确认命令 family，或实现新的 `CommandModule`。
2. 注册 `CommandSpec(CommandSyntax, CommandHandler)`，补齐 arity、key spec、transaction policy 和 reply admission requirement。
3. 在 `handler.parse(CommandArgs)` 中完成纯 argv 解析；错误抛 `CommandParseException`，不得访问 session 或 DB。
4. 返回 `Function<CommandSession, PreparedCommand>`，在 `apply(session)` 中取得当前 DB、准备 mutation/source，并构造 `PreparedCommand`。
5. 为 prepared command 给出真实 reservation shape；可变 preview 接上 validation，可见 mutation 留在 execute。
6. execute 返回 `CommandResult` 与语义 `RedisReply`；不要引用或调用 `RedisReplyWriter`。
7. streamed source 使用 `PreparedCommands.owned(...)` 或 `ownedAction(...)` 明确生命周期与 retained memory charge。
8. 补 handler parse-isolation、dispatcher、reply preflight、命令 family 和错误路径测试；server-only command 还应覆盖 server composition。

若命令暴露 encoding 或 memory 信息，还要同步检查 `OBJECT ENCODING`、`MEMORY USAGE` 和 `MEMORY STATS` 的行为。
