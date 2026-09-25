# 协议参考

Yierdis 对外只有一条 TCP 协议入口：Redis 风格的 RESP。本页覆盖这条入口的实现边界——请求字节如何变成 argv、回包如何编码、协议错误如何处理、当前注册了哪些命令、以及哪些 Redis 协议能力只是基础兼容。

## 分层边界

协议层把线上字节解析成 argv，命令层返回语义结果，两层之间不共享 RESP 字节：

| 边界对象 | 由谁产生 | 由谁消费 |
| --- | --- | --- |
| `RespDecodedMessage.Request` / `RespProtocolError` | `RespRequestDecoder` | reply admission（`RespDecodedMessageGate`） |
| `ExecutionRequest` | `NettyExecutionRequestIngress` 从变体里取出 | `CommandDispatcher.prepare(session, request)` |
| `CommandResult`（内含语义 `RedisReply`） | `PreparedCommand.execute(...)` | `RedisReplyRenderer` |
| `RedisReplyWriter` | executor 按 `ReplyPlan` 创建 | renderer 作为协议端口调用 |

命令实现不接触 RESP 字节、不接触 writer，也不自己拼回包；同一个命令实现不需要知道请求来自 RESP array 还是 inline command。

## 请求主路径

连接是在 `YierdisServerChannelInitializer` 里组装的，pipeline 顺序固定：

```text
Netty ByteBuf
  -> inboundReadCredit                (inboundReadCredit)
  -> InboundByteAccountingHandler     (inboundByteAccounting)
  -> RespRequestDecoder               (respRequestDecoder)
  -> NettyExecutionRequestIngress     (executionRequestIngress)
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
```

`RespRequestDecoder` 只负责 RESP / inline 解析、协议上限、ingress admission 和协议错误，并把结果封闭为 `RespDecodedMessage.Request` 或 `RespProtocolError`。decoder 不直接调用 ingress，而是通过 `RespDecodedMessageGate`（生产实现 `NettyReplyDecodedMessageGate`）做 handoff：gate 先给这条消息注册 reply slot，再把已注册槽位随消息一起包成 `RegisteredRespMessage` 交给 ingress。ingress 从 request 变体里取出传输无关的 `ExecutionRequest` 才提交给 executor。因此协议错误和正常请求拥有同一套 reply 槽位与 FIFO 顺序。

## RESP2 array 请求

RESP2 array/multibulk 是默认请求格式，也是 Redis 客户端通常发送的格式。数组里每个元素都是 bulk string：

- `argv[0]` 是命令名；
- `argv[1..]` 是命令参数；
- 参数按 byte array 传递，保持二进制安全；`$-1\r\n` 表示 null bulk。

例如 `GET a` 的请求字节是：

```text
*2\r\n$3\r\nGET\r\n$1\r\na\r\n
```

`RespRequestDecoder` 对 RESP2 array 的约束（括号内是拒绝时实际返回的 error 文本）：

- multibulk header 必须是 `*<argc>\r\n`，`argc` 为非负十进制整数（`ERR Protocol error: invalid multibulk length`）；
- 非 null 参数头必须是 `$<len>\r\n<body>\r\n`，以其它类型字节开头时报 `ERR Protocol error: expected '$', got other`；
- bulk length 允许 `-1` 或非负整数，不能小于 `-1`（`ERR Protocol error: invalid bulk length`）；
- 非 null bulk 的 body 后必须紧跟 `\r\n`（`ERR Protocol error: invalid bulk string terminator`）。

argc 还受两道上限约束：`RespProtocolLimits.DEFAULT_MAX_ARGS` 是 decoder 内建硬上限，超过报 `ERR Protocol error: invalid multibulk length`；`protocolMaxArgs` 是运营配置上限，超过报 `ERR Protocol error: too many arguments`。bulk length 同理：硬上限是 `RespProtocolLimits.DEFAULT_MAX_BULK_BYTES`，配置上限是 `protocolMaxBulkBytes`，两者都报 `invalid bulk length`。

连接刚建立时回包版本默认是 RESP2（`EngineSession` 字段 `respVersion = 2`），因此不执行 `HELLO` 的普通 Redis 客户端会收到 RESP2 编码的 simple string、integer、bulk string、array 和 error。

## inline command

如果请求的第一个字节不是 `*`，`RespRequestDecoder` 会按 inline command 尝试解析。它主要用于手工调试和基础兼容：

```text
PING\r\n
SET a 1\r\n
```

它和 RESP array 的区别值得记清：

- 无类型前缀。整行就是命令，不再有 `*`/`$` 结构，也没有 null bulk 表达；
- 以 CRLF 结束，按 `' '` 和 `'\t'` 切分（`InlineCommandParser.isSpace` 只认这两个字符，不含 `\v`/`\f`）；
- 支持单引号、双引号；双引号内可用 `\xHH` 十六进制转义，以及 `\n \r \t \b \a`，其他 `\x` 序列按字面字符处理；单引号内只识别 `\'`；
- 闭合引号后必须紧跟空白或行尾，否则报 `ERR Protocol error: invalid inline command`；引号不闭合报同一消息，也不会被执行；
- 空白行（全为空格/制表符）被忽略，不生成任何请求，也不占用配额。

inline command 不是二进制安全入口：空 bytes 参数、任意控制字符、需要精确表达的 payload 都应改用 RESP array。空的 RESP array（`*0\r\n`）会走到命令层，由 dispatcher 回 `ERR empty command`。

## HELLO 2 / HELLO 3

`HELLO` 用来协商当前连接的回包版本。支持的基础形式：

```text
HELLO
HELLO 2
HELLO 3
HELLO 2 SETNAME <name>
HELLO 3 SETNAME <name>
```

`HELLO 2` 把连接设为 RESP2 回包，`HELLO 3` 把连接切到基础 RESP3 reply encoding；不带版本号的 `HELLO` 只回显当前版本，不改动它。切换成功后，作为连接 session owner 的 `EngineSession` 会记录当前版本（`setRespVersion`，只接受 2 或 3，其余值抛 `NOPROTO unsupported protocol version`）。

回复使用的协议版本在 prepare/容量预留时刻读取一次并捕获进 `ReplyPlan`；命令执行返回语义 `RedisReply` 后，executor 按这份捕获值创建 `RespReplyWriter`，中央 renderer 再编码成相应 RESP 形态。这也是 `HELLO` 能在一条命令里同时协商并渲染的原因：它的准备函数用 `PreparedCommands.action(shape, targetRespVersion, action)` 显式声明协商后的目标版本，本回复的容量预留与写出都按该版本计算。

`HELLO` 返回 5 个字段：`server`、`version`、`proto`、`mode`、`role`（常量值 `yierdis`、构建版本、请求的 proto、`standalone`、`master`）。在 RESP2 下这个 reply 是 flat array，在 RESP3 下是 map。例如 `HELLO 3` 成功后，响应包含 `proto: 3`，后续 map、set、null 语义会使用 RESP3 基础编码。

限制：

- 不支持的版本，例如 `HELLO 4`，返回 `-NOPROTO unsupported protocol version`（`CommandParseException` 抛出的消息本身已带 Redis 风格前缀，renderer 不会再补 `ERR `）；
- `HELLO ... AUTH ...` 固定返回 no-password-configured 错误，因为项目没有认证配置面；
- `HELLO` 的 `TransactionPolicy` 是 `DISALLOWED_IN_MULTI`，在 `MULTI` 中报 `ERR HELLO is not allowed in MULTI`；
- 除版本号和 `SETNAME`/`AUTH` 之外的参数报 `ERR syntax error`；
- RESP3 协商只表示回包编码切换，不表示完整 Redis RESP3 客户端生态兼容。

## RedisReply 到 RESP 回包

`RedisReply` 是命令结果的语义模型。根接口的 default `shape()` 用 sealed hierarchy 上的穷尽 switch 集中完成 `ReplyShape` 投影，各 variant 不声明自己的 `shape()`。`ReplyShapes` 只管 shape 构造与规范化，`RedisReplyRenderer` 则是唯一的命令结果遍历点。命令实现只构造 `SimpleString`、`IntegerValue`、`BulkString`、`Aggregate`、`NullValue`、`Error` 等变体，renderer 再调用 `RedisReplyWriter`。所以 `RedisReplyWriter` 只是 renderer 面向 RESP encoder 的端口，并非命令 API。协议错误或 ingress admission 失败属于命令管线之外的控制回复，仍由网络边界直接编码。

错误的字节形态是 `ReplyShapes.normalizeError` 决定的，不是 handler 原样写入：先替换 CR/LF 为空格，再判断首 token 是否已是 Redis 风格前缀（只由 `-`、`_`、数字、大写字母组成），不是则补 `ERR `，最后按 UTF-8 截断到 512 字节。`RespReplyWriter.controlError` 走同一条路径，并额外把输出改记为「控制预留」容量（`ReplyReservationSink.useControlReservation()`）；simple string 走 `sanitizeSimple`，只把 CR/LF 换成空格。协议错误和控制错误在 RESP2/RESP3 下字节相同，因此 ingress 用当前 session 版本构造 writer 即可。

RESP2 下的典型映射是：

| `RedisReply` 语义 | RESP2 编码 |
| --- | --- |
| `SimpleString("OK")` | `+OK\r\n` |
| `IntegerValue(1)` | `:1\r\n` |
| `BulkString(...)` | `$<len>\r\n<body>\r\n` |
| `NullValue` | `$-1\r\n` |
| `NullArray` | `*-1\r\n` |
| `Aggregate(ARRAY, ...)` | `*<n>\r\n` |
| `Aggregate(MAP, ...)` | flat array，长度为 field/value 元素数 |
| `ByteAggregate(SEQUENCE, ...)` | array，逐元素 bulk string |
| `ByteAggregate(SET, ...)` | array，逐元素 bulk string |
| `ByteAggregate(MAP, ...)` | flat array，交替 field/value bulk string |
| `Error(message)` | `-ERR ...\r\n` 或已有 Redis error prefix |

RESP3 下，已有专属形态的语义会换成 RESP3 编码（RESP2/RESP3 差异只在 `RespProtocolVersion` 里定义一次，sizer 与 writer 都从它取值）：

| `RedisReply` 语义 | RESP3 编码 |
| --- | --- |
| `NullValue` / `NullArray` | `_\r\n` |
| `Aggregate(MAP, ...)` | `%<pairs>\r\n` |
| `ByteAggregate(SET, ...)` | `~<n>\r\n` |
| `ByteAggregate(MAP, ...)` | `%<pairs>\r\n` |

`RedisReply` 没有 Boolean、Double、BigNumber、VerbatimString、BlobError 变体，`AggregateKind` 只有 `ARRAY` 和 `MAP`，因此 `HELLO 3` 之后也不会发出 RESP3 的 `#`、`,`、`(`、`=`、`!`、`>`、`|` 形态。没有 RESP3 专属形态的语义仍使用通用表达，例如 simple string、integer、bulk string、error 和 array。zset 的 score 走 bulk string（`RedisReplies.bulkString(newScore)` / `longAscii`），而不是 double。

## 已注册命令清单

清单以 `CommandRegistries.dispatcher(...)` 的实际注册为准。生产组装传入两个模块：`DefaultCommandModules.create(...)`（含 connection/key/string/hll/list/hash/set/zset 八组）与 `ServerCommandModule`；另外 `CommandRegistries.dispatcher` 会先注册事务控制命令。合计 66 个命令名：

| 模块 | 命令 |
| --- | --- |
| `CoreConnectionCommands` | `PING` `ECHO` `COMMAND` `SELECT` `QUIT` `CLIENT` `AUTH` `FLUSHDB` |
| `KeyCommands` | `TYPE` `MEMORY` `OBJECT` `KEYS` `SCAN` `DEL` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` |
| `StringCommands` | `SET` `GET` `STRLEN` `APPEND` `SETBIT` `GETBIT` `BITCOUNT` `INCR` `DECR` |
| `HllCommands` | `PFADD` `PFCOUNT` `PFMERGE` |
| `ListCommands` | `LPUSH` `RPUSH` `LRANGE` `LPOP` `RPOP` |
| `HashCommands` | `HSET` `HGET` `HGETALL` `HLEN` `HDEL` `HSCAN` |
| `SetCommands` | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SSCAN` |
| `ZSetCommands` | `ZADD` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZREMRANGEBYSCORE` `ZREMRANGEBYRANK` `ZREM` `ZSCAN` |
| `TransactionCommands` | `MULTI` `EXEC` `DISCARD`（由 `CommandRegistries.dispatcher` 注册） |
| `ServerCommandModule` | `HELLO` `INFO` `STATS` |

部分命令名带子命令，子命令不是独立的注册项：

- `CLIENT SETINFO` / `CLIENT SETNAME` / `CLIENT GETNAME`（`CLIENT` 其余子命令报 `ERR unknown subcommand '<x>'. Try CLIENT HELP.`）；
- `MEMORY USAGE` / `MEMORY STATS`；
- `OBJECT ENCODING`；
- `COMMAND`（裸形式列全部命令）/ `COMMAND COUNT` / `COMMAND INFO <name>...`。

`COMMAND` 的信息仅 6 个元素（name、arity、空 flags 数组、first key、last key、step），不回显 flags、ACL 类别或更为完整的 Redis 元数据；查询未注册命令时对应元素是 null array。

### 当前未覆盖的命令

下面这些 Redis 命令名不在注册表中，调用会走 dispatcher 的 unknown command 分支（`ERR unknown command '<name>'`）：

- string：`MSET`、`MGET`、`SETNX`、`SETEX`、`PSETEX`、`GETSET`、`GETDEL`、`GETEX`、`SETRANGE`、`GETRANGE`、`INCRBY`、`DECRBY`、`INCRBYFLOAT`、`MSETNX`、`BITOP`、`BITPOS`、`BITFIELD`；
- list：`LINDEX`、`LSET`、`LREM`、`LINSERT`、`LTRIM`、`LPOS`、`RPOPLPUSH`、`LMOVE`、`LMPOP`、阻塞版本 `BLPOP`/`BRPOP`；
- hash：`HMGET`、`HMSET`、`HEXISTS`、`HKEYS`、`HVALS`、`HINCRBY`、`HINCRBYFLOAT`、`HRANDFIELD`、`HSETNX`；
- set：`SPOP`、`SRANDMEMBER`、`SMOVE`、`SINTER`、`SUNION`、`SDIFF`、`SINTERSTORE`、`SMISMEMBER`、`SINTERCARD`；
- zset：`ZCARD`、`ZCOUNT`、`ZSCORE`、`ZRANK`、`ZREVRANK`、`ZINCRBY`、`ZPOPMIN`、`ZPOPMAX`、`ZRANGEBYLEX`、`ZUNIONSTORE`、`ZINTERSTORE`、`ZRANDMEMBER`、`ZREMRANGEBYLEX`；
- key/keyspace：`RENAME`、`RENAMENX`、`RANDOMKEY`、`UNLINK`、`TOUCH`、`COPY`、`DUMP`、`RESTORE`、`MOVE`、`SWAPDB`、`DBSIZE`、`PEXPIRETIME`、`EXPIRETIME`、`OBJECT FREQ`、`OBJECT IDLETIME`、`OBJECT REFCOUNT`；
- server/连接：`CONFIG`、`SHUTDOWN`、`DEBUG`、`TIME`、`RESET`、`ACL`、`SAVE`、`BGSAVE`、`LASTSAVE`、`SLOWLOG`、`MONITOR`、`CLIENT ID`/`LIST`/`KILL`；
- 完全缺失的子系统：Pub/Sub（`SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`/`PUBSUB`）、Lua 脚本（`EVAL`/`SCRIPT`）、复制（`REPLICAOF`/`SLAVEOF`/`WAIT`）、集群、模块。

## 协议错误和断连

malformed RESP 没有可靠的重同步点。Yierdis 的做法是：尽量把一条 RESP error 交给已注册的 reply slot，然后关闭当前连接。实现上，`RespRequestDecoder` 产出 `RespProtocolError` 变体，reply admission 把它与已注册槽位一起放入 `RegisteredRespMessage`；`NettyExecutionRequestIngress` 用对应 `ReplySlot` 和当前 session 的 RESP 版本写入 `controlError`，把 slot 标记为 terminal（`markReady(true)`），并调用 `NettyExecutionConnection.markClosing()` 阻止继续读；sequencer flush 后断开连接。

FIFO 保护是例外：若 ingress 队列里仍有更早提交、但尚未拿到容量而延迟发布的 slot，把当前错误写进这个更晚的 slot 会让客户端误判哪条命令出错。此时 ingress 改为取消所有未发布 slot 并直接拆除连接（fail-closed），而不是补一条 `ERR busy`。

常见协议错误文案：

- `ERR Protocol error: invalid multibulk length`；
- `ERR Protocol error: too many arguments`；
- `ERR Protocol error: expected '$', got other`；
- `ERR Protocol error: invalid bulk length`；
- `ERR Protocol error: invalid bulk string terminator`；
- `ERR Protocol error: invalid inline command`；
- `ERR Protocol error: command is too large`（命中 `protocolMaxCommandBytes`）；
- `ERR request exceeds configured memory limit`（ingress admission 预算不足）。

这个做法让坏请求后面的残留 bytes 不再被解释成下一条请求，避免请求和响应错配。

## 协议上限

默认上限由 `RespProtocolLimits` 定义，对应的配置键在 `YierdisServerFileConfig` 里：

| 项目 | 默认值 | 常量 | 配置键 |
| --- | ---: | --- | --- |
| bulk string body | 512 MiB | `DEFAULT_MAX_BULK_BYTES` | `protocolMaxBulkBytes` |
| 单条请求参数数量 | 1,048,576 | `DEFAULT_MAX_ARGS` | `protocolMaxArgs` |
| inline/header 行长度 | 1 MiB | `DEFAULT_MAX_INLINE_BYTES` | `protocolMaxLineBytes` |
| 单条请求累计字节数 | 64 MiB | `DEFAULT_MAX_COMMAND_BYTES` | `protocolMaxCommandBytes` |

这些参数在 server 启动时传给 `YierdisServerChannelInitializer`，再进入 `RespRequestDecoder` 的 `withIngressAdmission(...)`。`protocolMaxLineBytes` 约束所有 CRLF 行（multibulk header、bulk length header、以及 inline 行），不只是 inline。除了 ingress 内存预算 `protocolGlobalInFlightBytes`，还有 executor 与 reply 侧的独立预算见 [`production-hardening-operations.md`](./production-hardening-operations.md)。

## 和 Redis 兼容性的边界

Yierdis 支持 Redis 风格 RESP 入口和一组基础握手命令，但不声明自己是 Redis 的 drop-in replacement，也不声明完整 Redis client ecosystem compatibility。

可以依赖的边界：

- RESP2 是默认请求和回包兼容目标；
- `HELLO 3` 可以切换到基础 RESP3 回包编码；
- `CLIENT SETINFO` 只接受属性名 `LIB-NAME`/`LIB-VER`（其它属性报 `ERR Unrecognized option '<x>'`），`CLIENT SETNAME`/`CLIENT GETNAME` 维护连接名，`AUTH` 固定返回 no-password-configured 错误；
- 达到 `maxClients` 上限时，新连接先收到 `-ERR max number of clients reached\r\n`，随后被关闭；
- malformed RESP 返回协议错误并关闭连接；
- 命令语义以第「已注册命令清单」一节列出的命令为准。

不应从协议兼容推出完整 Redis 命令集、ACL、复制、集群、Pub/Sub、Lua、模块系统或完整 RESP3 客户端生态能力已经实现。

## Bounded Transport Ownership

decoder-side protocol limits and `protocolGlobalInFlightBytes` bound admitted request ownership. Reply encoding runs on a separate receive-order, bounded chunk path. Protocol errors also get an ordered slot, so they cannot overtake an earlier reply. Output that is oversized or of unknown result closes the transport instead of bypassing that path. The exact reply defaults and operator diagnostics are in [`production-hardening-operations.md`](./production-hardening-operations.md).