# 协议参考

Yierdis 当前公开的 TCP 协议有明确的实现边界：RESP 请求如何进入系统、回包如何编码、协议错误如何处理，以及哪些 Redis 协议能力还只是基础兼容。下面按这条线展开。

## 协议定位

Yierdis 的公开网络入口是 Redis RESP 风格的 TCP 协议。协议层把线上字节解析成 argv，再交给命令层；命令层返回 `CommandResult` 和其中的语义 `RedisReply`，不直接编码 RESP。两层之间的边界是 `ExecutionRequest`、`CommandResult` 和 `RedisReply`，不是 RESP 字节，也不是 writer 调用。

请求进入系统的主路径是：

```text
Netty ByteBuf
  -> RespRequestDecoder
  -> ByteArrayExecutionRequest
  -> CommandExecutor
  -> CommandDispatcher.prepare(session, request)
```

`RespRequestDecoder` 只处理 RESP / inline 字节、协议上限、ingress admission 和协议错误，把结果封闭为 `RespDecodedMessage.Request` 或 `RespProtocolError`。reply admission 注册槽位后，execution ingress 才从 request 变体里取出传输无关的 `ExecutionRequest`。因此同一个命令实现不用关心请求来自 RESP array 还是 inline command，也不用自己拼 RESP 回包。

## RESP2 请求

RESP2 array/multibulk 是默认请求格式，也是 Redis 客户端通常发送的格式。数组里的每个元素都是 bulk string：

- `argv[0]` 是命令名；
- `argv[1..]` 是命令参数；
- 参数按 byte array 传递，保持二进制安全。

例如 `GET a` 的请求字节是：

```text
*2\r\n$3\r\nGET\r\n$1\r\na\r\n
```

`RespRequestDecoder` 对 RESP2 array 的约束包括：

- multibulk header 必须是 `*<argc>\r\n`；
- `argc` 必须是非负整数，并且不能超过协议参数上限；
- 非 null 参数是 `$<len>\r\n<body>\r\n`；`$-1\r\n` 是 null bulk，没有 body；
- bulk length 可以是 `-1` 或非负整数，不能小于 `-1`，也不能超过 bulk 上限；
- 非 null bulk 的 body 后必须紧跟 `\r\n`。

连接刚建立时，回包版本也默认是 RESP2。也就是说，不执行 `HELLO` 的普通 Redis 客户端会收到 RESP2 编码的 simple string、integer、bulk string、array 和 error。

## inline command

如果请求不是以 `*` 开头，`RespRequestDecoder` 会按 inline command 尝试解析。它主要用于手工调试和基础兼容：

```text
PING\r\n
SET a 1\r\n
```

inline command 以 CRLF 结束，按空白切分参数，并支持单引号、双引号和部分反斜杠转义。空白 inline 行会被忽略，不会生成命令请求。

inline command 不是二进制安全入口。需要传递任意 bytes、空 bytes 或复杂参数时，应使用 RESP array。

## HELLO 2 / HELLO 3

`HELLO` 用来协商当前连接的回包版本。支持的基础形式：

```text
HELLO
HELLO 2
HELLO 3
HELLO 2 SETNAME <name>
HELLO 3 SETNAME <name>
```

`HELLO 2` 把连接设为 RESP2 回包；`HELLO 3` 把连接切到基础 RESP3 reply encoding。切换成功后，作为连接 session owner 的 `EngineSession` 会记录当前版本。回复使用的协议版本在 prepare/容量预留时刻读取一次并捕获进 `ReplyPlan`，命令执行返回语义 `RedisReply` 后，executor 按这份捕获值创建 `RespReplyWriter`，再由中央 renderer 编码成相应 RESP 形态；HELLO 自身在 prepare 时声明协商后的目标版本，所以协商回复的容量预留与写出都按协商后版本计算。

`HELLO` 返回 5 个字段：`server`、`version`、`proto`、`mode`、`role`。在 RESP2 下这个 reply 是 flat array，在 RESP3 下是 map。例如 `HELLO 3` 成功后，响应包含 `proto: 3`，并且后续 map、set、null 语义会使用 RESP3 基础编码。

这里有几条限制：

- 请求不支持的版本，例如 `HELLO 4`，会返回 `-NOPROTO unsupported protocol version`；
- `HELLO ... AUTH ...` 固定返回 no-password-configured 错误，因为项目没有认证配置面；
- `HELLO` 在 `MULTI` 中被禁止；
- RESP3 协商只表示回包编码切换，不表示完整 Redis RESP3 客户端生态兼容。

## RedisReply 到 RESP 回包

`RedisReply` 是命令结果的语义模型。根接口的 default `shape()` 用 sealed hierarchy 上的穷尽 switch 集中完成 `ReplyShape` 投影，各 variant 不声明自己的 `shape()`。`ReplyShapes` 只管 shape 构造与规范化，`RedisReplyRenderer` 则是唯一的命令结果遍历点。命令实现只构造 `SimpleString`、`IntegerValue`、`BulkString`、`Aggregate`、`NullValue`、`Error` 等变体，renderer 再调用 `RedisReplyWriter`。所以 `RedisReplyWriter` 只是 renderer 面向 RESP encoder 的端口，不是命令 API。ingress admission 或协议错误属于命令管线之外的控制回复，仍由网络边界直接编码。

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

RESP3 下，已有专属形态的语义会换成 RESP3 编码：

| `RedisReply` 语义 | RESP3 编码 |
| --- | --- |
| `NullValue` / `NullArray` | `_\r\n` |
| `Aggregate(MAP, ...)` | `%<pairs>\r\n` |
| `ByteAggregate(SET, ...)` | `~<n>\r\n` |
| `ByteAggregate(MAP, ...)` | `%<pairs>\r\n` |

`RedisReply` 没有 Boolean、Double、BigNumber、VerbatimString、BlobError 变体，`AggregateKind` 只有 `ARRAY` 和 `MAP`，因此 `HELLO 3` 之后也不会发出 RESP3 的 `#`、`,`、`(`、`=`、`!`、`>`、`|` 形态。没有 RESP3 专属形态的语义仍使用通用表达，例如 simple string、integer、bulk string、error 和 array。

## 协议错误和断连

malformed RESP 没有可靠的重同步点。Yierdis 的做法是：尽量返回 RESP error reply，然后关闭当前连接。实现上，`RespRequestDecoder` 产出 `RespProtocolError` 变体，reply admission 把它与已注册槽位一起放入 `RegisteredRespMessage`；`NettyExecutionRequestIngress` 使用对应 `ReplySlot` 和当前 session 的 RESP 版本写入 control error，并把该 slot 标记为 terminal；sequencer flush 后断开连接。FIFO 保护是例外：若仍有更早的容量延迟提交占着未发布 slot，ingress 不会把它们的回复伪造成 `ERR busy`，而是取消这些 slot 并直接拆除连接（fail-closed），避免更晚的终端错误与更早的命令错位。

常见协议错误包括：

- multibulk header 非法；
- array 参数数量超过上限；
- array 元素不是 bulk string；
- bulk length 非法或超过上限；
- bulk body 没有以 `\r\n` 结束；
- inline command 行太长或格式非法；
- inline 参数数量超过上限。

这个做法让坏请求后面的残留 bytes 不再被解释成下一条请求，避免请求和响应错配。

## 协议上限

默认上限由 `RespProtocolLimits` 定义：

| 项目 | 默认值 | 服务端参数 |
| --- | ---: | --- |
| bulk string body | 512 MiB | `--protocolMaxBulkBytes` |
| 单条请求参数数量 | 1,048,576 | `--protocolMaxArgs` |
| inline/header 行长度 | 1 MiB | `--protocolMaxLineBytes` |
| 单条请求累计字节数 | 64 MiB | `--protocolMaxCommandBytes` |

这组参数在 server 启动时传给 `YierdisServerChannelInitializer`，再进入 `RespRequestDecoder`。超过上限属于协议错误，会返回 error 并关闭连接。

## 和 Redis 兼容性的边界

Yierdis 支持 Redis 风格 RESP 入口和一组基础握手命令，但不声明自己是 Redis 的 drop-in replacement，也不声明完整 Redis client ecosystem compatibility。

可以依赖的边界：

- RESP2 是默认请求和回包兼容目标；
- `HELLO 3` 可以切换到基础 RESP3 回包编码；
- `CLIENT SETINFO` 校验 arity 与 `LIB-NAME`/`LIB-VER` 属性名，`CLIENT SETNAME`/`CLIENT GETNAME` 维护连接名，`AUTH` 固定返回 no-password-configured 错误；
- 达到 `--maxClients` 上限时，新连接先收到 `-ERR max number of clients reached`，随后被关闭；
- malformed RESP 返回协议错误并关闭连接；
- 命令语义以当前已实现命令为准。

不应从协议兼容推出完整 Redis 命令集、ACL、复制、集群、Pub/Sub、Lua、模块系统或完整 RESP3 客户端生态能力已经实现。

## Bounded Transport Ownership

decoder-side protocol limits and `--protocolGlobalInFlightBytes` bound admitted request ownership. Reply encoding has a separate receive-order, bounded chunk path; protocol errors also receive an ordered slot so they cannot overtake an earlier reply. Oversized or result-unknown output closes the transport rather than bypassing that path. The exact reply defaults and operator diagnostics are in [`production-hardening-operations.md`](./production-hardening-operations.md).
