# Protocol Reference

本文解释 Yierdis 对外 TCP 协议。Yierdis exposes Redis RESP as its public TCP protocol. RESP2 is the default compatibility target for redis-cli, Jedis, Lettuce, and go-redis. RESP3 is available for basic negotiated replies through HELLO 3.

如果你已经看过：

- [`project-overview.md`](./project-overview.md)
- [`request-execution-flow.md`](./request-execution-flow.md)

那么这篇文档重点回答：

- 客户端应该发送什么 RESP 字节？
- 服务端如何把 RESP 请求转成命令执行？
- RESP2 / RESP3 回包和协议错误分别怎么处理？

## 一句话心智模型

Yierdis 的线上协议是 Redis RESP：

- request：RESP array/multibulk，例如 `*2\r\n$3\r\nGET\r\n$1\r\na\r\n`
- reply：RESP2 默认回包，例如 `+OK\r\n`、`$-1\r\n`、`*2\r\n...`
- negotiated reply：同一连接发送 `HELLO 3` 后，支持基础 RESP3 map/null/bool/double 等回包

服务端仍然把命令内部建模成 argv 风格的 `ExecutionRequest`。协议层只负责解析 RESP 和编码 reply，命令层不直接拼协议字节。

## Request Framing

### RESP array 请求

Redis 客户端通常发送 RESP array。每个数组元素是一个 bulk string，argv[0] 是命令名，后续元素是参数。

`GET a` 的请求字节是：

```text
*2\r\n$3\r\nGET\r\n$1\r\na\r\n
```

`SET a 1` 的请求字节是：

```text
*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n
```

常见客户端不需要手写这些字节，直接使用 Redis 客户端库或 `redis-cli` 即可：

```bash
redis-cli -p 6378 SET a 1
redis-cli -p 6378 GET a
```

### Inline commands

Yierdis 也接受 Redis inline command，主要用于 `redis-cli` 兼容和手工调试：

```text
PING\r\n
SET a 1\r\n
```

inline 解析只适合命令行式输入；需要二进制安全参数时应使用 RESP array。

## Protocol Limits

协议上限由 `RespProtocolLimits` 定义：

- bulk string 默认最大 512 MiB
- 单条请求默认最多 1048576 个参数
- inline command 默认最大 1 MiB

超过上限属于协议错误。服务端会写一个 RESP error reply，然后关闭连接。

## From RESP To Execution

请求进入 server 的主链路是：

```text
RespRequestDecoder
  -> RespCommandRequest
  -> RespCommandAdapter
  -> RespExecutionAdapter
  -> ByteArrayExecutionRequest
```

这条桥的作用是保持边界：

- `RespRequestDecoder` 只理解 RESP 字节和协议上限
- `RespExecutionAdapter` 只把 RESP argv 转成 `ExecutionRequest`
- 命令执行层只看 `ExecutionRequest` / `ReplyWriter`

对应源码：

- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`

## RESP2 Replies

RESP2 是默认回包版本。常见 reply 示例：

```text
+OK\r\n
:1\r\n
$5\r\nhello\r\n
$-1\r\n
*2\r\n$1\r\na\r\n$1\r\nb\r\n
-ERR wrong number of arguments for 'get' command\r\n
```

语义对应关系：

- `ReplyWriter.simpleString("OK")` -> `+OK`
- `ReplyWriter.integer(1)` -> `:1`
- `ReplyWriter.bulkString(...)` -> bulk string
- null bulk -> `$-1`
- array -> `*<n>`
- map -> RESP2 下编码为 flat key/value array
- error -> `-ERR ...`

## RESP3 Negotiation

`HELLO [2|3] [SETNAME name]` 用于同一连接的协议版本协商：

```bash
redis-cli -p 6378 HELLO 3
```

`HELLO 3` 成功后，连接的 `ServerSession.respVersion()` 会切到 3，后续 `RespReplyWriter` 可以写 RESP3 的基础类型，例如：

- map：`%<pairs>`
- null：`_`
- bool：`#t` / `#f`
- double：`,`
- set：`~`
- push：`>`

RESP3 只表示回包版本协商，不意味着 Yierdis 覆盖 Redis 的完整命令语义或所有 RESP3 客户端特性。

## Protocol Errors

malformed RESP 不具备可靠的重同步点。Yierdis 的策略是：

1. 尽量写出一个 RESP error reply
2. 标记 close-after-reply
3. flush 后关闭连接

这样可以避免坏请求之后的字节被错误配对到后续 reply。

常见协议错误包括：

- unknown RESP type byte
- bulk length 非法或超过上限
- array 参数数量超过上限
- inline command 超过上限
- frame 未完整结束

## Client Helpers

仓库内置 CLI 和 benchmark 共用 RESP helper：

- `RespClientCodec.writeCommand(...)`
- `RespClientCodec.encodeCommand(...)`
- `RespClientCodec.readReply(...)`

CLI 仍保持一问一答模型。发生 timeout 或 parse failure 时会关闭连接，避免 FIFO reply 错配。

## 最值得看的测试

1. `RespClientCodecTest`
   看 client 侧 RESP command 编码和 reply 解析。
2. `RespRequestDecoderTest`
   看 server 侧 array、inline、协议上限和错误处理。
3. `RespReplyWriterTest`
   看 RESP2/RESP3 reply 编码。
4. `RespHandshakeIntegrationTest`
   看 `HELLO 3` 如何切换连接级 RESP version。
5. `RespProtocolErrorIntegrationTest`
   看 malformed RESP 如何写错误并关闭连接。

## 一句话总结

把 Yierdis 的协议记成下面这句话，基本就不会迷路：

“客户端发 Redis RESP，服务端默认回 RESP2；`HELLO 3` 后同一连接可拿到基础 RESP3 回包；坏 RESP 会收到错误并断连。”
