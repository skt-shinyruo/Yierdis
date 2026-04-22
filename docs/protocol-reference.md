# Protocol Reference

本文专门解释 Yierdis 对外的 `Custom Protocol v1`。如果你已经看过：

- [`project-overview.md`](./project-overview.md)
- [`request-execution-flow.md`](./request-execution-flow.md)

那么这篇文档的目标就是回答两个更细的问题：

- 客户端到底该发什么格式的字节流？
- 服务端收到错误帧以后，为什么大多数时候还能继续处理后续请求？

## 一句话心智模型

Yierdis 的协议不是 RESP，而是一种“长度前缀 + 单行 JSON”的自定义协议：

- request：`<len>:<json>\n`
- reply：NDJSON，也就是“一条 reply 对应一行 JSON”

这套协议的设计重点不是追求和 Redis 生态兼容，而是：

- 保持 framing 简单
- 保持命令参数是 argv 风格
- 在 JSON 可读性和二进制安全之间做折中
- 在协议错误时尽量可恢复

## Request Framing

### 线上的字节格式

每一条请求都是：

```text
<len>:<json-payload>\n
```

其中：

- `len` 是十进制 ASCII 数字
- `len` 表示 `json-payload` 的 UTF-8 字节长度
- `:` 是 header 和 payload 的分隔符
- 最后必须有一个 `\n` 作为整帧终止符

例如，一个 `PING` 请求长这样：

```text
24:{"cmd":"PING","args":[]}
```

写成真正发送到 socket 的形式是：

```text
24:{"cmd":"PING","args":[]}\n
```

一个 `SET a 1` 请求则是：

```text
29:{"cmd":"SET","args":["a","1"]}\n
```

### 为什么 payload 必须是单行

`CustomRequestDecoder` 明确拒绝 payload 里的原始 `CR/LF` 字节。原因很直接：

- 如果 payload 可以跨多行，出错后就更难判断“下一帧从哪里开始”
- 单行约束让 decoder 可以在出错时更容易 resync 到下一个 `\n`

注意这里说的是“原始换行字节”。如果参数里需要换行，应该放在 JSON string 里，以 `\n` 转义后的形式出现。

## Request Payload Schema

### 逻辑结构

request payload 是一个严格 JSON object。服务端真正接受的字段只有两个：

- `cmd`
- `args`

最常见的形态是：

```json
{"cmd":"PING","args":[]}
```

或：

```json
{"cmd":"SET","args":["user:1","alice"]}
```

### `cmd` 的规则

- 必须存在
- 必须是 string
- 去掉首尾 ASCII 空白后不能为空
- 服务端内部会把它当作 argv[0]

这也是为什么 encoder 和 parser 都会对命令名做首尾空白裁剪。

### `args` 的规则

- 可以省略
- 可以是 `null`
- 更常见的是一个数组
- 数组元素只能是 `string` 或 `null`

也就是说，下面这些都能表达“无参数”：

```json
{"cmd":"PING"}
{"cmd":"PING","args":null}
{"cmd":"PING","args":[]}
```

但项目内置 client/bench/encoder 统一会发第三种，也就是始终带 `args` 数组。

### 什么会被判定为 schema 错误

这些情况会被 `CustomProtocolV1RequestPayloadParser` 拒绝：

- `cmd` 缺失
- `cmd` 不是 string
- `cmd` 去空白后为空
- `args` 不是 `null` 或数组
- `args` 数组里出现非 `string|null` 元素
- 出现未知字段
- 出现重复字段

## 从 Protocol DTO 到执行层 argv

协议层不会直接把 JSON DTO 交给命令执行器。中间还有一层桥接：

```text
CustomRequestDecoder
  -> CustomProtocolV1ArgvRequest
  -> ProtocolCommandAdapter
  -> ByteArrayExecutionRequest
```

这条桥的作用是：

- 协议层只负责“解析协议”
- 执行层只负责“处理 argv 风格命令”
- 两层通过 `ExecutionRequest` 契约解耦

如果你想对照源码看，最关键的文件是：

- `yierdis-protocol/.../CustomRequestDecoder.java`
- `yierdis-protocol/.../CustomProtocolV1RequestPayloadParser.java`
- `yierdis-server/.../ProtocolCommandAdapter.java`

## Reply Format

### 成功回包

成功回包统一包在下面这个 envelope 里：

```json
{"ok":true,"result":...}
```

并且每条 reply 结尾都有一个换行：

```text
{"ok":true,"result":"PONG"}\n
```

常见例子：

```json
{"ok":true,"result":"OK"}
{"ok":true,"result":1}
{"ok":true,"result":null}
{"ok":true,"result":["a","b","c"]}
```

### 错误回包

错误回包统一是：

```json
{"ok":false,"error":{"kind":"...","message":"..."}}
```

`kind` 目前主要有三类：

- `command`
- `protocol`
- `internal`

例如：

```json
{"ok":false,"error":{"kind":"command","message":"ERR wrong number of arguments for 'get' command"}}
{"ok":false,"error":{"kind":"protocol","message":"Protocol error: invalid JSON"}}
{"ok":false,"error":{"kind":"internal","message":"ERR internal error"}}
```

## Tagged Values

JSON 本身不擅长表达“二进制 bytes”“嵌套错误”“非普通对象 map”这类值，所以 Yierdis 的 reply 里有几种 tagged value。

### 1. map

协议不会把 map 直接编码成普通 JSON object，而是编码成：

```json
{"$map":[["server","yierdis"],["proto",1]]}
```

因此 `HELLO`、`INFO yierdis`、`STATS`、`MEMORY STATS` 这类结构化命令，最终都长成：

```json
{"ok":true,"result":{"$map":[["server","yierdis"],["proto",1]]}}
```

### 2. 非 UTF-8 bytes

如果 bulk string 不是合法 UTF-8，reply writer 会退化成 base64 tagged value：

```json
{"$b64":"wyg="}
```

这样既不会丢失原始字节，也不会强行用错误编码把内容变成乱码。

### 3. 嵌套 error

如果错误值出现在数组或 map 内部，而不是整条 reply 的顶层 envelope，编码会使用：

```json
{"$error":{"kind":"command","message":"ERR nope"}}
```

例如：

```json
{"ok":true,"result":[1,{"$error":{"kind":"command","message":"ERR nope"}},null]}
```

## Error Handling And Resync

### 协议错误不一定断连

这个协议有一个很重要的设计取向：协议错误尽量可恢复。

最典型的流程是：

1. decoder 发现坏帧
2. 输出一个 `ProtocolError` 事件
3. `ProtocolErrorReplyHandler` 把它写成 `kind=protocol` 的错误回包
4. decoder 尝试恢复到下一帧边界
5. 后续合法帧仍然可以继续执行

这也是为什么 `CustomProtocolResyncIntegrationTest` 会验证：

- 先发一条坏帧
- 再发一条合法 `PING`
- 最终依然能收到 protocol error 之后的 `PONG`

### 什么时候进入 discard-to-LF

对 decoder 来说，大致有两类错误：

第一类：边界已经不可信。

例如：

- 非法 length header
- header 过长
- payload 超上限
- 丢失 frame terminator

这时 decoder 会进入 `DISCARD_TO_LF` 状态，持续丢弃直到下一个 `\n`。

第二类：边界仍然可信，只是 payload 内容不合法。

例如：

- JSON 非法
- request schema 非法
- payload 内出现原始 CR/LF，但 decoder 已经完整吃掉该帧和结尾换行

这时它会返回 protocol error，然后直接继续读下一帧。

### 什么时候会断连

并不是所有错误都能无限恢复。为了避免恶意输入让 server 一直丢弃垃圾字节，decoder 有一个内部 discard budget：

- 如果长期找不到下一个 `\n`
- 且累计丢弃字节超过预算

连接会被直接关闭。

这个 budget 不是单独的 CLI 参数，而是由协议上限间接约束出来的。

### internal error 和 protocol error 的区别

协议错误通常被视为“客户端输入问题”，所以连接一般保持打开。

但如果已经进入 `YierdisFastCommandHandler.exceptionCaught(...)` 的 internal error 路径，处理逻辑会更保守：

- 先返回 `kind=internal`
- 再把连接标记为 closing
- 然后关闭连接，避免队列里已经入队的命令继续产生副作用

## Protocol Limits

默认上限定义在 `ProtocolLimits`：

- `DEFAULT_MAX_REQUEST_PAYLOAD_BYTES = 64 MiB`
- `DEFAULT_MAX_ARGS = 1024`
- `DEFAULT_MAX_HEADER_BYTES = 64 KiB`

服务端启动时可以通过这些参数覆盖：

- `--protocolMaxBulkBytes`
- `--protocolMaxArgs`
- `--protocolMaxLineBytes`

对于开放网络或压测场景，最值得先调整的是 `--protocolMaxBulkBytes`，因为它直接决定了单条请求可能占用的解析和排队内存规模。

更完整的运行时说明见：

- [`configuration-and-operations.md`](./configuration-and-operations.md)

## 初学者最值得对照的测试

如果你想通过测试来理解协议，而不是一上来读实现，推荐按这个顺序看：

1. `CustomProtocolV1RequestEncoderTest`
   看 request frame 怎么被编码出来
2. `CustomRequestDecoderTest`
   看 decoder 如何分帧、报错和恢复
3. `JsonLineReplyWriterTest`
   看 success/error envelope 和 tagged value 的具体样子
4. `CustomProtocolV1ReplyParserTest`
   看 client 侧如何把 reply line 还原成结构化对象
5. `CustomProtocolResyncIntegrationTest`
   看“坏帧之后还能继续跑”的完整链路

## 一句话总结

把 `Custom Protocol v1` 记成下面这句话，基本就不会迷路：

“客户端发 `len + 单行 JSON argv`，服务端回 `一行一个 JSON envelope`；出错时尽量 resync，而不是立刻把连接打死。”
