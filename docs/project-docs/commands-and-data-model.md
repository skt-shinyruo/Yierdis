# 命令层与数据模型

本文说明命令层如何注册、解析和分发命令，以及命令语义如何映射到 DB 能力、逻辑类型和内部编码。

## 命令层职责

命令层位于协议和 DB 之间。它接收传输无关的 `ExecutionRequest`，选择对应命令，解析参数，调用 DB capability，并通过 `ReplyWriter` 写回语义结果。

主路径可以简化成：

```text
ExecutionRequest
  -> YierdisFastCommandProcessor
  -> CommandRegistry
  -> CommandSpec<ExecutionRequest> / typed CommandSpec<T>
  -> CommandContext
  -> CommandSupport
  -> DbReads / DbWrites / DbEngine
  -> typed ops
```

命令层不直接解析 RESP 字节，也不直接管理 allocator、entry table 或具体 value root。它负责 Redis 风格最小子集的命令语义：已经实现的命令尽量采用 Redis 风格参数、错误和返回值；未实现的 Redis 能力不在这里隐式承诺。

## 分发与事务专题入口

本页只保留命令抽象、命令家族和逻辑类型模型。

- 如果你要追 `CommandRegistry`、`CommandSpec`、`ArgReader`、parse error、unknown command、change observer gate，请看 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。
- 如果你要追 `MULTI/EXEC/DISCARD`、队列快照、abort、replay 和 queue limit，请看 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## CommandRegistry 和 CommandSpec

`YierdisFastCommandProcessor` 构造时创建 `CommandRegistry`。它先注册 `TransactionCommands`，再注册注入的 `CommandModule`。生产默认模块来自 `DefaultCommandModules`，server runtime 还会补充 `ServerCommandModule`。

`CommandRegistry` 是命令名到 `CommandSpec` 的查找表。处理器读取 `argv[0]` 后，按 ASCII case-insensitive 方式查找注册项；找不到时返回 unknown command。

`CommandSpec<T>` 是单条命令的统一注册形状，包含：

- parser：把 `ExecutionRequest` 解析成 `T`；
- handler：执行 parsed command；
- `CommandDescriptor`：描述 arity 和 key 位置，供 `COMMAND INFO` 风格能力使用；
- MULTI policy：声明命令在事务中是否允许入队或执行。

有些简单命令可以直接使用 `CommandSpec<ExecutionRequest>`，也就是 handler 仍接收原始 argv 视图；更复杂的命令通常使用 typed parsed object，把参数形状和业务执行分开。

完整的分发表构建、`YierdisFastCommandProcessor.execute(...)` 主流程和事务入队前复用校验，不在本页展开，统一见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## 参数解析和错误

参数解析集中在 command API 的几个小组件里：

- `ArgReader` 包住 `ExecutionRequest`，提供 `argc()`、`bytes(index)`、`is(index, literal)`、`longAt(...)`、`positiveLongAt(...)` 等读取能力，并尽量直接基于 argv bytes 做 ASCII 比较。
- `CommandArity` 表达参数个数规则，包括 exact、min、range、one-of 和 pair-tail。pair-tail 用于 `HSET field value ...`、`ZADD score member ...` 这类尾部成对参数。
- `CommandParsers` 把常见 arity rule 包成 `CommandParser<T>`，也支持 mapper 把 `ArgReader` 转成 typed parsed object。
- `CommandParseError` 集中表达 wrong arity、syntax、integer out of range 和自定义错误，并转换成 Redis 风格 reply 文案。

`CommandSpec.parse(...)` 返回 parse result。解析失败时，处理器直接通过 `ReplyWriter.error(...)` 写出错误；解析成功时，handler 才会运行。这个约束同样用于事务入队：`MULTI` 状态下，普通命令会先 lookup spec、检查事务策略、运行 parser，通过后才保存 `ExecutionRequest` 快照并返回 `QUEUED`。

这里保留的是 parser 抽象和错误模型；真正的分支顺序、unknown command、change observer gate 和错误翻译，见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## CommandContext 和 ReplyWriter

`CommandContext` 是一次命令执行的环境对象，通常提供：

- `ReplyWriter`；
- `CommandSessionCapabilities` 视图；
- 当前 DB index / transaction / client metadata / protocol negotiation 能力；
- 可通过上下文取得的 DB runtime 能力。

`RedisReplyWriter` 是命令层唯一的 Redis reply 语义模型；现有执行边界继续暴露兼容别名 `ReplyWriter`。命令 handler 写的是 `simpleString`、`bulkString`、`integer`、`arrayHeader`、`mapHeader`、`nullValue`、`error` 等 Redis reply 形状，不写 `+OK\r\n`、`$-1\r\n` 这类协议字节。RESP2 / RESP3 的差异由协议 writer 根据连接版本处理。

`CommandSupport` 是内置命令的公共工具箱。它帮助各个 `*Commands` 类读取参数、解析整数、取得 `DbReads` / `DbWrites` / `DbEngine`、复用 scratch buffer，并把 DB 返回的 bulk-string 序列适配到 `ReplyWriter`。

集合读命令通常按这个顺序写回：

```text
read DB result
  -> out.arrayHeader(...) / out.mapHeader(...)
  -> result.emitTo(new BulkStringReplyAdapter(out))
```

这让 DB/value/off-heap 代码不依赖 RESP，也避免命令层为了回包提前复制完整集合结果。

## 命令家族总览

当前命令语义是 Redis-style minimum subset where implemented。具体支持情况以 `CommandRegistry`、内置命令模块和对应测试为准；不要把文档表格当作完整 Redis command reference。

| 家族 | 主要模块 | 代表命令 |
| --- | --- | --- |
| connection/server | `CoreConnectionCommands`、`ServerCommandModule` | `PING`、`ECHO`、`COMMAND`、`SELECT`、`QUIT`、`CLIENT SETINFO`、`CLIENT SETNAME`、`CLIENT GETNAME`、`AUTH`、`HELLO`、`INFO`、`STATS`、`FLUSHDB` |
| key/TTL | `KeyCommands` | `TYPE`、`MEMORY USAGE`、`MEMORY STATS`、`OBJECT ENCODING`、`KEYS`、`SCAN`、`DEL`、`EXISTS`、`EXPIRE`、`PEXPIRE`、`EXPIREAT`、`PEXPIREAT`、`PERSIST`、`TTL`、`PTTL` |
| string/bitmap | `StringCommands` | `SET`、`GET`、`STRLEN`、`APPEND`、`SETBIT`、`GETBIT`、`BITCOUNT`、`INCR`、`DECR` |
| HLL | `HllCommands` | `PFADD`、`PFCOUNT`、`PFMERGE` |
| list | `ListCommands` | `LPUSH`、`RPUSH`、`LRANGE`、`LPOP`、`RPOP` |
| hash | `HashCommands` | `HSET`、`HGET`、`HGETALL`、`HLEN`、`HDEL` |
| set | `SetCommands` | `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD` |
| zset | `ZSetCommands` | `ZADD`、`ZRANGE`、`ZREVRANGE`、`ZRANGEBYSCORE`、`ZREVRANGEBYSCORE`、`ZREMRANGEBYSCORE`、`ZREMRANGEBYRANK`、`ZREM` |
| transaction | `TransactionCommands` | `MULTI`、`EXEC`、`DISCARD` |

connection/server 命令更多操作连接态、握手兼容、server runtime 信息和执行框架；数据结构命令则通过 DB capability 修改或读取逻辑类型。

## 逻辑类型和内部编码

用户看到的是逻辑类型，DB 记录的是逻辑类型加内部编码。逻辑类型由 `ValueType` 表达，当前包括：

- `STRING`
- `LIST`
- `SET`
- `HASH`
- `ZSET`

内部编码由 `ValueEncoding` 表达，常见映射是：

| 逻辑类型 | 内部编码 |
| --- | --- |
| string | `STRING_INT`、`STRING_EMBSTR`、`STRING_RAW` |
| hash | `HASH_PACKED`、`HASH_HT` |
| list | `LIST_PACKED`、`LIST_QUICKLIST` |
| set | `SET_INTSET`、`SET_HT` |
| zset | `ZSET_PACKED`、`ZSET_SKIPLIST` |

`OBJECT ENCODING key` 会把这些内部编码格式化成更熟悉的 Redis 风格名称，例如 `int`、`embstr`、`raw`、`listpack`、`hashtable`、`intset`、`quicklist`、`skiplist`。

编码切换由 value 层和 DB 层决定，不由命令 handler 手工选择。典型阈值包括：

- hash packed 到 hashtable：entry 数量或 field/value 字节大小超过阈值；
- list packed 到 quicklist：紧凑块大小超过阈值；
- set intset 到 hashtable：出现非整数 member 或元素数量超过阈值；
- zset packed 到 skiplist：entry 数量或 member 字节大小超过阈值；
- string int/embstr/raw：根据是否可解析为 long 和字符串长度选择。

命令层只通过 `DbReads`、`DbWrites`、`DbEngine` 和 typed ops 操作这些逻辑类型。allocator handle、entry table、native object kind 和 off-heap payload 细节属于 DB/value 层。

## HLL、bitmap 和 string 的关系

bitmap 没有独立逻辑类型。`SETBIT`、`GETBIT`、`BITCOUNT` 操作的是 string bytes，因此它们和 `GET`、`STRLEN`、`APPEND` 等命令共享 `ValueType.STRING`。当一个 key 不是 string 时，bitmap 命令也会遵守同一类 wrongtype 约束。

HLL 也没有独立 `ValueType`。命令层有 `HllCommands`，DB 层有 `YierdisHllOps`，但底层对象仍然是 `ValueType.STRING`。是否是 HLL payload 由 `YierdisHyperLogLog` 的格式约定判断。

这种设计让 Redis 风格命令家族可以单独存在，同时避免类型系统为 bitmap 和 HLL 再扩出额外主类型。

## 事务中的命令语义

事务是连接级状态，由 `TransactionCommands` 和 command processor 协作实现。

`MULTI` 开启事务后，大多数普通命令不会立即执行。处理器会先查 `CommandRegistry`，检查对应 `CommandSpec` 的 MULTI policy，并运行同一套 parser。解析通过后，当前 `ExecutionRequest` 会保存为执行快照，客户端收到 `QUEUED`。

`EXEC` 回放已入队的请求，按队列顺序调用同一个命令处理器执行；`DISCARD` 清空队列并退出事务。事务控制命令本身有特殊策略，例如 `HELLO` 这类连接协议协商命令在 `MULTI` 中被禁止。

这里的事务语义是 Redis 风格最小子集：它提供连接级队列和顺序回放，但不应被理解成完整 Redis 事务生态、Lua、watch 或集群语义。

本页只解释事务在 command model 里的位置。`TransactionState` 的所有权、为什么队列里保存的是 `ExecutionRequest` 快照、`EXEC` 如何 replay、abort/cleanup/queue limit 如何收敛，都放在 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 新增命令时的路线

新增命令时优先沿着现有命令层边界走：

1. 确认命令属于哪个 family，或是否需要新的 `CommandModule`。
2. 在 `CommandRegistry` 中注册 `CommandSpec`，补齐 parser、handler、`CommandDescriptor` 和 MULTI policy。
3. 用 `ArgReader`、`CommandArity`、`CommandParsers`、`CommandParseError` 表达参数规则和错误，不在 handler 里散落重复校验。
4. 通过 `CommandContext` 取得 `ReplyWriter`，通过 `CommandSupport` 取得 DB capability。
5. 让 handler 调用 typed ops，不直接触碰 value root、allocator handle 或 RESP 字节。
6. 补命令家族测试、错误路径测试；如果新增 server-only 行为，再补 server-main 组装或协议集成测试。

如果命令会暴露内部编码或 memory 信息，还需要同时检查 `OBJECT ENCODING`、`MEMORY USAGE`、`MEMORY STATS` 等 introspection 行为是否仍然一致。
