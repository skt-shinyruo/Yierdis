# 命令层与数据模型

本文说明命令层如何注册、解析和分发命令，以及命令语义如何映射到 DB 能力、逻辑类型和内部编码。

## 命令层职责

命令层位于协议和 DB 之间。它接收传输无关的 `ExecutionRequest`，选择对应命令，解析参数，调用 DB capability，并通过 `RedisReplyWriter` 写回语义结果。

主路径可以简化成：

```text
ExecutionRequest
  -> YierdisFastCommandProcessor
  -> CommandRegistry
  -> CommandDefinition<ArgReader> / typed CommandDefinition<T>
  -> CommandPreparationContext / PreparedCommand
  -> CommandExecutionContext
  -> CommandSupport
  -> DbReads / DbWrites / DbEngine
  -> typed ops
```

命令层不直接解析 RESP 字节，也不直接管理 allocator、entry table 或具体 value root。它负责 Redis 风格最小子集的命令语义：已经实现的命令尽量采用 Redis 风格参数、错误和返回值；未实现的 Redis 能力不在这里隐式承诺。

## 分发与事务专题入口

本页只保留命令抽象、命令家族和逻辑类型模型。

- 如果你要追 `CommandRegistry`、`CommandDefinition`、`ArgReader`、parse error、unknown command 或事务排队，请看 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。
- 如果你要追 `MULTI/EXEC/DISCARD`、队列快照、abort、replay 和 queue limit，请看 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## CommandRegistry 和 CommandDefinition

composition root 创建 `CommandRegistry` 和 `YierdisFastCommandProcessor`，再注册 `TransactionCommands` 与注入的 `CommandModule`。生产默认模块来自 `DefaultCommandModules`，server runtime 还会补充 `ServerCommandModule`。

`CommandRegistry` 是命令名到 `CommandDefinition` 的查找表。处理器读取 `argv[0]` 后，按 ASCII case-insensitive 方式查找注册项；找不到时返回 unknown command。

`CommandDefinition<T>` 是单条命令的统一注册形状，包含：

- `CommandSyntax`：保存命令名、`CommandArity`、`CommandKeySpec` 和 `TransactionPolicy`；
- `CommandParser<T>`：把 `ArgReader` 解析成 `T`；
- `CommandPreparer<T>`：把解析结果准备成带 `ReplyShape` 的 `PreparedCommand`。

简单命令通常使用 `CommandParsers.args()`，让 preparer 直接接收 `ArgReader`；更复杂的命令使用 typed parsed object，把参数形状和业务准备分开。`args.request()` 是需要 retained 参数、slice、bulk traversal 或 request snapshot 时的底层逃生口，不是默认 parser 形状。

完整的分发表构建、`YierdisFastCommandProcessor.prepare(...)` 主流程和事务入队前复用校验，不在本页展开，统一见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## 参数解析和错误

参数解析集中在 command API 的几个小组件里：

- `ArgReader` 包住 `ExecutionRequest`，提供 `argc()`、`bytes(index)`、`is(index, literal)`、`longAt(...)`、`positiveLongAt(...)` 等读取能力，并尽量直接基于 argv bytes 做 ASCII 比较。
- `CommandArity` 表达参数个数规则，包括 exact、min、range、one-of 和 pair-tail。pair-tail 用于 `HSET field value ...`、`ZADD score member ...` 这类尾部成对参数。
- `CommandParsers.args()` 是 `ArgReader` identity parser；需要 typed value 时由命令家族提供自己的 `CommandParser<T>`。
- `CommandParseError` 集中表达 wrong arity、syntax、integer out of range 和自定义错误，并转换成 Redis 风格 reply 文案。

`CommandDefinition.parse(...)` 先运行 arity 校验，再返回 parser 的结果。解析失败时，处理器准备一个 error command；解析成功时，preparer 才会运行。这个约束同样用于事务入队：`MULTI` 状态下，普通命令会先 lookup definition、检查事务策略、运行 parser，通过后才 retain `ExecutionRequest` 并返回 `QUEUED`。

这里保留的是 parser 抽象和错误模型；真正的分支顺序、unknown command、change observer gate 和错误翻译，见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)。

## 准备、执行上下文和 RedisReplyWriter

命令分成两个明确阶段：

- `CommandPreparationContext` 只提供 `CommandSession`。preparer 在 reply capacity 预留前完成参数校验、只读查询、mutation prepare 和 `ReplyShape` 计算。
- `CommandExecutionContext` 在 capacity 预留成功后创建，提供同一个 `CommandSession`、`RedisReplyWriter` 和请求级 `MutationContext`。`PreparedCommand.execute(...)` 在这个阶段执行一次。

`CommandSession` 聚合当前 DB index、transaction、client metadata、connection stats 和 protocol negotiation 能力。准备阶段不能写 reply；执行阶段则在已经预留的形状内写 reply，并把 mutation record 显式传给 DB 写视图。

`RedisReplyWriter` 是命令层唯一的 Redis reply 语义模型。命令 handler 写的是 `simpleString`、`bulkString`、`integer`、`arrayHeader`、`mapHeader`、`nullValue`、`error` 等 Redis reply 形状，不写 `+OK\r\n`、`$-1\r\n` 这类协议字节。RESP2 / RESP3 的差异由协议 writer 根据连接版本处理。

`CommandSupport` 是内置命令的公共工具箱。参数读取统一由 `ArgReader` 完成；`CommandSupport` 负责选择当前 DB、创建 preparation/execution 对应的 DB view、复用 scratch buffer，并把 DB 返回的 bulk-string 序列适配到 `PreparedCommand` 和 `RedisReplyWriter`。

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
| hash | `HashCommands` | `HSET`、`HGET`、`HGETALL`、`HLEN`、`HDEL`、`HSCAN` |
| set | `SetCommands` | `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`、`SSCAN` |
| zset | `ZSetCommands` | `ZADD`、`ZRANGE`、`ZREVRANGE`、`ZRANGEBYSCORE`、`ZREVRANGEBYSCORE`、`ZREMRANGEBYSCORE`、`ZREMRANGEBYRANK`、`ZREM`、`ZSCAN` |
| transaction | `TransactionCommands` | `MULTI`、`EXEC`、`DISCARD` |

connection/server 命令更多操作连接态、握手兼容、server runtime 信息和执行框架；数据结构命令则通过 DB capability 修改或读取逻辑类型。

## HSCAN、SSCAN 和 ZSCAN

集合扫描命令复用 `CollectionScanCommandSupport` 的参数解析和回复写入：

```text
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
SSCAN key cursor [MATCH pattern] [COUNT count]
ZSCAN key cursor [MATCH pattern] [COUNT count]
```

`cursor` 应传入上一页返回的非负十进制游标；`0` 表示开始一次迭代，返回 `0` 表示本轮迭代结束。`MATCH` 使用 glob pattern，只匹配 hash field、set member 或 zset member，不匹配 hash value 或 zset score。`COUNT` 默认为 10，参数必须在 `1..Integer.MAX_VALUE` 内。选项可以按任意顺序出现；`NOVALUES` 只对 `HSCAN` 有效，启用后只返回 field。

三条命令都返回两元素数组：第一个元素是 bulk string 形式的下一游标，第二个元素是 bulk string 数组。元素数组的形状分别是：

- `HSCAN`：默认按 `field, value, ...` 交替排列；`NOVALUES` 模式只包含 field。
- `SSCAN`：`member, ...`。
- `ZSCAN`：按 `member, score, ...` 交替排列，score 使用 Redis 风格十进制文本。

不存在的 key 返回游标 `0` 和空元素数组；存在但类型不匹配的 key 返回 `WRONGTYPE`。

`COUNT` 是工作量和期望返回量的 hint，不是精确页大小。具体行为取决于内部编码：

- `HASH_PACKED`、`SET_INTSET`、`ZSET_PACKED` 属于 compact 分支。游标为 `0` 时会过滤并物化全部匹配元素，一次返回且下一游标为 `0`，即使 `COUNT 1` 也可能返回整个集合；传入非零游标时返回空的终止页。这样可以避免 packed 数组删除、hash field 移位或 zset 改分重排破坏位置游标。
- `HASH_HT`、`SET_HT` 以及 `ZSET_SKIPLIST` 的 member table 属于字典分支。实现按物理 slot 做有界扫描，单次匹配的逻辑 field/member 数为 `min(COUNT, 1024)`；slot budget、空槽和 `MATCH` 过滤都可能让实际返回量更少，甚至返回空数组但下一游标仍非零。`HSCAN` 和 `ZSCAN` 的 field/member 与附属 value/score 仍作为一组计入这个逻辑条目上限。

字典分支不会复制整个集合。一次扫描只 pin 已选中的 native field/member/value handle，并把这些 pin 可能延迟回收的 native payload 纳入 reply retained-memory 预检；元素同步写入 `RedisReplyWriter` 后窗口立即关闭并 unpin。compact 分支则用稳定的 heap byte arrays 构造本页窗口，同样把窗口保留量计入 reply admission。

这些游标提供的是 Redis 风格弱一致迭代，不是快照，也没有稳定顺序。迭代期间新增的元素可能出现也可能不出现，已经返回的元素可能因 rehash 或结构代变化再次出现，调用方应自行去重；删除或改分会影响后续可见结果。generation-aware 游标以可推进、可终止为目标，并在 generation token 未回绕时避免遗漏整个迭代期间始终存在的元素，但不能作为跨 DB、跨 key 删除重建或跨集合生命周期长期保存的书签。

## `StringCommands` 的主路线

`StringCommands` 是 string / bitmap 家族的主入口。`SET`、`GET`、`APPEND`、`INCR` 这类命令不是各自独立地直连 DB，而是先走 `CommandSupport` 再走 typed ops / `DbEngine`，让 wrong-type、NX/XX/EX/PX/KEEPTTL 和整数分支都收敛到同一条命令语义里。

bitmap 只是 string bytes 的一种视图，因此 `SETBIT`、`GETBIT`、`BITCOUNT` 和普通 string 命令共享 `ValueType.STRING` 及其 wrong-type 约束。写路径最终仍会经过 `YierdisDbMutationExecutor`、`YierdisDbKeyLifecycle` 和对应的 TTL / memory 账本；读路径则通过 `RedisReplyWriter` 把结果写回，而不是直接拼 RESP bytes。

如果要改 `StringCommands`，优先看 string 家族测试、`StringWriteOps` / `StringReadOps`、`commands-and-data-model.md` 的逻辑类型映射，以及 [`request-execution-flow.md`](./request-execution-flow.md) 里的 `SET` 主链。

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

`MULTI` 开启事务后，大多数普通命令不会立即执行。处理器会先查 `CommandRegistry`，检查对应 `CommandSyntax.transactionPolicy()`，并运行同一套 `CommandDefinition.parse(...)`。解析通过后，事务队列会 retain 当前 `ExecutionRequest`，客户端收到 `QUEUED`。

`EXEC` 回放已入队的请求，按队列顺序调用同一个命令处理器执行；`DISCARD` 清空队列并退出事务。事务控制命令本身有特殊策略，例如 `HELLO` 这类连接协议协商命令在 `MULTI` 中被禁止。

这里的事务语义是 Redis 风格最小子集：它提供连接级队列和顺序回放，但不应被理解成完整 Redis 事务生态、Lua、watch 或集群语义。

本页只解释事务在 command model 里的位置。`TransactionState` 的所有权、为什么队列里保存 retained `ExecutionRequest`、`EXEC` 如何 replay、abort/cleanup/queue limit 如何收敛，都放在 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 新增命令时的路线

新增命令时优先沿着现有命令层边界走：

1. 确认命令属于哪个 family，或是否需要新的 `CommandModule`。
2. 在模块中注册 `CommandDefinition`，补齐 `CommandSyntax`、parser、preparer 和 transaction policy。
3. 用 `ArgReader`、`CommandArity`、`CommandParsers`、`CommandParseError` 表达参数规则和错误，不在 handler 里散落重复校验。
4. 在 preparation 阶段计算 `ReplyShape`，在 `CommandExecutionContext` 中通过 `RedisReplyWriter` 写出同一形状。
5. 通过 `CommandSupport` 取得 DB capability，让 preparer / prepared command 调用 typed ops，不直接触碰 value root、allocator handle 或 RESP 字节。
6. 补命令家族测试、错误路径测试；如果新增 server-only 行为，再补 server-main 组装或协议集成测试。

如果命令会暴露内部编码或 memory 信息，还需要同时检查 `OBJECT ENCODING`、`MEMORY USAGE`、`MEMORY STATS` 等 introspection 行为是否仍然一致。
