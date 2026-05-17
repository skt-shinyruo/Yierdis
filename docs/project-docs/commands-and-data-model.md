# Commands And Data Model

本文回答两个经常一起出现的问题：

- Yierdis 到底实现了哪些命令家族，这些命令是怎么挂进处理器里的？
- 命令最后为什么会落到 `int` / `embstr` / `listpack` / `skiplist` 这些内部编码上？

如果你已经读过：

- [`request-execution-flow.md`](./request-execution-flow.md)
- [`main-path-walkthrough.md`](./main-path-walkthrough.md)

那这篇文档就是把“命令层”和“数据结构层”接起来。

## 先记住一条主线

一条命令从进入执行器到真正改 DB，通常会经过下面这条链：

```text
ExecutionRequest
  -> YierdisEngine / DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandRegistry / CommandModule
  -> CommandSupport
  -> DbReads / DbWrites / DbEngine
  -> Yierdis*Ops
  -> EntryRecord / typed ValueHandle / TypeRoot
```

也就是说：

- 命令层决定“调用哪个操作”
- DB 层决定“这个逻辑类型现在应该用哪种内部编码”

## 命令是怎么注册进去的

`YierdisFastCommandProcessor` 构造时会创建一个 `CommandRegistry`，并先注册 `TransactionCommands`。生产启动时，应用组合根会再注入 `DefaultCommandModules` 和 server 侧的 `ServerCommandModule`；其中 `DefaultCommandModules` 依次注册：

1. `CoreConnectionCommands`
2. `KeyCommands`
3. `StringCommands`
4. `HllCommands`
5. `ListCommands`
6. `HashCommands`
7. `SetCommands`
8. `ZSetCommands`

这意味着：

- `yierdis-command-core` 负责 registry / processor / transaction replay
- `yierdis-command-builtin` 负责“传输无关”的默认命令
- `yierdis-server-main` 只补充那些依赖 runtime / build info / observability 的命令

初学者最值得直接打开的文件是：

- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`

## `CommandDescriptor` 是什么

每个注册项除了 handler，还有一个 `CommandDescriptor`，里面记录：

- `arity`
- `firstKeyIndex`
- `lastKeyIndex`
- `keyStep`

它对应的是 Redis `COMMAND INFO` 风格的元数据。

初学者可以先只把它理解成两件事：

- 这个命令参数个数该怎么校验
- 哪些参数位置会被当作 key

## 命令解析 API 的统一契约

命令注册项真正执行前，都会先经过 `CommandSpec<T>.parse(...)`。这层 parser
不是随手写在 handler 里的普通 if，而是 command API 的一组稳定小组件：

- `ArgReader`
  包住 `ExecutionRequest`，提供 `argc()`、`is(index, literal)`、`longAt(...)`、
  `positiveLongAt(...)` 等常用读取能力。它直接按 argv bytes 做 ASCII
  case-insensitive 比较，避免为了判断选项先把热路径参数转成字符串。
- `CommandArity`
  表达参数个数规则，包括 exact、min、range、one-of 和 pair-tail。pair-tail
  主要服务 `HSET field value ...`、`ZADD score member ...` 这类尾部成对参数。
- `CommandParsers`
  把常见 arity rule 包成 `CommandParser<T>`，也支持通过 mapper 把 `ArgReader`
  转成 typed parsed object。
- `CommandParseResult`
  只表达两种结果：parsed value 或 `CommandParseError`。
- `CommandParseError`
  集中把 wrong arity、syntax、integer out of range 和自定义错误映射成 Redis
  风格 reply 文案。

这套契约有两个重要效果。

第一，普通执行和事务入队前校验复用同一套 parser。`MULTI` 状态下，普通命令不是
先无脑入队，而是先 lookup `CommandSpec`、检查 MULTI policy、运行 parser；只有解析
通过才会保存 `ExecutionRequest` 快照并返回 `QUEUED`。因此 `EXEC` replay 不会再遇到
一批本该在入队阶段发现的 arity/syntax 错误。

第二，命令 handler 拿到的是 typed parsed object，而不是每个 handler 自己重新解析
argv。新增命令时，优先把“参数形状”和“业务执行”分开：parser 负责 arity、选项冲突、
整数/score 边界；handler 只负责调用 DB capability 和写 reply。

## 命令家族总览

### 1. Connection / Session

来自 `CoreConnectionCommands` 和 `TransactionCommands`：

- `PING`
- `ECHO`
- `COMMAND`
- `SELECT`
- `QUIT`
- `CLIENT SETINFO ...`
- `CLIENT SETNAME name`
- `CLIENT GETNAME`
- `AUTH`
- `FLUSHDB [SYNC|ASYNC]`
- `MULTI`
- `EXEC`
- `DISCARD`

这些命令更多是在操作“连接态”、客户端握手兼容和“执行框架”，而不是复杂数据结构。当前 `AUTH` 没有真正的认证配置面；它只是返回 Redis 风格的 no-password-configured 错误，避免常见 Redis 客户端握手时出现未知命令。

### 2. Key / TTL / Introspection

来自 `KeyCommands`：

- `TYPE`
- `MEMORY USAGE`
- `MEMORY STATS`
- `OBJECT ENCODING`
- `KEYS`
- `SCAN`
- `DEL`
- `EXISTS`
- `EXPIRE`
- `PEXPIRE`
- `EXPIREAT`
- `PEXPIREAT`
- `PERSIST`
- `TTL`
- `PTTL`

这一组命令最适合拿来观察“key 生命周期”和“内部编码”。

### 3. String / Bitmap

来自 `StringCommands`：

- `SET`
- `GET`
- `STRLEN`
- `APPEND`
- `SETBIT`
- `GETBIT`
- `BITCOUNT`
- `INCR`
- `DECR`

注意这里的 bitmap 并没有单独的逻辑类型，而是建立在 string bytes 之上。

### 4. HLL

来自 `HllCommands`：

- `PFADD`
- `PFCOUNT`
- `PFMERGE`

HLL 在逻辑上是一组独立命令，但在存储上并不是独立 `ValueType`。它复用了 string 容器，内部值由 `YierdisHyperLogLog` 约定。

### 5. List

来自 `ListCommands`：

- `LPUSH`
- `RPUSH`
- `LRANGE`
- `LPOP`
- `RPOP`

### 6. Hash

来自 `HashCommands`：

- `HSET`
- `HGET`
- `HGETALL`
- `HLEN`
- `HDEL`

### 7. Set

来自 `SetCommands`：

- `SADD`
- `SREM`
- `SMEMBERS`
- `SISMEMBER`
- `SCARD`

### 8. ZSet

来自 `ZSetCommands`：

- `ZADD`
- `ZRANGE`
- `ZREVRANGE`
- `ZRANGEBYSCORE`
- `ZREVRANGEBYSCORE`
- `ZREMRANGEBYSCORE`
- `ZREMRANGEBYRANK`
- `ZREM`

### 9. Server-only Extra Commands

来自 `ServerCommandModule`：

- `HELLO`
- `INFO`
- `STATS`

这一组命令故意不放在 `yierdis-command-builtin`，因为它们依赖 server runtime 里的真实统计和 build info。

## 从命令层到 DB 能力边界

命令实现一般不会直接依赖 `YierdisDb` 本体，而是先通过 `CommandSupport` 取得：

- `DbReads`
- `DbWrites`
- `DbEngine`

这样做有三个结果：

1. 命令层只关心“能力接口”，不关心 DB 细节
2. `YierdisDb` 内部可以继续拆分协作者
3. 多 DB 路由只需要返回不同 `DbEngine`

这也是为什么你在命令代码里经常看到的是：

- `support.dbReads(ctx).strings()`
- `support.dbWrites(ctx).keyspace()`

而不是直接 new 或调用具体数据结构类。

## DB 结果如何流式写回

集合读命令不会要求 DB 先构造完整的 `List<byte[]>` 再交给命令层。DB API 里有一组
专门的 bulk-string 结果接口：

- `BulkStringValue`
  表达单个 bulk string，可以是 null、heap bytes、`BytesSlice`、off-heap slice 或
  long-ascii。
- `BulkStringSequence`
  表达一组 bulk string，例如 `LRANGE`、`SMEMBERS`、`ZRANGE`。
- `BulkStringMapPairs`
  表达 field/value pair 序列，例如 `HGETALL`。
- `BulkStringSink`
  是 DB/value 层向外写 bulk string 的中立端口。
- `BulkStringReplyAdapter`
  在 command 层把 `BulkStringSink` 适配到 `ReplyWriter`。

命令层的固定顺序是：

```text
read DB result
  -> out.arrayHeader(...) / out.mapHeader(...)
  -> result.emitTo(new BulkStringReplyAdapter(out))
```

这条边界让 DB/value/off-heap 代码不依赖 `ReplyWriter` 或 RESP；同时又允许
`BytesSlice`、`YierdisFfmBytesRefSlice`、`OffHeapSlice` 这类对象直接写到 reply sink，
避免为了回包把集合结果全部复制成 heap byte array。

## 逻辑类型和内部编码

对初学者来说，最重要的一点是：

- 用户看到的是“逻辑类型”
- DB 真正存的是“逻辑类型 + 内部编码”

### `ValueType`

逻辑类型定义得很少，只有：

- `STRING`
- `LIST`
- `SET`
- `HASH`
- `ZSET`

这说明项目的重点不在“类型枚举很多”，而在“每种类型内部会随着数据规模切换编码”。

### `ValueEncoding`

内部编码包括：

- string：`STRING_INT` / `STRING_EMBSTR` / `STRING_RAW`
- hash：`HASH_PACKED` / `HASH_HT`
- list：`LIST_PACKED` / `LIST_QUICKLIST`
- set：`SET_INTSET` / `SET_HT`
- zset：`ZSET_PACKED` / `ZSET_SKIPLIST`

对外通过 `OBJECT ENCODING key` 看到的名字会被格式化成更熟悉的 Redis 风格：

- `int`
- `embstr`
- `raw`
- `listpack`
- `hashtable`
- `intset`
- `quicklist`
- `skiplist`

## String：为什么会有 `int / embstr / raw`

string 由 `EntryRecord` 记录类型和 encoding，由 `StringRoot` 通过
`ValueHandle` 持有真实 bytes。这里的 `ValueHandle` 是 `NativeHandle` raw value 的包装，
带有 `STRING_BYTES` kind；它不是 native physical address。

大致规则是：

- 如果值能解析成 long，用 `STRING_INT`
- 如果是普通字符串且较短，用 `STRING_EMBSTR`
- 如果比较长，用 `STRING_RAW`

这背后的设计意图和 Redis 很像：

- 小整数避免重复存字符串 bytes
- 短字符串用更紧凑的表示
- 长字符串走普通 raw 存储

`SET`、`APPEND`、`INCR`、`DECR` 这几类命令最能把这一点看清楚。

## Handle 和物理存储的边界

命令层不直接接触 `EntryHandle`、`ValueHandle` 或 allocator。它只通过 DB API 读写逻辑类型。

DB 内部的主图是：

```text
NativeKeyDirectory
  key bytes -> EntryHandle(raw NativeHandle)

EntryTable
  EntryHandle -> native ENTRY_RECORD

EntryRecord
  ValueType + ValueEncoding + ValueHandle(raw NativeHandle) + TTL/accounting/access metadata

TypeRoot
  ValueHandle -> allocator-backed root/string object -> payload adapter / off-heap bytes
```

`EntryHandle` 要求 handle kind 是 `ENTRY_RECORD`，并且由 stable allocator object table 背书；`ValueHandle` 按 string/list/hash/set/zset
写入对应 `NativeObjectKind`。string 使用 allocator-backed `STRING_BYTES`，集合 root 使用 allocator-backed `LIST_NODE` / `HASH_NODE` / `SET_NODE` / `ZSET_NODE` root records；`LIST_QUICKLIST_NODE` metadata records 也进入 allocator。集合 payload bytes 和 hash/set/zset internals 仍有 adapter-owned / legacy FFM-owned 边界，不能把任意集合内部 identity 当作 allocator object resolve。allocator 可以移动这些 allocator-backed objects 的 physical block，DB graph 仍然保存同一个 stable handle。

这层细节在命令语义里通常不可见，但它解释了为什么 `MEMORY`、`OBJECT ENCODING`、TTL、delete 和 active defrag 都必须以 entry metadata 和 stable handle 为准。完整 allocator 语义见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## Hash：packed 什么时候升级成 hashtable

`HashValue` 先尝试用 packed 表示，也就是 listpack 风格的紧凑布局；当数据变大或元素变大时，再升级成 hashtable。

升级条件主要和两类阈值有关：

- entry 数量
- field/value 的字节大小

对应常量在 `YierdisEncodingThresholds`：

- `HASH_MAX_LISTPACK_ENTRIES = 512`
- `HASH_MAX_LISTPACK_VALUE_BYTES = 64`

## List：为什么叫 quicklist 风格

`ListValue` 小数据时用 packed listpack；超过阈值后升级为 quicklist 风格。

这里的“quicklist 风格”并不是在逐字节复刻 Redis 源码，而是复刻它的设计思路：

- 小列表尽量紧凑
- 大列表拆成多个 node，避免单一大块结构持续扩张

当前 list node 的紧凑块大小近似由：

- `LIST_MAX_LISTPACK_BYTES = 8 * 1024`

控制。

## Set：为什么先用 intset

`SetValue` 一开始优先尝试 intset 路线，因为纯整数集合在空间上更划算。

一旦出现：

- 非整数 member
- 或元素数量超过阈值

就会升级成 hashtable。

关键阈值是：

- `SET_MAX_INTSET_ENTRIES = 512`

## ZSet：packed 和 skiplist 的切换

`ZSetValue` 小数据时使用 packed 表示；数据规模变大后升级成：

- member -> node 的字典
- score 有序视图的 skiplist

这和 Redis 的“dict + skiplist”思路是一致的，只是实现细节更简化。

关键阈值是：

- `ZSET_MAX_LISTPACK_ENTRIES = 128`
- `ZSET_MAX_LISTPACK_VALUE_BYTES = 64`

## HLL：为什么没有独立 `ValueType`

很多初学者第一次读到这里会困惑：

- 命令里明明有 `PFADD/PFCOUNT/PFMERGE`
- 为什么 `ValueType` 里没有 `HLL`

答案是：这个项目把 HLL 当成“特殊格式的 string payload”。

也就是说：

- 命令层有 `HllCommands`
- DB 层有 `YierdisHllOps`
- 但底层对象仍然是 `ValueType.STRING`
- 是否是 HLL 由 `YierdisHyperLogLog.isHllString(...)` 判断

这样做的结果是：

- 逻辑命令可以单独存在
- 类型系统本身不必再新增一套分支

## FFM 对这些编码有什么影响

如果启用了默认的 FFM runtime，这些值类通常都有两套路径：

- 纯 Java heap 路径
- 基于 FFM blob store / native structure 的路径

但“逻辑类型 -> 内部编码”的规则本身没有变，变的是底层 payload 放在哪里。

想继续追这部分，建议接着看：

- [`ffm-usage.md`](./ffm-usage.md)
- [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)

## 初学者最值得看的测试

想理解命令和数据结构，不建议直接从最大类开始啃。更好的顺序是：

1. `CommandProcessorTest`
   看主流程和基础行为
2. `CommandErrorTest`
   看 arity、syntax、wrongtype 的约束
3. `TransactionCommandTest`
   看 `MULTI/EXEC/DISCARD` 的连接级语义
4. `SetCommandTest` / `HashCommandTest` / `ListCommandTest` / `ZSetCommandTest` / `HllCommandTest`
   看每个命令家族的行为面
5. `HashValueTest` / `ListValueTest` / `ZSetValueTest`
   看内部值类如何编码和升级
6. `MemoryStatsCommandTest`
   看 introspection 命令怎么把内部状态暴露出来

如果你想继续把这些命令最终落到 DB 内核的 key lifecycle、mutation executor 和 memory ledger 上，再看 [`db-internals.md`](./db-internals.md)。

## 一句话总结

可以把这一层记成：

“命令模块决定调哪个 DB 能力，值类决定这个逻辑类型此刻该用哪种内部编码；Redis 风格真正被复刻的重点就在这里。”
