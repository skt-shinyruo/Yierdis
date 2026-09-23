# 命令层与数据模型

命令层注册、解析和执行命令，并把命令语义映射到 DB capability、逻辑类型和内部编码。本页讲命令抽象、命令家族、streamed result 和逻辑类型模型。

## 命令层职责

命令层位于协议和 DB 之间：接收 transport-neutral 的 `ExecutionRequest`，选出 `CommandSpec`，用 `CommandArgs` 解析参数，调用 handler 返回的准备函数准备 DB 操作，最后返回语义 `CommandResult`。

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

命令层不解析 RESP bytes，不直接管理 allocator、entry table 或 value root，也不写 reply sink。`RedisReplyWriter` 是 renderer 的 RESP-facing port，只出现在 executor 调用的 `RedisReplyRenderer` 一侧；command implementation 只构造 `RedisReply`。

## 分发与事务专题入口

- 查表、`CommandArgs`、parse error、unknown command 和 transaction preflight 见 [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)；
- `MULTI/EXEC/DISCARD`、retained request、abort、replay 和 queue limit 见 [`transaction-and-replay.md`](./transaction-and-replay.md)；
- 从 Netty 到 renderer 的完整主路径见 [`request-execution-flow.md`](./request-execution-flow.md)。

## `CommandRegistry`、`CommandSpec` 和 handler

`YierdisServerBootstrap` 调用 `CommandRegistries.dispatcher(...)` 创建 `CommandRegistry` 与 `CommandDispatcher`。registry 依次接收 `DefaultCommandModules.create(...)` 和 `ServerCommandModule`，其中 `MULTI/EXEC/DISCARD` 由 `CommandRegistries.dispatcher` 里的 `TransactionCommands` 先注册，最后 seal。

`CommandRegistry` 是 upper-case command name 到 `CommandSpec` 的单一映射，底层是 `LinkedHashMap`，seal 时用 `Map.copyOf` 冻结。`CommandSpec` 只有两个字段：

- `CommandSyntax`：name、`CommandArity`、`CommandKeySpec`、`TransactionPolicy` 和 `ReplyAdmissionRequirement`；
- `CommandHandler`：`parse(CommandArgs)`，成功时返回 `Function<CommandSession, PreparedCommand>`。

`CommandSyntax` 构造时把名称 trim + 大写，并校验非空且全 ASCII；非法名称在启动注册阶段就抛 `IllegalArgumentException`，不会流入运行时。`DefaultCommandModules` 创建的八个命令家族统一使用 `TransactionPolicy.QUEUEABLE` 和 `ReplyAdmissionRequirement.PIPELINED`，只有 `HELLO`（`DISALLOWED_IN_MULTI`）和 `EXEC`（`BARRIER_UNTIL_CLEANUP`）是例外。

dispatcher 先检查命令名、null argument、lookup 和 arity，再调用 handler。handler 只读 argv、生成不可变的解析结果，不读 session、不路由 DB、也不调用 server provider。这条限制让普通执行与 `MULTI` 入队 preflight 共用同一套 parse 行为。

handler 返回的准备函数是参数解析与状态访问之间的边界。它在 `apply(CommandSession)` 时可以根据当前 DB index 和连接状态：

- 准备带 validation 的 mutation；
- 执行只读查询并取得有所有权的 result source；
- 返回固定语义回复；
- 构造在执行阶段调用 DB capability 的 action。

无论哪种分支，最终都返回 `PreparedCommand`，由 executor 负责容量预留、validation、execution、render 和 close。

## 参数解析和错误

参数处理集中在两个层次：

- `CommandArity` 表达 exact、min、range、one-of 和 pair-tail 等 argc 规则；`CommandArity.exact(2)` 只接受 argc==2，`oneOf` 接受枚举值（如 `LPOP` 的 `oneOf(2, 3)`），`pairTail(4, 2)` 要求 argc>=4 且从索引 2 起成对（`HSET`）；
- `CommandArgs` 提供 argv shape、`BytesSlice`、byte array、ASCII literal、UTF-8 和整数读取。

wrong arity 在进入 handler 前由 dispatcher 生成，文案为 `ERR wrong number of arguments for '<小写命令名>' command`（多命令名如 `client|setinfo` 用竖线拼接）。命令特有的 option、subcommand、cursor、score 和 integer 约束由 handler 检查，出错时抛出 `CommandParseException`，其中带上最终的 Redis error message。

parse 阶段只产生 `Function<CommandSession, PreparedCommand>`，不会访问 DB，也不会创建 reply source。`MULTI` 中的 queueable command 也走同一个 handler parse；成功后只 retain request 并返回 `QUEUED`，准备函数要到 `EXEC` replay 才会应用。

## 准备、执行和语义结果

`PreparedCommand` 把执行前和执行后的责任分开：

- `reservationShape()` 给出 encoded reply 与 retained source 的容量上界；
- `replyProtocolVersion()` 可选声明本次回复的目标 RESP 版本（`HELLO` 用它声明协商后版本，其余命令返回空，由 executor 取 session 当前版本）；
- `validateBeforeExecute()` 检查 prepare 时观察的状态是否仍可执行；
- `execute(CommandSession)` 在容量已预留时提交动作，返回 `CommandResult`；
- `close()` 归还 mutation、DB source、retained request 或其他 owner。

`CommandSupport.preparedMutation(...)` 与 `PreparedCommands.ownedAction(...)` 是构造这些对象的两个主要入口：前者把 `PreparedMutation.isCurrent()` 接到 validation，后者把任意 `AutoCloseable` owner 挂到 prepared command 上。

executor 把当前 `CommandSession` 直接传给 `PreparedCommand.execute(...)`。command API 没有 reply writer；`CommandResult` 包含语义 `RedisReply` 和 `closeAfterReply` flag。

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

RESP2 / RESP3 的标量与 aggregate 编码由协议 writer 根据 session version 处理。`QUIT` 也不调用 writer；它返回 `CommandResult.closeAfterReply(SimpleString("OK"))`，由 executor 根据 flag 调用 `connection.markClosing()`，再由 ordered reply slot 完成关闭。

## `CommandSupport` 与 DB capability

`CommandSupport` 是 built-in command 的公共边界。它持有 `YierdisDbRouter`、可选 `ServerInfoProvider` 和不可变的 `SlowCommandLimits`，并提供：

- `commandDb(CommandSession)`：prepare 和 execute 阶段都按 session 的 DB index 走 `YierdisDbRouter.dbFor(session)` 选择数据库；
- `databases()`：`SELECT` 的索引上界；
- `infoProvider()` / `slowCommandLimits()`：`INFO`/`STATS` 与 `KEYS` 的时间预算来源。

`DbEngine` 是 DB 的语义操作边界，命令家族只经过它，不触碰 native handle 或 RESP bytes：

| 访问器 | 类型 | 代表方法 |
| --- | --- | --- |
| `strings()` | `StringOps` | `prepareSet`、`getStringValue`、`strlen`、`append`、`setBit`、`getBit`、`bitcount`、`incrBy` |
| `hashes()` | `HashOps` | `hset`、`hget`、`hgetall`、`hlen`、`hdel`、`hscan` |
| `lists()` | `ListOps` | `lpush`、`rpush`、`lrange`、`preparePop` |
| `sets()` | `SetOps` | `sadd`、`srem`、`smembers`、`sismember`、`scard`、`sscan` |
| `zsets()` | `ZSetOps` | `zadd`、`zrange`、`zrevrange`、`zrangeByScore`、`zrevrangeByScore`、`zremrangeByScore`、`zremrangeByRank`、`zrem`、`zscan` |
| `hll()` | `HllOps` | `pfadd`、`pfcount`、`pfmerge` |
| `keyspace()` | `KeyspaceOps` | `typeOf`、`keys`、`scan`、`del`、`existsKey` |
| `ttl()` | `TtlOps` | `expire`、`pexpire`、`expireAtSeconds`、`expireAtMillis`、`persist`、`ttlSeconds`、`ttlMillis` |
| — | `DbEngine` 直接方法 | `memoryUsage`、`memoryStats`、`objectEncoding`、`flushDb`、`flushDbAsync` |

`CommandSupport.preparedMutation(...)` 把 `PreparedMutation.isCurrent()` 接到 validation，将 mutation owner 交给 `PreparedCommand`，并在 execute 中通过 `translateExpectedCommandFailure` 把 `WrongTypeException` / `YierdisCommandException` 转成 `CommandResult.controlError(...)`。

## 命令家族与 ops 映射

| 家族 | 模块 | 命令 → ops |
| --- | --- | --- |
| connection/server | `CoreConnectionCommands`、`ServerCommandModule` | `SELECT`/`FLUSHDB` 用会话与 `flushDb*`；`INFO`/`STATS`/`MEMORY STATS` 走 `ServerInfoProvider`；`PING`/`ECHO`/`COMMAND`/`QUIT`/`CLIENT`/`AUTH`/`HELLO` 不访问 DB |
| key/TTL | `KeyCommands` | `TYPE`→`keyspace().typeOf`；`KEYS`→`keyspace().keys`；`SCAN`→`keyspace().scan`；`DEL`→`keyspace().del`；`EXISTS`→`keyspace().existsKey`；四个 EXPIRE 系列→`ttl().expire/pexpire/expireAtSeconds/expireAtMillis`；`PERSIST`→`ttl().persist`；`TTL`/`PTTL`→`ttl().ttlSeconds/ttlMillis`；`MEMORY USAGE`→`memoryUsage`；`MEMORY STATS`→`memoryStats`；`OBJECT ENCODING`→`objectEncoding` |
| string/bitmap | `StringCommands` | `SET`→`prepareSet`；`GET`→`getStringValue`；`STRLEN`→`strlen`；`APPEND`→`append`；`SETBIT`/`GETBIT`→`setBit`/`getBit`；`BITCOUNT`→`bitcount`；`INCR`/`DECR`→`incrBy(key, ±1)` |
| HLL | `HllCommands` | `pfadd`/`pfcount`/`pfmerge` |
| list | `ListCommands` | `lpush`/`rpush`/`lrange`/`preparePop` |
| hash | `HashCommands` | `hset`/`hget`/`hgetall`/`hlen`/`hdel`/`hscan` |
| set | `SetCommands` | `sadd`/`srem`/`smembers`/`sismember`/`scard`/`sscan` |
| zset | `ZSetCommands` | `zadd`/`zrange`/`zrevrange`/`zrangeByScore`/`zrevrangeByScore`/`zremrangeByScore`/`zremrangeByRank`/`zrem`/`zscan` |
| transaction | `TransactionCommands` | `MULTI`/`EXEC`/`DISCARD` 只操作 `TransactionState`，不访问 DB |

命令家族当前只覆盖已实现范围内的 Redis-style minimum subset。哪些命令名已注册、哪些 Redis 命令尚未实现，见 [`protocol-reference.md`](./protocol-reference.md) 的「已注册命令清单」一节。

## 逻辑类型和内部编码

用户看到逻辑类型，DB 记录逻辑类型与内部编码。`ValueType` 当前包括 `STRING`、`LIST`、`SET`、`HASH`、`ZSET`。

`ValueEncoding` 的映射（`internal/value/ValueEncoding.java`）：

| 逻辑类型 | 内部编码 |
| --- | --- |
| string | `STRING_INT`、`STRING_EMBSTR`、`STRING_RAW` |
| hash | `HASH_PACKED`、`HASH_HT` |
| list | `LIST_PACKED`、`LIST_QUICKLIST` |
| set | `SET_INTSET`、`SET_HT` |
| zset | `ZSET_PACKED`、`ZSET_SKIPLIST` |

`OBJECT ENCODING key` 把内部编码格式化为 Redis 风格名称（`YierdisDb.encodingName`）：

| `ValueEncoding` | 输出 |
| --- | --- |
| `STRING_INT` / `STRING_EMBSTR` / `STRING_RAW` | `int` / `embstr` / `raw` |
| `HASH_PACKED` / `LIST_PACKED` / `ZSET_PACKED` | `listpack` |
| `HASH_HT` / `SET_HT` | `hashtable` |
| `SET_INTSET` | `intset` |
| `LIST_QUICKLIST` | `quicklist` |
| `ZSET_SKIPLIST` | `skiplist` |

编码选择与升级由 value/DB 层决定，命令 handler 不手工选择编码：

- hash 根据 entry 数或 field/value 长度从 packed 升为 hashtable；
- list 根据 compact block 约束从 packed 升为 quicklist；
- set 遇到非整数 member 或超过阈值时从 intset 升为 hashtable；
- zset 根据 entry 数或 member 长度从 packed 升为 skiplist；
- string 根据整数可解析性和长度选择 int、embstr 或 raw。

## native 与 heap 两侧的表示

同一个逻辑类型在 native 和 heap 两侧的落点不同，`NativeObjectKind` 是这份映射的权威枚举（`yierdis-db/.../memory/api/NativeObjectKind.java`）：

| 数据 | native 对象 | heap 侧 |
| --- | --- | --- |
| key | `KEY_BYTES`（`NativeKeyDirectory` 持久化 allocator-backed key bytes） | 查找输入、`SCAN`/`KEYS` 输出时的临时 view 或快照 |
| string payload | `STRING_BYTES`（`StringRoot`） | 解析整数、构造 embstr 时的短生命周期数组 |
| packed hash/list/zset | `LISTPACK_BYTES`（`NativeListpack`，hash 用 `HASH_FIELD_BYTES`/`HASH_VALUE_BYTES`） | `listpack.get(...)` 返回的 `byte[]` 元素 |
| hash/set/zset dictionary | `NativeByteMap` + `HASH_FIELD_BYTES`/`HASH_VALUE_BYTES`/`SET_MEMBER_BYTES`/`ZSET_MEMBER_BYTES`/`SCORE_BYTES` | `NativeByteMap` 的稀疏引用槽位 |
| set intset | —（纯 heap） | `short[]`/`int[]`/`long[]`（`intset16`/`intset32`/`intset64`） |
| zset skiplist 索引 | `ZSET_TABLE`/`ZSET_NODE` | `ZSkipList` forward/span 数组 |
| entry | `ENTRY_RECORD` | — |
| 类型 root | `LIST_ROOT`/`HASH_ROOT`/`SET_ROOT`/`ZSET_ROOT`、`LIST_NODE`、`*_TABLE` | root 上的 heap 计量与回调 |

关键点是：packed 编码不等于 heap 编码。hash/list/zset 的 compact 形态是 `NativeListpack`，只有 `SET_INTSET` 是真正 heap 数组；`SET_HT`/`HASH_HT`/`ZSET_SKIPLIST` 的 member/field 字节仍在 allocator 里，heap 上只留 dictionary 与索引骨架。allocator handle、entry table 和 off-heap payload 都属于 DB/value 层，命令层看不到。

## `StringCommands` 主路线

`SET`、`GET`、`APPEND`、`INCR` 等命令先由 handler 解析 argv，再由准备函数或 prepared action 使用 typed string ops。

- `SET` 在 prepare 阶段用 `prepareSet(key, value, mode, expire, getOld)` 创建 `PreparedMutation`；preview 决定回复（`GET` 旧值、`OK`、或 NX/XX 未命中时的 `NullValue`），executor 预留和 validation 后才 `commit()`；
- `GET` 在 prepare 阶段取得 `ByteValue`，用 `DbReplies.value(...)` 生成 semantic bulk reply，并以 `PreparedCommands.owned(...)` 持有 source，渲染后释放（native pin 在 `StringRoot.retainedValue` 取出时建立）；
- `APPEND`、`SETBIT`、`INCR`/`DECR` 等已知 reply 上界的动作在 execute 阶段访问 string typed ops，预留 `ReplyShapes.integerUpperBound()`。

bitmap 是 string bytes 的一种视图，因此 `SETBIT`、`GETBIT`、`BITCOUNT` 与普通 string 命令共享 `ValueType.STRING` 和 wrong-type 约束。`SETBIT` 在 parse 阶段就拒绝 offset/8 >= 512 MiB（`MAX_STRING_BYTES`），写路径仍经过 DB mutation、TTL 和 memory ledger；读路径返回 `RedisReply`，不直接编码 RESP。

`SET` 的选项解析有一条实现上的边界值得记住：`EX`/`PX`/`EXAT`/`PXAT` 要求参数为正，因此过去但为正的 `EXAT`/`PXAT` 不在 parse 阶段失败，而会在 execute 时先写后过期——这样 `MULTI` preflight 不会因为入队时刻的时钟把整个事务判成 `EXECABORT`。

## HLL 和 string

HLL 没有独立 `ValueType`。命令层由 `HllCommands` 表达语义，DB 层由 `HllOps` 处理，但底层对象仍是 `ValueType.STRING`；payload 是否为有效 HLL 由格式约定判断。payload 格式与 Redis 对齐（`HYLL` header、sparse/dense 编码），因此 PF* 对相同 member 给出 Redis 类计数。把普通 string 当作 HLL 使用会走 HLL 的格式校验并可能返回 `WRONGTYPE Key is not a valid HyperLogLog string value.`。

命令家族因此可以独立演进，主类型系统也不必为 bitmap 和 HLL 增加额外逻辑类型。

## HSCAN、SSCAN 和 ZSCAN

三条命令复用 `CollectionScanCommandSupport` 的参数规则：

```text
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
SSCAN key cursor [MATCH pattern] [COUNT count]
ZSCAN key cursor [MATCH pattern] [COUNT count]
```

规则：

- `cursor` 由 `ScanCursorV2.of(args.nonNegativeLongAt(2))` 解析，即不透明非负整数；负数与溢出报 `ERR value is not an integer or out of range`。旧 cursor 无法映射到当前表拓扑时由存储层按重启迭代处理，不会报错；
- `MATCH` 只匹配 field/member，不匹配 hash value 或 zset score；
- `COUNT` 默认 10（`DEFAULT_COUNT`），必须为正，超过 `Integer.MAX_VALUE` 报整数错误；
- option 可以换序；`NOVALUES` 只适用于 `HSCAN`，`SSCAN`/`ZSCAN` 上出现报 `ERR syntax error`。

三条命令都返回两元素 array：下一 cursor（bulk string）和元素 sequence。元素形状分别为：

- `HSCAN`：`field, value, ...`，`NOVALUES` 时只有 field；
- `SSCAN`：`member, ...`；
- `ZSCAN`：`member, score, ...`（score 走 bulk string）。

不存在的 key 返回 cursor `0` 与空 sequence，类型不匹配返回 `WRONGTYPE`。`COUNT` 是工作量 hint，不是精确页大小：

- `HASH_PACKED`、`SET_INTSET`、`ZSET_PACKED` 的 compact 分支在初始 cursor（0）上过滤并物化当前稳定 heap window，一次返回并终止；传入非 0 cursor 时直接返回空 window；
- `HASH_HT`、`SET_HT`、`ZSET_SKIPLIST` dictionary 分支按物理 slot 有界扫描，空槽与 `MATCH` 过滤会使实际返回少于 count，甚至为空但 cursor 非零。

dictionary window 只 pin 被选中的 native payload，并把延迟回收量放进 `retainedSourceBytes`。renderer 发射 window 后，executor 关闭它并 unpin。compact window 用 `MaterializedCollectionScanWindow` 持有稳定 heap byte arrays，也参与 reply admission。

这些 cursor 提供 Redis 风格弱一致迭代，不是快照或稳定顺序。并发新增、删除、rehash 和编码切换会影响可见元素，调用方应允许重复并自行去重；cursor 不能跨 DB、key 删除重建或集合生命周期长期保存。

## semantic streamed reply source

只读 DB API 不必先复制完整 payload，可以直接返回：

- `ByteValue`：单个 bulk-string 或 null；
- `ByteSequenceSource`：bulk-string sequence；
- `ByteMapSource`：field/value pairs；
- `CollectionScanWindow`：带 cursor 的一次 scan window。

`DbReplies` 把这些 source 转成 `RedisReply.BulkString` 或 `ByteAggregate`（kind 为 SEQUENCE/SET/MAP）；`DbReplies.singleValue` 用于 pop 单元素，`DbReplies.map` 用于 HGETALL。语义 reply 记录元素/键值对计数、payload lengths、retained source bytes 和同步 emitter；`PreparedCommands.owned(...)` 让 prepared command 持有 source。

`LRANGE`、`SMEMBERS`、`HGETALL` 和 `ZRANGE*` 在 prepare 时把选中元素拷进独立 source。这些集合会 in-place 改同一 native handle；reply-capacity 若把 render 推迟到其他连接写入之后，live emit 会和冻结的 elementCount 错位并拆开 RESP。GET 仍 pin native string；SCAN window 仍按既有 pin/materialize 规则，不保证跨 mutation 的快照。

executor 先把 source 的 retained memory 纳入 reply preflight，再执行并交给 renderer。renderer 在 command owner thread 同步调用 emitter；渲染完成后 executor 关闭 prepared command，source 才 unpin 或释放。source ownership 不会转移给 `RedisReplyWriter` 或 Netty event loop。

`EXEC` 的 streamed child 也遵守这一规则：child prepared command 持有 source，外层 transaction prepared command 持有 child，直到 renderer 消费完整个 aggregate 才逆序关闭。细节见 [`transaction-and-replay.md`](./transaction-and-replay.md)。

## 带状态的只读命令与重试

几个只读命令并不返回固定形状，而是返回「依赖当前状态」的 source 或错误，并需要在 validation 阶段确认它仍然成立：

- `KEYS`/`SCAN` 返回 `KeyScanWindow`，prepared command 的 validation 是 `window.current() ? VALID : STALE`；window 失效时 executor 重新 prepare（`ExecutionAttempt.REPREPARE`），最多尝试 `KEY_WINDOW_DISCOVERY_ATTEMPTS = 2` 次，仍失败则返回 `ERR scan window changed before reply preflight` 或 `ERR KEYS scan window changed...`；
- `KEYS` 还受 `SlowCommandLimits` 约束：一旦发现 `nextCursor` 非 0（结果被截断）就直接返回 `ERR KEYS scan incomplete; use SCAN`，不会给出不完整的 key 列表；
- `ZRANGE` 系列在 prepare 时若底层因参数不合法抛 `IllegalArgumentException`，会被包成 `ERR <message>` 的语义错误，而不是让异常逃到 executor。

## 事务中的命令语义

事务状态属于每连接 `CommandSession`。生产中的 `EngineSession` 就是该连接 session 的具体 owner，`TransactionState` 中保存 active、aborted、retained request queue 和 queue limits。

`MULTI` 后，queueable command 仍要经过 registry、arity、transaction policy 和 handler parse；全部通过之后才 retain `ExecutionRequest` 并返回 `QUEUED`（reply reservation 与 `tryEnqueue` 发生在 execute 阶段）。DB preparation 与 execution 不发生在排队阶段。

`EXEC` 对 retained requests 走同一条 dispatcher replay path，逐条得到 child `CommandResult`，聚合 `RedisReply` 后交回 executor 的单一 renderer。`DISCARD` 关闭队列并退出 transaction。

该实现是 Redis-style 的连接级排队与顺序重放，不隐含 `WATCH`、Lua 或 cluster transaction 语义。

## 新增命令时的路线

1. 确认命令 family，或实现新的 `CommandModule`。
2. 注册 `CommandSpec(CommandSyntax, CommandHandler)`，补齐 arity、key spec、transaction policy 和 reply admission requirement。
3. 在 `handler.parse(CommandArgs)` 中完成纯 argv 解析；错误抛 `CommandParseException`，不得访问 session 或 DB。
4. 返回 `Function<CommandSession, PreparedCommand>`，在 `apply(session)` 中取得当前 DB、准备 mutation/source，并构造 `PreparedCommand`。
5. 为 prepared command 给出真实 reservation shape；可变 preview 接上 validation，可见 mutation 留在 execute。
6. execute 返回 `CommandResult` 与语义 `RedisReply`；不要引用或调用 `RedisReplyWriter`。
7. streamed source 使用 `PreparedCommands.owned(...)` 或 `PreparedCommands.ownedAction(...)` 明确生命周期与 retained memory charge。
8. 补 handler parse-isolation、dispatcher、reply preflight、命令 family 和错误路径测试；server-only command 还应覆盖 server composition。

若命令暴露 encoding 或 memory 信息，还要同步检查 `OBJECT ENCODING`、`MEMORY USAGE` 和 `MEMORY STATS` 的行为；若命令返回新的 state-dependent shape，需要提供对应的 validation 与重试语义。