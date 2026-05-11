# Glossary

本文把仓库里反复出现、但对初学者不够直观的词汇集中解释一下。读源码时如果遇到某个名词总觉得“好像懂，但又不确定”，可以先回来看这里。

## Request / Reply Path

### `ExecutionRequest`

执行层看到的“命令请求”抽象。它本质上是 argv 风格视图，命令层通过它读取：

- `argc`
- 某个参数是不是 `null`
- 某个参数的长度和字节内容

它不是协议 DTO。协议层必须先经过 `RespCommandAdapter` / `RespExecutionAdapter` 才会变成 `ExecutionRequest`。

### `ExecutionRecord`

用于记录“执行过的命令快照”的对象，主要给事务回放和变更事件使用。可以把它理解成“附带 DB index 的命令归档条目”。

### `ReplyWriter`

server 命令写回的单一语义出口。命令处理器和命令模块不会自己拼 RESP 字节，而是统一调用：

- `simpleString`
- `bulkString`
- `integer`
- `arrayHeader`
- `mapHeader`
- `error`

然后由具体协议实现把这些语义编码成线上格式。

### `RespReplyWriter`

`ReplyWriter` 在 RESP 下的具体实现。它负责把 reply 变成：

- RESP2 默认回包，例如 `+OK`、bulk string、array、error
- `HELLO 3` 后的基础 RESP3 回包，例如 map、null、bool、double

### protocol DTO

指协议层专用的数据对象，例如 `RespCommandRequest`。它只描述“协议长什么样”，不直接参与命令执行。

### RESP3 negotiated reply

指连接执行 `HELLO 3` 后启用的基础 RESP3 回包形态，例如 map、null、bool、double。命令层仍只调用 `ReplyWriter` 语义 API。

## Command Layer

### `CommandContext`

命令执行时随请求一起传下去的上下文。它通常提供：

- `ReplyWriter`
- 当前连接会话
- 当前 DB index 提供者

可以把它理解成“执行本次命令所需的环境对象”。

### `CommandRegistry`

命令名到 `CommandSpec` 的注册表。`YierdisFastCommandProcessor` 在构造阶段先注册 `TransactionCommands`，再把注入的命令模块注册到这里；生产默认命令来自 `DefaultCommandModules`。

### `CommandDescriptor`

命令元数据，包含 arity 和 key 位置信息。`COMMAND INFO` 的很多数据就来自这里。

### `CommandSupport`

命令实现的公共工具箱。它帮各个 `*Commands` 类做：

- 取参数
- 解析整数
- 拿到 `DbReads/DbWrites`
- 复用 scratch buffer

### `YierdisFastCommandProcessor`

server 侧真正执行命令的处理器。它负责：

- 做早期参数和空值校验
- 处理事务队列逻辑
- 根据命令名找到 handler
- 捕获 `WRONGTYPE` / command error / OOM 等异常
- 通过 `ReplyWriter` 写回结果

## DB / Runtime

### `DbEngine`

命令层看到的数据库能力视图。它通常再拆成：

- `DbReads`
- `DbWrites`
- `memory()`
- 其他按能力分组的接口

它的存在是为了让命令层依赖“能力边界”，而不是直接依赖 `YierdisDb` 实现类。

### `YierdisDb`

单个逻辑 DB 的状态 owner。它不是一个“大 Map 类”，而是把：

- keyspace
- expire index
- 各种 `*Ops`
- memory ledger
- mutation executor

这些协作者组织在一起的核心对象。

### `YierdisInstance`

实例级资源 owner。它关心的是：

- 一共有几个逻辑 DB
- FFM runtime 怎么分配
- maxmemory 是全局还是 per-db
- 对外暴露哪些 runtime seam

不要把它和单个 `YierdisDb` 混为一谈。

### owner thread

真正允许访问 DB 状态的那条线程。在 server 里，这通常就是 command executor 的执行线程。

这个概念非常关键，因为它解释了为什么：

- Netty 可以多线程收包
- 但 DB 仍然保持单线程命令语义

### runtime seam

指 runtime 层暴露给 server 的那条“受控接缝”。比如 owner-thread 绑定、maintenance 调度、关闭过程，不希望 server 直接向下转型或内联细节，而是通过 runtime seam 协作。

### `NettyExecutionConnection`

连接级状态总入口。它把下面这些连接态放在一起：

- session
- transaction queue
- pending / pendingBytes
- closing 标记
- backpressure 相关状态

如果你在查 `SELECT`、`MULTI` 或背压，几乎一定会碰到它。

### maintenance tick

后台周期性维护动作，目前最典型的是过期清理。虽然定时器由 worker event loop 触发，但真正执行 cleanup 的地方仍然回到 owner thread。

## Keyspace / TTL / Memory

### keyspace

主索引现在由 `NativeKeyDirectory` 承担，也就是“key 到 `EntryHandle`”的映射。`EntryHandle` 再进入 `EntryTable` 取得 `EntryRecord`，读写一个 key 时，绝大多数路径都会先碰到这组结构。

### expire index

记录“key 到过期时间”的辅助索引。它不存真正的 value，只负责 TTL 相关的元数据。

### `KeyHandle`

DB 内部对 key 的句柄化表示。它用于把 key 的生命周期、内存统计和索引更新放在更稳定的内部抽象上，而不是到处传裸 `byte[]`。

### `YierdisDbKeyLifecycle`

围绕 key 生命周期的协作者。它负责处理：

- 取仍未过期的 `EntryRecord`
- 判断和删除过期 key
- 更新 expire 元数据
- 触摸访问时间

### `YierdisDbMutationExecutor`

DB 内部受控写路径的执行器。很多写命令不会直接改状态，而是先构造 mutation plan，再通过它执行，以便统一处理内存预算和副作用。

### maxmemory

实例或 DB 的最大内存预算。超过预算时：

- `noeviction` 会报错
- `allkeys-random` / `allkeys-lru` 会尝试淘汰

它是当前项目唯一公开的 native-memory 预算入口。

### backpressure

当执行器队列或单连接 pending 达到阈值时，server 暂停继续从 socket 读请求的机制。最直接的表现通常是：

- channel `autoRead=false`
- `ERR busy ...`

### retained bytes

协议请求在被解码成 argv 后，逻辑上“保留下来”的参数字节数估计。它主要用于排队、事务队列和预算控制，而不是给业务代码直接使用。

## Data Model

### `EntryRecord`

DB native key graph 的 entry metadata。它持有：

- `ValueType`
- `ValueEncoding`
- `ValueHandle`
- expireAt
- 估算字节数
- LRU / LFU 相关字段

真实 payload 由对应的 `TypeRoot` 通过 `ValueHandle` 管理。

### `ValueType`

逻辑类型枚举。当前只有：

- `STRING`
- `LIST`
- `SET`
- `HASH`
- `ZSET`

### `ValueEncoding`

内部编码枚举，用来表达某个逻辑类型此刻的具体表示方式，例如：

- `STRING_INT`
- `HASH_PACKED`
- `SET_INTSET`
- `ZSET_SKIPLIST`

### listpack

紧凑编码的统称。在这个项目里，hash/list/zset 的“小对象模式”都会尽量往 listpack 风格靠拢。

### quicklist

list 的“大对象模式”。可以把它理解成“多个紧凑节点串起来”，避免一个大列表永远用单块结构表示。

### intset

set 的整数紧凑编码。只在成员都是整数、且规模不大时使用。

### skiplist

zset 的有序大对象编码，用来支持按 score 有序访问和范围查询。

### HLL string

虽然有 `PFADD/PFCOUNT/PFMERGE`，但 HLL 在底层并不是独立 `ValueType`，而是“具有特定 header 和 payload 的 string”。

## FFM / Native Memory

### FFM runtime

这里通常指 `YierdisFfmMemoryRuntime` 一类对象，也就是整个 native-memory 路径的底座。项目当前统一使用 JDK 25 `java.lang.foreign`。

### blob store

在 FFM 路径中，用来存放变长 bytes 的 off-heap 容器。hash/list/set/zset 等值类会用它来保存成员或元素内容，而不是把所有数据都塞进 heap `byte[]`。

### off-heap

指不在 JVM heap 里的那部分内存。这个项目默认会把很多内部结构放到 FFM/off-heap 路径中，但观测、预算和编码语义仍然尽量保持统一。

## 读代码时怎么用这份术语表

一个实用方法是：

1. 看到类名先判断它属于 protocol、command、db、runtime 还是 memory
2. 再用这份术语表给它贴一个角色标签
3. 最后再读它的实现细节

这样会比“直接逐行啃源码”轻松很多。
