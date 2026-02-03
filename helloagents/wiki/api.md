# 命令 / API 手册（RESP2/RESP3）

## 概览

本项目对外暴露的“API”即 Redis 风格命令（RESP over TCP）：默认 RESP2，支持最小 RESP3（`HELLO 3` 协商后切换），并支持 inline command（便于调试，支持引号/转义/`\\xHH`）。

> 内置 `YierdisCli` 仍使用 RESP2 发送命令；如需使用 RESP3，请用 `redis-cli --resp3`。

> 注意：当前实现的“RESP3 支持”仍以 **reply 侧类型覆盖** 为主；request 侧主路径是 RESP2 的 multi-bulk/inline（与 Redis 生态客户端一致），但额外兼容 RESP3 `|` attributes 前缀与 `*` 命令数组内的部分 RESP3 标量类型（作为参数映射为 argv bytes）。
> 这并不等价于完整 RESP3 request：不支持聚合类型作为参数，遇到不支持的类型会返回 protocol error。

> 注意：该手册描述的是当前已实现能力；新增能力以 `helloagents/plan/` 与 `helloagents/history/` 为准（以代码为准，文档做同步）。

---

## 已实现命令（简化版）

### 通用
- `PING [message]`
- `ECHO <message>`
- `HELLO [2|3]`（支持 2/3；`HELLO 3` 会切换连接为 RESP3）
- `COMMAND`（最小子集：`COMMAND`/`COMMAND COUNT`/`COMMAND INFO <name ...>`）
- `SELECT <index>`（默认支持 `0..15`；可通过 `--databases` 调整）
- `QUIT`
- `INFO [section]`（Redis bulk string 形态；`section` 目前为最小子集）
- `STATS`（结构化统计；用于排障/教学）

#### HELLO（最小子集）

- `HELLO 2`：使用 RESP2 回复（array of bulk strings）
- `HELLO 3`：切换连接为 RESP3 回复，并返回 RESP3 map（`%...`）
- 返回字段包含：`server/version/proto/mode/role`
- `version` 来自构建版本（`project.version` 资源注入），避免硬编码常量漂移

### Key/TTL
- `SET key value [EX seconds|PX milliseconds] [NX|XX]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `PEXPIRE key milliseconds`
- `EXPIREAT key unix_seconds`
- `PEXPIREAT key unix_milliseconds`
- `PERSIST key`
- `TTL key`
- `PTTL key`
- `KEYS pattern`（支持 Redis 风格最小 glob 子集：`*`/`?`/`[]` 范围与否定/反斜杠转义；按 byte 匹配）
- `SCAN cursor [MATCH pattern] [COUNT n]`（best-effort；cursor 为“遍历顺序偏移量”的语义）
- `TYPE key`
- `FLUSHDB [SYNC|ASYNC]`（本实现为单线程执行器，二者语义等价）
- `OBJECT ENCODING key`（教学用：查看内部编码）
- `MEMORY USAGE key`
- `MEMORY STATS`（RESP2 flat array / RESP3 map）

### String
- `INCR key`
- `DECR key`
- `APPEND key value`
- `STRLEN key`

### BITMAP（复用 STRING）
- `SETBIT key offset value`（value 仅支持 0/1；offset 超大时会被拒绝以避免不可控内存分配）
- `GETBIT key offset`
- `BITCOUNT key [start end]`（start/end 为 byte-range，支持负数索引）

### HyperLogLog / HLL（复用 STRING）
- `PFADD key element [element ...]`（不存在则创建；若 key 存在但不是 HLL string，则返回 `WRONGTYPE`）
- `PFCOUNT key [key ...]`
- `PFMERGE destkey sourcekey [sourcekey ...]`（结果写入 destkey，写入后会清除 destkey 的 TTL）

### List
- `LPUSH key value [value ...]`
- `RPUSH key value [value ...]`
- `LRANGE key start stop`
- `LPOP key [count]`
- `RPOP key [count]`

### Hash
- `HSET key field value [field value ...]`
- `HGET key field`
- `HGETALL key`
- `HLEN key`
- `HDEL key field [field ...]`

### Set
- `SADD key member [member ...]`
- `SMEMBERS key`
- `SISMEMBER key member`
- `SCARD key`

### ZSet
- `ZADD key score member [score member ...]`
- `ZRANGE key start stop [WITHSCORES] [REV]`
- `ZREVRANGE key start stop [WITHSCORES]`
- `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
- `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`
- `ZREMRANGEBYRANK key start stop`
- `ZREMRANGEBYSCORE key min max`
- `ZREM key member [member ...]`

### Transaction（最小子集）
- `MULTI`
- `EXEC`
- `DISCARD`

事务边界与兼容性提示（重要）：
- 事务队列为连接级状态，server 提供条数/bytes 的硬上限（防 OOM；触发错误会进入 aborted，后续 `EXEC` 返回 `EXECABORT` 并丢弃队列）。
- `HELLO` 属于连接级协议协商命令（RESP2/RESP3 握手），为避免在 `EXEC` reply 中混入不同协议类型前缀导致客户端解析失败，MULTI 模式下禁止 `HELLO`（返回错误并触发 `EXECABORT`）。
