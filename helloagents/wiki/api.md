# 命令 / API 手册（RESP2/RESP3）

## 概览

本项目对外暴露的“API”即 Redis 风格命令（RESP over TCP）：默认 RESP2，支持最小 RESP3（`HELLO 3` 协商后切换），并支持 inline command（便于调试，支持引号/转义/`\\xHH`）。

> 内置 `YierdisCli` 仍使用 RESP2 发送命令；如需使用 RESP3，请用 `redis-cli --resp3`。

> 注意：该手册描述的是当前已实现能力；新增能力以 `helloagents/plan/` 与 `helloagents/history/` 为准。

---

## 已实现命令（简化版）

### 通用
- `PING [message]`
- `ECHO <message>`
- `HELLO [2|3]`（支持 2/3；`HELLO 3` 会切换连接为 RESP3）
- `COMMAND`（返回空数组）
- `SELECT 0`
- `QUIT`

### Key/TTL
- `SET key value [EX seconds|PX milliseconds] [NX|XX]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `TTL key`
- `KEYS pattern`（仅支持 `*` 和简单 glob）
- `TYPE key`
- `FLUSHDB`

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
