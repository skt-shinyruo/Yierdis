# 命令 / API 手册（RESP2）

## 概览

本项目对外暴露的“API”即 Redis 风格命令（RESP2 over TCP），可通过 `redis-cli --resp2` 或内置 `YierdisCli` 调用。

> 注意：该手册描述的是当前已实现能力；新增能力以 `helloagents/plan/` 与 `helloagents/history/` 为准。

---

## 已实现命令（简化版）

### 通用
- `PING [message]`
- `ECHO <message>`
- `HELLO [2|3]`（仅支持 2）
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

