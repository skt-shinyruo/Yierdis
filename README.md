# yierdis (Java 17 + Netty)

一个简化版的 Yierdis Server（兼容 Redis），实现 **RESP2 协议** 的核心子集，适合用来学习/演示 Netty 网络编程与 Redis 协议。

## 环境

- JDK 17
- Maven 3.x

## 启动

```bash
mvn -q -DskipTests package
java -jar target/yierdis-0.1.0-SNAPSHOT.jar --port 6378
```

然后可以用 `redis-cli` 连接：

```bash
redis-cli -p 6378 ping
redis-cli -p 6378 set a 1
redis-cli -p 6378 get a
redis-cli -p 6378 incr a
redis-cli -p 6378 expire a 10
redis-cli -p 6378 ttl a
```

## 已实现命令（简化版）

### 通用

- `PING [message]`
- `ECHO <message>`
- `HELLO [2|3]`（仅支持 2，3 会返回错误提示）
- `COMMAND`（返回空数组，方便部分客户端探测）
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

## 说明

- 这是一个 **单机内存版** 实现：不包含 AOF/RDB 持久化、复制、集群、事务、Lua、ACL 等复杂功能。
- TTL 采用“访问时惰性删除”，并带一个轻量级后台清理任务（可关）。
