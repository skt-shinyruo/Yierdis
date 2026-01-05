# yierdis (Java 17 + Netty)

一个简化版的 Yierdis Server（兼容 Redis），适合用来学习/演示 Netty 网络编程与 Redis 协议。

## 环境

- JDK 17
- Maven 3.x

## 启动

```bash
mvn -q -DskipTests package
java -jar yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar --port 6378
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

## 客户端（CLI）

项目内置一个极简的 RESP2 客户端，方便本地调试（默认连接 `127.0.0.1:6378`）。

```bash
# 单次执行
java -cp yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar yier.bubu.redis.client.YierdisCli PING
java -cp yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar yier.bubu.redis.client.YierdisCli SET a 1
java -cp yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar yier.bubu.redis.client.YierdisCli GET a

# 交互模式（输入 quit/exit 退出）
java -cp yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar yier.bubu.redis.client.YierdisCli
```

常用参数：

- `--host <host>`
- `--port <port>`
- `--timeoutMillis <ms>`
- `--hex`（bulk string 以 hex 输出，便于观察二进制数据）

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

### ZSet

- `ZADD key score member [score member ...]`
- `ZRANGE key start stop [WITHSCORES] [REV]`
- `ZREVRANGE key start stop [WITHSCORES]`
- `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
- `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`
- `ZREMRANGEBYRANK key start stop`
- `ZREMRANGEBYSCORE key min max`
- `ZREM key member [member ...]`

## 说明

- 这是一个 **单机内存版** 实现：不包含 AOF/RDB 持久化、复制、集群、事务、Lua、ACL 等复杂功能。
- TTL 采用“访问时惰性删除”，并带一个轻量级后台清理任务（可关）。

## 内存管理（maxmemory / 淘汰，教学用）

Yierdis 提供一个“Redis 风格、但刻意简化”的 maxmemory/淘汰机制，方便演示：

- `--maxmemoryBytes <bytes>`：启用最大内存预算（默认 `0` 表示不限制）。
- `--maxmemoryPolicy noeviction|allkeys-random|allkeys-lru`：淘汰策略（默认 `noeviction`）。
  - `noeviction`：不淘汰，写入会返回 OOM 错误。
  - `allkeys-random`：随机淘汰任意 key。
  - `allkeys-lru`：基于采样的近似 LRU（更接近 Redis 的默认思路）。
- `--maxmemorySamples <n>`：LRU 采样数量（默认 `5`）。

示例：开启 10MB 内存预算，并使用 LRU 淘汰：

```bash
java -jar yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar \
  --port 6378 \
  --maxmemoryBytes 10485760 \
  --maxmemoryPolicy allkeys-lru \
  --maxmemorySamples 5
```

可用于观察内部编码与内存估算的命令：

- `OBJECT ENCODING <key>`
- `MEMORY USAGE <key>`

## Off-heap（实验）

项目内置一层“堆外内存操作”抽象 API，并提供多个后端实现：

- `netty`：基于 Netty direct `ByteBuf`
- `unsafe`：基于 `sun.misc.Unsafe`（通过 Netty `PlatformDependent`）管理 native memory（无需 incubator modules）
- `foreign`：基于 Java 17 incubator 的 Foreign Memory API（需要 Maven profile + JVM module 参数）

目前该层主要用于逐步迁移（先抽象，再替换实现）。默认 `--offheapBackend none` 不影响现有逻辑；当显式启用
off-heap 后端后，当前已用于字符串值的存储与回复（例如 `GET` 会优先走 off-heap slice 的写出路径，避免为已存储值再分配新的 heap `byte[]`）。
此外，当选择 `unsafe` 后端时，keyspace 的 key bytes 以及过期索引（expires）也会使用同一个 allocator 的 off-heap 内存（因此 `--offheapMaxBytes` 需要包含索引/keys 的固定开销）。

### 运行/测试 Foreign Memory 后端

默认 `mvn test` 不会编译 Foreign Memory 后端源码。开启方式：

```bash
mvn -Pforeign-memory test
```

如果需要把 Foreign Memory 后端打进 shaded jar，也需要带 profile 进行打包：

```bash
mvn -Pforeign-memory -DskipTests package
```

运行时如果选择 `foreign` 后端，需要显式添加模块（Java 17）：

```bash
java --add-modules jdk.incubator.foreign -jar yierdis-server/target/yierdis-0.1.0-SNAPSHOT.jar --offheapBackend foreign
```

### Server 参数（预留）

- `--offheapBackend none|netty|unsafe|foreign`（默认 `none`）
- `--offheapMaxBytes <bytes>`（默认 `0` 表示不限制；>0 时作为硬限制，超限命令返回 OOM 错误）

## 压力测试（可重复）

本项目没有内置 JMH，但提供一个“可重复压测脚本”用于对比 `none/netty/unsafe` 三种后端的吞吐与延迟分位数（纯 Java 实现，不依赖
`redis-benchmark` 等系统工具）。

一键运行：

```bash
./scripts/bench.sh
```

常用可调参数（环境变量）：

- `PORT_BASE`：起始端口（默认 `16378`，每个后端 +1）
- `REQUESTS` / `CLIENTS` / `PIPELINE`：吞吐压测参数（每种命令单独跑一次）
- `DATA_SIZE` / `KEYSPACE`：value 大小与 keyspace
- `XMS` / `XMX` / `MAX_DIRECT_MEMORY`：JVM 内存与 Direct Memory 上限
- `MAXMEMORY_BYTES` / `OFFHEAP_MAX_BYTES`：server 预算参数（容器环境建议保守）
- `SKIP_PREFILL=1`：跳过预置数据（可能导致 GET 大量 miss，影响可比性）
- `SKIP_LATENCY=1`：跳过延迟压测

也可以直接运行 Java 工具查看完整参数：

```bash
java -jar yierdis-bench/target/yierdis-bench-0.1.0-SNAPSHOT.jar --help
```
