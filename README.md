# yierdis (Java 17 + Netty)

一个教学/演示导向的内存 KV 服务端，适合用来学习/演示 Netty 网络编程与单线程命令执行、背压与淘汰等思路。

对外协议使用自定义协议 **Custom Protocol v1**（不兼容 Redis 原生协议）。

## 定位与兼容性边界（重要）

Yierdis 的目标是 **教学/演示**：可以用项目内置 CLI 做交互学习、用最小命令集理解数据结构/协议/背压/淘汰等思路。

但它不是 Redis 的 drop-in replacement（README 明确将不少能力定义为 out-of-scope），并且即便是已实现命令，也有不少“刻意简化/最小子集”的语义差异（例如 TTL 清理、`KEYS` glob 覆盖范围、事务边界行为等）。

- **包含（In scope）**：单机内存版数据结构、基础命令子集、TTL（惰性删除 + 轻量后台清理）、maxmemory（教学口径的估算 + 近似淘汰）、最小事务子集（`MULTI/EXEC/DISCARD`）、自定义协议（Custom Protocol v1：length-prefixed request + NDJSON reply；协议错误尽量可恢复）、`INFO/STATS/MEMORY STATS` 可观测性
- **不包含（Out of scope）**：AOF/RDB 持久化、复制/集群、Lua、ACL/TLS、PubSub/订阅模式、完整的模块化运维能力

## 环境

- JDK 17
- Maven 3.x

## 开发者：模块边界（契约 / 组装）

本项目内部模块做过一次“边界收敛”，目的是让依赖方向更清晰（契约在 core，协议模型专注协议，组装在 server）：

- **执行契约（Command/ReplyWriter/Session...）**：统一放在 `yierdis-core-contract`（包名 `yier.bubu.redis.contract.*`），不再放在 `yierdis-protocol-model`。
- **协议模型（limits/build-info/reply IR）**：继续位于 `yierdis-protocol-model`（包名 `yier.bubu.redis.protocol.*`）。
- **core-command 默认装配**：`yierdis-core-command` 仅保留传输无关的默认命令模块；`HELLO/INFO/STATS` 这类需要 protocol/build-info/运行时观测组装的 server-facing commands 位于 `yierdis-server`，而 `PING/ECHO/COMMAND/SELECT/QUIT/FLUSHDB` 这类传输无关或 DB 生命周期命令继续留在 core。
- **CLI 输入解析**：`InlineCommandParser` 位于 `yierdis-client`（`yier.bubu.redis.client.InlineCommandParser`）。
- **instance 暴露面**：`YierdisInstance` 仅负责 DB 生命周期、资源 ownership 与 `DbEngine` 能力视图（`engine(int)` / `engines()` 防御性拷贝），避免上层依赖 `YierdisDb` 具体实现，也不再承担 command processor 组装。
- **runtime owner-thread seam**：server 不应再通过公开 `DbEngine` 视图做 `RuntimeDbEngine` 向下转型，也不应在 bootstrap 中内联 `bindToCurrentThread()/close()` 细节；owner-thread 维护、maintenance、关闭应通过 `yierdis-core-runtime` 提供的 runtime-local seam 协作。
- **DB 内部协作者**：`YierdisDb` 仍然是状态 owner，但过期清理、maxmemory/淘汰这类高密度内部策略应优先收敛到 package-local collaborator，而不是继续在单个超大类中内联扩张。
- **off-heap 组装**：`YierdisOffHeapAllocators` 仅通过 `ServiceLoader` 发现 provider；server 侧通过引入对应 backend 模块（如 `yierdis-memory-netty/unsafe/foreign`）完成组装。

## 启动

```bash
mvn -q -DskipTests package
java -jar yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 6378
```

然后使用项目内置 CLI 连接（默认 `127.0.0.1:6378`）：

```bash
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar PING
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar SET a 1
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar GET a
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar INFO
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar INFO yierdis
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar STATS
```

也可以用 `nc` 直接发送一帧（示例：PING）：

```bash
printf '24:{"cmd":"PING","args":[]}\n' | nc 127.0.0.1 6378
```

说明：

- `len` 是 JSON payload 的 UTF-8 字节长度；其他命令需要按 payload 实际字节数计算。
- 服务端返回 NDJSON：每个 reply 一行 JSON（以 `\n` 结尾）。
- 对外仅支持 Custom Protocol v1（不兼容 Redis 原生协议与其生态客户端）。

## 客户端（CLI）

项目内置一个极简 client/CLI，方便本地调试（默认连接 `127.0.0.1:6378`）。

```bash
# 单次执行
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar PING
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar SET a 1
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar GET a

# 交互模式（输入 quit/exit 退出）
java -jar yierdis-client/target/yierdis-client-0.1.0-SNAPSHOT.jar
```

常用参数：

- `--host <host>`
- `--port <port>`
- `--timeoutMillis <ms>`
- `--hex`（raw JSON reply line 以 hex 输出）

说明：

- client/CLI 使用 Custom Protocol v1：
  - request：`<len>:<json>\n`，payload 形如 `{"cmd":"PING","args":["a","1"]}`
  - reply：NDJSON（一行一个 JSON 对象）
- CLI 默认打印服务端返回的 JSON 单行文本；`--hex` 仅改变展示方式（不改变协议）。
- REPL 输入解析规则保持 sdssplitargs 风格（支持单/双引号、反斜杠转义、`\\xHH` 十六进制转义）。

## 已实现命令（简化版）

### 通用

- `PING [message]`
- `ECHO <message>`
- `HELLO [ignored]`（信息型命令：返回 server/version/proto/mode/role；不进行协议协商）
- `COMMAND`（最小子集：`COMMAND`/`COMMAND COUNT`/`COMMAND INFO <name ...>`）
- `SELECT <index>`（默认支持 `0..15`；可通过 `--databases` 调整）
- `QUIT`

### Key/TTL

- `SET key value [EX seconds|PX milliseconds] [NX|XX]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `TTL key`
- `KEYS pattern`（支持 Redis 风格最小 glob 子集：`*`/`?`/`[]` 范围与否定/反斜杠转义；按 byte 匹配）
- `TYPE key`
- `FLUSHDB`

### String

- `INCR key`
- `DECR key`
- `APPEND key value`
- `STRLEN key`

### Bitmap

- `SETBIT key offset value`
- `GETBIT key offset`
- `BITCOUNT key [start end]`

### HLL

- `PFADD key element [element ...]`
- `PFCOUNT key [key ...]`
- `PFMERGE destkey sourcekey [sourcekey ...]`

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

事务边界与上限（重要）：

- 事务队列是连接级状态：为避免大事务/大参数导致 JVM OOM，server 提供硬上限保护：
  - `--transactionQueueMaxCommands <n>`：事务队列最大命令数（0 表示不限制）
  - `--transactionQueueMaxBytes <bytes>`：事务队列最大参数 bytes（按入队参数拷贝估算；0 表示不限制）
- 当 MULTI 模式入队阶段触发上述上限时：该连接事务会被标记为 aborted；后续 `EXEC` 会返回 Redis 风格 `EXECABORT` 并丢弃队列（对齐 Redis 的“入队阶段出错 → EXEC 终止”语义）。

## 说明

- 这是一个 **单机内存版** 实现：不包含 AOF/RDB 持久化、复制、集群、Lua、ACL、TLS 等复杂功能。
- 事务仅支持最小子集：`MULTI/EXEC/DISCARD`（不包含 WATCH 等）。
- 协议层为 Custom Protocol v1（length-prefixed JSON request + NDJSON reply）；协议错误尽量可恢复（返回 error 并尝试读取下一帧；在安全上限触达时可能断连）。
- TTL 采用“访问时惰性删除”，并带一个轻量级后台清理任务（可关）。

## 内存管理（maxmemory / 淘汰，教学用）

Yierdis 提供一个“Redis 风格、但刻意简化”的 maxmemory/淘汰机制，方便演示：

- `--maxmemoryBytes <bytes>`：启用最大内存预算（默认 `0` 表示不限制）。
- `--maxmemoryScope global|per-db`：maxmemory 预算口径（默认 `global`，更贴近 Redis “全实例 maxmemory”；`per-db` 为兼容模式：将 `maxmemoryBytes` 按 DB 数量硬分摊）。
- `--maxmemoryPolicy noeviction|allkeys-random|allkeys-lru`：淘汰策略（默认 `noeviction`）。
  - `noeviction`：不淘汰，写入会返回 OOM 错误。
  - `allkeys-random`：随机淘汰任意 key。
  - `allkeys-lru`：基于采样的近似 LRU（更接近 Redis 的默认思路）。
- `--maxmemorySamples <n>`：LRU 采样数量（默认 `5`）。

示例：开启 10MB 内存预算，并使用 LRU 淘汰：

```bash
java -jar yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar \
  --port 6378 \
  --maxmemoryBytes 10485760 \
  --maxmemoryScope global \
  --maxmemoryPolicy allkeys-lru \
  --maxmemorySamples 5
```

可用于观察内部编码与内存估算的命令：

- `OBJECT ENCODING <key>`
- `MEMORY USAGE <key>`
- `MEMORY STATS`
- `INFO` / `INFO YIERDIS` / `STATS`

## 协议上限与反压（推荐）

为了避免“少量大 bulk 积压导致内存驻留不可解释”的情况，server 支持按 **条数 + bytes** 做双约束：

- `--protocolMaxBulkBytes <bytes>` / `--protocolMaxArgs <n>` / `--protocolMaxLineBytes <bytes>`：输入上限（DoS 防护）
- `--executorQueueCapacity <n>`：全局执行队列条数上限（有界队列）
- `--executorQueueMaxBytes <bytes>`：全局执行队列 bytes 上限（`0` 表示禁用）
- `--backpressureHigh/--backpressureLow`：连接级条数背压水位线（滞回）
- `--backpressureBytesHigh/--backpressureBytesLow`：连接级 bytes 背压水位线（滞回；`0` 表示禁用）

开放网络环境建议（重要）：

- 如果部署在公网/弱隔离环境，建议 **显式收紧** `--protocolMaxBulkBytes`（以及 `--protocolMaxArgs/--protocolMaxLineBytes`），不要依赖默认值。
- 即便 decoder 侧已做“尽量低拷贝”的优化，大包请求仍可能导致解析与字符串驻留带来显著内存/CPU 压力；输入上限是更有效的第一道护栏。

busy 可诊断性（排障）：

- 当投递被拒绝时，server 会返回错误（`message` 以 `ERR busy <reason>` 开头；`reason` 用于人类排障）：
  - `not_running`：执行器未启动或正在关闭
  - `queue_full`：全局队列已满
  - `bytes_budget`：全局 queued-bytes 预算耗尽
  - `offer_failed`：入队失败（通常是竞态/关闭路径）
- `STATS` 会输出对应计数器，便于定位 busy 的主因（例如 `submit_rejected_queue_full_total` 等）。

## Off-heap（实验）

项目内置一层“堆外内存操作”抽象 API，并提供多个后端实现：

- `netty`：基于 Netty direct `ByteBuf`（适配层在 `yierdis-memory-netty`；`yierdis-memory-api` 不依赖 Netty）
- `unsafe`：基于 `sun.misc.Unsafe`（通过 Netty `PlatformDependent`）管理 native memory（无需 incubator modules）
- `foreign`：基于 Java 17 incubator 的 Foreign Memory API（默认构建已包含；运行时需要启用模块，server 可自动补齐）

目前该层主要用于逐步迁移（先抽象，再替换实现）。默认 `--offheapBackend none` 不影响现有逻辑；当显式启用
off-heap 后端后，当前已用于字符串值的存储与回复（例如 `GET` 会优先走 off-heap slice 的写出路径，避免为已存储值再分配新的 heap `byte[]`）。
此外，当选择 `unsafe` 后端时，keyspace 的 key bytes 以及过期索引（expires）也会使用同一个 allocator 的 off-heap 内存（因此 `--offheapMaxBytes` 需要包含索引/keys 的固定开销）。

### 运行/测试 Foreign Memory 后端

从 2026-02-08 起，`foreign-memory` profile 默认启用，因此默认构建会编译并打包 foreign 后端：

```bash
mvn test
mvn -DskipTests package
```

如果你的构建/运行环境不包含 `jdk.incubator.foreign`（例如不是 JDK 17），可以显式禁用该 profile：

```bash
mvn -P!foreign-memory test
mvn -P!foreign-memory -DskipTests package
```

运行时若选择 `foreign` 后端，Java 17 需要启用 incubator 模块。推荐显式添加（避免一次自动重启）：

```bash
java --add-modules jdk.incubator.foreign -jar yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --offheapBackend foreign
```

为降低部署复杂度，如果你直接运行 `java -jar ... --offheapBackend foreign`，server 会检测到模块未启用并自动重启补齐
`--add-modules jdk.incubator.foreign`（并保留原 JVM 参数，例如 `-Xmx` / `-XX:MaxDirectMemorySize` 等）。

### Server 参数（预留）

- `--offheapBackend none|netty|unsafe|foreign`（默认 `none`）
- `--offheapMaxBytes <bytes>`（默认 `0` 表示不限制；>0 时作为硬限制，超限命令返回 OOM 错误）

⚠️ 重要提示：如果启用了 off-heap 后端但未配置 `--offheapMaxBytes`（保持 0），off-heap 会表现为“无限上限”；此时即使配置了 `--maxmemoryBytes`，也可能出现“以为有硬限制但 off-heap 仍持续增长”的误解。建议在容器/受限环境中总是显式配置 `--offheapMaxBytes`。

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

## 最小 Smoke（推荐）

用于快速验证“server 启动/CLI/bench strictReplies”链路：

```bash
./scripts/smoke.sh
```
