# yierdis (Java 25 + Netty)

Yierdis 是一个用 Java 25 + Netty 实现的 Redis-style 单机内存 KV server。它对外暴露 Redis RESP TCP 协议，RESP2 是默认 wire target，`HELLO 3` 可以协商基础 RESP3 replies。

它不是 Redis drop-in replacement。当前实现重点是单机内存数据结构、基础命令子集、TTL、maxmemory、最小事务、backpressure、observability 和 JDK FFM native-memory 路径；AOF/RDB、replication/cluster、Lua、ACL/TLS、PubSub 和完整 Redis ecosystem compatibility 属于后续方向。

## 环境

- JDK 25
- Maven 3.x

构建和测试必须在 JDK 25 环境下运行：

```bash
mvn test
mvn -DskipTests package
```

## 文档入口

第一次进入代码库先读 [`docs/project-docs/readme.md`](docs/project-docs/readme.md)。那是内部文档地图，会按不同目标给出阅读路径：

- 先理解项目定位、能力边界和模块结构
- 跟一条请求从 RESP 走到 DB 再写回
- 查协议、命令、DB、执行器、native memory 等专题
- 准备改代码并选择验证范围

推荐第一轮阅读：

1. [`docs/project-docs/readme.md`](docs/project-docs/readme.md)
2. [`docs/project-docs/project-overview.md`](docs/project-docs/project-overview.md)
3. [`docs/project-docs/request-execution-flow.md`](docs/project-docs/request-execution-flow.md)
4. [`docs/project-docs/main-path-walkthrough.md`](docs/project-docs/main-path-walkthrough.md)
5. [`docs/project-docs/module-architecture.md`](docs/project-docs/module-architecture.md)

## 启动

```bash
mvn -q -DskipTests package
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 6378 --maxmemoryBytes 0
```

用 `redis-cli` 验证：

```bash
redis-cli -p 6378 PING
redis-cli -p 6378 SET a 1
redis-cli -p 6378 GET a
redis-cli -p 6378 HELLO 3
```

也可以用项目内置 CLI：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar SET a 1
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar GET a
```

直接发送 RESP frame：

```bash
printf '*2\r\n$3\r\nGET\r\n$1\r\na\r\n' | nc 127.0.0.1 6378
```

## 常用入口

| 目标 | 入口 |
| --- | --- |
| 协议边界和 RESP 行为 | [`docs/project-docs/protocol-reference.md`](docs/project-docs/protocol-reference.md) |
| 命令层和数据模型 | [`docs/project-docs/commands-and-data-model.md`](docs/project-docs/commands-and-data-model.md) |
| 配置、maxmemory、backpressure 和运行场景 | [`docs/project-docs/configuration-and-operations.md`](docs/project-docs/configuration-and-operations.md) |
| 生产 hardening 限制、关闭和验收操作 | [`docs/project-docs/production-hardening-operations.md`](docs/project-docs/production-hardening-operations.md) |
| native memory 和 off-heap copy 边界 | [`docs/project-docs/native-memory-runtime.md`](docs/project-docs/native-memory-runtime.md) |
| CLI 和 benchmark 内部实现 | [`docs/project-docs/client-and-bench-internals.md`](docs/project-docs/client-and-bench-internals.md) |
| 改代码前的源码导航 | [`docs/project-docs/development-navigation.md`](docs/project-docs/development-navigation.md) |
| 测试和排障路径 | [`docs/project-docs/testing-and-debugging.md`](docs/project-docs/testing-and-debugging.md) |

## CLI

项目内置 CLI 默认连接 `127.0.0.1:6378`：

```bash
# 单次执行
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar INFO
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar STATS

# 交互模式，输入 quit 或 exit 退出
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar
```

常用参数：

- `--host <host>`
- `--port <port>`
- `--timeoutMillis <ms>`
- `--hex`

## Benchmark 和 Smoke

`yierdis-benchmark` 提供两条刻意分离的测量路径：默认命令通过 TCP/RESP 压测已经运行的 Yierdis；显式 `storage` 子命令在进程内直接测量 DB SET 和存储 footprint，不需要启动 server。两类吞吐覆盖的边界不同，不能直接横向比较。

### RESP benchmark

默认 RESP benchmark 是 connect-only。项目不会由它启动 Yierdis，不会运行 Redis 或 `redis-benchmark`，也不定义或执行把两边组合在一起的 harness。Redis 进程、官方工具和 Redis 结果文件都由操作者单独管理。

1. 先单独启动 Yierdis，benchmark 默认目标端口是 `16378`：

```bash
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 16378
```

2. 在另一个终端运行 Yierdis benchmark：

```bash
./scripts/bench.sh
```

默认 workload 值是 `HOST=127.0.0.1`、`PORT=16378`、`REQUESTS=100000`、`CLIENTS=50`、`DATA_SIZE=3`、`PIPELINE=1`、`FORMAT=human`。例如，生成 CSV 或选择部分官方 case：

```bash
FORMAT=csv ./scripts/bench.sh
TESTS=ping,set,get REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

`KEYSPACE` 未设置时保留固定 key 模式；`KEYSPACE=0` 是显式随机 keyspace 配置，不能与省略混为一谈。还可设置 `KEEP_ALIVE`、`PRECISION`、`SEED`、`BENCH_USERNAME`、`PASSWORD`、`DATABASE` 和 `BENCH_JVM_OPTS`。`BENCH_USERNAME` 是 portable ACL username knob；`USERNAME` 仅保留为非保留环境中的兼容 fallback，不要在 zsh 中使用 `USERNAME=value` 调用脚本。`SKIP_BUILD=1` 只跳过 benchmark module 的构建，不改变 connect-only 行为。

3. 由操作者在独立的 Redis 环境中单独运行官方 `redis-benchmark`，使用等价 workload 值。例如 Redis 监听 `6379` 时：

```bash
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -d 3 -P 1 --csv
```

4. 按对应 canonical title 比较两份结果。完整 built-in catalog 固定为 21 行，顺序是：

`PING_INLINE`, `PING_MBULK`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `SADD`, `HSET`, `SPOP`, `ZADD`, `ZPOPMIN`, `LPUSH (needed to benchmark LRANGE)`, `LRANGE_100 (first 100 elements)`, `LRANGE_300 (first 300 elements)`, `LRANGE_500 (first 500 elements)`, `LRANGE_600 (first 600 elements)`, `MSET (10 keys)`, `XADD`。

CSV 只把前八个官方 Redis-style columns 作为共享比较面；字段名依次是：

```text
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
```

Yierdis CSV 在这八列之后增加 `status` 和 `reason`，它们不是共享 Redis fields。操作者负责保存、配对和解释两边结果；项目不计算跨 server ratio 或 release threshold。

5. Yierdis 当前完整运行的支持状态是 `17 SUCCESS / 4 UNSUPPORTED`。`SPOP`、`ZPOPMIN`、`MSET (10 keys)` 和 `XADD` 仍保留各自 canonical row，但七个数值 metrics 为空，不能按零吞吐或零延迟解释。

### Storage footprint benchmark

存储基准在单 owner thread 上直接调用 `RuntimeDbEngine` 的 `StringWriteOps.setString(...)`，排除 TCP、RESP、server 和 executor 开销。默认写入 1,000,000 个唯一 key，key/value 都是固定 16 bytes，并先在一个随后关闭的临时 DB 中预热 50,000 次：

```bash
./scripts/storage-bench.sh
STORAGE_KEYS=10000000 FORMAT=csv ./scripts/storage-bench.sh
```

`STORAGE_KEYS` 最大为 10,000,000；还可设置 `STORAGE_KEY_SIZE`、`STORAGE_VALUE_SIZE`、`STORAGE_WARMUP_OPERATIONS`、`STORAGE_PRECISION`、`FORMAT`、`BENCH_JVM_OPTS` 和 `SKIP_BUILD`。

主 footprint 口径是 `heap estimate + native metadata committed + native data committed`。SET 计时结束后会先完成未决的增量 rehash，再抓取稳定 snapshot；`bytes/key` 使用该 accounted footprint 减去空 DB baseline 后再除以 key 数，不使用 RSS。Linux RSS 来自 `/proc/self/status`，仅是 best-effort 辅助观测；不可用时 human 输出显示 `unavailable`，CSV 对应列留空。

### Smoke

快速 smoke：

```bash
./scripts/smoke.sh
```

完整 CLI/benchmark 内部说明见 [`docs/project-docs/client-and-bench-internals.md`](docs/project-docs/client-and-bench-internals.md)。
