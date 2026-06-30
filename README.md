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
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 6378
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

可重复 benchmark：

```bash
./scripts/bench.sh
```

常用对比参数：

```bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

正式性能报告：

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/manual-release
```

baseline/current 对比报告：

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --baselineServerJar artifacts/baseline/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/release-comparison
```

Redis/current 对比报告：

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --includeRedis \
  --redisHost 127.0.0.1 \
  --redisPort 6379 \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/redis-comparison
```

建议用专门的 Redis 实例运行对比，并固定配置，避免后台持久化或淘汰策略影响结果：

```text
save ""
appendonly no
maxmemory-policy noeviction
```

快速 smoke：

```bash
./scripts/smoke.sh
```

完整 CLI/benchmark 内部说明见 [`docs/project-docs/client-and-bench-internals.md`](docs/project-docs/client-and-bench-internals.md)。
