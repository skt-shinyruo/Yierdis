# 压测与基准测试（Benchmark）

本项目定位为“教学/演示导向”的简化 Redis 服务端，因此默认以功能正确性测试为主；但为了更直观地观察 off-heap 后端（`none/netty/unsafe`）的差异，仓库提供了一套“纯 Java”可重复压测工具与一键脚本：

- Java 工具模块：`yierdis-bench`
- 一键脚本：`scripts/bench.sh`

---

## 1. 前置依赖

压测工具为纯 Java 实现，不依赖 `redis-benchmark` 等系统工具。

脚本默认会执行 `mvn -DskipTests package` 进行构建，因此需要 JDK 17 + Maven 3.x。

---

## 2. 一键压测

在项目根目录执行：

```bash
./scripts/bench.sh
```

脚本会自动：

1. Maven 构建 server jar（可通过 `SKIP_BUILD=1` 跳过）
2. 依次启动 `none`、`netty`、`unsafe` 三种后端（端口从 `PORT_BASE` 起连续占用）
3. 预置数据（写入 keyspace）
4. 执行吞吐压测（pipeline）
5. 执行 pipeline=1 的延迟压测，输出 p50/p95/p99
6. 输出汇总表格

---

## 3. 容器环境（memory limit=16G）建议

共享容器下，除了 `-Xmx` 之外，还需要关注 Direct Memory 与 off-heap 的上限，否则容易出现“堆没满但容器 OOM / direct memory OOM”。

建议先从保守预算开始（示例）：

- JVM：`-Xms4g -Xmx4g -XX:MaxDirectMemorySize=6g`
- Server：`--maxmemoryBytes 7GiB`（示例：`7516192768`）
- Off-heap：`--offheapMaxBytes 4GiB`（示例：`4294967296`）

如需更激进的压测，请逐步加大预算并观察 RSS/容器内存曲线，而不是只看 `maxmemory` 的逻辑统计。

---

## 4. 常用可调参数

脚本参数以环境变量方式暴露，常用：

- 并发与 pipeline：`REQUESTS`、`CLIENTS`、`PIPELINE`
- 数据规模：`DATA_SIZE`、`KEYSPACE`
- JVM：`XMS`、`XMX`、`MAX_DIRECT_MEMORY`
- server 预算：`MAXMEMORY_BYTES`、`OFFHEAP_MAX_BYTES`
- 跳过阶段：`SKIP_PREFILL=1`、`SKIP_LATENCY=1`

示例：

```bash
REQUESTS=3000000 CLIENTS=400 PIPELINE=32 DATA_SIZE=1024 KEYSPACE=2000000 ./scripts/bench.sh
```

如需查看 Java 工具的全部参数（含 `--serverArg` 等），可直接运行：

```bash
java -jar yierdis-bench/target/yierdis-bench-0.1.0-SNAPSHOT.jar --help
```
