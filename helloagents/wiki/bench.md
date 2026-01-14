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

---

## 5. 正确性统计与 RESP3 兼容性说明

### errors 统计（吞吐/延迟都会展示）
bench 在汇总表格中会同时展示 throughput/latency 与 `errors`：

- 只要遇到 `-ERR ...`（RESP error reply），就会计入 `errors`
- `errors` 会在吞吐与延迟两类结果里都展示，便于区分“性能变快但语义错误”的情况

### `--strictReplies`：最小语义校验（可选）
默认情况下，bench 主要用于性能对比：只要响应不是 `-ERR`，就会按成功统计。

当开启 `--strictReplies` 时，会对不同 workload 的响应做“最小类型校验”，例如：
- `PING` 期望 simple string
- `SET` 期望 simple string
- `GET` 期望 bulk string 或 null bulk string

若返回类型不符合预期，也会计入 `errors`（用于捕捉协议/实现差异导致的隐藏错误）。

### RESP3 基础类型跳过（为了兼容不同服务端）
当目标服务端处于 RESP3（例如执行 `HELLO 3` 后），某些响应可能出现额外的 RESP3 基础类型（如 null、map、set、push 等）。

bench 的响应跳过逻辑支持跳过这些“非本次 workload 关心”的基础类型，以便：
- 能在 pipeline 模式下持续对齐请求-响应边界
- 不因为额外的 RESP3 元信息导致解析错位或误判为连接异常
