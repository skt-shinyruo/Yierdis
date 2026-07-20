# 客户端与基准测试内部

本文解释项目内置 CLI、Netty client、RESP benchmark、进程内 storage benchmark 和 smoke/bench 脚本如何工作。

`yierdis-cli` 和默认的 benchmark 根命令都通过真实 TCP、真实 RESP frame 和 `yierdis-networking-resp` 的 client codec 工作，因此更接近外部使用者视角。显式 `storage` 子命令是例外：它直接创建 `RuntimeDbEngine`，只测 DB SET hot path 和存储 footprint。

CLI 负责人工交互和轻量验证；RESP benchmark 负责固定 built-in workload 下的吞吐、延迟和最小 reply-shape 校验。比较网络结果时必须保证 `requests`、`clients`、`pipeline`、`data-size`、`keyspace`、keepalive、认证和 DB selection 等输入等价，并记录运行环境。默认 benchmark 只连接已经运行的 Yierdis；官方 Redis 结果由操作者在独立 Redis 环境中单独运行 `redis-benchmark` 获得。项目不会启动或运行 Redis，也没有组合两边执行的 harness。

## 它们更适合验证什么

- CLI：快速确认协议、回包和单命令行为。
- smoke：快速确认 server 启动、基础命令和 CLI/RESP 主链。
- RESP benchmark：观察高并发请求、pipeline 和 backpressure 行为，但不是 correctness oracle。
- storage benchmark：隔离观察单 owner DB SET 吞吐、延迟和 heap/native footprint，不代表端到端吞吐。

## yierdis-cli

入口是 `YierdisCli.main(...)`，参数模型是 `YierdisCliArgs`。默认连接 `127.0.0.1:6378`，常用参数与根 `README.md` 一致：

- `--host <host>`
- `--port <port>`
- `--timeoutMillis <ms>`
- `--hex`

单次命令模式：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar SET a 1
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar GET a
```

`YierdisCli` 用 picocli 解析选项，并通过 `cmd.setStopAtPositional(true)` 让第一个位置参数之后都成为命令 argv。单次命令会把每个字符串按 UTF-8 转成 `byte[]`，调用 `YierdisClient.execute(...)`，再按 Redis CLI 风格打印 reply。返回值是错误 reply 时进程退出码为 `1`，成功为 `0`。

REPL 模式：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar
```

没有位置参数时进入 `yierdis> ` 提示符。空行跳过，输入 `quit` 或 `exit` 时 best-effort 发送 `QUIT` 后退出。每一行通过 `InlineCommandParser.splitUtf8(...)` 转成 argv，再走同一个 `YierdisClient.execute(...)` 路径。`--hex` 只影响非 UTF-8 bulk string 的展示；协议内容不变。

源码入口：

- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCliArgs.java`

## InlineCommandParser

`InlineCommandParser` 是 Redis `sdssplitargs` 风格的 inline command parser，不是简单 `split(" ")`。实现位于 `yierdis-networking-resp`，CLI REPL 和服务端 inline command 解码共用同一套规则。

它支持：

- space / tab 空白分隔。
- 单引号，单引号内只特殊处理 `\\'`。
- 双引号。
- 双引号内反斜杠转义：`\n`、`\r`、`\t`、`\b`、`\a` 和默认保留字符。
- 双引号内 `\xHH` 十六进制字节。
- `maxArgs` 上限，超出时报 `Protocol error: array length too large`。

`parse(...)` 返回 `Decoded`，其中保存一块 decoded byte buffer，以及每个 argv 的 offset/length。`splitUtf8(...)` 是 CLI REPL 使用的便利方法，会把 Java `String` 先编码成 UTF-8，再复制出每个 argv。服务端 decoder 使用显式的 `parseUnlimited(...)`，解析后再按连接配置返回更精确的参数数量错误。

CLI 和 server 共用同一套 inline 语法，但边界不同：CLI 侧更偏人手输入和单机验证，服务端侧更偏协议适配和错误关闭。两边都不把 inline 解析当成 RESP array 的替代品，只是为了给手工调试、`redis-cli` 风格输入和基础兼容留一条可控路径。

这个 parser 也解释了为什么 CLI REPL 可以输入带空格或二进制转义的参数，例如：

```text
SET "hello key" "line\nvalue"
SET raw "\x00\x01"
```

## YierdisClient

`YierdisClient` 是 blocking socket client，故意保持简单的一问一答模型：

- `connect(host, port)` 创建 `Socket`，启用 `TCP_NODELAY`，连接超时固定 `5000` ms。
- `execute(List<byte[]> args, timeoutMillis)` 要求 timeout 大于 `0`。
- `requestLock` 保证同一个 client 实例一次只发一个请求。
- 每次请求前设置 socket read timeout。
- 用 `RespClientCodec.writeCommand(...)` 写 RESP request，flush 后用 `RespClientCodec.readReply(...)` 读一个 reply。

它不做 pipelining。原因是 CLI 需要清晰的请求/回包 pairing，而不是最大吞吐。

超时或解析失败会关闭连接。这是有意设计：RESP reply 是 FIFO，如果一个请求超时但连接保留，迟到 reply 可能被下一条请求读到，造成 desync。关闭连接比尝试“跳过未知 reply”更可靠。

`executeUtf8(...)` 只是把 `List<String>` 转成 UTF-8 bytes 后复用 `execute(...)`。

源码入口：

- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java`
- `yierdis-cli/src/test/java/yier/bubu/redis/app/client/YierdisClientTest.java`

## RespClientCodec

`RespClientCodec` 位于 `yierdis-networking-resp`，不依赖 Netty。CLI 和 benchmark 都复用它。

写请求：

- `encodeCommand(List<byte[]> args)` 返回完整 RESP frame bytes。
- `writeCommand(OutputStream out, List<byte[]> args)` 写 RESP array，每个 argv 写成 bulk string。
- `null` argv 会按空 bulk string 写出。
- 数字写出复用 `ThreadLocal<byte[]> INT_BUF`，减少临时分配。

读回包：

- `readReply(InputStream in, int maxBulkBytes)` 支持 RESP simple string、error、integer、bulk string、array 和 RESP3 null `_`。
- bulk string 会检查 `maxBulkBytes`，超过时报错。
- array 递归读取子 reply。
- 返回 `RespReply` record，包含 `kind`、`text`、`bytes`、`integer` 和 `values`，并对 bytes/list 做防御性复制。

这个 codec 是 client-side 工具路径的事实标准：CLI 用它做单请求通信；RESP benchmark 只用它编码 workload、AUTH 和 SELECT frame，`NioBenchmarkClient` 通过 `IncrementalRespReplyDecoder` 增量读取和校验 reply。storage benchmark 不经过 codec。

## yierdis-benchmark

`yierdis-benchmark` 在同一个 launcher 中提供两个边界不同的入口：根命令是真实 RESP 协议路径 benchmark，`storage` 子命令是直接调用 DB API 的进程内 benchmark。两者都不是 JMH microbenchmark。

### RESP benchmark

入口 `YierdisBench.main(...)` 是一个薄 launcher。picocli 保留 `RedisBenchmarkCommand` 作为根命令，并额外注册 `StorageBenchmarkCommand` 子命令。RESP 路径的核心架构固定为：

```text
RedisBenchmarkOptions
  -> BenchmarkConfig
  -> RedisBenchmarkCatalog.select()
  -> RedisBenchmark
  -> NioBenchmarkRunner (one Selector)
  -> BenchmarkLatencyRecorder
  -> BenchmarkCaseResult / BenchmarkRunResult
  -> BenchmarkOutputRenderer
```

`RedisBenchmarkOptions` 只描述 connect-only workload：

- `--host 127.0.0.1`、`--port 16378`。
- `--requests 100000`、`--clients 50`、`--data-size 3`、`--pipeline 1`。
- 可选 `--keyspace`。省略时请求保留 literal `__rand_int__`，显式 `0` 则把每个 placeholder 展开成 `000000000000`。
- `--keep-alive true`、可选逗号分隔的 `--tests`、`--precision 3`、可选 `--seed`。
- `--format human|quiet|csv`，以及可选 `--username`、`--password`、`--database`。

`BenchmarkConfig` 归一化 host 和 selector，并在发送测量请求前验证端口、request/client/payload/pipeline 范围、keyspace、histogram precision、format 和 DB。`RedisBenchmarkCatalog.select()` 按 selector 做去重后的 canonical-order selection；空 selection 返回完整 catalog，`ping` 同时选择两个 PING case，任一 LRANGE selector 都会先选择 measured LPUSH setup row。

完整 catalog 是以下 21 个输出 row：

| Selector | Canonical title | Yierdis 当前状态 |
| --- | --- | --- |
| `ping_inline` / `ping` | `PING_INLINE` | `SUCCESS` |
| `ping_mbulk` / `ping` | `PING_MBULK` | `SUCCESS` |
| `set` | `SET` | `SUCCESS` |
| `get` | `GET` | `SUCCESS` |
| `incr` | `INCR` | `SUCCESS` |
| `lpush` | `LPUSH` | `SUCCESS` |
| `rpush` | `RPUSH` | `SUCCESS` |
| `lpop` | `LPOP` | `SUCCESS` |
| `rpop` | `RPOP` | `SUCCESS` |
| `sadd` | `SADD` | `SUCCESS` |
| `hset` | `HSET` | `SUCCESS` |
| `spop` | `SPOP` | `UNSUPPORTED` |
| `zadd` | `ZADD` | `SUCCESS` |
| `zpopmin` | `ZPOPMIN` | `UNSUPPORTED` |
| any LRANGE selector | `LPUSH (needed to benchmark LRANGE)` | `SUCCESS` |
| `lrange_100` | `LRANGE_100 (first 100 elements)` | `SUCCESS` |
| `lrange_300` | `LRANGE_300 (first 300 elements)` | `SUCCESS` |
| `lrange_500` | `LRANGE_500 (first 500 elements)` | `SUCCESS` |
| `lrange_600` | `LRANGE_600 (first 600 elements)` | `SUCCESS` |
| `mset` | `MSET (10 keys)` | `UNSUPPORTED` |
| `xadd` | `XADD` | `UNSUPPORTED` |

因此当前默认 Yierdis 结果是 17 个 `SUCCESS` 和 4 个 `UNSUPPORTED` row；真实连接或协议故障会把受影响的 supported row 改为 `FAILED`，LRANGE setup 失败还会使依赖 row 成为 `SKIPPED`。`RedisBenchmark` 不会给这些非成功状态构造性能数字。

`NioBenchmarkRunner` 为一个 case 打开一个 `Selector`，把配置数量的 non-blocking `SocketChannel` 注册到同一个 event loop。它预编译 pipeline frame，只在需要时改写随机 placeholder，处理 partial connect/write/read，并用 incremental RESP decoder 验证每个 reply 的最小 shape。keepalive 关闭时每个 pipeline 后重连；认证和 DB selection 会作为每个新连接第一次 measured write 的 prefix，其 replies 不进入 request 或 histogram count。

measurement 以 selector run 的起止边界计算 elapsed time 和 completed-reply throughput。每个 pipeline 以第一次可读时间作为该 batch replies 的 latency；histogram 只保留配置的前 `requests` 个 samples，而 throughput 使用 stop boundary 已完成的 replies。`BenchmarkLatencyRecorder` 用 HdrHistogram 生成 mean、min、p50、p95、p99 和 max，最大记录延迟 clamp 到 3 秒。

`BenchmarkCaseResult` 的状态只有 `SUCCESS`、`UNSUPPORTED`、`SKIPPED`、`FAILED`；`BenchmarkRunResult` 保留 catalog 顺序并由任意 `FAILED` 决定非零退出码。`BenchmarkOutputRenderer` 提供 human、quiet 和 CSV。吞吐与延迟仍是观测值，不是 correctness oracle，也不替代协议、命令或 DB direct-op tests。

## Command templates 和 reply validation

`RedisBenchmarkCommandTemplate` 声明每个 built-in case 的 wire shape。`PING_INLINE` 使用 raw `PING\r\n`；其余 case 通过 `RespClientCodec.encodeCommand(...)` 生成 RESP array。payload 由 `BenchmarkPayload` 按官方确定性 data generator 每个 catalog pass 生成一次，并由需要 data 的 case 复用。

省略 keyspace 时，`__rand_int__` 保持 literal，因此每次使用固定 key/member；启用 keyspace 时，每个 placeholder 在每条 concrete command 中独立展开为 12 位十进制数。ZADD score 也按同一开关决定固定 `0` 或独立随机值。pipeline frame 预先展开，运行时只原地更新这些固定宽度 digits。

catalog 为每个 case 声明 expected reply shape：`PONG`、`OK`、integer、bulk-or-null 或 array。RESP error、错误 shape、多余 reply、连接断开、无进展 timeout 和 cleanup failure 都会形成带 reason 的 `FAILED` row，而不是伪造吞吐或延迟。四个当前未支持的 case 不发送网络请求，直接形成 `UNSUPPORTED` row。

## 输出和独立比较

CSV header 是：

```text
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms","status","reason"
```

前八列与官方 Redis-style CSV 对齐，是两份独立结果的共享比较面；`status` 和 `reason` 是 Yierdis 扩展。非 `SUCCESS` row 的七个 numeric fields 保持为空，不能解释为零。human 和 quiet format 同样只给成功 row 渲染 metrics。

Redis 侧由操作者在独立环境中运行官方工具，使用等价的 request、client、payload、pipeline、keyspace、keepalive、认证和 DB 设置，再按 canonical title 配对结果。项目 benchmark 不启动 Redis、不调用官方工具、不收集 Redis result，也不定义 combined run。artifact identity、环境记录、结果保存、ratio 和 release threshold 都是 benchmark 之外的 operator policy。

### Storage benchmark

`storage` 子命令的测量边界固定为单线程、单 owner、进程内 `RuntimeDbEngine` SET，不包括 TCP、RESP、server dispatch 或 executor。它用于回答 native payload + heap topology 的存储成本和 DB hot-path 上限，不能把 ops/s 与 RESP benchmark 直接比较。

```text
StorageBenchmarkOptions
  -> StorageBenchmarkConfig
  -> StorageBenchmarkRunner
  -> disposable warmup RuntimeDbEngine
  -> measured single-owner RuntimeDbEngine
  -> StorageLatencyRecorder / StorageMemorySnapshot
  -> StorageBenchmarkResult
  -> StorageBenchmarkRenderer
```

warmup 使用独立 DB，完成后 shutdown；正式测量再创建并绑定一个干净 DB。runner 在 driver buffer 和 histogram 就绪后记录 empty baseline，完成全部 unique SET 后停止计时，再用 DB maintenance 完成未决的增量 rehash，确认 `pending hash tables = 0` 后记录稳定 loaded snapshot，最后 shutdown。固定宽度 mutable key buffer 在循环内复用，因此 benchmark driver 不为每个 key 创建新数组。

每次 SET latency 用纳秒 HdrHistogram 记录，超过 10 秒的样本 clamp 到 10 秒；总吞吐用整个写入循环的起止时间计算。这个 throughput 包含固定宽度 key 编码、两次计时读取、结果校验和 histogram record，是 instrumented direct workload throughput，不是裸 `setString(...)` 方法调用的理论上限。rehash 稳定化发生在计时窗口之后，不计入 SET throughput。

参数边界如下：

- `--keys` 为 `1..10000000`，默认 `1000000`。
- `--key-size` 上限 1024，且至少容纳 `k` 加最大 key index；默认 16 bytes。
- `--value-size` 为 `0..1048576`，默认 16 bytes。
- `--warmup-operations` 为 `0..1000000`，默认 `50000`。
- `--precision` 为 `0..4`；`--format` 为 `human|quiet|csv`。

footprint 使用 DB 自己的物理内存核算：`accounted = heap estimated + native metadata committed + native data committed`。`accounted delta` 是 loaded 减 empty baseline，`accounted delta bytes/key` 再除以实际 key 数。`native data live` 是逻辑存活 payload；`native reclaimable` 是 allocator 识别出的回收候选/提示量，不代表页面已经 trim，也不能从 `accounted` footprint 中扣除。`live object count` 用于观察 native object topology，而不是字节量。

进程 RSS 从 Linux `/proc/self/status` best-effort 读取，会受 warmup 残留、GC、JVM heap committed、JIT、native arena 和 OS residency 影响。`rss_delta` 不是 DB footprint delta，也不参与 accounted delta 或 bytes/key；不可用时 human/quiet 输出 `unavailable`，CSV 的 `rss_bytes` 和 `rss_delta_bytes` 留空。CSV 总计 21 列，其中 `pending_hash_table_count` 在成功的稳定 snapshot 中必须为 0；字段顺序以 `StorageBenchmarkRenderer` 为准。

## smoke.sh、bench.sh 和 storage-bench.sh

`scripts/smoke.sh` 是最小端到端健康检查：

- 默认只构建 server main 和 CLI 及其依赖，可用 `SKIP_BUILD=1` 跳过。
- 启动 server jar 到 `HOST:PORT`，默认 `127.0.0.1:16379`。
- 用 `READY_TIMEOUT_SEC` 控制 readiness 等待。
- 如果本机有 `redis-cli`，把它作为 RESP client 对 Yierdis 跑 `PING/SET/GET`；否则回退到项目 Java CLI。
- 可用 `ALLOCATOR_SMOKE=1` 额外跑 allocator-sensitive 命令路径。

`scripts/bench.sh` 是纯 connect-only 压测外壳：

- 除非 `SKIP_BUILD=1`，只构建 `yierdis-benchmark` 及其 Maven 依赖，然后定位 shaded benchmark jar。
- 必传默认值来自 `HOST`、`PORT`、`REQUESTS`、`CLIENTS`、`DATA_SIZE`、`PIPELINE` 和 `FORMAT`。
- 非空 `KEYSPACE` 才会成为 CLI argument，因此省略和显式零保持不同。
- 非空 `TESTS`、`KEEP_ALIVE`、`PRECISION`、`SEED`、`BENCH_USERNAME`、`PASSWORD`、`DATABASE` 才会追加；`KEEP_ALIVE=false` 会编码为单个 `--keep-alive=false` argument。`BENCH_USERNAME` 是 portable ACL username knob，并优先于仅为非保留环境保留的 `USERNAME` compatibility fallback；zsh 中不要使用 `USERNAME=value` 调用脚本。
- `BENCH_JVM_OPTS` 只控制 benchmark JVM。
- 脚本不查找、启动、轮询或停止任何 server artifact；目标 Yierdis 的生命周期始终由操作者管理。

`scripts/storage-bench.sh` 是进程内存储测量外壳：

- 除非 `SKIP_BUILD=1`，只构建 `yierdis-benchmark` 及其 Maven 依赖，然后定位 shaded benchmark jar。
- 固定调用 `storage` 子命令，不接收 host/port，也不启动 server。
- 默认值来自 `STORAGE_KEYS`、`STORAGE_KEY_SIZE`、`STORAGE_VALUE_SIZE`、`STORAGE_WARMUP_OPERATIONS`、`STORAGE_PRECISION` 和 `FORMAT`；`BENCH_JVM_OPTS` 只控制 benchmark JVM。

典型命令：

```bash
./scripts/smoke.sh
./scripts/bench.sh
FORMAT=csv KEYSPACE=0 KEEP_ALIVE=false ./scripts/bench.sh
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
./scripts/storage-bench.sh
STORAGE_KEYS=10000000 FORMAT=csv ./scripts/storage-bench.sh
```
