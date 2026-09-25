# 客户端与基准测试内部

本文覆盖项目内置 CLI、阻塞式 RESP client、RESP benchmark、进程内 storage benchmark，以及 bench/storage-bench 两个外壳脚本的工作方式。目标读者是要读这批源码、并需要按文档复现一次测量或排障的工程师。

先分清两条互不相同的边界：

- `yierdis-cli` 和 benchmark 根命令走真实 TCP、真实 RESP frame 和 `yierdis-networking-resp` 的 client codec，视角接近外部使用者。
- benchmark 的显式 `storage` 子命令是例外：它不走网络，直接创建 `RuntimeDbEngine`，只测 DB SET hot path 与存储 footprint。

CLI 面向人工交互和轻量验证；RESP benchmark 在固定 built-in workload 下测吞吐与延迟，并做最小 reply-shape 校验。默认 benchmark 只连接一个已经在跑的 Yierdis，不启动 Redis，也没有把两边合跑一遍的 harness。官方 Redis 基线由操作者在独立 Redis 环境里用官方工具单独测。

## 它们更适合验证什么

- CLI：快速确认协议、回包形态和单命令行为。
- RESP benchmark：观察高并发请求、pipeline 和 backpressure 行为下的吞吐与延迟；它不是 correctness oracle。
- storage benchmark：隔离观察单 owner DB SET 的吞吐、延迟和 heap/native footprint，不代表端到端吞吐。

比较两次结果时，必须先把输入对齐，再谈数字。对齐的输入是：`requests`、`clients`、`pipeline`、`data-size`、`keyspace`（省略 ≠ `0`）、keepalive、`database`。同时记录运行环境。项目自身不提供 AUTH/用户名/密码开关（见“两个外壳脚本”一节），所以“认证”不构成一个可对齐的输入维度。

## yierdis-cli

入口是 `YierdisCli.main(...)`，参数模型是 `YierdisCliArgs`（手写解析，第一个位置参数起即为命令单词）。参数与默认值如下：

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `--host <host>` | `127.0.0.1` | 目标 host。 |
| `--port <port>` | `6378` | 目标端口。 |
| `--timeoutMillis <ms>` | `5000` | 单条命令超时（毫秒）。 |
| `--hex` | 关闭 | 仅当 bulk string 不是合法 UTF-8 时，把它按十六进制打印。 |
| 位置参数 `COMMAND [ARG...]` | 空 | 0 个或多个；省略则进入 REPL。 |

`YierdisCliArgs.parse(...)` 只解析第一个位置参数之前的选项，其后的内容不再被当作选项。这意味着 `yierdis-cli GET --hex` 会把 `--hex` 当成 GET 的一个参数，而不是 CLI 选项——把选项放在命令之前。

单次命令模式：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar PING
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar --port 16379 SET a 1
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar GET a
```

单次模式下，每个字符串按 UTF-8 转成 `byte[]`，调用 `YierdisClient.execute(args, timeoutMillis)`，再按 Redis CLI 风格打印 reply：

- `NULL`（含 `$-1`）或空 reply → `(nil)`。
- `ERROR` → `(error) <text>`。
- `INTEGER` → 整数值。
- `BULK_STRING` → 能解成 UTF-8 就打印文本；否则在 `--hex` 时打印 `0x...`，否则按 UTF-8 尽力打印。
- `ARRAY` / `MAP` / `SET` → 逐项递归，带 `1) `、`2) ` 前缀。

退出码：reply 不是 `ERROR` 时为 `0`，是 `ERROR` 时为 `1`；`runClient` 抛异常时向 stderr 打印 `(error) <message>` 并返回 `1`。

REPL 模式（不带位置参数）：

```bash
java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar
```

进入后提示符是 `yierdis> `。行为：

- 空行（trim 后）跳过。
- 输入 `quit` 或 `exit`（大小写不敏感）时，best-effort 发一条 `QUIT` 后退出，返回 `0`——`QUIT` 失败被忽略。
- 读到 EOF（`readLine()` 返回 `null`）返回 `0`。
- 其他行经 `InlineCommandParser.splitUtf8(line, RespProtocolLimits.DEFAULT_MAX_ARGS)` 转成 argv，再走和单次模式相同的 `execute` 路径。解析失败打印 `(error) ...` 后继续下一行。

`--hex` 只影响展示，不改变发出去的协议内容。

源码入口：

- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCliArgs.java`

## InlineCommandParser

`InlineCommandParser` 是 Redis `sdssplitargs` 风格的 inline command parser，不是 `split(" ")`。它位于 `yierdis-networking-resp`，CLI REPL 和服务端 inline command 解码共用同一套规则。

语法规则（`parseSyntax`）：

- 空白分隔符只有 space 和 tab（`isSpace`），不含换行。
- 单引号内只特殊处理 `\'`（`\\'` → `'`）。
- 双引号内支持反斜杠转义：`\n`、`\r`、`\t`、`\b`、`\a`，其余字符保留原样。
- 双引号内 `\xHH` 解成一个字节（`HH` 必须是两位十六进制）。
- 闭合引号后必须紧跟空白或输入结束，否则报 `Protocol error: invalid inline command`。
- 引号未闭合报 `Protocol error: unbalanced quotes in inline command`。
- 整行为空（`argc == 0`）在 `requireArguments` 中报 `Protocol error: empty inline command`。
- 参数个数超过 `maxArgs` 报 `Protocol error: array length too large`；`RespProtocolLimits.DEFAULT_MAX_ARGS` 为 `1048576`。

API 分两层：

- `parseResult(...)` 返回具名的 `Parsed`，只保存已验证的输入快照、`argc()` 和 `retainedBytes()`，**不**分配 argv。`retainedBytes` 用 `HeapRequestFootprint` 计算，与 `ByteArrayExecutionRequest` 同口径。
- 调用方完成限制与内存准入后，再用一次性的 `takeArgs()` 物化 argv；同一个 `Parsed` 只能 `takeArgs()` 一次，否则抛 `inline arguments already taken`。
- `parse(...)`、`parseUnlimited(...)`、`splitUtf8(...)` 是 CLI 与普通调用方的便利入口。

服务端 decoder 走 `parseResult(...)`，好处是语法、参数数量和 retained bytes 都出自同一套 parser 规则，并且在限制检查前不分配完整请求载荷。

这也解释了 CLI REPL 为什么能输入带空格或二进制转义的参数：

```text
SET "hello key" "line\nvalue"
SET raw "\x00\x01"
```

CLI 和 server 共用 inline 语法，但边界不同：CLI 侧偏人手输入与单机验证，服务端侧偏协议适配与错误关闭。两边都不把 inline 解析当成 RESP array 的替代品，只是给手工调试和 `redis-cli` 风格输入留一条可控路径。

## YierdisClient

`YierdisClient` 是 blocking socket client，有意保持一问一答模型。

连接建立：

- `connect(host, port)` 新建 `Socket`，启用 `TCP_NODELAY`，连接超时固定为 `CONNECT_TIMEOUT_MILLIS = 5000` ms（与 `--timeoutMillis` 无关）。
- 连接失败时先关闭 socket，再把关闭异常作为 suppressed 挂到原异常上。

请求执行 `execute(List<byte[]> args, long timeoutMillis)`：

- `timeoutMillis <= 0` 直接抛 `IllegalArgumentException("timeoutMillis must be > 0")`。
- 全程 `synchronized (requestLock)`，保证同一个 client 实例一次只发一个请求。
- 每次请求前用 `socket.setSoTimeout(...)` 设读超时；超过 `Integer.MAX_VALUE` 的毫秒值会被钳到 `Integer.MAX_VALUE`。
- 用 `RespClientCodec.writeCommand(out, args)` 写请求，`flush()`，再用 `RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES)` 读一个 reply，`DEFAULT_MAX_BULK_BYTES` 为 `512 MiB`。

**不做 pipelining**：CLI 要的是清晰的请求/回包配对，不追求吞吐。

失败处理，以及为什么这样做：

- 读超时（`SocketTimeoutException`）→ 关连接，抛 `IllegalStateException("Timeout waiting for response (connection closed to prevent response desync)")`。
- 其他 `IOException`/`RuntimeException` → 关连接；消息里含 `eof` 或 `closed` 时抛 `Connection closed`，否则抛 `Invalid RESP reply (connection closed to prevent desync)`。

原因是 RESP reply 是 FIFO：一个请求超时后若保留连接，迟到的 reply 会被下一条请求读到，造成 desync。关闭连接比尝试“跳过未知 reply”更可靠。`YierdisClient` 实现 `AutoCloseable`，`close()` 是幂等的静默关连接。

源码入口：

- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java`
- `yierdis-cli/src/test/java/yier/bubu/redis/app/client/YierdisClientTest.java`

## RespClientCodec

`RespClientCodec` 位于 `yierdis-networking-resp`，不依赖 Netty。CLI 和 benchmark 都复用它。

写请求：

- `encodeCommand(List<byte[]> args)` 返回完整 RESP frame bytes。
- `writeCommand(OutputStream out, List<byte[]> args)` 写 RESP array，每个 argv 写成 bulk string。
- `null` argv 写成 RESP null bulk string（`$-1\r\n`），与 `readReply(...)` 把 `$-1` 读成 `NULL` 对称；空 `byte[]` 仍写成 `$0\r\n\r\n`，两者不混淆。
- 数字写出复用 `ThreadLocal<byte[]> INT_BUF`，减少临时分配。

读回包 `readReply(InputStream in, int maxBulkBytes)`：

- 支持 RESP simple string、error、integer、bulk string、array，以及 RESP3 null `_`。
- bulk string 会检查 `maxBulkBytes`，超过即报错。
- array 递归读取子 reply。
- 返回 `RespReply` record，含 `kind`、`text`、`bytes`、`integer`、`values`，并对 bytes/list 做防御性复制。

使用范围：CLI 用它做单请求通信；RESP benchmark 用它编码 workload，并在 `database != 0` 时编码一条 `SELECT` 前缀。`NioBenchmarkClient` 改用 `IncrementalRespReplyDecoder` 增量读取和校验 reply。storage benchmark 完全不经过 codec。

## yierdis-benchmark

`YierdisBench.main(...)` 是薄 launcher：`execute(...)` 按第一个位置参数路由——`storage` 进 `StorageBenchmarkCommand`（进程内 DB 路径），其余全部进 `RedisBenchmarkCommand`（真实 RESP 路径）。两者都不是 JMH microbenchmark。

配置与选项解析抛出的 `IllegalArgumentException`（未知选项、类型错误、配置越界）由 command 统一按用法错误处理：只打一行原因、退出码 2，不打 usage。launcher 在退出码非 0 时 `System.exit`。

### RESP benchmark

架构是固定的：

```text
RedisBenchmarkOptions
  -> BenchmarkConfig            # 唯一校验边界
  -> RedisBenchmarkCatalog.select()
  -> RedisBenchmark
  -> NioBenchmarkRunner (one Selector)
  -> LatencyRecorder
  -> BenchmarkCaseResult / BenchmarkRunResult
  -> BenchmarkOutputRenderer
```

`RedisBenchmarkOptions` 的选项、默认值与取值范围（范围校验在 `BenchmarkConfig` 的紧凑构造器里）：

| 选项 | 默认值 | 范围 / 说明 |
| --- | --- | --- |
| `--host` | `127.0.0.1` | 去空白后不能为空。 |
| `--port` | `16378` | `1..65535`。 |
| `--requests` | `100000` | `> 0`，每个 case 的测量请求数。 |
| `--clients` | `50` | `> 0`，并发 client 数。 |
| `--data-size` | `3` | `1..1073741824`（1 GiB），payload 字节数。 |
| `--pipeline` | `1` | `> 0`，每个 pipeline 发送的请求数。 |
| `--keyspace` | 未设置 | 可选；`>= 0`，且必须放进 12 位十进制（`< 1000000000000`）。 |
| `--keep-alive` | `true` | 关闭时每个 pipeline 批次后重连。 |
| `--tests` | 未设置 | 逗号分隔 selector，大小写不敏感。 |
| `--precision` | `3` | `0..4`，HdrHistogram 有效位数。 |
| `--seed` | 未设置 | 缺省时取 `System::nanoTime`，即不可复现。 |
| `--format` | `human` | `human` / `quiet` / `csv`。 |
| `--database` | `0` | `>= 0`；非 0 时每个新连接先发 `SELECT`。 |

**请求生成模型**（`RedisBenchmarkCommandTemplate.prepare`）：

1. 先编译一条具体的 command frame：`PING_INLINE` 用 raw `PING\r\n`（inline），其余 case 用 `RespClientCodec.encodeCommand(...)` 生成 RESP array。
2. 把该 frame 复制 `pipeline` 份拼成一个 `byte[]`（总长 `frame.length × pipeline`，上限 `Integer.MAX_VALUE - 8`）。
3. 若启用了 keyspace：把 frame 里所有 `__rand_int__` marker 就地改写成 `000000000000` 并记下偏移，运行时只原地覆写这 12 位数字；ZADD 的 score 用 marker（随机 12 位），未启用时用固定 `0`。
4. 若未启用 keyspace：marker 保持 literal，ZADD score 固定 `0`，每个 case 反复用同一组 key/member。

随机数来自 `BenchmarkRandom(seed)`（`SplittableRandom`），`writeTwelveDigits` 固定写 12 位。payload 由 `BenchmarkPayload.generate(size)` 生成，用确定性 LCG 产出 `'0' + (state >>> 16) & 63` 的字节，每个 catalog pass 生成一次、被需要 data 的 case 复用。因此**换了 `--seed` 就换了 key 的取值，但字节长度和命令形状不变**。

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
| any LRANGE selector | `LPUSH (needed to benchmark LRANGE)` | `SUCCESS`（setup row） |
| `lrange_100` | `LRANGE_100 (first 100 elements)` | `SUCCESS` |
| `lrange_300` | `LRANGE_300 (first 300 elements)` | `SUCCESS` |
| `lrange_500` | `LRANGE_500 (first 500 elements)` | `SUCCESS` |
| `lrange_600` | `LRANGE_600 (first 600 elements)` | `SUCCESS` |
| `mset` | `MSET (10 keys)` | `UNSUPPORTED` |
| `xadd` | `XADD` | `UNSUPPORTED` |

`RedisBenchmarkCatalog.select()` 的行为：空 selection（不传 `--tests`）返回完整 catalog；`ping` 同时命中两个 PING row；任一 LRANGE selector 会先选中 measured setup row `LPUSH`，再选依赖它的 LRANGE row；出现未知 selector 时抛 `SelectionException`，经 `RedisBenchmarkCommand` 转成 usage error。

当前 Yierdis 的默认结果是 17 个 `SUCCESS` + 4 个 `UNSUPPORTED`。真实连接或协议故障会把受影响的 supported row 记为 `FAILED`；LRANGE setup 失败还会把依赖 row 变成 `SKIPPED`。

**measurement 过程**（`NioBenchmarkRunner`）与可观测口径：

- 每个 case 打开一个 `Selector`，把 `clients` 个 non-blocking `SocketChannel` 注册到同一个 event loop；`measuredStartNanos` 记录测量开始时间。
- `writeIfReady`：client 处于 READY 时，只要 `issued < requests` 就发一个 pipeline 批次（`issued += pipeline`），记录 `batchStartNanos`；处于 WRITING 时继续写，写完转 awaitRead。
- `readReplies`：每读到一条 reply 先按 case 的 expected shape 校验（`BenchmarkReplyExpectation`：`PONG`/`OK`/`INTEGER`/`BULK_OR_NULL`/`ARRAY`），通过后 `completedReplies++`；只有前 `requests` 条 reply 进入 histogram（`histogramSamples < requested`），并记录该批次的首个可读时间作为 latency。
- **stop boundary**：当恰好记满 `requests` 个样本的那个 client（`thresholdClient`）把自己的待收 reply 收完后，记 `stopNanos` 并置 `stopping = true`，其余 client 被挂起。因此 throughput 用“stop boundary 已完成的 replies”除以“start→stop 的 elapsed”，而 histogram 只保留前 `requests` 个样本。
- `keepAlive == false` 时每批结束后 `replaceClient` 重连（旧连接关闭）。
- `database != 0` 时，每个新连接的第一条 measured write 前带一条 `SELECT n`，其 reply 按 `OK` 校验且不计入 `completedReplies`/histogram。

失败形态都会变成带 reason 的 `FAILED` row，不伪造吞吐或延迟：

- RESP error 或 shape 不符 → 校验抛 `IOException`（如 `expected PONG`、`expected an integer reply`）。
- 多出计划外的 reply → `benchmark server sent an unexpected extra reply`。
- 对端断开且仍有未收 reply → `disconnect with benchmark replies outstanding`。
- 30 秒无任何进展（默认 `DEFAULT_NO_PROGRESS_TIMEOUT = 30s`）→ `no progress timeout after PT30S`。

`LatencyRecorder`（case 用 `micros(precision)`）：`lowestDiscernible = 10` µs、`highestTrackable = 3000000` µs（3 秒）、`recordFloor = 0`，超过上限的样本钳进最高桶。`summary()` 给出 mean、min、p50、p95、p99、max。`BenchmarkCaseResult` 的状态只有 `SUCCESS`、`UNSUPPORTED`、`SKIPPED`、`FAILED`；`BenchmarkRunResult` 保留 catalog 顺序，并由任意 `FAILED` 决定非零退出码。四个 unsupported case 不发送网络请求，直接产出 `UNSUPPORTED` row。

### RESP benchmark 的输出

CSV header 恰好是（`BenchmarkOutputRenderer.CSV_HEADER`）：

```text
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms","status","reason"
```

前八列（`test`、`rps`、`avg_latency_ms`、`min_latency_ms`、`p50_latency_ms`、`p95_latency_ms`、`p99_latency_ms`、`max_latency_ms`）与官方 Redis-style CSV 对齐，是两份独立结果的**共享比较面**；`status` 和 `reason` 是 Yierdis 扩展。这个“前八个字段”的口径与 `production-hardening-operations.md` 一致，比较时必须按 canonical title 配对，且只能落在前八列上。

非 `SUCCESS` row 的七个 numeric fields 全部留空，不能读成 `0`。human format 逐 case 打印：

```text
====== <title> ======
  <N> requests completed in <X> seconds
  <N> parallel clients
  <N> bytes payload
  keep alive: 0|1

Summary:
  throughput summary: <rps> requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        ...
```

非成功 row 在 human 下打印 `status:`、`reason:`（`FAILED` 还多一行 `completed replies:`）；quiet 下打印 `<title>: <rps> requests per second, p50=<...> msec`，或 `<status> (<reason>)`。CSV 字段用双引号包裹并把内部 `"` 转义成 `""`；human/quiet 里的 `\r`/`\n`/`\\` 被转义为字面文本。

### 如何复现一次对比

1. 用同一个 JDK 启动一个 Yierdis（见 `production-hardening-operations.md` 的 JDK 25 约定）：

   ```bash
   printf 'port=16378\nmaxmemoryBytes=0\n' > /tmp/yierdis-bench-target.conf
   java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --config /tmp/yierdis-bench-target.conf
   ```

2. 固定输入并导出 CSV：

   ```bash
   SKIP_BUILD=1 FORMAT=csv HOST=127.0.0.1 PORT=16378 \
   REQUESTS=100000 CLIENTS=50 DATA_SIZE=3 PIPELINE=1 \
   ./scripts/bench.sh > target/yierdis-benchmark.csv
   ```

3. 在独立 Redis 环境里用官方工具，用等价输入单独跑一次，按 canonical title 配对，只比前八列。
4. 想复现同一组随机 key，必须显式传 `SEED`；否则 `--seed` 默认取 `System::nanoTime`，两次运行不可复现。

项目 benchmark 不启动 Redis、不调用官方工具、不收集 Redis 结果、也不定义 combined run。artifact identity、环境记录、结果保存、ratio 和 release threshold 都是 benchmark 之外的 operator policy。

## storage benchmark

`storage` 子命令的测量边界固定为单线程、单 owner、进程内 `RuntimeDbEngine`，不含 TCP、RESP、server dispatch 或 executor。它回答的是“native payload + heap topology 的存储成本”和“DB hot-path 上限”，不要把它的 ops/s 与 RESP benchmark 直接比。

```text
StorageBenchmarkOptions
  -> StorageBenchmarkConfig
  -> StorageBenchmarkRunner
  -> disposable warmup RuntimeDbEngine
  -> measured single-owner RuntimeDbEngine
  -> LatencyRecorder / StorageMemorySnapshot
  -> StorageBenchmarkResult
  -> StorageBenchmarkRenderer
```

选项、默认值与范围（范围校验在 `StorageBenchmarkConfig`）：

| 选项 | 默认值 | 范围 / 说明 |
| --- | --- | --- |
| `--keys` | `1000000` | `1..10000000`（`MAX_KEYS`），unique SET key 数。 |
| `--key-size` | `16` | 下限 `digits(keys-1)+1`（要能放下 `k` 前缀加最大 index），上限 `1024`。 |
| `--value-size` | `16` | `0..1048576`。 |
| `--warmup-operations` | `50000` | `0..1000000`，在一次性 warmup DB 上做。 |
| `--precision` | `3` | `0..4`。 |
| `--format` | `human` | `human` / `quiet` / `csv`。 |

测量流程（`StorageBenchmarkRunner.run`）：

1. warmup：起一个一次性 DB，做 `warmup-operations` 次 SET（key index 取 `operation % keys`），完成后 shutdown；`warmup-operations == 0` 则跳过。
2. 起正式 DB，`bindToCurrentThread`，先记 empty baseline snapshot。
3. **阶段一 SET**：`keys` 个唯一 key（`'k'` + 左补零数字）逐个 `strings.setString(k, value, NORMAL, null)`，校验写入成功；用整个循环的起止时间算吞吐。
4. `stabilizeHashTables`：循环 `engine.runMaintenance()` 直到 `pendingHashTableCount == 0`（上限 100000 tick），再记 loaded snapshot；校验 `keyCount == keys` 且 `pending == 0`，否则抛异常。这一步在**计时窗口之后**，不计入 SET throughput。
5. **阶段二 TTL churn**：用 `'c'` 前缀的 key（避免覆盖已加载的 `'k'` keyspace）做 `keys` 次 SET 后紧跟 `ttl().pexpire(key, 0)`（TTL<=0 立即删除路径），每次操作带一次删除。
6. **阶段三 DEL**：对 `'k'` keyspace 逐个 `keyspace().del([key])`，期望返回 `1`；结束后 `keyCount` 必须为 `0`。

驱动侧固定宽度 mutable key buffer 在循环内复用（`encodeKeyIndex`），所以 driver 不为每个 key 新建数组；每个 SET latency 用 `LatencyRecorder.nanos(precision)` 记录，`lowestDiscernible = 1` ns、`highestTrackable = 10s`、`recordFloor = 1`（不足 1 ns 钳到 1 ns）。因此这个 throughput 含 key 编码、两次计时读取、结果校验和 histogram record，属于 instrumented direct workload throughput，不等于裸 `setString(...)` 的理论上限。

footprint 核算（`StorageMemorySnapshot`）：`accounted = heap estimated + native metadata committed + native data committed`。字段含义：

- `accounted delta` 是 loaded 减 empty baseline，`accounted delta bytes/key` 再除以 `keys`。
- `native data live` 是逻辑存活 payload；`native reclaimable` 是 allocator 识别的回收候选/提示量，不表示页面已 trim，也不能从 `accounted` 里扣除。
- `live object count` 用来观察 native object topology，不衡量字节量。
- `pending_hash_table_count` 在成功的稳定 snapshot 中必须为 `0`。
- `rss_bytes`/`rss_delta_bytes` 从 Linux `/proc/self/status` best-effort 读取，受 warmup 残留、GC、JVM heap committed、JIT、native arena 和 OS residency 影响；不是 DB footprint delta，也不参与 accounted delta 或 bytes/key。不可用时 human/quiet 打印 `unavailable`，CSV 留空。

CSV 总计 **29 列**，顺序以 `StorageBenchmarkRenderer.CSV_HEADER` 为准：

```text
"keys","key_size_bytes","value_size_bytes","warmup_operations","elapsed_seconds","ops_per_second","p50_latency_ns","p99_latency_ns","heap_estimated_bytes","native_metadata_committed_bytes","native_data_committed_bytes","native_data_live_bytes","native_reclaimable_bytes","accounted_bytes","baseline_accounted_bytes","accounted_delta_bytes","accounted_delta_bytes_per_key","live_object_count","pending_hash_table_count","rss_bytes","rss_delta_bytes","ttl_churn_elapsed_seconds","ttl_churn_ops_per_second","ttl_churn_p50_latency_ns","ttl_churn_p99_latency_ns","del_elapsed_seconds","del_ops_per_second","del_p50_latency_ns","del_p99_latency_ns"
```

前 21 列是 SET phase 的量；`ttl_churn_*`（第 22–25 列）与 `del_*`（第 26–29 列）分别对应阶段二、阶段三。storage CSV 与 RESP CSV 是两套不同 header，不要混用或按列位置硬对齐。

## bench.sh 和 storage-bench.sh

两个脚本都只跑 benchmark：它们**不**查找、启动、轮询或停止任何 server artifact，目标 Yierdis 的生命周期始终由操作者管理。

### scripts/bench.sh

纯 connect-only 压测外壳：

- 除非 `SKIP_BUILD=1`，用 `mvn -pl yierdis-benchmark -am -q -DskipTests package` 构建 shaded benchmark jar。
- 必传参数来自 `HOST`/`PORT`/`REQUESTS`/`CLIENTS`/`DATA_SIZE`/`PIPELINE`/`FORMAT`，默认值与 CLI 一致（`127.0.0.1`/`16378`/`100000`/`50`/`3`/`1`/`human`）。
- 只有非空的 `KEYSPACE`/`TESTS`/`KEEP_ALIVE`/`PRECISION`/`SEED`/`DATABASE` 才追加成参数；因此“省略 KEYSPACE”和“显式 `KEYSPACE=0`”不同。`KEEP_ALIVE=false` 会编码为单个 `--keep-alive=false`。
- `BENCH_JVM_OPTS` 只控制 benchmark JVM。
- 脚本**不支持** AUTH/用户名/密码（没有 `--username`/`--password` 或相关环境变量）；`RedisBenchmarkOptions` 里同样没有对应选项。这与 `production-hardening-operations.md` 的“无 AUTH”声明一致。

### scripts/storage-bench.sh

进程内存储测量外壳：

- 除非 `SKIP_BUILD=1`，只构建 `yierdis-benchmark` 及其依赖，然后定位 shaded benchmark jar。
- 固定调用 `storage` 子命令，不接收 host/port，也不启动 server。
- 默认值来自 `STORAGE_KEYS`（`1000000`）、`STORAGE_KEY_SIZE`（`16`）、`STORAGE_VALUE_SIZE`（`16`）、`STORAGE_WARMUP_OPERATIONS`（`50000`）、`STORAGE_PRECISION`（`3`）、`FORMAT`（`human`）；`BENCH_JVM_OPTS` 只控制 benchmark JVM。

典型命令：

```bash
./scripts/bench.sh
FORMAT=csv KEYSPACE=0 KEEP_ALIVE=false ./scripts/bench.sh
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 SEED=12345 ./scripts/bench.sh
./scripts/storage-bench.sh
STORAGE_KEYS=10000000 FORMAT=csv ./scripts/storage-bench.sh
```