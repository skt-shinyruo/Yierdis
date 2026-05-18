# Client And Bench Internals

本文解释项目内置 CLI、Netty client、benchmark 和 smoke/bench 脚本如何沿真实 RESP 路径工作。

## 先记住一句话

`yierdis-cli` 和 `yierdis-benchmark` 不是进程内 DB 调试入口。它们都通过真实 TCP、真实 RESP frame 和 `yierdis-networking-resp` 的 client codec 工作，因此更接近外部使用者视角。

CLI 负责人工交互和轻量验证；benchmark 负责固定 workload shape 下的吞吐、延迟和 correctness smoke。比较 benchmark 结果时必须保证 workload shape 一致，例如相同的 `requests`、`clients`、`pipeline`、`keyspace`、`dataSize`、server jar、JVM 参数和 server 启动参数。baseline/current 任一侧启动失败、协议错误、返回错误或缺少必要测量时，输出应视为 `non-comparable`，不能当成可信性能结论。

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

`InlineCommandParser` 是 Redis `sdssplitargs` 风格的 inline command parser，不是简单 `split(" ")`。

它支持：

- space / tab 空白分隔。
- 单引号，单引号内只特殊处理 `\\'`。
- 双引号。
- 双引号内反斜杠转义：`\n`、`\r`、`\t`、`\b`、`\a` 和默认保留字符。
- 双引号内 `\xHH` 十六进制字节。
- `maxArgs` 上限，超出时报 `Protocol error: array length too large`。

`parse(...)` 返回 `Decoded`，其中保存一块 decoded byte buffer，以及每个 argv 的 offset/length。`splitUtf8(...)` 是 CLI REPL 使用的便利方法，会把 Java `String` 先编码成 UTF-8，再复制出每个 argv。

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

这个 codec 是 client-side 工具路径的事实标准：CLI 用它做单请求通信，benchmark 用它写 workload frame 和读 reply。

## yierdis-benchmark

`yierdis-benchmark` 是真实协议路径 benchmark，不是 JMH microbenchmark，也不是直接调用 DB API。

入口是 `YierdisBench.main(...)`。流程是：

```text
YierdisBenchArgs
  -> parse unmatched server args into YierdisBenchServerArgs
  -> normalizeAndValidate()
  -> BenchConfig.from(...)
  -> optional ServerProcess
  -> ThroughputWorker / LatencyWorker
  -> RespCommandWriter + RespClientCodec
  -> summary / comparison output
```

`YierdisBenchArgs` 定义 bench 自己的参数，例如 `--host`、`--portBase`、`--noStartServer`、`--serverJar`、comparison mode、server JVM 参数、`--keyspace`、`--dataSize`、`--requests`、`--clients`、`--pipeline`、latency 参数、`--skipPrefill`、`--skipLatency`、`--strictReplies` 和 native eval 参数。`@Unmatched serverArgs` 接住 `--` 后的 server 启动参数。

`YierdisBenchServerArgs` 是 bench-local server launch argv 模型。它覆盖 port、DB 数量、cleanup、executor/backpressure、transaction queue、protocol limits、maxmemory、eviction、expire cleanup、native defrag 和 `KEYS` budget。它会归一化 `executorSchedulingPolicy`、`maxmemoryScope` 和 `maxmemoryPolicy`，再由 `toArgv()` 生成子进程 server 参数。

重要 caveat：bench launch argv model 不是完整 server args model。`SERVER_ARGS_EXTRA` 会先通过 `YierdisBenchServerArgs` 的 picocli parser，再由 `toArgv()` 传给 server child process；当前不包含 server-only 的 `--client-idle-timeout-millis`、`--client-output-buffer-limit-bytes`、`--client-output-buffer-over-limit-millis`。要验证慢客户端保护，应直接启动 server，或先扩展 bench model。

## ServerProcess

非 `--noStartServer` 模式下，benchmark 会启动真实 server 子进程。`ServerProcess` 负责：

- 拼 `java` 命令。
- 加上 `-Xms`、`-Xmx`、`-XX:MaxDirectMemorySize`。
- 加上 `-jar <serverJar>`。
- 加上 `YierdisBenchServerArgs.toArgv()` 生成的 server argv。
- 将 stdout/stderr 重定向到 log file。
- 停止时先 `destroy()`，超时后 `destroyForcibly()`。

这意味着 benchmark 的默认模式会覆盖真实进程启动、真实 Netty server、真实 RESP decode/execute/reply 路径。`--noStartServer` 则连接已有 server，适合手工启动特殊参数后跑同一 workload。

comparison mode 会分别启动 baseline/current jar。只有双方完成同一组必要测量且没有 workload/protocol/reply errors 时，结果才标记 comparable；否则 summary 里会标记 `non-comparable`。

## workload workers

当前 workload 包括：

- `PING`
- `SET_RANDOM`
- `SET_SEQUENTIAL`
- `APPEND`
- `GET_RANDOM`
- `PFADD_SPARSE`
- `PFADD_DENSE`
- `PFCOUNT`

`ThroughputWorker` 面向吞吐：

- 每个 worker 建立自己的 socket。
- 使用 `BufferedOutputStream` / `BufferedInputStream`。
- 支持 pipeline：一批写出多个 request，flush 后按同样数量读 reply。
- 用 `SplittableRandom` 在 keyspace 内选 key。
- 返回 `WorkerCounter(ops, errors)`。

`LatencyWorker` 面向单请求往返延迟：

- pipeline 固定等价于 `1`。
- 每次写一条、flush、读一条。
- 用 `System.nanoTime()` 记录每个 request round trip。
- 返回 samples 和 errors，主线程再算分位数。

`RespCommandWriter` 是 workload 写出热点。它预置常用命令字节和 `PING` frame，复用 `MutableRequestArgs` 这个 `AbstractList<byte[]>`，再调用 `RespClientCodec.writeCommand(...)`。这样既保留真实 RESP request 编码路径，又减少每条命令额外创建 list 的成本。

## strict reply validation

`--strictReplies` 开启最小语义校验。未开启时，worker 主要区分正常 reply 和 RESP error reply；开启后会进一步验证 workload 对应的基本语义：

- `PING` 必须是 `PONG`。
- `SET_RANDOM` / `SET_SEQUENTIAL` 必须是 `OK`。
- `APPEND` 必须是非负 integer，且通常不小于写入 value size。
- `PFADD_*` 必须是 `0` 或 `1`。
- `PFCOUNT` 必须是非负 integer。
- `GET_RANDOM` 可以是 null bulk；如果是 bulk string，长度要匹配 `dataSize`。

strict reply validation 让 benchmark 兼有 correctness smoke 的作用。若 strict 校验失败，worker 会计入 errors；comparison mode 下这类 errors 会让结果不可比较。

## smoke.sh 和 bench.sh

`scripts/smoke.sh` 是最小端到端健康检查：

- 默认先 `mvn -q -DskipTests package`，可用 `SKIP_BUILD=1` 跳过。
- 启动 server jar 到 `HOST:PORT`，默认 `127.0.0.1:16379`。
- 用 `READY_TIMEOUT_SEC` 控制 readiness 等待。
- 如果本机有 `redis-cli`，用它跑 `PING/SET/GET`；否则回退到项目 Java CLI。
- 可用 `ALLOCATOR_SMOKE=1` 额外跑 allocator-sensitive 命令路径。
- 最后用 benchmark 的 `--noStartServer --strictReplies --skipLatency` 跑很小的 correctness smoke。

`scripts/bench.sh` 是压测外壳：

- 默认构建 server/bench jar。
- 通过环境变量组装 bench 参数：`HOST`、`PORT_BASE`、`KEYSPACE`、`DATA_SIZE`、`REQUESTS`、`CLIENTS`、`PIPELINE`、`LATENCY_REQUESTS`、`LATENCY_CLIENTS`。
- 通过 `XMS`、`XMX`、`MAX_DIRECT_MEMORY` 控制 server child process JVM。
- 通过 `MAXMEMORY_BYTES`、`MAXMEMORY_POLICY`、`MAXMEMORY_SAMPLES` 和 `SERVER_ARGS_EXTRA` 追加 bench model 支持的 server args。
- 通过 `BENCH_ARGS_EXTRA` 和 `BENCH_JVM_OPTS` 控制 benchmark 进程本身。

典型命令：

```bash
./scripts/smoke.sh
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
```

再次强调：`SERVER_ARGS_EXTRA` 不是无限制透传。它会被 shell split 后交给 `YierdisBenchServerArgs` 解析；bench model 未声明的 server-only 参数会解析失败。

## 推荐源码和测试

推荐源码：

- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCliArgs.java`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespClientCodec.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`
- `scripts/smoke.sh`
- `scripts/bench.sh`

推荐测试：

- `YierdisClientTest`：client 连接、超时、desync 防护和 RESP 使用方式。
- `YierdisCli` / `InlineCommandParser` 当前主要通过源码和 CLI 路径测试间接覆盖；补 parser 行为时应优先补专门的 parser 单测。
- `RespClientCodecTest`：RESP request 编码和 reply 解析。
- `RespCommandWriterTest`：benchmark request writer 和 RESP codec 复用。
- `BenchServerArgsReuseTest`：bench server launch argv 归一化和复制。
- `YierdisBenchSummaryFormatTest`：summary 输出稳定性。
- `YierdisBenchComparisonRenderTest`：comparison / non-comparable 输出。
- `SmokeScriptContractTest`：`scripts/smoke.sh` readiness 和 allocator smoke contract。
- `BenchScriptContractTest`：`scripts/bench.sh` 环境变量 contract。
- `MaxmemoryScopeTest` 和 `TransactionQueueLimitTest`：从 client 视角覆盖 server 参数影响。
