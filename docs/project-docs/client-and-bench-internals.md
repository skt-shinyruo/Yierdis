# Client And Bench Internals

本文解释两个经常被当成“配套工具”的模块：

- `yierdis-cli`
- `yierdis-benchmark`

它们看起来像外围工具，但实际上很重要，因为它们直接体现了：

- RESP 是如何被消费的
- 仓库作者自己是如何验证 server 行为和请求路径的

## 先记住一句话

client 和 bench 不是直接嵌进 DB 内部的调试入口，而是：

- 走真实 TCP
- 走真实 RESP
- 尽量复用 `yierdis-networking-resp` 的编码/解析组件

这让它们更接近“真实外部使用者”，而不是“进程内私有测试钩子”。

## `yierdis-cli` 在做什么

client 模块主要有四个角色：

- `YierdisCli`
- `YierdisCliArgs`
- `YierdisClient`
- `InlineCommandParser`

协议读写由 `RespClientCodec` 完成。

## CLI 的主链

命令行入口是：

- `YierdisCli.main(...)`

大致流程是：

1. 用 `YierdisCliArgs` 解析 host/port/timeout/hex 等参数
2. 建立 `YierdisClient.connect(...)`
3. 如果是单次命令执行，则直接把命令编码后发出
4. 如果没有位置参数，则进入 REPL
5. REPL 中每一行通过 `InlineCommandParser` 解析成 argv
6. reply 按 Redis 风格打印；`--hex` 只影响非 UTF-8 bulk string 的展示

这说明 CLI 本身非常薄：

- 参数解析和交互控制在 CLI
- 真正的协议通信在 `YierdisClient`

## `InlineCommandParser` 的角色

这个类实现的是 Redis `sdssplitargs` 风格的输入解析。

它支持：

- 空白分隔
- 单引号
- 双引号
- 反斜杠转义
- `\\xHH` 十六进制转义

所以 CLI 在交互模式下并不是简单 `split(" ")`，而是刻意做成更接近 Redis CLI 输入体验。

## `YierdisClient` 的内部模型

`YierdisClient` 是一个基于 blocking socket 的客户端，并且故意保持了非常简单的请求模型：

- 1-at-a-time request/response
- 不做 pipelining

### 执行请求

`execute(List<byte[]> args, timeoutMillis)` 的逻辑很有代表性：

1. 检查 client 是否已关闭
2. 在 `requestLock` 下保证一次只发一条请求
3. 使用 `RespClientCodec.writeCommand(...)` 生成 RESP request
4. 写入 socket output stream
5. 用 `RespClientCodec.readReply(...)` 读取一个 response
6. 超时或解析失败则主动关闭连接，避免 response pairing 失步

### 为什么超时后要关连接

因为它采用的是严格 FIFO 的一问一答模型。

如果一条请求超时，但连接仍然保留：

- 迟到的 reply 可能会被下一条请求错配

所以 client 明确选择：

- 发生 timeout 或 parse failure 就关闭连接，防止 desync

这是一种很保守但很清晰的协议消费策略。

## `yierdis-benchmark` 在做什么

bench 模块不是 JMH，也不是进程内 microbenchmark。

它的定位更接近：

- 通过真实 TCP 和 RESP 跑一组可重复的 workload
- 输出吞吐和延迟结果

这点非常重要，因为它说明 benchmark 的关注点是：

- request path
- protocol path
- server child process 启动参数

而不是单个 Java 方法的纳秒级 microbenchmark。

## `YierdisBench` 的总体结构

可以把它记成下面几层：

```text
YierdisBenchArgs
  -> parse bench-local server launch argv
  -> BenchConfig
  -> optional ServerProcess
  -> ThroughputWorker / LatencyWorker
  -> RespCommandWriter + RespClientCodec
  -> summary output
```

## bench 为什么有自己的 server launch argv 模型

`YierdisBench` 在处理 bench 自己的参数之后，还会：

1. 再创建一个 `YierdisBenchServerArgs`
2. 解析用户附带的 server 参数
3. 调 `normalizeAndValidate()`
4. 通过 `BenchConfig.from(...)` 保存成启动 server child process 的基础配置

这意味着：

- bench 不依赖 server runtime config
- 它只维护启动子进程所需的 argv 归一化模型

这样做的好处是：

- bench 不再通过共享参数模块形成隐藏依赖桥
- server runtime config 可以留在 `yierdis-server-main`

## `ServerProcess` 做什么

如果不是 connect-only 模式，bench 会启动一个真实的 server 子进程。

`ServerProcess` 负责：

- 拼 JVM 参数
- 拼 `-jar` 和 server argv
- 启动子进程
- 输出日志到文件
- 停止子进程

这里的 server argv 来自：

- `YierdisBenchServerArgs.toArgv()`

也就是说，bench 不是随便拼字符串，而是从自己的 launch argv 模型生成稳定参数。

## workload 是怎么跑的

当前 benchmark workload 主要有：

- `PING`
- `SET_RANDOM`
- `SET_SEQUENTIAL`
- `GET_RANDOM`

bench 会区分两类 worker：

### `ThroughputWorker`

特点：

- 支持 pipeline
- 一批写出后批量读回 reply
- 用于测吞吐

### `LatencyWorker`

特点：

- pipeline = 1
- 一次写一条，一次收一条
- 记录单次请求往返耗时

### `RespCommandWriter`

这是 bench 最值得注意的内部组件之一。

它不会每次都走最重的高层路径，而是：

- 预置 `PING/GET/SET` 命令字节
- 复用 `MutableRequestArgs`
- 复用 `intBuf`
- 通过 `RespClientCodec.writeCommand(...)` 写 RESP frame

这说明 bench 很关心：

- request 写出路径本身的分配成本

## strict reply validation 是什么

bench 可以开启：

- `--strictReplies`

这时它不仅看“有没有回包”，还会做最小语义校验，例如：

- `GET` reply 是否真的匹配预期大小
- `PING` / `SET` reply 是否是合法 RESP simple string
- null bulk 和 bulk string 是否按 workload 预期出现

这让 bench 不只是性能工具，也兼带一层 correctness smoke。

## `scripts/smoke.sh` 和 `scripts/bench.sh` 的角色

仓库根目录两个脚本其实是这条工具链的外壳：

### `scripts/smoke.sh`

适合做：

- server jar / client jar / bench jar 是否都能正常工作
- server 启动后 `redis-cli` 是否能连；如果本机没有 `redis-cli`，回退到 Java CLI
- bench strictReplies correctness smoke 是否通过
- 通过 `READY_TIMEOUT_SEC` 调整 server readiness 等待时间

### `scripts/bench.sh`

适合做：

- 一键启动 RESP benchmark
- 透传 JVM 参数
- 透传 bench server launch argv 模型支持的 server 参数
- 透传 bench 额外参数
- 复现实验环境

这里的“server 参数”不是直接无限制转发给 `yierdis-server-main`。`SERVER_ARGS_EXTRA` 会先进入 `YierdisBenchServerArgs` 的 picocli parser，再由 `toArgv()` 生成子进程 argv；因此它只支持 bench launch 模型里声明过的参数。当前 bench 模型覆盖 port、DB 数量、cleanup、executor/backpressure、transaction queue、protocol limits、maxmemory、KEYS budget 等参数，但不包含 server-only 的 client idle/output-buffer 慢客户端保护参数。需要验证这类 server-only 参数时，应直接启动 server 或先扩展 bench launch argv 模型。

所以脚本层不是另起一套实现，而是把：

- `yierdis-server-main`
- `yierdis-cli`
- `yierdis-benchmark`

三者拼成一条可复用的工作流。

## 最值得看的测试

- `YierdisClientTest`
  看 client 的连接、超时、desync 防护和 RESP 使用方式。
- `RespClientCodecTest`
  看 RESP request 编码和 reply 解析。
- `TransactionQueueLimitTest`
  看 client 视角下事务队列限制如何体现。
- `MaxmemoryScopeTest`
  看 client 视角下全局/per-db 预算行为。
- `RespCommandWriterTest`
  看 bench request writer 如何复用 RESP codec，并关注分配成本。
- `BenchServerArgsReuseTest`
  看 bench 的 server launch argv 复制和归一化如何保持与 server 参数对齐。
- `YierdisBenchSummaryFormatTest`
  看 bench 输出格式是否稳定。

## 对照源码时推荐看的顺序

1. `YierdisCli`
2. `InlineCommandParser`
3. `YierdisClient`
4. `RespClientCodec`
5. `YierdisBenchArgs`
6. `YierdisBench`
7. `scripts/smoke.sh`
8. `scripts/bench.sh`

## 一句话总结

client 和 bench 看起来是外围工具，但它们其实是：

- RESP 的第一批消费者
- request-path 和运维脚本的真实验证者

如果你想知道“作者自己是怎么和这个 server 交互、验证和压测的”，这两个模块就是最直接的答案。
