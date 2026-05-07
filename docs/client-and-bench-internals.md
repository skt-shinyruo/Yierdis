# Client And Bench Internals

本文解释两个经常被当成“配套工具”的模块：

- `apps/yierdis-client`
- `apps/yierdis-bench`

它们看起来像外围工具，但实际上很重要，因为它们直接体现了：

- 自定义协议是如何被消费的
- 仓库作者自己是如何验证 server 行为和请求路径的

## 先记住一句话

client 和 bench 不是直接嵌进 DB 内部的调试入口，而是：

- 走真实 TCP
- 走真实 Custom Protocol v1
- 尽量复用协议编码/解析组件

这让它们更接近“真实外部使用者”，而不是“进程内私有测试钩子”。

## `apps/yierdis-client` 在做什么

client 模块主要有四个角色：

- `YierdisCli`
- `YierdisCliArgs`
- `YierdisClient`
- `InlineCommandParser`

外加一个小型 reply helper：

- `CustomProtocolV1Replies`

## CLI 的主链

命令行入口是：

- `YierdisCli.main(...)`

大致流程是：

1. 用 `YierdisCliArgs` 解析 host/port/timeout/hex 等参数
2. 建立 `YierdisClient.connect(...)`
3. 如果是单次命令执行，则直接把命令编码后发出
4. 如果没有位置参数，则进入 REPL
5. REPL 中每一行通过 `InlineCommandParser` 解析成 argv
6. reply 直接打印为 NDJSON 文本，或在 `--hex` 下打印原始 line 的 hex

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

`YierdisClient` 是一个基于 Netty 的客户端，但它故意保持了非常简单的请求模型：

- 1-at-a-time request/response
- 不做 pipelining

### 连接建立

`connect(host, port)` 会：

1. 创建一个 `NioEventLoopGroup(1)`
2. 用 Netty `Bootstrap` 建立连接
3. pipeline 中加入：
   - `JsonLineDecoder`
   - `ClientHandler`

这意味着 client 侧接收的 framing 不是 request decoder，而是 reply line decoder。

### 执行请求

`execute(List<byte[]> args, timeoutMillis)` 的逻辑很有代表性：

1. 检查 client 是否已关闭或已出现 terminal error
2. 在 `requestLock` 下保证一次只发一条请求
3. 先清掉任何意外残留 response
4. 使用 `CustomProtocolV1RequestEncoder.encodeRequestFrame(args)` 生成 request frame
5. 写入 channel 并等待一个 response event
6. 超时则主动关闭连接，避免 response pairing 失步
7. 收到 line 后，再用 `CustomProtocolV1ReplyParser.parse(...)` 解析 reply

### 为什么超时后要关连接

因为它采用的是严格 FIFO 的一问一答模型。

如果一条请求超时，但连接仍然保留：

- 迟到的 reply 可能会被下一条请求错配

所以 client 明确选择：

- 发生 timeout 就关闭连接，防止 desync

这是一种很保守但很清晰的协议消费策略。

## `CustomProtocolV1Replies` 的角色

这个类不是底层 parser，而是 client/CLI 视角的辅助函数集合。

它封装了最常见的 reply 访问模式：

- 判断 `ok`
- 取 `result`
- 取 `error`
- 解码 `$map`
- 解码 `$b64`

也就是说：

- `CustomProtocolV1ReplyParser` 负责底层解析
- `CustomProtocolV1Replies` 负责上层使用便利性

## `apps/yierdis-bench` 在做什么

bench 模块不是 JMH，也不是进程内 microbenchmark。

它的定位更接近：

- 通过真实 TCP 和自定义协议跑一组可重复的 workload
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
  -> CustomCommandWriter + JsonReplyReader
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
- server runtime config 可以留在 `yierdis-server-app`

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

### `CustomCommandWriter`

这是 bench 最值得注意的内部组件之一。

它不会每次都走最重的高层路径，而是：

- 预置 `PING/GET/SET` 命令字节
- 复用 `MutableRequestArgs`
- 复用 `intBuf`
- 通过 `CustomProtocolV1RequestEncoder.writeRequestFrame(...)` 写 frame

这说明 bench 很关心：

- request 写出路径本身的分配成本

## strict reply validation 是什么

bench 可以开启：

- `--strictReplies`

这时它不仅看“有没有回包”，还会做最小语义校验，例如：

- `GET` reply 是否真的匹配预期大小
- reply 是否是合法 envelope
- UTF-8 和 `$b64` 路径是否都可接受

这让 bench 不只是性能工具，也兼带一层 correctness smoke。

## `scripts/smoke.sh` 和 `scripts/bench.sh` 的角色

仓库根目录两个脚本其实是这条工具链的外壳：

### `scripts/smoke.sh`

适合做：

- server jar / client jar / bench jar 是否都能正常工作
- server 启动后 CLI 是否能连
- bench strictReplies correctness smoke 是否通过

### `scripts/bench.sh`

适合做：

- 一键启动 benchmark
- 透传 JVM 参数
- 透传 server 参数
- 复现实验环境

所以脚本层不是另起一套实现，而是把：

- `yierdis-server-app`
- `apps/yierdis-client`
- `apps/yierdis-bench`

三者拼成一条可复用的工作流。

## 最值得看的测试

- `YierdisClientTest`
  看 client 的连接、超时、desync 防护和协议使用方式
- `TransactionQueueLimitTest`
  看 client 视角下事务队列限制如何体现
- `MaxmemoryScopeTest`
  看 client 视角下全局/per-db 预算行为
- `CustomCommandWriterTest`
  看 bench request writer 如何复用共享 encoder，并关注分配成本
- `BenchServerArgsReuseTest`
  看 bench 如何复用 server args SSOT
- `YierdisBenchSummaryFormatTest`
  看 bench 输出格式是否稳定

## 对照源码时推荐看的顺序

1. `YierdisCli`
2. `InlineCommandParser`
3. `YierdisClient`
4. `CustomProtocolV1Replies`
5. `YierdisBenchArgs`
6. `YierdisBench`
7. `scripts/smoke.sh`
8. `scripts/bench.sh`

## 一句话总结

client 和 bench 看起来是外围工具，但它们其实是：

- 自定义协议的第一批消费者
- request-path 和运维脚本的真实验证者

如果你想知道“作者自己是怎么和这个 server 交互、验证和压测的”，这两个模块就是最直接的答案。
