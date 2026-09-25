# C 版与 Java 版 Redis 客户端的设计差异

## 一、这篇文档回答什么

把 hiredis 式 C 客户端的功能迁移到 Java 客户端时，哪些功能、机制与纪律需要额外自建。本文只给背景性结论与选型取舍，具体实现见 [`client-and-bench-internals.md`](./client-and-bench-internals.md)，协议口径见 [`protocol-reference.md`](./protocol-reference.md)。对照的 C 侧以 hiredis / redis-cli 为代表，Java 侧以 Jedis / Lettuce / Redisson 为代表。

## 二、三件必须自建的机制

C 侧由 hiredis 的 reader 回调兜底，Java 侧要么自己写，要么整体交给 Netty / 框架：

```text
TCP 字节流 -> 增量解码器(跨半帧状态机) -> 完整 RespReply(校验后交付) -> 请求 FIFO 队列(队首 future 完成)
   |                                    |
   半帧留在 buffer                       超时 / 解析失败 -> 直接关连接（跳过迟到的 reply 会 desync）
```

1. **增量解码器**：一次 `read()` 不保证读满一帧，解析器必须是跨半帧的状态机。
2. **FIFO 配对**：多路复用 / pipeline 时，请求队列按序与 reply 匹配，future 按序完成。
3. **超时即关连接**：RESP reply 是 FIFO，超时后无法跳过迟到的 reply，保连接必然 desync。

## 三、实现方式

| | C（hiredis / redis-cli） | Java |
| --- | --- | --- |
| 模型 | 裸 socket + 手动读写缓冲区；同步 `redisCommand()`，异步 `redisAsyncCommand()` 挂 ae/epoll/kqueue 适配器 | 三条路线：Jedis（阻塞 socket + 连接池）、Lettuce（Netty NIO + 单连接多路复用）、Redisson（Netty + 分布式数据结构抽象） |
| 解析器 | `redisReader` 增量回调式，逐 token 产出 reply 对象 | 自己写增量状态机，或交给 Netty `ByteToMessageDecoder` |
| 同步/异步 | 两套独立 API | 同步、Future、Reactive 三种签名共存 |

Java 额外要做：决定 blocking vs NIO；决定单连接多路复用还是一连接一请求；重连时的协议重协商序列（`HELLO` → `AUTH` → `SELECT` → 订阅重建）。

## 四、API 设计

| | C | Java |
| --- | --- | --- |
| 命令构造 | `redisCommand(ctx, fmt, ...)` 字符串拼接，无类型 | 类型化接口（`StringCommands`/`HashCommands`/`KeyCommands`…），编译期检查 |
| 回包形态 | `redisReply*` 结构体树（`type/integer/len/str/element[]`），用户递归遍历并 `freeReplyObject()` | 自动映射 Java 类型（`String`/`Long`/`List<T>`/`Map<K,V>`…），返回值不可变 |
| 版本兼容 | 升级 server 后行为可能漂移，无编译期提示 | 命令库需按 Redis 6/7 演进，常为生成式维护 |

Java 额外要做：返回值必须与内部 buffer 脱钩（防御性复制或独立分配），否则下个请求会污染已交付结果；设计 RESP2/RESP3 的统一类型映射层。

## 五、内存管理

| | C | Java |
| --- | --- | --- |
| 回包生命周期 | 逐节点 malloc，忘记 `freeReplyObject()` 即泄漏 | GC 接管，但热路径分配变成 GC 压力与 P99 毛刺 |
| 缓冲 | `realloc` 增长，`sds` 管字符串 | Netty `ByteBuf` 池化 + 引用计数，直接内存发送（零拷贝到 socket） |
| 大 payload | 上限自己把控 | 必须显式限制（如 `maxBulkBytes`），否则异常 bulk 声明可直接打爆堆 |

Java 额外要做（与 C 差异最大的一块）：

- 防 OOM 前置校验：bulk string 头声明的长度先做上限检查再分配。
- 减少 per-request 分配：命令 frame 预编译、`ThreadLocal` 编码缓冲、数字转字节复用。
- 引用计数纪律：`ByteBuf` 手动 release，配 leak detection 排查。
- 直接内存限额：`-XX:MaxDirectMemorySize` 与池化配置对齐。
- 大 value 场景评估压缩与分块。

C 的代价是忘 free → 泄漏；Java 的代价是忘复用 → GC 尖刺，性质完全不同。堆外与 GC 的通用背景见 [`jvm-constraints-and-offheap-rationale.md`](./jvm-constraints-and-offheap-rationale.md)。

## 六、并发处理

| | C | Java |
| --- | --- | --- |
| 线程安全 | `redisContext` 非线程安全，每线程一 context；异步模式跨线程要装 ae 锁函数 | Jedis 用 `JedisPool` 借还（try-with-resources）；Lettuce 单连接共享 + 每条命令一个 `RedisFuture`，FIFO 自动配对、自动 pipeline |
| 并发模型 | 事件循环 + 回调，单线程跑完 | 事件循环 / 阻塞池 / JDK 21 虚拟线程三选一 |

Java 额外要做：pipeline 的 FIFO 配对；`future.cancel()` 不能回收队列 slot、只能作废 future；订阅连接与请求连接分离；RESP3 push 与普通 reply 同连接混合时的解复用；`MULTI`/`EXEC` 队列状态机的客户端侧管理；事件循环亲和（回调不做重活）。

## 七、数据序列化

C 只认字节，序列化是上层的事；Java 必须选型并治理：

| 方案 | 取舍 |
| --- | --- |
| UTF-8 String（默认） | 跨语言最安全；必须显式 UTF-8，禁止平台默认 charset |
| JDK 序列化 | 脆弱：serialVersionUID 漂移、安全风险、跨语言不可读，生产基本淘汰 |
| JSON（Jackson） | 可读性好、跨语言；体积大、性能一般 |
| Kryo / FST / Protostuff | 紧凑快速；需类注册与 schema 兼容治理 |

Java 额外要做：codec 抽象层（Redisson 的 `Codec` 组合模式）、schema 版本演进策略、大 value 压缩、RESP3 新类型映射（double/big number/map/set/push）、RESP3 客户端缓存的失效消息处理。

## 八、异常处理

| | C | Java |
| --- | --- | --- |
| 错误通道 | `NULL` 返回 + `errstr` + `errno`；`SIGPIPE` 必须忽略（`redisIgnoreSigPipe`） | 全 unchecked 分层：`RedisConnectionException` / `RedisCommandTimeoutException` / `RedisCommandExecutionException`（服务器 `-ERR`） |
| 异步错误 | 回调 err 参数 | 经 `CompletableFuture` 传播，不消费会静默丢失 |
| 超时 | 应用侧自管 | 超时 ≠ 失败（请求可能已执行，重试要求幂等）；超时后必须关连接防 desync |

Java 额外要做：连接级超时与命令级超时分层；重连退避与风暴抑制；Sentinel/Cluster failover（拓扑刷新、`MOVED`/`ASK` 重定向）；`close()` 幂等；`InterruptedException` 传播策略。

## 九、关键技术点清单

Java 版需要特别注意或额外实现的点：

1. 增量 RESP 解码器：半帧累积、嵌套深度限制（防栈溢出/恶意嵌套）、inline 命令。
2. 请求/回包 FIFO 配对与 pipeline future 队列。
3. 超时即关连接的 desync 纪律。
4. 写缓冲水位与背压。
5. 池化与分配控制（ByteBuf 引用计数、直接内存限额）。
6. 大 payload 前置上限检查。
7. 重连重协商序列：`HELLO` → `AUTH` → `SELECT` → 订阅重建，顺序不可乱。
8. Sentinel / Cluster 客户端逻辑：CRC16 slot 计算、`MOVED`/`ASK` 重定向、拓扑定期刷新。
9. RESP2/RESP3 类型统一映射 + push 解复用 + 客户端缓存失效处理。
10. 显式 UTF-8 与序列化 schema 治理。
11. 取消/中断语义与 future 泄漏排查。
12. 线程安全边界文档化：返回值不泄露内部 buffer。

## 十、与 Yierdis 的对应

- `YierdisClient`：阻塞一问一答 + 超时关连接（第 3 条的最小可用实现）。
- `RespClientCodec`：同步编解码、bulk 上限检查（第 1、6 条）。
- `NioBenchmarkClient` + `IncrementalRespReplyDecoder`：NIO 增量解码路径（第 1 条）。

三者均不见 pipeline future 队列（第 2 条）与重连重协商（第 7 条），属于当前有意省略的能力；细节见 [`client-and-bench-internals.md`](./client-and-bench-internals.md)。
