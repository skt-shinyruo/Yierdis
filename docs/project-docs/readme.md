# 代码库文档地图

Yierdis 的内部代码库文档地图如下。根部 `README.md` 覆盖项目定位、环境、构建和启动；本目录的文档面向要读这批源码的工程师，按主题给出源码级说明。

## 按目标选阅读路径

三条主线，顺序即依赖顺序，后面的文档假定你已读前面的。

**第一轮通读（约 5 篇，建立全局认知）**

1. [`readme.md`](./readme.md)（本文）— 知道有哪些专题、怎么排队。
2. [`project-overview.md`](./project-overview.md) — 定位、能力边界、模块清单、12 个入口文件、最短启动路径。
3. [`module-architecture.md`](./module-architecture.md) — 九个 Maven 模块的真实依赖方向（以各 leaf `pom.xml` 为准）。
4. [`request-execution-flow.md`](./request-execution-flow.md) — 一次请求从 socket 到 reply 的完整时序。
5. [`netty-adapter-design.md`](./netty-adapter-design.md) — Netty 只出现在哪几个模块、pipeline 如何装配、背压如何在 Netty 侧体现。

**想改某类功能时（先读改造对象，再读验证范围）**

1. 先读本文下面的「文档分层」定位条目。
2. 读 [`development-navigation.md`](./development-navigation.md)：把改动类型映射到类和验证范围。
3. 读 [`testing-and-debugging.md`](./testing-and-debugging.md)：确认跑哪些测试、怎么排障。
4. 改协议/命令/DB 前，先读对应的专题手册，避免只改表达层。

**想追一条请求（PING/SET/QUIT/EXEC）**

1. [`request-execution-flow.md`](./request-execution-flow.md)：主链、最短路径、错误路径、线程切换点。
2. [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md)：`CommandDispatcher` 的分支顺序。
3. [`transaction-and-replay.md`](./transaction-and-replay.md)：`MULTI`/`EXEC` 的排队与 replay 状态机。
4. [`executor-and-backpressure.md`](./executor-and-backpressure.md)：提交预算与背压的精确口径。

## 文档分层

- 入口导读: [`readme.md`](./readme.md), [`project-overview.md`](./project-overview.md)。记录项目定位、能力边界、模块入口和 12 个最先打开的源文件。
- 系统主线: [`request-execution-flow.md`](./request-execution-flow.md), [`module-architecture.md`](./module-architecture.md), [`netty-adapter-design.md`](./netty-adapter-design.md)。串起请求执行链、Maven 模块边界和 Netty transport 适配。
- 专题手册: [`protocol-reference.md`](./protocol-reference.md), [`commands-and-data-model.md`](./commands-and-data-model.md), [`command-parsing-and-dispatch.md`](./command-parsing-and-dispatch.md), [`transaction-and-replay.md`](./transaction-and-replay.md), [`db-internals.md`](./db-internals.md), [`db-design-analysis.md`](./db-design-analysis.md), [`db-behavior-gaps.md`](./db-behavior-gaps.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`proxy-logic.md`](./proxy-logic.md), [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`configuration-and-operations.md`](./configuration-and-operations.md), [`client-and-bench-internals.md`](./client-and-bench-internals.md), [`ffm-primer.md`](./ffm-primer.md), [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。按协议、命令、DB、TTL/maxmemory、执行器、代理、bytes、配置、客户端和 native memory 等主题提供深入说明。
- 开发导航: [`development-navigation.md`](./development-navigation.md), [`testing-and-debugging.md`](./testing-and-debugging.md)。把常见改动类型、排障路径和验证范围连在一起。
- 参考资料: [`glossary.md`](./glossary.md)。集中解释高频术语；源码入口和测试范围分别维护在开发导航与测试手册中。

## Production Hardening

[`production-hardening-operations.md`](./production-hardening-operations.md) 说明 ingress、maxmemory 与有界回复的容量口径，以及 result-unknown 关闭语义、graceful shutdown 和 soak。benchmark 对照使用 canonical title 和前八个共享 CSV 字段；通过与否由外部策略决定，文档本身不设性能门槛。

## 核心命令链路基准

所有涉及命令执行的专题文档都应以当前唯一链路为准：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> Function<CommandSession, PreparedCommand>.apply(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(session)
  -> CommandResult -> RedisReplyRenderer
```

事务 queueable 命令在 parse 阶段做 preflight，`EXEC` replay 接管子 `PreparedCommand` 和 retained request 的所有权。语义流式 source 由对应 `PreparedCommand` 持有，renderer 同步消费结果后再由 executor 关闭；`QUIT` 通过 `CommandResult` 表达 reply 后关闭。`EngineSession` 只拥有连接 session 状态；普通 command handler 不直接使用 `RedisReplyWriter`，语义结果由 renderer 写出，executor/ingress 控制路径可以直接写协议错误与终止回复。

## 文档维护约定

- 文档里的类名、方法名、常量、配置项和文件路径必须能在仓库源码中直接找到；改了源码要同步改文档，宁缺毋滥。
- 相对链接保持 `[文字](./xxx.md)` 的文件名不变，也没有锚点链接，所以标题可以重构。
- 代码块可以扩展（补真实签名、真实调用顺序），但不能与源码不符；链路的唯一基准是上面那段主链。
- 描述“为什么”时给出源码依据或反面对比，不要只下结论。