# 代码库文档地图

本文是 Yierdis 的内部代码库文档地图。根部 `README.md` 负责项目定位、环境、构建和启动；这里负责帮助维护者按任务找到架构说明、源码入口、运行时边界和验证路径。

## 先选你的阅读路径

| 目标 | 建议路径 |
| --- | --- |
| 第一次了解项目 | [`project-overview.md`](./project-overview.md) -> [`request-execution-flow.md`](./request-execution-flow.md) -> [`module-architecture.md`](./module-architecture.md) |
| 跟一条请求读源码 | `request-execution-flow.md` -> `main-path-walkthrough.md` -> `core-logic-index.md` |
| 理解协议和命令 | `protocol-reference.md` -> `commands-and-data-model.md` -> `testing-and-debugging.md` |
| 理解 DB 和内存 | `db-internals.md` -> `ttl-and-expiration-lifecycle.md` -> `maxmemory-and-eviction.md` -> `native-memory-runtime.md` -> `native-allocator-and-handles.md` -> `offheap-copy-behavior.md` |
| 理解代理和变更事件 | `change-event-and-proxy-logic.md` -> `core-logic-index.md` -> `development-navigation.md` |
| 准备改代码 | `development-navigation.md` -> `testing-and-debugging.md` -> 对应专题文档 |

## 文档分层

- 入口导读: [`readme.md`](./readme.md), [`project-overview.md`](./project-overview.md)。负责建立入口地图、项目定位、能力边界和第一轮阅读顺序。
- 系统主线: [`request-execution-flow.md`](./request-execution-flow.md), [`main-path-walkthrough.md`](./main-path-walkthrough.md), [`module-architecture.md`](./module-architecture.md)。负责串起请求执行链、源码主路径和 Maven 模块边界。
- 专题手册: [`protocol-reference.md`](./protocol-reference.md), [`commands-and-data-model.md`](./commands-and-data-model.md), [`db-internals.md`](./db-internals.md), [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`change-event-and-proxy-logic.md`](./change-event-and-proxy-logic.md), [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`configuration-and-operations.md`](./configuration-and-operations.md), [`client-and-bench-internals.md`](./client-and-bench-internals.md), [`ffm-primer.md`](./ffm-primer.md), [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。负责按协议、命令、DB、TTL/maxmemory、执行器、代理/变更事件、bytes、配置、客户端和 native memory 等主题提供深入说明。
- 开发导航: [`development-navigation.md`](./development-navigation.md), [`testing-and-debugging.md`](./testing-and-debugging.md)。负责把常见改动类型、排障路径和验证范围连接起来。
- 参考资料: [`core-logic-index.md`](./core-logic-index.md), [`code-logic-coverage.md`](./code-logic-coverage.md), [`glossary.md`](./glossary.md)。负责集中索引核心类、核心方法、覆盖追踪和高频术语，方便读源码时快速定位。

## Production Hardening

部署、容量调参、事故排查或发布验收前，先读 [`production-hardening-operations.md`](./production-hardening-operations.md)。它统一说明 ingress、commit-stream、maxmemory 和有界回复的容量口径，result-unknown 关闭语义、graceful shutdown、soak 和四命令性能门槛。

## 核心命令链路基准

所有涉及命令执行的专题文档都应以当前唯一链路为准：

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

事务 queueable 命令在 parse 阶段做 preflight，`EXEC` replay 负责子 `PreparedCommand` 和 retained request 的所有权；语义流式 source 由结果持有到 renderer 消费完成，`QUIT` 通过 `CommandResult` 表达 reply 后关闭。`EngineSession` 只拥有连接 session 状态，`RedisReplyWriter` 只作为 renderer 的 RESP-facing 端口。

## 推荐第一轮阅读

1. [`project-overview.md`](./project-overview.md)
2. [`request-execution-flow.md`](./request-execution-flow.md)
3. [`main-path-walkthrough.md`](./main-path-walkthrough.md)
4. [`module-architecture.md`](./module-architecture.md)
5. [`protocol-reference.md`](./protocol-reference.md)
6. [`commands-and-data-model.md`](./commands-and-data-model.md)
7. [`db-internals.md`](./db-internals.md)
8. [`ttl-and-expiration-lifecycle.md`](./ttl-and-expiration-lifecycle.md)
9. [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md)
10. [`executor-and-backpressure.md`](./executor-and-backpressure.md)
11. [`testing-and-debugging.md`](./testing-and-debugging.md)
12. [`development-navigation.md`](./development-navigation.md)

## 维护者提示

- 新命令、新协议行为、新 DB API 或 native-memory 结构需要同时检查 [`development-navigation.md`](./development-navigation.md)、[`testing-and-debugging.md`](./testing-and-debugging.md) 和 [`code-logic-coverage.md`](./code-logic-coverage.md)；覆盖状态统一维护在覆盖矩阵里，不要再分散记在别处。
- native-memory 事实应在 [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`db-internals.md`](./db-internals.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 之间保持一致。
- 根部 `README.md` 应保持 quick-start 页面定位，不要扩张成内部实现手册。
