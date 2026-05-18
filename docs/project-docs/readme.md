# Codebase Guide

本文是 Yierdis 的代码库文档地图。它不替代仓库根部的 `README.md`，而是帮助你根据目标选择阅读路径：先理解项目、跟一次请求、查某个专题、准备改代码，或定位核心类。

## 先选你的阅读路径

| 目标 | 建议路径 |
| --- | --- |
| 第一次了解项目 | `project-introduction.md` -> `project-overview.md` -> `request-execution-flow.md` |
| 跟一条请求读源码 | `request-execution-flow.md` -> `main-path-walkthrough.md` -> `core-logic-index.md` |
| 理解协议和命令 | `protocol-reference.md` -> `commands-and-data-model.md` -> `operation-test-coverage-matrix.md` |
| 理解 DB 和内存 | `db-internals.md` -> `native-memory-runtime.md` -> `native-allocator-and-handles.md` |
| 准备改代码 | `development-navigation.md` -> `testing-and-debugging.md` -> 对应专题文档 |

## 文档分层

- 入口导读: [`readme.md`](./readme.md), [`project-introduction.md`](./project-introduction.md), [`project-overview.md`](./project-overview.md)。负责建立入口地图、项目定位、能力边界和第一轮阅读顺序。
- 系统主线: [`request-execution-flow.md`](./request-execution-flow.md), [`main-path-walkthrough.md`](./main-path-walkthrough.md), [`module-architecture.md`](./module-architecture.md)。负责串起请求执行链、源码主路径和 Maven 模块边界。
- 专题手册: [`protocol-reference.md`](./protocol-reference.md), [`commands-and-data-model.md`](./commands-and-data-model.md), [`db-internals.md`](./db-internals.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md), [`configuration-and-operations.md`](./configuration-and-operations.md), [`client-and-bench-internals.md`](./client-and-bench-internals.md), [`ffm-usage.md`](./ffm-usage.md), [`ffm-primer.md`](./ffm-primer.md), [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。负责按协议、命令、DB、执行器、bytes、配置、客户端和 native memory 等主题提供深入说明。
- 开发导航: [`development-navigation.md`](./development-navigation.md), [`testing-and-debugging.md`](./testing-and-debugging.md), [`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md)。负责把常见改动类型、排障路径和命令覆盖矩阵连接到具体测试范围。
- 参考资料: [`core-logic-index.md`](./core-logic-index.md), [`glossary.md`](./glossary.md)。负责集中索引核心类、核心方法和高频术语，方便读源码时快速定位。

## 推荐第一轮阅读

1. [`project-introduction.md`](./project-introduction.md)
2. [`project-overview.md`](./project-overview.md)
3. [`request-execution-flow.md`](./request-execution-flow.md)
4. [`main-path-walkthrough.md`](./main-path-walkthrough.md)
5. [`protocol-reference.md`](./protocol-reference.md)
6. [`commands-and-data-model.md`](./commands-and-data-model.md)
7. [`db-internals.md`](./db-internals.md)
8. [`executor-and-backpressure.md`](./executor-and-backpressure.md)
9. [`testing-and-debugging.md`](./testing-and-debugging.md)
10. [`development-navigation.md`](./development-navigation.md)

## 维护者提示

- [`operation-test-coverage-matrix.md`](./operation-test-coverage-matrix.md) 会被测试解析，改格式前需要同步更新对应测试。
- native-memory 事实应在 [`native-memory-runtime.md`](./native-memory-runtime.md), [`native-allocator-and-handles.md`](./native-allocator-and-handles.md), [`db-internals.md`](./db-internals.md) 之间保持一致。
- 根部 `README.md` 应保持 quick-start 页面定位，不要扩张成内部实现手册。
