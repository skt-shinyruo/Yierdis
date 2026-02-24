# 模块索引

> 通过此文件快速定位模块文档。

## 模块清单

| 模块 | 职责 | 状态 | 文档 |
|------|------|------|------|
| server | Netty 启动/管线/Handler（含单线程执行器装配与背压治理） | ✅Stable | [server.md](./server.md) |
| protocol | 协议无关抽象（model）+ JSON/Custom Protocol v1 codec（codec）+ 聚合层 | ✅Stable | [protocol.md](./protocol.md) |
| protocol-netty | Netty codec/adapters（Custom Protocol v1 decoder/line decoder + 连接态适配） | ✅Stable | [protocol-netty.md](./protocol-netty.md) |
| command | 命令路由与参数校验（SSOT），错误映射与 reply 形状写出 | ✅Stable | [command.md](./command.md) |
| db | 内存存储、编码、TTL、maxmemory（ledger）、淘汰策略 | ✅Stable | [db.md](./db.md) |
| offheap | 堆外内存抽象与多后端（netty/unsafe/foreign） | 🚧In Development | [offheap.md](./offheap.md) |
| client | 内置 Custom Protocol v1 CLI 客户端（调试工具） | ✅Stable | [client.md](./client.md) |

## 其他文档

- [overview.md](./overview.md) - 项目概览与边界（旧 wiki 迁移）
- [arch.md](./arch.md) - 架构设计与调用链（旧 wiki 迁移）
- [api.md](./api.md) - 命令/API 手册（旧 wiki 迁移）
- [data.md](./data.md) - 数据模型与内部结构（旧 wiki 迁移）
- [bench.md](./bench.md) - 压测与基准测试（旧 wiki 迁移）

## 模块依赖关系（简图）

```
client/bench/server → protocol-netty → protocol
server → core(runtime/db/command) → protocol(model)
db/core → offheap-api (optional)
```

## 状态说明
- ✅ Stable：稳定
- 🚧 In Development：开发中
