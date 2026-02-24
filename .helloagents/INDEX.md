# Yierdis 知识库

> 本文件是知识库的入口点。

## 快速导航

| 需要了解 | 读取文件 |
|---------|---------|
| 项目概况、技术栈、开发约定 | [context.md](context.md) |
| 模块索引 | [modules/_index.md](modules/_index.md) |
| 某个模块的职责和接口 | [modules/{模块名}.md](modules/{模块名}.md) |
| 项目变更历史 | [CHANGELOG.md](CHANGELOG.md) |
| 历史方案索引 | [archive/_index.md](archive/_index.md) |
| 当前待执行的方案 | [plan/](plan/) |
| 历史会话记录 | [sessions/](sessions/) |

## 模块关键词索引

> AI 读取此表即可判断哪些模块与当前需求相关，按需深读。

| 模块 | 关键词 | 摘要 |
|------|--------|------|
| server | netty, pipeline, executor, backpressure, stats | 负责服务端启动、pipeline 组装与连接生命周期（含背压/公平调度/可观测）。 |
| protocol | protocol, ndjson, json, reply, codec, model | 定义 Command/ReplyWriter/Session 等协议无关端口与 v1 编解码（NDJSON）。 |
| protocol-netty | netty, decoder, framing, resync | 提供 Custom Protocol v1 的 Netty 侧解码/适配层，负责 framing 与可恢复 resync。 |
| command | command, registry, router, multi, execabort, wrongtype | 命令语义与路由 SSOT：解析 argv、调用 DbEngine、写出结构化回复。 |
| db | keyspace, ttl, maxmemory, eviction, ledger, scan | 实现数据结构与内存/过期/淘汰语义，并冻结关键契约（KeyHandle/MemoryLedger）。 |
| offheap | offheap, allocator, unsafe, foreign, direct | 提供 off-heap API 与后端实现，演示不同内存路径与可观测口径。 |
| client | client, cli, custom protocol | 提供调试用 CLI/Netty client，用于回归与排障（Custom Protocol v1）。 |

## 知识库状态

```yaml
kb_version: 2.2.9
最后更新: 2026-02-24 23:14
模块数量: 7
待执行方案: 0
```

## 读取指引

```yaml
启动任务:
  1. 读取本文件获取导航
  2. 读取 context.md 获取项目上下文
  3. 检查 plan/ 是否有进行中方案包

任务相关:
  - 涉及特定模块: 读取 modules/{模块名}.md
  - 需要历史决策: 搜索 archive/_index.md → 读取对应 archive/{YYYY-MM}/{方案包}/proposal.md
  - 继续之前任务: 读取 plan/{方案包}/*
```

## 兼容目录（Legacy）

- `wiki/` 与 `history/` 为旧结构目录：本次升级默认保留，不作为新结构的入口。
- 新结构以 `modules/` 与 `archive/` 为准；后续如需清理旧目录，可基于备份再做删除。
