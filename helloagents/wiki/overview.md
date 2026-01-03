# Yierdis

> 项目级核心信息入口；更细的模块说明位于 `modules/`。

---

## 1. 项目概览

### 目标与背景

Yierdis 是一个“教学/演示导向”的简化版 Redis 兼容服务端：使用 Java 17 + Netty，实现 RESP2 over TCP 的核心命令子集，便于学习网络编程与 Redis 协议/数据结构思路。

### 范围

- **包含（In scope）**：内存数据结构、基础命令集、TTL（惰性删除 + 可选后台清理）、maxmemory（教学简化版）
- **不包含（Out of scope）**：AOF/RDB 持久化、复制、集群、事务、Lua、ACL、TLS 等

---

## 2. 模块索引

| 模块 | 责任 | 状态 | 文档 |
|------|------|------|------|
| server | Netty 启动/管线/Handler | ✅Stable | [modules/server.md](modules/server.md) |
| protocol | RESP2 编解码与对象模型 | ✅Stable | [modules/protocol.md](modules/protocol.md) |
| command | 命令路由与参数解析 | ✅Stable | [modules/command.md](modules/command.md) |
| db | 内存存储、编码、TTL、淘汰 | ✅Stable | [modules/db.md](modules/db.md) |
| offheap | 堆外内存抽象与后端 | 🚧In Development | [modules/offheap.md](modules/offheap.md) |
| client | 内置 RESP2 CLI 客户端 | ✅Stable | [modules/client.md](modules/client.md) |

---

## 3. 快速链接

- [技术约定（SSOT）](../project.md)
- [架构设计](arch.md)
- [命令/API 手册](api.md)
- [数据模型](data.md)
- [变更历史](../history/index.md)

