# 项目上下文

## 1. 基本信息

```yaml
名称: Yierdis
描述: 教学/演示导向的内存 KV 服务端（Redis 风格命令语义子集），对外仅承诺 Custom Protocol v1。
类型: 服务端（含 CLI/bench 工具）
状态: 开发中（稳定迭代）
```

## 2. 技术上下文

```yaml
语言: Java 17
框架: Netty 4.x（server/client）
包管理器: Maven
构建工具: Maven（多模块）
测试框架: JUnit 4
日志: SLF4J + Logback
协议: Custom Protocol v1（`<len>:<json>\n` request + NDJSON reply）
```

### 主要依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| Netty | 4.1.109.Final | 网络 I/O、server/client 基础设施 |
| SLF4J | 2.0.17 | 日志 API |
| Logback | 1.5.18 | 日志实现 |
| JUnit | 4.13.2 | 单元测试 |
| picocli | 4.7.6 | 参数解析（yierdis-args） |

## 3. 项目概述

### 核心功能
- 内存数据结构与 Redis 风格命令子集（String/List/Hash/Set/ZSet 等）
- TTL（惰性删除 + 可选后台清理）
- maxmemory（教学简化版，支持 `global/per-db` 口径）
- 最小事务子集：`MULTI/EXEC/DISCARD`（含队列上限保护）
- 可观测性：`INFO` / `STATS` / `MEMORY STATS`
- 对外协议：Custom Protocol v1 over TCP

### 项目边界
```yaml
范围内:
  - 内存 KV 数据结构与命令语义（教学/演示）
  - 自定义协议与可恢复错误模型
  - 背压、淘汰、过期清理等可观测与治理思路
范围外:
  - AOF/RDB 持久化
  - 复制、集群
  - Lua、ACL、TLS
```

## 4. 开发约定

### 代码规范（摘自 `project.md`，以代码行为为准）
```yaml
包名前缀: yier.bubu.redis
服务端类命名: Yierdis*
缩进: 4 空格（无 Tab）
协议实现边界: 对外仅 Custom Protocol v1；`HELLO` 为信息型命令，不做协议协商
```

### 错误处理
```yaml
命令错误: Redis 风格文本（ERR/WRONGTYPE/EXECABORT/OOM 等）
协议错误: 返回 ok=false(kind=protocol) 并尽量 resync（可恢复）
```

### 测试要求
```yaml
推荐命令: mvn test
要求: 尽量可重复；涉及 TTL 的断言允许小幅时间误差
```

### Git 规范
```yaml
分支策略: （未在知识库中强约束）
提交格式: （未在知识库中强约束）
```

## 5. 当前约束（来自历史决策/约定）

| 约束 | 原因 | 参考 |
|------|------|------|
| 对外仅承诺 Custom Protocol v1 | 教学/可控，避免 Redis wire 兼容成本 | `archive/2026-02/202602061601_custom_protocol_v1/` |
| SSOT 模块尽量 Netty-free（core/protocol/offheap-api 等） | 复用性与依赖边界清晰 | `archive/2026-01/202601142007_architecture_refactor/` |
| 单线程命令语义（DB owner-thread 绑定 + fail-fast） | 降低竞态与语义漂移 | `archive/2026-01/202601161551_arch_guardrails/` |

## 6. 已知技术债务（可选）

> 暂未在知识库中维护统一清单；如需补齐，可在后续迭代中沉淀到此处。
