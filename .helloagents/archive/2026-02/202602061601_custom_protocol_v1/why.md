<!-- migrated_from: history/2026-02/202602061601_custom_protocol_v1/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Custom Protocol v1（完全替换 RESP）

## Requirement Background

当前协议栈以 RESP2/RESP3 兼容为中心，request/reply 两侧存在“严格 + 最小子集”的边界策略：request 侧强约束命令形态，遇到不支持的聚合类型会按 protocol error 处理并断连。这种严格错误模型与部分客户端（或工具链）的“宽容解析”预期不一致，且随着 RESP3 类型覆盖扩展，边界解释、测试与文档同步的维护成本持续上升。

本改造的目标是不再依赖 Redis/RESP 生态，而是使用**全新的自定义协议**来承载现有功能，统一 request/reply 的表达方式与错误模型：协议解析出错时返回错误并尽量继续读取下一帧，降低“单个坏帧导致连接不可用”的风险与排障成本。

## Change Content

1. 定义 Custom Protocol v1 的 wire format：**length-prefixed request** + **JSON line reply**（每个 reply 一行 JSON）。
2. server/client/bench 全面切换到新协议，端口保持 `6378`（不再兼容 redis-cli/RESP 工具链）。
3. 引入协议无关的命令/回复抽象（替代 `RespCommand/RespWriter` 作为 core 的 API），并逐步清理 RESP 相关实现与文档。
4. 增加协议错误的“可恢复”实现与覆盖测试：非法/不支持帧返回 error，但连接尽量保持可用并继续读下一帧。

## Impact Scope

- **Modules:**
  - `yierdis-protocol`（新增协议无关抽象 + JSON codec；RESP 实现进入“legacy”）
  - `yierdis-protocol-netty`（新增自定义协议 Netty codec；替换 server/client pipeline）
  - `yierdis-core`（命令处理 API 替换为协议无关抽象；保持现有功能）
  - `yierdis-server`（切换 decoder/handler 错误模型；协议错误不再默认断连）
  - `yierdis-client`（切换 request 编码与 reply 解析/打印）
  - `yierdis-bench`（切换压测写入/读取逻辑）
  - `README.md` + `helloagents/wiki/*`（协议说明、踩坑与 Roadmap 全面更新）
- **Files:** 跨模块多文件改动（详见 task.md）
- **APIs:** wire protocol 发生不兼容变更（对外）
- **Data:** 内存数据结构保持不变；输入输出从 RESP bytes 改为 UTF-8 文本（协议层约束）

## Core Scenarios

### Requirement: 自定义协议切帧与错误可恢复（Resync）
**Module:** protocol-netty / server
以自定义协议承载 request，保证在存在协议错误时尽量返回 error 并继续读取后续帧。

#### Scenario: 正常 request/reply（单条）
客户端发送一条合法 request（length-prefix + JSON payload）。
- 服务端成功解析并执行命令
- 返回一行 JSON reply（无换行注入，稳定 one-line）

#### Scenario: pipelining
客户端连续发送 N 条 request（不等待 reply）。
- 服务端按 FIFO 处理并连续返回 N 行 JSON reply
- client/bench 可正确配对与统计

#### Scenario: 非法帧/不支持帧
客户端发送非法 header、非法 length、非法 JSON 或语义不支持的 request。
- 服务端返回 `ok=false` 的错误 reply
- 服务端尽量跳过该帧并继续读取下一帧（在安全上限允许范围内）

### Requirement: 现有功能在新协议下可用（命令语义保持）
**Module:** core
保持现有命令集与执行语义（实现逻辑不因协议替换而改变）。

#### Scenario: 常用命令
- `PING`/`ECHO`
- `SET`/`GET`/`DEL`/`EXPIRE`/`TTL`
- `HSET`/`HGET`/`HGETALL`
- `LPUSH`/`LRANGE`/`LPOP`/`RPOP`
- `SADD`/`SMEMBERS`
- `ZADD`/`ZRANGE`
- `MULTI/EXEC/DISCARD`
- `SELECT`/`QUIT`/`INFO`/`STATS`

### Requirement: client/CLI/bench 同步迁移
**Module:** client / bench
切换到新协议后仍具备可用的调试与基准能力。

#### Scenario: CLI 单次执行与 REPL
- CLI 发送命令与参数（UTF-8）
- 打印服务端返回的 JSON reply（稳定可读）

#### Scenario: Bench 读写回归
- 通过 bench 执行 PING/GET/SET workloads
- strict reply 校验逻辑更新为 JSON schema

## Risk Assessment

- **Risk: 生态不兼容（破坏性变更）**：端口 `6378` 不再兼容 RESP，redis-cli/Jedis/Lettuce 等不可用。
  - **Mitigation:** 明确在 README/overview 标注；必要时提供“并存端口”作为后续扩展（非本次必须）。
- **Risk: JSON 性能与大回复体积**：相较 RESP，JSON 体积更大、编码开销更高。
  - **Mitigation:** 采用最小 JSON writer（不 pretty-print），复用缓冲区；限制最大 reply 行长度与字段数量。
- **Risk: 可恢复解析的 resync 难度**：length/header 错误可能导致解码器失去同步。
  - **Mitigation:** 设计明确的 framing + 严格上限；实现“丢弃到下一帧边界”的状态机；超出上限时允许降级为断连（安全兜底）。
- **Risk: 安全与 DoS**：恶意 length、超长 JSON、控制字符注入。
  - **Mitigation:** 统一限制（maxHeaderBytes/maxPayloadBytes/maxDepth）；reply 侧对 `\r/\n` 等控制字符进行 JSON 级转义；错误信息限长。
