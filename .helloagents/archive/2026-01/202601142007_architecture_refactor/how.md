<!-- migrated_from: history/2026-01/202601142007_architecture_refactor/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: architecture_refactor

## Technical Solution

### Core Technologies

- Java 17
- Netty（server/client 网络与 pipeline）
- Maven 多模块（用于边界治理与依赖方向约束）
- （可选）picocli（统一 server/bench 参数解析与校验，提升可用性与一致性）

### Implementation Key Points

**Solution Package Location**

- `helloagents/history/2026-01/202601142007_architecture_refactor/`

**Key Principles**

1. **SSOT 优先**：RESP codec、参数模型、executor 行为定义必须有唯一可信实现
2. **依赖单向**：core/protocol 不依赖 netty；netty-adapter 层依赖 core/protocol
3. **渐进迁移**：先引入新模块与适配层，再迁移实现，最后移除旧路径
4. **可验证**：每个阶段都要有对应的单测/回归测试支撑，避免“拆完更不稳定”

## Architecture Design

### Overall Architecture

建议目标架构（模块级）：

- `yierdis-protocol`：RESP 对象模型 + 编解码（SSOT）
- `yierdis-core`：DB、数据结构、命令处理、淘汰/TTL、off-heap 集成（不依赖 Netty）
- `yierdis-server`：Netty server bootstrap + pipeline + executor 绑定（依赖 core + protocol）
- `yierdis-client`：（可选）客户端库，供 CLI/bench 复用（依赖 protocol，可选 netty）
- `yierdis-bench`：压测与基准（优先复用 protocol/client；保留 raw socket 模式作为可选实现）
- `yierdis-offheap-*`：off-heap API 与各后端实现（API 去 Netty 依赖；后端与适配解耦）

### Core Flow

目标调用链（简化）：

1. Netty pipeline 解码 RESP -> `RespCommand`
2. executor 接收任务 -> 调用 core 命令处理入口
3. core 操作 DB/Keyspace/Value/TTL/淘汰/off-heap
4. 生成回复对象或写出事件 -> 使用统一 RESP codec 编码 -> 返回客户端

## Architecture Decision ADR

### ADR-001: 抽取 yierdis-protocol 作为 RESP SSOT

- Decision: 将 `Resp*` 对象与 codec 抽离为独立模块，server/client/bench/测试统一复用
- Rationale: 避免多套 RESP 实现偏差，降低 RESP3 子集扩展成本
- Consequences: 需要调整依赖结构与包迁移；短期改动面较大

### ADR-002: core 层不依赖 Netty，Netty 仅作为适配层

- Decision: DB/命令处理/淘汰/过期等核心逻辑迁移到 `yierdis-core`
- Rationale: 让核心逻辑可被非 Netty 场景复用（例如 bench/嵌入式测试），并减少耦合

### ADR-003: bench 复用统一 codec，并保留 raw socket 作为传输选项

- Decision: bench 的编码/解码/语义校验复用 `yierdis-protocol`；网络传输可保留 JDK Socket
- Rationale: bench 关注吞吐/延迟，同时需要语义一致；两者通过“共享 codec + 可选传输层”兼顾

### ADR-004: 引入统一参数解析库（picocli）并把参数模型化

- Decision: 使用 picocli 或同级别轻量库统一 args 解析（server 与 bench 共用核心参数模型）
- Rationale: 减少手写解析带来的漂移与边界错误，提升 help/校验一致性

### ADR-005: executor 实现收敛为单一 canonical 实现

- Decision: 以当前 Netty 场景的 `NettyCommandExecutor` 为主线，定义稳定行为；其它 executor 要么移除要么仅做兼容封装
- Rationale: 降低行为分叉，避免同一命令在不同执行路径表现不一致

## API Design

### CLI/Args（示例方向）

- 将 `ServerConfig` 与 bench 的 config 统一为“可复用参数模型”
- 保留现有 flag 名称作为兼容别名（避免脚本与文档断裂）
- 对关键参数增加约束校验（例如 watermark 必须满足 low < high、maxmemoryBytes ≥ 0 等）

## Data Model

- 不改变现有数据模型目标；本次聚焦“边界/一致性/可维护性”
- 与数据结构相关的优化（如 listpack/skiplist/hll 编码）可作为后续独立提案

## Security and Performance

- Security：禁止引入任何生产环境连接/敏感信息明文存储；off-heap/unsafe 操作需保持明确边界与 OOM/泄漏测试
- Performance：协议与 off-heap 抽象要避免在热路径引入额外拷贝；必要时保留 fast-path（但必须由测试验证等价语义）

## Testing and Deployment

- 单测：补齐 codec round-trip、executor 语义、参数校验、bench 严格回复检查等测试
- 集成：保留现有 `scripts/bench.sh`，并在实现阶段将参数与输出说明同步到 `helloagents/wiki/bench.md`
- 交付验收：`mvn test` 通过 + bench 在相同参数下可重复跑通（吞吐/延迟不作强保证，但语义必须一致）
- 环境基线（2026-01-14）：Java 17.0.17（OpenJDK, Ubuntu），Maven 3.8.7，Linux 6.6.87.2（WSL2）
- 构建基线（2026-01-14）：`mvn test` ✅ BUILD SUCCESS，real=8.01s（user=21.54s, sys=3.88s）

### 最小 Smoke（链路连通）

- 执行：`./scripts/smoke.sh`
- 验证点：server 启动/关闭 + CLI `PING` + bench `--strictReplies`（connect-only）
- 最近一次记录：2026-01-15 ✅ 通过（bench errors=0）

### 阶段性回归记录（mvn -pl ... -am test）

- 2026-01-15：
  - `mvn -pl yierdis-protocol -am test` ✅ BUILD SUCCESS（Total time: 2.428 s，Finished at: 2026-01-15T00:19:46+08:00）
  - `mvn -pl yierdis-core -am test` ✅ BUILD SUCCESS（Total time: 4.243 s，Finished at: 2026-01-15T00:19:57+08:00）
  - `mvn -pl yierdis-server -am test` ✅ BUILD SUCCESS（Total time: 12.377 s，Finished at: 2026-01-15T00:32:26+08:00）
  - `mvn -pl yierdis-args -am test` ✅ BUILD SUCCESS（Total time: 1.177 s，Finished at: 2026-01-15T00:20:37+08:00）
  - `mvn -pl yierdis-bench -am test` ✅ BUILD SUCCESS（Total time: 2.898 s，Finished at: 2026-01-15T00:20:51+08:00）
