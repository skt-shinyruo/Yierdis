<!-- migrated_from: history/2026-01/202601152357_arch_hardening/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: 架构加固（Shutdown / Backlog Bytes / Split-package / Off-heap Capabilities / Version SSOT）

## Technical Solution

### Core Technologies
- Java 17
- Maven multi-module
- Netty 4.1.x
- picocli（参数 SSOT：`yierdis-args`）
- JUnit 4（回归测试）

### Implementation Key Points

1. **Graceful Shutdown：形成可等待的关停契约**
   - 为 `NettyCommandExecutor` 增加“可等待关停”能力（例如 `shutdownGracefully()` 返回 `CompletionStage<Void>` 或 Netty `Future<?>`）。
   - 关停时序建议：
     1) 停止接入（关闭 serverChannel/停止 accept）
     2) 停止读入（按连接 `autoRead=false` 或在 pipeline 层拒绝新命令）
     3) drain 执行器（回收队列残留命令 frame/ByteBuf）
     4) 在执行器线程内执行 `db.shutdown()`（保证 owner thread 语义）
     5) `commandGroup.shutdownGracefully().syncUninterruptibly()`
     6) `bossGroup/workerGroup.shutdownGracefully().syncUninterruptibly()`
   - 关键点：**DB 关闭必须发生在执行器线程**（或严格确保执行器已完全停止并不会再访问 DB）。

2. **Backlog Bytes Budget：将“条数限制”升级为“条数 + bytes 双约束”**
   - 新增全局 backlog bytes 上限（例如 `--executorQueueMaxBytes`），作为更可靠的第一预算。
   - 新增连接级 bytes 反压阈值（例如 `--backpressureBytesHigh/Low`），与 existing pending-count hysteresis 并存。
   - 为实现 bytes 预算，需要能够估算“一个命令在队列中保留的 bytes（retained bytes）”：
     - 推荐做法：在 `RespCommand` 中增加 `retainedBytes` 字段，由 decoder 在构造命令时写入。
     - Netty decoder（retained slice）可准确使用 `endIdx - startIdx`；inline command 可使用 `decodedBytesLength`。
   - 在 executor 入队前进行预算判断：
     - 超过 bytes 阈值 → disable autoRead + fail-fast（返回 `-ERR busy` 或更明确的错误）
     - 入队成功 → 增加 per-channel 与 global 的 bytes 计数
     - 命令执行完成 → 在 finally 中减少计数并按 low watermark 恢复 autoRead

3. **Decoder Limits 可配置化（与 bytes 预算配合）**
   - 将 RESP decoder 的上限从“硬编码默认值”提升为可配置（参数 SSOT 在 `yierdis-args`）：
     - `--protocolMaxBulkBytes`
     - `--protocolMaxArgs`
     - `--protocolMaxLineBytes`
   - `yierdis-server` 根据 `ServerConfig` 传参构造 decoder；为此需要将 `RespCommandDecoder/RespDecoder` 的参数化构造器变为 public（或提供 factory）。

4. **Split-package 消除：协议 SSOT 与 Netty adapter 用包名形成硬边界**
   - 将 `yierdis-protocol-netty` 内的 Netty 类迁移到独立 package：
     - `yier.bubu.redis.protocol.RespCommandDecoder` → `yier.bubu.redis.protocol.netty.RespCommandDecoder`
     - `RespDecoder/RespEncoder/NettyRespFrame/NettyRespSession` 同步迁移
   - 同步更新：
     - `yierdis-server` pipeline 引用
     - `yierdis-client`/`yierdis-bench` 引用
     - 全部测试与文档
   - 迁移后，`yierdis-protocol` 将独占 `yier.bubu.redis.protocol` 包，消除 split-package 风险。

5. **off-heap capabilities/SPI：统一“可插拔”语义**
   - 在 `yierdis-offheap-api` 定义最小 capabilities 接口集合（示例）：
     - `OffHeapStringSupport`：是否支持 string payload off-heap + direct write
     - `OffHeapDbIndexSupport`：是否支持 keyspace/expires off-heap（目前仅 unsafe）
   - `YierdisOffHeapAllocator` 可提供 `capabilities()` 或 `dbSupport()` 的可选返回值。
   - `yierdis-core`：
     - 构造阶段依据 capabilities 选择 keyspace/expires 的实现（避免 `instanceof`）
     - unsafe 专用结构（keyspace/expires）建议移动到 `yierdis-offheap-unsafe` 模块内，由能力实现提供 factory，减少 core 与 unsafe 细节耦合。

6. **Version SSOT：版本来源统一为构建元数据**
   - 通过 Maven resource filtering 生成资源（例如 `yierdis-version.properties`），在运行时读取版本号。
   - `HELLO` 返回的 version 字段从资源读取（并在需要时保留 `-SNAPSHOT`）。
   - 版本输出位置统一：`HELLO` + server startup log + 可选的诊断命令（视需求）。

## Architecture Design

关键边界目标：SSOT 模块（`core/protocol/args/offheap-api`）不依赖 Netty；Netty 相关能力集中在 `protocol-netty` 与 `server` 适配层；off-heap 的 DB 集成点通过 capabilities 明确表达。

```mermaid
flowchart LR
  subgraph SSOT[SSOT]
    Core[yierdis-core]
    Protocol[yierdis-protocol]
    Args[yierdis-args]
    OffheapApi[yierdis-offheap-api]
  end

  subgraph Adapters[Adapters]
    ProtocolNetty[yierdis-protocol-netty\n(package: ...protocol.netty)]
    OffheapNetty[yierdis-offheap-netty]
    OffheapUnsafe[yierdis-offheap-unsafe]
    OffheapForeign[yierdis-offheap-foreign]
  end

  subgraph Apps[Apps]
    Server[yierdis-server]
    Client[yierdis-client]
    Bench[yierdis-bench]
  end

  Server --> Args
  Server --> Core
  Server --> ProtocolNetty

  Client --> ProtocolNetty
  Bench --> ProtocolNetty
  Bench --> Args

  ProtocolNetty --> Protocol
  Core --> Protocol
  Core --> OffheapApi
  Protocol --> OffheapApi

  OffheapNetty --> OffheapApi
  OffheapUnsafe --> OffheapApi
  OffheapForeign --> OffheapApi
```

## Architecture Decision ADR

### ADR-20260115-03: 引入 backlog bytes 预算（以 retained bytes 为 SSOT）
**Context:** 仅以“命令条数”做预算时，大 bulk 积压会导致 ByteBuf 驻留不可解释，且不受 `maxmemory` 控制。  
**Decision:** 在协议命令结构中显式记录 retained bytes，并在执行器中按 bytes 做全局/连接级预算与反压。  
**Rationale:** retained bytes 可由 decoder 精确给出；bytes 预算比条数更贴近真实内存风险；可与现有条数阈值并存形成双保险。  
**Alternatives:** 仅降低 decoder `maxBulkBytes` → 拒绝过多正常场景且仍无法解释多包累计；仅依赖 Netty 水位线 → 无法与应用级语义一致。  
**Impact:** 增加少量计数逻辑与新参数；需要补齐回归测试与压测验证。

### ADR-20260115-04: 消除 split-package（protocol-netty 迁移到独立包）
**Context:** `yierdis-protocol` 与 `yierdis-protocol-netty` 共享同包名导致边界不清晰，未来做强封装/模块化与演进存在隐患。  
**Decision:** 将 netty codec/adapters 全部迁移到 `yier.bubu.redis.protocol.netty`（或等价独立包名），让 `yierdis-protocol` 独占 `yier.bubu.redis.protocol`。  
**Rationale:** 包名是最廉价的边界护栏；可减少包级可见性穿透与隐式依赖；提升可维护性。  
**Alternatives:** 保持 split-package 不变 → 边界风险长期存在；用文档约束替代 → 约束不可执行、不可自动验证。  
**Impact:** 源码级破坏性变更（仓库内引用需同步更新）；需更新知识库与变更说明。

### ADR-20260115-05: off-heap capabilities/SPI 统一 DB 集成点
**Context:** core 侧通过 `instanceof` 识别 unsafe 后端，导致后端差异隐式、可插拔语义不一致。  
**Decision:** 在 offheap-api 引入 capabilities/SPI，core 仅依赖能力接口选择可选 DB 集成点；unsafe 专用结构由 offheap-unsafe 提供 factory。  
**Rationale:** 显式能力比隐式类型分支更可解释；降低 core 与后端实现细节的耦合；便于新增后端/灰度演进。  
**Alternatives:** 继续 `instanceof` 扩展 → 耦合点扩散；完全反射加载 → 类型安全差且更难测试。  
**Impact:** 需要新增接口与实现，并调整构造路径；需补齐集成测试与 usedBytes 回归验证。

## Security and Performance

- **Security:**
  - decoder 上限参数化并收敛为 SSOT（避免不一致导致 DoS 风险）
  - backlog bytes 预算减少大包积压导致的资源耗尽风险
  - 错误输出继续保持 CRLF 清洗与限长（防 response splitting）

- **Performance:**
  - bytes 计数为 O(1) 增减，避免引入额外分配
  - 保持 `RespCommand` 的低分配风格（字段扩展不引入对象图）
  - 通过 bench 对比改造前后吞吐与 tail latency（p95/p99）

## Testing and Deployment

- **Testing:**
  - `mvn test` 全量回归
  - 新增/增强测试：graceful shutdown、bytes 预算触发、split-package 迁移后编译与行为一致、off-heap capabilities 行为可预测
  - `./scripts/smoke.sh` 做最小链路验证（server/cli/bench strictReplies）

- **Deployment:**
  - 先以默认参数保持行为接近现状；在 README/知识库中提供安全默认值建议（尤其是 maxBulkBytes 与 bytes 预算）
  - 若存在外部依赖该仓库包名的用户，需在变更说明中明确迁移步骤

