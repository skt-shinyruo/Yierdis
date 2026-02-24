# Technical Design: 架构模块重组（arch_module_reorg）

## Technical Solution

### Core Technologies
- Java 17（Maven 多模块）
- Netty 4.1（仅 server/client/protocol-netty adapter）
- picocli（args 解析）
- JUnit 4（单元/集成回归）
- SLF4J + Logback（日志）

### Implementation Key Points

1. **core 拆分为 API/DB/Command/Runtime 四模块 + 兼容聚合层**
   - `yierdis-core-api`：稳定能力边界（`DbEngine` + 子 ops + 稳定类型），Netty-free，禁止依赖 `db.impl`
   - `yierdis-core-db`：具体 DB/存储实现（含 off-heap 适配），实现 `DbEngine`
   - `yierdis-core-command`：命令实现，仅依赖 `core-api` + `protocol-model`
   - `yierdis-core-runtime`：`YierdisInstance` 装配与生命周期，依赖 `core-db` + `core-command`
   - `yierdis-core`：migration aggregator（仅聚合依赖，逐步迁移后可移除）

2. **消除 `ops`/`command` 对 `db` 类型的依赖**
   - 用 `yierdis-bytes` 的 `BytesSource/BytesSlice`（或新增 `BytesView`）替代 `YierdisBytesView` 的最小能力
   - 将 `ValueType`/`ScanCursorV2` 迁移为 `core-api` 所属的稳定类型（或独立 `core-model` 包）

3. **executor core/adaptor 拆分与背压收敛**
   - 新增 `yierdis-executor-core`（Netty-free）：队列、预算、调度与背压“决策”
   - server 侧 adapter：将决策映射为 Netty 的 `autoRead` 控制与写回策略（避免 `Channel` 渗透到 core）
   - 背压控制统一通过“原因集合/状态机”实现，避免 pipeline 与 executor 双向互相覆盖

4. **server 连接态拆分**
   - `ServerSessionState`：实现 `ServerSession`/`TransactionState`，只由 executor owner thread 读写（单线程语义）
   - `ServerRuntimeState`：pending/pendingBytes/closing/计数器；跨线程写入仅限原子字段
   - Channel.attr 绑定：两对象独立 key，避免“一个对象承载所有语义”

5. **护栏系统化**
   - Maven enforcer：限制模块依赖方向（例如 `core-api` 不得依赖 `core-db`，禁止引入 `io.netty.*`）
   - ArchUnit（或等价）：包级依赖方向与 import 白名单
   - 关键性质测试：autoRead/backpressure 抖动、closing/skip-side-effects、tx queue limit、模块边界扫描

6. **off-heap unsafe Netty internal 隔离**
   - 将 `PlatformDependent` 访问封装在单一 façade（例如 `NettyPlatformMemoryAccess`），其余代码只依赖后端自定义的最小 raw-memory 接口
   - 长期策略：优先引导使用 `foreign` backend（纯 JDK），unsafe backend 保持“可选/可审计”

## Architecture Design

```mermaid
flowchart TD
  Bytes[yierdis-bytes\n(BytesSource/BytesSlice/...)]

  ProtoModel[yierdis-protocol-model\n(Command/ReplyWriter/Session/CommandContext)]
  ProtoCodec[yierdis-protocol-codec\n(Custom Protocol v1 codec)]
  ProtoNetty[yierdis-protocol-netty\n(Netty codec adapter)]

  CoreApi[yierdis-core-api\n(DbEngine + ops + stable types)]
  CoreDb[yierdis-core-db\n(DB impl + off-heap integration)]
  CoreCmd[yierdis-core-command\n(command handlers)]
  CoreRt[yierdis-core-runtime\n(YierdisInstance assembly)]
  CoreAgg[yierdis-core\n(migration aggregator)]

  ExecCore[yierdis-executor-core\n(queue/budget/scheduling decisions)]
  Server[yierdis-server\n(netty bootstrap + adapter)]

  Bytes --> CoreApi
  ProtoModel --> CoreCmd
  CoreApi --> CoreCmd
  CoreApi --> CoreDb
  CoreDb --> CoreRt
  CoreCmd --> CoreRt
  CoreApi --> CoreAgg
  CoreRt --> CoreAgg

  ProtoCodec --> ProtoNetty
  ProtoNetty --> Server

  ExecCore --> Server
  CoreRt --> Server
```

## Architecture Decision ADR

### ADR-20260224-01: 拆分 core 为 API/DB/Command/Runtime + 聚合层（Recommended）
**Context:** 当前 `yierdis-core` 内部包含 ops/db/command/runtime，编译期无法强约束依赖方向，且 `ops`/`command` 容易被 `db` 类型污染。  
**Decision:** 以 Maven 模块边界拆分出 `yierdis-core-api` / `yierdis-core-db` / `yierdis-core-command` / `yierdis-core-runtime`，并保留 `yierdis-core` 作为迁移期聚合层。  
**Rationale:** 让“依赖方向”从约定变成编译期事实；降低未来替换存储/命令/装配的成本。  
**Alternatives:** 仅通过测试扫描约束包依赖 → 拒绝原因：约束强度不足，长期易回退。  
**Impact:** 大范围包迁移与 pom 调整；需要严格的分阶段回归策略。

### ADR-20260224-02: 用 `yierdis-bytes` 统一 key/view 能力，移除 `YierdisBytesView`
**Context:** `YierdisBytesView(len/byteAt)` 位于 `db` 包导致 `ops` 必然依赖 `db`，边界被污染。  
**Decision:** 用 `BytesSlice`（已有 `length/getByte/getBytes`）或新增 `BytesView(length + random access)` 承载最小能力，统一落在 `yierdis-bytes`（Netty-free）。  
**Rationale:** bytes 抽象本就属于中立模块；让 `ops` 依赖 bytes 而非 db 更符合语义。  
**Alternatives:** 保留 `YierdisBytesView` 但移动到 `ops` → 拒绝原因：名字与语义仍指向 db，且会继续误导依赖方向。  
**Impact:** 会影响 `KeyspaceOps/TtlOps/...` 等接口签名，需要批量迁移与回归测试兜底。

### ADR-20260224-03: executor 拆分为 Netty-free core + server adapter，并用统一背压状态机控制 autoRead
**Context:** `NettyCommandExecutor` 目前同时承担排队/调度/预算/背压/写回/autoRead 控制；背压触发源多头导致状态复杂。  
**Decision:** 新增 `yierdis-executor-core` 承载纯决策与可测试逻辑；server 侧 adapter 负责与 Netty 的 `Channel`/`autoRead` 对接，并将背压原因集合收敛为单一 controller。  
**Rationale:** 核心调度逻辑可在无 Netty 环境单测；背压状态机可明确建模并回归锁定。  
**Alternatives:** 在现有类内部重构但不拆模块 → 拒绝原因：编译期仍无法阻止 Netty 渗透与职责回退。  
**Impact:** server 侧接入点会变化；需要补齐 busy/backpressure/fair scheduling 的回归测试。

## Security and Performance

- **Security:**
  - 保持协议错误净化/限长为 SSOT（避免 response splitting）
  - 连接关闭/异常路径必须保证 “已入队命令不再产生副作用” 的语义一致
  - SPI/ServiceLoader 发现路径保持 best-effort 诊断，不泄漏敏感信息
- **Performance:**
  - bytes 抽象保持随机访问 + 可选 memoryAddress fast-path
  - executor core 只做决策，不引入额外锁；server adapter 只做必要的线程切换/autoRead 控制

## Testing and Deployment

- **Testing:**
  - 分阶段 `mvn test`（每次只推进一个可回归的迁移批次）
  - 新增边界护栏（enforcer + ArchUnit）并纳入默认测试集
  - 关键链路回归：backpressure/autoRead、closing/skip-side-effects、tx queue limit、off-heap backend SPI 选择
- **Deployment:**
  - 先引入新模块并保持旧入口可用（聚合层迁移期存在）
  - 分批切换 `yierdis-server` 的依赖与装配入口
  - 完成迁移后再清理 compatibility 层与废弃包名（如需要）
