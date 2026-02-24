<!-- migrated_from: history/2026-02/202602011923_redis_compat_extended/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: Redis 兼容性扩展（RESP3 / 多 DB / 事务 / PubSub / 持久化 / ACL/TLS）

## Technical Solution

### Core Technologies
- Java 17（Maven，聚合模块 `pom.xml`）
- Netty 4.1.x（`pom.xml` 中 `netty.version`）
- RESP 协议抽象与写出：`yierdis-protocol`（`RespWriter`/`RespSession`）
- Netty adapter：`yierdis-protocol-netty`（`RespCommandDecoder`/`RespFrame`）
- 命令分发：`yierdis-core`（`YierdisFastCommandProcessor` + `CommandRegistry`）
- 单线程语义执行器：`yierdis-server`（`NettyCommandExecutor`）
- CLI 参数：`yierdis-args`（picocli）

### Implementation Key Points

1. **RESP3 扩展以 writer 为 SSOT**：以 `yierdis-protocol` 的 `RespWriter` 作为协议写出语义的唯一真相（避免 Netty codec 与 server fast-path 漂移）。
2. **连接态扩展但跨模块隔离**：多 DB / MULTI / PubSub 等状态归属“连接态”，但 core 模块不直接依赖 server；通过协议层 session 或新的轻量接口进行解耦。
3. **多 DB 的数据结构选择**：用 `YierdisDb[]`（或 `List<YierdisDb>`）代表 DB0..N-1，每个实例仍保持单线程语义（绑定同一 executor thread），连接态持有当前 dbIndex。
4. **事务实现策略**：`MULTI` 期间不直接执行命令，而是将 `RespCommand`（或其 canonical form）入队到连接态的 transaction queue；`EXEC` 时在 executor 线程顺序执行，返回结果数组。
5. **PubSub 兼容策略**：为每个 channel 维护订阅集合；`PUBLISH` 通过 server-side pubsub broker 广播到订阅者。RESP3 下使用 push 语义（需要扩展 writer），RESP2 下使用数组形态兼容。
6. **AOF 最小闭环**：在 executor thread 内对“写命令”追加记录，启动时回放；默认关闭，提供 fsync 配置（always/everysec/no）与重写策略（可后续迭代）。
7. **TLS/ACL 最小基线**：TLS 用 Netty `SslHandler` 注入 pipeline（可选 listener）；ACL 先实现 `requirepass` + `AUTH` + `NOAUTH` gate，后续再扩展 `ACL SETUSER` 等。
8. **资源预算统一与可观测**：对“maxmemory / offheap / executor backlog / protocol limits”提供统一的 `INFO` 与 `CONFIG GET` 输出；将命令层 prepareWrite 的估算常量与 DB accounting 对齐为单点定义。
9. **生态兼容命令优先级**：优先对齐 `INFO/CONFIG/CLIENT/COMMAND/SCAN` 等“生态工具常用”的入口命令，避免因为 reply shape 不一致导致工具不可用。
10. **连接关闭语义收敛**：对“协议错误关闭/客户端断开/QUIT close-after-reply”三类关闭路径统一维护 connection closing 状态，确保 backlog 中命令不会产生不可预期副作用（必要时只回收不执行）。

## Architecture Design

```mermaid
flowchart TD
    C[Client redis-cli/SDK] -->|TCP| N[Netty Pipeline]
    N --> D[RespCommandDecoder]
    D --> H[YierdisFastCommandHandler]
    H --> E[NettyCommandExecutor\n(backlog+backpressure)]
    E --> P[YierdisFastCommandProcessor\n(CommandRegistry)]
    P --> R[Command Handlers\n(Server/Key/String/...)]
    R --> DB[DbRouter -> YierdisDb[dbIndex]]
    R --> S[Session State\n(dbIndex/multi/pubsub/auth)]
    R --> W[RespWriter\n(RESP2/RESP3)]
    R --> AOF[AOF Writer (optional)]
    N --> TLS[SslHandler (optional)]
```

## Architecture Decision ADR

### ADR-001: 多 DB 采用“多实例 YierdisDb + 连接态 dbIndex”模型
**Context:** 当前仅支持 DB0（`ServerCommands.select(...)`），但 Redis 生态普遍假设支持多 DB（默认 16）。  
**Decision:** server 启动时创建 `databases` 个 `YierdisDb` 实例并绑定到同一 executor 线程；连接态增加 `dbIndex`，命令执行时通过 router 选择目标 DB。  
**Rationale:** 兼容 Redis 行为；改动集中；保持单线程语义，避免引入跨线程并发复杂度。  
**Alternatives:**  
- 单 `YierdisDb` 内部引入 DB map → Rejection: 会放大单实例结构复杂度并干扰现有 keyspace/off-heap 结构。  
- 为每个 DB 创建独立 executor thread → Rejection: 行为不再接近 Redis，且需要跨线程协调 PubSub/事务。  
**Impact:** 需要在命令层引入“按连接选择 DB”的能力，并扩展 `INFO/FLUSHDB` 等语义。

### ADR-002: RESP3 扩展优先覆盖“客户端互操作必需类型”
**Context:** 当前 RESP3 仅支持 map/set/null 的极小子集（`RespWriter`），不足以支撑 PubSub push 与更多结构化回复。  
**Decision:** 以“主流 client + redis-cli”的互操作为目标，分层实现 RESP3 类型：先 push/boolean/double/blob error/verbatim，再补齐 attribute 等低优先级类型。  
**Rationale:** 降低一次性实现全部类型的风险；确保每一步都有测试闭环。  
**Alternatives:** 一次性完整实现 RESP3 spec → Rejection: 范围过大、测试成本高、容易引入细节错误。  
**Impact:** 需要建立 RESP3 兼容测试矩阵，并在 `COMMAND/INFO/CONFIG` 输出中明确类型差异。

### ADR-003: AOF 采用“追加 RESP 命令日志 + 启动回放”的教学友好实现
**Context:** 缺少持久化会导致重启丢数据，影响体验与部分框架假设。  
**Decision:** 提供最小 AOF：记录写命令的 RESP2 形式（canonical），启动时按同一执行链路回放；默认关闭。  
**Rationale:** 实现成本可控；不引入复杂二进制格式；便于调试。  
**Alternatives:** RDB 快照 / 混合持久化 → Rejection: 工作量大且不利于教学拆解。  
**Impact:** 需要严格定义“可持久化命令集合”，并处理 replay 时的错误/幂等问题。

### ADR-004: INFO 保持 Redis 兼容形态，yierdis 指标输出迁移到 STATS/扩展 section
**Context:** 当前 `INFO`/`STATS` 输出为 RESP2 array / RESP3 map（`NettyServerInfoProvider`），与 Redis `INFO` 的 bulk string（文本分节）不一致，导致部分生态工具/脚本解析失败。  
**Decision:** 将 `INFO` 调整为 Redis 风格 bulk string 输出（至少覆盖基础 section 与常用字段），并保留现有结构化指标输出能力（通过 `STATS` 或 `INFO YIERDIS` section 提供）。  
**Rationale:** 提升生态互操作性，同时不丢失现有排障能力；兼容策略清晰。  
**Alternatives:** 维持当前 `INFO` map/array 形态 → Rejection: 与 Redis 生态预期偏差大，工具兼容成本高。  
**Impact:** 需要更新 `NettyServerInfoProvider`、命令层 `ServerCommands.info(...)` 的参数解析，并增加兼容测试。

### ADR-005: SCAN 采用“keyspace 层提供 cursor 扫描 API”，避免每次构建全量快照
**Context:** 缺少 `SCAN` 会导致工具回退到 `KEYS`，在大 keyspace 下引发阻塞与不可预测延迟。直接用 `forEach` 每次构建快照会导致高内存与 O(N) 扫描退化。  
**Decision:** 在 keyspace 抽象（`YierdisKeyspace`）上新增 `scan(cursor,count,match)` 风格 API，由具体实现（heap/off-heap）以“表索引 cursor”进行增量遍历；rehash 期间采用 best-effort 处理（允许重复/漏掉少量 key，但保证不崩溃并可前进）。  
**Rationale:** 更接近 Redis 的扫描语义与性能特征；避免额外分配；适用于 heap/off-heap 双实现。  
**Alternatives:** 每次 SCAN 构建 keys 快照 → Rejection: 内存压力大且延迟不稳定；不适合教学对比。  
**Impact:** 需要修改 `YierdisKeyspace` 接口与两套实现（`ByteArrayKeyspace`、`YierdisUnsafeOffHeapKeyspace`），并新增 SCAN 命令实现与测试。

### ADR-006: HELLO 选项必须“真实生效或明确拒绝”，禁止静默忽略
**Context:** 当前 `HELLO 3` 只做协议切换，对 `AUTH/SETNAME` 等额外参数会静默忽略，可能导致客户端误判状态（尤其引入认证后属于安全风险）。  
**Decision:** 对 HELLO 的 options 做严格解析：支持的选项必须生效（例如 AUTH 触发认证、SETNAME 写入连接属性）；不支持的选项返回 `ERR syntax error` 或 `ERR unsupported option`；同时将 `HELLO 3` 的 reply 字段/类型向 Redis 对齐（例如 `proto` 使用 integer，而不是 bulk string），并对缺失字段（如 `id/modules`）采取“补齐或明确声明不支持”的策略，避免客户端能力探测误判。  
**Rationale:** 行为可预测且更接近 Redis；避免“看似成功实际未生效”。  
**Alternatives:** 保持忽略 → Rejection: 兼容性与安全风险高。  
**Impact:** 需要扩展连接态（用户名/认证状态/客户端名），并补充集成测试（redis-cli/典型 SDK）；同时需要更新 `RespWriter`（若缺少必要类型）与 `HELLO` 输出测试，以避免 RESP2/RESP3 reply 形态漂移。

### ADR-007: MEMORY STATS 的 value 类型向 Redis 对齐（整数而非 bulk string）
**Context:** 当前 `MEMORY STATS` 在 RESP2/RESP3 下把数值写为“十进制 bulk string”，并且现有测试依赖该行为；但 Redis 生态更常见的约定是 value 使用 integer 类型。  
**Decision:** 将 `MEMORY STATS` 的 value 输出改为 integer：RESP2 下仍是扁平 key/value 数组（key=bulk string，value=integer）；RESP3 下 map 的 value 也使用 integer。  
**Rationale:** 更接近 Redis，减少严格类型解析工具/测试的兼容问题。  
**Alternatives:** 保持 bulk string → Rejection: 生态兼容性较弱，且与其他命令的数值输出风格不一致。  
**Impact:** 需要更新 `KeyCommands`、相关文档（`helloagents/wiki/modules/db.md`），并更新/重写现有 RESP3 collection reply 测试。

### ADR-008: 内置调试工具与文档的 SSOT 收敛，避免“代码演进但 README 漂移”
**Context:** 当前 `README.md` 与实际命令实现、知识库文档存在漂移（例如 `COMMAND`、`KEYS` glob 语义、命令列表），且内置 client/CLI 与 parser 仅覆盖 RESP3 最小子集，无法支撑扩展后的调试与回归。  
**Decision:** 
- 引入“可自动生成/可校验”的命令清单与兼容边界：以 `CommandRegistry`/命令注册点为 SSOT 输出摘要，README 只保留高层介绍与指向性链接。  
- 同步扩展内置 `yierdis-client` / `RespDecoder` / `RespObjectParser`，确保在扩展 RESP3（含 push）后仍可用于测试与调试。  
**Rationale:** 降低长期维护成本，避免文档误导与工具不可用导致的排障困难。  
**Alternatives:** 人工维护 README 与工具 → Rejection: 漂移不可避免，回归成本高。  
**Impact:** 需要增加 guard tests（文档/命令注册）、并扩展协议解析器的类型覆盖面。

## Security and Performance

- **Security:**
  - 输入上限与 DoS 防护继续以 `protocolMax*` 与 executor backlog 预算为主，并在 `CONFIG GET` 中可观测。
  - TLS 默认关闭；启用时从文件加载证书/私钥，禁止在日志输出敏感内容（密码/私钥路径等）。
  - `requirepass` 模式下，除 `AUTH/HELLO/QUIT/PING` 等少数命令外，未认证统一返回 `NOAUTH Authentication required.`
- **Performance:**
  - retained-slice 风险控制：将 frame compaction/copy 策略显式化（阈值 + ratio + 上限），并在压力测试中对比驻留 bytes 与延迟分位数。
  - 多 DB：所有 DB 仍在同一 executor 上，避免锁；但要避免 `INFO`/`KEYS` 类命令在全 DB 上做 O(n) 扫描（必要时引入渐进式迭代或限制）。
  - SCAN：必须避免“每次扫描都生成全量 keys 快照”的实现方式，优先采用增量 cursor 迭代与低分配输出。

## Testing and Deployment

- **Testing:**
  - 新增集成测试覆盖：RESP3 类型、SELECT 多 DB、MULTI/EXEC、PubSub push、AUTH gate、AOF replay。
  - 保留现有 server/protocol 单元测试（例如 `yierdis-server/src/test/java/...`），新增用例尽量复用已有 Netty 测试基础设施。
- **Deployment:**
  - 默认行为保持教学可控（RESP2 + DB0 + 无持久化/无 TLS/无认证），通过新增参数开启扩展能力。
