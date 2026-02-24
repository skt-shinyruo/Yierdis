# Technical Design: DB/Executor 分层解耦与组件化（ReplySink + 巨型类拆分）

## Technical Solution

### Core Technologies

- Java 17 + Maven 多模块构建
- 协议无关抽象：`yierdis-protocol` 的 `Command/ReplyWriter/Session`
- Bytes 抽象：`yierdis-bytes` 的 `BytesSource/BytesSlice/BytesSink`
- Netty 执行器：`EventExecutor` + 自研 backlog/backpressure/scheduling 组件

### Implementation Key Points

1. **引入窄接口 `ReplySink`：**
   - 定位：作为 `ReplyWriter` 的子集能力，仅表达 “bulk string 值的写出”（heap byte[] / off-heap `BytesSlice` / long-ascii）。
   - 目的：让 value/数据结构侧只依赖“写值”而不依赖“写 shape（array/map/header/error）”，将“回包优化 API”从 db 对外 API 中移走。
   - 策略：`ReplyWriter` 通过 `extends ReplySink`（或等价关系）天然可作为 sink 传递，避免在 hot path 增加额外 adapter 分配。

2. **迁移 streaming reply API 的边界（先可编译、再去耦彻底）：**
   - 第一步：将 `ops` 层面涉及 streaming 的参数类型从 `YierdisBulkStringOutput` 迁移到 `ReplySink`；
   - 第二步：命令层直接把 `ReplyWriter` 作为 `ReplySink` 传入（取消 `CommandSupport.bulkOut` 的桥接对象）；
   - 第三步：db 内部逐步将 `YierdisBulkStringOutput` 替换/删除，统一使用 `ReplySink` 或纯数据迭代接口。

3. **将 `*ReplyCount/*ReplyInto` 从 `YierdisDb` 对外移除：**
   - `YierdisDb` 仅作为 `DbEngine` 的实现与状态容器；
   - 聚合读取的“计数 + streaming 写出”能力继续存在，但被收敛到更明确的边界（`ops`/value 组件），并逐步重命名为 data-oriented（例如 `rangeCount/rangeInto`）。

4. **db 层移除 `protocol.Command` 依赖：**
   - 新增/引入中立的 “argument bytes view/slice” 表达（优先复用 `BytesSource + offset + len` 或自定义轻量结构），由命令层负责从 `Command.frame/argOffset` 提取；
   - `YierdisObject` 的 string 构造/覆盖写入从 “接收 `Command`” 改为 “接收 bytes view/slice”，继续保留：
     - int-encoding 探测（读取 ASCII）
     - off-heap address allocator 的 `copyMemory` 快路径（若 source 暴露 memoryAddress）
     - heap-only fallback（需要时再 `copyToByteArray`）

5. **`YierdisDb`/ValueOps 组件化：**
   - 将 `YierdisDbValueOps` 进一步按类型拆分（Strings/Hashes/Lists/Sets/ZSets/Hll），降低单文件体积与变更半径；
   - `YierdisDb` 保留：thread-guard、keyspace/expires、ledger/reservation、expiration/eviction coordinator 的装配与统一入口。

6. **执行器拆分（移动代码优先）：**
   - `NettyCommandExecutor` 保留对外契约与可观测性计数器，但将内部职责拆分为：
     - `Submitter`：trySubmit + backlog bytes 预算 + 拒绝原因
     - `DrainLoop`：drain tick + budget（maxDrain/timeBudget）+ schedule next tick
     - `ReplyWriteBatcher`：per-channel ByteBuf 分配、ReplyWriter 创建、write 合并、flush 收敛
     - `Scheduling`：GLOBAL/FAIR 的任务选择策略（对现有 `NettyExecutorTaskQueue` 做包裹或抽象）
   - 目标：拆分后每个组件可被单测驱动，且单个文件复杂度显著下降。

## Architecture Design

```mermaid
flowchart TD
  subgraph protocol[yierdis-protocol]
    RW[ReplyWriter]
    RS[ReplySink]
    CMD[Command]
  end

  subgraph core[yierdis-core]
    CP[YierdisFastCommandProcessor]
    CS[CommandSupport]
    OPS[ValueOps / *Ops]
    DB[YierdisDb (DbEngine)]
    VO[Value components\n(String/Hash/List/Set/ZSet/HLL)]
  end

  subgraph server[yierdis-server]
    IO[Netty I/O threads]
    EX[NettyCommandExecutor facade]
    DL[DrainLoop]
    SUB[Submitter + Budget]
    BP[BackpressureController]
    SCH[Scheduling (GLOBAL/FAIR)]
    RB[ReplyWriteBatcher]
  end

  RW --> RS
  IO --> SUB --> EX
  EX --> DL --> SCH --> CP --> CS --> OPS --> DB --> VO
  CS --> RW
  OPS --> RS
  EX --> BP
  DL --> RB --> RW
```

## Architecture Decision ADR

### ADR-20260208-01: 引入 ReplySink，并将 streaming bulk 输出从 db 包迁出

**Context:** 目前 streaming 读路径通过 `YierdisBulkStringOutput`（位于 db 包）连接 `ReplyWriter`，并在 `YierdisDb` 公开 API 中出现 `*ReplyCount/*ReplyInto` 等“回包形态 API”，造成分层渗透与未来协议演进阻力。

**Decision:** 在 `yierdis-protocol` 引入窄接口 `ReplySink`，由 `ReplyWriter` 继承该能力；命令层与 `ops` 层以 `ReplySink` 作为 streaming bulk 写出边界，逐步移除 db 包内的回包桥接类型与 `YierdisDb` 的 reply 形态方法。

**Rationale:** 用最小接口表达最小能力，把协议无关的“写值”能力固定在上层抽象中，降低存储与协议的耦合，同时保持现有低分配 streaming 路径与回归锚点。

**Alternatives:**
- 继续使用 `YierdisBulkStringOutput`：改动小但分层问题不解决（拒绝）
- 让 value 直接依赖 `ReplyWriter`：实现简单但接口过宽，容易把 shape/error 等协议语义下沉到 db（拒绝）
- 彻底改为迭代器/Pull 模式：边界更干净，但一次性改动较大（暂缓，作为后续演进选项）

**Impact:** streaming 输出边界更清晰；短期可能存在 adapter 过渡层（可接受，后续可逐步移除）。

### ADR-20260208-02: NettyCommandExecutor 组件化拆分（保持外部契约不变）

**Context:** `NettyCommandExecutor` 集中背压/调度/预算/写回/flush 合并等编排，变更风险高、难以单测验证。

**Decision:** 将其拆解为 submitter/drain loop/scheduling/reply write batcher 等组件，`NettyCommandExecutor` 作为 facade 维持原有对外契约与统计口径。

**Rationale:** 降低单类复杂度，增强可测试性，并为未来引入新协议/更细粒度调度策略留出结构空间。

**Impact:** 文件数量增加但职责更清晰；需要用现有 executor 测试套件做严格回归。

## Security and Performance

- **Security:**
  - sink/adapter 只允许“同步写出”，禁止缓存/持有 frame/ByteBuf/off-heap slice 引用
  - 保持协议错误净化与输入上限不变（DoS 护栏不弱化）
  - 保持 `maxmemory` 与 reservation 的错误顺序约束（避免双 reply）
- **Performance:**
  - streaming 路径仍以 “count + writeInto” 形态避免中间集合分配
  - `ReplySink` 设计为零分配（由 `ReplyWriter` 直接实现/继承）
  - db 写入保留 off-heap `copyMemory` 快路径与 heap fallback

## Testing and Deployment

- **Testing:**
  - 新增/补齐 streaming reply 的单测锚点（LRANGE/HGETALL/SMEMBERS/ZRANGE/Z*BYSCORE）
  - 回归执行器测试：公平调度/背压/关闭语义/queued-bytes 预算
  - 全量：运行 `mvn test`
- **Deployment:**
  - 该重构不改变对外协议承诺（仍为 Custom Protocol v1）
  - 建议在 bench 环境下对比关键 workload 的 QPS/P99，明确可接受退化边界

