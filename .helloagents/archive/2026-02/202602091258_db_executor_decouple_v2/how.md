<!-- migrated_from: history/2026-02/202602091258_db_executor_decouple_v2/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: DB/Executor 边界解耦 v2（移除 server→YierdisDb 直接依赖 + 持续拆分巨型类）

## Technical Solution

### Core Technologies

- Java 17 + Maven 多模块构建
- Netty（server I/O + 单线程执行器 `EventExecutor`）
- `yierdis-core`：DB/命令处理 SSOT（Netty-free）
- `yierdis-protocol`：协议无关抽象（`Command/ReplyWriter/Session`）+ streaming 窄接口（当前为 `ReplySink`）
- `yierdis-bytes`：中立 bytes 抽象（`BytesSource/BytesSink/BytesSlice`）

### Implementation Key Points

1. **server→DB 绑定动作依赖倒置（彻底移除 `YierdisDb` 直连）：**
   - 将 executor 的“线程绑定”能力视为 `Runnable bindToCurrentThread`（或 `DbBinder` 之类窄接口），由上层装配时传入；
   - `NettyCommandExecutor` 不再需要 `YierdisDb` 类型，仅在 `start()` 阶段执行 binder；
   - `YierdisServerBootstrap` 通过 `YierdisInstance#bindToCurrentThread` 注入 binder，避免直接持有 `YierdisDb[]`。

2. **server info provider 依赖收敛：**
   - `NettyServerInfoProvider` 不依赖 `YierdisDb` 具体实现；
   - 通过 `DbEngine`/`MemoryOps`/`KeyspaceOps` 或更窄的只读 view 获取统计信息；
   - 以“只读查询接口”替代“实现类数组绑定”，减少出边界调用风险。

3. **executor 组件化继续推进：**
   - 引入 `NettyCommandExecutorConfig`（或拆分为 `QueueConfig/BackpressureConfig/DrainConfig`）收敛构造参数；
   - 将 drain loop、flush batch、close-after-reply 路径拆成独立组件，`NettyCommandExecutor` 只负责编排；
   - 每个组件有单独的 contract 与单测，降低“集成式断言”比例。

4. **DB 巨型类拆分策略（以“概念抽取 + 对外能力接口化”为主）：**
   - `YierdisDb` 仅作为 orchestrator：持有 state + 将能力暴露为 `DbEngine` 子组件（`values()/expiration()/eviction()/keyspace()/memory()/ttl()/lifecycle()` 等）；
   - 将稳定概念（例如：错误类型、选项枚举、ledger/记账、reservation token）迁移到 `ops` 或 `db` 子组件；
   - 将容易膨胀的执行逻辑（expire cleanup/eviction sample/预算计算）进一步外置为可测试的独立类。

5. **清理残余 reply 命名渗透：**
   - 对 `ZSetValue` 的内部 helper（仍保留 `ReplyCount/ReplyInto`）改为数据语义命名；
   - 保持行为与性能不变，仅修复“语义暗示”的长期维护成本。

---

## Architecture Design

```mermaid
flowchart TD
  subgraph Server[ yierdis-server (Netty) ]
    IO[Netty I/O threads] -->|trySubmit(cmd)| Exec[NettyCommandExecutor (glue)]
    Exec -->|drain loop| Proc[YierdisFastCommandProcessor]
    Exec -->|ReplyWriterFactory| RW[ReplyWriter]
  end

  subgraph Core[ yierdis-core ]
    Proc --> Router[YierdisDbRouter]
    Router --> Engine[DbEngine]
    Engine --> Ops[ValueOps/Expiration/Eviction/Keyspace/Memory/Ttl]
    Ops --> Values[Hash/List/Set/ZSet/String/HLL Values]
  end

  subgraph Protocol[ yierdis-protocol ]
    RW --> Sink[ReplySink (bulk-string streaming)]
  end

  Values -->|stream bulk values| Sink
```

核心约束：
- server 不直接引用 `YierdisDb`；DB 线程绑定通过 binder 注入；
- command 层通过 `DbEngine/*Ops` 获取能力；
- value 层仅依赖 streaming 窄接口（不依赖 reply shape）。

---

## Architecture Decision ADR

### ADR-001: server 不直接依赖具体 DB 实现（依赖倒置到 binder + engine 接口）

**Context:** server/executor 目前在构造器与 info provider 中直接引用 `YierdisDb`，导致边界渗透与替换成本高。  
**Decision:**  
- `NettyCommandExecutor` 只接收 `Runnable bindToCurrentThread`（或等价窄接口）用于 owner-thread 绑定；  
- server 侧对 DB 的只读查询通过 `DbEngine/*Ops` 或专用只读 view 完成；  
- `YierdisServerBootstrap` 仅持有 `YierdisInstance` 与必要的接口引用，不暴露/缓存 `YierdisDb[]`。

**Rationale:**  
- 将“线程绑定语义”从“DB 实现类型”剥离出来，提升替换能力；  
- 让 executor 更纯粹：只负责入队/调度/回包；  
- 对未来（多 DB 实现、代理、持久化、复制）更友好。

**Alternatives:**  
- A) 继续使用 `YierdisDb` 入参并靠注释约束 → 拒绝原因：无法阻止边界渗透，且不利于测试与替换  
- B) 让 executor 直接依赖 `YierdisInstance` 并在内部调用 bind → 拒绝原因：executor 变成 runtime 装配点，职责扩大

**Impact:**  
- 需要调整 `NettyCommandExecutor` 构造器与 call sites；  
- server 代码对 DB 类型依赖减少，编译期约束更强；  
- 测试需要从 `new YierdisDb()` 迁移到 instance/engine 方式构造。

### ADR-002（可选）: streaming sink 抽象是否下沉为 bytes 中立接口

**Context:** value 层为了低拷贝 streaming bulk-string 写出，目前依赖 `ReplySink`（语义上是“reply”）。  
**Decision (Proposed):** 先完成 server→`YierdisDb` 解耦与 executor 组件化；若后续仍认为 “storage 依赖 protocol” 影响演进，再评估将 streaming sink 下沉到更中立模块（如 `yierdis-bytes`）并通过 adapter 兼容。  
**Rationale:** 优先解决高收益且低风险的“server 直连实现类”问题，避免一次性引入过大的依赖重排风险。

---

## API Design

### Java API: `NettyCommandExecutor`

- **Change:** 移除/弃用 `NettyCommandExecutor(YierdisDb db, ...)` overload  
- **Keep:** `NettyCommandExecutor(Runnable bindToCurrentThread, YierdisFastCommandProcessor, EventExecutor, ReplyWriterFactory, ...)` 为唯一主构造入口  
- **Compatibility strategy:** 若外部存在使用点（bench/tools/tests），提供短期 adapter（deprecated），内部仅做 `db::bindToCurrentThread` 转发，并引导迁移到 binder 形式。

---

## Security and Performance

- **Security:**
  - 不引入新的外部网络/存储依赖；
  - 严格保持 close-after-reply / backpressure 行为一致，避免协议层面错误回包（double reply）。

- **Performance:**
  - binder 注入不引入额外分配（method reference / lambda 仅在装配阶段产生）；
  - executor 组件化以“零额外 per-command 分配”为硬约束：组件对象在 executor 初始化阶段创建并复用；
  - streaming bulk-string 维持现有的 slice 写出能力（避免 heap copy）。

---

## Testing and Deployment

- **Testing:**
  - executor 行为：队列满拒绝、背压 enter/exit、drain time limit、公平调度、close-after-reply 跳过后续命令；
  - DB 行为：ZSetValue helper 命名迁移不改变输出/计数结果（用既有 zset 测试覆盖）。

- **Deployment:**
  - 纯重构变更，无数据迁移；
  - 建议先在 CI 上跑 `mvn test`，并在 bench 场景做吞吐与 tail latency 对比。

