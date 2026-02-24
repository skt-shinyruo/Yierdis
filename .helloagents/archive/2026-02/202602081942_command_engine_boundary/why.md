<!-- migrated_from: history/2026-02/202602081942_command_engine_boundary/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Command/DB 边界收口与执行器协议解耦（DbEngine + Ops 化）

## Requirement Background

截至 **2026-02-08** 的代码现状已较早期版本有明显改善（例如 `ReplySink` 引入、executor 子组件拆分等），但“核心实现层次耦合偏重”的问题仍以更隐蔽的形式存在：

1. **命令层仍直接依赖具体实现类 `YierdisDb`：**
   - `yierdis-core` 的 `command/*` 代码通过 `CommandSupport.db(out)` 直接拿到 `YierdisDb`，并调用其大量 public API。
   - `ops` 层接口（如 `StringOps`）仍在签名中引用 `YierdisDb` 的嵌套类型（`SetMode`/`ExpireOption` 等），导致“抽象边界存在但无法阻断依赖”。
2. **“为了回包优化”的 API 语义仍残留在 `ops` 边界：**
   - `*ReplyCount/*ReplyInto` 虽已不再出现在 `YierdisDb` 对外 API 上，但仍存在于 `ops`/value 访问路径中，命名与形态会持续暗示“存储层对 reply 形状负责”，长期演进仍会被协议/性能诉求牵着走。
3. **执行器仍与具体协议实现耦合：**
   - `NettyCommandExecutor` / `YierdisFastCommandHandler` 直接构造 `JsonLineReplyWriter`，意味着未来更换 wire protocol（或引入多协议）需要在 executor 编排层做侵入式修改。

这些耦合在短期能压低实现成本，但长期会放大以下风险：
- 分层失效：协议形态、命令语义、存储实现难以独立演进；
- 变更爆炸半径变大：一个小 feature 牵动多个层次与大量调用点；
- 可测试性下降：边界不清导致难以做“组件级”回归锚点。

本提案目标是在 **不要求一次性物理拆分 `YierdisDb`** 的前提下，通过“接口边界收口 + 适配层”建立可持续演进的分层护栏，并为后续进一步拆分巨型类留出空间。

## Change Content

1. **以 `DbEngine` 为命令层唯一 DB 入口（编译期护栏）：**
   - `command` 层只依赖 `yier.bubu.redis.ops.*`（`DbEngine` + 子 ops），禁止直接 import `yier.bubu.redis.db.YierdisDb`。
2. **补齐 command-facing 的子能力边界：**
   - 在 `ops` 层新增 `KeyspaceOps/TtlOps/MemoryOps/DbLifecycleOps`（或等价拆分），覆盖 `TYPE/DEL/EXISTS/TTL/SCAN/MEMORY/FLUSHDB` 等命令需要的能力；
   - `DbEngine` 暴露这些子 ops 的访问器以保持单入口形态。
3. **迁移“嵌套类型泄漏”（SetMode/ExpireOption/异常类型）到 `ops`：**
   - 将 `YierdisDb.SetMode` / `YierdisDb.ExpireOption` 提升为 `ops` 层稳定类型（或以等价替代类型迁移）；
   - 将 `WrongTypeException/CommandException` 作为 `ops` 层异常（供 command 捕获与映射），避免处理器必须依赖 `YierdisDb`。
4. **重塑 `*ReplyCount/*ReplyInto` 的形态与命名（去 reply 语义）：**
   - 将命名从 “Reply*” 迁移为数据语义（例如 `*count/*writeTo` 或 `*cursor/*forEach`），并明确：存储/value 层允许依赖 `ReplySink`（仅 bulk-string 子集），但不得承担 reply shape（array/map header、错误形态等）。
5. **执行器协议解耦：引入 `ReplyWriterFactory`（或等价注入点）：**
   - `NettyCommandExecutor` / `YierdisFastCommandHandler` 仅依赖 `ReplyWriter` 接口与工厂，默认实现仍为 Custom Protocol v1，但 executor 不再直接 new `JsonLineReplyWriter`。

## Impact Scope

- **Modules:**
  - `yierdis-core`：`command/*`、`ops/*`、`runtime/YierdisInstance`、`db/YierdisDb*`
  - `yierdis-protocol`：`ReplyWriter/ReplySink`（接口保持稳定；仅补充能力时才调整）
  - `yierdis-server`：`NettyCommandExecutor`、`YierdisFastCommandHandler`（reply writer 工厂注入）
- **APIs:** 内部 API 允许调整（包名/类名/方法签名）；对外协议维持 Custom Protocol v1
- **Data:** 无（不引入新的持久化格式）

## Core Scenarios

### Requirement: R1 Command → DbEngine 边界收口（编译期护栏）
<a id="r1"></a>
**Module:** core(command) / core(ops) / core(db)

命令层只通过 `DbEngine` 与子 ops 访问 DB 能力，禁止直接引用具体实现类 `YierdisDb`。

#### Scenario: S1 command 包不再出现 `import YierdisDb`
<a id="r1-s1"></a>
条件：完成迁移后执行源码扫描与编译。
- 期望：`yierdis-core/src/main/java/yier/bubu/redis/command/**` 不再出现 `import yier.bubu.redis.db.YierdisDb;`
- 期望：命令语义与错误输出不变（以现有测试为基线）

#### Scenario: S2 多 DB/SELECT 路由仍正常工作
<a id="r1-s2"></a>
条件：连接级 session 切换 DB index（`SELECT`），执行读写命令。
- 期望：路由通过 `DbEngineRouter`（由 session 决定）选择目标 DB
- 期望：`INFO/HELLO/STATS` 等与连接态相关逻辑不受影响

### Requirement: R2 写入选项与异常边界 ops 化（移除嵌套类型泄漏）
<a id="r2"></a>
**Module:** core(command) / core(ops) / core(db)

将 `SetMode/ExpireOption/异常类型` 从 `YierdisDb` 的嵌套类型迁移到 `ops`，让 command 只依赖稳定类型。

#### Scenario: S1 SET 的 NX/XX/KEEPTTL/过期选项语义不回归
<a id="r2-s1"></a>
条件：组合测试 `SET key value [NX|XX] [KEEPTTL|EX|PX|EXAT|PXAT]`。
- 期望：语义与错误风格保持一致（以现有命令层约定为 SSOT）
- 期望：maxmemory preflight 顺序保持不变（避免双 reply）

#### Scenario: S2 WRONGTYPE/ERR 映射保持一致
<a id="r2-s2"></a>
条件：对错误类型 key 执行操作或触发参数错误。
- 期望：命令层仍输出 `WRONGTYPE ...` / `ERR ...`，不泄漏内部实现细节

### Requirement: R3 聚合读 streaming API 去 reply 语义（命名与形态治理）
<a id="r3"></a>
**Module:** core(ops) / core(db) / protocol

将 `*ReplyCount/*ReplyInto` 的命名与形态从“回包语义”迁移为“数据语义”，并维持 value 层只依赖 `ReplySink` 的窄接口。

#### Scenario: S1 LRANGE/SMEMBERS/HGETALL/ZRANGE 仍可低分配写出
<a id="r3-s1"></a>
条件：集合类结果较大，且 hot path 不应构建中间 `List<byte[]>`。
- 期望：命令层仍可 streaming 写出结果（必要时允许双遍历，但不得引入不可控分配）
- 期望：reply shape（array/map header）仍由命令层决定

#### Scenario: S2 value 层不感知 ReplyWriter 的容器语义
<a id="r3-s2"></a>
条件：value 层仅接收 `ReplySink` 或等价的“bulk value sink”。
- 期望：value 层不调用 `arrayHeader/mapHeader/error` 等 reply shape API
- 期望：未来替换协议时，仅需替换 writer/factory，不需要改 value 实现

### Requirement: R4 执行器协议解耦（ReplyWriterFactory 注入）
<a id="r4"></a>
**Module:** server / protocol

executor 只负责调度、背压与 flush 合并，不直接依赖某个协议的具体 writer 实现类。

#### Scenario: S1 `NettyCommandExecutor` 不再直接构造 `JsonLineReplyWriter`
<a id="r4-s1"></a>
条件：executor 写回响应时通过 factory 获取 `ReplyWriter`。
- 期望：Custom Protocol v1 行为不变
- 期望：未来可在不改 executor 的情况下替换协议 writer

#### Scenario: S2 reject/error 路径与正常路径复用同一 writer 工厂
<a id="r4-s2"></a>
条件：队列满（`ERR busy`）/内部错误（`ERR internal error`）/正常执行返回。
- 期望：回包路径一致、编码一致，避免 “协议细节散落在各处”

## Risk Assessment

- **Risk:** 内部 API 破坏导致联动改动多、回归范围大  
  - **Mitigation:** 以“边界先行”的阶段化迁移推进；每个阶段都可独立编译与回归（`mvn test` + `rg` 约束）
- **Risk:** 语义回归（错误风格、空值语义、事务边界、集合 shape）  
  - **Mitigation:** 以命令层为 SSOT；为关键组合场景补齐测试锚点（SET 选项、聚合读 streaming、busy/backpressure）
- **Risk:** 性能回归（额外分配、双遍历、adapter 过多间接调用）  
  - **Mitigation:** 允许引入新抽象但必须给出分配/性能解释；对热点路径做 A/B（bench）并在接口设计上提供零分配实现路径
