# Change Proposal: DB/Executor 边界解耦 v2（移除 server→YierdisDb 直接依赖 + 持续拆分巨型类）

## Requirement Background

当前实现已经做过一轮分层治理（例如引入 `DbEngine/ValueOps/*Ops`、将 bulk-string streaming 写出收敛为窄接口 `ReplySink`、将 executor 的 backlog/backpressure 拆成独立组件等），整体方向是正确的；但从代码可维护性与长期演进风险看，仍存在两个高耦合热点：

1. **`YierdisDb` 仍是超大实现类（约 2.8k 行）**  
   其内部同时承载 keyspace/TTL/expire cleanup/maxmemory/eviction/memory ledger/部分语义异常类型等；虽然对外 API 已较为克制，但单文件过大导致：
   - 修改半径大：任何行为修正都需要触达同一文件的大段逻辑；
   - 语义边界不清：过期/淘汰/内存记账/生命周期绑定等细节互相渗透；
   - 后续引入持久化/复制/多协议适配时，演进成本偏高。

2. **server 侧仍存在对具体 DB 实现 `YierdisDb` 的直接依赖**  
   典型模式是：`NettyCommandExecutor` 的构造函数直接接收 `YierdisDb`，仅用于 `db::bindToCurrentThread` 的绑定动作；同时 `YierdisServerBootstrap/NettyServerInfoProvider` 也持有 `YierdisDb[]`。这会导致：
   - server 与具体 DB 实现耦合，难以替换 DB（例如多实现、代理、mock、持久化版）；
   - “执行器/回包/调度策略”与“DB 线程绑定语义”混在一起，边界不清晰；
   - 单测往往只能用 `new YierdisDb()` 走真实实现，难以在边界处做隔离测试。

此外，value 实现里仍有少量 **`ReplyCount/ReplyInto` 的残留命名（例如 `ZSetValue` 的内部 helper）**，虽然不一定暴露为对外 API，但会持续暗示“存储层对 reply 形状负责”，不利于长期演进。

---

## Change Content

1. **彻底移除 server→`YierdisDb` 的直接依赖：**  
   - server/executor 侧只依赖“线程绑定动作”（`Runnable bindToCurrentThread`）与“命令处理能力”（`YierdisFastCommandProcessor` 或更窄接口）；  
   - DB 实现细节仅存在于 core/runtime（例如 `YierdisInstance` 内部装配），server 不再显式引用 `YierdisDb`。

2. **将 executor 的“胶水类”进一步组件化：**  
   保持单线程执行语义不变，但将 submission/drain/reply flush/close-after-reply/backpressure 等职责拆为可独立测试的组件或配置对象，减少 `NettyCommandExecutor` 单文件复杂度与参数爆炸。

3. **持续拆分 `YierdisDb` 与清理剩余的 reply 命名渗透：**  
   - 将内部的枚举/异常/ledger 等稳定概念迁移到 `ops` 或 `db` 子组件，进一步缩小 `YierdisDb`；  
   - 将 `ZSetValue` 等实现中的 `Reply*` 内部 helper 命名迁移为数据语义（`*Count/*WriteTo` 或 iterator/cursor 形式）。

4. **兼容性要求：**  
   - 现有对外可见行为与协议语义保持不变；  
   - 如确需 API 变更，提供过渡期（deprecated + adapter），避免一次性大爆炸改动。

---

## Impact Scope

- **Modules:**
  - `yierdis-server`（executor/bootstrap/info provider 解耦与组件化）
  - `yierdis-core`（进一步拆分 `YierdisDb`、暴露更窄的边界能力）
  - `yierdis-protocol`（仅在需要调整 streaming 接口位置/命名时涉及）
  - `yierdis-bytes`（仅当需要将 streaming sink 下沉为更中立抽象时涉及）

- **Files (expected):**
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/ZSetValue.java`

- **APIs:**
  - `NettyCommandExecutor` 构造函数：移除/弃用 `YierdisDb` 入参的 overload，仅保留 `Runnable bindToCurrentThread` 形式
  - server 的 DB 绑定：通过 `YierdisInstance#bindToCurrentThread` 或显式 binder 传入

- **Data:** 无

---

## Core Scenarios

### Requirement: server 层不直接依赖具体 DB 实现
**Module:** server/core

#### Scenario: 启动 server 并执行命令
在不显式引用 `YierdisDb` 的前提下完成：
- server 启动与 executor 线程绑定；
- 命令入队、串行执行、回包写出；
- close-after-reply 行为保持（flush 后关闭连接、并跳过后续命令）。

#### Scenario: embedded/测试场景替换 DB 实现
在测试或 embedded 场景中，能够通过 `DbEngine`/router/processor 的替换或装配，避免 server 直接 `new YierdisDb()`。

### Requirement: executor 职责边界清晰、可独立测试
**Module:** server

#### Scenario: 全局/连接级背压进入与退出
背压逻辑（预算、水位线、autoRead 开关）可以在组件层级单测覆盖，减少对大 executor 的集成式断言。

#### Scenario: 队列预算与公平调度
在 backlog 受限与多连接竞争时，队列预算与调度策略可替换/可测试，避免在单一巨型类中不断堆叠“特判”。

### Requirement: DB/value 层 API 语义收敛（去 reply 语义）
**Module:** core/db

#### Scenario: streaming bulk-string 值写出
存储层只表达“值的写出”，不表达“reply shape（array/map/header/error）”；命名与形态使用数据语义（`*Count/*WriteTo`、cursor/forEach 等）。

---

## Risk Assessment

- **Risk:** 重构引入行为回归（特别是 close-after-reply、背压水位线滞回、队列预算等）  
  **Mitigation:** 为每个关键 contract 增补针对性回归测试（executor 单测 + server 集成测试），并尽量采用“先引入新入口、后迁移、最后删除旧入口”的渐进策略。

- **Risk:** 性能回退（hot path adapter/额外分配）  
  **Mitigation:** 保持窄接口与零/少分配原则；对关键路径（streaming bulk、队列 draining）使用 micro-bench 或基准用例做对比；引入指标统计与采样日志用于回归分析。

- **Risk:** 模块依赖方向被意外改变（产生循环依赖）  
  **Mitigation:** 依赖倒置优先使用已有的 `ops/*` 抽象；如需下沉接口，优先放在最中立的模块（如 `yierdis-bytes`），并用 adapter 维持兼容。

