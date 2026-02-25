# 变更提案: change_event_real_change

## 元信息
```yaml
类型: 重构/优化
方案类型: implementation
优先级: P0
状态: 草稿
创建: 2026-02-25
```

---

## 1. 需求

### 背景
当前变更事件（`YierdisChangeEvent(dbIndex, argv)`）的发射条件由命令层硬编码名单决定：`yierdis-core-command/.../YierdisFastCommandProcessor#isWriteCommand`。

这会产生两类架构问题（“漂移 + 虚假/缺失事件”），并直接阻塞后续 AOF/replication/审计等能力的可信接入：

1) **漏发（false negative）**：新增/改动写命令时，若忘记更新名单，则该命令永远没有 change event（对外扩展会静默失真）。

2) **误发（false positive）**：命令被列为“写”，但本次执行并未产生任何 Keyspace/Value/TTL 的真实变化时（例如 `SET ... NX` 未写入、`DEL` 删除 0 个 key、`PERSIST` 没有 TTL、`EXPIRE` key 不存在），仍会发射事件，导致事件流与实际状态不一致。

本提案选择把“最严重问题”定义为：**change event 触发机制不可维护、不可验证、容易漂移**。

### 目标
在不改变对外事件模型的前提下（仍是 argv 可重放的 `YierdisChangeEvent`），将发射条件升级为“真实变更(Keyspace/Value + TTL 元数据) 才 emit”，使事件流成为可依赖的生产扩展护栏。

具体目标：
- 移除 `isWriteCommand` 作为 change event 的 SSOT（不再通过手工名单决定 emit）。
- 命令执行成功后，仅当本次命令 **实际导致** Keyspace/Value/TTL 元数据发生变更时才 emit。
- 背景维护任务（过期清理/淘汰）**不产生日志事件**（本轮不纳入范围，保持现状）。

### 约束条件
```yaml
时间约束: 无（按可回归、可验证优先）
性能约束: 单命令新增开销应接近 0 分配（仅重置几个 flag / thread-local 读取）；热路径不引入额外 copy/遍历
兼容性约束:
  - 对外事件模型保持不变: YierdisChangeEvent(dbIndex, argv)
  - 事件触发仍是 best-effort：sink 消费失败不影响命令执行
  - 仍仅在“命令执行成功路径” emit（命令报错不 emit）
业务约束: 教学/演示优先，但需要为未来 AOF/replication/审计提供稳定契约
```

### 验收标准
- [ ] 删除 `YierdisFastCommandProcessor#isWriteCommand`（或不再用于事件发射）且新增命令不需要维护“写命令名单”。
- [ ] 以下场景不再产生虚假事件：`SET k v NX` 未写入、`DEL missing` 删除 0 个 key、`PERSIST` 无 TTL、`EXPIRE missing`。
- [ ] 以下场景必须 emit：`SET` 成功写入、`DEL` 实际删除、`EXPIRE/PERSIST` 实际改动 TTL、事务 `EXEC` 中真实写入的命令。
- [ ] 背景维护（过期清理/淘汰）不产生 change event 且不会污染“本次命令是否变更”的判定。
- [ ] 新增/更新单测覆盖上述边界，并通过 `mvn test`。

---

## 2. 方案

### 技术方案（推荐）
采用“**命令作用域变更追踪（ChangeScope）+ DB/ops 层真实变更标记**”的方式：

1) 在命令执行开始时（`YierdisFastCommandProcessor.execute` 入口）开启一个 **command-scope** 的变更追踪上下文（建议为 thread-local 复用结构，避免分配）。
2) DB/ops 层在真正发生 Keyspace/Value/TTL 元数据变更时调用 `markValueChanged()` / `markTtlChanged()`。
3) 命令执行成功返回后，命令层读取追踪上下文的结果：若本次命令 scope 内存在任何 value/ttl 变更，则 emit `YierdisChangeEvent(dbIndex, argv)`；否则不 emit。

该方案将 “是否 emit” 的依据从 **命令名名单（易漂移）** 下沉为 **真实变更事实（更接近 SSOT）**，同时仍保持 event payload 为 argv（可重放）。

### 备选方案（不推荐，但需记录）
| 方案 | 描述 | 优点 | 缺点（关键） |
|------|------|------|--------------|
| B. 命令层 ChangeOutcome 上报 | 每个命令 handler 返回/写入 “本次是否修改 value/ttl” | 实现直观，基本不动 DB | 漂移风险仍然存在（新增命令容易忘记上报）；更接近“手工名单”的另一种形态 |
| C. DbEngine 代理/装饰器 | `CommandSupport.db(ctx)` 返回代理 DbEngine，代理捕捉实际写入调用并汇总 | 逻辑集中，命令层可无感 | 代理覆盖面大且易引入性能/行为漂移；需要保证代理与原生 DB 语义完全一致，风险高 |

### 影响范围
```yaml
涉及模块:
  - yierdis-core-api: 增加变更追踪的最小内部契约（thread-local scope + mark API）
  - yierdis-core-command: 重构事件发射判定（移除 isWriteCommand），对接 ChangeScope
  - yierdis-core-db: 在真实写入/删 key/TTL 改动处标记 value/ttl 变更
  - yierdis-core(测试): 更新/新增回归测试（验证 no-op 不 emit、真实变更 emit、事务 EXEC 场景）
预计变更文件: 8-20（取决于 DB 写入点的集中程度与覆盖的数据类型范围）
```

### 风险评估
| 风险 | 等级 | 应对 |
|------|------|------|
| DB 写入点漏标记导致漏发事件 | 高 | 以“集中 hook 点 + 回归测试矩阵”降低漏标概率；先覆盖 Keyspace/TTL/核心写命令，再逐步补齐类型写入点 |
| 变更追踪上下文泄漏（early return 未关闭 scope） | 高 | ChangeScope 必须由 try/finally/try-with-resources 管理；覆盖 MULTI 入队 early return、异常路径的测试 |
| 误把维护任务变更计入命令变更 | 中 | mark API 在无 active scope 时必须 no-op；维护任务不打开 scope |
| 性能退化（每次命令额外开销） | 中 | scope 设计为 thread-local 复用，仅重置 flag；避免 per-command 分配 |
| 语义边界争议（例如“SET 相同值算不算变更”） | 低 | 本轮定义“变更=effectful mutate 成功提交”，以命令返回的成功条件/写入路径为准（不做昂贵的值相等比较） |

---

## 3. 技术设计（可选）

### 架构设计
```mermaid
flowchart TD
    A[NettyCommandExecutor/owner thread] --> B[YierdisFastCommandProcessor.execute]
    B --> C[ChangeScope.begin (thread-local)]
    B --> D[CommandHandler.execute]
    D --> E[DbEngine ops / YierdisDb*Ops]
    E --> F[YierdisDb (真实写入/DEL/TTL)]
    F --> G[ChangeScope.markValue/markTtl]
    D --> H[ReplyWriter 写出回复]
    B --> I{scope changed?}
    I -- yes --> J[changeSink.onChange(YierdisChangeEvent(dbIndex, argv))]
    I -- no --> K[不发射事件]
    B --> L[ChangeScope.close/clear]
```

### API设计
该方案不改变对外事件模型，仅新增最小内部 API（示例命名，最终以实现为准）：

- `yier.bubu.redis.runtime.api.YierdisChangeTracking`（core-api）
  - `beginScope(): AutoCloseable`（重置并激活 thread-local scope）
  - `markValueChanged()` / `markTtlChanged()`（无 active scope 时 no-op）
  - `changedValue()` / `changedTtl()` / `changedAny()`（供命令层读取）

> 注：scope/ThreadLocal 仅依赖“DB 单线程语义”，并不引入 Netty 依赖，满足 core-api boundary。

### 数据模型
无新增持久化数据模型；事件载荷保持 `argv`，不引入 key-level 结构化 event（本轮约束）。

---

## 4. 核心场景

### 场景: SET NX 未写入（不应 emit）
**模块**: core-command + core-db  
**条件**: key 已存在，执行 `SET k v NX`  
**行为**: 命令返回未写入（如返回 `null` 或 `0`/错误语义以实现为准）  
**结果**: ChangeScope 未标记 value/ttl 变更，事件不发射

### 场景: DEL 删除 0 个 key（不应 emit）
**模块**: core-db  
**条件**: keys 均不存在或均已过期  
**行为**: `del` 返回 0  
**结果**: ChangeScope 不标记 value 变更，事件不发射

### 场景: EXPIRE / PERSIST 真实修改 TTL（必须 emit）
**模块**: core-db  
**条件**: key 存在且 TTL 发生变化（设置或移除）  
**行为**: `expire/persist` 返回 true  
**结果**: ChangeScope 标记 ttl 变更，事件发射

### 场景: MULTI/EXEC（仅对 EXEC 内真实写入 emit）
**模块**: core-command  
**条件**: MULTI 模式下入队写命令与读命令  
**行为**: 入队阶段不应 emit；EXEC 重放时每条命令独立 scope 判定  
**结果**: 仅对真实写入的命令 emit；读命令不 emit

---

## 5. 技术决策

### change_event_real_change#D001: 变更事件触发点从“命令名单”迁移到“真实变更事实”
**日期**: 2026-02-25  
**状态**: ✅采纳  
**背景**: `isWriteCommand` 手工名单会导致事件漂移（漏发/误发），阻塞 AOF/replication/审计等扩展能力接入。  
**选项分析**:
| 选项 | 优点 | 缺点 |
|------|------|------|
| A: DB/ops 层 ChangeScope（推荐） | 更接近 SSOT；移除命令名单；只要 DB 写入点统一标记即可；新增命令无需维护名单 | 需要在 DB 写入点补齐标记，初始改动面较大 |
| B: 命令层 ChangeOutcome | 直观、改动局部 | 仍需每个命令维护“是否变更”判断，漂移风险仍高 |
| C: DbEngine 代理 | 命令层最少侵入 | 性能/行为漂移风险高，实现复杂度大 |
**决策**: 选择方案 A  
**理由**: 将 emit 的依据下沉为真实变更事实，减少人为维护点，长期最稳。  
**影响**: `yierdis-core-api`/`yierdis-core-command`/`yierdis-core-db`/测试与知识库文档。  
