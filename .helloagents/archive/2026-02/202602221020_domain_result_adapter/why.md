<!-- migrated_from: history/2026-02/202602221020_domain_result_adapter/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: core 分层解耦（domain result → adapter，移除 db/数据结构对 ReplySink 的依赖）

## Requirement Background

当前 core 的分层依赖方向被“写入式输出”打穿：存储/数据结构/业务（`yierdis-core` 的 `db/*Value`、`db/offheap/*`、`ops/*Ops`）直接依赖协议输出接口 `ReplySink`（位于 `yierdis-protocol-model`），导致：

- DB/算法实现与协议输出接口绑定，降低可测试性（单测难以隔离协议/回包）、可替换性（未来换协议/换 reply 编码器时牵连 storage）、可复用性（embedded/离线工具难复用 storage）。
- off-heap 数据结构还直接依赖“命令语义异常”（例如 `YierdisCommandException`），使得底层结构难以作为通用组件复用与验证。

代表性位置（非穷举）：
- `yierdis-core/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
- `yierdis-core/src/main/java/yier/bubu/redis/ops/StringOps.java`
- `yierdis-core/src/main/java/yier/bubu/redis/db/offheap/YierdisUnsafeOffHeapZSet.java`

## Change Content

1. **引入 domain result（协议无关的结果模型）并在命令层做 adapter：**
   - storage/数据结构层不再“写出到 ReplySink”，而是返回可同步消费的 domain result（支持 streaming，避免中间 `List<byte[]>` 物化）。
   - 命令层负责将 domain result 适配为 `ReplyWriter` 的写出（协议/编码细节保留在边界层）。
2. **迁移 streaming 读路径：**
   - `GET/LRANGE/SMEMBERS/HGETALL/ZRANGE/ZRANGEBYSCORE/...` 等读命令，使用 domain result + adapter 完成 header + streaming 输出。
3. **off-heap 结构异常解耦：**
   - off-heap 数据结构不再依赖 `YierdisCommandException`；改为抛出更中立的异常（例如 `IllegalArgumentException` 或 core 内的 domain error），由命令层统一映射为兼容的错误回复。
4. **增加架构护栏：**
   - 通过测试/规则（轻量、无三方依赖）约束 `yier.bubu.redis.db.*` / `yier.bubu.redis.db.offheap.*` 不得再 import `yier.bubu.redis.protocol.*`，防止回归。

## Impact Scope

- **Modules:**
  - `yierdis-core`（主要改动：ops/db/command）
  - `yierdis-protocol-model`（预计无需改动；仍作为命令层边界端口）
- **Files (representative):**
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/*Ops.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/{HashValue,ListValue,SetValue,ZSetValue}.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/{YierdisListpack}.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/offheap/{YierdisUnsafeOffHeapListpack,YierdisUnsafeOffHeapZSet}.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/*Commands.java`
  - `yierdis-core/src/test/java/yier/bubu/redis/*`（新增边界回归测试）
- **APIs:**
  - core 内部 API 允许调整（`*Ops` 接口签名会变更）
  - wire 协议与对外行为保持不变（仍由 `ReplyWriter` 语义驱动）
- **Data:** 无（纯内存结构与接口重构）

## Core Scenarios

### Requirement: 存储/算法层协议解耦（domain result → adapter）
**Module:** core(db/ops/command)

将 storage/value/off-heap 的输出从 “write-to `ReplySink`” 改为 “return domain result + command adapter”。

#### Scenario: GET 在 off-heap string 下仍保持低分配写出
条件：value 存储在 off-heap（`YierdisOffHeapSlice` 路径）。
- 期望：storage 返回 domain result（包含 slice view），命令层 adapter 直接写入 `ReplyWriter`，避免 heap copy。
- 期望：`null bulk string` / `STRING_INT` 的语义与现状一致。

#### Scenario: LRANGE / SMEMBERS / ZRANGE 等聚合读仍可 streaming 输出
条件：集合类结果量大。
- 期望：命令层仍先写出 aggregate header（array/map），再 streaming 输出元素/entry。
- 期望：避免构建中间 `List<byte[]>`（保持现有低分配路径）。
- 期望：db/value/off-heap 不再 import `yier.bubu.redis.protocol.*`。

### Requirement: off-heap 数据结构不依赖命令异常
**Module:** core(db/offheap)

#### Scenario: ZADD score 非法时错误语义保持一致
条件：score 不是合法 float（NaN/Inf/非法文本）。
- 期望：底层抛出中立异常（不依赖 `YierdisCommandException`），命令层映射为 `ERR value is not a valid float`（与现状一致）。

## Risk Assessment

- **Risk:** 性能回归（新增 result 对象/adapter、额外间接调用、double→string 编码位置变化）
  - **Mitigation:** domain result 设计为同步消费、无 per-element 分配；关键读路径（GET/LRANGE/SMEMBERS/ZRANGE）保留零/少拷贝写出；必要时在 bench 中做 A/B。
- **Risk:** 语义回归（null bulk string、map/array header count、WITHSCORES 计数）
  - **Mitigation:** 为上述核心场景补齐回归测试锚点；保持“命令层写出 shape”的 SSOT 不变。
- **Risk:** off-heap slice 生命周期被错误延长（泄漏/越界）
  - **Mitigation:** domain result/adapter 明确“同步写出、不得缓存引用”的约束，并在安全检查与测试中覆盖。

