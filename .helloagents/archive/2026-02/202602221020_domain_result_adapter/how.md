<!-- migrated_from: history/2026-02/202602221020_domain_result_adapter/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: core 分层解耦（domain result → adapter，移除 db/数据结构对 ReplySink 的依赖）

## Technical Solution

### Core Technologies
- Java 17 / Maven multi-module
- 中立 bytes 抽象：`yierdis-bytes`（`BytesSlice/BytesSink`）
- 协议无关 reply 端口：`yierdis-protocol-model`（`ReplyWriter`）

### Implementation Key Points

1. **Domain Result（SSOT）**
   - 在 `yierdis-core` 定义协议无关的结果模型（domain result），用于表达“可流式消费的 bytes 值序列/映射/标量”：
     - 标量：可表示 `null`、heap bytes slice（`byte[] + off/len`）、`BytesSlice`（支持 off-heap）、以及 `long` 的 ASCII 表示（避免额外分配）。
     - 聚合：提供 `count()` / `pairCount()` 与 `emitTo(...)`，命令层据此写出 aggregate header 后再 streaming 消费。
   - 约束：result 必须 **同步消费**，不得跨线程/跨命令保存引用（尤其是 off-heap slice）。

2. **Adapter（边界层）**
   - 命令层实现 adapter：将 domain result 写入 `ReplyWriter`（调用 `ReplyWriter`/`ReplySink` 的 bulk string 写出能力）。
   - 该 adapter 是唯一触达 `yier.bubu.redis.protocol.*` 的位置（除命令层本身外），db/value/off-heap 不再 import 协议类型。

3. **迁移策略（渐进、可回归）**
   - 先引入 domain result + adapter 与测试护栏，再逐个迁移 `String/List/Set/Hash/ZSet` 的 streaming 读路径。
   - 每次迁移保持既有命令行为（header 计算、null 语义、WITHSCORES count）不变。

## Architecture Design

```mermaid
flowchart TD
    C[Command Handlers] -->|调用 ops| O[*Ops (domain API)]
    O --> D[YierdisDbValueOps / db value]
    D --> R[Domain Result]
    C -->|adapter| A[Domain→ReplyWriter Adapter]
    A --> W[ReplyWriter]
    W --> P[protocol-codec / transport]
```

## Architecture Decision ADR

### ADR-20260222-01: 用 domain result 替代 ReplySink 下沉依赖
**Context:** db/value/off-heap 直接依赖 `ReplySink`，使存储实现与协议端口耦合，并且 off-heap 结构对命令异常类型存在依赖。  
**Decision:** storage/ops 返回 domain result（可 streaming 消费），命令层通过 adapter 将其写入 `ReplyWriter`；同时通过测试护栏阻止 db/off-heap 再次 import `yier.bubu.redis.protocol.*`。  
**Rationale:**
- 使 storage/算法可独立单测与复用（无需构造协议 writer/sink）。
- 将协议/编码细节收敛到边界层，降低未来协议替换成本。
- domain result 仍可保持低分配/少拷贝（使用 `BytesSlice` 与同步消费约束）。
**Alternatives:**
- 将 `ReplySink` 迁移为 core Port（最小改动） → 拒绝原因：仍把 storage API 设计绑定在“回包写出语义”，且无法表达更通用的结果消费方式（iterator/collector/test double）。
**Impact:**
- `*Ops` 接口签名变化，命令层调用与 db/value 实现需要联动迁移。
- 需要新增架构护栏与回归测试，确保 count/header/语义不回归。

## Security and Performance

- **Security:**
  - domain result/adapter 必须同步写出、不得缓存 `BytesSlice` 引用，避免 off-heap 越界/泄漏。
  - 错误 message 仍由命令层统一输出，确保 CR/LF 净化与长度限制策略不弱化（以现有 SSOT 为准）。
- **Performance:**
  - 结果模型设计为：每条命令最多分配常数个 result 对象/adapter，对元素写出不引入 per-element 对象分配。
  - 对 `GET` 的 off-heap slice 路径保持零拷贝；对 `WITHSCORES` 的 score 编码保持与现状相当的分配特性。

## Testing and Deployment

- **Testing:**
  - 增加架构边界回归测试：扫描 `yier.bubu.redis.db.*` 与 `yier.bubu.redis.db.offheap.*` 源码，禁止 import `yier.bubu.redis.protocol.*`。
  - 增量补齐核心场景测试：GET（null/int/off-heap）、LRANGE、SMEMBERS、HGETALL、ZRANGE/ZRANGEBYSCORE（WITHSCORES）。
- **Deployment:**
  - 无外部部署变更；仅内部接口与实现调整，确保 `mvn test` 通过后合入。

