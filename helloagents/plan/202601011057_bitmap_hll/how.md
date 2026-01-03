# 技术设计：BITMAP / HyperLogLog（复用 STRING）

## Technical Solution

### Core Technologies

- Java 17（不引入新依赖）
- 复用现有 `YierdisDb` 与 `YierdisObject` 的 STRING 存储路径
- 测试使用 JUnit 4

### Implementation Key Points

#### BITMAP（SETBIT/GETBIT/BITCOUNT）

- **存储类型**：仍为 `ValueType.STRING`（binary-safe）
- **bit 编号**：按 Redis 规则，bit 0 是第 0 字节的最高位（MSB）
- **扩容策略**：`SETBIT` 需要按 `offset/8 + 1` 扩容字符串长度，不足部分补 0
- **计数**：`BITCOUNT` 使用 popcount（可用预计算表）并支持可选 byte-range
- **maxmemory**：在可能扩容/创建时调用 `ensureWriteAllowed` 与 `enforceMaxmemory`

#### HLL（PFADD/PFCOUNT/PFMERGE）

- **存储类型**：仍为 `ValueType.STRING`，但内容包含 HLL header + 数据区，用 header 区分“普通 string”与“HLL string”
- **hash**：使用确定性 hash（避免依赖），并与 Redis 思路保持一致（目标是统计结果尽量接近）
- **寄存器**：采用 Redis 常用参数（`p=14`，寄存器数 16384，寄存器位宽 6）
- **双编码**：支持 sparse/dense 两种表示
  - sparse：适合小集合，减少内存占用（实现复杂，需严格校验与转换）
  - dense：适合大集合，操作简单、可快速合并
- **合并**：`PFMERGE` 对寄存器取 max（union），必要时将源 key 转为 dense 再合并

## Architecture Design

本变更不引入新模块，仅在现有 `command` 与 `db/offheap` 内补齐能力。

```mermaid
flowchart TD
    CP[CommandProcessor] --> DB[YierdisDb]
    DB --> OBJ[YierdisObject (STRING)]
    OBJ --> BIT[bitmap ops]
    OBJ --> HLL[hll ops]
```

## Architecture Decision ADR

### ADR-001: HLL 采用 sparse/dense 双编码并存储为 STRING

**Context:** 需求希望 HLL 结果尽量贴近 Redis，同时保持“复用 STRING”这一设计边界。  
**Decision:** 采用 header + 数据区的 HLL string 格式，并实现 sparse/dense 两种内部表示与互转。  
**Rationale:** 更贴近 Redis 的存储策略与行为，可在小集合场景下避免直接分配 dense 大块内存。  
**Alternatives:** 仅实现 dense（更简单） → 拒绝原因：与用户选择的“更贴近 Redis 存储”不一致。  
**Impact:** 实现复杂度显著上升，需要更多边界测试与编码校验逻辑。

## API Design（命令语法）

- `SETBIT key offset value`
- `GETBIT key offset`
- `BITCOUNT key [start end]`
- `PFADD key element [element ...]`
- `PFCOUNT key [key ...]`
- `PFMERGE destkey sourcekey [sourcekey ...]`

## Data Model（内部二进制格式）

### BITMAP

- 与 Redis 一致：bitmap 即 string bytes，本项目不额外引入 header。

### HLL

- 通过固定 magic header 区分普通 string 与 HLL string
- 数据区根据 encoding 分为 sparse/dense（并提供互转）
- 需要定义：magic、encoding 类型、寄存器参数（p）、校验字段（如长度/版本）

## Security and Performance

- **Security:** 严格校验 offset/start/end/value 参数，防止负数与超大分配；不持久化任何敏感信息
- **Performance:** BITCOUNT 使用 popcount；HLL 合并尽量走 dense；必要时做 lazy upgrade（sparse→dense）

## Testing and Deployment

- **Testing:** 新增 heap/off-heap 双路径的命令语义测试，覆盖 bit 顺序、range 规则、HLL 合并与计数
- **Deployment:** 仅需 `mvn test` 验证；运行 jar 后用 `redis-cli --resp2` 手工回归

