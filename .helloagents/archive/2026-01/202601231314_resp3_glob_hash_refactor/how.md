<!-- migrated_from: history/2026-01/202601231314_resp3_glob_hash_refactor/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Technical Design: RESP3 友好化 + KEYS Glob 对齐 + Hash(off-heap) 编码对齐 + 写入语义修复

## Technical Solution

### Core Technologies
- Java 17 / Maven 多模块
- Netty pipeline（`yierdis-protocol-netty` 解码 + `yierdis-server` 执行器）
- RESP2/RESP3（`HELLO 3` 协商后切换）

### Implementation Key Points

#### P0：写命令“preflight → 执行 → 再写 reply”的统一框架
1. 新增 DB 写入 preflight API（建议命名：`prepareWrite(estimatedExtraBytes)` 或等价）：
   - 在执行具体写入前：
     - `cleanupExpired()`（在压力下优先清过期）
     - 若启用淘汰策略（`allkeys-random` / `allkeys-lru`），则按预算进行预淘汰，尽量为 `estimatedExtraBytes` 预留空间
     - 若 `noeviction`，则严格拒绝写入（保持 Redis 语义）
2. 所有写命令调整顺序：
   - 先调用 DB preflight（包含更强预淘汰/预检查）
   - 执行写入（DB 更新）
   - 再做必要的维护（如轻量 cleanup / non-throwing 的 best-effort enforce）
   - 最后写 reply（避免双 reply）

> 说明：由于不同数据结构的结构性开销差异较大，`estimatedExtraBytes` 需要按命令类型做“保守上界估算”，避免预留不足导致写入后才 OOM 的概率过高。

#### P1：Hash(off-heap) 编码对齐 Redis
1. off-heap `HashValue` 初始化为 packed(listpack-like)：
   - 使用 `YierdisUnsafeOffHeapListpack` 保存 `[field][value]` 对（二进制安全）
2. 升级到 dict：
   - 触发条件：新增 entry 导致超过 `hash-max-listpack-entries` 或 field/value 超过 `hash-max-listpack-value`
   - 修正升级触发顺序：优先判定是否为“更新已有 field”，避免“仅更新也升级”
3. dict 存储：
   - dict value 为 `YierdisUnsafeOffHeapSds` 地址（`0L` 表示 null）
   - 加强异常安全：`dict.put` 失败时必须释放已分配的 SDS，避免泄漏

#### P2：RESP3 友好输出 + KEYS glob 全量兼容
1. RESP3 set 类型：
   - 在 `yierdis-protocol` 中新增 `RespSet`（对象模型）与 `RespType.SET`
   - 扩展 `RespWriter`：
     - 新增 `setHeader(int count)`：RESP3 下输出 `~count\r\n`
   - 扩展 `RespObjectParser`：
     - 支持解析 `~` set
2. 命令输出按连接协议分流：
   - `HGETALL`：
     - RESP2：保持数组（field,value,field,value,...）
     - RESP3：输出 map（field -> value）
   - `MEMORY STATS`：
     - RESP2：保持平铺数组
     - RESP3：输出 map（key -> value）
   - `SMEMBERS`：
     - RESP2：保持数组
     - RESP3：输出 set（~count + 元素）
3. `KEYS` glob：
   - 在 DB 层实现 Redis 风格 `stringmatch` 语义（byte 级匹配）：
     - `*` 任意长度
     - `?` 单字节
     - `[]` 字符集合、范围（`a-z`）、否定（`^`/`!`）
     - `\\` 转义特殊字符

## Architecture Design

```mermaid
flowchart LR
    A[RespCommandDecoder] --> B[YierdisFastCommandProcessor]
    B --> C[Write Preflight<br/>cleanup+pre-evict]
    C --> D[YierdisDb mutation]
    D --> E[RespWriter encode reply<br/>(RESP2/RESP3)]
```

## Architecture Decision ADR

### ADR-001: 引入统一写入 preflight/commit 路径以避免双 reply，并降低 OOM 后副作用
**Context:** 写命令在 reply 写出后再做 maxmemory enforcement 会导致双 reply；同时 OOM 错误可能发生在写入已落地之后。  
**Decision:** 将所有写命令统一为“preflight（含预淘汰/预检查）→ 执行写入 → 再写 reply”。  
**Rationale:**  
- 保证任何可能抛错的步骤发生在 reply 之前，从根源避免双 reply。  
- 通过预淘汰/预留空间降低“报错但已写入”的概率。  
**Alternatives:** 仅在命令层调整 reply 顺序（拒绝原因：仍可能产生写入后报错的语义偏差且无法系统性收敛）。  
**Impact:** 命令层与 DB 层需要新增一套可复用的写入 preflight API，并更新所有写命令调用点。

### ADR-002: 扩展 RESP3 类型建模，集合类在 RESP3 下返回 map/set
**Context:** 当前 RESP3 仅覆盖 `HELLO 3` 与 null；集合类仍返回 RESP2 数组形态，不够“RESP3 友好”。  
**Decision:** 增加 `RespType.SET`/`RespSet` 与 `RespWriter.setHeader`；在 RESP3 下对 `HGETALL`/`MEMORY STATS`/`SMEMBERS` 使用 map/set。  
**Rationale:** 提升 RESP3 客户端的结构化解码体验，且在 RESP2 下保持兼容。  
**Alternatives:** 全部继续返回数组（拒绝原因：与“RESP3 友好化”目标冲突）。  
**Impact:** 协议库与部分命令实现需要升级；新增端到端字节级测试固定输出。

### ADR-003: off-heap Hash 与 Redis 编码策略对齐（packed 起步，按阈值升级 dict）
**Context:** off-heap hash 直接 dict 起步与 Redis 不一致，且 packed 分支变成死代码。  
**Decision:** off-heap hash 初始化为 packed（off-heap listpack），超过阈值/oversize 再升级 dict。  
**Rationale:** 与 Redis 行为一致，且使 packed 逻辑真实可用并降低死分支维护风险。  
**Impact:** `HashValue` 需要重构初始化与升级判断顺序，并加强 SDS 分配的异常安全。

## Security and Performance
- **Security:**
  - glob 解析严格按字节处理，避免按 UTF-8 解码产生歧义；对不完整 `[]` 结构采取兼容处理，避免越界。
  - 继续保持错误信息的 CRLF 清洗（已在 `RespWriter.error` 中实现）。
- **Performance:**
  - glob 使用非递归实现，避免深递归与不可控栈增长。
  - preflight 的预淘汰遵循既有时间预算（避免长尾阻塞）。
  - off-heap hash 使用 packed 编码减少小 hash 的 dict 开销。

## Testing and Deployment
- **Testing:**
  - P0：新增 maxmemory 场景用例，确保错误时不会产生双 reply（并覆盖“结构开销导致的预估不足”边界）
  - P1：新增/扩展 hash encoding 测试，覆盖 off-heap packed→dict 升级与更新不触发升级
  - P2：新增 server 端到端测试：`HELLO 3` 后 `HGETALL`/`MEMORY STATS`/`SMEMBERS` 的 RESP3 字节输出；新增 KEYS glob `[]`/转义匹配测试
- **Deployment:**
  - 无额外部署步骤；通过 `mvn test` 回归验证。

