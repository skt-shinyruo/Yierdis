<!-- migrated_from: history/2026-01/202601231314_resp3_glob_hash_refactor/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: RESP3 友好化 + KEYS Glob 对齐 + Hash(off-heap) 编码对齐 + 写入语义修复

## Requirement Background

当前项目已实现 RESP2/部分 RESP3（`HELLO 3` 切换 + RESP3 null），并实现了若干 Redis 核心命令与数据结构（String/List/Hash/Set/ZSet/TTL/maxmemory 等）。
但在“已实现功能范围内”仍存在以下关键问题，影响兼容性、正确性与稳定性：

1. **P0：写命令可能产生双 reply（协议损坏）**
   - 若写命令在写出 reply 后再执行 `enforceMaxmemory()`，一旦 `enforceMaxmemory()` 抛错，会导致同一个命令输出“正常 reply + error reply”两条响应，破坏 RESP 流。
   - 并且该错误可能发生在写入已落地之后，造成“返回错误但数据已写入”的语义偏差。

2. **P1：Hash(off-heap) 编码策略与 Redis 不一致，且存在死分支风险**
   - 目标行为应为：小 hash 使用 packed（listpack-like）编码，达到阈值或元素过大时升级为 hashtable（dict）。
   - 现状：off-heap hash 直接使用 dict，packed 分支不可达，导致编码行为与注释/预期不一致，并增加未来维护风险。

3. **P2：RESP3 兼容范围偏窄，KEYS glob 语义不完整**
   - 目标：在已支持 `HELLO 3` 的基础上，让集合类返回更 RESP3 友好（map/set 等），并将 `KEYS` 的 glob 语义补齐至 Redis 级别（`[]`/范围/否定/转义等）。

## Change Content

1. 统一写命令执行路径：引入“写入 preflight / 预淘汰 / 预检查”并保证**任何可能抛错的 maxmemory/淘汰逻辑都发生在写 reply 之前**。
2. Hash(off-heap) 改为“packed 起步 → dict 升级”，并修正升级触发顺序（避免仅更新已有 field 时无谓升级）。
3. 扩展 RESP3 输出能力：
   - `HGETALL`：RESP3 下返回 map（field -> value）
   - `MEMORY STATS`：RESP3 下返回 map（key -> value）
   - `SMEMBERS`：RESP3 下返回 set
4. `KEYS` glob：实现 Redis 风格 glob（`*`/`?`/`[]`/范围/否定/`\\` 转义），保持二进制安全（按 raw bytes 匹配）。
5. 补齐测试覆盖：覆盖 P0 双 reply、P1 off-heap hash packed→dict 升级、P2 RESP3 输出与 KEYS glob 行为。

## Impact Scope

- **Modules:**
  - `yierdis-core`（命令层、DB、HashValue、KEYS glob）
  - `yierdis-protocol`（RESP3 set/map 类型建模与解析/输出）
  - `yierdis-server`（RESP3 端到端管线测试）
- **Files (expected hot spots):**
  - `yierdis-core/src/main/java/yier/bubu/redis/command/*Commands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/HashValue.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespType.java` / 新增 `RespSet`
  - `yierdis-server/src/test/java/yier/bubu/redis/*`（新增/扩展测试）

## Core Scenarios

### Requirement: P0 写命令回复与 maxmemory 语义对齐
**Module:** `yierdis-core` / `yierdis-server`

#### Scenario: 写命令在 maxmemory 压力下不产生双 reply
在 maxmemory 约束触发错误时：
- 仅返回单条 `-ERR ...` 响应
- 不出现“正常 reply + error reply”的协议损坏

#### Scenario: 写入前进行更强的预淘汰/预检查，减少“报错但已写入”
在采用淘汰策略（`allkeys-random`/`allkeys-lru`）时：
- 在执行具体写入前尽可能先清理过期键并进行预淘汰，为写入留出空间
- 使“写入后才报 OOM”的概率显著降低

### Requirement: P1 Hash(off-heap) 编码策略对齐 Redis
**Module:** `yierdis-core`

#### Scenario: off-heap Hash 默认为 packed，并在阈值/oversize 时升级到 dict
- 小 hash：`OBJECT ENCODING` 为 `listpack`
- 超过阈值/元素过大：升级后 `OBJECT ENCODING` 为 `hashtable`

#### Scenario: 更新已有 field 不应触发无谓升级
- 仅更新 value（不新增 entry）不应因为“已达阈值”而强制升级

### Requirement: P2 RESP3 输出友好化与 KEYS glob 对齐
**Module:** `yierdis-protocol` / `yierdis-core`

#### Scenario: RESP3 下集合类返回 map/set
- `HGETALL` → RESP3 map
- `MEMORY STATS` → RESP3 map
- `SMEMBERS` → RESP3 set

#### Scenario: `KEYS` glob 支持 `[]` 与转义
- 支持 `[abc]` / `[a-z]` / `[^...]`（或等价否定）
- 支持 `\\*`、`\\?`、`\\[` 等转义，按 raw bytes 匹配

## Risk Assessment

- **Risk:** RESP3 输出类型变化可能影响部分客户端的解码路径（例如期待数组而非 map/set）。
  - **Mitigation:** 仅在连接协议为 RESP3 时启用 map/set；RESP2 行为保持不变；新增端到端字节级测试确保输出稳定。
- **Risk:** glob 语义更完整后可能引入性能退化（尤其是复杂模式）。
  - **Mitigation:** 使用非递归线性回溯算法（与 Redis stringmatch 思路一致），并限制解析复杂度，保证不会出现指数级回溯。
- **Risk:** Hash(off-heap) 变更涉及内存管理（SDS 分配/释放），存在泄漏风险。
  - **Mitigation:** 对关键路径增加异常安全释放；加入 off-heap 升级测试覆盖与基本内存行为验证。

