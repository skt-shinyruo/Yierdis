<!-- migrated_from: history/2026-01/202601231314_resp3_glob_hash_refactor/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: RESP3 友好化 + KEYS Glob 对齐 + Hash(off-heap) 编码对齐 + 写入语义修复

Directory: `helloagents/history/2026-01/202601231314_resp3_glob_hash_refactor/`

---

## 1. 写入语义与 maxmemory（P0）
- [√] 1.1 设计并实现 DB 写入 preflight API（预淘汰/预检查），在 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`，verify why.md#requirement-p0-写命令回复与-maxmemory-语义对齐-scenario-写命令在-maxmemory-压力下不产生双-reply
- [√] 1.2 统一调整写命令执行顺序（preflight → 执行 → enforce/维护 → reply），覆盖 `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java` 与 `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java`，verify why.md#requirement-p0-写命令回复与-maxmemory-语义对齐-scenario-写命令在-maxmemory-压力下不产生双-reply
- [√] 1.3 统一调整写命令执行顺序（同 1.2），覆盖 `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java` 与 `yierdis-core/src/main/java/yier/bubu/redis/command/ZSetCommands.java`，verify why.md#requirement-p0-写命令回复与-maxmemory-语义对齐-scenario-写命令在-maxmemory-压力下不产生双-reply
- [√] 1.4 统一调整写命令执行顺序（同 1.2），覆盖 `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java` 与 `yierdis-core/src/main/java/yier/bubu/redis/command/HllCommands.java`，verify why.md#requirement-p0-写命令回复与-maxmemory-语义对齐-scenario-写命令在-maxmemory-压力下不产生双-reply

## 2. Hash(off-heap) 编码对齐（P1）
- [√] 2.1 重构 off-heap `HashValue` 初始化为 packed，并实现 packed→dict 升级与异常安全，修改 `yierdis-core/src/main/java/yier/bubu/redis/db/HashValue.java`，verify why.md#requirement-p1-hashoff-heap-编码策略对齐-redis-scenario-off-heap-hash-默认为-packed并在阈值oversize-时升级到-dict
- [√] 2.2 修正升级触发顺序（先判定是否更新已有 field），避免无谓升级，修改 `yierdis-core/src/main/java/yier/bubu/redis/db/HashValue.java`，verify why.md#requirement-p1-hashoff-heap-编码策略对齐-redis-scenario-更新已有-field-不应触发无谓升级

## 3. RESP3 类型建模与输出（P2）
- [√] 3.1 扩展协议对象模型：新增 `RespType.SET` 与 `RespSet`，并扩展 `RespObjectParser` 支持 `~`，修改/新增：
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespType.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespSet.java`
  verify why.md#requirement-p2-resp3-输出友好化与-keys-glob-对齐-scenario-resp3-下集合类返回-mapset
- [√] 3.2 扩展 `RespWriter` 支持 RESP3 set（`setHeader`），修改 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`，verify why.md#requirement-p2-resp3-输出友好化与-keys-glob-对齐-scenario-resp3-下集合类返回-mapset
- [√] 3.3 命令层按 RESP3 输出 map/set：修改 `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java` 与 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java`，verify why.md#requirement-p2-resp3-输出友好化与-keys-glob-对齐-scenario-resp3-下集合类返回-mapset
- [√] 3.4 命令层按 RESP3 输出 set：修改 `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java`，verify why.md#requirement-p2-resp3-输出友好化与-keys-glob-对齐-scenario-resp3-下集合类返回-mapset

## 4. KEYS glob 全量兼容（P2）
- [√] 4.1 实现 Redis 风格 glob（`*`/`?`/`[]`/否定/范围/转义），修改 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`（`globMatches`），verify why.md#requirement-p2-resp3-输出友好化与-keys-glob-对齐-scenario-keys-glob-支持-与转义

## 5. Security Check
- [√] 5.1 执行安全检查（输入校验、CRLF 注入、防越界、off-heap 内存泄漏/重复释放），重点覆盖 glob/RESP3/Hash(off-heap) 路径

## 6. Testing
- [√] 6.1 新增 P0 双 reply 回归测试（maxmemory 下触发错误仍只输出单条 reply），新增 `yierdis-core/src/test/java/yier/bubu/redis/command/MaxmemoryDoubleReplyRegressionTest.java`
- [√] 6.2 新增/扩展 P1 hash encoding 测试（含 off-heap packed→dict 升级），修改 `yierdis-core/src/test/java/yier/bubu/redis/command/HashCommandTest.java`
- [√] 6.3 新增 P2 RESP3 输出端到端测试（HELLO 3 后 HGETALL/MEMORY STATS/SMEMBERS 为 map/set），新增 `yierdis-server/src/test/java/yier/bubu/redis/Resp3CollectionReplyTest.java`
- [√] 6.4 新增 P2 KEYS glob `[]`/转义匹配测试，修改 `yierdis-core/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`
- [√] 6.5 运行 `mvn test` 并修复本次改动引入的问题（不扩大范围）

## 7. Documentation Update
- [√] 7.1 同步更新知识库模块文档（命令语义/RESP3 输出/KEYS glob/Hash 编码），优先更新：
  - `helloagents/wiki/modules/yierdis-core.md`（如存在）
  - `helloagents/wiki/modules/yierdis-protocol.md`（如存在）
  - `helloagents/wiki/arch.md`（如需）
