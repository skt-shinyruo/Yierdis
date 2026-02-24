<!-- migrated_from: history/2026-02/202602081942_command_engine_boundary/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Command/DB 边界收口与执行器协议解耦（DbEngine + Ops 化）

Directory: `helloagents/plan/202602081942_command_engine_boundary/`

---

## 1. yierdis-core（DbEngine 边界与命令层去耦）
- [√] 1.1 新增 command-facing 子接口骨架（`KeyspaceOps`/`TtlOps`/`MemoryOps`）到 `yierdis-core/src/main/java/yier/bubu/redis/ops/`，verify why.md#r1-s1
- [√] 1.2 扩展 `DbEngine`：增加 `keyspace()/ttl()/memory()/lifecycle()` 访问器（保持现有 `values()/expiration()/eviction()` 语义不变），修改 `yierdis-core/src/main/java/yier/bubu/redis/ops/DbEngine.java`，verify why.md#r1-s1, depends on task 1.1
- [√] 1.3 新增 `DbLifecycleOps`（最小集合：`flushDb/size` 等命令所需能力）到 `yierdis-core/src/main/java/yier/bubu/redis/ops/`，verify why.md#r1-s1, depends on task 1.2

- [√] 1.4 迁移写入选项类型：新增 `SetMode/ExpireOption`（或等价新类型）到 `yierdis-core/src/main/java/yier/bubu/redis/ops/`，并先让 `StringOps` 签名切换到新类型，修改 `yierdis-core/src/main/java/yier/bubu/redis/ops/StringOps.java`，verify why.md#r2-s1
- [√] 1.5 迁移异常边界：新增 `WrongTypeException/CommandException`（ops 层）并让 `YierdisFastCommandProcessor` 捕获新异常类型，修改 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`，verify why.md#r2-s2, depends on task 1.4

- [√] 1.6 路由与 support 去耦：将 `YierdisDbRouter` 的返回类型改为 `DbEngine`，并让 `CommandSupport.db(out)` 返回 `DbEngine`，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisDbRouter.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/CommandSupport.java`
  verify why.md#r1-s2, depends on task 1.2

- [√] 1.7 迁移 command handlers（key/server）：移除对 `YierdisDb` 的 import，改为使用 `DbEngine.keyspace()/ttl()/memory()/lifecycle()`，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`
  verify why.md#r1-s1, depends on task 1.6

- [√] 1.8 迁移 command handlers（string）：移除对 `YierdisDb` 的 import，改用 ops 化的 `SetMode/ExpireOption`，修改 `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`，verify why.md#r2-s1, depends on task 1.4

- [√] 1.9 迁移 command handlers（list/hash/set）：移除对 `YierdisDb` 的 import，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java`
  verify why.md#r1-s1, depends on task 1.6

- [√] 1.10 迁移 command handlers（zset/hll）：移除对 `YierdisDb` 的 import，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HllCommands.java`
  verify why.md#r1-s1, depends on task 1.6

- [√] 1.11 db 实现适配：让 `YierdisDb` 实现/暴露新增的 `DbEngine` 子 ops（keyspace/ttl/memory/lifecycle），修改 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`，verify why.md#r1-s2, depends on task 1.2

- [√] 1.12 instance 装配对齐：更新 `yierdis-core/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java` 与相关 router 构造链，确保多 DB + SELECT 场景下返回 `DbEngine`，verify why.md#r1-s2, depends on task 1.6

## 2. yierdis-core（Streaming 聚合读 API 去 reply 语义）
- [√] 2.1 list：将 `lrangeReplyCount/lrangeReplyInto` 迁移为数据语义命名（例如 `lrangeCount/lrangeWriteTo`），并同步修改调用方，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/ListOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java`
  verify why.md#r3-s1, depends on task 1.9

- [√] 2.2 hash：将 `hgetallReplyCount/hgetallReplyInto` 迁移为数据语义命名并同步修改调用方，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/HashOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java`
  verify why.md#r3-s1, depends on task 1.9

- [√] 2.3 set：将 `smembersReplyCount/smembersReplyInto` 迁移为数据语义命名并同步修改调用方，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/SetOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java`
  verify why.md#r3-s1, depends on task 1.9

- [√] 2.4 zset：将 `z*ReplyCount/z*ReplyInto` 迁移为数据语义命名并同步修改调用方，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/ZSetOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
  verify why.md#r3-s1, depends on task 1.10

- [√] 2.5 文档对齐：更新分层约束与命名规范，修改：
  - `helloagents/wiki/modules/db.md`
  - `helloagents/wiki/modules/command.md`
  verify why.md#r3-s2, depends on task 2.4

## 3. yierdis-server（执行器协议解耦）
- [√] 3.1 引入 `ReplyWriterFactory`（或等价接口）并提供 Custom Protocol v1 默认实现，改造 executor 通过 factory 获取 writer，修改：
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  verify why.md#r4-s1

- [√] 3.2 拆分回包写出/flush 合并为独立组件（保持 backpressure/fair scheduling 语义不变），修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`（以及新增组件类），verify why.md#r4-s2, depends on task 3.1

## 4. Security Check
- [√] 4.1 执行安全检查（G9）：输入校验、敏感信息处理、权限边界、避免 off-heap slice 生命周期被错误延长、避免双 reply

## 5. Testing
- [√] 5.1 全量回归：运行 `mvn test`
- [√] 5.2 分层约束回归：`rg -n \"import yier\\.bubu\\.redis\\.db\\.YierdisDb\" yierdis-core/src/main/java/yier/bubu/redis/command` 结果应为空（或仅保留被允许的测试工具代码）
- [√] 5.3 executor 回归：运行/补齐 `NettyCommandExecutorTest` 覆盖 busy/backpressure/fair scheduling 与 close-after-reply 计数器口径
