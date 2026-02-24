<!-- migrated_from: history/2026-02/202602081454_db_executor_decouple/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: DB/Executor 分层解耦与组件化（ReplySink + 巨型类拆分）

Directory: `helloagents/plan/202602081454_db_executor_decouple/`

---

## 1. yierdis-protocol（ReplySink 抽象）
- [√] 1.1 新增窄接口 `ReplySink`（bulk string 写出子集），并让 `ReplyWriter` 继承该接口，修改/新增：
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/ReplySink.java`
  - `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/ReplyWriter.java`
  verify why.md#requirement-replysink-分层将-streaming-bulk-输出从-db-包迁出-scenario-getecho-等-bulk-string-输出保持一致

## 2. yierdis-core（命令层/ops 层迁移到 ReplySink）
- [√] 2.1 将 `*Ops` 的 streaming 写出参数类型从 `YierdisBulkStringOutput` 迁移为 `ReplySink`，并保持现有 count/header/writeInto 调用顺序不变，优先落地 list/hash/set/zset/string 读路径，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/ops/*.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/*Commands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/CommandSupport.java`
  verify why.md#requirement-replysink-分层将-streaming-bulk-输出从-db-包迁出-scenario-lrange--smembers-结果以-streaming-方式写出
- [√] 2.2 为迁移后的 streaming 路径补齐单测锚点（至少覆盖：LRANGE/HGETALL/SMEMBERS/ZRANGE/ZRANGEBYSCORE），新增/修改：
  - `yierdis-core/src/test/java/yier/bubu/redis/command/*Test.java`
  verify why.md#requirement-replysink-分层将-streaming-bulk-输出从-db-包迁出-scenario-lrange--smembers-结果以-streaming-方式写出
- [√] 2.3 清理 db 包内 `YierdisBulkStringOutput` 的桥接依赖（若仍作为过渡层存在，需明确标记为 deprecated，并规划移除点），修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisBulkStringOutput.java`
  depends on task 2.1

## 3. yierdis-core（db 层解耦与组件化）
- [√] 3.1 将 `YierdisDb` 对外公开的 `*ReplyCount/*ReplyInto` 方法移除或收敛为 internal（以 `ValueOps` 为唯一入口），并清理未使用的重复 API，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  verify why.md#requirement-replysink-分层将-streaming-bulk-输出从-db-包迁出-scenario-lrange--smembers-结果以-streaming-方式写出
- [√] 3.2 移除 `db` 包对 `protocol.Command` 的直接依赖：为 string 写入/覆盖写入引入中立 bytes view/slice（优先复用 `BytesSource + offset + len`），并迁移 SET/APPEND 等热路径调用，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisObject.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`
  verify why.md#requirement-db-层与-protocolcommand-解耦为未来协议复制持久化铺路-scenario-setappend-等写命令在-off-heap-后端下仍保持低拷贝特性
- [?] 3.3 按 value 类型拆分 `YierdisDbValueOps` 的实现（Strings/Hashes/Lists/Sets/ZSets/Hll），使单文件复杂度显著下降，修改/新增：
  - `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/db/*ValueOps*.java`（新增若干类型组件）
  verify why.md#requirement-yierdisdb-组件化降低单类复杂度与变更爆炸半径-scenario-过期清理淘汰内存记账行为保持一致

## 4. yierdis-server（NettyCommandExecutor 拆分）
- [√] 4.1 将 `NettyCommandExecutor` 拆分为 drain loop / submitter / reply write batcher 等组件（以移动代码为主、保持外部契约与计数器口径不变），修改/新增：
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutor*.java`（必要时新增组件类）
  verify why.md#requirement-nettycommandexecutor-拆分可测试可演进-scenario-backpressure-与公平调度回归
- [√] 4.2 扩展/加固执行器测试（背压滞回、FAIR 调度、关闭语义、queued-bytes 预算），修改：
  - `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutor*Test.java`
  verify why.md#requirement-nettycommandexecutor-拆分可测试可演进-scenario-backpressure-与公平调度回归

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：确认 sink/adapter 不缓存引用、off-heap slice 生命周期安全、retainedBytes/queued-bytes 口径一致、错误消息净化与输入上限不弱化；记录发现与结论

## 6. Documentation Update
- [√] 6.1 更新知识库文档以匹配新分层（`protocol/db/command/server`），至少更新：
  - `helloagents/wiki/modules/protocol.md`
  - `helloagents/wiki/modules/db.md`
  - `helloagents/wiki/modules/command.md`
  - `helloagents/wiki/modules/server.md`
- [√] 6.2 更新 `helloagents/CHANGELOG.md`：记录本次重构的边界变化、风险提示与回归结果

## 7. Testing & Bench
- [√] 7.1 运行 `mvn test` 确保全量测试通过，并记录关键结果
- [√] 7.2 运行 `yierdis-bench` 做 A/B（至少覆盖 GET/SET + 聚合读场景），记录 QPS/P99 对比与可接受退化说明
