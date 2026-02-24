<!-- migrated_from: history/2026-02/202602022147_redis_compat_alignment/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Redis 兼容性对齐与功能扩展（RESP3 / 事务 / 全局 maxmemory）

Directory: `helloagents/plan/202602022147_redis_compat_alignment/`

---

## 1. Protocol（RESP3 编码一致性与扩展）
- [√] 1.1 扩展 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespEncoder.java`：补齐 RESP3 类型写出（set/boolean/double/bignum/verbatim/blob error/push/attribute/null），verify why.md#requirement-r1_resp3-编码一致性与扩展-s1_respencoder-覆盖-resp3-全类型
- [√] 1.2 更新 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObject.java` 的注释与类型声明说明（RESP2→RESP2/RESP3），verify why.md#requirement-r1_resp3-编码一致性与扩展-s1_respencoder-覆盖-resp3-全类型
- [√] 1.3 新增协议 round-trip 单测：`RespObject` → `RespEncoder` → `RespDecoder` → `RespObjectParser`（覆盖关键 RESP3 类型），verify why.md#requirement-r1_resp3-编码一致性与扩展-s1_respencoder-覆盖-resp3-全类型，depends on task 1.1

## 2. Client/CLI（RESP3 友好展示）
- [√] 2.1 扩展 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`：补齐 RESP3 set/boolean/double/bignum/verbatim/blob error/push/attribute 的打印逻辑，verify why.md#requirement-r1_resp3-编码一致性与扩展-s2_cli-对-resp3-类型友好展示
- [√] 2.2 新增/增强 CLI 测试用例：`HELLO 3` 后对 map/set/null/boolean/double 的解析与展示稳定性，verify why.md#requirement-r1_resp3-编码一致性与扩展-s2_cli-对-resp3-类型友好展示，depends on task 2.1

## 3. Transaction（MULTI 队列上限与 EXECABORT）
- [√] 3.1 新增 server 参数：在 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 增加 `--transactionQueueMaxCommands/--transactionQueueMaxBytes` 并完成 normalize/validate，verify why.md#requirement-r2_事务队列上限与-execabort-语义-s1_multi-队列超限保护
- [√] 3.2 扩展连接态事务实现：在 `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java` 的 TransactionState 中实现队列条数/bytes 统计与超限处理（入队错误 + aborted 标记），verify why.md#requirement-r2_事务队列上限与-execabort-语义-s1_multi-队列超限保护，depends on task 3.1
- [√] 3.3 对齐 EXECABORT 行为：在 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` 与 `yierdis-core/src/main/java/yier/bubu/redis/command/TransactionCommands.java` 中实现“入队错误→EXECABORT”的语义闭环（必要时扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespTransactionState.java`），verify why.md#requirement-r2_事务队列上限与-execabort-语义-s1_multi-队列超限保护，depends on task 3.2
- [√] 3.4 新增事务超限回归测试：覆盖超限、EXECABORT、DISCARD 复位与连接关闭清理路径，verify why.md#requirement-r2_事务队列上限与-execabort-语义-s1_multi-队列超限保护，depends on task 3.3

## 4. Memory（maxmemory 全局口径与跨 DB 淘汰）
- [√] 4.1 引入 `--maxmemoryScope global|per-db` 参数（默认 global），在 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 与 `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java` 中落地并校验，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s1_跨-db-统一预算
- [√] 4.2 调整启动装配：在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java` 中将 `--maxmemoryBytes` 在 global 模式下作为实例级预算（不再按 DB 分摊），并保留 per-db 兼容模式，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s1_跨-db-统一预算，depends on task 4.1
- [√] 4.3 设计并实现全局预算协调器（global eviction）：跨 DB 近似采样 allkeys-random/allkeys-lru，并将 shared off-heap usedBytes 只计一次，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s1_跨-db-统一预算，depends on task 4.2
- [√] 4.4 对齐 MEMORY/INFO 口径：更新 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java` 的 `MEMORY STATS` 输出为全局口径（或提供兼容子路径），并同步 `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java` 的 memory summary，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s1_跨-db-统一预算，depends on task 4.3
- [√] 4.5 新增 global maxmemory 多 DB 回归测试：跨 DB 写入触发淘汰、INFO/MEMORY STATS 校验、off-heap 统计不双计，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s2_off-heap-与-maxmemory-关系清晰且安全，depends on task 4.4
- [√] 4.6 启动时误配置提示：当启用 off-heap 但 `offheapMaxBytes=0` 时输出明确风险提示/建议配置，verify why.md#requirement-r3_maxmemory-全局口径与跨-db-淘汰-s2_off-heap-与-maxmemory-关系清晰且安全，depends on task 4.2

## 5. Executor/Backpressure（busy 可诊断性与边界行为）
- [√] 5.1 细化 busy 错误原因：在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java` 与 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 中让拒绝路径输出可诊断原因（保持 RESP error 兼容形态），verify why.md#requirement-r4_busy-背压可诊断性-s1_busy-返回原因或可通过-stats-定位
- [√] 5.2 校验并补齐 STATS/INFO 的“原因→计数器”映射文档与示例，verify why.md#requirement-r4_busy-背压可诊断性-s1_busy-返回原因或可通过-stats-定位，depends on task 5.1

## 6. Compatibility Roadmap（缺失特性路线图）
- [√] 6.1 更新 `README.md`：明确 in-scope/out-of-scope、RESP3 支持范围、maxmemory/global 模式说明、事务限制说明，verify why.md#requirement-r5_兼容性路线图与边界声明-s1_用户理解一致
- [√] 6.2 更新知识库模块文档：
  - `helloagents/wiki/modules/protocol.md`
  - `helloagents/wiki/modules/protocol-netty.md`
  - `helloagents/wiki/modules/server.md`
  - `helloagents/wiki/modules/db.md`
  - `helloagents/wiki/modules/command.md`
  - `helloagents/wiki/modules/client.md`
  - `helloagents/wiki/arch.md`（必要时补 ADR index）
  verify why.md#requirement-r5_兼容性路线图与边界声明-s1_用户理解一致
- [√] 6.3 在 `helloagents/wiki/overview.md` 增加“兼容性路线图”章节：AOF/RDB、PubSub、TLS/ACL、Lua、复制/集群 的阶段性里程碑与风险说明，verify why.md#requirement-r5_兼容性路线图与边界声明-s1_用户理解一致

## 7. Security Check
- [√] 7.1 执行安全检查（G9）：输入上限、事务队列上限、错误消息净化、拒绝路径无资源泄漏、off-heap 上限不允许“隐式无限”，并记录结论

## 8. Testing
- [√] 8.1 执行 `mvn test` 并确认通过；新增测试覆盖 RESP3/事务/maxmemory/global/busy 原因
- [√] 8.2 执行 `./scripts/smoke.sh` 验证 server+cli+bench strictReplies 基本链路
- [-] 8.3（可选）执行 `./scripts/bench.sh` 对比 busy 比例与 tail latency，确认无明显退化
  > Note: 已通过 `scripts/smoke.sh` 覆盖最小 bench strictReplies；`scripts/bench.sh` 的 latency 阶段默认压测规模较大，本次仅做了部分运行后中断以避免长时间占用。

## 9. Documentation + Release Hygiene
- [√] 9.1 更新 `helloagents/CHANGELOG.md`：记录行为变更（maxmemory 口径、事务限制、RESP3 扩展）
- [√] 9.2 进行一致性审计：README/wiki 与代码行为一致（以代码为准，修正文档漂移）
- [√] 9.3 迁移 solution package 到 `helloagents/history/YYYY-MM/202602022147_redis_compat_alignment/` 并更新 `helloagents/history/index.md`（执行阶段完成后必做）
