# Task List: TX HELLO 协议切换修复 + 文档 SSOT 同步

Directory: `helloagents/plan/202602031053_tx_hello_protocol_fix/`

---

## 1. Protocol（事务 aborted 能力补齐）
- [√] 1.1 为 `RespTransactionState` 增加事务 aborted 标记入口（default 方法），更新 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespTransactionState.java`，verify why.md#requirement-tx_hello_protocol_guard-scenario-multi-中执行-hello-3（RESP2连接）

## 2. Server（连接级事务状态实现）
- [√] 2.1 在 server 的 `TransactionState` 实现 aborted 标记与“aborted 后拒绝继续入队”的策略，更新 `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java`，verify why.md#requirement-tx_hello_protocol_guard-scenario-multi-中执行-hello-3（RESP2连接），depends on task 1.1

## 3. Core（命令入队护栏）
- [√] 3.1 在 MULTI 入队路径中识别并拒绝 `HELLO 2/3`（不入队，返回错误并标记 aborted），更新 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`，verify why.md#requirement-tx_hello_protocol_guard-scenario-multi-中执行-hello-3（RESP2连接），depends on task 2.1

## 4. Tests（回归测试）
- [√] 4.1 新增 fast pipeline 回归测试：`MULTI` 中 `HELLO 3/2` 返回错误且 `EXEC` 返回 `EXECABORT`，并验证协议未被切换，新增 `yierdis-server/src/test/java/yier/bubu/redis/HelloInMultiProtocolGuardTest.java`，verify why.md#requirement-tx_hello_protocol_guard-scenario-multi-中执行-hello-3（RESP2连接）与 why.md#requirement-tx_hello_protocol_guard-scenario-multi-中执行-hello-2（RESP3连接），depends on task 3.1

## 5. Documentation Update（知识库 SSOT 同步）
- [√] 5.1 更新命令/API 手册使其与实现一致：`COMMAND` 子集、`SELECT <index>`、`KEYS` glob、Key/TTL 命令集合（含 `SCAN/PEXPIRE/PTTL/...`），更新 `helloagents/wiki/api.md`，verify why.md#requirement-docs_ssot_sync-scenario-api-md-与实现一致
- [√] 5.2 在知识库补充/强化兼容性边界说明：RESP3 request 非目标、单线程执行模型下的大输出/O(N) 风险、`maxmemory` 与 `off-heap` 预算口径与配置建议，更新 `helloagents/wiki/overview.md`（必要时补充模块文档），verify why.md#requirement-performance_boundary_clarity-scenario-高压与-busy-行为说明 与 why.md#requirement-offheap_maxmemory_clarity-scenario-offheapmaxbytes-0-的风险提示

## 6. Security Check
- [√] 6.1 执行安全检查（输入校验、错误信息净化、资源释放路径、无敏感信息输出；避免引入 EHRB 风险）

## 7. Testing
- [√] 7.1 运行 `mvn test`，确保所有测试通过并覆盖新增场景（HELLO-in-MULTI/EXEC）

## 8. Changelog & Migration（收尾）
- [√] 8.1 更新 `helloagents/CHANGELOG.md` 记录本次修复与文档同步
- [√] 8.2 将本 solution package 迁移到 `helloagents/history/YYYY-MM/` 并更新 `helloagents/history/index.md`（按 G11 要求）
