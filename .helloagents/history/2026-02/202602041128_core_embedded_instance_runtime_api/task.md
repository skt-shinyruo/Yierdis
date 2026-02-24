# Task List: Core Embedded Instance Runtime API (Netty-free Instance SSOT)

Directory: `helloagents/plan/202602041128_core_embedded_instance_runtime_api/`

---

## 1. Embedded Instance API (core/runtime)
- [√] 1.1 新增 `YierdisInstance`/`YierdisInstanceConfig`（或 Builder）骨架：可创建 N DB、暴露 `dbs()`/`db(int)`/`router()`/`newCommandProcessor(...)`，verify why.md#requirement-embedded-instance-api-netty-free
- [√] 1.2 明确线程语义：为 instance 提供 `bindToCurrentThread()` 并在文档/Javadoc 中写明“单线程 DB 语义”为 SSOT，verify why.md#requirement-embedded-instance-api-netty-free，depends on task 1.1
- [√] 1.3 明确生命周期：instance close 负责关闭 DB 与（可选）allocator；补齐 owner 语义与异常安全，verify why.md#requirement-embedded-instance-api-netty-free，depends on task 1.1

## 2. Instance Runtime SSOT (Extract global maxmemory / LRU)
- [√] 2.1 抽离 global maxmemory 协调逻辑：从 `YierdisDb` 内部迁移到 core runtime（保持对外行为一致），verify why.md#requirement-instance-responsibilities-ssot-global-maxmemory--lru-clock，depends on task 1.1
- [√] 2.2 `YierdisDb` 适配：global 模式下写入/淘汰/LRU clock 委托给 runtime SSOT；保留 per-db 模式行为，verify why.md#requirement-instance-responsibilities-ssot-global-maxmemory--lru-clock，depends on task 2.1
- [√] 2.3 兼容入口：保留 `YierdisDb.enableGlobalMaxmemory(...)`，但内部改为委托 runtime（便于渐进迁移与回滚），verify why.md#requirement-backward-compatibility-for-server-wiring，depends on task 2.2

## 3. Server Wiring Migration (Use core instance API)
- [√] 3.1 `YierdisServerBootstrap` 改为使用 `YierdisInstance` 装配 DB/off-heap/maxmemory（保持现有参数语义不变），verify why.md#scenario-server-bootstrap-uses-core-instance-api-without-behavior-regression，depends on task 1.3
- [√] 3.2 校验 close 顺序与资源归属：server close 中避免 double-close allocator，verify why.md#scenario-server-bootstrap-uses-core-instance-api-without-behavior-regression，depends on task 3.1

## 4. Regression Tests (Instance-level)
- [√] 4.1 新增 core 回归测试：multi-db + global maxmemory + shared allocator 只计一次且可淘汰/拒写，verify why.md#scenario-global-maxmemory-counts-shared-off-heap-once-across-dbs，depends on task 2.2
- [√] 4.2 新增 misuse 回归：未 bind 或跨线程访问 instance/db fail-fast，verify why.md#requirement-embedded-instance-api-netty-free，depends on task 1.2
- [√] 4.3 兼容性回归：现有 maxmemory 相关测试保持通过（包括双 reply 回归），verify why.md#requirement-backward-compatibility-for-server-wiring，depends on task 2.3

## 5. Security Check
- [√] 5.1 Execute security check（per G9：资源释放、off-heap 生命周期、拒写点一致性、错误净化）

## 6. Documentation Update (Knowledge Base)
- [√] 6.1 更新 `helloagents/wiki/arch.md`：补齐 core runtime/instance 与 server 执行/治理的边界与依赖方向
- [√] 6.2 更新 `helloagents/wiki/modules/db.md`：补齐 instance/runtime 的职责与与 DB 的交互点（global maxmemory/LRU）
- [√] 6.3 更新 `helloagents/wiki/modules/server.md`：说明 server 仅做执行/装配/治理，并引用 core instance API
