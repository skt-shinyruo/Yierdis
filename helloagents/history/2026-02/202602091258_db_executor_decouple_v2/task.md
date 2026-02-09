# Task List: DB/Executor 边界解耦 v2（移除 server→YierdisDb 直接依赖 + 持续拆分巨型类）

Directory: `helloagents/plan/202602091258_db_executor_decouple_v2/`

---

## 1. server：彻底移除对 YierdisDb 的直接依赖

- [√] 1.1 调整 `NettyCommandExecutor`：弃用/移除 `NettyCommandExecutor(YierdisDb db, ...)` overload，仅保留 `Runnable bindToCurrentThread` 构造入口；同步更新 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，verify why.md#requirement-server-层不直接依赖具体-db-实现
- [√] 1.2 修改 `YierdisServerBootstrap`：通过 `YierdisInstance#bindToCurrentThread` 注入 binder，不再缓存/传递 `YierdisDb[]` 给 executor；修改 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`，verify why.md#requirement-server-层不直接依赖具体-db-实现
- [√] 1.3 修改 `NettyServerInfoProvider`：将 DB 绑定从 `YierdisDb[]` 迁移到 `DbEngine[]` 或只读 view（`MemoryOps/KeyspaceOps` 等），并在 bootstrap 侧完成装配；修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`，verify why.md#requirement-server-层不直接依赖具体-db-实现

## 2. server：executor 组件化与配置收敛（降低巨型类复杂度）

- [√] 2.1 引入 `NettyCommandExecutorConfig`（或拆分为 queue/backpressure/drain 子配置），减少构造参数爆炸；修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 与相关装配点，verify why.md#requirement-executor-职责边界清晰可独立测试
- [-] 2.2 将 drain loop 与 flush batch 的编排逻辑从 `NettyCommandExecutor` 中抽取为独立类（保持零/少分配），并为关键 contract 增补单测；修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` + 新增 1 个组件类，verify why.md#requirement-executor-职责边界清晰可独立测试（已抽取 flush batch；drain loop 进一步拆分与专项单测留待后续，避免一次性大改引入行为漂移）

## 3. core/db：持续拆分 YierdisDb 巨型类（缩小修改半径）

- [√] 3.1 将 `YierdisDb` 内部稳定概念（错误类型/选项枚举/异常/ledger 等）迁移到 `yierdis-core/src/main/java/yier/bubu/redis/ops/` 或 `db` 子组件（避免 inner type 持续膨胀）；修改 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java` 及至多 2 个引用点，verify why.md#requirement-dbvalue-层-api-语义收敛去-reply-语义

## 4. core/db：清理残余 reply 命名渗透

- [√] 4.1 `ZSetValue`：将内部 `*ReplyCount/*ReplyInto` helper 命名迁移为数据语义（例如 `*Count/*WriteTo`），保持行为不变；修改 `yierdis-core/src/main/java/yier/bubu/redis/db/ZSetValue.java`，verify why.md#requirement-dbvalue-层-api-语义收敛去-reply-语义

## 5. Security Check

- [√] 5.1 执行安全检查（G9）：确认无敏感信息明文、无危险命令引入、无越权/资源泄漏；重点关注 close-after-reply、队列预算与 backpressure 的边界行为

## 6. Documentation Update

- [√] 6.1 更新知识库：同步 `helloagents/wiki/arch.md` 与 `helloagents/wiki/modules/server.md` 的边界说明（server 不依赖具体 DB 实现；executor 组件职责），并在 `helloagents/CHANGELOG.md` 记录本次重构变更

## 7. Testing

- [√] 7.1 更新 executor 相关测试：将 `NettyCommandExecutorTest` 等从 `new YierdisDb()` 迁移到 instance/binder 构造方式，覆盖 close-after-reply 与后续命令 skip；修改 `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`（必要时加 1 个测试文件）
- [√] 7.2 跑回归测试：执行 `mvn test`，并记录关键测试用例覆盖点（背压、队列预算、公平调度、close-after-reply）

## 8. Solution Package Lifecycle

- [√] 8.1 执行完成后：按 G11 将该 solution package 迁移到 `helloagents/history/YYYY-MM/` 并更新 `helloagents/history/index.md`
