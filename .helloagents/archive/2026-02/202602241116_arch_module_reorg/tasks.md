<!-- migrated_from: history/2026-02/202602241116_arch_module_reorg/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 架构模块重组（arch_module_reorg）

Directory: `helloagents/plan/202602241116_arch_module_reorg/`

---

## 1. Maven 模块拆分（core API/DB/Command/Runtime + executor-core）
- [√] 1.1 新增 `yierdis-core-api` 模块骨架（pom + 空包占位），并将其加入根 `pom.xml` modules，verify why.md#r1-s1
- [√] 1.2 新增 `yierdis-core-db` 模块骨架（pom + 依赖 `core-api`/`offheap-api`），加入根 `pom.xml`，verify why.md#r1-s1, depends on task 1.1
- [√] 1.3 新增 `yierdis-core-command` 模块骨架（pom + 依赖 `core-api`/`protocol-model`），加入根 `pom.xml`，verify why.md#r1-s1, depends on task 1.1
- [√] 1.4 新增 `yierdis-core-runtime` 模块骨架（pom + 依赖 `core-db`/`core-command`），加入根 `pom.xml`，verify why.md#r1-s2, depends on task 1.2,1.3
- [√] 1.5 将现有 `yierdis-core` 调整为 migration aggregator（仅保留聚合依赖/兼容入口），verify why.md#r1-s2, depends on task 1.4
- [√] 1.6 新增 `yierdis-executor-core` 模块骨架（pom，Netty-free），加入根 `pom.xml`，verify why.md#r3-s1

## 2. core-api：稳定类型归属与 `db` 污染清理（签名迁移）
- [√] 2.1 设计并落地“key/view 最小能力”类型：用 `yierdis-bytes` 的 `BytesSlice` 直接替代或新增 `BytesView`（length + random access），并迁移 `ops` 接口签名，verify why.md#r1-s1
- [√] 2.2 将 `ValueType` 从 `db` 迁移到 `core-api`（或 core-model 子包），并更新命令层引用，verify why.md#r1-s1, depends on task 2.1
- [√] 2.3 将 `ScanCursorV2` 等对上层稳定的游标类型迁移到 `core-api`（避免 `ops` import `db`），并更新 `KeyspaceOps`/相关实现，verify why.md#r1-s1, depends on task 2.1
- [√] 2.4 在 `core-api` 增加 ArchUnit/扫描护栏：禁止 `core-api` 依赖 `yier.bubu.redis.db.*` 与 `io.netty.*`，verify why.md#r4-s1

## 3. core-db：实现模块迁移与 `DbEngine` 适配
- [√] 3.1 将 `yierdis-core` 里的 `db/**` 迁移到 `yierdis-core-db`（保持行为不变优先），并实现/适配 `DbEngine`，verify why.md#r1-s1, depends on task 1.2,2.1
- [√] 3.2 调整 off-heap capability 依赖：`core-db` 只依赖 `yierdis-offheap-api`（以及必要后端的 runtime 发现），verify why.md#r5-s1

## 4. core-command：命令实现迁移与“只依赖 core-api”护栏
- [√] 4.1 将 `yierdis-core` 里的 `command/**` 迁移到 `yierdis-core-command`，并保证命令只依赖 `DbEngine`/ops，不再 import `db` 包，verify why.md#r1-s1, depends on task 1.3,2.1
- [√] 4.2 补齐命令层护栏：禁止 `core-command` 依赖 `core-db`（Maven enforcer + ArchUnit），verify why.md#r4-s1

## 5. core-runtime：装配入口迁移（YierdisInstance）
- [√] 5.1 将 `yierdis-core` 里的 `runtime/**` 迁移到 `yierdis-core-runtime`，并调整装配入口（`YierdisInstance.create(...)` 等）以依赖新模块，verify why.md#r1-s2, depends on task 3.1,4.1
- [√] 5.2 保持 `yierdis-core` 迁移期聚合入口可用（仅转发/聚合，不再承载实现），verify why.md#r1-s2, depends on task 5.1

## 6. executor：拆分 core + server adapter（背压状态机收敛）
- [√] 6.1 将 `NettyExecutorTaskQueue`/预算/调度逻辑迁移到 `yierdis-executor-core`（去 Netty 类型），verify why.md#r3-s1, depends on task 1.6
- [√] 6.2 在 `yierdis-server` 实现 Netty adapter：把 `Channel`/autoRead/writability 映射到 executor-core 的决策接口，verify why.md#r3-s1, depends on task 6.1
- [√] 6.3 增加 executor/backpressure 回归测试（busy/backpressure/fair scheduling/close-after-reply 口径），verify why.md#r3-s1

## 7. server：连接态拆分（session vs runtime）
- [√] 7.1 将 `ServerConnectionState` 拆分为 `ServerSessionState` 与 `ServerRuntimeState`（分别绑定 Channel.attr），并更新 `NettyCommandExecutor`/handlers 使用点，verify why.md#r2-s1
- [√] 7.2 增加 closing/skip-side-effects 端到端集成测试（internal error/close-after-reply/队列残留命令回收），verify why.md#r2-s1, depends on task 7.1

## 8. off-heap：unsafe Netty internal 依赖隔离
- [√] 8.1 将 `PlatformDependent` 调用收敛到单一 façade（unsafe backend 内部），并为其添加最小单测（allocate/free/copy 语义与计数一致），verify why.md#r5-s1
- [√] 8.2 更新启动诊断与错误提示，确保 backend 不可用时信息可操作且不输出冗余堆栈，verify why.md#r5-s1

## 9. Security Check
- [√] 9.1 执行安全检查（G9）：异常路径资源释放、ByteBuf/reply ownership、SPI provider 诊断不泄漏敏感信息、禁止新增 Netty 依赖到 Netty-free 模块

## 10. Documentation Update（知识库同步）
- [√] 10.1 更新 `helloagents/wiki/arch.md`：模块拆分后的 SSOT、依赖方向、核心调用链图，verify why.md#r1
- [√] 10.2 更新相关模块文档（`helloagents/wiki/modules/*.md`：core/server/offheap/protocol-netty），同步新的边界与护栏说明

## 11. Testing
- [√] 11.1 分阶段回归：每完成一个迁移批次运行 `mvn test`，并记录关键回归点（backpressure/tx/close-after-reply/off-heap）
- [√] 11.2 最小烟测：运行 `./scripts/smoke.sh`（如存在），验证 server 启动/关闭 + 基本命令 + bench connect-only

回归记录（摘要）：
- `mvn -pl yierdis-core -am test`
- `mvn test`
- `./scripts/smoke.sh`
