<!-- migrated_from: history/2026-01/202601081106_netty_executor_integration/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Netty 执行器融合改造（覆盖问题 1/2/3/4/5）

Directory: `helloagents/plan/202601081106_netty_executor_integration/`

---

## 1. yierdis-server: 执行模型融合（Solution 2）
- [√] 1.1 新增基于 Netty `EventExecutorGroup(1)` 的命令执行器骨架（全局单线程语义），包含 pending/backlog 计数与可配置阈值，文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，验证 why.md#requirement-端到端低分配与-off-heap-写入-路径改造聚焦-hot-path
- [√] 1.2 将 `YierdisServer` 接入新执行器（替换/旁路 `CommandExecutor`），并保证 `YierdisDb.bindToCurrentThread()` 在执行线程发生，文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`，验证 why.md#requirement-pipeline-吞吐与-flush-合并
- [√] 1.3 改造 `YierdisFastCommandHandler`：将 `RespCommand` 投递给新执行器并在超限时返回 `-ERR busy`，同时确保命令帧生命周期正确回收，文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`，验证 why.md#requirement-背压闭环

## 2. yierdis-server: flush 合并与背压闭环
- [√] 2.1 为单连接实现滞回阈值（high/low watermark）与 `autoRead` 切换逻辑，必要时在执行器侧调度恢复，文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，验证 why.md#requirement-背压闭环
- [√] 2.2 实现批量 drain：对同一连接在一次 drain 内 `write` 聚合并在末尾 `flush`（或达到 batch 上限 flush），保证响应顺序一致，文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，验证 why.md#requirement-pipeline-吞吐与-flush-合并
- [√] 2.3 新增/调整测试覆盖：backlog 触发 busy 与恢复 autoRead、flush 合并不乱序，文件：`yierdis-server/src/test/java/yier/bubu/redis/` 下新增测试类，验证 why.md#requirement-背压闭环 与 why.md#requirement-pipeline-吞吐与-flush-合并

## 3. yierdis-server: hot path 低分配（聚焦 SET/GET 与 off-heap 双拷贝）
- [√] 3.1 为 off-heap backend 增加“从 `RespCommand` frame slice 直接写入 off-heap buf/string”的写入入口（避免 `byte[]` 中转），文件：`yierdis-server/src/main/java/yier/bubu/redis/db/YierdisObject.java`，验证 why.md#requirement-端到端低分配与-off-heap-写入
- [√] 3.2 在 `SET/APPEND` 等路径接入新的写入入口（仅在 off-heap backend 时启用），文件：`yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`，验证 why.md#requirement-端到端低分配与-off-heap-写入
- [√] 3.3 命令层减少不必要的 `toByteArray`：对可使用 `YierdisBytesView` 的 key 操作保持零分配；对必须物化的 key/value 仅分配“最终存储所需”的拷贝，文件：`yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`，验证 why.md#requirement-端到端低分配与-off-heap-写入

## 4. yierdis-bench: 正确性与 RESP3 基础类型兼容
- [√] 4.1 bench 统计改造：将 `-` error 响应计入 errors（Throughput/Latency 都要统计），并在汇总中展示 errors，文件：`yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`，验证 why.md#requirement-resp3bench-校验
- [√] 4.2 扩展 `RespResponseSkipper`：支持 RESP3 基础类型跳过（至少 `_`、`%`，并保持向后兼容），文件：`yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`，验证 why.md#requirement-resp3bench-校验
- [√] 4.3 可选严格校验开关：对 `PING/SET/GET` 在 bench 中做最小语义校验（失败计入 errors 并记录），文件：`yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`，验证 why.md#requirement-resp3bench-校验

## 5. yierdis-server: maxmemory/过期/淘汰稳定性
- [√] 5.1 将 `enforceMaxmemory/cleanupExpired` 的预算策略显式化并可配置（避免长尾），并确保维护任务在压力下不被长期饿死，文件：`yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`，验证 why.md#requirement-maxmemory过期淘汰在压力下稳定
- [√] 5.2 新增压力语义测试：maxmemory 下淘汰/错误码稳定、过期清理可生效，文件：`yierdis-server/src/test/java/yier/bubu/redis/command/` 下新增/增强测试，验证 why.md#requirement-maxmemory过期淘汰在压力下稳定

## 6. Security Check
- [√] 6.1 执行安全检查（输入上限、错误信息净化、资源回收、off-heap close 路径、避免无界队列/内存增长），并在实现中补齐必要的防护与测试

## 7. Documentation Update
- [√] 7.1 在实现完成后同步更新知识库：`helloagents/wiki/modules/` 对应模块说明、并记录变更到 `helloagents/CHANGELOG.md`

## 8. Testing
- [√] 8.1 运行 `mvn test`（至少覆盖 `yierdis-server` 与 `yierdis-bench`），并在结果中确认：无资源泄漏、顺序正确、errors 统计正确、bench 可复现对比
