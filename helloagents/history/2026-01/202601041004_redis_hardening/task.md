# Task List: redis_hardening

Directory: `helloagents/plan/202601041004_redis_hardening/`

---

## 1. server（I/O 与执行解耦 + 背压）
- [√] 1.1 设计并实现单线程 `CommandExecutor`（队列上限 + 拒绝策略），新增/修改 `yierdis-server/src/main/java/yier/bubu/redis/*`，verify why.md#requirement-io-与执行解耦（单线程命令语义保持）-scenario-重命令不阻塞-io-event-loop
- [√] 1.2 改造 Netty pipeline：I/O 线程只解码与投递，命令在执行器线程运行，verify why.md#requirement-io-与执行解耦（单线程命令语义保持）-scenario-重命令不阻塞-io-event-loop, depends on task 1.1
- [√] 1.3 增加队列满/繁忙场景测试（返回 `ERR busy` 或等价错误），verify why.md#requirement-io-与执行解耦（单线程命令语义保持）-scenario-重命令不阻塞-io-event-loop, depends on task 1.2

## 2. protocol（RESP 错误安全净化）
- [√] 2.1 在 RESP error 写出层实现统一净化（过滤 CR/LF + 限长），修改 `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespWriter.java`，verify why.md#requirement-resp-错误输出安全净化-scenario-unknown-command-不可注入额外-resp-回复
- [√] 2.2 统一 unknown command / 参数错误等错误消息生成，避免拼接不可信输入，修改 `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`（及相关组件），verify why.md#requirement-resp-错误输出安全净化-scenario-unknown-command-不可注入额外-resp-回复, depends on task 2.1
- [√] 2.3 增加 CRLF 注入回归测试（构造包含 `\\r\\n` 的命令名/参数），verify why.md#requirement-resp-错误输出安全净化-scenario-unknown-command-不可注入额外-resp-回复, depends on task 2.2

## 3. command（命令实现收敛 + 命令表驱动）
- [√] 3.1 引入命令表（command name → handler），将长 if/else 收敛到统一注册点，修改/新增 `yierdis-server/src/main/java/yier/bubu/redis/command/*`，verify why.md#requirement-命令执行路径收敛与语义锁定-scenario-同一命令仅存在单一权威实现（ssot）
- [√] 3.2 收敛执行路径：将对象式 `CommandProcessor` 移除或降级为测试辅助（且不再作为线上路径），同步迁移测试到 fast pipeline，verify why.md#requirement-命令执行路径收敛与语义锁定-scenario-同一命令仅存在单一权威实现（ssot）, depends on task 3.1
- [√] 3.3 增强“语义一致性”测试：以 RESP pipeline 为主做核心命令集回归，verify why.md#requirement-命令执行路径收敛与语义锁定-scenario-同一命令仅存在单一权威实现（ssot）, depends on task 3.2

## 4. db（maxmemory 口径与 MEMORY USAGE 一致性）
- [√] 4.1 明确并实现 `maxmemoryBytes` 的统计口径（heap 元数据估算 + off-heap usedBytes），修改 `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`，verify why.md#requirement-maxmemory-口径统一且可解释-scenario-off-heap-启用时淘汰-拒写触发时机可预测
- [√] 4.2 修正/补齐 `MEMORY USAGE` 与淘汰/拒写逻辑的口径一致性测试，修改/新增 `yierdis-server/src/test/java/yier/bubu/redis/command/MaxmemoryEvictionTest.java`（或新增测试），verify why.md#requirement-maxmemory-口径统一且可解释-scenario-off-heap-启用时淘汰-拒写触发时机可预测, depends on task 4.1

## 5. offheap（资源释放回归验证）
- [√] 5.1 增加 off-heap allocator 泄漏回归测试：执行一组写入/删除/过期/淘汰后 shutdown，断言 `usedBytes` 回到基线，新增/修改 `yierdis-offheap/*` 与 `yierdis-server/src/test/java/*` 测试代码，verify why.md#requirement-off-heap-资源释放可回归验证-scenario-db-shutdown-后-allocator-usedbytes-归零（或符合预期）
- [√] 5.2 补齐关键释放路径（覆盖、删除、淘汰、过期清理、shutdown）遗漏点，verify why.md#requirement-off-heap-资源释放可回归验证-scenario-db-shutdown-后-allocator-usedbytes-归零（或符合预期）, depends on task 5.1

## 6. Security Check
- [√] 6.1 执行安全检查（输入校验、错误输出注入风险、队列/返回大小 DoS 风险、off-heap 资源释放路径），并记录结果到方案执行记录

## 7. Documentation Update
- [√] 7.1 同步更新知识库：`helloagents/wiki/modules/server.md`、`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/modules/command.md`、`helloagents/wiki/modules/db.md`（如涉及），并更新 `helloagents/CHANGELOG.md`

## 8. Testing
- [√] 8.1 `mvn test` 全量通过；对关键场景补充 `redis-cli --resp2` 手工回归步骤（可选）
