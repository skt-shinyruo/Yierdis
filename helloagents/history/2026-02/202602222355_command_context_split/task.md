# Task List: CommandContext 全链路重构（拆分输入 Session 与输出 ReplyWriter）

Directory: `helloagents/plan/202602222355_command_context_split/`

---

## 1. yierdis-protocol-model（上下文/路由输入侧建模）
- [√] 1.1 新增 `CommandContext` 与 `DbIndexProvider`，并将 `ServerSession` 继承 `DbIndexProvider`，修改：
  - `yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/CommandContext.java`（新增）
  - `yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/DbIndexProvider.java`（新增）
  - `yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/ServerSession.java`
  verify why.md#requirement-路由输入解耦db-路由不依赖-replywriter-scenario-select-切换-db-后路由仍正确

## 2. yierdis-protocol-model（ReplyWriter 纯输出化）
- [√] 2.1 移除 `ReplyWriter.session()`，并调整 `ReplyWriterFactory` 与 Custom Protocol v1 writer/factory，修改：
  - `yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/ReplyWriter.java`
  - `yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/ReplyWriterFactory.java`
  - `yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`
  - `yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterFactory.java`
  verify why.md#requirement-replywriter-纯输出端口不再暴露-session-scenario-命令层不再出现-outsession-读取
  depends on task 1.1

## 3. yierdis-core（Router 输入侧切换：ReplyWriter → DbIndexProvider）
- [√] 3.1 调整 `YierdisDbRouter` 签名为 `dbFor(DbIndexProvider)`，并修改 `YierdisInstance` 默认 router 路由逻辑，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisDbRouter.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
  verify why.md#requirement-路由输入解耦db-路由不依赖-replywriter-scenario-embedded单测缺少连接态时默认-db0
  depends on task 1.1

## 4. yierdis-core（命令执行入口签名切换：CommandContext）
- [√] 4.1 修改 `CommandRegistry` handler 签名为 `(Command, CommandContext)`，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
  verify why.md#requirement-replywriter-纯输出端口不再暴露-session-scenario-命令层不再出现-outsession-读取
  depends on task 1.1
- [√] 4.2 修改 `YierdisFastCommandProcessor#execute` 与 `CommandSupport`，使用 `ctx.session()` 做事务/变更事件/rollback 路由输入，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/CommandSupport.java`
  verify why.md#requirement-事务可观测慢命令治理统一基于上下文-scenario-multiexec-仅在-server-连接态可用
  depends on task 3.1
  depends on task 4.1

## 5. yierdis-core（命令族批量迁移：*Commands）
- [√] 5.1 迁移 `ServerCommands` 与 `TransactionCommands` 到 `(Command, CommandContext)`，并消除 `out.session()`/分支散落，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
  verify why.md#requirement-事务可观测慢命令治理统一基于上下文-scenario-multiexec-仅在-server-连接态可用
  depends on task 4.2
- [√] 5.2 迁移 `KeyCommands`（含 KEYS 慢命令治理），并在命令层统一以 `CommandContext` 传递预算输入，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/SlowCommandGovernor.java`
  verify why.md#requirement-事务可观测慢命令治理统一基于上下文-scenario-infostats-provider-可基于-session-访问连接态
  depends on task 4.2
- [√] 5.3 迁移其余命令族（String/Hash/List/Set/ZSet/HLL）到 `(Command, CommandContext)`，修改（按 2-3 文件/批次推进）：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HashCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/SetCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/HllCommands.java`
  verify why.md#requirement-replywriter-纯输出端口不再暴露-session-scenario-命令层不再出现-outsession-读取
  depends on task 4.2

## 6. yierdis-core（可观测性扩展点：ServerInfoProvider）
- [√] 6.1 将 `ServerInfoProvider` 改为基于 `CommandContext`（info/stats/memoryStats），并更新调用点，修改：
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ServerInfoProvider.java`
  - `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`
  verify why.md#requirement-事务可观测慢命令治理统一基于上下文-scenario-infostats-provider-可基于-session-访问连接态
  depends on task 5.1

## 7. yierdis-server（executor/handler/info provider/boot 适配）
- [√] 7.1 修改 `NettyCommandExecutor`：使用新 `ReplyWriterFactory` 创建 writer，并组装/复用 `CommandContext` 传入 processor，修改：
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  verify why.md#requirement-replywriter-纯输出端口不再暴露-session-scenario-writer-实现不再持有连接态
  depends on task 2.1
  depends on task 4.2
- [√] 7.2 修改 `NettyServerInfoProvider`：通过 `CommandContext.session()` 获取 `ServerConnectionState`（不再从 writer 读取），修改：
  - `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
  verify why.md#requirement-事务可观测慢命令治理统一基于上下文-scenario-infostats-provider-可基于-session-访问连接态
  depends on task 6.1
- [√] 7.3 修改 `YierdisServerBootstrap` 与 handler：适配 `SlowCommandGovernor`/writer factory 新签名，修改：
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  verify why.md#requirement-replywriter-纯输出端口不再暴露-session-scenario-命令层不再出现-outsession-读取
  depends on task 7.1

## 8. yierdis-core tests（embedded/单测可测试性）
- [√] 8.1 更新 `FastTestClient`：不再依赖 writer 携带 session，改为显式 `CommandContext`，修改：
  - `yierdis-core/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
  verify why.md#requirement-路由输入解耦db-路由不依赖-replywriter-scenario-embedded单测缺少连接态时默认-db0
  depends on task 4.2

## 9. Security Check
- [√] 9.1 执行安全检查（G9）：确认 context 不跨线程共享、server 连接态不通过 writer “旁路”读取、busy/error 回包路径不泄漏敏感信息；记录结论

## 10. Documentation Update
- [√] 10.1 同步更新知识库文档以匹配新边界（ReplyWriter 不再承载 session；路由/INFO/事务基于 context），至少更新：
  - `helloagents/wiki/modules/protocol.md`
  - `helloagents/wiki/modules/command.md`
  - `helloagents/wiki/modules/server.md`
- [√] 10.2 更新 `helloagents/CHANGELOG.md`：记录本次边界变化、风险提示与回归结果

## 11. Testing
- [√] 11.1 运行 `mvn test` 全量回归，并记录关键结果（BUILD SUCCESS / 失败原因）
- [√] 11.2 运行源码约束扫描：`rg \"\\.session\\(\\)\"` 确认不再存在 `out.session()` 使用点（仅允许对 `CommandContext.session()` 的调用）
