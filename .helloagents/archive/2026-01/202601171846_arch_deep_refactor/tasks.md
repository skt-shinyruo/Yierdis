<!-- migrated_from: history/2026-01/202601171846_arch_deep_refactor/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 架构深度重构（协议/执行/客户端分层与 SSOT 收敛）

Directory: `helloagents/plan/202601171846_arch_deep_refactor/`

---

## 1. 协议默认值 SSOT（RespLimits）
- [√] 1.1 新增协议默认上限 SSOT：实现 `RespLimits`（或等价类）在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespLimits.java`，verify why.md#r2-s1-defaults-consistent
- [√] 1.2 统一 `RespObjectParser` 默认值来源：改造 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java` 使用 `RespLimits`，verify why.md#r2-s1-defaults-consistent
- [√] 1.3 统一 Netty decoder 默认值来源：改造
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java`
  - `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java`
  使用 `RespLimits`，verify why.md#r2-s1-defaults-consistent
- [√] 1.4 统一 server args 的默认值与校验：改造 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 引用 `RespLimits` 的默认常量（保持注解 defaultValue 为编译期常量），verify why.md#r2-s1-defaults-consistent
- [√] 1.5 增加“一致性锁定”单测：新增/调整测试用例（优先放在 `yierdis-protocol` 或 `yierdis-args` 测试中）验证默认值一致与不会漂移，verify why.md#r2-s1-defaults-consistent

## 2. 连接态二分：协议会话 vs server 运行时状态
- [√] 2.1 将 `ConnectionContext`（或其替代实现）收敛为纯 `RespSession`：调整 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/ConnectionContext.java`（或引入新类并迁移引用），verify why.md#r1-s1-session-only
- [√] 2.2 在 server 模块新增连接运行时状态对象（pending/pendingBytes/closing/backpressure/counters）：新增 `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java`（名称可调整），verify why.md#r1-s2-server-state
- [√] 2.3 server pipeline 绑定两个对象：更新 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java` 在连接建立时分别 getOrCreate session 与 server state，verify why.md#r1-s1-session-only
- [√] 2.4 执行器改造：将 `NettyCommandExecutor` 从 `ConnectionContext` 迁移到 server connection state 读取/更新 pending/backpressure/counters，修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，verify why.md#r4-s1-invariants
- [√] 2.5 INFO/STATS 输出改造：将 `NettyServerInfoProvider` 中连接级指标读取改为 server connection state（协议相关字段仍从 session 获取），修改 `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`，verify why.md#r1-s2-server-state
- [√] 2.6 增加回归测试：覆盖 HELLO 2/3 协商、QUIT close-after-reply、backpressure 高低水位线切换（含 bytes 水位线），verify why.md#r1-s1-session-only

## 3. 编码输出 SSOT：RespWriter 唯一语义写出
- [√] 3.1 明确 RespEncoder 的去留策略（保留则 adapter 化；移除则迁移测试）：更新 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespEncoder.java`（或删除并替换使用点），verify why.md#r3-s1-one-writer
- [√] 3.2 更新/补齐 encoder 相关测试：调整
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespEncoderTest.java`
  - `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/RespRoundTripTest.java`
  使其验证 writer/encoder 行为一致与安全净化一致，verify why.md#r3-s1-one-writer

## 4. 执行器深度重构：组件化与不变量测试
- [ ] 4.1 执行器内部组件化：将队列调度/背压/预算/drain loop 拆分为独立类（每个类尽量 ≤1-2 文件变更），优先从 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 抽取，verify why.md#r4-s1-invariants
- [ ] 4.2 增加不变量单测：新增 server 侧单测覆盖 pending/bytes/slots 配平与 autoRead 状态机（含关闭与异常路径），verify why.md#r4-s1-invariants
- [ ] 4.3 性能/行为回归：使用 bench（可选）验证在默认配置下吞吐/延迟无明显退化；至少保证 `mvn test` 全绿，verify why.md#r4-s1-invariants

## 5. maxmemory 语义与文档护栏
- [√] 5.1 校验并补齐 maxmemory/估算口径说明：更新 `helloagents/wiki/data.md` 或 `helloagents/wiki/modules/db.md`（视现有结构）明确“估算/实占”的区别与字段含义，verify why.md#r5-s1-memory-stats-contract
- [√] 5.2 增加回归测试：覆盖 `MEMORY STATS` 字段稳定性（至少 key 列表与关键字段不缺失），verify why.md#r5-s1-memory-stats-contract

## 6. client/CLI 深度加固（同栈参数 + 资源边界）
- [√] 6.1 引入 picocli 并统一 CLI 参数建模：更新 `yierdis-client/pom.xml` 增加依赖，并新增 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCliArgs.java`（或等价类），verify why.md#r6-s1-cli-parsing
- [√] 6.2 改造 `YierdisCli` 使用 picocli：修改 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`，保持现有参数兼容并集中退出码决策，verify why.md#r6-s1-cli-parsing
- [√] 6.3 client response queue 边界化：修改 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`（例如有界队列/溢出关闭/异常唤醒），verify why.md#r6-s2-client-queue-bound
- [√] 6.4 client/CLI 回归测试：新增/调整测试覆盖超时关闭连接、异常路径 frame 回收、REPL quit 行为，verify why.md#r6-s2-client-queue-bound

## 7. Security Check
- [√] 7.1 执行安全检查（G9）：输入上限/错误净化/资源释放（ByteBuf/off-heap）/无明文敏感信息/无破坏性命令误用，记录发现与处理策略

## 8. Documentation Update
- [√] 8.1 同步知识库：更新 `helloagents/wiki/arch.md`（新增 ADR 索引或更新状态）、以及受影响模块文档（`helloagents/wiki/modules/*.md`），并在 `helloagents/CHANGELOG.md` 记录变更摘要

## 9. Testing
- [√] 9.1 全量回归：执行 `mvn test`（必要时补充 bench 运行说明），并记录关键验证点（RESP2/RESP3/inline/backpressure/maxmemory/client/CLI）
