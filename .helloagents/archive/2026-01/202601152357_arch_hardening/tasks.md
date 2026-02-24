<!-- migrated_from: history/2026-01/202601152357_arch_hardening/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 架构加固（Shutdown / Backlog Bytes / Split-package / Off-heap Capabilities / Version SSOT）

Directory: `helloagents/plan/202601152357_arch_hardening/`

---

## 1. server：优雅关停（Graceful Shutdown）
- [√] 1.1 定义并实现执行器关停契约（可等待/可测试）：在 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 增加 graceful shutdown + drain 完成信号，verify why.md#requirement-优雅关停graceful-shutdown
- [√] 1.2 调整服务端关停顺序：在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java` 中实现“停止接入 → 停止读入 → drain executor → executor 线程内关闭 DB → 等待线程池退出”，verify why.md#requirement-优雅关停graceful-shutdown
- [√] 1.3 增强回归测试：新增或更新 `yierdis-server/src/test/java/yier/bubu/redis/*` 覆盖 in-flight backlog 下关停无竞态与资源回收，verify why.md#scenario-in-flight-backlog-下关停

## 2. protocol/protocol-netty：retained bytes 与 decoder 上限可配置
- [-] 2.1 为 `RespCommand` 增加 retained-bytes/帧长度字段与只读 getter：`yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespCommand.java`，verify why.md#requirement-backlog-bytes-budget
  > Note: 未在 `RespCommand` 增加 retained-bytes 字段；改为在 `RespFrame.length()` 提供稳定帧长度（SSOT 口径），避免扩大 `RespCommand` 字段面与跨模块耦合。
- [-] 2.2 在 Netty 解码器写入 retained bytes：`yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/RespCommandDecoder.java`（RESP2 multi-bulk 与 inline 两条路径都覆盖），verify why.md#scenario-少量大包积压不应打穿内存
  > Note: retained bytes 由 decoder 构造 `NettyRespFrame` 时固化为 `frame.length()`（multi-bulk 使用 retainedSlice 长度；inline 使用 materialized frame 长度），executor 以 `cmd.frame().length()` 做 bytes 预算。
- [√] 2.3 使 decoder 上限可配置并对外暴露：为 `RespCommandDecoder`（以及 client 用的 `RespDecoder`）提供 public 参数化构造或 factory，verify why.md#requirement-backlog-bytes-budget

## 3. server/args：bytes-based backlog 预算与反压策略落地
- [√] 3.1 在 `yierdis-args` 增加 bytes 预算与 decoder 上限参数（参数 SSOT）：`yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`，并补齐校验与默认值策略，verify why.md#requirement-backlog-bytes-budget
- [√] 3.2 透传配置并应用到 server：更新 `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java` 与 `YierdisServer.java`，使用配置构造 decoder/executor，verify why.md#requirement-backlog-bytes-budget
- [√] 3.3 在 `NettyCommandExecutor` 中实现全局/连接级 bytes 预算计数、阈值判断与 autoRead 滞回，verify why.md#requirement-backlog-bytes-budget
- [√] 3.4 增加测试：构造大 bulk 触发 bytes 预算的 fail-fast/backpressure 行为，verify why.md#scenario-少量大包积压不应打穿内存

## 4. protocol-netty：消除 split-package（包迁移）
- [√] 4.1 将 netty codec/adapters 迁移到独立包（建议 `yier.bubu.redis.protocol.netty`）：更新 `yierdis-protocol-netty/src/main/java/...` 的 package 与引用，verify why.md#requirement-消除-split-package边界护栏
- [√] 4.2 更新仓库内所有引用点：`yierdis-server`、`yierdis-client`、`yierdis-bench`、tests 的 import 与使用，verify why.md#scenario-构建与测试通过
- [√] 4.3 更新知识库文档：`helloagents/wiki/modules/protocol.md`、`helloagents/wiki/modules/protocol-netty.md`、`helloagents/wiki/arch.md`（如需更新依赖图/ADR 索引），verify why.md#requirement-消除-split-package边界护栏

## 5. off-heap：capabilities/SPI 与 DB 集成点统一
- [√] 5.1 在 `yierdis-offheap-api` 定义最小 capabilities/SPI（例如 dbSupport/capabilities）：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/*`，verify why.md#requirement-off-heap-capabilities-统一
- [√] 5.2 `yierdis-offheap-unsafe` 提供 unsafe DB 集成能力实现（keyspace/expires factory 等），并对外暴露给 core（避免 core 直接 `instanceof`），verify why.md#requirement-off-heap-capabilities-统一
- [√] 5.3 `yierdis-core` 构造路径改造：在 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java` 移除 `instanceof YierdisUnsafeOffHeapAllocator` 分支，改为 capabilities 驱动，verify why.md#scenario-不同后端行为可预测
- [√] 5.4 回归测试：覆盖 delete/expire/evict/shutdown 下 off-heap usedBytes 回到基线，verify why.md#requirement-off-heap-capabilities-统一

## 6. protocol：Version SSOT（构建元数据注入）
- [√] 6.1 增加构建时版本资源注入（Maven resource filtering 或等价方式）：更新根/模块 `pom.xml` 与资源文件；在 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`（HELLO）读取该版本，verify why.md#requirement-version-ssot
- [√] 6.2 文档同步：更新 `README.md`、`helloagents/wiki/api.md`（如涉及行为差异/新增参数），以及 `helloagents/CHANGELOG.md` 记录变更，verify why.md#requirement-version-ssot

## 7. Security Check
- [√] 7.1 执行安全检查（G9）：输入上限与错误输出（CRLF 注入）、资源释放（ByteBuf/off-heap）、关停顺序、参数校验一致性

## 8. Testing
- [√] 8.1 `mvn test` 全量回归（含 `foreign-memory` profile 的说明/可选验证）
- [√] 8.2 `./scripts/smoke.sh` 最小链路验证（server/cli/bench strictReplies）
