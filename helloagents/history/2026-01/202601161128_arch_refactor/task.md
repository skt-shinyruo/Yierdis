# Task List: 架构重构（命令/DB 解耦 + 执行模型硬化 + 预算/背压可解释化）

Directory: `helloagents/plan/202601161128_arch_refactor/`

---

## 1. yierdis-protocol / yierdis-protocol-netty：retained bytes 与 frame 生命周期能力
- [√] 1.1 为 `RespFrame` 增加 `retainedBytes()`（默认回退到 `length()`），并在 `NettyRespFrame` 中实现更接近真实 retained 内存的估算，文件：`yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespFrame.java`、`yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/NettyRespFrame.java`，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理-scenario-bytes-预算口径更接近实际-retained-memory
- [√] 1.2 增强 `RespCommandBuilder`：提供安全的 frame 替换（关闭旧 frame，避免泄漏），为 server 侧 compaction 提供支撑，文件：`yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespCommandBuilder.java`，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理-scenario-必要时支持-frame-compaction降低驻留体积
- [√] 1.3 新增/调整 codec 回归测试：确保 recycle/close 路径正确、retainedBytes 口径稳定，文件：`yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/` 下新增/增强测试类，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理

## 2. yierdis-server：执行路径单入口 + 公平调度 + compaction
- [√] 2.1 强制 server 只走执行器路径：移除/限制 `YierdisFastCommandHandler` 的“直接执行”构造方式，确保不可能绕过 executor，文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`，验证 why.md#requirement-db-单线程语义硬化防误用-scenario-server-侧无法绕过执行器直接访问-db
- [√] 2.2 在 bootstrap 侧显式收敛装配：`YierdisServer` 总是构造并注入 executor（包括测试/工具场景），文件：`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`，验证 why.md#requirement-db-单线程语义硬化防误用
- [√] 2.3 改造 `NettyCommandExecutor`：引入连接级公平调度（per-channel queue + round-robin）并保留 flush coalescing，文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理-scenario-多连接公平调度避免热点连接挤占
- [√] 2.4 在 executor enqueue 路径接入 retainedBytes 口径，并实现可配置的 frame compaction（必要时复制为精确长度 frame），文件：`yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理-scenario-必要时支持-frame-compaction降低驻留体积
- [√] 2.5 新增公平性/compaction/backpressure 单测：多连接不饿死、compaction 后底层 buf 释放、busy 行为可预期，文件：`yierdis-server/src/test/java/yier/bubu/redis/` 下新增测试类，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理

## 3. yierdis-core：DB 单线程语义硬化 + 内部职责拆分
- [√] 3.1 将 `YierdisDb` 改为严格线程绑定：未 bind 或跨线程访问 fail-fast；并提供最小可用的测试辅助入口（例如测试基类中 bind），文件：`yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`，验证 why.md#requirement-db-单线程语义硬化防误用-scenario-db-未绑定线程时访问立即失败
- [√] 3.2 抽取 `DbThreadGuard/DbMemoryAccounting/YierdisMemoryStats`（package-private）并在 `YierdisDb` 提供 `memoryStats()`：将 thread guard 与预算分解职责从 `YierdisDb` 拆出，文件：`yierdis-core/src/main/java/yier/bubu/redis/db/` 下新增组件与调整 `YierdisDb.java`，验证 why.md#requirement-maxmemory-预算可解释与稳定
- [√] 3.3 将 maxmemory 相关“结构性开销估算项”显式化（rehash 双表、expires/index 等），并提供可观测的预算分解对象（供命令/日志输出复用），文件：`yierdis-core/src/main/java/yier/bubu/redis/db/` 相关新增类 + `YierdisDb.java`，验证 why.md#requirement-maxmemory-预算可解释与稳定-scenario-oom-淘汰行为可解释
- [√] 3.4 增强/新增诊断命令：实现 `MEMORY STATS` 并联动 DB `memoryStats()`，文件：`yierdis-core/src/main/java/yier/bubu/redis/command/` 下新增命令实现并联动 DB introspection，验证 why.md#requirement-maxmemory-预算可解释与稳定

## 4. yierdis-core：命令层拆分（Registry + Context + Domain Commands）
- [√] 4.1 新增 `CommandRegistry/CommandSupport` 基础设施，并保持现有行为不变（先落地骨架），文件：`yierdis-core/src/main/java/yier/bubu/redis/command/` 下新增类，验证 why.md#requirement-命令层模块化与可测试性
- [√] 4.2 迁移通用命令（PING/ECHO/HELLO/SELECT/COMMAND/FLUSHDB），文件：`yierdis-core/src/main/java/yier/bubu/redis/command/` 下新增 `ServerCommands`（或等价拆分）并调整 registry，验证 why.md#requirement-命令层模块化与可测试性-scenario-新增命令修改命令实现不触碰-db执行器
- [√] 4.3 迁移 Key/TTL 相关命令（TYPE/MEMORY/OBJECT/KEYS/DEL/EXISTS/EXPIRE/TTL/MEMORY STATS），文件：同上（拆分到 `KeyCommands`），验证 why.md#requirement-命令层模块化与可测试性
- [√] 4.4 迁移 String/Bitmap 命令族（SET/GET/APPEND/STRLEN/INCR/DECR/SETBIT/GETBIT/BITCOUNT），并将 HLL 命令族拆到 `HllCommands`（PF*），文件：同上（`StringCommands`/`HllCommands`），验证 why.md#requirement-命令层模块化与可测试性-scenario-低分配热路径保持可控
- [√] 4.5 迁移 List/Hash/Set/ZSet 命令族（LPUSH/RPUSH/LRANGE/LPOP/RPOP/H*/S*/Z*），文件：同上（拆分到对应 domain commands），验证 why.md#requirement-命令层模块化与可测试性
- [√] 4.6 清理原 `YierdisFastCommandProcessor` 的遗留聚合代码：保留为 dispatcher façade，文件：`yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`，验证 why.md#requirement-命令层模块化与可测试性

## 5. yierdis-args / yierdis-server：新增配置项（SSOT）与兼容默认值
- [√] 5.1 在 `yierdis-args` 中新增/调整参数：调度策略、compaction 阈值、offheapKeysEnabled、诊断开关等，并补齐校验与 `toArgv` 输出，文件：`yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`、`yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java`，验证 why.md#requirement-零拷贝队列驻留风险与公平性治理 与 why.md#requirement-off-heap-风险收敛默认安全可选增强
- [√] 5.2 `ServerConfig` 承接新增参数并接入 server/executor/DB 构造，文件：`yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`、`yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`，验证 why.md#requirement-db-单线程语义硬化防误用

## 6. off-heap：默认安全 + 后端可用性校验 + 回归测试
- [√] 6.1 将 keys/expires 的 off-heap 使用改为显式开关（默认关闭），并在 DB 初始化时按开关选择 keyspace/expires 实现，文件：`yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`（以及相关 off-heap keyspace/expires 类若需调整），验证 why.md#requirement-off-heap-风险收敛默认安全可选增强-scenario-默认-off-heap只覆盖-value低风险收益路径
- [√] 6.2 将 off-heap 后端加载升级为启动期强校验（优先 ServiceLoader provider；保底为 startup 预检），文件：`yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java`（以及各后端模块的 provider/配置），验证 why.md#requirement-off-heap-风险收敛默认安全可选增强-scenario-后端不可用时启动期直接失败
- [√] 6.3 增强 off-heap 泄漏/关闭路径回归：在异常路径与关闭时确保 allocator.usedBytes 归零或符合预期，文件：`yierdis-core/src/test/java/yier/bubu/redis/db/` 下新增/增强测试，验证 why.md#requirement-off-heap-风险收敛默认安全可选增强

## 7. Security Check
- [√] 7.1 执行安全检查并补齐必要防护：输入上限、错误信息净化、所有异常路径资源回收、避免无界队列/内存增长、off-heap close 路径可达性，覆盖 server/protocol/core，验证 why.md#risk-assessment

## 8. Documentation Update（知识库同步）
- [√] 8.1 更新知识库：`helloagents/wiki/arch.md`（新增 ADR 索引/执行器公平性/retainedBytes 口径）、`helloagents/wiki/modules/command.md`、`helloagents/wiki/modules/db.md`、`helloagents/wiki/modules/server.md`、`helloagents/wiki/modules/protocol-netty.md`、`helloagents/wiki/modules/offheap.md`，并记录到 `helloagents/CHANGELOG.md`

## 9. Testing
- [√] 9.1 运行 `mvn test`（全模块），并补充 `scripts/smoke.sh` 的端到端验证：redis-cli 基本命令、RESP3 HELLO 兼容、busy/backpressure 行为可复现
