<!-- migrated_from: history/2026-01/202601081106_netty_executor_integration/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: Netty 执行器融合改造（覆盖问题 1/2/3/4/5）

## Requirement Background
当前已实现的 Yierdis 具备基本的 RESP2 请求解析、命令执行与 off-heap 存储能力，但与 Redis 的成熟实现对比，在性能、稳定性与兼容性上存在系统性问题，主要集中在以下 5 类（本次变更要求 1/2/3/4/5 全部落地）：

1. 端到端零拷贝/低分配未贯通：fast path 解码后仍在关键写路径大量物化 `byte[]`，off-heap 写入存在“双拷贝”风险。
2. 单线程语义的实现方式引入额外开销：每条命令跨线程投递+每命令 flush，不利于 pipeline 吞吐与尾延迟。
3. 背压闭环不完整：仅靠执行队列容量拒绝请求，缺少连接级读控制（autoRead/watermark 等），在高压下可能出现自激式 CPU/带宽消耗与内存占用上升。
4. RESP3 与压测可信度：RESP3 当前仅停留在握手级；bench 工具默认“跳过响应”而不判断错误，可能掩盖 `-ERR`（例如 busy）导致结果失真。
5. maxmemory/过期/淘汰策略在压力场景下的抖动与饥饿风险：写入路径同步淘汰可能放大尾延迟；维护任务在队列压力下可能被饿死。

## Change Content
1. 以 Netty 的 `EventExecutorGroup(单线程)` 融合命令执行：替代自建阻塞队列执行线程模型，保持 Redis 风格全局单线程语义，同时让 write/flush 语义更自然地落在 Netty 体系内。
2. 引入“连接级 + 执行级”背压闭环：基于 pending/队列阈值切换 `autoRead`，并提供明确的忙时响应与恢复机制，避免请求体在内存中无界堆积。
3. 优化写回策略：减少 per-command flush，支持批量写入后一次 flush（可配置/可测试）。
4. bench 工具改造：将 `-ERR` 明确计入 errors；可选开启“响应类型/协议校验”；补齐 RESP3 基础类型跳过逻辑，避免未来扩展时压测工具失真。
5. 内存/淘汰/过期：在压力场景下提供更稳定的时间预算与触发策略，避免维护任务饥饿；将可观测指标暴露给 bench/日志以便回归比较。

## Impact Scope
- **Modules:**
  - `yierdis-server/`（网络/执行模型/背压/flush/DB 写入路径）
  - `yierdis-bench/`（压测统计与校验）
  - `yierdis-offheap/`（可能需要补齐 off-heap 写入的“直接拷贝”能力或计量接口）
- **Files (expected):**
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/CommandExecutor.java`（替换/保留兼容/迁移测试）
  - `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisObject.java`
  - `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`
  - 相关单元测试文件（新增/调整）
- **APIs:** 对外协议仍为 Redis RESP2/部分 RESP3（HELLO 3），重点调整内部执行与背压策略。
- **Data:** 不引入持久化格式变化；重点关注内存与过期/淘汰行为的一致性与可验证性。

## Core Scenarios

### Requirement: 端到端低分配与 off-heap 写入
**Module:** yierdis-server
在 `SET/APPEND` 等写路径中减少 `byte[]` 中转；off-heap backend 尽量避免“双拷贝”。

#### Scenario: off-heap backend 下高频 SET
在相同 keyspace 与 valueSize 下，写路径不应产生额外的堆对象洪峰，吞吐与尾延迟改善可在 bench 中复现。

### Requirement: pipeline 吞吐与 flush 合并
**Module:** yierdis-server
减少每命令 flush，支持批量写后一次 flush，并保持响应顺序一致。

#### Scenario: pipeline=16 / clients=200
吞吐曲线应更平滑；在高压下错误率可控且不出现连接挂死。

### Requirement: 背压闭环
**Module:** yierdis-server
当执行侧 backlog 超阈值时暂停读；恢复后自动继续读，并保持忙时错误可观测。

#### Scenario: backlog 触发与恢复
在队列/阈值触发时返回 `-ERR busy`（或等价错误），同时不允许请求体在内存中无界增长。

### Requirement: RESP3/bench 校验
**Module:** yierdis-bench / yierdis-server
bench 统计必须将 `-ERR` 计入 errors；可选进行协议类型校验；支持 RESP3 基础类型跳过以适配未来扩展。

#### Scenario: 强制制造错误并被统计
当服务端返回错误（例如 busy、语法错误、OOM），bench 必须在输出中反映 errors，避免“吞吐虚高”。

### Requirement: maxmemory/过期/淘汰在压力下稳定
**Module:** yierdis-server
淘汰与过期清理具时间预算；维护任务不会在压力下长期饥饿；maxmemory 行为与 Redis 语义尽量对齐。

#### Scenario: maxmemory=7GiB + allkeys-lru
在持续写入下触发淘汰，服务端不应长时间卡顿；errors 可解释且稳定。

## Risk Assessment
- **Risk:** 执行模型切换可能引入响应乱序、死锁、吞吐下降或尾延迟抖动。
  - **Mitigation:** 以测试覆盖顺序一致性/背压/压力场景；bench 引入错误统计与可选校验；分阶段落地并做 A/B 对比。
- **Risk:** autoRead 切换与 backlog 阈值不当导致吞吐下降或连接饥饿。
  - **Mitigation:** 引入滞回阈值（high/low watermark）与可配置参数；默认值保守；提供日志/指标辅助调参。
- **Risk:** off-heap 直接写入路径可能引入内存泄漏或越界。
  - **Mitigation:** 复用现有 allocator 的生命周期语义；新增泄漏/边界测试；保持严格参数校验与异常路径 close。

