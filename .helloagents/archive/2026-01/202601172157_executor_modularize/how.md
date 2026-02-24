<!-- migrated_from: history/2026-01/202601172157_executor_modularize/how.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# How: 执行策略与改造方案

## 方案概览
采用“最小改动的组件化”策略：

1. **抽取队列调度组件**
   - 新增 `NettyExecutorTask`（承载 ctx/cmd/retainedBytes）
   - 新增 `NettyExecutorTaskQueue`（封装 GLOBAL/FAIR 分支、round-robin、公平队列 drain leftover）
   - `NettyExecutorChannelState` 的队列泛型从 `Object` 收敛为 `NettyExecutorTask`，消除 cast 与潜在误用

2. **抽取背压控制组件**
   - 新增 `NettyExecutorBackpressureController`
   - 负责：
     - autoRead disable/enable（只在“执行器确实禁用过”的前提下恢复）
     - 全局恢复（当 global backpressure cleared 时，扫描并恢复满足条件的 channel）
     - 维护 `channelsWithAutoReadDisabled` 容器与 close cleanup
   - `NettyCommandExecutor` 仅保留“何时触发背压”的判定点，把“如何实现背压”下沉

3. **补齐不变量测试**
   - 新增/增强 `NettyCommandExecutorTest` 覆盖：
     - queueMaxBytes 拒绝路径：第二条命令因 bytes budget 被拒绝，且预算/计数最终归零
     - bytes watermark 背压：单条大请求触发 bytes 背压，执行完成后恢复 autoRead
     - 通过 `statsSnapshot()` 与 `ServerConnectionState` counters 断言关键不变量

## 风险与规避
- **并发/时序不稳定**：测试继续使用 `DefaultEventExecutorGroup(1)` + latch 控制 drain tick，避免 flakiness。
- **行为漂移**：组件抽取只迁移代码位置，不改变判定条件与计数更新顺序；所有新逻辑由现有测试 + 新增不变量测试双重覆盖。

