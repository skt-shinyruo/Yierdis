# Task List: 执行器组件化与不变量测试补齐

Directory: `helloagents/plan/202601172157_executor_modularize/`

---

## 1. 队列调度组件化
- [√] 1.1 新增 `NettyExecutorTask`（ctx/cmd/retainedBytes），替代 `NettyCommandExecutor.Task`
- [√] 1.2 新增 `NettyExecutorTaskQueue`：封装 GLOBAL/FAIR 分支与 round-robin 逻辑，替代内联/内部类
- [√] 1.3 收敛 `NettyExecutorChannelState` 队列泛型为 `NettyExecutorTask`
- [√] 1.4 `NettyCommandExecutor` 重构为调用新组件（保持行为一致）

## 2. 背压控制组件化
- [√] 2.1 新增 `NettyExecutorBackpressureController`：抽取 autoRead disable/enable + global recovery + tracking
- [√] 2.2 `NettyCommandExecutor` 改为通过 controller 触发与恢复背压（保持阈值判定点不变）

## 3. 不变量回归测试
- [√] 3.1 增强 `NettyCommandExecutorTest`：bytes budget 拒绝路径 + 预算归零验证
- [√] 3.2 增强 `NettyCommandExecutorTest`：bytes watermark 背压 disable/enable 闭环验证

## 4. 验证与文档同步
- [√] 4.1 `mvn test` 全量回归
- [√] 4.2 同步知识库（server 模块边界/组件说明），更新 `helloagents/CHANGELOG.md`
- [√] 4.3 迁移方案包到 `helloagents/history/2026-01/` 并更新 `helloagents/history/index.md`
