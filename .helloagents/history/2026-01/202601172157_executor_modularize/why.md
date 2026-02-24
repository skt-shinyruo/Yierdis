# Why: NettyCommandExecutor 组件化与不变量测试补齐

## 背景
当前 `yierdis-server` 的 `NettyCommandExecutor` 负责：
- I/O 线程提交（trySubmit）与全局/连接级预算校验
- 公平调度（per-channel queue + round-robin）
- drain loop（批量执行/flush 合并）
- backpressure（autoRead disable/enable，含全局恢复）
- close-after-reply（QUIT）与跳过 post-QUIT backlog
- 统计（INFO/STATS）

虽然此前已抽取了部分组件（如 backlog bytes 预算、frame compaction、per-channel state），但执行器仍然是“单体大类”，并且缺少针对“预算/背压/关闭”关键不变量的专项回归测试。

## 目标（Success Criteria）
1. **层次清晰**：把“队列调度”和“背压控制”从 `NettyCommandExecutor` 中抽离为独立类（server 私有），降低单文件复杂度与修改半径。
2. **不变量可验证**：补齐回归测试，锁定以下行为不会退化：
   - backlog 预算（slots/bytes）不会泄漏（处理完成后回到 0；拒绝路径不产生负数/不丢释放）
   - bytes 背压水位线触发与恢复闭环（disable/enable autoRead）
   - 连接关闭（QUIT/close-after-reply）不会造成资源驻留或状态机卡死
3. **行为不变（或更安全）**：对外语义保持一致，不引入新的依赖方向，不破坏 `mvn test`。

## 非目标（Out of Scope）
- 大规模重写 drain loop（本次只做“结构抽取 + 不变量测试”，避免引入高风险行为变更）
- 引入新的线程模型或更复杂的调度策略
- 性能 benchmark 级别的调优（确保无明显退化即可）

