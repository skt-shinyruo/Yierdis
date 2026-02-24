# Task List: 架构问题 5 项治理（arch_refactor_5issues）

Directory: `helloagents/history/2026-01/202601161845_arch_refactor_5issues/`

---

## 1. 模块边界与依赖治理（R1）
- [√] 1.1 新增中立 bytes 抽象模块（`yierdis-bytes`），并更新父 `pom.xml` 模块列表；verify why.md#r1-s1
- [√] 1.2 在 `yierdis-offheap/api` 中提供对新 bytes 抽象的桥接/兼容（保留旧接口或提供适配器），保证现有模块可编译；verify why.md#r1-s1, depends on task 1.1
- [√] 1.3 调整 `yierdis-protocol` 的依赖关系：改为依赖 `yierdis-bytes`，移除对 `yierdis-offheap-api` 的直接依赖；verify why.md#r1-s1, depends on task 1.2
- [√] 1.4 将 `NettyCommandExecutor` 拆分为 core + netty adapter（或等价分层），降低调度/写回耦合；verify why.md#r1-s2, depends on task 1.3

## 2. 生命周期与资源安全（R2）
- [√] 2.1 统一 `RespCommand` 的回收语义（例如实现 `AutoCloseable` 并提供 `close()`=recycle），并在 handler/executor/decoder 关键路径使用统一模板；verify why.md#r2-s1
- [√] 2.2 补齐 server 启动失败路径的资源清理（allocator/db/executor/线程组），避免提前 return 导致泄漏；verify why.md#r2-s2, depends on task 2.1
- [√] 2.3 增加覆盖 busy/异常/关闭路径的资源释放测试（必要时启用更严格的 Netty leak detector）；verify why.md#r2-s1, depends on task 2.2

## 3. 背压/拒绝策略统一（R3）
- [√] 3.1 调整连接级背压语义：高水位触发“暂停读取”，不直接对已读命令返回 busy（除非全局预算/关闭）；verify why.md#r3-s1
- [√] 3.2 引入可恢复的全局 backpressure（queue/bytes 高低水位 + 恢复机制），避免 busy 风暴与永久禁读；verify why.md#r3-s2, depends on task 3.1
- [√] 3.3 新增/更新测试：覆盖全局过载、恢复读取、以及 GLOBAL/FAIR 两种调度策略下的行为一致性；verify why.md#r3-s2, depends on task 3.2

## 4. RESP3 能力对齐（最小子集）（R4）
- [√] 4.1 扩展协议对象模型：新增 `RespMap`（或等价结构）并加入 `RespType.MAP`；verify why.md#r4-s1
- [√] 4.2 扩展 client 侧 `RespDecoder`：支持解析 `%`（map）与 `_`（RESP3 null），并保持 RESP2 行为不变；verify why.md#r4-s1, depends on task 4.1
- [√] 4.3 更新 `YierdisCli` 输出：支持打印 map 与 RESP3 null（尽量贴近 redis-cli 的展示习惯）；verify why.md#r4-s2, depends on task 4.2
- [√] 4.4 增加集成测试：启动 server，client 执行 `HELLO 3` 并验证返回结构（map/null/基本命令）；verify why.md#r4-s1, depends on task 4.3

## 5. 可维护性/可观测性提升（R5）
- [√] 5.1 抽取 server bootstrap（将装配/资源管理从 `YierdisServer.main` 下沉到可复用类），并补齐对应测试入口；verify why.md#r5-s1
- [√] 5.2 增强 server 异常日志：在 `exceptionCaught` 记录 root cause（区分协议错误/内部错误），保持对外消息净化；verify why.md#r5-s2, depends on task 5.1
- [√] 5.3 统一 CLI 与 server inline parser 行为：抽共享解析器或共享同一套测试向量，避免规则漂移；verify why.md#r5-s3, depends on task 5.2

## 6. Security Check
- [√] 6.1 执行安全检查（输入校验、错误信息净化、敏感信息处理、资源泄漏与背压策略的 DoS 风险评估）
> Note: 已复核协议解码上限（maxBulk/maxArgs/maxLine）、RESP error 的 CRLF 注入净化、busy/异常路径命令帧回收、以及全局 backpressure 的可恢复性（避免永久禁读）。

## 7. Documentation Update
- [√] 7.1 更新 `README.md` 与 `helloagents/wiki/arch.md`（以及必要的模块文档），同步说明新模块/背压/RESP3 行为；depends on tasks 1-5
- [√] 7.2 更新 `helloagents/CHANGELOG.md` 记录本次重构与行为变化；depends on task 7.1

## 8. Testing
- [√] 8.1 运行 `mvn test` 并记录关键用例覆盖（executor/RESP3/backpressure）；depends on tasks 1-7
> Note: `mvn test` 通过；新增/更新覆盖点包括：全局 backpressure 恢复、RESP3 map/null 解码、HELLO 3 端到端回归、CLI 与 server inline parser 规则一致。
