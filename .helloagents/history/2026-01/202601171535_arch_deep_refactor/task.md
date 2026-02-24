# 任务列表：Yierdis 架构深度重构（Pipeline 化 + 执行器组件化 + 连接态解耦）

Directory: `helloagents/plan/202601171535_arch_deep_refactor/`

---

## 1. yierdis-protocol-netty（协议严格性与会话边界）
- [√] 1.1 明确 request decoder 的允许输入集合（array + inline），对 RESP reply/RESP3 前缀统一 protocol error，验证 why.md#核心场景-协议严格性与错误语义统一
- [√] 1.2 精简 `ConnectionContext`：仅保留协议协商与通用统计容器，移除执行器调度状态占位，验证 why.md#核心场景-connectioncontext-解耦（协议会话-ssot）
- [√] 1.3 补充协议侧专项测试：非法前缀/超长行/嵌套深度/边界值，验证 why.md#核心场景-协议严格性与错误语义统一

## 2. yierdis-server（Pipeline 与执行器组件化）
- [√] 2.1 定义 server 内部的“处理流水线”结构（decode→schedule→execute→encode），梳理责任边界并落地骨架类，验证 why.md#核心场景-执行器组件化与复杂度下降
- [√] 2.2 将 `NettyCommandExecutor` 拆分为可测试组件（QueuePolicy/BytesBudget/Backpressure/Flush/Compaction/Stats），保持现有语义与配置项兼容，验证 why.md#核心场景-大改动后仍保持单线程命令语义
- [√] 2.3 将 per-channel 调度状态迁移为 server 私有 state（Channel.attr 或内部 map），并更新使用点（submit/drain/recovery），验证 why.md#核心场景-更换调度策略不触及协议层
- [√] 2.4 增强异常路径与 close-after-reply 路径的释放/跳过语义测试（QUIT/backlog/拒绝/关闭），验证 why.md#核心场景-资源生命周期与泄漏防护

## 3. yierdis-core（命令层与错误语义对齐，按需）
- [-] 3.1 统一 “protocol error / internal error / busy / OOM” 的错误映射规范，并用测试固定输出语义，验证 why.md#需求背景
  > Note: 当前错误映射主要由 `YierdisFastCommandProcessor`（命令/DB 异常映射）与 `YierdisFastCommandHandler`（解码异常→协议错误返回并关闭）覆盖，本次未额外引入新的错误码体系以避免兼容性变化。
- [-] 3.2 评估并拆分 `YierdisDb` 内聚度（仅做最小可行拆分：把内存统计/淘汰/TTL 的边界更清晰），验证 why.md#变更内容
  > Note: `YierdisDb` 属于高风险大拆分点，本次聚焦在协议严格性与 server/执行器解耦；DB 拆分建议作为单独方案包进行（避免与执行器/协议变更耦合回归）。

## 4. 可观测性与重复逻辑收敛（跨模块）
- [√] 4.1 将版本资源读取逻辑收敛为可复用工具（避免多处复制），并更新 server/client/tests 使用点，验证 why.md#需求背景
- [√] 4.2 更新 `helloagents/wiki/arch.md` 与相关 module 文档，记录新的 Pipeline/连接态边界与 ADR，验证 why.md#影响范围

## 5. Security Check
- [√] 5.1 执行安全检查（输入校验、错误消息净化、资源释放、反压与预算的 DoS 防护），并记录关键结论到 how.md#security-and-performance

## 6. Testing
- [√] 6.1 运行 `mvn test` 并补齐新增测试覆盖；如涉及时间相关（TTL/清理），确保测试可重复、无 `Thread.sleep` 依赖
