<!-- migrated_from: history/2026-01/202601171535_arch_deep_refactor/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# 变更提案：Yierdis 架构深度重构（执行器组件化 + 连接态解耦 + 协议严格性与资源生命周期加固）

## 需求背景

Yierdis 当前的总体分层方向是正确的：`core/protocol/args` 作为 SSOT，`server/client/bench` 作为装配层，Netty 相关实现下沉到 `*-netty` 模块。

但随着功能演进，仍出现以下系统性问题，导致维护成本与风险上升：

1. **执行器过度中心化**：`NettyCommandExecutor` 承载了排队/预算/反压/公平调度/flush 合并/compaction/统计/close-after-reply 等多种职责，复杂度高、可测试性弱，后续改动容易引入非预期行为。
2. **连接态 SSOT 边界模糊**：`ConnectionContext` 同时承载协议协商与执行器调度状态，形成“协议层 ↔ server 调度”语义耦合，未来替换执行策略时容易产生牵连修改。
3. **协议严格性与错误语义不一致**：请求解码对某些前缀/非法输入的处理存在“被当作 inline 命令”或“错误类型不一致”的可能，排障成本偏高。
4. **资源生命周期需要更强的护栏**：ByteBuf/RespFrame 的 ownership 与异常路径释放需要被持续验证；小 frame retain 大底层 buf 的驻留问题虽已缓解，但需要更系统化的回归用例与可观测性基线。
5. **重复逻辑与副作用边界**：版本资源读取、参数解析错误输出等存在重复与副作用，增加了“行为漂移”和测试集成成本。

## 变更内容

1. **引入“请求处理流水线”边界**：将 server 的处理过程显式拆分为 decode → schedule → execute → encode 四段，使职责边界可见、可测试、可替换。
2. **执行器组件化**：将 `NettyCommandExecutor` 拆分为多个可独立验证的组件（队列/调度、预算、反压、flush 合并、compaction、统计），保留单线程 DB 语义不变。
3. **连接态解耦**：`ConnectionContext` 聚焦为“协议会话 + 连接级统计容器”，把执行器调度状态迁移到 server 私有 state（仍保持单一 attr 方案或显式双 attr 方案，并在 ADR 中确定）。
4. **协议严格性统一**：明确 request decoder 的“允许输入集合”（array-of-bulk-strings + inline command），对 RESP reply 前缀或无效字节统一返回 protocol error，并补充测试。
5. **资源生命周期与可观测性加固**：新增专项测试覆盖异常路径释放、QUIT/backlog 跳过、反压滞回、frame compaction 触发；同时将关键指标输出与文档对齐。

## 影响范围

- **Modules：**
  - `yierdis-server`
  - `yierdis-protocol-netty`
  - `yierdis-protocol`
  - `yierdis-core`
  - `yierdis-client`（可选：仅当需要统一工具侧行为/诊断输出）
  - `helloagents/wiki/*`（同步架构与模块文档）
- **Files（预估）：**
  - server：`NettyCommandExecutor.java`、`YierdisFastCommandHandler.java`、`YierdisServerBootstrap.java` 及新增组件类
  - protocol-netty：`ConnectionContext.java`、`RespCommandDecoder.java`
  - protocol：`RespWriter.java`（仅当需要更明确的 session/close-after-reply 合约表达）
  - core：命令层/错误映射/INFO 输出（按需）
- **APIs：**对外 RESP 行为应保持兼容或“更严格但可解释”，并在文档与测试中固定语义
- **Data：**不涉及持久化结构变更（仍为内存 DB）

## 核心场景

### Requirement: 执行器组件化与复杂度下降
**Module:** yierdis-server
将执行器拆分为可测试组件，降低改动风险与维护成本。

#### Scenario: 大改动后仍保持单线程命令语义
在高并发请求下，所有命令仍按连接内 FIFO 语义与 DB 单线程约束执行。
- 预期：功能行为与现状一致
- 预期：性能不明显回退（至少不出现数量级下降）

### Requirement: ConnectionContext 解耦（协议会话 SSOT）
**Module:** yierdis-protocol-netty / yierdis-server
连接态 SSOT 仅表达协议协商与通用统计，执行器调度状态归属 server。

#### Scenario: 更换调度策略不触及协议层
替换/调整执行器调度实现时，不需要改动 `yierdis-protocol-netty` 的协议会话对象语义。
- 预期：protocol-netty 仍只承担 codec 与 session 适配
- 预期：server 侧可独立演进调度策略

### Requirement: 协议严格性与错误语义统一
**Module:** yierdis-protocol-netty
对非法前缀与不允许的输入统一返回 protocol error，并保证不会误路由为 inline 命令。

#### Scenario: 请求流遇到 RESP reply 前缀
客户端错误发送 `+OK`/`%...`/`_` 等前缀时，server 返回明确协议错误，并可选择关闭连接。
- 预期：错误消息可诊断（避免 “unknown command” 误导）
- 预期：不会导致后续连接状态错乱

### Requirement: 资源生命周期与泄漏防护
**Module:** yierdis-server / yierdis-protocol-netty
确保所有异常/拒绝/关闭路径都能释放 frame/ByteBuf，并对“retainedBytes 预算”口径提供回归锚点。

#### Scenario: backpressure + bytes budget + compaction 组合
在小 frame retain 大底层 buf 的情况下，能触发 compaction 或预算拒绝，并保持连接可恢复。
- 预期：不会无限制驻留内存
- 预期：反压恢复符合滞回逻辑

## 风险评估

- **风险：行为兼容性变化**（协议更严格/错误语义改变）
  - 缓解：以测试固定语义；对外行为变化写入文档；必要时提供兼容开关（默认安全/严格）
- **风险：性能回退**（拆分组件导致额外对象/间接调用）
  - 缓解：组件化不等于大量对象化；关键路径保持无分配/低分配；基准测试与压测工具回归
- **风险：资源释放责任变化**（ByteBuf/RespFrame ownership 边界调整）
  - 缓解：在任务分解中优先补“释放/异常路径”测试；保持 try-with-resources 与 close 合约清晰

