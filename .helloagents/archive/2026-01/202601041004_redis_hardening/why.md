<!-- migrated_from: history/2026-01/202601041004_redis_hardening/why.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Change Proposal: redis_hardening

## Requirement Background

当前 Yierdis 在“已实现功能范围内”，与 Redis 相比仍存在若干实现层面的关键问题，会影响稳定性、安全性、可预期性与长期维护：

1. **I/O 与命令执行强耦合**：命令执行运行在 Netty `worker` event-loop 上，遇到重命令/大返回会拖住 I/O 与其他连接的读写。
2. **RESP 错误输出安全性不足**：部分错误消息会拼接用户输入内容，存在 CRLF 注入导致 RESP 响应拆分/污染的风险。
3. **maxmemory/内存统计口径不清晰且不准确**：尤其启用 off-heap 后，部分结构的估算返回 0，导致淘汰/拒写触发时机不可解释。
4. **双命令处理路径漂移风险**：对象式 `CommandProcessor` 与写出式 `YierdisFastCommandProcessor` 并存，长期维护容易出现行为分叉。
5. **off-heap/Unsafe 内存安全兜底不足**：依赖手写 close/free 路径，缺少系统性“泄漏可检测/可回归”的验证机制。

本变更目标：在不扩大命令集合的前提下（不引入持久化/复制/集群等新特性），将现有实现提升到“可观测、可解释、可维护、可防御”的工程基线。

## Change Content

1. 引入单线程命令执行器（保持 Redis 风格单线程语义），将 Netty I/O 与命令执行解耦
2. 对所有 RESP error 输出做统一安全净化（CR/LF 过滤 + 长度限制），消除响应拆分风险
3. 重新定义并实现 maxmemory 的“可解释统计口径”，并与 off-heap 的实际占用对齐
4. 收敛为单一命令执行路径（消除 fast/slow 漂移来源），并用测试锁定语义
5. 为 off-heap allocator 建立可回归的泄漏检测与资源释放验证

## Impact Scope

- **Modules:**
  - server
  - protocol
  - command
  - db
  - offheap
- **Files:**（预估）
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespWriter.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - `yierdis-offheap/*`（allocator 统计/测试辅助）
- **APIs:** 无新增外部 API；对错误输出与资源限制的行为将更严格且更可预期
- **Data:** 无数据结构对外格式变更；仅调整统计口径与执行架构

## Core Scenarios

### Requirement: I/O 与执行解耦（单线程命令语义保持）
**Module:** server / command
将命令执行从 Netty I/O event-loop 中移出，避免 event-loop 因重命令阻塞。

#### Scenario: 重命令不阻塞 I/O event-loop
条件：执行开销较大的命令（如全表扫描/大数组返回）
- 预期：I/O event-loop 线程只负责解码与投递，不直接执行重逻辑
- 预期：具备明确的保护栏（限量/限时/限输出），避免单条命令占用执行器过久

### Requirement: RESP 错误输出安全净化
**Module:** protocol / command
错误输出不得被客户端输入注入 CRLF 破坏 RESP framing。

#### Scenario: unknown command 不可注入额外 RESP 回复
条件：客户端构造包含 `\\r`/`\\n` 的命令名或参数触发错误
- 预期：服务端返回单条 `-ERR ...`，且错误消息已过滤 CR/LF 并限长

### Requirement: maxmemory 口径统一且可解释
**Module:** db / offheap
将 maxmemory 定义为“数据集（key/value/索引）内存预算”，并在启用 off-heap 时对齐实际 allocator 占用。

#### Scenario: off-heap 启用时淘汰/拒写触发时机可预测
条件：启用 off-heap 后端并写入数据直至触顶
- 预期：`MEMORY USAGE` 与淘汰/拒写逻辑口径一致
- 预期：避免明显的双计数或漏计数

### Requirement: 命令执行路径收敛与语义锁定
**Module:** command / test
消除“双实现”导致的行为漂移来源。

#### Scenario: 同一命令仅存在单一权威实现（SSOT）
条件：执行同一命令在不同通道/测试路径下
- 预期：结果一致且由同一实现产出

### Requirement: off-heap 资源释放可回归验证
**Module:** offheap / test
建立能在 CI 中稳定跑的泄漏检测与资源释放验证。

#### Scenario: DB shutdown 后 allocator usedBytes 归零（或符合预期）
条件：执行一系列写入/删除/过期/淘汰操作后关闭 DB
- 预期：off-heap allocator 的使用量回到基线（或按定义归零）
- 预期：测试可复现、可回归

## Risk Assessment

- **Risk:** 引入执行器与命令路径收敛涉及改动面大，可能引入语义回归或吞吐变化
  - **Mitigation:** 先建立语义回归测试（fast pipeline 端到端）再重构；分阶段落地；对关键路径加基准/边界测试
- **Risk:** maxmemory 口径调整可能改变既有触发时机
  - **Mitigation:** 明确文档口径；增加可观测输出；对关键阈值场景补测试
- **Risk:** off-heap 安全兜底增加测试/断言可能影响性能或引入平台差异
  - **Mitigation:** 将泄漏检测与重检查限定在测试/调试模式；生产路径保持零额外开销
