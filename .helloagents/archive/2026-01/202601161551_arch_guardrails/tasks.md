<!-- migrated_from: history/2026-01/202601161551_arch_guardrails/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: 架构护栏与可观测性加固（arch_guardrails）

Directory: `helloagents/plan/202601161551_arch_guardrails/`

---

## 1. 文档与边界澄清（Knowledge Base）
- [√] 1.1 明确 `yierdis-protocol` 依赖 `yierdis-offheap-api` 的原因与边界约束：更新 `helloagents/wiki/arch.md`
- [√] 1.2 补充 protocol 文档：解释 `RespFrame/RespSession` 与 bytes 抽象的关系，更新 `helloagents/wiki/modules/protocol.md`（依赖任务 1.1）
- [√] 1.3 补充 offheap 文档：区分 “bytes 抽象” 与 “allocator 后端”，更新 `helloagents/wiki/modules/offheap.md`（依赖任务 1.1）
- [√] 1.4 补充 db 文档：强调 maxmemory/统计为“估算但稳定可解释”，更新 `helloagents/wiki/modules/db.md`

## 2. Off-heap 后端诊断与可观测性（offheap-api + server）
- [√] 2.1 在 `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java` 增加“provider 发现/诊断”能力（例如返回可用 providers 列表、是否冲突、缺失原因提示）
- [√] 2.2 在 server 启动装配层输出诊断信息：更新 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`（或 `ServerConfig.java`）将后端选择/可用性写入日志（依赖任务 2.1）
- [√] 2.3 增强缺失后端时的错误信息可操作性（提示需要的模块/profile/运行参数），更新 `YierdisOffHeapAllocators`（依赖任务 2.1）

## 3. 命令注册护栏（core）
- [√] 3.1 新增最小命令集回归测试：在 `yierdis-core/src/test/java/yier/bubu/redis/command/` 添加测试，确保关键命令不会因为忘记注册而退化为 `ERR unknown command`
- [-] 3.2 （可选）增加启动期自检开关：当启用时验证 registry 含关键命令，并在失败时给出清晰提示（避免默认影响启动）
  > Note: 本轮以测试护栏为主，启动期自检涉及 CLI/配置开关设计，放入后续演进评估（避免无谓增加启动复杂度）。

## 4. 连接级状态隔离护栏（protocol-netty）
- [√] 4.1 新增 session 隔离测试：在 `yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/` 覆盖多连接（多 `Channel`）下的 RESP2/RESP3 状态互不干扰
- [-] 4.2 （可选）补充 server 侧集成测试：覆盖 “A 连接 HELLO 3 → B 连接仍 RESP2” 的回复编码差异
  > Note: 当前已通过 `NettyRespSession` 的 per-channel attribute 测试验证隔离；server 侧端到端集成测试需要引入更重的网络/并发编排，放入后续演进。

## 5. Security Check
- [√] 5.1 执行安全检查（G9）：启动诊断日志不输出敏感信息；SPI provider 冲突/缺失 fail-fast；避免引入新的 Netty 依赖到 `yierdis-core` / `yierdis-protocol`

## 6. Testing
- [√] 6.1 执行 `mvn test` 并确认新增回归测试通过

## 7. 后续可选演进（不在本轮默认执行范围）
- [-] 7.1 评估抽取 `yierdis-bytes` 模块的收益/成本，并给出迁移计划（如确认执行，再进入下一轮方案设计）
  > Note: 本轮先通过“文档澄清 + 护栏 + 启动诊断”降低退化风险；是否抽取 bytes 模块需要另行确认收益/成本与迁移窗口。
