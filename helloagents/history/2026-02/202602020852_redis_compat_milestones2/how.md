# Technical Design: Redis 生态兼容性（二期 Roadmap：里程碑拆分）

## Technical Solution

### Core Technologies
- Java 17
- Netty（server I/O + ByteBuf）
- 现有模块：`yierdis-core`（命令与 DB）、`yierdis-server`（Netty 集成）、`yierdis-protocol*`（RESP）、`yierdis-client`（内置工具）、`yierdis-args`（参数）

### Implementation Key Points

1. **里程碑交付原则**
   - 每个 Milestone 都必须可独立验证：实现 + 测试 + 文档/已知限制
   - 优先处理“生态高频默认依赖”的能力（SCAN / 事务探测 / AUTH / PubSub）
   - 避免引入跨模块强耦合：core 通过 protocol 的 session 接口获取连接态，不直接依赖 server/Netty
2. **连接态（Session）收敛策略**
   - 复用现有 `RespServerSession`（由 server 连接态实现），在其上扩展：
     - `dbIndex`（已具备）
     - `auth` 状态（authenticated + requirepass 是否开启）
     - `transaction` 状态（MULTI queue）
     - `pubsub` 状态（subscriptions + pubsub-mode flag）
3. **单线程执行器语义**
   - 维持当前“单线程 DB 绑定”的教学定位
   - 事务/订阅/AOF 都在 executor 线程内执行，以保证一致性与减少锁复杂度
4. **RESP2/RESP3 兼容策略**
   - RESP3：用于更丰富类型（set/map/boolean/double）与 PubSub push（`>`）
   - RESP2：保持兼容数组回复形态（PubSub 的 message/subscribe/unsubscribe 事件）
5. **可观测性与失败策略**
   - 对“未实现能力”返回明确错误（避免静默忽略）
   - 对“仅提供最小语义”的能力写入文档并提供测试护栏

## Architecture Decision ADR

### ADR-009: 里程碑拆分与最小闭环交付
**Context:** 兼容性缺口涉及协议、命令、执行器、工具链与安全能力，若一次性落地会导致风险不可控。  
**Decision:** 以 M1..M5 拆分交付，每个 Milestone 独立验证，优先解决生态高频依赖能力。  
**Rationale:** 降低回归风险，便于逐步扩大兼容面并保持可解释性。  
**Alternatives:** 一次性实现事务+PubSub+AOF+TLS → 拒绝原因：测试与行为对齐成本过高，容易引入不可定位问题。  
**Impact:** 需要在 `task.md` 中明确每个 milestone 的验收用例与测试覆盖。

### ADR-010: PubSub push 与内置 client/CLI 同步演进
**Context:** RESP3 push（`>`）会打破现有 client 的 1-request-1-response 假设。  
**Decision:** Milestone 3 同步改造 `yierdis-client`：增加 push 分流机制与订阅模式 API。  
**Rationale:** 避免“server 支持但工具链不可用”，并减少排障成本。  
**Alternatives:** 只改 server 不改 client → 拒绝原因：会导致内部测试/CLI 失效，且真实生态仍会踩坑。  
**Impact:** Milestone 3 必须包含 client/CLI 测试用例。

### ADR-011: AOF 最小实现的边界
**Context:** Redis AOF 涉及 rewrite、fsync 策略、命令重写与复杂边界。  
**Decision:** 仅实现最小 AOF：对可支持的写命令追加记录，启动时回放；rewrite/多进程优化不在本期范围。  
**Rationale:** 满足“重启不丢数据”的最小诉求，同时避免实现复杂度爆炸。  
**Alternatives:** 直接实现 RDB 或完整 AOF → 拒绝原因：超出当前项目定位与测试投入。  
**Impact:** 文档必须明确支持的命令集合与一致性边界。

## Security and Performance

- **Security**
  - `--requirepass` 默认关闭；启用后必须避免把密码写入日志
  - TLS 默认关闭；启用时要求证书/私钥路径可读且格式校验失败要快速失败
  - PubSub/事务/AOF 都属于“可被滥用放大资源开销”的能力，需要结合现有 backpressure/queue 预算做保护
- **Performance**
  - SCAN 采用“时间/步数预算”的最佳努力迭代，避免大 keyspace 阻塞 executor
  - PubSub push 应避免在 I/O 线程做重逻辑，尽量在 executor 线程统一调度写出
  - AOF 写入需要可配置 fsync 策略，默认选择更安全但可接受的策略（并文档化）

## Testing and Deployment

- **Testing**
  - 每个 milestone 至少新增 1 组集成测试覆盖核心场景
  - 重点覆盖：SCAN cursor、事务队列与错误处理、PubSub push 与 client 分流、AUTH gate、AOF replay
- **Deployment**
  - 以参数开关逐步启用（databases/auth/tls/aof）
  - README 与 `helloagents/wiki/modules/*` 同步更新，明确默认行为与兼容边界
