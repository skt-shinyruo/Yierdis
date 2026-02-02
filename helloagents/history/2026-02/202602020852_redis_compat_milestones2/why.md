# Change Proposal: Redis 生态兼容性（二期 Roadmap：事务 / PubSub / SCAN / 安全基线 / AOF）

## Requirement Background

项目定位是“教学导向的 Redis-compatible server（Java + Netty）”，但随着功能逐步扩展，已经进入一个新阶段：不仅要“跑通基本命令”，还需要尽量贴近 Redis 生态的默认假设（客户端驱动、连接池、缓存框架、运维脚本与工具链）。

上一期（`202602011923_redis_compat_extended`）已完成的关键能力包括：

1. **多 DB 基础能力**：新增 `--databases` 与连接级 DB 路由，`SELECT 0..N-1` 生效。
2. **RESP3 reply 类型扩展**：补齐更完整的 RESP3 回复类型写出/解析（为未来 push 机制铺路）。
3. **INFO 生态对齐**：`INFO` 默认改为 Redis 兼容的 bulk string；保留 `INFO YIERDIS`/`STATS` 的结构化输出。
4. **maxmemory/off-heap 预算口径收敛**：减少多 DB 场景下重复计入 off-heap used bytes 的误导与过度淘汰风险。
5. **错误语义与关闭路径的一致性改进**：降低“连接已关闭但 backlog 仍执行”的副作用风险。

但在真实 Redis 生态中，仍有一批“高频默认依赖”的能力缺口与行为差异，会导致：
- 客户端探测能力失败或退化（例如依赖 `SCAN` 遍历、依赖事务探测 `MULTI/EXEC`、依赖 `AUTH`、依赖 PubSub）。
- 框架使用体验不稳定（例如订阅 push 与 1-request-1-response 的冲突、命令参数细节与错误文本不对齐）。
- 运维/脚本失效或行为不可预测（例如 `FLUSHDB ASYNC|SYNC`、`SET` 选项冲突、TTL 家族命令缺失）。

因此，本期目标是以“里程碑拆分”的方式，把剩余兼容性缺口拆成多个可独立交付的阶段，每个阶段都有最小闭环（实现 + 测试 + 文档/已知限制）。

## Change Content

1. **Milestone 1：生态命令面补齐 + 语义对齐**
   - `SCAN`（以及必要的 `SSCAN/HSCAN/ZSCAN`）最小可用实现
   - TTL 命令族补齐：`PTTL/PEXPIRE/PERSIST/EXPIREAT/PEXPIREAT`
   - `SET` 选项冲突/范围校验对齐（NX/XX、EX/PX、KEEPTTL/GET 等按裁剪目标补齐）
   - `FLUSHDB` 参数校验（`ASYNC|SYNC` 支持/拒绝策略明确）
   - `LPOP/RPOP` count 语义对齐（负数/0/不存在 key 的返回形态）
2. **Milestone 2：事务（MULTI/EXEC/DISCARD）**
   - 引入连接级事务状态（queue + flags）
   - 实现 `MULTI/EXEC/DISCARD` 并与单线程执行器语义一致
3. **Milestone 3：PubSub + RESP3 push + 内置 client/CLI 适配**
   - `SUBSCRIBE/UNSUBSCRIBE/PUBLISH` 最小链路
   - RESP3 下以 push（`>`）下发消息；RESP2 下保持数组兼容
   - 内置 client/CLI 支持 push（避免响应错配与 desync）
4. **Milestone 4：安全基线（AUTH requirepass + 可选 TLS）**
   - `--requirepass` + `AUTH` + `NOAUTH` gate（与 `HELLO AUTH` 语义一致）
   - 可选 TLS listener（默认关闭），降低误用风险
5. **Milestone 5：最小持久化（AOF）**
   - `--appendonly/--appendfilename/--appendfsync` 基础配置
   - 写命令追加与启动回放（明确兼容边界，不追求 Redis 全量 AOF 语义）

## Impact Scope

- **Modules:** protocol / protocol-netty / core(command,db) / server / client / args / helloagents docs
- **Files:** 预计涉及 `yierdis-core`、`yierdis-server`、`yierdis-client`、`yierdis-args`、`yierdis-protocol*` 与 `helloagents/wiki/*`
- **APIs:** Redis 命令面扩展（`SCAN`、事务、PubSub、AUTH）与启动参数扩展
- **Data:** 新增可选 AOF 文件（本地文件系统）；新增 TLS 证书/私钥配置（本地文件系统）

## Core Scenarios

### Requirement: Milestone 1 - Ecosystem Command Surface & Semantics Alignment
**Module:** command / db
补齐 Redis 生态高频依赖的命令面，并对细节语义做明确对齐或明确拒绝（禁止静默忽略）。

#### Scenario: SCAN cursor iteration works for tooling
条件：工具执行 `SCAN 0 MATCH pattern COUNT n` 进行 keyspace 遍历
- 预期：返回 `[nextCursor, keys]`，cursor 递进且可终止（cursor=0）
- 预期：大 keyspace 下不阻塞单线程 executor（最佳努力，按时间/步数预算）

#### Scenario: TTL family is consistent and overflow-safe
条件：执行 `PTTL/PEXPIRE/EXPIREAT/PEXPIREAT/PERSIST`
- 预期：与 `TTL/EXPIRE` 的边界行为一致（不存在 key/无 TTL/溢出）
- 预期：错误文本与 Redis 风格接近，便于生态工具识别

### Requirement: Milestone 2 - Transactions (MULTI/EXEC/DISCARD)
**Module:** command / protocol-session / server-executor
补齐事务基本语义，满足客户端“探测/使用 MULTI”的默认假设。

#### Scenario: MULTI queues and EXEC returns array
- `MULTI` 进入事务态
- 普通命令返回 `QUEUED`，不立即生效
- `EXEC` 按队列顺序执行并返回数组结果（保持单线程原子视角）
- `DISCARD` 清空队列并退出事务态

### Requirement: Milestone 3 - PubSub + RESP3 Push + Client Support
**Module:** server / command / protocol / client
实现最小 PubSub 生态链路，并在 RESP3 下以 push 消息下发。

#### Scenario: SUBSCRIBE then PUBLISH delivers push messages
- 连接 A `SUBSCRIBE ch` 后进入订阅模式并收到订阅确认
- 连接 B `PUBLISH ch hello` 后，A 收到消息推送（RESP2/RESP3 形态按协商一致）
- 连接断开后订阅清理，不保留悬挂引用

### Requirement: Milestone 4 - AUTH Baseline + Optional TLS
**Module:** args / server / command
提供最小安全基线，避免“看似支持但实际无效”的安全误导。

#### Scenario: NOAUTH gate blocks commands until AUTH
- 配置 `--requirepass` 后，未认证连接执行命令返回 `NOAUTH`
- `AUTH <pass>` 成功后恢复执行
- `HELLO 3 AUTH ...`：支持则生效，不支持则明确报错（禁止静默忽略）

### Requirement: Milestone 5 - Minimal AOF
**Module:** server / command / db
提供最小 AOF：写入追加与重启回放。

#### Scenario: Restart restores dataset when AOF enabled
- 开启 AOF 后写入若干 key
- 重启后数据按可支持的命令集合恢复一致

## Risk Assessment

- **Risk:** 范围过大导致交付不可控、兼容细节漂移、测试不足。
  - **Mitigation:** 采用里程碑拆分，每个 milestone 必须实现“最小闭环”（实现 + 测试 + 文档/已知限制）。
- **Risk:** PubSub push 与现有内置 client 的 1-request-1-response 模型冲突。
  - **Mitigation:** 在 milestone 3 同步改造 client/CLI，增加 push 分流与订阅模式。
- **Risk:** AOF/TLS 牵涉本地文件与密钥配置，容易误用或泄露敏感信息。
  - **Mitigation:** 默认关闭；严格参数校验；避免记录明文密码/私钥内容到日志；文档明确安全建议。
