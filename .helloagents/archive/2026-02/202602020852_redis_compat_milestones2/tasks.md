<!-- migrated_from: history/2026-02/202602020852_redis_compat_milestones2/task.md -->
<!-- migrated_at: 2026-02-24 23:14:10 -->

# Task List: Redis 生态兼容性（二期 Roadmap：里程碑拆分）

Directory: `helloagents/plan/202602020852_redis_compat_milestones2/`

---

## Milestone 1：生态命令面补齐 + 语义对齐（SCAN/TTL/SET/FLUSHDB/LPOP）

- [√] 1.1 引入 scan cursor 抽象（best-effort）：新增 `yierdis-core/src/main/java/yier/bubu/redis/db/ScanCursor.java` 并为 `YierdisDb` 增加可迭代 key 视图入口（不暴露底层 map 细节），verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment-scenario-scan-cursor-iteration-works-for-tooling
- [√] 1.2 实现 `SCAN`：在 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java` 增加 `SCAN`（支持 MATCH/COUNT 基本解析与错误对齐），并在 `yierdis-core/src/main/java/yier/bubu/redis/command/CommandRegistry.java` 注册，verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment-scenario-scan-cursor-iteration-works-for-tooling, depends on task 1.1
- [√] 1.3 实现 TTL 命令族：在 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java` 增加 `PTTL/PEXPIRE/PERSIST/EXPIREAT/PEXPIREAT` 并与 `YierdisDb` 统一边界语义/溢出保护，verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment-scenario-ttl-family-is-consistent-and-overflow-safe
- [√] 1.4 对齐 `SET` 选项校验：更新 `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`（NX+XX、EX/PX/EXAT/PXAT、负数/0/溢出、可选补齐 KEEPTTL/GET），verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment
- [√] 1.5 对齐 `FLUSHDB` 参数：更新 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`（wrong arity/非法参数/ASYNC|SYNC 策略明确），verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment
- [√] 1.6 对齐 `LPOP/RPOP` count：更新 `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java`（负数报错、0 返回空数组、key 不存在返回 nil 等），verify why.md#requirement-milestone-1---ecosystem-command-surface--semantics-alignment

## Milestone 2：事务（MULTI/EXEC/DISCARD）

- [√] 2.1 扩展连接态事务接口：在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespServerSession.java` 增加事务状态访问（queue/flags），并在 `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java` 实现，verify why.md#requirement-milestone-2---transactions-multiexecdiscard-scenario-multi-queues-and-exec-returns-array
- [√] 2.2 实现命令：新增 `yierdis-core/src/main/java/yier/bubu/redis/command/TransactionCommands.java`（或在现有 `ServerCommands` 内扩展）实现 `MULTI/EXEC/DISCARD` 并注册，verify why.md#requirement-milestone-2---transactions-multiexecdiscard-scenario-multi-queues-and-exec-returns-array, depends on task 2.1
- [√] 2.3 接入执行链路：更新 `yierdis-core/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` 在事务态下对普通命令做 QUEUED/入队，对 EXEC 做队列执行与数组回复，verify why.md#requirement-milestone-2---transactions-multiexecdiscard-scenario-multi-queues-and-exec-returns-array, depends on task 2.2
- [√] 2.4 executor close/backlog 一致性：更新 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java` 保证事务态连接关闭时队列清理且不产生副作用，verify why.md#requirement-milestone-2---transactions-multiexecdiscard

## Milestone 3：PubSub + RESP3 push + 内置 client/CLI 适配

- [-] 3.1 新增 PubSub broker：新增 `yierdis-server/src/main/java/yier/bubu/redis/PubSubBroker.java` 并在 `YierdisServerBootstrap` 初始化与注入，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support-scenario-subscribe-then-publish-delivers-push-messages
  > Note: 本次执行未进入 PubSub 里程碑，留待后续独立方案包交付。
- [-] 3.2 订阅连接态与断开清理：扩展 `ServerConnectionState` 记录订阅集合，并在 `YierdisFastCommandHandler.channelInactive(...)` 清理订阅，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support-scenario-subscribe-then-publish-delivers-push-messages, depends on task 3.1
- [-] 3.3 PubSub 命令族：新增 `yierdis-core/src/main/java/yier/bubu/redis/command/PubSubCommands.java` 实现 `SUBSCRIBE/UNSUBSCRIBE/PUBLISH`（最小集合）并注册，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support-scenario-subscribe-then-publish-delivers-push-messages
- [-] 3.4 RESP3 push 写出与 RESP2 兼容：在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java` 增加 PubSub message 的 push/array 写出辅助，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support-scenario-subscribe-then-publish-delivers-push-messages
- [-] 3.5 内置 client push 分流：更新 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java` 支持 push（`>`）与常规 reply 分离，并提供 subscribe 模式 API，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support, depends on task 3.3
- [-] 3.6 CLI 订阅模式：更新 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java` 支持持续输出订阅消息并提供退出策略，verify why.md#requirement-milestone-3---pubsub--resp3-push--client-support, depends on task 3.5

## Milestone 4：安全基线（AUTH requirepass + 可选 TLS）

- [-] 4.1 增加启动参数与配置：扩展 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` / `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java` 增加 `--requirepass` 与 TLS 参数（证书/私钥/端口），verify why.md#requirement-milestone-4---auth-baseline--optional-tls-scenario-noauth-gate-blocks-commands-until-auth
  > Note: 本次执行未进入安全基线里程碑，避免引入证书/密码等额外敏感配置面。
- [-] 4.2 实现 `AUTH` 与 gate：在 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`（或新增 `AuthCommands`）实现 `AUTH`，并在 `YierdisFastCommandProcessor` 加入 `NOAUTH` gate，verify why.md#requirement-milestone-4---auth-baseline--optional-tls-scenario-noauth-gate-blocks-commands-until-auth, depends on task 4.1
- [-] 4.3 TLS listener（可选）：更新 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java` 在启用 TLS 参数时注入 SSL handler 并监听 TLS 端口，verify why.md#requirement-milestone-4---auth-baseline--optional-tls

## Milestone 5：最小持久化（AOF）

- [-] 5.1 增加 AOF 参数：扩展 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` / `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java` 增加 `--appendonly/--appendfilename/--appendfsync`，verify why.md#requirement-milestone-5---minimal-aof-scenario-restart-restores-dataset-when-aof-enabled
  > Note: 本次执行未进入持久化里程碑（涉及文件 IO/一致性策略），建议单独方案包推进。
- [-] 5.2 实现 AOF 追加：新增 `yierdis-server/src/main/java/yier/bubu/redis/AofWriter.java` 并在 executor 写命令路径中追加（明确仅记录支持的写命令），verify why.md#requirement-milestone-5---minimal-aof-scenario-restart-restores-dataset-when-aof-enabled, depends on task 5.1
- [-] 5.3 实现 AOF 回放：在 `YierdisServerBootstrap` 启动 early phase 回放 AOF 并重建数据集，verify why.md#requirement-milestone-5---minimal-aof-scenario-restart-restores-dataset-when-aof-enabled, depends on task 5.2

## Security Check

- [-] 6.1 执行安全审计：输入校验、敏感信息日志、路径校验（TLS/AOF）、DoS 风险（PubSub/SCAN/事务队列），并补齐必要的保护与测试用例
  > Note: 本次执行仅覆盖功能与兼容性回归，未做系统性安全审计（建议与 Milestone 3-5 一起推进）。

## Documentation Update

- [√] 7.1 更新 `README.md`：补充本期新增能力开关与兼容边界（SCAN/事务/PubSub/AUTH/TLS/AOF）
- [√] 7.2 更新 `helloagents/wiki/modules/command.md` / `helloagents/wiki/modules/server.md` / `helloagents/wiki/modules/client.md`：补齐语义说明、默认行为与已知限制
- [-] 7.3 文档漂移护栏：新增“命令列表/INFO 形态/错误文本”的 guard（自动生成摘要或测试），避免 README 再次漂移
  > Note: 本次通过测试用例与模块文档减少漂移风险，但未实现“一键生成摘要/强制 guard”机制。

## Testing

- [-] 8.1 新增集成测试：`yierdis-server/src/test/java/yier/bubu/redis/` 覆盖 SCAN/TTL/SET/FLUSHDB/LPOP、MULTI/EXEC、PubSub push、AUTH gate、AOF replay
  > Note: 已新增/扩展覆盖 Milestone 1/2 的测试（含 core 与 server），但未覆盖 PubSub/AUTH/AOF（对应里程碑未落地）。
- [√] 8.2 回归测试：执行 `mvn test` 并记录关键用例覆盖范围与已知限制
  > Note: 2026-02-02 执行 `mvn test`，BUILD SUCCESS。

---

## Execution Notes

> Note: 本次 execution 交付 Milestone 1 + Milestone 2：补齐 SCAN/TTL/SET/FLUSHDB/LPOP 语义对齐，并落地 MULTI/EXEC/DISCARD（避免 ByteBuf 生命周期 pin 住大 buffer 的风险）。
> Note: Milestone 3-5（PubSub/AUTH/TLS/AOF）属于更大范围工程化能力，本次按里程碑拆分后选择延期交付，以降低一次性改动与风险。
