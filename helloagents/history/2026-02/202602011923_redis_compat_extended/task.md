# Task List: Redis 兼容性扩展（RESP3 / 多 DB / 事务 / PubSub / 持久化 / ACL/TLS）

Directory: `helloagents/plan/202602011923_redis_compat_extended/`

---

## 1. 协议层（RESP3 扩展）
- [√] 1.1 扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`：补齐 RESP3 必需类型写出（push/boolean/double/blob error/verbatim/attribute 的最小集合），verify why.md#requirement-resp3-compatibility-expansion-scenario-hello-3-handshake-and-reply-types
- [-] 1.2 评估并扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespSession.java`：为多 DB / MULTI / PubSub 所需的“会话态”提供可扩展接口（或引入新接口并保持跨模块隔离），verify why.md#requirement-multi-db-support
- [√] 1.2 评估并扩展 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespSession.java`：为多 DB / MULTI / PubSub 所需的“会话态”提供可扩展接口（或引入新接口并保持跨模块隔离），verify why.md#requirement-multi-db-support
- [√] 1.3 在 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespCommandDecoder.java` 增加 RESP3 兼容测试与必要的行为调整（保持 request 兼容集合不扩大但更清晰可诊断；补齐“null bulk string request”的协议错误处理与关闭语义，避免与 Redis 生态预期不一致），verify why.md#requirement-resp3-compatibility-expansion
- [√] 1.4 扩展 reply 切帧 decoder：更新 `yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/RespDecoder.java` 以识别 RESP3 新类型（至少 `~` set 与 `>` push，以及与 writer/对象解析器一致的前缀集合），保证内置 client/测试可解码扩展后的 reply，verify why.md#requirement-resp3-compatibility-expansion
- [√] 1.5 扩展调试/测试解析器：更新 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespObjectParser.java` 支持新增 RESP3 类型（boolean/double/verbatim/blob error/push 等），用于 CLI/测试断言，verify why.md#requirement-resp3-compatibility-expansion

## 2. 多 DB（连接态 + DB 路由）
- [√] 2.1 扩展 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 增加 `--databases`（默认 16）与相关校验，verify why.md#requirement-multi-db-support
- [√] 2.2 扩展 `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java` 承载 databases 配置并贯穿 `YierdisServerBootstrap`，verify why.md#requirement-multi-db-support
- [√] 2.3 改造 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`：创建并绑定多个 `YierdisDb` 实例（DB0..N-1）并提供 db router，verify why.md#requirement-multi-db-support-scenario-select-switching-affects-subsequent-commands
- [√] 2.4 扩展 `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionState.java`：增加连接级 `dbIndex`，并提供线程安全访问，verify why.md#requirement-multi-db-support
- [√] 2.5 改造 `yierdis-core/src/main/java/yier/bubu/redis/command/CommandSupport.java` / `YierdisFastCommandProcessor.java`：从“单 DB 固定依赖”升级为“按连接选择 DB”，verify why.md#requirement-multi-db-support
- [√] 2.6 扩展 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java` 的 `select(...)`：支持 0..N-1、错误语义与 Redis 对齐、并影响后续命令执行，verify why.md#requirement-multi-db-support-scenario-select-switching-affects-subsequent-commands

## 3. 事务（MULTI/EXEC/DISCARD）
- [-] 3.1 新增连接态事务结构（queue + flags），放置在 server 连接态并通过 core 命令层可访问（不直接依赖 server 模块），verify why.md#requirement-transactions-multiexec
- [-] 3.2 在 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java` 或新增命令文件中实现 `MULTI/EXEC/DISCARD` 并接入 `CommandRegistry`，verify why.md#requirement-transactions-multiexec-scenario-multi-queues-commands-and-exec-applies-atomically
- [-] 3.3 调整 `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`：确保事务执行与 close-after-reply/背压/错误处理一致，不出现双回复或队列泄漏，verify why.md#requirement-transactions-multiexec

## 4. PubSub（SUBSCRIBE/PUBLISH）+ RESP3 push
- [-] 4.1 在 `yierdis-server/src/main/java/yier/bubu/redis/` 新增轻量 PubSub broker（频道 -> subscribers），并处理连接关闭清理，verify why.md#requirement-pubsub--resp3-push
- [-] 4.2 在命令层新增 `SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH` 并注册，verify why.md#requirement-pubsub--resp3-push-scenario-subscribe-then-publish-delivers-messages
- [-] 4.3 在 `yierdis-protocol/src/main/java/yier/bubu/redis/protocol/RespWriter.java`（或新增 writer API）实现 RESP3 push 消息写出，并在 RESP2 下保持数组兼容，verify why.md#requirement-pubsub--resp3-push

## 5. 持久化（最小 AOF）
- [-] 5.1 扩展 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 增加 `--appendonly/--appendfilename/--appendfsync`，verify why.md#requirement-minimal-persistence-aof
- [-] 5.2 在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java` 引入 AOF 组件（默认关闭），在 executor 线程内对写命令追加日志，verify why.md#requirement-minimal-persistence-aof
- [-] 5.3 启动恢复：server 启动前（或 bootstrap early phase）回放 AOF 并按同一命令执行链路重建数据集，verify why.md#requirement-minimal-persistence-aof-scenario-restart-restores-dataset-when-aof-enabled

## 6. ACL/TLS（最小安全基线）
- [-] 6.1 扩展 `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java` 增加 `--requirepass` 与 TLS 相关参数（证书/私钥/端口），verify why.md#requirement-acltls-baseline
- [-] 6.2 在命令层实现 `AUTH`（以及必要的 NOAUTH gate），并明确允许的免认证命令集合，verify why.md#requirement-acltls-baseline-scenario-auth-required-blocks-commands-until-authenticated
- [-] 6.3 在 `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java` 增加可选 `SslHandler` 注入（或单独 TLS listener），verify why.md#requirement-acltls-baseline

## 7. 预算口径统一与可观测性（减少误配）
- [√] 7.1 收敛“prepareWrite 估算常量”：将 `CommandSupport.ENTRY_OVERHEAD_ESTIMATE_BYTES` 与 `YierdisDb` 的 accounting 对齐为单点定义，并补充测试防漂移，verify why.md#change-content
- [-] 7.2 扩展 `INFO/STATS/CONFIG GET` 输出：统一展示 maxmemory/offheap/backlog/protocol limits 等关键配置与当前值，verify why.md#change-content
- [-] 7.3 改进 busy/oom/protocol/internal error 的错误文本与关闭策略一致性（handler vs executor）；并统一数值解析/语法错误的错误文本（避免携带 label 等与 Redis 不一致的细节），verify why.md#change-content

## 8. 生态命令兼容（INFO/CONFIG/CLIENT/COMMAND/SCAN）
- [√] 8.1 调整 `INFO` 为 Redis 兼容的 bulk string 输出，并将结构化指标保留在 `STATS` 或 `INFO YIERDIS` 扩展 section：更新 `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java` + `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan-scenario-redis-cli--tooling-expects-redis-like-info
- [-] 8.2 实现最小 `CONFIG GET/SET`（覆盖与兼容性相关的配置：maxmemory/offheap/backpressure/protocol limits 等），并在 help/文档中明确只读/只写范围，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan
- [-] 8.3 实现最小 `CLIENT SETNAME/GETNAME/ID`（连接级属性），并与 `HELLO SETNAME` 复用同一连接态字段，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan
- [-] 8.4 扩展 `COMMAND` 子命令覆盖（优先 `COMMAND DOCS`/`COMMAND GETKEYS`/`COMMAND GETKEYSANDFLAGS` 或明确返回兼容错误），并补齐文档与测试，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan
- [-] 8.5 实现 `SCAN`（支持 cursor/MATCH/COUNT 的最小集合），并确保在 rehash/过期键场景下 best-effort 可前进且不崩溃，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan-scenario-keyspace-traversal-uses-scan-instead-of-keys
- [-] 8.6 评估并实现（可选）`HSCAN/SSCAN/ZSCAN`：若生态兼容目标包含常用框架（例如 Spring Data Redis）的扫描行为，则纳入；否则明确“不支持”的错误与替代方案，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan

## 9. 现有命令语义对齐（HELLO/EXPIRE/OBJECT/FLUSHDB/SET/LPOP）
- [-] 9.1 `HELLO` options 严格解析 + reply 类型对齐：支持的必须生效（AUTH/SETNAME），不支持的必须报错（禁止静默忽略）；`HELLO 3` 的 reply 字段/类型向 Redis 对齐（例如 `proto` 用 integer），更新 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb-scenario-hello-options-must-be-applied-or-rejected
- [√] 9.2 `EXPIRE` 语义对齐与溢出防护：`seconds<=0` 的删除/失效行为对齐 Redis，并对 `seconds*1000` 做 long 溢出保护，更新 `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java` + `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java`，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb-scenario-expire-seconds0-and-overflow-handling
- [√] 9.3 `OBJECT ENCODING` 输出类型对齐：由 simple string 调整为 bulk string，更新 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java`，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb-scenario-object-encoding-returns-bulk-string
- [-] 9.4 `FLUSHDB` 参数/arity 校验：支持/拒绝 `ASYNC|SYNC` 并对非法参数返回兼容错误，更新 `yierdis-core/src/main/java/yier/bubu/redis/command/ServerCommands.java`，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb-scenario-flushdb-arityoptions-are-validated
- [-] 9.5 `SET` 选项冲突/范围校验与缺失项补齐：检测 NX+XX、EX+PX/EXAT+PXAT 冲突并返回 syntax error；对负数/0/溢出等边界值做 Redis 风格校验与错误文本对齐；评估补齐 `KEEPTTL/GET` 等常用选项（按生态兼容目标裁剪），更新 `yierdis-core/src/main/java/yier/bubu/redis/command/StringCommands.java`，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb
- [-] 9.6 `LPOP/RPOP` count 语义对齐：负数 count 返回错误、0 的行为与 Redis 对齐（空数组/空 bulk），更新 `yierdis-core/src/main/java/yier/bubu/redis/command/ListCommands.java` + `yierdis-core/src/main/java/yier/bubu/redis/db/YierdisDb.java`（如需），verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb
- [√] 9.7 `MEMORY STATS` value 类型对齐：将数值从 bulk string 调整为 integer（RESP2/RESP3 一致），同步更新 `yierdis-core/src/main/java/yier/bubu/redis/command/KeyCommands.java` 与相关测试/文档，verify why.md#requirement-ecosystem-command-surface-compatibility-infoconfigclientcommandscan
- [-] 9.8 补齐 TTL 命令面（生态常用）：实现 `PTTL/PEXPIRE/PERSIST/EXPIREAT/PEXPIREAT`（按兼容目标裁剪），并与 `EXPIRE/TTL` 保持一致的错误/边界行为，verify why.md#requirement-semantics-alignment-for-existing-commands-helloexpireobjectflushdb

## 10. 连接关闭/异常路径一致性（避免副作用与资源浪费）
- [√] 10.1 协议错误关闭时标记 connection closing，并让 executor 跳过该连接后续 backlog 命令（只回收不执行）：更新 `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java` + `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`，verify why.md#change-content
- [-] 10.2 增加 `channelInactive`/连接断开清理钩子：清理 PubSub 订阅、事务队列、连接态资源，避免泄漏与悬挂引用，verify why.md#change-content

## 11. Security Check
- [-] 11.1 Execute security check（输入校验、CRLF 注入、敏感信息日志、权限控制、文件权限与路径校验、DoS 风险）

## 12. Documentation Update
- [√] 12.1 更新 `helloagents/wiki/modules/protocol.md` / `helloagents/wiki/modules/server.md` / `helloagents/wiki/modules/command.md`：记录新增 RESP3/多 DB/事务/PubSub/AOF/TLS/ACL/INFO/SCAN 的兼容边界与配置说明
- [√] 12.2 更新 `README.md`：补充新特性开关参数与兼容范围说明（默认仍保持教学模式）
- [-] 12.3 文档漂移审计与护栏：修复 `README.md` 中与代码不一致的描述（例如 `COMMAND`/`KEYS` glob/命令列表），并新增 guard test（或自动生成摘要）防止未来再次漂移，verify how.md#adr-008-内置调试工具与文档的-ssot-收敛避免代码演进但-readme-漂移

## 13. Testing
- [-] 13.1 新增/扩展集成测试：`yierdis-server/src/test/java/yier/bubu/redis/` 覆盖 RESP3（含 push）、多 DB、事务、PubSub、AUTH、AOF replay、INFO 兼容输出、SCAN cursor 行为等核心场景
- [√] 13.2 回归测试：执行 `mvn test`（含必要 profile），并记录关键用例覆盖范围与已知限制
- [√] 13.3 扩展协议解析器测试矩阵：新增 RESP3 类型解析/切帧用例（`RespDecoder` + `RespObjectParser`），并更新现有用例（例如 `Resp3CollectionReplyTest`）以匹配新的兼容策略（例如 MEMORY STATS 的整数值类型）

## 14. 内置客户端/工具链（随协议扩展同步演进）
- [-] 14.1 更新 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`：在 RESP3 模式下识别并区分 push（`>`）与常规 reply，避免 1-request-1-response 模型在 PubSub 下响应错配；为 PubSub 引入专用 subscribe API 或“push 回调/队列”机制（最小可用即可）
- [-] 14.2 更新 `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`：支持订阅/持续输出模式（接收 push 消息）；并为非订阅模式提供明确的诊断（例如遇到 push 时提示并安全退出），避免 silent desync
- [-] 14.3 增加 client/CLI 回归用例：覆盖 `HELLO 3` 后的 `SMEMBERS`（RESP3 set）、`HGETALL`（RESP3 map）、以及 PubSub push 基本链路，确保工具链可用于回归与排障

---

## Execution Notes

> Note: 本次 execution 聚焦：RESP3 reply 类型覆盖、request 解码严格化、多 DB（`--databases` + `SELECT` + per-connection dbIndex 路由）、预算口径收敛（单点 entry overhead 常量 + 数值解析错误文本对齐）、以及 INFO/MEMORY/EXPIRE 的生态兼容对齐。
> Note: 事务/PubSub/AOF/ACL/TLS/SCAN/CONFIG 等能力范围较大，未在本次落地；建议拆分为后续独立 solution packages 分阶段交付。
