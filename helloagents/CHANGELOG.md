# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Breaking
- 对外协议完全替换：服务端不再接受旧协议请求；改为 Custom Protocol v1（`<len>:<json>\\n` request + NDJSON reply），并将“协议错误”策略调整为**返回 error 并尝试继续读下一帧（resync）**（触达安全上限时可能断连）。
- Custom Protocol v1 reply 的值语义收敛（breaking）：map/attribute 统一改为 tagged value `{"$map":[[k,v],...]}`（不再依赖 JSON object key 为 string 的隐式限制与 fallback）；非 UTF-8 bytes 统一输出 `{"$b64":"<base64>"}`；嵌套错误值输出 `{"$error":{...}}`；错误 message 的 CRLF 净化/限长由协议层 encoder SSOT 单点统一。
- 移除 deprecated bytes alias（`YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink` 等），bytes SSOT 统一为 `yierdis-bytes`。
- 删除 legacy 模式：移除 SCAN cursor v1（`ScanCursor`）与相关开关/兼容路径，统一为 `ScanCursorV2`（rehash-aware + 可 time-slice）。
- server 制品坐标对齐：`yierdis-server` 的 artifactId 从 `yierdis` 调整为 `yierdis-server`（依赖坐标与服务端 jar 名称同步变化）。

### Added
- 新增事务队列硬上限：`--transactionQueueMaxCommands/--transactionQueueMaxBytes`，防止 MULTI 大事务/大参数导致 OOM。
- 新增 maxmemory 预算口径参数：`--maxmemoryScope global|per-db`（默认 global，更贴近 Redis 全实例口径；保留 per-db 兼容模式）。
- 新增 Custom Protocol v1 协议栈：length-prefixed JSON request decoder + NDJSON reply writer，并补齐协议错误 resync 的单测与集成测试。
- `JsonParser` 增加 strict UTF-8 的 `ByteBuffer` 输入重载，便于 transport 层在无谓拷贝的前提下复用协议层 JSON 解析（保持 Netty-free）。
- 多 DB 支持：新增 `--databases`（默认 16），并在连接态维护 `dbIndex`，支持 `SELECT 0..N-1` 与按连接路由到目标 DB。
- 新增 `yierdis-bytes` 中立模块：承载 `BytesSource/BytesSink/BytesSlice` 抽象，供 protocol/off-heap/I/O 复用，避免 `yierdis-protocol` 通过 “off-heap” 命名模块复用 bytes 接口造成依赖误导。
- 新增 `InlineCommandParser`（sdssplitargs 风格），CLI REPL 共用同一套解析规则，避免规则漂移。
- 新增 `YierdisServerBootstrap`：抽取 server 装配/生命周期管理，可在测试/工具中复用启动与关停逻辑。
- 新增 core 可嵌入 instance API：`yier.bubu.redis.runtime.YierdisInstance`（Netty-free），统一多 DB 装配/路由/生命周期语义；server bootstrap 迁移为复用该 SSOT，减少装配重复与行为漂移。
- 新增内核契约（SSOT）基座：`KeyHandle`（`yier.bubu.redis.db.key.KeyHandle`）与 `MemoryLedger`（`yier.bubu.redis.db.memory.MemoryLedger`）的最小类型/不变量测试，并补齐 SCAN cursor/TTL/maxmemory 的契约级 smoke 覆盖，为后续渐进迁移与灰度开关落地提供稳定边界。
- 新增 SCAN cursor v2 SSOT：`ScanCursorV2`（rehash-aware）+ keyspace time-slice scan，SCAN 默认使用 v2 并以“数字 bulk string”保持生态兼容。
- 新增慢命令治理 SSOT：`SlowCommandGovernor` + `--keysTimeBudgetMillis/--keysMaxResults`，KEYS 在预算/上限触达时返回部分结果（推荐使用 SCAN 做完整遍历）。
- 新增生产能力扩展前置接口：`YierdisChangeSink`（事件流）与 `YierdisSnapshot`（time-slice 快照），作为 AOF/RDB/replication/ACL/modules 的 guardrails 基座（本版本不启用真实持久化）。
- DB core 组件化拆分：引入 `yier.bubu.redis.ops.*`（`DbEngine/ValueOps/*Ops/ExpirationManager/EvictionCoordinator`）并迁移命令层调用，降低存储-命令耦合与修改半径。
- 新增 off-heap keys 零 canonical heap copy 回归：`OffHeapKeyCopyDiagnostics` + `OffHeapKeysZeroCopyReadPathTest`，锁定 `GET/EXISTS/TYPE/TTL` 热路径不触发 heap key 拷贝。
- Maven 多模块拆分：引入协议层模块（`yierdis-protocol-model` 端口/模型 SSOT + `yierdis-protocol-codec` JSON/v1 codec SSOT；`yierdis-protocol` 为兼容聚合层）、`yierdis-core`（DB/命令 SSOT）、`yierdis-args`（参数 SSOT）、`yierdis-client`（client/CLI），并调整 `yierdis-server`/`yierdis-bench` 依赖方向。
- 新增协议层二次拆分：引入 `yierdis-protocol-model`（端口/Reply IR SSOT）与 `yierdis-protocol-codec`（JSON + Custom Protocol v1 codec SSOT），并保留 `yierdis-protocol` 作为兼容聚合层（migration）。
- 引入 `ReplySink`（bulk string streaming 子集）并让 `ReplyWriter` 继承：作为命令层写出 bulk 值的协议端口子集；db/value/off-heap 不再依赖 `ReplySink`，streaming bulk 输出改为 core 的 domain result / `BulkStringSink`，命令层通过 adapter 写入 `ReplyWriter`；同时保持移除 `YierdisDb` 的 `*ReplyCount/*ReplyInto` 回包形态 API（以 `ValueOps/*Ops` 为唯一入口），并删除旧桥接接口 `YierdisBulkStringOutput`。
- 新增分层护栏回归测试：禁止 `yier.bubu.redis.db.*` / `yier.bubu.redis.ops.*` import `yier.bubu.redis.protocol.*`，避免协议端口向下渗透。
- 修复 off-heap 空字节串 streaming 输出：允许 `(address==0,len==0)` 表达空 slice，避免集合/字典中空值写出时触发 `IllegalArgumentException`；并补充命令回归（SMEMBERS/HGETALL/ZRANGE 空 bulk string）。
- 引入 `ReplyWriterFactory`（协议写出注入点）：server 的执行器/handler 通过 factory 获取 `ReplyWriter`，避免直接 `new JsonLineReplyWriter(...)`，为后续协议替换/多协议共存提供边界。
- 新增 `yierdis-protocol-netty`：承载 Netty codec/adapters（Custom Protocol v1 decoder + reply line decoder）。
- 升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：批量 `write` + 末尾 `flush` 合并、连接级 `autoRead` 背压（high/low 滞回阈值）、全局有界队列与 `-ERR busy` 保护。
- 增加 off-heap allocator 泄漏回归测试，覆盖淘汰/删除/过期与 shutdown 释放路径。
- 新增 `ServerConnectionState`（server 私有）：承载 dbIndex/事务队列/pending/backpressure/closing/counters 等连接级运行时状态，避免跨模块重复维护。
- 新增 `ProtocolLimits`（协议默认安全上限 SSOT）：收敛 `maxRequestPayloadBytes/maxArgs/maxHeaderBytes` 的默认值来源，并通过单测锁定 decoder/args 默认值一致，防止安全参数漂移。
- 新增 `INFO`/`STATS` 命令（通过 `ServerInfoProvider` 由 server 注入实现），输出执行器/连接级统计摘要（队列/背压/关闭等）。
- 增加纯 Java 压测工具模块 `yierdis-bench` 与一键脚本 `scripts/bench.sh`，用于对比 `none/netty/unsafe` 后端的吞吐与延迟分位数。
- CLI REPL 输入解析规则：支持单/双引号、反斜杠转义、`\\xHH` 十六进制转义（sdssplitargs 风格）。
- bench 增强：吞吐/延迟统计加入 `errors` 计数，支持 `--strictReplies` 最小语义校验（PING/SET/GET）。
- 增加 BITMAP（`SETBIT/GETBIT/BITCOUNT`）与 HyperLogLog（`PFADD/PFCOUNT/PFMERGE`）命令族（复用 STRING），并提供 heap/off-heap 双路径语义测试。
- 增加 `scripts/smoke.sh`：一键验证 server 启动 + CLI + bench strictReplies 的最小链路。
- 协议/执行器加固：引入 backlog bytes 预算（`Command.retainedBytes()` 口径）与连接级 bytes 背压（与条数阈值并存），避免少量大请求积压导致内存驻留不可解释。
- 执行器增强：支持连接级公平调度（per-channel queue + round-robin），降低热点挤占风险。
- server 优雅关停：执行器支持可等待的 `shutdownGracefully()`（drain backlog + 资源回收），并保证 DB `shutdown()` 在执行器线程内执行（避免竞态）。
- decoder 输入上限参数化（DoS 防护）：新增 `--protocolMaxBulkBytes/--protocolMaxArgs/--protocolMaxLineBytes` 并由 server 透传到 protocol-netty decoder。
- off-heap capabilities：新增 `YierdisOffHeapAddressAllocator/YierdisOffHeapBlock`，以 capability 显式表达 raw address 能力（keyspace/expires 等索引结构可选启用）。
- Version SSOT：构建时注入 `yierdis-version.properties`，`HELLO` 的 `version` 字段从资源读取，避免硬编码常量漂移。
- 新增 `YierdisBuildInfo`（protocol SSOT）：收敛版本资源读取与 ASCII bytes 缓存，供 server/client/bench 复用一致的版本输出逻辑，避免多处复制漂移。
- 新增 `MEMORY STATS`：输出 maxmemory/heap/off-heap/结构开销等预算分解（明确为估算），用于解释拒写/淘汰行为。
- 命令层拆分：引入 `CommandRegistry` 与 domain `*Commands`，降低新增命令的修改半径并提升可测试性。
- off-heap 风险收敛：keys/expires 的 off-heap 使用改为显式开关（`--offheapKeysEnabled`，仅允许 unsafe 后端），默认安全。
- off-heap 后端发现升级：引入 `YierdisOffHeapAllocatorProvider`（ServiceLoader），并在 server fat-jar（shade）场景合并 services 资源，提升可运维性与错误可读性。
- 增加架构退化护栏测试：`CommandRegistryGuardTest`（最小命令集注册）、`CustomProtocolResyncIntegrationTest`（协议错误 resync）。
- 新增 `yierdis-bytes-netty`：提供 `NettyByteBufSink` 适配器，收敛 ByteBuf↔bytes 写出边界，供 server/offheap-netty 复用。
- INFO 输出：`INFO` 输出为 JSON string（文本分节），保留 `INFO YIERDIS`/`STATS` 的结构化指标输出用于教学与排障。

### Changed
- maxmemory 默认口径升级为 global（跨 DB 全局预算协调器）；并在 `INFO memory` 中增加 `yierdis_maxmemory_scope` 与 per-db 分摊诊断字段（便于排障与口径解释）。
- busy 拒绝增强为 `-ERR busy <reason>`，并通过 `STATS` 暴露原因计数器（not_running/queue_full/bytes_budget/offer_failed），提升可诊断性。
- 事务边界行为对齐 Redis：MULTI 入队阶段触发错误（例如触达队列上限）会进入 aborted，后续 EXEC 返回 `EXECABORT` 并丢弃事务队列。
- protocol-netty：`CustomRequestDecoder` request payload 解码低拷贝化（`ByteBuf` slice + `ByteBuffer` 解析），避免在 decoder 中按 `len` 分配整帧 heap `byte[]`；极端 composite 场景保留受上限约束的保守 fallback，并补充文档与回归测试。
- `yierdis-protocol` 依赖收敛：不再直接依赖 `yierdis-offheap-api`，改为依赖 `yierdis-bytes`；同时移除 `yierdis-offheap-api` 的 bytes 兼容别名，避免 SSOT 漂移。
- protocol 依赖方向收敛：`yierdis-core`/`yierdis-args` 依赖 `yierdis-protocol-model`（避免编译期引入 codec）；`yierdis-protocol-netty` 依赖 `yierdis-protocol-codec`；`yierdis-protocol` 调整为兼容聚合层以降低迁移成本。
- bench/server 参数体系收敛：共享参数由 `yierdis-args` 解析与校验；bench 通过 `--` 透传 server 参数，避免维护两套默认值。
- server↔DB 依赖倒置：server 装配与运维任务不再直接依赖 `YierdisDb`，统一通过 `DbEngine`/子 ops（expiration/eviction/memory 等）交互；执行器装配参数收敛到 `NettyCommandExecutorConfig`，降低构造参数爆炸与测试维护成本。
- `maxmemoryBytes` 统计口径调整为“heap 估算 + off-heap allocator.usedBytes 实占”，并避免对 off-heap string payload 双计数。
- 写命令执行顺序统一为 preflight（`prepareWrite`，含预淘汰/预检查）→执行→`enforceMaxmemory`→reply，避免 maxmemory 抛错导致的双 reply/协议损坏。
- 集合类回复结构化：`HGETALL`/`MEMORY STATS` 输出 object，`SMEMBERS` 输出 array。
- `KEYS` glob 语义补齐至 Redis 风格最小子集：支持 `[]`/范围/否定/反斜杠转义，并保持 byte 级二进制安全匹配。
- `MEMORY STATS` 数值字段类型对齐为 integer。
- `OBJECT ENCODING` 的非状态类字符串输出类型对齐为 bulk string（缺失 key 仍返回 nil）。
- `EXPIRE seconds<=0` 对齐为“立即删除”（返回 1 表示 key 存在并删除，0 表示不存在）。
- 整数解析错误文本对齐 Redis：统一为 `ERR value is not an integer or out of range`（不再携带参数 label）。
- Hash(off-heap) 编码策略对齐 Redis：packed(listpack-like) 起步，按阈值/oversize 升级到 dict，并移除不可达分支。
- 写命令热路径进一步减少不必要的 `byte[]` 物化：`SET/APPEND` 可从 `Command.frame()` 的参数 slice 直接拷贝到最终 payload（off-heap 或 raw string）。
- 命令执行路径收敛：以 `YierdisFastCommandProcessor` 为唯一权威实现（SSOT），测试主要覆盖协议无关的命令/回复语义。
- 淘汰与过期清理加入可配置时间预算（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），并将维护调度迁移到执行器线程，降低高压下维护任务的 tail latency 风险。
- STRING 扩展：补齐随机读写/按需扩容的零填充语义，支持 BITMAP/HLL 的原地修改；unsafe off-heap string 增加 `setByte/setBytes` 以支持随机写。
- protocol-netty：codec/adapters 迁移到独立包 `yier.bubu.redis.protocol.netty`，`yierdis-protocol` 独占 `yier.bubu.redis.protocol`（消除 split-package）。
- reply 写出语义收敛：命令层仅依赖 `ReplyWriter`；Custom Protocol v1 reply 统一由 `JsonLineReplyWriter` 编码为 NDJSON。
- reply bytes value streaming：`CustomProtocolV1NdjsonEncoder` 对 bulk-string(bytes) 的 strict UTF-8 校验 + JSON string escape + `$b64` fallback 改为 streaming；`JsonLineReplyWriter.bulkString(BytesSlice)` 不再按 value 大小 `new byte[len]` 全量拷贝，且在无需 escape 的 UTF-8 场景走 `BytesSlice.writeTo(BytesSink)`，贯通 off-heap/Netty sink fast-path。
- off-heap：core 通过 `YierdisOffHeapAddressAllocator` capability 选择 keyspace/expires 的 off-heap 路径，避免对具体后端类型的 `instanceof` 耦合；`yierdis-core` 不再编译期依赖 `yierdis-offheap-unsafe`。
- off-heap 可观测性增强：`YierdisOffHeapAllocators` 增加 ServiceLoader providers 发现摘要；server 启动期输出 backend/providers 诊断信息；缺失后端错误信息附带 discovered providers（摘要在失败路径懒加载，避免成功路径额外 ServiceLoader 扫描）。
- 背压语义增强：在全局队列满/bytes 预算耗尽时，触发可恢复的全局 backpressure（禁读 + 滞回恢复），降低 busy 风暴与“禁读后无法恢复”的风险。
- server args：`--port 0` 允许绑定随机端口（便于测试/开发避免端口冲突）。
- QUIT 行为收敛：`QUIT` 不再由 server handler 特判，改为 core 命令（通过 `ReplyWriter` 请求 close-after-reply），由执行器在 flush 后关闭连接并丢弃 post-QUIT backlog。
- server 写回路径改用 `NettyByteBufSink`（bytes-netty），避免通用写出适配依赖 off-heap 后端模块；offheap-netty slice 写出 fast-path 同步支持该 sink。
- client/CLI help 不再硬编码 jar 版本号，改为读取构建注入的 `yierdis-version.properties`。
- client/CLI 参数解析收敛到 picocli（usage/校验一致，避免手写 parser 漂移）。
- server bootstrap 内聚：pipeline 组装下沉到 `YierdisServerChannelInitializer`，bootstrap 聚焦启动与生命周期管理，降低装配逻辑分散与测试成本。
- 协议栈收敛：server/client/bench 统一走 Custom Protocol v1 decoder/line decoder，避免多套 codec 漂移。
- 连接态收敛：连接级运行时状态统一落到 server 私有 `ServerConnectionState`；执行器调度 state（per-channel 队列 + scheduled 标志）收敛到 `NettyExecutorChannelState`（`Channel.attr`），避免跨模块重复维护状态。
- request 解码语义明确：严格 schema（`cmd` string + 可选 `args` array，元素仅 `string|null`）；解析/校验错误返回 error 并尽量 resync。
- 命令路由加速：`CommandRegistry` 从线性扫描升级为开放寻址哈希索引（期望 O(1)，运行时零分配）。

### Fixed
- 修复知识库漂移：同步更新 `helloagents/wiki/api.md` 与 `helloagents/wiki/overview.md`，使命令清单与边界说明与代码实现一致（以代码为准）。
- 修复 server 参数校验失败静默退出：现在会输出明确错误信息 + usage，并保持退出码稳定（exit=2）。
- off-heap foreign 默认可用性增强：`foreign-memory` profile 默认启用；当选择 `--offheapBackend foreign` 且未启用 `jdk.incubator.foreign` 时，server 会自动重启补齐 `--add-modules`（并保留可预期错误提示作为兜底）。
- 安全性/可调试性平衡：仅对“可预期配置错误”做友好提示与稳定退出码处理；未知异常保留堆栈便于定位真实 bug。
- bench 依赖更新：压测工具写请求时改用 `yierdis-bytes` 的 `BytesSink`（避免依赖已移除的包路径）。
- 修复协议错误与 null 参数导致的连接断开：现在会返回明确的 `ERR ...`，并尽量保持连接可用（resync；触达安全上限时断连）。
- 错误消息输出统一做 CR/LF 过滤与限长，降低 response splitting 风险。
- unknown command 不再回显客户端输入。
- server args 增强：新增 `--executorSchedulingPolicy`、`--offheapKeysEnabled`、`--protocolMaxBulkBytes/--protocolMaxArgs/--protocolMaxLineBytes`，并在 args SSOT 层完成归一化与校验。
- 修复 client 超时后继续复用连接可能导致的响应错配：超时后关闭连接并标记不可复用。
- client 加固：response queue 边界化（有界队列 + 溢出关闭 + close/exception 唤醒），避免 flood/OOM 与无意义超时等待。
- 修复 pipeline 场景下 QUIT 可能破坏顺序语义/产生副作用：QUIT 后连接关闭并跳过该连接后续已入队命令（仅回收，不执行）。
- 修复 `DirectBytesSink` 的 `memoryAddress()` 行为：当 `hasMemoryAddress()==false` 时，明确抛出 `UnsupportedOperationException`（避免错误的 `Interface.super` 调用）。
- 修复多处写命令在写出 reply 后再执行 `enforceMaxmemory()` 可能产生的“双 reply”（正常 reply + error reply）问题。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
