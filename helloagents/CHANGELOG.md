# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Breaking
- 移除 deprecated bytes alias（`YierdisBytesSink/YierdisBytesSource/YierdisDirectBytesSink` 等），bytes SSOT 统一为 `yierdis-bytes`。

### Added
- 新增 `yierdis-bytes` 中立模块：承载 `BytesSource/BytesSink/BytesSlice` 抽象，供 protocol/off-heap/I/O 复用，避免 `yierdis-protocol` 通过 “off-heap” 命名模块复用 bytes 接口造成依赖误导。
- 新增 `RespMap`（RESP3 map 最小对象模型）与 client 侧 RESP3 最小解码能力（`%` map、`_` null），用于覆盖 `HELLO 3` 分支。
- 新增 `RespInlineCommandParser`（sdssplitargs 风格），server inline command 与 CLI REPL 共用同一套解析规则，避免规则漂移。
- 新增 `YierdisServerBootstrap`：抽取 server 装配/生命周期管理，可在测试/工具中复用启动与关停逻辑。
- Maven 多模块拆分：引入 `yierdis-protocol`（RESP SSOT）、`yierdis-core`（DB/命令 SSOT）、`yierdis-args`（参数 SSOT）、`yierdis-client`（client/CLI），并调整 `yierdis-server`/`yierdis-bench` 依赖方向。
- 新增 `yierdis-protocol-netty`：承载 Netty codec/adapters（decoder/encoder/frame/session）；`yierdis-protocol` 收敛为 Netty-free SSOT（对象模型 + `RespWriter` + `RespFrame/RespSession` 抽象）。
- 升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：批量 `write` + 末尾 `flush` 合并、连接级 `autoRead` 背压（high/low 滞回阈值）、全局有界队列与 `-ERR busy` 保护。
- 增加 off-heap allocator 泄漏回归测试，覆盖淘汰/删除/过期与 shutdown 释放路径。
- 新增 `ConnectionContext`（协议会话 SSOT）：承载 RESP2/RESP3 协商状态（`RespSession`，`Channel.attr` 绑定），避免跨模块重复维护协议状态。
- 新增 `ServerConnectionState`（server 私有）：承载 pending/backpressure/closing/counters 等运行时连接状态，并通过 `RespSession` 委托协议协商状态，避免 protocol adapter 携带 server 语义。
- 新增 `RespLimits`（协议默认安全上限 SSOT）：收敛 `maxBulkBytes/maxArgs/maxLineBytes/maxArrayLen/maxNestingDepth` 的默认值来源，并通过单测锁定 parser/decoder/args 默认值一致，防止安全参数漂移。
- 新增 `INFO`/`STATS` 命令（通过 `ServerInfoProvider` 由 server 注入实现），输出执行器/连接级统计摘要（队列/背压/关闭等）。
- 增加纯 Java 压测工具模块 `yierdis-bench` 与一键脚本 `scripts/bench.sh`，用于对比 `none/netty/unsafe` 后端的吞吐与延迟分位数。
- 支持最小 RESP3（`HELLO 3` 协商 + null 返回）与 inline command（调试用；支持单/双引号、反斜杠转义、`\\xHH`），提升常见 Redis 客户端兼容性。
- bench 增强：吞吐/延迟统计加入 `errors` 计数，支持 `--strictReplies` 最小语义校验（PING/SET/GET），并复用 `yierdis-protocol-netty` 的 RESP codec。
- 增加 BITMAP（`SETBIT/GETBIT/BITCOUNT`）与 HyperLogLog（`PFADD/PFCOUNT/PFMERGE`）命令族（复用 STRING），并提供 heap/off-heap 双路径语义测试。
- 增加 `scripts/smoke.sh`：一键验证 server 启动 + CLI + bench strictReplies 的最小链路。
- 协议/执行器加固：引入 backlog bytes 预算（`RespFrame.retainedBytes()` 口径）与连接级 bytes 背压（与条数阈值并存），避免少量大 bulk 积压导致内存驻留不可解释。
- 执行器增强：支持连接级公平调度（per-channel queue + round-robin）与可配置 frame compaction（阈值/比率/最大拷贝上限），降低热点挤占与驻留抖动风险。
- server 优雅关停：执行器支持可等待的 `shutdownGracefully()`（drain backlog + 资源回收），并保证 DB `shutdown()` 在执行器线程内执行（避免竞态）。
- decoder 输入上限参数化（DoS 防护）：新增 `--protocolMaxBulkBytes/--protocolMaxArgs/--protocolMaxLineBytes` 并由 server 透传到 protocol-netty decoder。
- off-heap capabilities：新增 `YierdisOffHeapAddressAllocator/YierdisOffHeapBlock`，以 capability 显式表达 raw address 能力（keyspace/expires 等索引结构可选启用）。
- Version SSOT：构建时注入 `yierdis-version.properties`，`HELLO` 的 `version` 字段从资源读取，避免硬编码常量漂移。
- 新增 `YierdisBuildInfo`（protocol SSOT）：收敛版本资源读取与 ASCII bytes 缓存，供 server/client/bench 复用一致的版本输出逻辑，避免多处复制漂移。
- 新增 `MEMORY STATS`：输出 maxmemory/heap/off-heap/结构开销等预算分解（明确为估算），用于解释拒写/淘汰行为。
- 新增 RESP3 set（`~`）最小子集支持：补齐 `RespType.SET`/`RespSet`/`RespWriter.setHeader` 与 `RespObjectParser` 对 `~` 的解析能力。
- 命令层拆分：引入 `CommandRegistry` 与 domain `*Commands`，降低新增命令的修改半径并提升可测试性。
- off-heap 风险收敛：keys/expires 的 off-heap 使用改为显式开关（`--offheapKeysEnabled`，仅允许 unsafe 后端），默认安全。
- off-heap 后端发现升级：引入 `YierdisOffHeapAllocatorProvider`（ServiceLoader），并在 server fat-jar（shade）场景合并 services 资源，提升可运维性与错误可读性。
- 增加架构退化护栏测试：`CommandRegistryGuardTest`（最小命令集注册）、`ConnectionContextIsolationTest`（连接级状态隔离）。
- 新增 `yierdis-bytes-netty`：提供 `NettyByteBufSink` 适配器，收敛 ByteBuf↔bytes 写出边界，供 server/offheap-netty 复用。

### Changed
- `yierdis-protocol` 依赖收敛：不再直接依赖 `yierdis-offheap-api`，改为依赖 `yierdis-bytes`；同时移除 `yierdis-offheap-api` 的 bytes 兼容别名，避免 SSOT 漂移。
- bench/server 参数体系收敛：共享参数由 `yierdis-args` 解析与校验；bench 通过 `--` 透传 server 参数，避免维护两套默认值。
- `maxmemoryBytes` 统计口径调整为“heap 估算 + off-heap allocator.usedBytes 实占”，并避免对 off-heap string payload 双计数。
- 写命令执行顺序统一为 preflight（`prepareWrite`，含预淘汰/预检查）→执行→`enforceMaxmemory`→reply，避免 maxmemory 抛错导致的双 reply/协议损坏。
- RESP3 连接下集合类回复更友好：`HGETALL`/`MEMORY STATS` 输出 map，`SMEMBERS` 输出 set；RESP2 行为保持不变。
- `KEYS` glob 语义补齐至 Redis 风格最小子集：支持 `[]`/范围/否定/反斜杠转义，并保持 byte 级二进制安全匹配。
- Hash(off-heap) 编码策略对齐 Redis：packed(listpack-like) 起步，按阈值/oversize 升级到 dict，并移除不可达分支。
- 写命令热路径进一步减少不必要的 `byte[]` 物化：`SET/APPEND` 可从 `RespCommand.frame()` 的参数 slice 直接拷贝到最终 payload（off-heap 或 raw string）。
- 命令执行路径收敛：以 `YierdisFastCommandProcessor` 为唯一权威实现（SSOT），测试主要覆盖 fast RESP pipeline。
- 淘汰与过期清理加入可配置时间预算（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），并将维护调度迁移到执行器线程，降低高压下维护任务的 tail latency 风险。
- STRING 扩展：补齐随机读写/按需扩容的零填充语义，支持 BITMAP/HLL 的原地修改；unsafe off-heap string 增加 `setByte/setBytes` 以支持随机写。
- protocol-netty：codec/adapters 迁移到独立包 `yier.bubu.redis.protocol.netty`，`yierdis-protocol` 独占 `yier.bubu.redis.protocol`（消除 split-package）。
- protocol-netty：`RespEncoder` 写出语义收敛到 `RespWriter`（通过 `NettyByteBufSink` 适配），避免 server fast-path 与 codec 输出行为漂移。
- off-heap：core 通过 `YierdisOffHeapAddressAllocator` capability 选择 keyspace/expires 的 off-heap 路径，避免对具体后端类型的 `instanceof` 耦合；`yierdis-core` 不再编译期依赖 `yierdis-offheap-unsafe`。
- off-heap 可观测性增强：`YierdisOffHeapAllocators` 增加 ServiceLoader providers 发现摘要；server 启动期输出 backend/providers 诊断信息；缺失后端错误信息附带 discovered providers（摘要在失败路径懒加载，避免成功路径额外 ServiceLoader 扫描）。
- 背压语义增强：在全局队列满/bytes 预算耗尽时，触发可恢复的全局 backpressure（禁读 + 滞回恢复），降低 busy 风暴与“禁读后无法恢复”的风险。
- server args：`--port 0` 允许绑定随机端口（便于测试/开发避免端口冲突）。
- QUIT 行为收敛：`QUIT` 不再由 server handler 特判，改为 core 命令（通过 `RespWriter` 请求 close-after-reply），由执行器在 flush 后关闭连接并丢弃 post-QUIT backlog。
- server 写回路径改用 `NettyByteBufSink`（bytes-netty），避免通用写出适配依赖 off-heap 后端模块；offheap-netty slice 写出 fast-path 同步支持该 sink。
- client/CLI help 不再硬编码 jar 版本号，改为读取构建注入的 `yierdis-version.properties`。
- client/CLI 参数解析收敛到 picocli（usage/校验一致，避免手写 parser 漂移）。
- server bootstrap 内聚：pipeline 组装下沉到 `YierdisServerChannelInitializer`，bootstrap 聚焦启动与生命周期管理，降低装配逻辑分散与测试成本。
- 协议栈收敛：`RespDecoder` 仅切帧输出 `NettyRespFrame`（frame/zero-copy 取向），client/bench/CLI 统一走 frame；对象模型解析仅用于调试/输出（按需解析）。
- 连接态二分：`ConnectionContext` 仅表达连接级协议会话（RESP2/RESP3）；pending/backpressure/closing/counters 迁移到 server 私有 `ServerConnectionState`；执行器调度 state（per-channel queue + scheduled 标志）继续收敛到 `NettyExecutorChannelState`（`Channel.attr`），避免 protocol 模块携带 server 语义与调度实现细节。
- request 解码严格化：明确允许集合（array + inline），对 RESP reply/RESP3 前缀与控制字符前缀统一 protocol error，并由 server 返回错误后关闭连接，避免状态错乱。
- 命令路由加速：`CommandRegistry` 从线性扫描升级为开放寻址哈希索引（期望 O(1)，运行时零分配）。

### Fixed
- bench 依赖更新：压测工具写请求时改用 `yierdis-bytes` 的 `BytesSink`（避免依赖已移除的包路径）。
- 修复协议错误与 `$-1`（null bulk string）参数导致的连接断开：现在会返回明确的 `ERR ...`（协议错误会关闭连接）。
- RESP error 输出统一做 CR/LF 过滤与限长，降低 response splitting 风险。
- unknown command 不再回显客户端输入。
- server args 增强：新增 `--executorSchedulingPolicy`、`--frameCompaction*`、`--offheapKeysEnabled`，并在 args SSOT 层完成归一化与校验。
- 修复 client 超时后继续复用连接可能导致的 RESP 响应错配：超时后关闭连接并标记不可复用。
- client 加固：response queue 边界化（有界队列 + 溢出关闭 + close/exception 唤醒），避免 flood/OOM 与无意义超时等待。
- 修复 pipeline 场景下 QUIT 可能破坏顺序语义/产生副作用：QUIT 后连接关闭并跳过该连接后续已入队命令（仅回收，不执行）。
- 修复 `DirectBytesSink` 的 `memoryAddress()` 行为：当 `hasMemoryAddress()==false` 时，明确抛出 `UnsupportedOperationException`（避免错误的 `Interface.super` 调用）。
- 修复多处写命令在写出 reply 后再执行 `enforceMaxmemory()` 可能产生的“双 reply”（正常 reply + error reply）问题。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
