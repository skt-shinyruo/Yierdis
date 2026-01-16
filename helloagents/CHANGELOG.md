# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Added
- Maven 多模块拆分：引入 `yierdis-protocol`（RESP SSOT）、`yierdis-core`（DB/命令 SSOT）、`yierdis-args`（参数 SSOT）、`yierdis-client`（client/CLI），并调整 `yierdis-server`/`yierdis-bench` 依赖方向。
- 新增 `yierdis-protocol-netty`：承载 Netty codec/adapters（decoder/encoder/frame/session）；`yierdis-protocol` 收敛为 Netty-free SSOT（对象模型 + `RespWriter` + `RespFrame/RespSession` 抽象）。
- 升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：批量 `write` + 末尾 `flush` 合并、连接级 `autoRead` 背压（high/low 滞回阈值）、全局有界队列与 `-ERR busy` 保护。
- 增加 off-heap allocator 泄漏回归测试，覆盖淘汰/删除/过期与 shutdown 释放路径。
- off-heap API 升级：引入 `YierdisBytesSink/YierdisBytesSource` 抽象，`yierdis-offheap-api` 去 Netty 依赖；Netty 适配下沉到 `yierdis-offheap-netty`。
- 增加纯 Java 压测工具模块 `yierdis-bench` 与一键脚本 `scripts/bench.sh`，用于对比 `none/netty/unsafe` 后端的吞吐与延迟分位数。
- 支持最小 RESP3（`HELLO 3` 协商 + null 返回）与 inline command（调试用；支持单/双引号、反斜杠转义、`\\xHH`），提升常见 Redis 客户端兼容性。
- bench 增强：吞吐/延迟统计加入 `errors` 计数，支持 `--strictReplies` 最小语义校验（PING/SET/GET），并复用 `yierdis-protocol-netty` 的 RESP codec。
- 增加 BITMAP（`SETBIT/GETBIT/BITCOUNT`）与 HyperLogLog（`PFADD/PFCOUNT/PFMERGE`）命令族（复用 STRING），并提供 heap/off-heap 双路径语义测试。
- 增加 `scripts/smoke.sh`：一键验证 server 启动 + CLI + bench strictReplies 的最小链路。
- 协议/执行器加固：引入 backlog bytes 预算（`RespFrame.length()` 口径）与连接级 bytes 背压（与条数阈值并存），避免少量大 bulk 积压导致内存驻留不可解释。
- server 优雅关停：执行器支持可等待的 `shutdownGracefully()`（drain backlog + 资源回收），并保证 DB `shutdown()` 在执行器线程内执行（避免竞态）。
- decoder 输入上限参数化（DoS 防护）：新增 `--protocolMaxBulkBytes/--protocolMaxArgs/--protocolMaxLineBytes` 并由 server 透传到 protocol-netty decoder。
- off-heap capabilities：新增 `YierdisOffHeapAddressAllocator/YierdisOffHeapBlock`，以 capability 显式表达 raw address 能力（keyspace/expires 等索引结构可选启用）。
- Version SSOT：构建时注入 `yierdis-version.properties`，`HELLO` 的 `version` 字段从资源读取，避免硬编码常量漂移。

### Changed
- bench/server 参数体系收敛：共享参数由 `yierdis-args` 解析与校验；bench 通过 `--` 透传 server 参数，避免维护两套默认值。
- `maxmemoryBytes` 统计口径调整为“heap 估算 + off-heap allocator.usedBytes 实占”，并避免对 off-heap string payload 双计数。
- 写命令热路径进一步减少不必要的 `byte[]` 物化：`SET/APPEND` 可从 `RespCommand.frame()` 的参数 slice 直接拷贝到最终 payload（off-heap 或 raw string）。
- 命令执行路径收敛：以 `YierdisFastCommandProcessor` 为唯一权威实现（SSOT），测试主要覆盖 fast RESP pipeline。
- 淘汰与过期清理加入可配置时间预算（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），并将维护调度迁移到执行器线程，降低高压下维护任务的 tail latency 风险。
- STRING 扩展：补齐随机读写/按需扩容的零填充语义，支持 BITMAP/HLL 的原地修改；unsafe off-heap string 增加 `setByte/setBytes` 以支持随机写。
- protocol-netty：codec/adapters 迁移到独立包 `yier.bubu.redis.protocol.netty`，`yierdis-protocol` 独占 `yier.bubu.redis.protocol`（消除 split-package）。
- off-heap：core 通过 `YierdisOffHeapAddressAllocator` capability 选择 keyspace/expires 的 off-heap 路径，避免对具体后端类型的 `instanceof` 耦合；`yierdis-core` 不再编译期依赖 `yierdis-offheap-unsafe`。

### Fixed
- 修复协议错误与 `$-1`（null bulk string）参数导致的连接断开：现在会返回明确的 `ERR ...`（协议错误会关闭连接）。
- RESP error 输出统一做 CR/LF 过滤与限长，降低 response splitting 风险。
- unknown command 不再回显客户端输入。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
