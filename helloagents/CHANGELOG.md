# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Added
- 升级为 Netty 体系内单线程 `NettyCommandExecutor`（`DefaultEventExecutorGroup(1)`）：批量 `write` + 末尾 `flush` 合并、连接级 `autoRead` 背压（high/low 滞回阈值）、全局有界队列与 `-ERR busy` 保护。
- 增加 off-heap allocator 泄漏回归测试，覆盖淘汰/删除/过期与 shutdown 释放路径。
- off-heap buf 增强：支持从 Netty `ByteBuf` 直接写入（`YierdisOffHeapBuf#setBytes(int, ByteBuf, int, int)`），用于降低 RESP frame → off-heap 写路径的 heap 中转分配。
- 增加纯 Java 压测工具模块 `yierdis-bench` 与一键脚本 `scripts/bench.sh`，用于对比 `none/netty/unsafe` 后端的吞吐与延迟分位数。
- 支持最小 RESP3（`HELLO 3` 协商 + null 返回）与 inline command（调试用；支持单/双引号、反斜杠转义、`\\xHH`），提升常见 Redis 客户端兼容性。
- bench 增强：吞吐/延迟统计加入 `errors` 计数，支持 `--strictReplies` 最小语义校验，并扩展 RESP3 基础类型跳过以兼容不同服务端。
- 增加 BITMAP（`SETBIT/GETBIT/BITCOUNT`）与 HyperLogLog（`PFADD/PFCOUNT/PFMERGE`）命令族（复用 STRING），并提供 heap/off-heap 双路径语义测试。

### Changed
- `maxmemoryBytes` 统计口径调整为“heap 估算 + off-heap allocator.usedBytes 实占”，并避免对 off-heap string payload 双计数。
- 写命令热路径进一步减少不必要的 `byte[]` 物化：`SET/APPEND` 可从 `RespCommand.frame()` 的参数 slice 直接拷贝到最终 payload（off-heap 或 raw string）。
- 命令执行路径收敛：以 `YierdisFastCommandProcessor` 为唯一权威实现（SSOT），测试主要覆盖 fast RESP pipeline。
- 淘汰与过期清理加入可配置时间预算（`--evictionTimeLimitMillis` / `--expireCleanupTimeLimitMillis`），并将维护调度迁移到执行器线程，降低高压下维护任务的 tail latency 风险。
- STRING 扩展：补齐随机读写/按需扩容的零填充语义，支持 BITMAP/HLL 的原地修改；unsafe off-heap string 增加 `setByte/setBytes` 以支持随机写。

### Fixed
- 修复协议错误与 `$-1`（null bulk string）参数导致的连接断开：现在会返回明确的 `ERR ...`（协议错误会关闭连接）。
- RESP error 输出统一做 CR/LF 过滤与限长，降低 response splitting 风险。
- unknown command 不再回显客户端输入。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
