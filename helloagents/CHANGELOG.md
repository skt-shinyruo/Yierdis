# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Added
- 引入单线程 `CommandExecutor` 解耦 Netty I/O 与命令执行（保持单线程命令语义），并增加 `--ioThreads` / `--executorQueueCapacity` 配置与 `ERR busy` 背压。
- 增加 off-heap allocator 泄漏回归测试，覆盖淘汰/删除/过期与 shutdown 释放路径。
- 增加纯 Java 压测工具模块 `yierdis-bench` 与一键脚本 `scripts/bench.sh`，用于对比 `none/netty/unsafe` 后端的吞吐与延迟分位数。

### Changed
- `maxmemoryBytes` 统计口径调整为“heap 估算 + off-heap allocator.usedBytes 实占”，并避免对 off-heap string payload 双计数。
- 命令执行路径收敛：以 `YierdisFastCommandProcessor` 为唯一权威实现（SSOT），测试主要覆盖 fast RESP pipeline。

### Fixed
- 修复协议错误与 `$-1`（null bulk string）参数导致的连接断开：现在会返回明确的 `ERR ...`（协议错误会关闭连接）。
- RESP error 输出统一做 CR/LF 过滤与限长，降低 response splitting 风险。
- unknown command 不再回显客户端输入。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
