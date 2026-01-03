# Changelog

本文件记录项目的重要变更，格式参考 Keep a Changelog（语义化版本 SemVer）。

## [Unreleased]

### Fixed
- 修复协议错误与 `$-1`（null bulk string）参数导致的连接断开：现在会返回明确的 `ERR ...`（协议错误会关闭连接）。

## [0.1.0-SNAPSHOT] - 2026-01-01

### Added
- 初始化 HelloAGENTS 知识库（`helloagents/`）。
