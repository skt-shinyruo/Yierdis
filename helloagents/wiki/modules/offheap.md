# offheap

## Purpose

提供堆外内存抽象层与多种后端实现，用于演示减少 GC 与不同内存管理策略。

## Module Overview

- **Responsibility:** 分配器 API、slice/buf 抽象、unsafe/netty/foreign 等后端
- **Status:** 🚧In Development
- **Last Updated:** 2026-01-04

## Specifications

### Requirement: 可选启用的堆外存储
**Module:** offheap
当用户启用 `--offheapBackend` 时，部分数据结构可迁移到堆外，且命令行为保持一致。

#### Scenario: GET 走 slice 写出路径
条件：字符串值存储在 off-heap
- 预期：`GET` 回复优先使用 off-heap slice，避免额外分配 heap `byte[]`

### Requirement: usedBytes 可回归验证（泄漏检测）
**Module:** offheap
分配器应提供 `usedBytes()` 观测点，并在测试中覆盖删除/过期/淘汰/shutdown 等关键路径，确保堆外资源可回归验证。

## Dependencies

- （无）

## Change History

- 2026-01-04：增加 off-heap allocator 泄漏回归测试（shutdown 后 usedBytes 回到基线/归零）。
