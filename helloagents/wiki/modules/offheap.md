# offheap

## Purpose

提供堆外内存抽象层与多种后端实现，用于演示减少 GC 与不同内存管理策略。

## Module Overview

- **Responsibility:** 分配器 API、slice/buf 抽象、unsafe/netty/foreign 等后端
- **Status:** 🚧In Development
- **Last Updated:** 2026-01-14

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

### Requirement: RESP frame slice 直接写入 off-heap（减少双拷贝）
**Module:** offheap
为了支持“端到端低分配”的写入路径，off-heap buf 提供从 `YierdisBytesSource` 写入的入口；当输入来自 Netty `ByteBuf` 时，通过 `yierdis-offheap-netty` 的 adapter 将其暴露为 `YierdisBytesSource`。

#### Scenario: SET/APPEND 等写命令零中转写入
条件：启用非 unsafe 的 off-heap backend（例如 `netty/foreign`），并使用 RESP bulk string 发送 value
- 预期：服务端可调用 `YierdisOffHeapBuf#setBytes(int, YierdisBytesSource, int, int)` 将 value 写入 off-heap
- 预期：当输入源是 Netty `ByteBuf` 时，通过 `yierdis-offheap-netty` 的 adapter 将其暴露为 `YierdisBytesSource`
- 预期：减少“RESP frame → heap byte[] → off-heap”的中间分配与额外拷贝

## Dependencies

- `yierdis-offheap-api`：核心 API（不依赖 Netty）
- `yierdis-offheap-netty`：Netty 适配（可选；依赖 Netty）

## Change History

- 2026-01-04：增加 off-heap allocator 泄漏回归测试（shutdown 后 usedBytes 回到基线/归零）。
- 2026-01-08：写路径增强：支持“从输入源直接写入 off-heap”，减少写路径的 heap 中转分配。
- 2026-01-14：offheap-api 去 Netty 依赖：以 `YierdisBytesSink/YierdisBytesSource` 替代 ByteBuf 直接依赖，Netty adapter 下沉到 offheap-netty 模块。
