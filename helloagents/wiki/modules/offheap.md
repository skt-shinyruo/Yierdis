# offheap

## Purpose

提供堆外内存抽象层与多种后端实现，用于演示减少 GC 与不同内存管理策略。

## Module Overview

- **Responsibility:** 分配器 API、slice/buf 抽象、unsafe/netty/foreign 等后端
- **Status:** 🚧In Development
- **Last Updated:** 2026-02-08

## Specifications

### Requirement: bytes 抽象（跨模块复用，不等同于“绑定 off-heap”）
**Module:** bytes/offheap-api
bytes 抽象的 SSOT 已抽取到 `yierdis-bytes`（Netty-free）：
- `BytesSource`：统一的只读 bytes view（可由 heap byte[]、Netty ByteBuf、off-heap slice 等实现）
- `BytesSink/DirectBytesSink`：统一的写入目标（供 `JsonLineReplyWriter`、off-heap buf 写路径等复用）

`yierdis-offheap-api` 不再提供 bytes 兼容别名（Breaking）：所有 bytes 视图/写出接口以 `yierdis-bytes` 为 SSOT；Netty 适配（`ByteBuf` sink/source）位于 `yierdis-bytes-netty`。

### Requirement: 可选启用的堆外存储
**Module:** offheap
当用户启用 `--offheapBackend` 时，部分数据结构可迁移到堆外，且命令行为保持一致。

#### Scenario: GET 走 slice 写出路径
条件：字符串值存储在 off-heap
- 预期：`GET` 回复优先使用 off-heap slice，避免额外分配 heap `byte[]`

### Requirement: usedBytes 可回归验证（泄漏检测）
**Module:** offheap
分配器应提供 `usedBytes()` 观测点，并在测试中覆盖删除/过期/淘汰/shutdown 等关键路径，确保堆外资源可回归验证。

### Requirement: capabilities/SPI（address allocator）
**Module:** offheap-api/core
为避免 core 侧对具体后端（unsafe/netty/foreign）的 `instanceof` 耦合，off-heap 后端能力以接口显式化：
- `YierdisOffHeapAllocator`：通用 buf/slice 分配能力（所有后端）
- `YierdisOffHeapAddressAllocator`：可选能力（raw address + copy/memset + block），供 keyspace/expires 等 off-heap 索引结构使用
- `YierdisOffHeapBlock`：`allocateBlock` 返回的 owning handle（`close()` 释放），避免 address 生命周期管理分散

当前实现策略：
- core 仅在 allocator 实现了 `YierdisOffHeapAddressAllocator` 且显式开启 `--offheapKeysEnabled` 时启用 off-heap keyspace/expires；否则 keyspace/expires 保持 heap 路径（默认安全）
- string value 的 off-heap 存储仍可由任意 `YierdisOffHeapAllocator` 支持（buf/slice 路径）

### Requirement: 后端发现（ServiceLoader）与启动期可用性校验
**Module:** offheap-api
后端加载由 `YierdisOffHeapAllocators` 统一负责：
- 优先通过 `ServiceLoader` 发现 `YierdisOffHeapAllocatorProvider`（netty/unsafe/foreign 各自注册）
- 在 server fat-jar（shade）场景下，使用 `ServicesResourceTransformer` 合并 `META-INF/services`，确保多后端可同时发现
- 若指定后端不可用，启动期直接抛出明确错误（提示缺失依赖/需要的 profile），避免运行中才暴露
- `foreign` 后端默认构建已包含（profile `foreign-memory` 默认启用）；运行时仍需启用 incubator 模块（`--add-modules jdk.incubator.foreign`）。为降低部署复杂度，当 server 检测到 `--offheapBackend foreign` 且模块未启用时会自动重启补齐该 JVM 参数（并保留原 JVM 参数，例如 `-Xmx` / `-XX:MaxDirectMemorySize`）。
- 建议在 server 启动时输出“可发现的 providers / 最终选择结果”，提升可运维性与排障效率

### Requirement: 输入 bytes 直写 off-heap（减少双拷贝）
**Module:** offheap
为了支持“端到端低分配”的写入路径，off-heap buf 提供从 `BytesSource` 写入的入口，避免无意义的中间拷贝。

#### Scenario: SET/APPEND 等写命令零中转写入
条件：启用非 unsafe 的 off-heap backend（例如 `netty/foreign`）
- 预期：服务端可调用 `YierdisOffHeapBuf#setBytes(int, BytesSource, int, int)` 将 value 写入 off-heap
- 说明：Custom Protocol v1 的 request 当前会将 JSON args 物化为 heap `byte[]`；如需进一步减少拷贝，可在未来引入更适合二进制 payload 的 framing，并通过 `Command.frame()` 暴露可选 `BytesSource` slice 视图。

## Dependencies

- `yierdis-bytes`：中立 bytes 抽象（SSOT，不依赖 Netty）
- `yierdis-bytes-netty`：Netty 适配层（可选；依赖 Netty）
- `yierdis-offheap-api`：核心 API（不依赖 Netty）
- `yierdis-offheap-netty`：Netty 适配（可选；依赖 Netty）
- `yierdis-offheap-unsafe`：Unsafe 后端（可选；通过 `sun.misc.Unsafe` 提供 raw memory 读写/copy）
- `yierdis-offheap-foreign`：Foreign Memory API 后端（可选；JDK 预览/演示用途）

## Change History

- 2026-01-04：增加 off-heap allocator 泄漏回归测试（shutdown 后 usedBytes 回到基线/归零）。
- 2026-01-08：写路径增强：支持“从输入源直接写入 off-heap”，减少写路径的 heap 中转分配。
- 2026-01-14：offheap-api 去 Netty 依赖：写出/适配边界收敛到 bytes 抽象，Netty 适配下沉到 adapter 模块（例如 bytes-netty/offheap-netty）。
- 2026-01-17：Breaking：移除 deprecated bytes alias（`YierdisBytes*`），bytes SSOT 统一为 `yierdis-bytes`。
- 2026-01-15：Unsafe 后端补齐 raw memory 访问封装（`YierdisUnsafeAccess`），并将 `PlatformDependent` 等 Netty internal 的使用限制在 off-heap/unsafe 后端实现内，降低依赖外溢风险。
- 2026-01-16：capabilities 加固：引入 `YierdisOffHeapAddressAllocator/YierdisOffHeapBlock`，core 通过 capability 选择 keyspace/expires 的 off-heap 路径，避免对具体后端的 `instanceof` 耦合。
- 2026-01-16：默认安全：keys/expires 的 off-heap 使用改为显式开关（`--offheapKeysEnabled`，仅允许 unsafe 后端）。
- 2026-01-16：后端加载升级：引入 `YierdisOffHeapAllocatorProvider`（ServiceLoader）并在 server shade 场景合并 services 资源，提升可运维性与错误可读性。
- 2026-01-16：可观测性增强：增加 providers 发现摘要（ServiceLoader）与 server 启动诊断输出；缺失后端错误信息附带 discovered providers（摘要在失败路径懒加载，避免成功路径额外 ServiceLoader 扫描）。
- 2026-01-23：off-heap Hash 编码策略对齐 Redis：小 hash 以 packed(listpack-like) 起步，按阈值/oversize 升级到 dict，并增强 SDS 分配/升级路径的异常安全（避免泄漏）。
- 2026-02-08：foreign-memory 默认启用：默认构建包含 `yierdis-offheap-foreign`；当选择 `--offheapBackend foreign` 且未启用 `--add-modules jdk.incubator.foreign` 时，server 自动重启补齐该参数。
- 2026-02-24：unsafe 后端审计加固：将 Netty internal `PlatformDependent` 的调用收敛到单一 façade（`NettyPlatformDependentMemoryAccess`），并在 backend 不可用/类加载失败（LinkageError）时输出可操作的诊断信息（避免冗余堆栈）。
