# 2026-04-19: Yierdis Performance Optimization Roadmap

## Summary

Yierdis 目前已经具备一个清晰的 Redis-like 单机内存服务端形态：Netty 负责连接与协议，`yierdis-core-command` 负责命令执行，`yierdis-core-db` 负责数据结构与 TTL/maxmemory，`yierdis-memory-foreign` 负责基于 JDK 25 FFM 的 native memory。

当前阶段最值得优化的，不是功能面，而是几条会直接影响吞吐、尾延迟和 GC 压力的关键路径：

- 请求入口仍存在明显的 `ByteBuf -> JSON -> String/List<String> -> byte[] -> off-heap` 中转链路。
- off-heap 能力已经接入，但多处内部接口仍以 `byte[]` 为边界，导致 TTL、淘汰、集合读取等路径会把 off-heap 数据重新 materialize 回 heap。
- FFM allocator 当前采用“每个 region 一个 `Arena.ofConfined()`”的模型，更偏简单与安全，而不是面向高碎片、高频小分配场景的 steady-state 吞吐。
- 单线程命令执行模型仍然是架构中心，在它被证明为主瓶颈之前，不应该过早进入多执行器或并行 DB kernel 重构。

因此，本次设计的目标不是一次性重写协议层、执行器或内存内核，而是给出一个 **可归因、可回滚、可测量** 的分阶段优化路线：先补基线与观测，再削掉高价值复制链路，再推进 off-heap 边界收敛与 allocator 池化，最后再判断是否需要突破单线程执行模型。

## Goals

- 建立一个适合当前仓库阶段的性能优化顺序，避免同时修改多个高耦合子系统。
- 优先优化请求解码与数据进入 DB 的高频链路，减少不必要的 UTF-8 编解码和 heap 分配。
- 让 off-heap/FFM 的收益不仅停留在“存储落地”，而是延伸到 TTL、淘汰、集合读写和 reply encode 等内部路径。
- 在不破坏当前模块边界与单线程命令语义的前提下，提高吞吐并降低 GC/heap 压力。
- 补齐 bench 与 observability，使后续每一步优化都能基于稳定数据比较，而不是靠体感或单次 benchmark。

## Non-Goals

- 本次不直接实现 Redis 协议兼容，也不改变 `Custom Protocol v1` 的既有对外兼容承诺。
- 本次不把整个 server 改造成多线程共享状态模型，不引入锁分段或跨线程 DB 访问。
- 本次不追求一次性把所有 `byte[]` API 都替换成 `BytesSlice`/`KeyHandle`；只优先覆盖最值得优化的热点路径。
- 本次不试图把 Yierdis 变成生产级分布式系统；复制、集群、持久化、ACL、TLS 等仍然保持 out-of-scope。
- 本次不把现有 bench 升级成 JMH 或外部 benchmark 框架优先工程；先在当前纯 Java bench 体系内补足关键 workload 与指标。

## Current State

### 1) Request/Reply Protocol Still Pays A Large Heap Tax

当前 `Custom Protocol v1` 的请求解码路径是：

1. Netty `ByteBuf` 进入 `CustomRequestDecoder`
2. 解析 `<len>:<json>\n`
3. 把 payload 解析成 JSON object
4. 取出 `cmd` 和 `args`，转成 `String` 和 `List<String>`
5. 在 `ProtocolCommandAdapter` 中重新编码成 UTF-8 `byte[]`
6. 交给 `ByteArrayExecutionRequest`
7. 写命令进入 DB 时，再视情况拷到 off-heap

这条路径的优点是简单、可恢复、便于 CLI/debug，但它让入口已经完成的字节解析结果多次跨越 `ByteBuf`、`String`、`byte[]`、off-heap 之间的边界。

对应路径：

- `yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
- `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`

### 2) Off-Heap Storage Exists, But Interface Boundaries Still Pull Data Back To Heap

项目已经明确走 FFM-only 路线，keyspace、expires、字符串和集合内部结构都默认使用 native memory。但不少内部 API 仍然是 `byte[]` 或 `List<byte[]>` 语义。

这会带来两个后果：

- 写路径虽然最终落到 off-heap，但入口通常仍是 heap `byte[]`
- 读路径、TTL、淘汰、scan、集合输出等一旦触碰到 `byte[]` 风格接口，就会把 off-heap 数据重新 materialize 回 heap

项目现有文档已经明确指出这一点，尤其是 `randomKey()`、`forEach()`、返回 `List<byte[]>` 的集合读取接口，以及某些非 direct sink 下的 reply 写回退化路径。

对应路径：

- `docs/offheap-copy-behavior.md`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`

### 3) FFM Allocation Model Favors Simplicity Over Small-Object Efficiency

当前 `YierdisFfmMemoryRuntime.allocateRegion(owner, bytes)` 每次分配都会新建一个 `Arena.ofConfined()`，然后返回一个独立 region。`YierdisFfmBlobStore` 也基本是“一块 blob 一个 region”的模型。

这个模型的优点：

- 生命周期边界非常清晰
- leak 检查容易做
- 与 owner-thread discipline 容易保持一致

它的代价：

- 小对象很多时，allocator 元数据与 region 数量可能过多
- 内存局部性较弱
- 高频碎片化 workload 下，吞吐和稳态内存形态未必理想

对应路径：

- `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBlobStore.java`
- `docs/ffm-usage.md`

### 4) Single-Thread Command Semantics Are Intentional, Not Accidental

当前 server 明确采用单线程命令执行器：I/O 线程负责 submit，命令在专用 executor 上串行执行，TTL cleanup 也通过 executor 线程协作执行。这保持了 Redis-like 的单线程命令语义，并降低了 DB kernel 的并发复杂度。

项目已经为这个模型做了不少细节优化：

- 有界队列
- bytes budget
- fair scheduling / global scheduling
- drain budget
- flush coalescing
- 连接关闭后的 side-effect skip

因此，多执行器或并行 DB 改造不应被视为默认下一步，而应被视为“在前述热点都优化后，若单线程仍是主瓶颈，再考虑”的后续阶段。

对应路径：

- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

### 5) Existing Observability And Bench Are Useful But Not Yet Sufficient For Fine-Grained Tuning

当前 `INFO/STATS/MEMORY STATS` 已经暴露了执行器排队、拒绝计数、drain 限制、memory stats 等信息；bench 也支持 prefill、吞吐和延迟压测。

但如果要支撑真正的调优决策，仍然缺几类信息：

- queue wait time / execute time / reply encode time 的拆分
- cleanup/eviction 命中与耗时
- mixed read/write、TTL-heavy、eviction-heavy、large-value 等 workload
- 更稳定的“改动前/改动后”对照输出

对应路径：

- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`

## Design Principles

### 1) Measure Before And Between Changes

任何高风险优化之前，都先补基线与指标。否则后续即使性能变化，也很难区分是协议、allocator、reply encode 还是 TTL/淘汰路径导致。

### 2) Prefer Boundary Tightening Over Cross-Cutting Rewrite

优先把最贵的边界收窄：

- `String`/UTF-8 中转边界
- `byte[]` materialization 边界
- per-region allocator 边界

而不是一次性推倒 protocol、command processor、DB kernel。

### 3) Preserve Existing Module Responsibilities

当前仓库边界已经比较清楚：

- `yierdis-protocol` 负责协议模型与 codec
- `yierdis-server` 负责 Netty 组装与连接执行链
- `yierdis-core-command` 负责命令分发
- `yierdis-core-db` 负责存储引擎与数据结构
- `yierdis-memory-foreign` 负责 FFM/native memory

优化应尽量沿这些边界推进，而不是重新混淆职责。

### 4) Do Not Spend Complexity Budget On Multi-Executor Too Early

只要单线程模型还能通过入口减拷贝、内部边界收敛、allocator 池化获得明显收益，就不应该提前引入 shard/router/ownership 复杂度。

## Proposed Optimization Roadmap

## Phase 0: Establish A Tuning Baseline

### Objectives

- 让当前系统的性能和内存行为可重复比较
- 为后续 Phase 1-4 提供前后对照

### Work

- 扩展纯 Java bench，增加以下 workload：
  - mixed read/write
  - TTL-heavy
  - eviction-heavy
  - large-value
  - queue-pressure / busy-rejection
- 在 `STATS` 或 `INFO YIERDIS` 中补充更细的热点指标：
  - queue wait
  - execute duration
  - reply encode / flush accounting
  - cleanup runs / expired removed
  - eviction attempts / success / deadline hit
- 固定 bench 输出格式，使同一 workload 在不同分支上易于 diff

### Exit Criteria

- 能稳定输出至少一组 baseline 报告
- 每次后续优化都能用同一套 workload 和字段做前后对比

## Phase 1: Remove String-Centric Request Hot Path Costs

### Objectives

- 降低请求入口上的 heap 分配和 UTF-8 重编码成本
- 在不打破 `Custom Protocol v1` 对外行为的前提下，新增 bytes-first 执行路径

### Work

- 为协议层新增一种不依赖 `String/List<String>` 的内部请求表示
- 让 server 在 decode 后尽早进入 bytes-first `ExecutionRequest` 语义
- 保留现有 CLI/debug 友好的 `Custom Protocol v1` 表现，不做对外协议破坏性变更
- 评估并收敛 decoder 到 adapter 的 copy 次数

### Preferred Direction

- 不直接废弃 `Custom Protocol v1`
- 不直接做 protocol big-bang rewrite
- 优先做“internal request representation rewrite”

### Exit Criteria

- 请求主路径不再依赖 `String -> UTF-8 byte[]` 的重复中转
- 相同 workload 下，heap allocation rate 和/或吞吐表现有可测改善

## Phase 2: Push Off-Heap-Friendly Interfaces Into TTL / Eviction / Read Paths

### Objectives

- 避免 off-heap 数据在 TTL、淘汰、scan、集合读取等高频路径上反复回退到 heap

### Work

- 为 keyspace / expire index / candidate selection 补齐 `KeyHandle` 或 `BytesSlice` 风格接口
- 优先替换这些热点路径：
  - expiration sampling
  - maxmemory victim selection
  - slow key iteration paths (`KEYS` / `SCAN`)
  - 集合类结果输出的 materialization 边界
- 只在协议写回最终需要 `byte[]`/JSON 编码时再做必要转换

### Exit Criteria

- TTL cleanup 和 eviction 主路径不再依赖 `randomKey() -> byte[]` 风格回退
- 集合读取和 key iteration 的 heap materialization 次数减少

## Phase 3: Replace Per-Region Allocation With A Pooled Native Allocation Strategy

### Objectives

- 提升大量小对象、离散 blob、频繁更新场景下的 steady-state 效率
- 保持现有 leak detection 与 owner-thread discipline 的核心约束

### Work

- 设计 page/slab/size-class 风格的 native allocator
- 区分连续 buffer 场景与 blob/ref 场景
- 保留现有运行时 accounting、memory reporter 与 shutdown leak check 的可观测性
- 尽量保持上层 API 稳定，让 `YierdisDb` 和 command 层不需要知道 allocator 细节

### Exit Criteria

- 相比 per-region arena 模型，allocator 开销和 region 数量显著下降
- 无新增 leak / use-after-free / owner-thread 违规

## Phase 4: Re-evaluate Single-Thread Scaling Ceiling

### Objectives

- 用前 0-3 阶段的结果判断是否仍需打破单线程执行模型

### Work

- 在基线和优化后数据基础上，判断瓶颈是否仍主要集中在：
  - 单线程 execute
  - 单队列 drain
  - flush / reply encode
  - DB owner-thread
- 只有在证据明确时，才设计下一轮架构：
  - per-db executor
  - sharded executor
  - router-assisted ownership partition

### Explicit Deferral

- 本设计不包含多执行器实现方案
- 本设计只规定“何时值得开始设计它”

## Validation Strategy

### Bench Validation

每个 phase 完成后至少重新执行：

- `PING`
- random `SET`
- random `GET`
- mixed read/write
- TTL-heavy
- eviction-heavy
- large-value

输出应包含吞吐、延迟分位数，以及与该 phase 相关的附加指标。

### Correctness Validation

每个 phase 都必须保持：

- 已有命令行为不回退
- `Custom Protocol v1` 请求/回包兼容性不变
- `MEMORY STATS` / `INFO` / `STATS` 输出保持可解析
- shutdown 无新增 native memory leak

### Risk-Based Validation

重点覆盖以下风险：

- 请求 decode 后对象生命周期错误
- off-heap slice / key handle 跨线程使用
- allocator 池化后的 double-free 或 leak
- TTL / eviction 因接口收敛而出现“删不掉/删错 key”
- 新增观测字段本身影响热点路径

## Risks And Trade-Offs

### 1) Internal Bytes-First Path Will Increase Protocol-Layer Complexity

这是必要复杂度，因为当前最昂贵的请求链路正是从这里开始。但应把复杂度限制在协议内部表示和 server adapter 上，不向 command family 扩散。

### 2) Interface Tightening May Touch Many Call Sites

`byte[]` 边界在当前代码里分布较广。为控制风险，必须只优先改热点路径，而不是追求一次性“全仓库去 byte[]”。

### 3) Allocator Rewrite Has The Highest Regression Risk

相比 Phase 0-2，allocator 池化最容易引入 native memory 生命周期 bug。因此它必须排在 request hot path 和 interface tightening 之后。

### 4) Single-Thread Semantics Are A Product Choice, Not Just A Limitation

如果项目仍然把“Redis-like 单线程命令语义 + 较低实现复杂度”视为核心价值，那么多执行器即便能带来吞吐提升，也会引入调试、维护和语义复杂度成本。该权衡必须在前几阶段数据充足后再做。

## Recommended Execution Order

推荐的实际推进顺序如下：

1. 先补基线与观测
2. 再做 request hot path 去 `String` 化
3. 再做 TTL / eviction / read-path 的 off-heap 边界收敛
4. 再做 allocator 池化
5. 最后才评估是否需要多执行器

这是当前仓库最稳妥的路线，因为它：

- 与现有 README 和模块边界一致
- 不违背“不要重启一轮 protocol-layer rewrite”的当前约束
- 能在每个阶段结束时拿到明确的收益或结论
- 避免把两个最难归因的改动同时推进

## Acceptance Criteria For Planning

当满足以下条件时，可以进入 implementation planning：

- 路线图已明确 phase 边界、目标、退出条件与风险
- 已明确“现在先不做”的内容
- 已明确第一轮实施优先级是 `Phase 0 + Phase 1`
- 团队接受“先测量、再去 String 化、再收敛 off-heap 边界、最后才碰 allocator 和多执行器”的节奏
