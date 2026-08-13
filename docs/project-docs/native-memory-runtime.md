# Native Memory 运行时

本文解释 Yierdis 如何把 JDK 25 FFM 接入 DB：backend、runtime、region、stable handle、maxmemory 和必须 materialize 到 heap 的边界。

## 当前结构

生产路径只公开 `YierdisFfmStableMemoryBackend`。内部 FFM 层由以下对象组成：

- `YierdisFfmMemoryRuntime`：region accounting 和 leak detection；
- `YierdisFfmRegion`：拥有一个 shared arena 及其 `MemorySegment`；
- `YierdisNativePageAllocator`：管理 small pages 与 medium/large spans；
- `YierdisNativeObjectTable`：把 stable handle 解析为当前物理 location；
- `YierdisFfmStableMemoryBackend`：组合 allocation、resolve、pin、epoch、realloc、defrag 和 close。

DB graph 保存 `NativeHandle` 或 typed wrapper，不保存 `MemorySegment`、physical address 或 packed location。region/block access 都是 backend 内部实现细节。

`YierdisFfmMemoryRuntime.allocateRegion(...)` 使用 `Arena.ofShared()`。region 可能在 bootstrap 阶段分配，再由 DB owner thread 释放，因此 arena 必须允许跨线程 close；这不表示 DB storage graph 可以跨线程并发访问。

## 启动和所有权

`YierdisServerBootstrap` 通过 `StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new` 创建 `YierdisDbEngineFactory`。每次 `DbEngineFactory.create(...)` 都为对应 DB 创建一个独立 backend；backend 构造自己的 `YierdisFfmMemoryRuntime`，并在 close 时负责关闭它。

```text
YierdisServerBootstrap
  -> YierdisDbEngineFactory
     -> StableMemoryBackendFactory.create("db-N", ...)
        -> YierdisFfmStableMemoryBackend
           -> YierdisFfmMemoryRuntime
```

global/per-db maxmemory scope 只改变预算协调方式，不改变 FFM 所有权：

- per-db scope 由当前 DB 的 ledger 和 physical snapshot 做 admission、cleanup、trim 和 eviction；
- global scope 由 instance governor 汇总多个 DB participant 的独占 snapshots；
- global scope 仍保留 per-DB backend/runtime ownership，也不把 runtime counter 叠加进 participant snapshots。

`YierdisDbEngineFactory` 创建 backend 后把所有权交给 `YierdisDb`。storage 组合开始后，backend 所有权立即转入 `YierdisDbKeyLifecycle`；组合失败和正常 shutdown 都由 lifecycle 在 storage graph 清理后关闭 backend，并把后续清理失败保留为 suppressed exception。

## Runtime 与 region

`YierdisFfmMemoryRuntime` 维护 name、`usedBytes`、`liveRegionCount` 和 closed 状态。`allocateRegion(owner, bytes)` 会校验输入和 runtime 状态，创建 shared arena，分配 segment，再递增两个 counter。native allocation 失败会映射成 `NativeCapacityExceededException`。

`YierdisFfmRegion` 保存 runtime、arena、segment 和 size。byte/int/long/bulk access 先检查 region lifecycle 和完整范围，再通过 `ValueLayout` 或 `MemorySegment.copy(...)` 执行。region close 会关闭 arena，并回报 runtime 扣减 accounting。

runtime close 不替调用方释放仍然存活的 regions；它先标记 closed，再检查 counter。残留 region 会报告 lifecycle leak。backend close 会 best-effort 清理 page/object-table/runtime，并聚合 lifecycle failure。

## Stable handle 与物理块

stable identity 来自 object table，而不是 FFM address：

```text
DB graph
  stores stable NativeHandle values

YierdisFfmStableMemoryBackend
  resolves handle through YierdisNativeObjectTable

YierdisNativePageAllocator
  owns FFM blocks allocated from runtime regions
```

因此 realloc 和 active defrag 可以分配新 block、复制内容并发布新 location，同时保持 handle 不变。复制或 validator 在 publication 前失败时，新 block 被关闭，旧 location 继续有效。publication 后，旧 block 按 epoch 状态立即释放或进入 retired list。

`NativeObjectView` 提供 byte/bulk/copy/comparison/typed access。FFM view 在委托接口默认实现前仍会完整检查 lifecycle、writability 和范围，所以无效的 multi-byte/copy 写入不会留下部分修改，read-only/closed 异常优先级也保持稳定。block-to-block realloc/defrag 使用一次直接 native copy。

pin、view、epoch、quarantine 和 allocation scope 的职责不同：

- pin/view 防止仍被观察的 object 过早释放；
- epoch 延迟已迁移旧 block 的物理回收；
- quarantine 保存已逻辑 free 但仍 pinned 的 object；
- allocation scope 跟踪 prepare 阶段的新 handles，commit 时 promote，abort 时反向释放。

## DB storage graph

FFM-backed storage 主要包括：

- `NativeKeyDirectory`：key 到 `EntryHandle`；key bytes 是 `KEY_BYTES` object；
- `EntryTable`：`EntryHandle` 到固定布局的 `ENTRY_RECORD`；
- `EntryRecord.expireAtMillis`：唯一 TTL deadline；
- string、list、hash、set、zset 的 root、node 和 payload objects；
- object table 与 page allocator 的 native metadata。

`EntryHandle`、`ValueHandle` 和 `KeyHandle` 是 stable-handle wrapper，不是 physical address，也不能被当作长期有效的 segment view。`YierdisDbKeyLifecycle` 统一发布、替换和释放 directory entry、entry record、value root 与 derived accounting。

## Maxmemory 与 memory stats

写路径由 `YierdisDbMutationExecutor` reserve upper bound，并用 allocation scope 实测 prepare peak。commit 后依次 promote、settle logical ledger、publish commit stream、release superseded resources，再按提示尝试 trim。

enforcement snapshot 固定为：

```text
heap estimated
  + native metadata committed
  + native data committed
```

runtime `usedBytes`/`liveRegionCount` 用于 FFM lifecycle 诊断，不是额外 maxmemory 账本。`nativeReclaimableBytes` 也只是候选量，只有 trim 返回实际 reclaimed bytes 并重新采样后，才能影响 admission 判断。

## Heap materialization 边界

native memory 不等于所有路径零复制。当前仍会 materialize 到 heap 的常见边界包括：

- RESP decode 把 argv materialize 成 heap `byte[]`；
- key lookup 把 `BytesView` 转为 owned key bytes；
- snapshot、`RANDOMKEY`、introspection 和显式返回 `byte[]` / `List<byte[]>` 的 API；
- 排序、聚合或协议组装需要脱离 native view 生命周期时复制。

`SCAN`、命令 `GET`/`HGET`、pop 和 collection streaming 路径会持有 pin/epoch/handle，
通过 native-backed `BytesSlice` 或等价的 retained view 有界写出；它们不会先 materialize
整批 payload。调用方不能让 callback-scoped view 逃逸。详细边界见
[`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 和 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)。

## Operations Cross-Check

native committed/reserved usage 参与 DB 和 maxmemory 诊断，但不会替代 ingress、commit stream 或 outbound reply 的独立容量限制。native allocation failure 仍按 mutation 的 commit 前/后边界决定返回 OOM 或 result-unknown；操作流程见 [`production-hardening-operations.md`](./production-hardening-operations.md)。
