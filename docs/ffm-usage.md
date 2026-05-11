# FFM Usage in Yierdis

本文整理 Yierdis 当前是如何使用 JDK 25 `java.lang.foreign` FFM API 的。

如果只关心“哪些路径会发生 heap / off-heap 拷贝”，请优先看 `docs/offheap-copy-behavior.md`。本文关注的是更上层的问题：FFM 在项目里扮演什么角色、从启动到 DB 内部是怎么接起来的、哪些数据真的放进了 native memory、以及生命周期和泄漏检查是如何工作的。

## 先说结论

Yierdis 里的 FFM 主要被当作统一的 native-memory substrate 来用，而不是用来调用 native function。

- 代码里实际使用的是 `Arena`、`MemorySegment`、`ValueLayout`
- 没有看到 `Linker`、`SymbolLookup`、`downcallHandle` 这类 native function 调用链
- FFM 在这里的核心职责是承载 off-heap bytes、off-heap table metadata、以及对这些内存块的生命周期管理

代表路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmAccess.java`

## 启动和组装

server 启动时，会先检查当前 JVM 是否支持 `java.lang.foreign`。如果不支持，直接报错并要求使用 JDK 25。

代表路径：

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ForeignMemoryAutoModules.java`

真正的组装链路是：

1. `YierdisServerBootstrap` 创建 `YierdisInstance`
2. `YierdisInstance` 会创建实例级的 FFM runtime 装配上下文
3. 默认的 `YierdisDbEngineFactory` 会按 maxmemory scope 决定 runtime 归属：
   - `GLOBAL` 模式下复用同一个 shared runtime
   - `PER_DB` 模式下为每个 `YierdisDb` 创建独立 runtime，避免跨 DB 的 off-heap 记账串扰
4. `YierdisDb` 再把所属 runtime 组装成字符串路径使用的 allocator，以及 keyspace / expires / 复合结构使用的 FFM 存储对象

代表路径：

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`

## FFM 基础层：Runtime / Region / Span / Access

Yierdis 对 FFM 做了一层很薄的封装，核心对象有四个：

- `YierdisFfmMemoryRuntime`
- `YierdisFfmRegion`
- `YierdisFfmSpan`
- `YierdisFfmAccess`

### `YierdisFfmMemoryRuntime`

`YierdisFfmMemoryRuntime.allocateRegion(owner, bytes)` 每次分配都会：

1. 创建一个 `Arena.ofConfined()`
2. 从这个 arena 中 `allocate(bytes)` 得到 `MemorySegment`
3. 用 `YierdisFfmRegion` 把 arena + segment 包起来
4. 把 region 记入 `liveRegions`
5. 把 bytes 累加到 `usedBytes`

这意味着它不是“整个实例只有一个大 arena”，而是“每个 region 自己拥有一个 confined arena”。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`

### `YierdisFfmRegion`

`YierdisFfmRegion` 是一个带 owner 名称的 native memory block，内部持有：

- `Arena`
- `MemorySegment`
- `size`
- `runtime`

`span(offset, length)` 会返回一个切片后的 `YierdisFfmSpan`。`close()` 会直接关闭底层 arena，然后通知 runtime 做 accounting 回收。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`

### `YierdisFfmSpan`

`YierdisFfmSpan` 只是 `MemorySegment` 的轻量 view，负责表示某个 region 的一个切片，不单独拥有内存。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSpan.java`

### `YierdisFfmAccess`

`YierdisFfmAccess` 把所有基础读写都收口到了一个地方，避免业务层直接碰 `MemorySegment`：

- `getByte` / `setByte`
- `getInt` / `setInt`
- `getLong` / `setLong`
- `getBytes` / `setBytes`
- `asByteBuffer`

这里实际使用的是 `ValueLayout.JAVA_BYTE`、`JAVA_INT_UNALIGNED`、`JAVA_LONG_UNALIGNED`。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmAccess.java`

## 两条上层接入路径

`YierdisDb` 拿到 `YierdisFfmMemoryRuntime` 后，实际上分出两条不同的使用路径。

### 路径一：`OffHeapAllocator` / `OffHeapBuf`

这条路径主要服务于 string 和 HLL 这种“连续字节缓冲”。

`YierdisForeignOffHeapAllocator.allocate(capacity)` 会：

1. 通过 runtime 申请一个 region
2. 把它包装成 `OffHeapBuf`
3. 用 allocator 自己的 `usedBytes` 做额外 accounting

`OffHeapBuf.close()` 最终会：

1. 关闭底层 region
2. 通知 allocator 扣减字节数

它还支持 `slice(index, len)` 返回 `OffHeapSlice`，让读取路径可以直接把 off-heap 内容暴露给上游，而不必先 materialize 成 `byte[]`。

代表路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java`

### 路径二：`YierdisFfmBlobStore` / `YierdisFfmBytesRef`

这条路径主要服务于 keyspace、expires、以及复合结构里的 field/member 等“离散字节块”。

`YierdisFfmBlobStore.store(byte[])` 会：

1. 分配一个 region
2. 把 bytes 拷进去
3. 返回一个 `YierdisFfmBytesRef(region, offset, length)`
4. 在 `refCounts` 里建立引用计数

之后同一块 blob 可以通过 `retain(ref)` / `release(ref)` 共享和释放。最后一次 release 才真正关闭底层 region。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmBlobStore.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmBytesRef.java`

## 字符串路径：FFM 如何进入 `SET` / `GET`

字符串值不走 `YierdisFfmBlobStore`，而是由 `StringRoot` 管理。
`StringRoot` 内部使用 `OffHeapAllocator` 分配连续 buffer，并用
`ValueHandle` 暴露给 entry 元数据和兼容对象。

### 写入

`YierdisStringOps.set(...)` 最终会调用：

- `YierdisObject.newString(stringRoot, value)`

这个对象是兼容 adapter：真正的字符串 bytes 落在 `StringRoot` 管理的
`OffHeapBuf` 里，adapter 只保存当前 `ValueHandle`。写入完成后，
`YierdisDbKeyLifecycle` 会把 key 同步到 `NativeKeyDirectory` 和
`EntryTable`，`EntryRecord` 里保存 type、encoding、value handle、TTL
和估算字节数。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`

### 覆盖和扩容

`YierdisObject.overwriteWithString(StringRoot, ...)` 会转发到
`StringRoot.overwrite(...)`。这个路径保留了关键优化：

- 如果旧 handle 指向的 `OffHeapBuf` 容量足够容纳新值

那么它会直接复用原 buffer，就地改写内容，而不是“先分配新 buffer，再释放旧
buffer”。

这么做的目的很明确：在 `maxmemory` 有硬预算时，避免 SET 覆盖路径临时同时持有 old + new 两份 off-heap 内存。

如果容量不够，则会重新分配新 buffer，并把旧内容做 off-heap -> off-heap 复制。
`EntryRecord` 的 estimate 会随后刷新，delete、expire 和 eviction 路径不再依赖
旧 adapter 上的估算值作为唯一来源。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`

### 读取

`GET` 的读路径会先解析 live entry，再通过当前 value handle 访问
`StringRoot`：

- 可流式输出时返回 `BulkStringValue.slice(slice)`，底层直接指向 `OffHeapSlice`
- 需要 materialize 时才复制成 heap `byte[]`

这就是字符串路径里真正的零拷贝读优化。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`

### HLL

HLL 并没有单独设计一套 native payload 类型，而是复用了 string root 的
off-heap 存储路径。也就是说：

- HLL 逻辑上是一个特殊的 string
- HLL bytes 也可以存在 `StringRoot` 管理的 `OffHeapBuf` 里

但要注意，`PFCOUNT` / `PFMERGE` 这类计算路径目前仍可能调用 `stringBytesView()` 把内容 materialize 成 `byte[]` 后再做计算，所以它不是完整的零拷贝方案。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java`

## Keyspace 路径：key 如何放进 FFM

keyspace 是 FFM 使用最核心的部分之一。

当前迁移期同时存在两层 key 结构：

- `YierdisFfmKeyspace<YierdisObject>`：保留给兼容 adapter 和旧 scan/helper 路径
- `NativeKeyDirectory`：保存 native entry graph 的 key -> `EntryHandle`

`YierdisFfmKeyspace.computeWithHandle(...)` 在 key 首次出现时会：

1. 通过 `blobStore.store(key)` 把 key bytes 存进 native memory
2. 基于这个 blob 创建 `KeyHandle.forFfm(ref, hash)`
3. 把 handle 传给上层 mutation 逻辑
4. 把 `ref` 放进兼容 keyspace table

mutation 完成后，`YierdisDbKeyLifecycle.syncEntry(...)` 会把同一个逻辑 key
同步到 native entry graph：

1. `NativeKeyDirectory` 存储 key bytes
2. `EntryTable` 分配或替换 `EntryRecord`
3. `EntryRecord.valueHandle()` 指向对应 `TypeRoot` 里的 payload

之后 DB 内部很多路径都围绕 `KeyHandle`、`EntryHandle` 和 `ValueHandle`
传递 identity，而不是不断回到新的 heap `byte[]` 或把 `YierdisObject` 当主存储。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmKeyspace.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java`

## Expire 路径：TTL 如何复用同一份 off-heap key

`YierdisFfmExpireIndex` 并不会为了 TTL 再复制一份 key bytes。

当调用 `setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis)` 时，它会：

1. 从 handle 中取出底层的 `YierdisFfmBytesRef`
2. 对这块 ref 执行 `blobStore.retain(ref)`
3. 把同一个 ref 放进 expires table

也就是说，keyspace 和 expires 共享同一份 off-heap key bytes，只是通过引用计数协调生命周期。

设置或移除 TTL 时，`YierdisDbKeyLifecycle` 还会同步更新 `EntryRecord.expireAtMillis`，
让过期、introspection 和 memory 路径都能从 entry metadata 看到同一份状态。

这个行为有专门测试覆盖。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireKeySharingTest.java`

## 复合结构：Hash / List / Set / ZSet

在复合结构里，命令 hot path 现在先定位 `EntryRecord.valueHandle()`，再进入
对应 root：

- `HashRoot`
- `ListRoot`
- `SetRoot`
- `ZSetRoot`

这些 root 仍复用 `HashValue`、`ListValue`、`SetValue`、`ZSetValue` 作为内部
adapter，但 key 的 canonical metadata 在 `EntryTable`，value identity 是
`ValueHandle`。总体思路是一致的：把成员 bytes 尽量放到 off-heap，把部分索引元数据也放到 off-heap，并在流式读路径里优先暴露 `OffHeapSlice` 风格接口。

### Hash

`HashRoot` 通过 `ValueHandle` 管理内部 `HashValue` adapter，adapter 用：

- `YierdisFfmBlobStore`
- `YierdisFfmListpack`
- `YierdisFfmByteMap<YierdisFfmBytesRef>`

来保存 field/value。

`HGETALL` 的流式写回路径可以直接输出 `YierdisFfmBytesRefSlice`，无需先拼成 `List<byte[]>`。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`

### List

`ListRoot` 通过 `ValueHandle` 管理内部 `ListValue` adapter，adapter 使用：

- `YierdisFfmListpack`
- FFM 版 quicklist-like 节点结构

来保存 list element bytes。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmListpack.java`

### Set

`SetRoot` 通过 `ValueHandle` 管理内部 `SetValue` adapter，adapter 有两条 FFM
路径：

- 小整数集合时走 `YierdisFfmIntSet`
- 非整数集合时走 `YierdisFfmByteMap<Object>`

其中 `YierdisFfmIntSet` 是更“纯”的 native array 风格实现；`YierdisFfmByteMap` 则是 Java table + off-heap member bytes 的混合实现。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmIntSet.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmByteMap.java`

### ZSet

`ZSetRoot` 通过 `ValueHandle` 管理内部 `ZSetValue` adapter，adapter 内部使用
`YierdisFfmZSet`。

在 `ZRANGE` / `ZRANGEBYSCORE` 这种输出路径里，member 可以直接作为 `YierdisFfmBytesRefSlice` 发送给 `BulkStringSink`，因此它也支持 off-heap 流式读取。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmZSet.java`

## “FFM-backed” 不等于“所有东西都在 native memory”

这点很重要。

项目里很多结构可以称为 “FFM-backed”，但这不等于它们的所有内部数组都已经搬进 native memory。

例如：

- `EntryTable` 的 entry slots 来自 slab allocator，但 `EntryHandle` 本身是 Java record
- `NativeKeyDirectory` 的 key bytes 在 native blob store，table 数组仍在 heap
- `YierdisFfmKeyspace` 的 `states` / `hashes` 在 native memory，但 `refs[]` / `values[]` 仍是 Java 数组
- `YierdisFfmExpireIndex` 的 `states` / `hashes` / `expireAt` 在 native memory，但 `refs[]` 仍在 heap
- `StringRoot` 管理 off-heap buffer，但 handle -> slot map 仍在 heap
- `HashRoot` / `ListRoot` / `SetRoot` / `ZSetRoot` 的 handle table 仍在 heap，payload 通过各 value adapter 进入 FFM-backed 结构
- `YierdisFfmByteMap` 的 table 索引数组本身仍在 heap，只是 key bytes 放在 off-heap
- `YierdisFfmListpack` 本身是 `ArrayList<YierdisFfmBytesRef>`，真正 off-heap 的是 entry bytes

所以更准确的描述应该是：

- 关键字节数据大量 off-heap 化
- 部分索引元数据 off-heap 化
- key、entry metadata 和 value payload 都通过 64-bit handle 串起来
- 但并不是“整个 DB 内部结构完全 native 化”

## 为什么这里可以放心使用 `Arena.ofConfined()`

这个设计依赖项目的单线程 DB 语义。

`DbThreadGuard` 强制每个 `YierdisDb` 必须显式绑定到唯一 owner thread。未绑定访问或跨线程访问都会直接失败。

server 启动时，`CommandExecutor` 会先调用 `runtimeAccess::bindToCurrentThread`，把 DB 绑定到命令执行线程。后台 maintenance 虽然是由 worker event loop 定时触发，但真正的 cleanup / maxmemory enforcement 会通过 `executeMaintenance(...)` 回到同一个 command executor 线程执行。

这使得项目可以把大部分 FFM 内存都建立在 `Arena.ofConfined()` 上，而不必为了并发共享去设计更复杂的 arena 同步策略。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/DbThreadGuard.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

## 内存统计、maxmemory 和泄漏检查

FFM 内存不是一个“统计旁路”，而是明确进入内存治理体系的一部分。

### maxmemory

`YierdisDbMemoryReporter.usedBytesForMaxmemory()` 会把：

- `ledger.usedBytes()`
- `memoryRuntime.usedBytes()`
- TTL 估算开销

综合起来，作为 DB 侧 maxmemory 判断依据。

这意味着：

- off-heap bytes 会影响 maxmemory
- keyspace / expires / entry table / native key directory / type root 的 native bytes 不会被忽略
- delete、expire 和 eviction 释放记账优先读 `EntryRecord`，避免依赖兼容 object 的旧估算值

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`

### 关闭和泄漏检测

`YierdisDbOwnedResources.releaseAll(...)` 的关闭顺序是：

1. 遍历兼容 store，释放 adapter 持有的 root payload
2. 清空 expires
3. 清空并关闭 `EntryTable`
4. 清空并关闭 `NativeKeyDirectory`
5. 关闭各 `TypeRoot`
6. 关闭 allocator
7. 关闭 runtime

如果 runtime 关闭时仍然存在 live region，会直接抛出 leak 错误。

这让 “native memory 没回收” 在测试和关闭路径上都能尽早暴露。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`

## 相关测试可以说明什么

下面这些测试基本能覆盖本文最重要的判断：

- `OffHeapKeysToggleTest`
  说明默认 DB 已经把 key 存到 off-heap
- `OffHeapStringStorageTest`
  说明字符串写入后确实占用 runtime native bytes，`GET` 也可以走 off-heap slice 读路径
- `UnsafeOffHeapKeyspaceTest`
  说明 TTL 清理和 shutdown 后 native bytes 能回到 0
- `UnsafeOffHeapDbSmokeTest`
  说明 string/list/hash/set/zset/HLL 等常用类型都能在共享 FFM runtime 下工作
- `ExpireKeySharingTest`
  说明 expires 和 keyspace 共享同一份 off-heap key ref
- `OffHeapCollectionReadStreamingTest`
  说明部分集合读路径可以直接流式输出 off-heap slice
- `NativeStorageRegressionTest`
  说明 string/list/hash/set/zset/HLL 删除后 native accounting 可以回到 0，且删除记账不依赖兼容 object 的旧估算值
- `MemoryStatsAccountingConsistencyTest`
  说明 memory reporter 和 maxmemory 统计保持一致

代表路径：

- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapKeysToggleTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapKeyspaceTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapDbSmokeTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireKeySharingTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`

## 最后再压缩成一句话

Yierdis 当前对 FFM 的使用方式可以概括为：

- 用 `YierdisFfmMemoryRuntime` 统一承载实例级 native memory
- 用 slab allocator、`EntryTable` 和 64-bit handle 承载 entry metadata
- 用 `NativeKeyDirectory` 把 key bytes 映射到 entry handle
- 用 `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 承载各类型 payload
- 用兼容 `YierdisObject` 桥接尚未移除的旧 helper 路径
- 用 `BlobStore + BytesRef + KeyHandle` 路径承载兼容 keyspace、TTL 索引和复合结构成员 bytes
- 用 `OffHeapSlice` / `YierdisFfmBytesRefSlice` 给读路径提供尽量少拷贝的输出接口
- 用单线程 owner model 约束 `Arena.ofConfined()` 的访问纪律
- 用 runtime accounting、memory reporter 和 shutdown leak check 把 FFM 内存纳入 maxmemory 与资源回收体系

这也是 README 里“项目现在统一使用 JDK 25 FFM API 管理 native memory”那句话在实现层面的具体含义。
