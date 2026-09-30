# Off-Heap Copy 边界

本文按边界梳理 heap、`ByteBuf` 与 FFM native memory 之间哪里发生 copy，哪里只是 view 或 handle。结论并非“用了 off-heap 就零拷贝”——当前写回路径通过有界复制避免完整结果 materialization，并不提供生产零拷贝能力。

Yierdis 的 native memory 主要降低稳态 heap 占用、改善 GC 压力并让部分 read/write-back 可以流式处理；只要接口边界要求 `byte[]`、`List<byte[]>`、`String`、snapshot 或长期 ownership，copy 仍然会发生。

相关背景见 [`bytes-and-fast-paths.md`](./bytes-and-fast-paths.md)（字节抽象与快速路径）与 [`native-memory-runtime.md`](./native-memory-runtime.md)（native 生命周期与选型推导）。本文既分析全链路各阶段**在哪里发生 copy / 哪里是 view**，也深入分析**这些 copy 的实际性能成本与真正的内核态切换边界**。

## 判定规则：什么时候复制，什么时候不复制

判断一次操作是否 copy，不看“有没有 off-heap”，而看两件事：

1. **两端的表示形态**：源和目标是不是同一种存储（heap array 对 off-heap object、off-heap 对 off-heap）。
2. **接口要求的 lifetime**：接收方要的是短生命周期 view、还是一份可以独立存活的 snapshot/`byte[]`。

只要跨存储形态传输字节，就一定是 copy；只要接收方要求“生命周期超过当前调用栈”，也一定是 copy。反过来说，只有当两端形态一致、且接收方接受一个受调用方 pin 保护的临时 view 时，才可能不复制。

| 场景 | 是否 copy | 判定依据（真实入口） |
| --- | --- | --- |
| heap argv → native object（新 key / 新 string / 新 collection member） | 是 | `NativeKeyDirectory` 写 `KEY_BYTES`、`StringRoot.store(...)`；源是 heap，目标是 native |
| `BytesSlice`（含 native slice）→ native object | 是 | `StringRoot.setBytes(...)` 经 8 KiB heap scratch 分块后 `view.setBytes(...)`；不是 address-to-address |
| native object → 返回 `byte[]` 的 API | 是 | `NativeByteStore.toByteArray(...)`、`StringRoot.copy(...)`、`YierdisDb.toByteArray(BytesView)` |
| native object → 聚合 source 快照（`copiedFrom`） | 是 | `CapturedByteItems.CapturingSink.value(BytesSlice)` 主动 `slice.getBytes(...)` 到 `byte[]` |
| native object → 长期 pin 的 `ByteValue`/slice（`GET`） | 否（仅 pin，不搬字节） | `StringRoot.retainedValueWithCloseHook(...)` 只 `pin` + `NativeBytesSlice.retained(...)` |
| native slice → 有界流式 reply | 是，但分块 | `NativeBytesSlice.writeTo(...)` 经 8 KiB scratch → sink chunk；不整体 materialize |
| off-heap 扩容（容量不足，`reallocate` 搬家） | 是 | `YierdisFfmStableMemoryBackend.reallocateLocal(...)` 里 `previous.copyTo(next, oldSize)` |
| active defrag 迁移对象 | 是 | `moveLiveObject(...)` 里 `previous.copyTo(target, size)` |
| `NativeHandle` 传递 / `NativeObjectView` 打开 | 否 | handle 是稳定身份，view 是带 pin 的解析结果，都不搬内容 |
| native intset 成员 / 整数 score 输出 | 否（按 `longAscii` 渲染） | `SetValue.membersInto` 走 `out.longAscii(...)`，不经 `byte[]` |

下面按边界逐条展开。

## Heap -> off-heap

常见写入链路从 heap 开始。

RESP decode 后，`ByteArrayExecutionRequest` 保存稳定的 heap `byte[][]` argv（`copyOf(...)` 对每个元素 `clone()`），并持有脱离 Netty 对象的 `RequestMemoryLease`。这是协议帧跨过 Netty decoder lifecycle 和 executor queue 的 ownership 边界。

DB lookup 当前也会 materialize heap key。DB read/write API 虽然接受 `BytesView`，但 `YierdisDbKeyLifecycle` 进入 `NativeKeyDirectory` 前会调 `YierdisDb.toByteArray(BytesView)` 把 key view 转成 heap `byte[]`，因为后者的 lookup/compute API 当前仍是 byte-array based。`toByteArray` 的注释已标明这是已知热点：`BytesView lookup 每次分配一个 heap byte[]`。

新 key 持久化时，`NativeKeyDirectory` 会分配 allocator-backed `KEY_BYTES` object，并把 heap key bytes 写入 native object。这里是 heap -> off-heap copy。

string 写入时，`StringRoot` 会把输入 `BytesSlice` / `byte[]` 内容写入 allocator-backed `STRING_BYTES` object：

- `store(byte[])`：`allocate(STRING_BYTES, len)` 后直接 `view.setBytes(0, value, 0, len)`。
- `store(BytesSlice)` / `overwrite(..., BytesSlice)` / `append(..., BytesSlice)`：走 `StringRoot.setBytes(view, index, value, len)`，用 `COPY_BUFFER_BYTES = 8 * 1024` 的 ThreadLocal scratch 分块 `value.getBytes(...)` → `view.setBytes(...)`。

因此不要把 `BytesSlice` 自动理解成 address-to-address copy：slice 到 native 之间当前经由 heap scratch 中转。

collection 写入也类似：root record 和 payload internals 都是 allocator-backed native objects（`NativeObjectKind` 覆盖 `LIST_ROOT`/`HASH_ROOT`/`SET_ROOT`/`ZSET_ROOT`、`LIST_NODE`、以及 `HASH_FIELD_BYTES`/`SET_MEMBER_BYTES`/`ZSET_MEMBER_BYTES`/`SCORE_BYTES` 等）。输入仍通常来自 heap argv，因此写入会把 field、member、score 或 list entry bytes 复制到 type-specific native handles。

## Off-heap -> heap

只要 API 语义要求 heap shape，就必须复制回 heap。

常见场景：

- 返回 `byte[]` 的 storage API（`NativeByteStore.toByteArray(...)`、`StringRoot.copy(...)`）。
- `YierdisDb.toByteArray(BytesView)`：key lookup 边界每次分配一个 heap `byte[]`。
- `RANDOMKEY`、snapshot、introspection 和测试断言。
- `MEMORY` / `OBJECT` 类命令需要构造诊断输出。
- 返回 `List<byte[]>` 的 collection read API，例如非流式聚合结果。
- `ByteSequenceSources` / `ByteMapSources.copiedFrom(...)`：见上文判定表，它在 prepare 阶段就把 live emit 结果拷成独立 heap 快照。

当前 string `GET` 路径通过 `StringRoot.retainedValue`（内部 `retainedValueWithCloseHook`）得到已 pin 的 `NativeBytesSlice`，再包装成 `RedisReply`，由中央 renderer 写入协议端口。这里建立的是 pin，不搬字节：`retainedValue` 先 `allocator.pin(nativeHandle)`，再用 `ByteValue.owned(NativeBytesSlice.retained(...), ..., () -> allocator.unpin(nativeHandle))` 转移一次 pin 的所有权。pin 保持到 `CommandExecutorExecutionSupport` 同步渲染后的 finally 分支 `closePrepared(task)`，`writeTo` 只使用这份已有 pin。调用方不得把这个 slice 留到 prepared command 关闭之后。

命令 `GET`、`HGET`、pop 和 `SET ... GET` 使用 retained native-backed view/slice；`SCAN` 则保留 cursor、目录元数据和 epoch，在输出阶段重放目录并生成 native-backed key slice。这些路径都不要求先把完整结果 materialize 到 heap。

keyspace 也一样：key bytes 持久化为 `KEY_BYTES` native object，但只要外部接口要 `byte[]`，就会通过 allocator resolve view 读取并复制出来。

## Off-heap -> bounded streaming output

有界流式写出能避免完整结果 heap materialization，前提是上下游接口都能表达“我可以按 slice 或 sink 工作”。

典型形状：

```text
NativeBytesSlice
  -> writeTo(BytesSink)
  -> reusable bounded heap scratch
  -> ReplyReservationSink / BoundedChunkedReplySink
  -> bounded ByteBuf chunks
```

`RespReplyWriter.bulkString(BytesSlice)` 会先写 RESP header（`$<len>\r\n`），再让 slice 同步写入 sink，最后写 CRLF。`NativeBytesSlice` 当前使用可复用的 8 KiB heap scratch（`COPY_CHUNK_BYTES = 8 * 1024`）分块读取 native bytes；`BoundedChunkedReplySink` 在 allocator 调用前把预留额度转换为 allocated credit，再写入固定上限的 `ByteBuf` chunk，每个 chunk 额外记 `CHUNK_COMPONENT_OVERHEAD_BYTES = 1_024` 的组件开销。

`LRANGE`、`HGETALL`、`SMEMBERS`、`ZRANGE*` 不走这条 live native 写出路径。这类命令在 prepare 时用 `copiedFrom` 把选中元素拷进独立 source，renderer 再把快照写入 `ByteValueSink`：`ListValue.rangeInto` / `HashValue.hgetallPairsInto` / `SetValue.membersInto` / `ZSetValue.zrangeByScoreWriteTo` 先 emit 非 retained 的 `store.slice(ref)`，`CapturedByteItems.CapturingSink` 立即 `slice.getBytes(0, copy, 0, length)` 成 heap `byte[]`。原因是 prepare 持有 source 期间 storage 仍可能 in-place 变更，count/length/emit 必须来自同一份快照，而不能再次读 live handle。

这条路径避免完整 heap 结果，但当前仍执行有界复制：

- source slice 可能来自 request heap bytes，或来自已经 pin 到 `closePrepared` 的 native handle view。`GET` 的 `writeTo` 使用这份已有 pin。
- native slice 先复制到有界 heap scratch，sink 再把数组范围同步复制到 reply chunk。
- 某些格式转换、排序、聚合、escape 或 base64 边界仍需要中间 buffer。

reply reservation、chunk ownership 和顺序写回见 [`request-execution-flow.md`](./request-execution-flow.md#netty-pipeline-装配顺序与各-handler-职责)。

## 同侧复制也存在

copy 不只发生在 heap/off-heap 之间，同一侧也存在。

heap -> heap：

- 协议适配、transaction replay、测试 recording sink、字符串编码转换都可能创建新的 heap array。
- `CapturedByteItems` 的 `copiedFrom` 快照。
- `StringRoot.slice(...)` 返回的 `HeapBackedBytesSlice` 会对内容 `Arrays.copyOf`。
- `ByteArrayExecutionRequest` 的 `clone()`。

off-heap -> off-heap：

- `StringRoot` append/growth 调用 allocator `reallocate(..., NativeReallocPolicy.PRESERVE_PREFIX)` 时，`reallocateLocal` 先判断 `newSize <= meta.capacity()`：容量够则原地更新 location，不搬字节；容量不足才 `pageAllocator.allocate(...)` 新块并 `previous.copyTo(next, oldSize)`（保留旧 prefix）。注意 `reallocateLocal` 要求 `meta.pinCount() == 0`，否则抛 `"native object is pinned"`。
- active defrag 移动 `KEY_BYTES`、`ENTRY_RECORD`、`STRING_BYTES`、collection root records、`LIST_NODE` metadata record 或 collection internal byte handles 时，`moveLiveObject(...)` 会 `previous.copyTo(target, sourceMeta.size())`，再通过 object table `publishMoved(...)` 发布新 location。defrag 同样跳过 `pinCount() > 0` 的对象。
- `YierdisNativeBlock.copyTo(...)` → `YierdisFfmRegion.copyTo(...)` → `MemorySegment.copy(...)` 是这些搬家的底层实现。
- `NativeObjectView.copyBytes(...)` 默认实现逐字节 `getByte`/`setByte`，用于对象内偏移搬移。
- `ByteBuf` 写回也可能发生 buffer copy，而不是 view 共享。

view / handle 不是 copy：

- FFM 的 `MemorySegment` 子视图（如 `asSlice(...)`）是子视图，不复制底层 memory；Yierdis 当前只对整个 region segment 做逻辑切片，不额外暴露 span wrapper，也不调用 `asSlice`/`reinterpret`。切片边界由 `NativeBytesSlice` 的 `offset`/`length` 表达。
- `NativeHandle` 是 stable identity（`allocatorId` + `localRaw`），不复制对象内容。
- `NativeObjectView` 是 resolved bounded view；打开 view 会 pin（`resolve` 自己持一次 retain，`resolvePinned` 借用调用方已有的 pin），对象内容没有因为 resolve 自动复制。

## ownership 与 lifetime：判定规则

判断“这个 slice/view 能不能留、留多久”，看它属于哪一类：

- **`BytesView`**：请求级 lookup 输入视图，javadoc 明确“实现必须视为短生命周期对象，不得被存入 DB”。它是 `NativeBytesSlice` 的顶层接口。
- **非 retained `NativeBytesSlice`**（`NativeByteStore.slice(...)`）：不是所有权载体。每次 `getByte`/`getBytes`/`writeTo` 都重新 `allocator.resolve(...)` 并 pin/unpin 一次。适合当前调用栈内的一次性读取，不适合跨调用保存。
- **retained `NativeBytesSlice`**（`NativeBytesSlice.retained(...)`）：借用一个已经存在的 pin。它由 `ByteValue.owned(...)` 的 owner 负责在 `close()` 时 `unpin`。调用方必须在 pin 存续期内使用，且只由 owner 放 pin。
- **`ByteValue.owned(...)`**：转移一次 pin 的所有权；`close()`（幂等）触发 `unpin` + 可选 `closeHook`。

据此可以给出四条绑定规则：

1. 进入 DB 的只能是 copy 后的 `byte[]` 或 native object 自身，不能是 `BytesView` 或临时 slice。
2. 需要把内容带出当前 prepared command 生命周期，就必须 copy 成 heap（`toByteArray`/`copiedFrom`），或显式持有 `ByteValue` 并在 `closePrepared` 前使用完。
3. `realloc` 与 defrag 都要求对象未被 pin；长期持有 retained view 会直接阻塞这两条回收/整理路径，把内存碎片压在原地。
4. 每次 `resolve` 都会 pin，每次 `resolvePinned` 都要求调用方已 pin；`pin`/`unpin` 必须同 owner 配对，否则泄漏或提前释放。

## 常见误读与误判后果

误读一：off-heap 等于零拷贝。

实际上，当前生产写回明确经过 bounded heap scratch 和 bounded reply chunk。很多边界有意复制，用来换取稳定 ownership 或避免泄漏 native view。误判后果通常是 reply reservation 按“零字节”估算，导致实际编码超限或容量误判。

误读二：`BytesView` lookup 已经避免 heap key。

当前 DB lifecycle 仍会把 `BytesView` 转成 heap `byte[]`，再进入 `NativeKeyDirectory`。新 key 的持久化 key bytes 是 `KEY_BYTES` native object，但 lookup 边界 copy 仍存在。误判后果是把它当零成本路径，从而低估 write/lookup 的分配压力。

误读三：stable handle 是可直接读写的地址。

`NativeHandle` 不是 physical address。必须通过 allocator resolve，拿到短生命周期 `NativeObjectView` 后读写。这个约束让 `realloc`、quarantine 和 active defrag 可以成立。把它当裸地址（或长期保存 view）的后果：对象被 free/quarantine 或 defrag 搬家后，再访问会拿到 `StaleNativeHandleException`，或在 pin 缺失时被拒（`resolvePinned` 要求 `pinCount() > 0`）。

误读四：collection nativeized 等于完全零拷贝。

当前 collection root records、list quicklist metadata records 和 hash/set/zset/list payload internals 都是 allocator-backed native handles。但 RESP decode、`copiedFrom` 聚合快照、返回 `byte[]` 的 API 和诊断输出仍需要 copy。误判后果是把聚合命令也当成流式零拷贝，从而错配 reply plan 的 source 保留量。

off-heap 扩容（容量不足时搬家）、`ByteBuf` write-back、active defrag、heap snapshot 和 retry buffer 都可能在同一侧发生 copy。误判后果是以为“同侧操作 O(1)”，忽略 `reallocate` 搬家与 defrag 的带宽成本。

---

## 拷贝成本口径与内核态边界判定

在分析 copy 路径时，常常伴随两个核心疑问：
1. `Arrays.copyOf(keyBytes, keyBytes.length)`、`bytesOf(value)` 这类堆内拷贝，是否涉及用户态到内核态的切换？
2. 如果不涉及，整条链路中真正的内核态切换发生在哪里？

结论先行：**这两类调用是同一 JVM 进程用户态内的内存操作（一次内存分配 + 一次 O(n) 内存搬运），绝不发起系统调用（syscall），因而没有内核态切换。**

### 1. 判定“是否进内核”的三要素

判定一次操作是否涉及用户态与内核态切换，只看三件事：

| 判定标准 | 堆内/堆外拷贝调用 | 深度说明 |
|---|---|---|
| **是否发起系统调用** | **否** | CPU 未执行 `syscall` / `sysenter` / `int 0x80` 指令，特权级始终在 Ring 3（用户态）。 |
| **是否触碰未驻留的 mmap 页** | **否（常态）** | 仅首次触碰未映射虚存时触发硬件缺页中断（Page Fault `#PF`）陷入内核填零，非常规系统调用。 |
| **是否经由 JNI Native 方法** | **否** | `System.arraycopy` 与数组 `clone()` 是 HotSpot JIT 深度识别的 Intrinsic；FFM 的 MemorySegment 访问在 JIT 编译后也是纯粹的用户态 load/store 指令。 |

### 2. 典型拷贝操作的成本构成

以写路径典型的两行预处理为例（`YierdisStringOps.prepareSet(...)`）：

```java
byte[] preparedKey = Arrays.copyOf(keyBytes, keyBytes.length);
byte[] preparedValue = bytesOf(value);
```

- **`Arrays.copyOf(keyBytes, keyBytes.length)`**：
  在 JDK 21 与 JDK 25 中，等长 `copyOf` 会被直接优化为 `keyBytes.clone()`。其开销是 **1 次 TLAB 指针 bump（分配新对象头与数组空间）+ 1 次 HotSpot intrinsic 向量化内存搬运**。其性能开销落在 **GC 垃圾产出** 上，绝非内核切换。
- **`bytesOf(BytesSlice)`**：
  分配 `new byte[length]` 后调用 `BytesView.getBytes(...)`。
  - 若具体切片（如 `NativeBytesSlice`）覆写了 `getBytes`，走 `MemorySegment.copy`（Intrinsic，接近物理带宽 34~45 GB/s）；
  - 若未覆写（如默认匿名切片），则退化为默认实现的逐字节接口虚调用（`dst[i] = getByte(i)`），吞吐跌至 12~15 GB/s。这是**虚分派与边界检查开销**，同样与内核态无关。

### 3. 真正的内核态切换发生在哪里？

在整个请求与存储处理链路中，真正的用户态/内核态切换集中在以下三处：

1. **网络传输 I/O（真正的内核切换源头）**：
   传输层 Netty `IoEventLoop` 在 Linux 上的底层是 Epoll：`epoll_wait`、`read` / `recvfrom`、`write` / `writev` / `sendmsg` 是实打实的系统调用，每次调用触发 CPU 上下文切换。
2. **Native 内存初始向 OS 申请与归还**：
   `YierdisFfmMemoryRuntime.allocateRegion` 底层由 `Arena.ofShared()` 触发系统的 `mmap`/`brk`，以及小页定期回收 `trimEmptyPages` 调用的 `madvise(..., MADV_DONTNEED)`。这发生在 Region/Page 粒度，**绝非每次命令执行时发生**。
3. **Socket 回复刷新与持久化**：
   Reply Chunk 最终刷新送达 TCP 缓冲区以及将来的 AOF/RDB 落盘。

### 4. 测量证据与成本模型

通过 8000 万次独立循环实测（`strace -c -f java CopyProbe`）：
- 8000 万次内存拷贝期间，进程内核系统调用次数中，拷贝循环体内为 **0 次**（所有 syscall 仅来自启动阶段 JIT 编译与 GC 线程的 `futex` 及内存提交 `mprotect`）。
- 成本模型应准确理解为：
  $$\text{单次拷贝成本} \approx \text{分配次数} \times \text{GC 分配压迫} + \frac{\text{搬运字节数}}{\text{内存带宽}} + \text{逐字节虚分派损耗}$$
- 优化重点是**复用缓冲区、避免短命 byte[] 分配、并让切片实现批量覆写 `getBytes`**，而非徒劳地去“减少内核切换”。