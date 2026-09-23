# FFM 在 Yierdis 中的用法

## 一、FFM 是什么

FFM（Foreign Function & Memory API，包名 `java.lang.foreign`）是 JDK 22 起定稿的 API，用来替代 `sun.misc.Unsafe` 和大部分 JNI 场景，做两件此前很难做好的事：

1. **堆外内存**：分配、读写、释放不受 GC 管理的内存，并让 JVM 知道这块内存何时失效。
2. **调用 native 函数**：把 C 函数按签名描述成 `MethodHandle` 直接调用，不用写 JNI 胶水代码。

它把"谁拥有这块内存、什么时候能访问、谁来释放"变成 API 层面可表达、可检查的约束，而不是靠约定。

## 二、核心 API 的角色

| API | 角色 |
| --- | --- |
| `Arena` | 生命周期作用域。决定这块内存由谁释放、何时释放、能否跨线程访问。 |
| `MemorySegment` | 一段有界内存的视图。heap 数组和 off-heap 地址都用它统一表示，`get`/`set` 读写。 |
| `ValueLayout` | 单个元素的类型 + 对齐 + 字节序描述，`get`/`set` 必须给 layout。 |
| `MemoryLayout`（`StructLayout`/`SequenceLayout`/`UnionLayout`/`PaddingLayout`） | 把结构体/数组/padding 描述成可计算 offset 的布局，供 `VarHandle` 和 `Linker` 使用。 |
| `Linker` / `SymbolLookup` | 调用 native 函数：查符号 + 生成 downcall handle。 |
| `FunctionDescriptor` / `MethodHandle` / `VarHandle` | 描述 native 函数签名、承载生成出的可调用对象、按结构化布局读写字段。 |

几个关键语义：

- **Arena 种类**：`Arena.ofConfined()` 要求创建/访问/关闭都在同一线程；`Arena.ofShared()` 允许任意线程访问和关闭；`Arena.ofAuto()` 交给 GC，释放时机不确定；`Arena.global()` 永不释放。
- **Segment 绑定 Arena**：`arena.allocate(size)` 返回的 segment 只在 arena 存活期间有效；arena 关闭后任何访问都抛异常。
- **对齐**：`JAVA_LONG`/`JAVA_INT` 这类对齐 layout 在未对齐 offset 上会抛 `IllegalArgumentException`；`*_UNALIGNED` 变体去掉该限制。
- **`MemorySegment.copy(...)`**：在 segment↔array、segment↔segment 之间批量搬运，是"搬运字节"的唯一正道（不是靠裸地址）。

## 三、Yierdis 实际用了哪几个

在仓库 main 源码里全局搜索 `import java.lang.foreign.`，只出现三处，且集中在两个文件：

- `yierdis-db/.../memory/foreign/YierdisFfmMemoryRuntime.java`：`Arena`、`MemorySegment`
- `yierdis-db/.../memory/foreign/YierdisFfmRegion.java`：`Arena`、`MemorySegment`、`ValueLayout`

**Yierdis 不调用任何 native 函数**：没有 `Linker`、`SymbolLookup`、`FunctionDescriptor`、`MethodHandle` 的使用，也没有 `MemoryLayout`——因为所有数据结构布局都是手写的固定 offset（见第八节 `EntryTable`），不需要 `StructLayout` + `VarHandle`。这里只用到了 FFM 的"堆外内存"那一半。

一个容易踩的坑：`NativeByteMap` 等文件里出现的 `ValueLayout.OBJECT_REFERENCES`、`ValueLayout.CONSTANT` 等，是仓库内部同名枚举（`storage.memory.internal.value.ValueLayout`），**不是** `java.lang.foreign.ValueLayout`。搜索时不要混淆。

所以下面只讲 `Arena`、`MemorySegment`、`ValueLayout` 在 Yierdis 里的真实用法。

## 四、Arena：一个 region 一个 arena

`YierdisFfmMemoryRuntime.allocateRegion(owner, bytes)` 是创建堆外内存的唯一入口：

```java
Arena arena = null;
MemorySegment segment;
try {
    arena = Arena.ofShared();
    segment = arena.allocate(bytes);
} catch (OutOfMemoryError failure) {
    if (arena != null) {
        arena.close();
    }
    throw new NativeCapacityExceededException(
            "native region allocation failed for " + bytes + " bytes",
            failure
    );
}
YierdisFfmRegion region = new YierdisFfmRegion(this, arena, segment, bytes);
liveRegionCount.incrementAndGet();
usedBytes.addAndGet(bytes);
```

**为什么是 `Arena.ofShared()` 而不是 `ofConfined()`**：region 可能在 bootstrap 期间创建，而释放发生在 DB owner thread，创建与关闭跨线程；`ofConfined()` 会拒绝这种用法。代价是放弃了 confined 自带的线程约束，所以这个约束改由 Yierdis 自己的 `MemoryOwner` / `bindToCurrentThread()` 承担——`StableMemoryBackend` 的接口注释也明确写了"内存访问须显式绑定，任何普通内存访问都不得代替调用方完成绑定"。

**失败的代价**：`arena.allocate` 可能抛 `OutOfMemoryError`。此时必须显式 `arena.close()`，否则半创建的 arena 会泄漏 native 内存；随后统一转成 `NativeCapacityExceededException`，让上层把它当作可预期的容量错误处理，而不是把 `Error` 一路抛穿业务逻辑。

## 五、Region 的生命周期与访问

`YierdisFfmRegion` 保存 runtime、arena、segment、size。所有访问先 `ensureOpen()`（`closed` 标志 + `arena.scope().isAlive()`），再 `checkRange(offset, length)`，然后访问 segment：

- `getByte` / `setByte` 使用 `ValueLayout.JAVA_BYTE`
- `getInt` / `setInt` 使用 `ValueLayout.JAVA_INT_UNALIGNED`
- `getLong` / `setLong` 使用 `ValueLayout.JAVA_LONG_UNALIGNED`
- `getBytes` / `setBytes` / `copyTo` 使用 `MemorySegment.copy(...)`

`arena.scope().isAlive()` 是 FFM 提供的存活检查：arena 关闭后它返回 false，访问会在这里被拦下，而不是等到真正读写 native 地址才 crash。它和 `MemorySegment` 自身在 arena 关闭后抛 `IllegalStateException` 形成双重兜底。

**为什么统一用 `*_UNALIGNED`**：region 内部按固定 offset 手工打包记录（例如 entry record 里的 `expireAtMillis`、`version`、`lruOrLfu`），8 字节字段可能落在不做 8 字节对齐的 offset 上。对齐 layout 在未对齐地址上会抛 `IllegalArgumentException`，所以这里统一用 `*_UNALIGNED` 变体，把"对齐是否正确"的判断权交回给上层布局代码。代价是失去对齐优化，但区域本身已按页切块，收益本就有限。

`MemorySegment.copy` 的三种重载在 `YierdisFfmRegion` 里各用一处：

- `copy(segment, JAVA_BYTE, offset, byte[], dstOff, len)`：region → heap array（`getBytes`）
- `copy(byte[], srcOff, segment, JAVA_BYTE, offset, len)`：heap array → region（`setBytes`）
- `copy(srcSegment, srcOff, dstSegment, dstOffset, len)`：region → region（`copyTo`，供 allocator 搬家与 defrag 使用）

`close()` 幂等，顺序是"先释放、再记账"：

```java
public void close() {
    if (closed) {
        return;
    }
    closed = true;
    int releasedBytes = size;
    RuntimeException failure = null;
    try {
        arena.close();                       // 真正把 native memory 还给 OS
    } catch (RuntimeException closeFailure) {
        failure = closeFailure;
    }
    try {
        runtime.onRegionClosed(releasedBytes); // 扣减 liveRegionCount 与 usedBytes
    } catch (RuntimeException accountingFailure) {
        ...                                   // 用 addSuppressed，不掩盖 close 失败
    }
    if (failure != null) {
        throw failure;
    }
}
```

## 六、Region 的粒度：谁在调用 allocateRegion

`allocateRegion` 的调用点只有两处，说明"一个 region"对应"一页（或一段连续页）/ 一个 metadata segment"：

- `YierdisNativePageAllocator`：小页固定 `PAGE_BYTES = 64 * 1024`；大对象走 span，容量为 `pagesFor(requestedBytes) * PAGE_BYTES`，超过 `MEDIUM_MAX_BYTES = 1024 * 1024` 记为大 span，否则记为中 span。
- `YierdisNativeObjectSegment`：每个对象表 metadata segment 一个 region，容量 `SLOTS_PER_SEGMENT (4096) * META_BYTES (36)`。

上层的 `NativeHandle` 稳定身份、object table、pin/epoch/quarantine 全都建立在 region 之上。

## 七、StableMemoryBackend 与 FFM 的桥

`YierdisFfmStableMemoryBackend` 把 region/page 封装成 `StableMemoryBackend`：`allocate(...)` 从 page allocator 取块，`resolve(...)` 返回 `NativeObjectView`。视图关闭后内容不可再访问，调用方用完必须 `close()`：

- `resolve(handle, mode)`：视图**自己持一次 pin**，`close()` 时 `unpin`。
- `resolvePinned(handle, mode)`：**借用调用方已有的 pin**，只接受 `NativeAccessMode.READ_ONLY`，`close()` 不会替调用方 `unpin`。

这就是"普通视图 vs 保留视图"的差别，也是写回路径里 `GET` 能一边 pin 一边流式写出的基础。

## 八、为什么是手写布局而不是 MemoryLayout

`EntryTable` 按固定 offset 把 entry record 写入 native memory；`NativeStorageLayout.ENTRY_RECORD_BYTES` 为 72。`writeHandle` 按 little-endian long 把 `allocatorId` 和 `localRaw` 写入 handle：

```text
0   key handle allocatorId
8   key handle localRaw
16  value handle allocatorId
24  value handle localRaw
32  key hash
36  type
40  encoding
44  flags
48  expireAtMillis
56  version
64  lruOrLfu
```

record 大小 72 字节 = 9 个 8 字节槽。`type`/`encoding`/`flags` 实际只占 4 字节，但仍各占一个槽——这是"固定布局 + 显式 offset"的取舍：一次 `getInt` 就能拿到字段，不用 `StructLayout` + `VarHandle` 的间接层。这些字段通过 `NativeObjectView.setLongLittleEndian(...)` / `setIntLittleEndian(...)` 写入，最终落到 `YierdisFfmRegion` 的 `MemorySegment.set(...)`。

（对比：如果改用 `MemoryLayout`/`StructLayout`，就能用 `VarHandle` 直接按字段名读写，但会引入布局对象的构造与校验成本，并且字段数量变化时要同步改布局描述。当前数据结构字段少且稳定，手写 offset 更直接。）

## 九、读源码时怎么定位

- 想看堆外内存的实际读写在哪：从 `StableMemoryBackend` 追到 `YierdisFfmStableMemoryBackend` → `YierdisNativePageAllocator` / `YierdisNativeObjectTable` → `YierdisFfmRegion`。
- 想看堆外总量、碎片、生命周期：`YierdisFfmMemoryRuntime.usedBytes()` / `liveRegionCount()`、`YierdisFfmStableMemoryBackend.memoryUsage()` / `stats()`。
- 快速锚点：`Arena.ofShared()`、`MemorySegment.copy`、`ValueLayout.*_UNALIGNED` 是这三处 FFM 调用的搜索关键词。