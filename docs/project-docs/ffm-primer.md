# JDK FFM 入门

本文是阅读 Yierdis native-memory 文档前的最小 JDK FFM 入门。它只解释后续文档会反复用到的概念。

## 先记住三句话

1. `Arena` 决定一批 native memory 的 lifetime。
2. `MemorySegment` 是带边界和 lifetime 检查的内存视图。
3. `ValueLayout` / `MemoryLayout` 决定一段 bytes 被解释成什么类型或结构。

FFM 是 Foreign Function and Memory API。Yierdis 当前使用 JDK 25 `java.lang.foreign`，主要用的是 foreign memory 部分：分配 native memory、切 segment、按 layout 读写 bytes，并把这些能力封装成 runtime、allocator 和 stable handle。

## Arena

`Arena` 像一个生命周期容器。用 arena 分配出来的 segment 共享同一组关闭规则；arena 关闭后，这些 segment 都不能继续访问。

最常见的几类 arena：

- `Arena.ofConfined()`：可手动关闭，只允许创建线程访问。
- `Arena.ofShared()`：可手动关闭，允许跨线程访问和关闭。
- `Arena.ofAuto()`：由 GC 触发释放，不能手动 close。
- `Arena.global()`：全局长期存在，不能 close。

入门示例通常用 confined arena：

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment segment = arena.allocate(64);
    segment.set(ValueLayout.JAVA_INT, 0, 42);
}
```

Yierdis 里不能简单套用入门示例。当前 `YierdisFfmMemoryRuntime.allocateRegion(...)` 用 `Arena.ofShared()`，因为 region 可能在 bootstrap 期间创建，再由 DB owner thread 释放。这个运行时语义见 [`native-memory-runtime.md`](./native-memory-runtime.md)。

## MemorySegment

`MemorySegment` 表示一段连续内存的 Java 视图。它不是裸指针：

- 有 byte size。
- 访问时做边界检查。
- 访问时检查所属 scope 是否仍 alive。
- 可以从 native memory、heap array、direct buffer、mapped file 等来源创建。

示例：

```java
MemorySegment segment = arena.allocate(16);
segment.set(ValueLayout.JAVA_BYTE, 0, (byte) 'Y');
byte first = segment.get(ValueLayout.JAVA_BYTE, 0);
```

如果 segment 只有 16 bytes，访问 offset 16 就越界；如果 arena 已经 close，再访问这个 segment 也会失败。

## ValueLayout

native memory 本质是一串 bytes。`ValueLayout` 描述“从某个 offset 开始，按什么 Java 类型读写”。

常用 layout：

- `ValueLayout.JAVA_BYTE`
- `ValueLayout.JAVA_INT`
- `ValueLayout.JAVA_LONG`
- `ValueLayout.JAVA_INT_UNALIGNED`
- `ValueLayout.JAVA_LONG_UNALIGNED`
- `ValueLayout.ADDRESS`

示例：

```java
segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 8, 100L);
long value = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 8);
```

Yierdis 的 `YierdisFfmAccess` 就是把这类读写集中起来：`getByte`、`setByte`、`getInt`、`setLong` 等方法先做 range check，再用 `ValueLayout` 访问 `YierdisFfmSpan` 背后的 segment。

## offset 和 index

FFM 里有两种常见访问方式。

`offset` 是 byte offset：

```java
segment.set(ValueLayout.JAVA_INT, 4, 123);
```

这里的 `4` 表示从第 4 个 byte 开始写一个 int。

`index` 是按 layout 大小计算的元素下标：

```java
segment.setAtIndex(ValueLayout.JAVA_INT, 1, 123);
```

这里的 `1` 表示第 1 个 int，也就是 byte offset `1 * Integer.BYTES`。

读 Yierdis 源码时，大多数 DB/allocator layout 都使用 byte offset，因为 entry record、root record 和 metadata 经常是多个字段拼在一起。

## slice

`slice` 是从一段 segment 派生出子视图，不复制底层 memory：

```java
MemorySegment page = arena.allocate(4096);
MemorySegment header = page.asSlice(0, 64);
MemorySegment body = page.asSlice(64, 4032);
```

子 segment 仍然受原 arena lifetime 控制。`YierdisFfmRegion.span(offset, length)` 和 `YierdisFfmSpan.slice(offset, length)` 都是这种模型：先检查范围，再用 `segment.asSlice(...)` 创建一个更小的视图。

## lifetime 和关闭后的访问

FFM 的重要安全性来自 lifetime check。关闭 arena 或 region 后，之前拿到的 segment、span、view 都不应该继续使用。

```java
MemorySegment leaked;
try (Arena arena = Arena.ofConfined()) {
    leaked = arena.allocate(8);
}
// leaked.get(...) 会失败，因为 arena 已关闭。
```

Yierdis 在更高层也维护同类约束：

- `YierdisFfmRegion.close()` 关闭 arena，并通知 runtime 扣减 live region accounting。
- allocator `resolve(...)` 返回短生命周期 `NativeObjectView`，使用完必须 close。
- active defrag、`realloc` 和 free 不能依赖长期缓存的 physical address。

这些约束见 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。

## MemoryLayout 和结构化内存

`MemoryLayout` 用来描述结构化 native memory，例如一个 C struct 或固定字段 record。

概念示例：

```java
MemoryLayout point = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("x"),
        ValueLayout.JAVA_INT.withName("y")
);
```

有了 layout 后，可以用 var handle 或手写 offset 访问字段。Yierdis 当前 DB record 多数选择手写固定 offset，例如 entry record：

```text
0   key handle identity
8   value handle raw
16  key hash
...
```

这种写法可维护性来自文档、测试和集中封装，而不是让业务代码到处猜 offset。

## native function 调用边界

FFM 也能调用 native function，核心概念是：

- `Linker`：把 native function 和 Java method handle 连接起来。
- `SymbolLookup`：在 library 中找 symbol。
- `FunctionDescriptor`：描述参数和返回值 layout。

简化示例：

```java
Linker linker = Linker.nativeLinker();
SymbolLookup lookup = linker.defaultLookup();
MemorySegment strlen = lookup.find("strlen").orElseThrow();
FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);
```

Yierdis native-memory 文档重点不是 native function 调用，而是 native memory 管理。读源码时如果看到 `Linker` / `SymbolLookup`，那是 FFM 的 foreign function 半边；当前核心存储路径主要在 memory 半边。

## 接下来读什么

- 想看 Yierdis 如何把 FFM 接进实例、DB、region、span、maxmemory：读 [`native-memory-runtime.md`](./native-memory-runtime.md)。
- 想看 stable handle、object table、pin、epoch、quarantine、active defrag：读 [`native-allocator-and-handles.md`](./native-allocator-and-handles.md)。
- 想看 heap、direct buffer、FFM native memory 之间什么时候复制：读 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。
