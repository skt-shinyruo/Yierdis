# 拷贝成本与内核边界

## 这篇文档回答什么

两个反复出现的问题：

1. `Arrays.copyOf(keyBytes, keyBytes.length)`、`bytesOf(value)` 这类 heap 拷贝，是否涉及用户态到内核态的切换？
2. 如果不涉及，这条链路上真正的内核切换发生在哪里？

结论先行：**这两类调用是同一 JVM 进程用户态内的内存操作——一次分配加一次 O(n) 搬运——不会发起系统调用，也就没有内核态切换。** 哪些边界会 copy 由 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 回答；本文只回答“这次 copy 要不要进内核，成本到底花在哪”。

判定“是否进内核”只看三件事：

| 问题 | 这两个调用 | 说明 |
| --- | --- | --- |
| 有没有发起系统调用 | 否 | 没有 `syscall` 指令，CPU 不会切到 ring 0 |
| 有没有触碰尚未驻留的 mmap 页 | 否（常态） | 命中缺页才陷入内核，这不是该 API 主动跨界 |
| 是不是 JNI native 方法 | 否 | `arraycopy`/数组 `clone()` 是 HotSpot intrinsic，FFM 访问是用户态 load/store |

## 两行代码的成本构成

以 `YierdisStringOps.prepareSet(...)` 为例：

```java
byte[] preparedKey = Arrays.copyOf(keyBytes, keyBytes.length);
byte[] preparedValue = bytesOf(value);
```

`Arrays.copyOf(keyBytes, keyBytes.length)`：JDK 21 与 25 的 `byte[]` 重载对“等长”有短路，直接返回 `clone()`；JDK 17 没有这个短路，走 `new byte[n]` + `System.arraycopy`，结果相同。

```java
// JDK 内置实现（java.util.Arrays），不是本仓库代码
public static byte[] copyOf(byte[] original, int newLength) {
    if (newLength == original.length) {
        return original.clone();
    }
    byte[] copy = new byte[newLength];
    System.arraycopy(original, 0, copy, 0, Math.min(original.length, newLength));
    return copy;
}
```

`bytesOf(BytesSlice)`（`YierdisStringOps`）是 `new byte[length]` 加 `BytesView.getBytes(...)`。走哪条搬运路径取决于 slice 实现是否覆写了 `getBytes`：

| `BytesSlice` 实现 | 覆写 `getBytes` | 实际搬运 |
| --- | --- | --- |
| `CommandArgs.RequestSlice` | 否 | `BytesView` 默认实现：逐字节 `dst[...] = getByte(...)`，每字节一次接口分派 |
| `YierdisStringOps.sliceOf(byte[])` 的匿名实现 | 否 | 同上 |
| `NativeBytesSlice` | 是（`NativeBytesSlice.java:62`） | `MemorySegment.copy`（FFM intrinsic） |
| `StringRoot.HeapBackedBytesSlice` | 是（`StringRoot.java:353`） | `System.arraycopy` |

关键结论：`SET`/`APPEND`（`StringCommands` → `prepareSet`/`append`）以及 `StorageBenchmarkRunner` 传下来的 slice 都是 `RequestSlice` 或 `sliceOf(...)`，**都没有覆写**，所以这两条命令路径上的 `bytesOf` 是逐字节虚调用，拿不到批量 intrinsic。批量路径只在 native/堆内 slice 的直接消费者（`NativeBytesSlice.writeTo`、`StringRoot` 的落盘循环、`CapturedByteItems.CapturingSink`）上生效。

两者的成本构成：

| 阶段 | 真实机器行为 | 是否进内核 |
| --- | --- | --- |
| 分配 | TLAB 指针 bump；TLAB 不足时走共享分配 | 否（只有堆要向 OS 要页或 GC 整理地址空间时才有系统调用） |
| 搬运 | `System.arraycopy`、数组 `clone()` 是 HotSpot intrinsic，x86 上内联为 SIMD 搬运；默认 `getBytes` 是每字节一次接口分派加边界检查 | 否 |
| 首次触碰新数组页 | 未映射页触发 `#PF` 缺页 | 是（硬件陷阱，不是这次 API 调用发起的系统调用） |
| GC | 分配压力累积后由 GC 触发，内部会做 `mmap`/`mprotect` | 是，但属于堆管理，与这两行代码的语义无关 |

## 内核切换真正在哪

- **网络 I/O。** 传输层是 `MultiThreadIoEventLoopGroup`（`YierdisServerBootstrap`），Linux 上底层是 epoll：`epoll_wait`/`read`/`write` 才是真正的用户态到内核态切换。decoder 里把 ByteBuf 内容读进 heap 数组的 `in.readBytes(bulk.buffer(), ...)`（`RespRequestDecoder`）只是一次用户态搬运；ByteBuf 即使是 pooled direct，读它也只是用户态拷贝（Netty 4.2 在 JDK 25+ 上默认不走 `sun.misc.Unsafe`：pooled direct buffer 经 `ByteBuffer` 访问、堆内 buffer 走数组；`MemorySegment` 只出现在 direct buffer 的分配/释放路径，与读写无关）。无论哪种实现都不额外进内核。唯一的例外是 mmap 文件页尚未驻留时的首次触碰缺页。
- **native region 分配。** `YierdisFfmMemoryRuntime.allocateRegion(...)` 的 `Arena.ofShared()` 加 `arena.allocate(bytes)` 底层是匿名内存的 `malloc`/`mmap`，这是货真价实的系统调用；但它按 region/页发生，不是每次读写，更不是每次 `SET`。
- **写回与持久化。** reply chunk 最终经 socket 写出，以及任何文件 I/O，都会进内核。

明确**不属于**内核切换的：`YierdisFfmRegion.setBytes` / `getBytes` 的 `MemorySegment.copy(...)`、`NativeObjectView` 的 `getByte`/`setByte` FFM 访问、`NativeKeyDirectory` 的 native hash 与 `equalsBytes` 比较。这些是用户态 load/store（FFM 另外带边界与生命周期检查，JIT 可消除一部分）。

## 实测证据：80M 次拷贝，syscall 只来自 JIT 与 GC 启动

探针是四段独立循环各 20M 次，源和目标都是 100 B（`Arrays.copyOf` 每次分配新数组）。JDK `25.0.4.1`，**不是 JMH**，只用来判断量级和“是否跨界”，不作为性能门槛。

```java
// 把下面两段与注释里的四段循环体拼成一个 CopyProbe 类，javac 后直接跑：
//   java CopyProbe 20000000
interface View {  // 等价 BytesView，省略了源码里的 null/负长度/越界检查，只保留搬运循环
    int length();
    byte getByte(int index);
    default void getBytes(int index, byte[] dst, int dstOff, int len) {
        for (int i = 0; i < len; i++) {
            dst[dstOff + i] = getByte(index + i);
        }
    }
}

static final class ArrayView implements View {  // 等价 YierdisStringOps.sliceOf(byte[])：不覆写 getBytes
    private final byte[] bytes;
    ArrayView(byte[] bytes) { this.bytes = bytes; }
    public int length() { return bytes.length; }
    public byte getByte(int index) { return bytes[index]; }
}
// 四段循环体：Arrays.copyOf(src, src.length) / System.arraycopy(src, 0, dst, 0, 100)
//            / MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, 0, dst, 0, 100) / view.getBytes(0, dst, 0, 100)
```

三次运行（20M 次/段，取区间）：

| 搬运方式 | ns/op @100 B | 等效带宽 |
| --- | --- | --- |
| `Arrays.copyOf`（等长，即 `clone()`） | 20–30 | 3.4–5.0 GB/s |
| `System.arraycopy` | 2.2–4.5 | 22–46 GB/s |
| `MemorySegment.copy` | 2.2–2.9 | 34–45 GB/s |
| `View.getBytes`（默认逐字节） | 6.5–8.2 | 12–15 GB/s |

`Arrays.copyOf` 比裸搬运慢一个数量级，唯一原因是它每次分配新数组：80M 次分配持续产出垃圾，成本落在 TLAB 与 GC 上，不在搬运上。逐字节 `getBytes` 与批量搬运约差 3 倍。

同一次运行的 syscall 计数（`strace -c -f java CopyProbe 20000000`，含 JVM 启动）：

```text
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 99.60   20.204231        4830      4183       956 futex
  0.10    0.020289          48       415           mprotect
  0.07    0.013424          42       317           mmap
100.00   20.285205        3116      6508      1052 total
```

80M 次拷贝对应整个进程 6508 次系统调用，且全部落在启动期的 `futex`（JIT 编译线程与 GC 线程 park/unpark）和 `mprotect`/`mmap`（堆与代码缓存提交）上。循环体内 0 次——这就是“不进内核”的直接证据：拷贝无论如何都不会产生 `syscall` 指令。

intrinsic 证据来自 `-XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining`：

```text
java.util.Arrays::copyOf (33 bytes)                                  inline (hot)
  @ 7  java.lang.Object::clone (0 bytes)                                           (intrinsic)
jdk.internal.foreign.AbstractMemorySegmentImpl::copy (255 bytes)     force inline by annotation
  @ 29 jdk.internal.misc.Unsafe::copyMemory0 (0 bytes)                             (intrinsic)
```

等长 `Arrays.copyOf` 确实被内联成 `Object::clone` 内在函数，`MemorySegment.copy` 落到 `Unsafe::copyMemory0` 内在函数；两条路都没有 native 方法边界，更不产生系统调用。

### 这些测量没有覆盖的内核触点

- **缺页异常。** 未驻留的 mmap 页首次触碰会陷入内核填零页，这是硬件陷阱不是 syscall，`strace` 看不到；本机无 `perf`，这一项未实测，判断依据是页表语义而非测量。常态下每页只发生一次，之后常驻。
- **TLAB 用尽。** 分配慢路径可能触发 `mprotect`（上表 415 次里包含这类），`-Xms == -Xmx` 加 `-XX:+AlwaysPreTouch` 可以提前付掉。
- **GC。** 停顿本身不是跨界，但 GC 线程的 park/unpark 走 `futex`；上表 `futex` 占 99.6% 的 syscall 时间只说明它在启动期密集出现，与每次拷贝无关。
- **竞争锁。** 无竞争的 monitor/CAS 是纯用户态；线程被 park 才退化成 `futex`。
- **socket 写回复。** 真正的 I/O 在 reply flush，不在这两行。

### 由测量反推的成本模型

```text
这两行的成本 ≈ 分配次数 × GC 压力 + 搬运字节数 ÷ 内存带宽
```

任何“减少内核态切换”的优化方向在这条链路上都是空的。能减的是分配次数（复用缓冲、避免多余的 `sliceOf` 包装）和把逐字节 `getBytes` 换成批量搬运。

## SET 的 value 在这条链路上被拷了几次

```text
ByteBuf（socket 读入）
  -> heap byte[]                 decode：RespRequestDecoder 的 in.readBytes(bulk.buffer(), ...)
  -> preparedValue（byte[]）     prepare：YierdisStringOps.bytesOf(BytesSlice)
  -> native STRING_BYTES         commit：StringRoot.setBytes 经 8 KiB ThreadLocal scratch 分块
```

前两次是 heap 到 heap，第三次是 heap 到 native。**三次都不进内核**；它们的存在理由是 ownership 与 lifetime（跨过 Netty decoder 生命周期、跨过调用方数组生命周期、以及 DB 要求 native 持久化），不是“绕内核”。判定规则见 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。

## 值得优先处理的热点：默认 getBytes 是逐字节虚调用

`BytesView.getBytes` 的默认实现逐字节调 `getByte`，而这两个实现都没有覆写它：

- `CommandArgs.RequestSlice`：`getByte` 转发 `ExecutionRequest.byteAt(...)`，`ByteArrayExecutionRequest.byteAt` 再付一次 JVM 插入的数组下标越界检查；
- `YierdisStringOps` 里的 `sliceOf(byte[])`。

于是 `bytesOf(value)` 对每个字节付一次接口分派加一次边界检查；`StringRoot.setBytes` 的分块循环里对 `sliceOf(preparedValue)` 也一样逐字节虚调用。这是**用户态开销，不是切换开销**。如果 profile 显示这里热，给这两个实现补一个批量 `getBytes`（内部 `System.arraycopy`）比任何“减少内核切换”的思路都更有效。

## 常见误读

误读一：heap 拷贝“可能走到内核”。

`arraycopy`/`clone` 不会发起系统调用。真正可能在内核里留下痕迹的只有新页缺页和 GC 的地址空间操作，两者都不是“每次拷贝一次切换”。

误读二：off-heap 访问更快，因为“绕过了内核”。

native 内存的收益是缩小 GC 扫描面、消除对象头膨胀、让字节级记账可行，见 [`jvm-constraints-and-offheap-rationale.md`](./jvm-constraints-and-offheap-rationale.md)。FFM/`Unsafe` 的每次访问仍是用户态 load/store，而且要带边界与生命周期检查；只有 region 分配释放和 mmap 缺页才涉及内核。

误读三：`Arrays.copyOf(x, x.length)` 等长就没有分配。

JDK 21/25 上它等价于 `x.clone()`，仍是一次新分配加一次搬运；“等长”只省掉 `Math.min` 与 `arraycopy` 分支，不省分配。

误读四：copy 成本约等于内存带宽。

只有走 intrinsic 的 `arraycopy`/`clone` 才接近带宽上限。默认逐字节 `getBytes` 是每字节一次接口调用，100 B 上实测 6.5–8.2 ns，而 `arraycopy`/`MemorySegment.copy` 是 2.2–2.9 ns，约 3 倍；真正差一个数量级的是**带分配的 `Arrays.copyOf`**（20–30 ns）。把三者都叫“一次拷贝”会严重误判热路径，见上文实测表。
