# FFM Guide and Usage in Yierdis

本文把 JDK FFM 入门和 Yierdis 内部 FFM 使用方式放在同一个入口里。阅读顺序建议是先建立 FFM 心智模型，再对照 Yierdis 的 native-memory runtime、allocator、keyspace、entry table 和各类型 root 读实现。

如果只关心“哪些路径会发生 heap / off-heap 拷贝”，请优先看 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。

## 阅读路线

- 第一次接触 FFM：从第一部分开始，先理解 `Arena`、`MemorySegment`、`ValueLayout`、`MemoryLayout` 和 native function 调用模型。
- 已经熟悉 JDK FFM：可以直接跳到第二部分，看 Yierdis 如何把 FFM 作为 native-memory substrate 使用。
- 排查 heap / off-heap 拷贝：配合 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md) 一起看。

## 第一部分：JDK FFM 入门

本部分面向从未接触过 FFM 的读者，目标不是讲全，而是帮你尽快建立正确心智模型，并能自己写出最基本的 FFM 代码。

### FFM 是什么

FFM 是 `Foreign Function and Memory API`，可以粗略理解成：

- `Foreign Memory`：让 Java 能安全地操作 JVM 堆外的 native memory
- `Foreign Function`：让 Java 能直接调用 C 等 native library 中的函数

这里的 `foreign` 不是“远程”，而是“JVM 之外”。

FFM 的目标可以概括成一句话：

> 用纯 Java API 访问 native memory 和 native function，尽量替代一部分 JNI 和 `Unsafe` 的使用场景。

在 OpenJDK 中，FFM 由 JEP 454 在 JDK 22 正式定稿；在 Java 25 中，相关 API 位于 `java.lang.foreign` 包。

### 为什么 Java 需要 FFM

如果 Java 程序只处理普通对象，通常不需要 FFM。你只有在碰到下面这些问题时，才会真正需要它：

- 需要调用 C/C++ 或系统库
- 需要操作堆外内存
- 需要映射文件或实现 off-heap 数据结构
- 想减少 JNI 胶水代码
- 想减少 GC 压力，把大块数据放到 native memory

FFM 出现之前，Java 常见的几种办法分别有明显问题：

#### `ByteBuffer.allocateDirect()`

它能分配堆外内存，但更像“direct buffer”而不是“完整的 native memory 模型”：

- 表达能力偏缓冲区模型
- 不适合复杂结构
- 生命周期控制不够直接

#### `sun.misc.Unsafe`

它很强，但也很危险：

- 能直接分配和释放 native memory
- 能直接按地址读写
- 也能轻易越界、悬空引用、造成 JVM 崩溃

#### JNI

JNI 能调用 native 代码，但开发体验很重：

- 要写 `native` 方法
- 要写 C/C++ 胶水代码
- 要处理构建、头文件、签名对齐
- 调试和维护成本高

FFM 的价值就是：

- 比 JNI 更直接
- 比 `Unsafe` 更安全
- 比 direct buffer 更完整

### 先记住 3 句话

如果你第一次接触 FFM，请先把下面三句话记住：

1. `MemorySegment` 是“带边界的内存块视图”。
2. `Arena` 是“这批内存什么时候死”。
3. `Linker + SymbolLookup + FunctionDescriptor` 是“怎么把 native 函数变成 Java 能调用的方法”。

这三句话基本概括了 FFM 的大部分内容。

### 先学哪一半

FFM 可以分成两半：

- 内存 half：`Arena`、`MemorySegment`、`ValueLayout`、`MemoryLayout`
- 函数 half：`Linker`、`SymbolLookup`、`FunctionDescriptor`

推荐学习顺序是：

1. 先学内存
2. 再学布局
3. 最后学 native function 调用

原因很简单：

- 内存这半边更容易理解
- 函数调用这半边如果签名写错，风险更高

### 核心概念一：`Arena`

`Arena` 控制它分配出来的 memory segment 的生命周期。

你可以把它想成“内存生命周期容器”：

- 你从 arena 里分配出很多 segment
- 它们共享同一个生命周期
- arena 关闭后，这些 segment 全部失效

这和 `malloc/free` 的区别非常大。FFM 不是让你回到“每块内存单独 free”的痛苦模式，而是给你一层更结构化的生命周期管理。

#### 最常见的 arena 类型

##### `Arena.ofConfined()`

最常用。

- 可手动关闭
- 只能由创建它的线程访问
- 生命周期清晰

大多数入门代码都应该先用它。

##### `Arena.ofShared()`

- 可手动关闭
- 可以跨线程访问

只有在你真的要跨线程共享 native memory 时才需要它。

##### `Arena.ofAuto()`

- 不能手动 close
- 生命周期由 GC 间接管理

它不是错，但不适合作为理解 FFM 生命周期模型的第一站。

##### `Arena.global()`

- 类似全局长期存在的 arena
- 不能关闭

适合极少量、几乎进程级常驻的数据，不适合日常业务代码。

### 核心概念二：`MemorySegment`

`MemorySegment` 表示一段连续内存。

它不像 Java 数组，也不像 C 裸指针，更准确地说，它是：

- 一块内存的视图
- 带边界检查
- 带生命周期检查

你可以把它理解成“受保护的指针对象”。

#### 为什么它比 `Unsafe` 安全

FFM 主要多了两层保护：

##### 空间边界

如果一块 segment 只有 100 字节：

- 访问第 0 到 99 字节可以
- 访问第 100 字节不行

##### 时间边界

如果 arena 已经关闭：

- 这块 segment 就不能再访问
- 再访问会失败，而不是静默踩坏内存

这就是 FFM 最重要的安全基础。

### 核心概念三：`ValueLayout`

内存本质上只是一堆字节。

问题在于：你怎么解释这些字节？

- 是 `byte`？
- 是 `int`？
- 是 `long`？
- 是地址？

`ValueLayout` 就是这种解释规则。最常见的有：

- `ValueLayout.JAVA_BYTE`
- `ValueLayout.JAVA_INT`
- `ValueLayout.JAVA_LONG`
- `ValueLayout.ADDRESS`

它告诉 JVM：

- 这个值有多大
- 应该按什么类型解释

### 第一段可运行代码：分配一段 off-heap int 数组

先从最简单的开始。

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Step1IntArray {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(10L * Integer.BYTES);

            for (int i = 0; i < 10; i++) {
                segment.setAtIndex(ValueLayout.JAVA_INT, i, i * 10);
            }

            for (int i = 0; i < 10; i++) {
                int value = segment.getAtIndex(ValueLayout.JAVA_INT, i);
                System.out.println("index=" + i + ", value=" + value);
            }
        }
    }
}
```

编译运行：

```bash
javac Step1IntArray.java
java Step1IntArray
```

这段代码做了什么：

- 创建一个 confined arena
- 分配 10 个 `int` 大小的 off-heap 空间
- 按数组索引写入数据
- 再按数组索引读回来
- 离开 `try` 自动释放

如果你能看懂这段，说明你已经掌握了 FFM 最基础的内存分配和访问模型。

### `set/get` 和 `setAtIndex/getAtIndex` 的区别

这是一个很常见的初学者疑问。

#### `set/get`

它们按字节偏移访问：

```java
segment.set(ValueLayout.JAVA_INT, 0, 42);
int x = segment.get(ValueLayout.JAVA_INT, 0);
```

这里的 `0` 表示字节偏移量。

#### `setAtIndex/getAtIndex`

它们按“数组下标”访问：

```java
segment.setAtIndex(ValueLayout.JAVA_INT, 3, 42);
int x = segment.getAtIndex(ValueLayout.JAVA_INT, 3);
```

这里的 `3` 表示第 3 个 `int` 元素，而不是第 3 个字节。

你可以理解成：

- `set/get` 更像底层内存操作
- `setAtIndex/getAtIndex` 更像数组操作

### 第二段可运行代码：按偏移读写

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Step2Offsets {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(16);

            segment.set(ValueLayout.JAVA_INT, 0, 100);
            segment.set(ValueLayout.JAVA_INT, 4, 200);
            segment.set(ValueLayout.JAVA_LONG, 8, 9999L);

            int a = segment.get(ValueLayout.JAVA_INT, 0);
            int b = segment.get(ValueLayout.JAVA_INT, 4);
            long c = segment.get(ValueLayout.JAVA_LONG, 8);

            System.out.println(a);
            System.out.println(b);
            System.out.println(c);
        }
    }
}
```

这里你已经开始按“内存布局”思考问题了：

- 0 到 3 字节放一个 `int`
- 4 到 7 字节放一个 `int`
- 8 到 15 字节放一个 `long`

这比数组更接近真实的内存编程。

### 字符串：Java 字符串和 C 字符串

native world 里很常见的是 C 风格字符串，也就是：

- UTF-8 字节
- 末尾通常带 `\0`

FFM 为这种场景准备了方便的 API。

### 第三段可运行代码：分配一个 C 字符串

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class Step3CString {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cString = arena.allocateFrom("hello ffm");
            String javaString = cString.getString(0);

            System.out.println(javaString);
        }
    }
}
```

这里发生了两件事：

- `allocateFrom("hello ffm")` 把 Java 字符串放进 native memory
- `getString(0)` 从偏移 0 开始按 C 字符串规则读回来

这是你以后调用 native function 时最常见的辅助操作之一。

### 切片：`slice`

很多时候你不想复制内存，只想从一大块内存里看其中一段。

这时候就会用到切片。

### 第四段可运行代码：切出子数组视图

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Step4Slice {
    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(5L * Integer.BYTES);

            for (int i = 0; i < 5; i++) {
                segment.setAtIndex(ValueLayout.JAVA_INT, i, i + 1);
            }

            MemorySegment tail = segment.asSlice(2L * Integer.BYTES, 3L * Integer.BYTES);

            for (int i = 0; i < 3; i++) {
                System.out.println(tail.getAtIndex(ValueLayout.JAVA_INT, i));
            }
        }
    }
}
```

输出会是：

```text
3
4
5
```

切片的重点不是“新建一块内存”，而是“新建一个视图”。

### 生命周期：故意踩一次坑

如果你不真正理解 arena 生命周期，后面写 FFM 很容易出错。

请看下面这个例子：

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Step5Lifetime {
    public static void main(String[] args) {
        MemorySegment segment;

        try (Arena arena = Arena.ofConfined()) {
            segment = arena.allocate(8);
            segment.set(ValueLayout.JAVA_INT, 0, 123);
        }

        System.out.println(segment.get(ValueLayout.JAVA_INT, 0));
    }
}
```

这段代码的问题是：

- `segment` 的 backing memory 属于 `arena`
- 离开 `try` 后 `arena` 已关闭
- `segment` 已经无效

你应该把它记成一句话：

> segment 可以离开变量作用域继续存在，但不能离开 arena 生命周期继续合法使用。

### 结构化内存：什么是 `MemoryLayout`

前面的例子都是手写偏移量。

这在简单场景下没问题，但一旦你要映射真实结构体，就应该用 `MemoryLayout`。

你可以把 `MemoryLayout` 理解成“内存说明书”。

例如 C 里的结构体：

```c
struct Point {
    int x;
    int y;
};
```

在 FFM 中，你可以显式描述它的布局，而不是靠魔法偏移硬编码。

### 第五段可运行代码：定义并使用 `Point`

```java
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class Step6PointStruct {
    static final MemoryLayout POINT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("x"),
        ValueLayout.JAVA_INT.withName("y")
    );

    static final long X_OFFSET = POINT.byteOffset(groupElement("x"));
    static final long Y_OFFSET = POINT.byteOffset(groupElement("y"));

    public static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment point = arena.allocate(POINT);

            point.set(ValueLayout.JAVA_INT, X_OFFSET, 10);
            point.set(ValueLayout.JAVA_INT, Y_OFFSET, 20);

            int x = point.get(ValueLayout.JAVA_INT, X_OFFSET);
            int y = point.get(ValueLayout.JAVA_INT, Y_OFFSET);

            System.out.println("x=" + x + ", y=" + y);
            System.out.println("POINT size = " + POINT.byteSize());
        }
    }
}
```

这段代码代表你已经迈进了“会描述结构化内存”的阶段。

这里最重要的不是 API 名字，而是思维方式的变化：

- 不再用裸偏移硬编码结构
- 改为先定义 layout
- 再从 layout 推导 offset

### 为什么 `MemoryLayout` 很重要

不用 layout 时，你可能会写：

```java
segment.set(ValueLayout.JAVA_INT, 0, 10);
segment.set(ValueLayout.JAVA_INT, 4, 20);
```

这种写法的问题是：

- 0 为什么是 `x`
- 4 为什么是 `y`
- 结构变了怎么办
- 有 padding 时怎么办

用了 `MemoryLayout` 后：

- 结构定义是显式的
- 偏移从布局里推导
- 代码更接近真实 native data model

### 什么时候再去学 `VarHandle`

`MemoryLayout` 还能生成 `VarHandle`，做更高层的字段访问。

但对初学者来说，不要一上来就学它。先把这三步练熟：

- `structLayout`
- `byteOffset`
- `segment.get/set`

等你对布局已经不陌生，再去学 `varHandle(...)` 会顺很多。

### 开始进入 native function 调用

到这里，你已经会：

- 分配 off-heap memory
- 管理 arena 生命周期
- 读写基本类型
- 表示结构体

现在才适合开始学 foreign function call。

这部分的核心对象有三个：

- `SymbolLookup`
- `FunctionDescriptor`
- `Linker`

你可以这样理解：

- `SymbolLookup`：去 native library 里找函数地址
- `FunctionDescriptor`：描述函数签名
- `Linker`：把地址和签名变成 Java 可调用对象

最终产物通常是一个 `MethodHandle`。

### 第六段可运行代码：调用 `strlen`

这是最经典的 FFM foreign function 入门例子。

```java
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class Step7Strlen {
    public static void main(String[] args) throws Throwable {
        Linker linker = Linker.nativeLinker();

        MethodHandle strlen = linker.downcallHandle(
            linker.defaultLookup().findOrThrow("strlen"),
            FunctionDescriptor.of(JAVA_LONG, ADDRESS)
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cString = arena.allocateFrom("hello ffm");
            long len = (long) strlen.invokeExact(cString);
            System.out.println(len);
        }
    }
}
```

编译：

```bash
javac Step7Strlen.java
```

运行：

```bash
java --enable-native-access=ALL-UNNAMED Step7Strlen
```

#### 这段代码做了什么

##### `Linker.nativeLinker()`

获取当前平台的 native linker。

##### `defaultLookup().findOrThrow("strlen")`

在默认查找范围里寻找 `strlen` 符号。

##### `FunctionDescriptor.of(JAVA_LONG, ADDRESS)`

描述签名：

- 返回值是 `long`
- 参数是一个地址

##### `downcallHandle(...)`

把“函数地址 + 函数签名”链接成 Java 里的 `MethodHandle`。

##### `invokeExact(cString)`

真正调用 C 函数。

### 为什么调用 native function 要额外小心

FFM 的内存 API 安全性很强，但函数调用那半边没有那么“自动安全”。

原因是：

- JVM 能检查 segment 是否越界
- 但 JVM 无法完全证明你写的函数签名一定和真实 native 函数一致

如果签名写错，后果可能是：

- 返回值异常
- 数据错乱
- JVM 崩溃

所以 foreign function 调用相关 API 被归类为 restricted methods。运行时通常需要：

```bash
--enable-native-access=ALL-UNNAMED
```

### 为什么 `strlen` 例子不是完全跨平台万能

入门教程常用 `strlen`，是因为它直观。

但你要知道：

- 它依赖底层平台能在默认查找范围里找到这个符号
- 常见 Unix-like 平台通常更顺手
- 某些平台上默认符号或库路径可能不同

这不影响你理解 FFM 的调用模型，但会影响示例是否开箱即跑。

### FFM 和 JNI、`Unsafe`、DirectByteBuffer 的关系

#### FFM vs JNI

JNI 的特点是：

- Java 一套
- C/C++ 一套
- 两边靠胶水代码拼起来

FFM 的特点是：

- 主要在 Java 里描述 native memory 和 native function
- 样板代码更少
- 维护更直接

#### FFM vs `Unsafe`

`Unsafe` 更像一把危险但强大的底层工具。

FFM 的定位则是：

- 覆盖其中一大块 foreign memory / native interop 场景
- 用更结构化的 API 替代手写地址运算

#### FFM vs direct buffer

direct buffer 更偏“buffer 视角”。

FFM 更偏：

- 真正的 native memory
- 带布局的内存结构
- native function interop

### 最容易踩的坑

#### 1. arena 关了，segment 还在用

这是最常见问题。

记住：

- segment 的变量还在，不等于内存还活着
- arena 死了，segment 就不能再合法访问

#### 2. 用了 `ofConfined()` 却跨线程访问

`ofConfined()` 的好处是简单清晰，但代价就是只能由 owner thread 访问。

#### 3. 以为 off-heap 一定更快

off-heap 的常见价值是：

- 降低 heap 占用
- 降低 GC 压力
- 更适合 native interop

但它不是“自动更快”的同义词。

#### 4. 把函数签名写错

这是 foreign function 调用里最危险的问题。

内存 API 的保护比函数调用 API 强得多。

#### 5. 一开始就想学全部 API

正确方式是分阶段：

1. `Arena + MemorySegment`
2. `ValueLayout`
3. `MemoryLayout`
4. `Linker`

别一开始就把 `VarHandle`、复杂 ABI、upcall、jextract 全塞进脑子里。

### 真正的学习顺序

如果你要从“会看”走到“会写”，建议按下面顺序练习：

1. 写出 `Step1IntArray`
2. 写出 `Step2Offsets`
3. 写出 `Step3CString`
4. 写出 `Step5Lifetime`
5. 写出 `Step6PointStruct`
6. 写出 `Step7Strlen`

如果这 6 个你能不看答案自己写出来，说明你已经不是 FFM 小白了。

### 给自己的 3 个模板

#### 模板一：分配一段 off-heap 数组

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment seg = arena.allocate(n * Integer.BYTES);
    for (int i = 0; i < n; i++) {
        seg.setAtIndex(ValueLayout.JAVA_INT, i, 0);
    }
}
```

#### 模板二：定义结构体

```java
static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT.withName("a"),
    ValueLayout.JAVA_LONG.withName("b")
);
```

#### 模板三：调用 native function

```java
Linker linker = Linker.nativeLinker();

MethodHandle fn = linker.downcallHandle(
    linker.defaultLookup().findOrThrow("some_function"),
    FunctionDescriptor.of(/* return layout */, /* arg layouts */)
);
```

### 学到这里，你应该已经会什么

如果你能看懂并改写本文中的示例，你应该已经具备这些能力：

- 理解 FFM 的核心目标
- 自己分配和释放 off-heap memory
- 按基本类型读写 `MemorySegment`
- 用 `MemoryLayout` 表达简单结构体
- 理解 arena 生命周期
- 调用一个简单的 native function
- 知道最常见的危险点在哪里

这已经足够进入真实项目里的 FFM 代码阅读阶段。

### 接下来该学什么

如果你要继续深入，通常有两条路线：

#### 路线 A：off-heap 数据结构

继续学：

- `sequenceLayout`
- `VarHandle`
- 更复杂的布局
- 文件映射

适合：

- 存储
- 缓存
- 高性能数据结构

#### 路线 B：native interop

继续学：

- `libraryLookup(...)`
- 更复杂的 `FunctionDescriptor`
- `upcall`
- `jextract`

适合：

- 调系统库
- 调 C/C++ 库
- 写跨语言绑定

### 官方资料

下面这些是最值得看的官方材料：

- JEP 454
  - `https://openjdk.org/jeps/454`
- Oracle FFM 总览
  - `https://docs.oracle.com/en/java/javase/25/core/foreign-function-and-memory-api.html`
- `java.lang.foreign` 包文档
  - `https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/package-summary.html`
- `Arena`
  - `https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Arena.html`
- `MemoryLayout`
  - `https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/MemoryLayout.html`
- `Linker`
  - `https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Linker.html`
- Restricted Methods
  - `https://docs.oracle.com/en/java/javase/25/core/restricted-methods.html`

### 最后再压缩成一句话

FFM 可以理解成：

> Java 官方提供的一套 API，让你用更现代、更结构化的方式操作 native memory 和 native function，同时尽量保留 Java 风格的生命周期管理和安全边界。

如果你看完本文还想继续走下一步，最自然的顺序就是：

1. 自己把本文 6 个示例重新敲一遍
2. 接着阅读本文第二部分，理解 Yierdis 里的实际落点
3. 最后回到 Yierdis 代码里对照 `YierdisFfmMemoryRuntime`、`YierdisFfmRegion`、`YierdisForeignOffHeapAllocator`、`NativeKeyDirectory`、`EntryTable` 和各类型 `*Root` 去读实现

## 第二部分：FFM 在 Yierdis 里的用法

本部分整理 Yierdis 当前是如何使用 JDK 25 `java.lang.foreign` FFM API 的。

如果只关心“哪些路径会发生 heap / off-heap 拷贝”，请优先看 [`offheap-copy-behavior.md`](./offheap-copy-behavior.md)。
本部分关注的是更上层的问题：FFM 在项目里扮演什么角色、从启动到 DB 内部是怎么接起来的、哪些数据真的放进了 native memory、以及生命周期和泄漏检查是如何工作的。

### 先说结论

Yierdis 里的 FFM 主要被当作统一的 native-memory substrate 来用，而不是用来调用 native function。

- 代码里实际使用的是 `Arena`、`MemorySegment`、`ValueLayout`
- 没有看到 `Linker`、`SymbolLookup`、`downcallHandle` 这类 native function 调用链
- FFM 在这里的核心职责是承载 off-heap bytes、off-heap table metadata、以及对这些内存块的生命周期管理

代表路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmAccess.java`

### 启动和组装

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

### FFM 基础层：Runtime / Region / Span / Access

Yierdis 对 FFM 做了一层很薄的封装，核心对象有四个：

- `YierdisFfmMemoryRuntime`
- `YierdisFfmRegion`
- `YierdisFfmSpan`
- `YierdisFfmAccess`

#### `YierdisFfmMemoryRuntime`

`YierdisFfmMemoryRuntime.allocateRegion(owner, bytes)` 每次分配都会：

1. 创建一个 `Arena.ofConfined()`
2. 从这个 arena 中 `allocate(bytes)` 得到 `MemorySegment`
3. 用 `YierdisFfmRegion` 把 arena + segment 包起来
4. 把 region 记入 `liveRegions`
5. 把 bytes 累加到 `usedBytes`

这意味着它不是“整个实例只有一个大 arena”，而是“每个 region 自己拥有一个 confined arena”。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`

#### `YierdisFfmRegion`

`YierdisFfmRegion` 是一个带 owner 名称的 native memory block，内部持有：

- `Arena`
- `MemorySegment`
- `size`
- `runtime`

`span(offset, length)` 会返回一个切片后的 `YierdisFfmSpan`。`close()` 会直接关闭底层 arena，然后通知 runtime 做 accounting 回收。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`

#### `YierdisFfmSpan`

`YierdisFfmSpan` 只是 `MemorySegment` 的轻量 view，负责表示某个 region 的一个切片，不单独拥有内存。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSpan.java`

#### `YierdisFfmAccess`

`YierdisFfmAccess` 把所有基础读写都收口到了一个地方，避免业务层直接碰 `MemorySegment`：

- `getByte` / `setByte`
- `getInt` / `setInt`
- `getLong` / `setLong`
- `getBytes` / `setBytes`
- `asByteBuffer`

这里实际使用的是 `ValueLayout.JAVA_BYTE`、`JAVA_INT_UNALIGNED`、`JAVA_LONG_UNALIGNED`。

对应路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmAccess.java`

### 两条上层接入路径

`YierdisDb` 拿到 `YierdisFfmMemoryRuntime` 后，实际上分出两条不同的使用路径。

#### 路径一：`OffHeapAllocator` / `OffHeapBuf`

这条路径主要服务于 string 和 HLL 这种“连续字节缓冲”。

`YierdisForeignOffHeapAllocator.allocate(capacity)` 会：

1. 先检查 allocator 是否关闭，以及 `maxBytes` hard cap
2. 委托内部的 `YierdisFfmSlabAllocator` 从 slab 中分配一段连续空间
3. 把 slab allocation 包装成 `OffHeapBuf`
4. 用 allocator 自己的 `usedBytes` 做逻辑 payload accounting

`OffHeapBuf.close()` 最终会：

1. 释放底层 slab block
2. 通知 allocator 扣减字节数
3. 如果 allocator 当前没有任何 live buffer，则关闭 idle slab allocator 并重建一个空 allocator，从而把空闲 slab 对应的 FFM region 还给 runtime

它还支持 `slice(index, len)` 返回 `OffHeapSlice`，让读取路径可以直接把 off-heap 内容暴露给上游，而不必先 materialize 成 `byte[]`。

代表路径：

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlab.java`

#### 路径二：`YierdisFfmBlobStore` / `YierdisFfmBytesRef`

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

### 堆外内存管理的三层模型

从实现上看，Yierdis 的堆外内存不是“每个 value 一个 direct buffer”，也不是“进程里维护一个大 native heap”。它更像三层组合：

1. FFM 原始内存层：`YierdisFfmMemoryRuntime` / `YierdisFfmRegion` / `YierdisFfmSpan` / `YierdisFfmAccess`
2. allocator 层：`YierdisFfmSlabAllocator` / `YierdisFfmSlab` / `YierdisForeignOffHeapAllocator`
3. DB 对象生命周期层：`NativeKeyDirectory` / `EntryTable` / `YierdisFfmBlobStore` / `YierdisFfmExpireIndex` / 各类型 `*Root`

#### 第一层：FFM region 是真实 native memory owner

`YierdisFfmMemoryRuntime` 是实例级 native memory runtime。它负责：

- 分配 region
- 跟踪 live regions
- 维护 `usedBytes`
- 在 `close()` 时做 leak check

每次 `allocateRegion(owner, bytes)` 都会创建新的 `Arena.ofConfined()`，再从这个 arena 分配一个 `MemorySegment`，最后包成 `YierdisFfmRegion`。

`YierdisFfmRegion` 才是某一块 native memory 的 owner。它内部持有：

- `Arena`
- `MemorySegment`
- `size`
- `runtime`

region 的 `span(offset, length)` 返回的是切片 view；`close()` 会关闭底层 arena，并通过 `runtime.onRegionClosed(...)` 从 runtime accounting 中扣掉对应 bytes。

`YierdisFfmSpan` 不拥有内存，只是 `MemorySegment` 的轻量 view。`YierdisFfmAccess` 把 byte/int/long/byte array 的读写收口起来，避免业务层到处直接操作 `MemorySegment`。

这层的核心约束是：谁分配 region，谁最终必须 close region。runtime 不会在正常关闭时主动帮你清理 live regions；如果还有 live region，它会直接报 leak。

#### 第二层：slab allocator 管理连续 buffer

`YierdisFfmSlabAllocator` 在 FFM region 上再做 slab allocation。

默认 slab 大小是 64 KiB。分配时它会：

1. 检查 `capacity`、关闭状态和 `maxBytes`
2. 在现有 slabs 的 free blocks 中找可用空间
3. 找不到就新建 `YierdisFfmSlab`
4. 从 slab 中切出一段 allocation
5. 返回 slab-backed `OffHeapBuf`

`YierdisFfmSlab` 内部维护 free block list。释放时把 block 加回 free list，按 offset 排序，再合并相邻空闲块。

这里有两个不同的 accounting 口径：

- `YierdisFfmSlabAllocator.usedBytes()`：已经分配给 live `OffHeapBuf` 的逻辑容量
- runtime 的 `usedBytes()`：底层 FFM region 真实还活着的 reserved native bytes

例如一个 64 KiB slab 里只分配了 16 bytes，allocator 的 `usedBytes()` 可能是 16，但 runtime 看到的 live region 是整个 slab。

`YierdisForeignOffHeapAllocator` 是对外暴露的 `OffHeapAllocator`。它进一步维护自己的 `usedBytes` 和 `maxBytes`，并在所有 live buffer 都释放后关闭 idle slab allocator，把空闲 slab region 释放掉。

#### 第三层：DB 对象用 handle 和 refcount 管生命周期

DB 层不会把 Java 对象直接塞进 native memory，而是用 handle 串起来：

- `KeyHandle` 表示 key identity
- `EntryHandle` 指向 `EntryTable` 中的 entry slot
- `ValueHandle` 指向某个 type root 中的 payload
- `YierdisFfmBytesRef` 表示一段 off-heap bytes

`NativeKeyDirectory` 是 key -> `EntryHandle` 的权威目录。它的 table 数组仍在 heap，但 key bytes 存在 `YierdisFfmBlobStore` 中。插入新 key 时，directory 会调用 `blobStore.store(keyBytes)`；删除 key 时，会调用 `blobStore.release(keyRef)`。

`EntryTable` 是 `EntryHandle` -> `EntryRecord`。当前生产组装里 entry slot 来自 `YierdisFfmSlabAllocator`，每条 record 逻辑大小是 56 bytes，字段包括：

- key handle identity
- value handle
- key hash
- type
- encoding
- flags
- expire time
- version / estimated bytes
- LRU / LFU 信息

`YierdisFfmBlobStore` 管理离散 bytes。`store(byte[])` 分配 region、拷贝 bytes、返回 `YierdisFfmBytesRef`，并建立 refcount。`retain(ref)` 增加引用，`release(ref)` 减少引用；最后一次 release 会关闭底层 region。

`YierdisFfmExpireIndex` 复用 keyspace 中同一份 key bytes。设置 TTL 时，它会从 `KeyHandle` 取出底层 `YierdisFfmBytesRef` 并 `retain(ref)`，TTL 删除或清理时再 `release(ref)`。因此 expires 不会为了 TTL 再复制一份 key bytes。

expire table 本身也部分 off-heap：

- `states` 在 FFM region
- `hashes` 在 FFM region
- `expireAt` 在 FFM region
- `refs[]` 仍在 heap，保存 `YierdisFfmBytesRef`

#### 写入、替换和删除的释放链路

写入时，命令通常先进入 `YierdisDbMutationExecutor`：

1. `YierdisDbMemoryLedger.reserve(estimatedExtraBytes)` 做 maxmemory 预算检查
2. 真正执行 mutation，期间可能分配 off-heap buffer、blob 或 entry slot
3. 成功后 `commit(reservation, actualDeltaBytes)`
4. 如果 maxmemory 或 off-heap hard cap 失败，rollback reservation

key 创建和替换由 `YierdisDbKeyLifecycle.computeWithHandle(...)` 收口：

1. 先准备或查找 `KeyHandle`
2. 新 entry 通过 `EntryTable.allocate(...)` 拿到 `EntryHandle`
3. `NativeKeyDirectory.compute(...)` 把 key bytes 存进 blob store，并映射到 entry handle
4. `EntryRecord.valueHandle()` 指向 `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 内部 payload

删除 key 时，释放链路是反向的：

1. 从 `NativeKeyDirectory` 移除 key，并 release key blob
2. 从 `EntryTable` release entry slot
3. 根据 `EntryRecord.type()` 调用对应 root 的 `release(valueHandle)`
4. TTL 路径如果 retain 过同一份 key ref，也会在 remove / clear 时 release

替换 value 时，`releaseReplacedValue(...)` 会检查新旧 type 和 `ValueHandle`。如果覆盖路径复用了原 `ValueHandle`，不会误释放旧 payload；如果换成了新的 handle，才释放旧 payload。

#### String 和 HLL 的连续 buffer 管理

字符串值由 `StringRoot` 管理，不走 `YierdisFfmBlobStore`。

`StringRoot.store(...)` 会通过 `OffHeapAllocator` 分配 `OffHeapBuf`，把 value bytes 写进去，然后用 `ValueHandle` 暴露给 `EntryRecord`。

覆盖时有一个重要优化：

- 如果旧 buffer capacity 足够容纳新 value，就原地覆盖
- 如果不够，才分配新 buffer，并关闭旧 buffer

这避免了 `SET` 覆盖路径在 maxmemory 很紧时临时同时持有 old + new 两份 payload。

`StringRoot.slice(...)` 返回 `OffHeapSlice`。读路径如果支持流式写出，就可以直接把 off-heap slice 写到上游 sink，而不是先复制成 heap `byte[]`。

这里的“直接”指 DB 读接口层不需要先 materialize 成 `byte[]`。当前 RESP 写出链路仍会调用
`BytesSlice.writeTo(...)`，由 slice 实现通过 `ThreadLocal<byte[]>` scratch buffer 分块复制到
sink，并不是 native memory 到 socket 的真正零拷贝。

HLL 逻辑上复用 string 存储路径，因此 HLL bytes 也可以落在 `StringRoot` 管理的 `OffHeapBuf` 中。

#### 复合结构的 off-heap 边界

List / Hash / Set / ZSet 的 root 负责 `ValueHandle` -> value adapter：

- `ListRoot` 管理 `ListValue`
- `HashRoot` 管理 `HashValue`
- `SetRoot` 管理 `SetValue`
- `ZSetRoot` 管理 `ZSetValue`

adapter 内部再使用 FFM-backed primitive：

- `YierdisFfmListpack`：entry bytes 存在 blob store，外层 entry list 仍是 heap `ArrayList`
- `YierdisFfmByteMap`：member / field bytes 存在 blob store，table 索引数组仍在 heap
- `YierdisFfmIntSet`：整数集合是更纯的 native long array
- `YierdisFfmZSet`：zset member bytes 存在 blob store，排序列表和分数仍主要在 heap

所以这里的原则是：把大块或重复的 bytes 尽量移到 off-heap，把复杂控制结构保留在 heap，以降低实现复杂度。

### 字符串路径：FFM 如何进入 `SET` / `GET`

字符串值不走 `YierdisFfmBlobStore`，而是由 `StringRoot` 管理。
`StringRoot` 内部使用 `OffHeapAllocator` 分配连续 buffer，并用
`ValueHandle` 暴露给 entry 元数据。

#### 写入

`YierdisStringOps.set(...)` 最终会调用：

- `StringRoot.store(...)` 创建新的 string payload
- `YierdisDbKeyLifecycle.newRecord(...)` 创建新的 `EntryRecord`

真正的字符串 bytes 落在 `StringRoot` 管理的 `OffHeapBuf` 里。
写入完成后，`YierdisDbKeyLifecycle` 会把 key 同步到
`NativeKeyDirectory` 和 `EntryTable`，`EntryRecord` 里保存
type、encoding、value handle、TTL 和估算字节数。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`

#### 覆盖和扩容

如果旧 entry 也是 string，覆盖路径会保留原 `ValueHandle` 并调用
`StringRoot.overwrite(...)`。这个路径保留了关键优化：

- 如果旧 handle 指向的 `OffHeapBuf` 容量足够容纳新值

那么它会直接复用原 buffer，就地改写内容，而不是“先分配新 buffer，再释放旧
buffer”。

这么做的目的很明确：在 `maxmemory` 有硬预算时，避免 SET 覆盖路径临时同时持有 old + new 两份 off-heap 内存。

如果容量不够，则会重新分配新 buffer，并把旧内容做 off-heap -> off-heap 复制。
`EntryRecord` 的 estimate 会随后刷新，delete、expire 和 eviction 路径不再依赖
旧对象估算值。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`

#### 读取

`GET` 的读路径会先解析 live entry，再通过当前 value handle 访问
`StringRoot`：

- 可流式输出时返回 `BulkStringValue.slice(slice)`，底层直接指向 `OffHeapSlice`
- 需要 materialize 时才复制成 heap `byte[]`

这就是字符串路径里避免 DB 层 `byte[]` materialization 的读优化。它不是端到端
native-to-socket 零拷贝：当前 `YierdisSlabBackedOffHeapSlice.writeTo(...)` 会用
8 KiB `ThreadLocal<byte[]>` scratch buffer 从 `MemorySegment` / `ByteBuffer` 分块读出，
再调用 `BytesSink.writeBytes(...)` 写入下游 sink。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`

#### HLL

HLL 并没有单独设计一套 native payload 类型，而是复用了 string root 的
off-heap 存储路径。也就是说：

- HLL 逻辑上是一个特殊的 string
- HLL bytes 也可以存在 `StringRoot` 管理的 `OffHeapBuf` 里
- HLL 的 sparse / dense 编码、register 计算、估算和 merge 仍是 Java heap 逻辑
- 最终只是把 HLL 格式化后的 bytes 作为 string payload 落到 off-heap

`PFADD` 对 dense HLL 可以通过 `StringRoot.byteAt(...)` / register 写入接口原地更新
底层 string payload；sparse HLL 则会把现有内容复制出来，在 heap 上重建 sparse 或 dense
bytes 后再 overwrite。`PFCOUNT` / `PFMERGE` 使用 `StringRoot.slice(handle)` 读取 HLL
bytes 并把它 merge 到 heap `int[] registers`，所以 HLL 不是 native algorithm，只是 payload
storage off-heap。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java`

### Keyspace 路径：key 如何放进 FFM

keyspace 是 FFM 使用最核心的部分之一。

当前 DB 只保留 native key directory 作为 key -> entry 的权威索引：

- `NativeKeyDirectory` 保存 native entry graph 的 key -> `EntryHandle`
- `EntryTable` 保存 `EntryHandle` -> `EntryRecord`

`NativeKeyDirectory.compute(...)` 在 key 首次出现时会：

1. 通过 `blobStore.store(key)` 把 key bytes 存进 native memory
2. 基于这个 blob 创建 `KeyHandle.forFfm(ref, hash)`
3. 把 key ref 和 `EntryHandle` 放进 native directory table

mutation 由 `YierdisDbKeyLifecycle.computeWithHandle(...)` 收口：

1. `NativeKeyDirectory` 存储 key bytes
2. `EntryTable` 分配、替换或释放 `EntryRecord`
3. `EntryRecord.valueHandle()` 指向对应 `TypeRoot` 里的 payload

之后 DB 内部很多路径都围绕 `KeyHandle`、`EntryHandle` 和 `ValueHandle`
传递 identity，而不是不断回到新的 heap `byte[]` 或旧对象容器。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java`

`YierdisFfmKeyspace<V>` 仍然保留在 `internal/ffm` 包里，但它不是当前生产 DB 的主索引。生产路径使用 `NativeKeyDirectory + EntryTable + EntryRecord`；`YierdisFfmKeyspace<V>` 更像一个低层 FFM keyspace primitive，当前主要由 rehash consistency 测试覆盖。

### Expire 路径：TTL 如何复用同一份 off-heap key

`YierdisFfmExpireIndex` 并不会为了 TTL 再复制一份 key bytes。

当调用 `setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis)` 时，它会：

1. 从 handle 中取出底层的 `YierdisFfmBytesRef`
2. 对这块 ref 执行 `blobStore.retain(ref)`
3. 把同一个 ref 放进 expires table

也就是说，keyspace 和 expires 共享同一份 off-heap key bytes，只是通过引用计数协调生命周期。

`YierdisFfmExpireIndex` 自己的 table 也是渐进 rehash 的 open-addressing 结构：

- `table0` 是当前主表，`table1` 是 rehash 目标表
- 插入前如果 `used + 1` 超过 `capacity * 0.75`，会启动扩容 rehash
- 删除后如果表过稀，或 tombstone 超过 `capacity / 4`，会启动 shrink / compact rehash
- 每次 `get` / `setExpireAtMillis` / `removeExpire` / scan / random 等操作都会推进一个 `rehashStep()`
- 每个 step 只迁移一个 filled slot，迁完后 `finishRehash()` 把旧 `table0` 放进 `retiredTables`
- 后续操作开头会调用 `closeRetiredTables()`，延迟关闭旧 table 持有的 FFM regions

table 里只有 `states`、`hashes`、`expireAt` 三组数组在 FFM region；`refs[]` 仍是 heap
数组，保存共享 key bytes 的 `YierdisFfmBytesRef`。

设置或移除 TTL 时，`YierdisDbKeyLifecycle` 还会同步更新 `EntryRecord.expireAtMillis`，
让过期、introspection 和 memory 路径都能从 entry metadata 看到同一份状态。

这个行为有专门测试覆盖。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireKeySharingTest.java`

### 复合结构：Hash / List / Set / ZSet

在复合结构里，命令 hot path 现在先定位 `EntryRecord.valueHandle()`，再进入
对应 root：

- `HashRoot`
- `ListRoot`
- `SetRoot`
- `ZSetRoot`

这些 root 仍复用 `HashValue`、`ListValue`、`SetValue`、`ZSetValue` 作为内部
adapter，但 key 的 canonical metadata 在 `EntryTable`，value identity 是
`ValueHandle`。总体思路是一致的：把成员 bytes 尽量放到 off-heap，把部分索引元数据也放到 off-heap，并在流式读路径里优先暴露 `OffHeapSlice` 风格接口。

#### 编码阈值和转换条件

复合类型不是一写入就进入“大结构”编码，而是先用 packed / intset 形态，超过阈值后再转换。
这些阈值集中在 `YierdisEncodingThresholds`，当前不是用户可配置项：

- Hash：packed listpack 最多 512 个 field；新增 field 前如果已达到 512，或 field / value 任一超过 64 bytes，就转换到 `YierdisFfmByteMap<YierdisFfmBytesRef>`
- List：packed listpack 的估算编码大小超过约 8 KiB 时，转换到 FFM quicklist-like nodes；批量 push 会先预测是否越过这个阈值
- Set：先走 `YierdisFfmIntSet`；整数集合超过 512 个元素，或出现非 canonical integer member，就转换到 `YierdisFfmByteMap<Object>`
- ZSet：packed zset 超过 128 个 member，或新增 member 超过 64 bytes，就转换到 skiplist-mode；FFM 路径里的这个 mode 实际是增加 member lookup map

转换通常是单向的：超过阈值后进入 hash table / quicklist / skiplist-mode，不因为后续删除而自动降回 packed。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisEncodingThresholds.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`

#### Hash

`HashRoot` 通过 `ValueHandle` 管理内部 `HashValue` adapter，adapter 用：

- `YierdisFfmBlobStore`
- `YierdisFfmListpack`
- `YierdisFfmByteMap<YierdisFfmBytesRef>`

来保存 field/value。

`HGETALL` 的流式写回路径可以直接输出 `YierdisFfmBytesRefSlice`，无需先拼成 `List<byte[]>`。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`

#### List

`ListRoot` 通过 `ValueHandle` 管理内部 `ListValue` adapter，adapter 使用：

- `YierdisFfmListpack`
- FFM 版 quicklist-like 节点结构

来保存 list element bytes。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmListpack.java`

#### Set

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

#### ZSet

`ZSetRoot` 通过 `ValueHandle` 管理内部 `ZSetValue` adapter，adapter 内部使用
`YierdisFfmZSet`。这里的名字容易造成误解：它不是完整 native skiplist。

`YierdisFfmZSet` 的真实结构是：

- `ordered = ArrayList<Entry>` 仍在 heap，用来按 score / member 字典序维护顺序
- `Entry.memberRef` 指向 blob store 里的 off-heap member bytes
- `Entry.score` 是 Java `double`
- packed mode 下查 member 是线性扫描 `ordered`
- 超过 128 个 member 或新增 member 超过 64 bytes 后，会创建 `YierdisFfmByteMap<Entry> byMember` 做 member lookup
- `ValueEncoding.ZSET_SKIPLIST` 在这条 FFM 路径里表示“有 byMember map 的大编码”，不是说排序索引主体已经变成 native skiplist

因此 ZSet 的 off-heap 收益主要来自 member bytes 和流式输出，排序主体仍由 heap
`ArrayList<Entry>` 承载。

在 `ZRANGE` / `ZRANGEBYSCORE` 这种输出路径里，member 可以直接作为 `YierdisFfmBytesRefSlice` 发送给 `BulkStringSink`，因此它也支持 off-heap 流式读取。
不过和 string 一样，当前 RESP sink 侧仍是 scratch buffer 分块写出，不是 native-to-socket 零拷贝。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmZSet.java`

### “FFM-backed” 不等于“所有东西都在 native memory”

这点很重要。

项目里很多结构可以称为 “FFM-backed”，但这不等于它们的所有内部数组都已经搬进 native memory。

例如：

- `EntryTable` 的 entry slots 来自 slab allocator，但 `EntryHandle` 本身是 Java record
- `NativeKeyDirectory` 的 key bytes 在 native blob store，table 数组仍在 heap
- `YierdisFfmExpireIndex` 的 `states` / `hashes` / `expireAt` 在 native memory，但 `refs[]` 仍在 heap
- `YierdisFfmZSet` 的 member bytes 在 native blob store，但 `ordered` 和 `Entry.score` 仍在 heap
- `StringRoot` 管理 off-heap buffer，但 handle -> slot map 仍在 heap
- `HashRoot` / `ListRoot` / `SetRoot` / `ZSetRoot` 的 handle table 仍在 heap，payload 通过各 value adapter 进入 FFM-backed 结构
- `YierdisFfmByteMap` 的 table 索引数组本身仍在 heap，只是 key bytes 放在 off-heap
- `YierdisFfmListpack` 本身是 `ArrayList<YierdisFfmBytesRef>`，真正 off-heap 的是 entry bytes

保留的低层 `YierdisFfmKeyspace<V>` 也使用 `table0` / `table1` 的渐进 rehash 模型，
但它不是生产 DB 的主 key directory。它和 `YierdisFfmExpireIndex` 一样，插入可能触发扩容，
删除 / tombstone 过多可能触发 shrink / compact，每次操作推进一个 rehash step，旧 table
进入 `retiredTables` 后再关闭 FFM regions。

所以更准确的描述应该是：

- 关键字节数据大量 off-heap 化
- 部分索引元数据 off-heap 化
- key、entry metadata 和 value payload 都通过 64-bit handle 串起来
- 但并不是“整个 DB 内部结构完全 native 化”

### 为什么这里可以放心使用 `Arena.ofConfined()`

这个设计依赖项目的单线程 DB 语义。

`DbThreadGuard` 强制每个 `YierdisDb` 必须显式绑定到唯一 owner thread。未绑定访问或跨线程访问都会直接失败。

server 启动时，`CommandExecutor` 会先调用 `runtimeAccess::bindToCurrentThread`，把 DB 绑定到命令执行线程。后台 maintenance 虽然是由 worker event loop 定时触发，但真正的 cleanup / maxmemory enforcement 会通过 `executeMaintenance(...)` 回到同一个 command executor 线程执行。

这使得项目可以把大部分 FFM 内存都建立在 `Arena.ofConfined()` 上，而不必为了并发共享去设计更复杂的 arena 同步策略。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/DbThreadGuard.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`

### 内存统计、maxmemory 和泄漏检查

FFM 内存不是一个“统计旁路”，而是明确进入内存治理体系的一部分。

#### maxmemory

`YierdisDbMemoryReporter.usedBytesForMaxmemory()` 会把：

- `ledger.usedBytes()`
- `offHeapAllocator.usedBytes()`
- `directNativeBytes()`
- TTL 估算开销

综合起来，作为 DB 侧 maxmemory 判断依据。

这里的 `directNativeBytes()` 不是简单取 `memoryRuntime.usedBytes()`，而是把 DB 可解释的 native 结构逐项汇总：

- `YierdisFfmExpireIndex.nativeBytes()`
- `EntryTable.nativeBytes()`
- `NativeKeyDirectory.nativeBytes()`
- `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 的 native bytes

这意味着 maxmemory 统计的是业务上可解释的 live data / metadata，而不是把 slab 内部暂时空闲但尚未关闭的 reserved bytes 全部算成用户数据。

这意味着：

- off-heap bytes 会影响 maxmemory
- keyspace / expires / entry table / native key directory / type root 的 native bytes 不会被忽略
- delete、expire 和 eviction 释放记账优先读 `EntryRecord`，避免依赖旧对象容器里的估算值

在实例级 `maxmemoryScope=global` 时，还要避免多 DB 共享 runtime / allocator 导致 off-heap
重复计数。这里的策略是：

- `YierdisDbComponentFactory` 给每个 DB reporter 传入 `() -> owner.maxmemoryCoordinator() == null`
- 没有 global coordinator 时，DB 自己的 `usedBytesForMaxmemory()` 会包含 allocator 和 direct native bytes
- 有 global coordinator 时，DB participant 的 `usedBytesForMaxmemory()` 只报 heap ledger 和 TTL 估算，不把 off-heap 加进去
- `YierdisGlobalMaxmemoryGovernor` 通过 `MaxmemoryUsageSource[] sharedUsage` 额外汇总一次共享 off-heap usage
- `YierdisInstance.sharedOffHeapUsedBytes(...)` 从各 DB 的 `memoryStats().offHeapUsedBytes()` 读取 off-heap 视图，作为 shared usage 参与全局预算

因此 global 模式下不是“每个 DB 都算一遍 native bytes”，而是 DB 参与者排除 off-heap，
再由 governor 的 shared usage source 汇总一次。`memoryStats()` 仍会暴露 off-heap
usage；只是单 DB 的 `offHeapIncludedInMaxmemory` 在 global coordinator 存在时会是 false，
实例级 observability 再把 global off-heap 作为已纳入全局 maxmemory 的数据展示。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernor.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java`

#### 关闭和泄漏检测

`YierdisDbOwnedResources.releaseAll(...)` 的关闭顺序是：

1. 先清空 expires、`EntryTable` 和 `NativeKeyDirectory`
2. 关闭 `EntryTable`
3. 关闭 `NativeKeyDirectory`
4. 依次关闭 `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot`
5. 关闭 allocator
6. 如果 DB 拥有 runtime，再关闭 runtime

如果 runtime 关闭时仍然存在 live region，会直接抛出 leak 错误。

这让 “native memory 没回收” 在测试和关闭路径上都能尽早暴露。

代表路径：

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`

### 相关测试可以说明什么

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
  说明 string/list/hash/set/zset/HLL 删除后 native accounting 可以回到 0，且删除记账以 entry metadata 为准
- `MemoryStatsAccountingConsistencyTest`
  说明 memory reporter 和 maxmemory 统计保持一致
- `NativeKeyDirectoryTest`
  说明生产 key directory 的 key -> `EntryHandle` 路径、删除和 clear 行为
- `EntryTableContractTest`
  说明 `EntryRecord` slot 的 allocate/get/replace/release 行为
- `StringRootTest`
  说明 string payload 的 `ValueHandle`、覆盖和释放行为
- `YierdisFfmRehashConsistencyTest`
  说明保留的低层 `YierdisFfmKeyspace<V>` primitive 在 rehash 时保持一致
- `YierdisFfmSlabAllocatorTest`
  说明 slab allocator 的分配、释放、复用和容量限制行为
- `YierdisForeignOffHeapAllocatorTest`
  说明 `OffHeapAllocator` 对 FFM slab 的封装、used bytes 记账和关闭泄漏检查
- `YierdisFfmBlobStoreTest`
  说明 blob store 的 bytes 读写、refcount、retain / release 生命周期
- `ExpireIndexContractTest`
  说明 expire index 的通用 TTL 行为，FFM 实现需要满足同一契约
- `ListValueTest` / `HashValueTest` / `SetValueTest` / `ZSetValueTest`
  说明复合类型的 packed -> large 编码转换、读写语义和释放路径
- `YierdisGlobalMaxmemoryGovernorTest`
  说明 global maxmemory coordinator 跨 DB 淘汰和 shared usage source 计数
- `MaxmemoryScopeTest`
  说明 server 配置里 global / per-db scope 的解析和运行时 wiring
- `OffHeapBytesViewTtlRegressionTest`
  说明 BytesView 读写和 TTL 路径在 off-heap key 场景下保持一致

代表路径：

- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapKeysToggleTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapKeyspaceTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapDbSmokeTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireKeySharingTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocatorTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocatorTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmBlobStoreTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/expire/ExpireIndexContractTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/HashValueTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/SetValueTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ZSetValueTest.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernorTest.java`
- `yierdis-cli/src/test/java/yier/bubu/redis/app/client/MaxmemoryScopeTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapBytesViewTtlRegressionTest.java`

### 最后再压缩成一句话

Yierdis 当前对 FFM 的使用方式可以概括为：

- 用 `YierdisFfmMemoryRuntime` 统一承载实例级 native memory
- 用 slab allocator、`EntryTable` 和 64-bit handle 承载 entry metadata
- 用 `NativeKeyDirectory` 把 key bytes 映射到 entry handle
- 用 `StringRoot` / `ListRoot` / `HashRoot` / `SetRoot` / `ZSetRoot` 承载各类型 payload
- 用 `EntryRecord`、`ValueHandle` 和 `TypeRoot` 作为 DB hot path 的权威状态
- 用 `BlobStore + BytesRef + KeyHandle` 路径承载 native key directory、TTL 索引和复合结构成员 bytes
- 用 `OffHeapSlice` / `YierdisFfmBytesRefSlice` 给读路径提供尽量少拷贝的输出接口
- 用单线程 owner model 约束 `Arena.ofConfined()` 的访问纪律
- 用 runtime accounting、memory reporter 和 shutdown leak check 把 FFM 内存纳入 maxmemory 与资源回收体系

这也是 README 里“项目现在统一使用 JDK 25 FFM API 管理 native memory”那句话在实现层面的具体含义。

### Stable native object allocator

`YierdisStableNativeAllocator` implements the production stable-handle ABI described in `docs/superpowers/specs/2026-05-14-production-allocator-handle-design.md`.

It provides:

- 64-bit `NativeHandle` values with domain, kind, slot id, generation, and flags
- native object-table metadata for address, size, capacity, generation, kind/domain, pin count, owner shard, state, and alloc/free epochs
- 64 KiB page allocation with small size classes, medium spans, and large spans
- generation checks for stale handle, double-free, wrong-kind, wrong-domain, and quarantined-object detection
- bounded resolved object views with read-only and read-write access modes
- stable-handle `realloc` semantics with prefix preservation, in-place resize when capacity allows, and move rollback coverage
- DB `EntryHandle` / `ValueHandle` integration through `NativeHandle` so entry records, key bytes, string bytes, and collection roots keep stable allocator references rather than physical addresses
- pin and epoch quarantine so freed or moved physical blocks are not released while resolved views, scan epochs, snapshot epochs, command epochs, or defrag epochs may still observe them
- active defrag cycles that move eligible unpinned objects by updating allocator metadata; DB graph references do not need to be rewritten because they store stable handles
- allocator stats for logical/reserved/committed/free bytes, internal/external fragmentation, page counts, object kind counts, quarantine bytes, pin counts, stale/double-free detections, realloc counters, defrag counters, and allocation latency buckets

The allocator still keeps physical address packing private. DB hot paths must resolve a handle only for a bounded operation, close the resolved view promptly, and never persist allocator-private page ids, offsets, spans, or raw memory addresses.
