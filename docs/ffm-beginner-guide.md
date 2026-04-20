# FFM Beginner Guide

本文面向从未接触过 FFM 的读者，目标不是讲全，而是帮你尽快建立正确心智模型，并能自己写出最基本的 FFM 代码。

如果你想看 FFM 在 Yierdis 里的具体落点，请继续阅读 `docs/ffm-usage.md`。本文只讲 JDK FFM 本身的入门。

## FFM 是什么

FFM 是 `Foreign Function and Memory API`，可以粗略理解成：

- `Foreign Memory`：让 Java 能安全地操作 JVM 堆外的 native memory
- `Foreign Function`：让 Java 能直接调用 C 等 native library 中的函数

这里的 `foreign` 不是“远程”，而是“JVM 之外”。

FFM 的目标可以概括成一句话：

> 用纯 Java API 访问 native memory 和 native function，尽量替代一部分 JNI 和 `Unsafe` 的使用场景。

在 OpenJDK 中，FFM 由 JEP 454 在 JDK 22 正式定稿；在 Java 25 中，相关 API 位于 `java.lang.foreign` 包。

## 为什么 Java 需要 FFM

如果 Java 程序只处理普通对象，通常不需要 FFM。你只有在碰到下面这些问题时，才会真正需要它：

- 需要调用 C/C++ 或系统库
- 需要操作堆外内存
- 需要映射文件或实现 off-heap 数据结构
- 想减少 JNI 胶水代码
- 想减少 GC 压力，把大块数据放到 native memory

FFM 出现之前，Java 常见的几种办法分别有明显问题：

### `ByteBuffer.allocateDirect()`

它能分配堆外内存，但更像“direct buffer”而不是“完整的 native memory 模型”：

- 表达能力偏缓冲区模型
- 不适合复杂结构
- 生命周期控制不够直接

### `sun.misc.Unsafe`

它很强，但也很危险：

- 能直接分配和释放 native memory
- 能直接按地址读写
- 也能轻易越界、悬空引用、造成 JVM 崩溃

### JNI

JNI 能调用 native 代码，但开发体验很重：

- 要写 `native` 方法
- 要写 C/C++ 胶水代码
- 要处理构建、头文件、签名对齐
- 调试和维护成本高

FFM 的价值就是：

- 比 JNI 更直接
- 比 `Unsafe` 更安全
- 比 direct buffer 更完整

## 先记住 3 句话

如果你第一次接触 FFM，请先把下面三句话记住：

1. `MemorySegment` 是“带边界的内存块视图”。
2. `Arena` 是“这批内存什么时候死”。
3. `Linker + SymbolLookup + FunctionDescriptor` 是“怎么把 native 函数变成 Java 能调用的方法”。

这三句话基本概括了 FFM 的大部分内容。

## 先学哪一半

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

## 核心概念一：`Arena`

`Arena` 控制它分配出来的 memory segment 的生命周期。

你可以把它想成“内存生命周期容器”：

- 你从 arena 里分配出很多 segment
- 它们共享同一个生命周期
- arena 关闭后，这些 segment 全部失效

这和 `malloc/free` 的区别非常大。FFM 不是让你回到“每块内存单独 free”的痛苦模式，而是给你一层更结构化的生命周期管理。

### 最常见的 arena 类型

#### `Arena.ofConfined()`

最常用。

- 可手动关闭
- 只能由创建它的线程访问
- 生命周期清晰

大多数入门代码都应该先用它。

#### `Arena.ofShared()`

- 可手动关闭
- 可以跨线程访问

只有在你真的要跨线程共享 native memory 时才需要它。

#### `Arena.ofAuto()`

- 不能手动 close
- 生命周期由 GC 间接管理

它不是错，但不适合作为理解 FFM 生命周期模型的第一站。

#### `Arena.global()`

- 类似全局长期存在的 arena
- 不能关闭

适合极少量、几乎进程级常驻的数据，不适合日常业务代码。

## 核心概念二：`MemorySegment`

`MemorySegment` 表示一段连续内存。

它不像 Java 数组，也不像 C 裸指针，更准确地说，它是：

- 一块内存的视图
- 带边界检查
- 带生命周期检查

你可以把它理解成“受保护的指针对象”。

### 为什么它比 `Unsafe` 安全

FFM 主要多了两层保护：

#### 空间边界

如果一块 segment 只有 100 字节：

- 访问第 0 到 99 字节可以
- 访问第 100 字节不行

#### 时间边界

如果 arena 已经关闭：

- 这块 segment 就不能再访问
- 再访问会失败，而不是静默踩坏内存

这就是 FFM 最重要的安全基础。

## 核心概念三：`ValueLayout`

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

## 第一段可运行代码：分配一段 off-heap int 数组

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

## `set/get` 和 `setAtIndex/getAtIndex` 的区别

这是一个很常见的初学者疑问。

### `set/get`

它们按字节偏移访问：

```java
segment.set(ValueLayout.JAVA_INT, 0, 42);
int x = segment.get(ValueLayout.JAVA_INT, 0);
```

这里的 `0` 表示字节偏移量。

### `setAtIndex/getAtIndex`

它们按“数组下标”访问：

```java
segment.setAtIndex(ValueLayout.JAVA_INT, 3, 42);
int x = segment.getAtIndex(ValueLayout.JAVA_INT, 3);
```

这里的 `3` 表示第 3 个 `int` 元素，而不是第 3 个字节。

你可以理解成：

- `set/get` 更像底层内存操作
- `setAtIndex/getAtIndex` 更像数组操作

## 第二段可运行代码：按偏移读写

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

## 字符串：Java 字符串和 C 字符串

native world 里很常见的是 C 风格字符串，也就是：

- UTF-8 字节
- 末尾通常带 `\0`

FFM 为这种场景准备了方便的 API。

## 第三段可运行代码：分配一个 C 字符串

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

## 切片：`slice`

很多时候你不想复制内存，只想从一大块内存里看其中一段。

这时候就会用到切片。

## 第四段可运行代码：切出子数组视图

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

## 生命周期：故意踩一次坑

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

## 结构化内存：什么是 `MemoryLayout`

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

## 第五段可运行代码：定义并使用 `Point`

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

## 为什么 `MemoryLayout` 很重要

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

## 什么时候再去学 `VarHandle`

`MemoryLayout` 还能生成 `VarHandle`，做更高层的字段访问。

但对初学者来说，不要一上来就学它。先把这三步练熟：

- `structLayout`
- `byteOffset`
- `segment.get/set`

等你对布局已经不陌生，再去学 `varHandle(...)` 会顺很多。

## 开始进入 native function 调用

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

## 第六段可运行代码：调用 `strlen`

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

### 这段代码做了什么

#### `Linker.nativeLinker()`

获取当前平台的 native linker。

#### `defaultLookup().findOrThrow("strlen")`

在默认查找范围里寻找 `strlen` 符号。

#### `FunctionDescriptor.of(JAVA_LONG, ADDRESS)`

描述签名：

- 返回值是 `long`
- 参数是一个地址

#### `downcallHandle(...)`

把“函数地址 + 函数签名”链接成 Java 里的 `MethodHandle`。

#### `invokeExact(cString)`

真正调用 C 函数。

## 为什么调用 native function 要额外小心

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

## 为什么 `strlen` 例子不是完全跨平台万能

入门教程常用 `strlen`，是因为它直观。

但你要知道：

- 它依赖底层平台能在默认查找范围里找到这个符号
- 常见 Unix-like 平台通常更顺手
- 某些平台上默认符号或库路径可能不同

这不影响你理解 FFM 的调用模型，但会影响示例是否开箱即跑。

## FFM 和 JNI、`Unsafe`、DirectByteBuffer 的关系

### FFM vs JNI

JNI 的特点是：

- Java 一套
- C/C++ 一套
- 两边靠胶水代码拼起来

FFM 的特点是：

- 主要在 Java 里描述 native memory 和 native function
- 样板代码更少
- 维护更直接

### FFM vs `Unsafe`

`Unsafe` 更像一把危险但强大的底层工具。

FFM 的定位则是：

- 覆盖其中一大块 foreign memory / native interop 场景
- 用更结构化的 API 替代手写地址运算

### FFM vs direct buffer

direct buffer 更偏“buffer 视角”。

FFM 更偏：

- 真正的 native memory
- 带布局的内存结构
- native function interop

## 最容易踩的坑

### 1. arena 关了，segment 还在用

这是最常见问题。

记住：

- segment 的变量还在，不等于内存还活着
- arena 死了，segment 就不能再合法访问

### 2. 用了 `ofConfined()` 却跨线程访问

`ofConfined()` 的好处是简单清晰，但代价就是只能由 owner thread 访问。

### 3. 以为 off-heap 一定更快

off-heap 的常见价值是：

- 降低 heap 占用
- 降低 GC 压力
- 更适合 native interop

但它不是“自动更快”的同义词。

### 4. 把函数签名写错

这是 foreign function 调用里最危险的问题。

内存 API 的保护比函数调用 API 强得多。

### 5. 一开始就想学全部 API

正确方式是分阶段：

1. `Arena + MemorySegment`
2. `ValueLayout`
3. `MemoryLayout`
4. `Linker`

别一开始就把 `VarHandle`、复杂 ABI、upcall、jextract 全塞进脑子里。

## 真正的学习顺序

如果你要从“会看”走到“会写”，建议按下面顺序练习：

1. 写出 `Step1IntArray`
2. 写出 `Step2Offsets`
3. 写出 `Step3CString`
4. 写出 `Step5Lifetime`
5. 写出 `Step6PointStruct`
6. 写出 `Step7Strlen`

如果这 6 个你能不看答案自己写出来，说明你已经不是 FFM 小白了。

## 给自己的 3 个模板

### 模板一：分配一段 off-heap 数组

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment seg = arena.allocate(n * Integer.BYTES);
    for (int i = 0; i < n; i++) {
        seg.setAtIndex(ValueLayout.JAVA_INT, i, 0);
    }
}
```

### 模板二：定义结构体

```java
static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT.withName("a"),
    ValueLayout.JAVA_LONG.withName("b")
);
```

### 模板三：调用 native function

```java
Linker linker = Linker.nativeLinker();

MethodHandle fn = linker.downcallHandle(
    linker.defaultLookup().findOrThrow("some_function"),
    FunctionDescriptor.of(/* return layout */, /* arg layouts */)
);
```

## 学到这里，你应该已经会什么

如果你能看懂并改写本文中的示例，你应该已经具备这些能力：

- 理解 FFM 的核心目标
- 自己分配和释放 off-heap memory
- 按基本类型读写 `MemorySegment`
- 用 `MemoryLayout` 表达简单结构体
- 理解 arena 生命周期
- 调用一个简单的 native function
- 知道最常见的危险点在哪里

这已经足够进入真实项目里的 FFM 代码阅读阶段。

## 接下来该学什么

如果你要继续深入，通常有两条路线：

### 路线 A：off-heap 数据结构

继续学：

- `sequenceLayout`
- `VarHandle`
- 更复杂的布局
- 文件映射

适合：

- 存储
- 缓存
- 高性能数据结构

### 路线 B：native interop

继续学：

- `libraryLookup(...)`
- 更复杂的 `FunctionDescriptor`
- `upcall`
- `jextract`

适合：

- 调系统库
- 调 C/C++ 库
- 写跨语言绑定

## 官方资料

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

## 最后再压缩成一句话

FFM 可以理解成：

> Java 官方提供的一套 API，让你用更现代、更结构化的方式操作 native memory 和 native function，同时尽量保留 Java 风格的生命周期管理和安全边界。

如果你看完本文还想继续走下一步，最自然的顺序就是：

1. 自己把本文 6 个示例重新敲一遍
2. 再去看 `docs/ffm-usage.md`
3. 最后回到 Yierdis 代码里对照 `YierdisFfmMemoryRuntime`、`YierdisFfmRegion`、`YierdisForeignOffHeapAllocator`、`YierdisFfmKeyspace` 去读实现
