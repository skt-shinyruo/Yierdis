# Netty-Free 设计与 Fast-Path 实现

本文详细解释 Yierdis 如何通过 `yierdis-common-bytes` 模块实现 Netty-free 的核心抽象，以及如何在需要时利用 Netty 的高性能特性。

## 核心概念

### Netty-Free

**Netty-free** 指核心业务逻辑（协议层、存储层、命令处理层）不直接依赖 Netty 的 API，而是通过一套中立的抽象接口进行编程。

**目标**：
- 解耦核心逻辑与网络框架
- 支持在非 Netty 环境（测试、嵌入式）运行
- 便于未来更换网络框架

### Netty Fast-Path

**Netty fast-path** 指当下游确实是 Netty `ByteBuf` 时，通过适配器暴露的特化方法，利用 Netty 的零拷贝能力进行优化写入。

**目标**：
- 在保持解耦的同时，保留高性能特性
- 支持堆外内存的直接写出
- 减少不必要的内存分配和数据复制

---

## 架构设计

### 分层结构

```
┌─────────────────────────────────────────────────────────────┐
│                    核心业务层 (Netty-free)                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  RespReply   │    │  YierdisDb   │    │  Command     │  │
│  │   Writer     │    │              │    │   Support    │  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
│         │                   │                   │          │
│         ▼                   ▼                   ▼          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │           BytesSlice / BytesSink (中立抽象)            │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Netty 适配层                             │
│         ┌───────────────────────────────────┐              │
│         │    NettyByteBufSink (适配器)       │              │
│         │  - 实现 BytesSink 接口            │              │
│         │  - 包装 Netty ByteBuf             │              │
│         │  - 提供 unwrap() 暴露原始 ByteBuf  │              │
│         └───────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### 接口继承关系

```
BytesSource (最小只读字节源)
    └── BytesView (带长度的只读视图)
            └── BytesSlice (可写出的字节切片)

BytesSink (最小字节接收器)
    └── DirectBytesSink (支持堆外写入的接收器)
```

---

## 核心接口详解

### 1. BytesSource

```java
public interface BytesSource {
    byte getByte(int index);
    void getBytes(int index, byte[] dst, int dstOff, int len);
    
    default boolean hasMemoryAddress() { return false; }
    default long memoryAddress() { 
        throw new UnsupportedOperationException("memoryAddress not supported"); 
    }
}
```

**用途**：最小化的随机访问字节源抽象，支持可选的内存地址暴露以进行性能优化。

### 2. BytesView

```java
public interface BytesView extends BytesSource {
    int length();
    
    default void getBytes(int index, byte[] dst, int dstOff, int len) {
        // 参数校验 + 逐字节复制的默认实现
    }
}
```

**用途**：带长度的只读字节视图，主要用于 key 等请求级 lookup 的输入视图。设计为短生命周期对象，不得存入 DB。

### 3. BytesSlice

```java
public interface BytesSlice extends BytesView {
    void writeTo(BytesSink out);
}
```

**用途**：带长度限制的可写入字节切片，用于服务器写路径，高效地流式传输值而无需强制堆拷贝。

### 4. BytesSink

```java
public interface BytesSink {
    void writeBytes(byte[] src, int srcIndex, int len);
    
    default void writeBytes(byte[] src) { ... }
}
```

**用途**：最小化的字节接收器抽象，被协议、堆外内存和 I/O 适配器共享。

### 5. DirectBytesSink

```java
public interface DirectBytesSink extends BytesSink {
    void ensureWritable(int len);
    int writerIndex();
    void writerIndex(int writerIndex);
    boolean hasMemoryAddress();
    long memoryAddress();
}
```

**用途**：支持直接（可能是堆外）写入，并暴露写入器游标状态。

---

## Netty 适配器实现

### NettyByteBufSink

```java
public final class NettyByteBufSink implements DirectBytesSink {
    private final ByteBuf buf;
    
    public ByteBuf unwrap() { return buf; }
    
    @Override
    public void writeBytes(byte[] src, int srcIndex, int len) {
        buf.writeBytes(src, srcIndex, len);
    }
    
    @Override
    public long memoryAddress() {
        return buf.memoryAddress();  // 暴露直接内存地址
    }
}
```

**关键设计**：
- `unwrap()` 方法允许在必要时获取原始 `ByteBuf`
- `memoryAddress()` 暴露 Netty ByteBuf 的直接内存地址，支持零拷贝

---

## 实际使用场景

### 场景一：协议层写出

```java
public final class RespReplyWriter implements RedisReplyWriter {
    private final BytesSink out;
    
    @Override
    public void bulkString(BytesSlice slice) {
        writeAscii("$" + len + "\r\n");
        slice.writeTo(out);  // 不依赖 Netty！
        writeCrlf();
    }
}
```

### 场景二：内存存储层包装

```java
public final class NativeBytesSlice implements BytesSlice {
    private final NativeAllocator allocator;
    private final NativeHandle handle;  // 堆外内存句柄
    private final int offset;
    private final int length;
    
    @Override
    public void writeTo(BytesSink out) {
        try (NativeObjectView view = readView()) {
            byte[] scratch = TL_COPY_BUF.get();  // 8KB ThreadLocal 缓冲区
            int remaining = length;
            int sourceOffset = offset;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                view.getBytes(sourceOffset, scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                sourceOffset += chunk;
                remaining -= chunk;
            }
        }
    }
}
```

### 场景三：命令参数包装

```java
private static final class CommandArgBytesSlice implements BytesSlice {
    private ExecutionRequest request;
    private int argIndex;
    
    @Override
    public void writeTo(BytesSink out) {
        // 直接从请求帧中读取参数并写出，避免中间拷贝
    }
}
```

---

## 完整调用链路

```
客户端请求 "GET key"
        ↓
ExecutionRequest (命令参数)
        ↓
CommandArgBytesSlice (包装请求参数)
        ↓
YierdisStringOps.get() 查询内存
        ↓
NativeBytesSlice (包装堆外存储的值)
        ↓
RespReplyWriter.bulkString(slice)
        ↓
NettyByteBufSink (写入 Netty ByteBuf)
        ↓
网络发送（可能零拷贝）
```

---

## Fast-Path 与 Fallback 策略

### 写入路径选择

```
BytesSlice.writeTo(BytesSink out)
            │
      ┌─────┴─────┐
      ▼           ▼
普通 BytesSink  DirectBytesSink
      │           │
      │     ┌─────┴─────┐
      │     ▼           ▼
      │  hasMemoryAddress()  unwrap()
      │     │           │
      │     ▼           ▼
      │  零拷贝写出   获取原始 ByteBuf
      │
      ▼
分块拷贝写出（fallback）
```

### Fast-Path 适用场景

| 场景 | 实现方式 |
|------|----------|
| API 边界使用 `BytesView` | command/DB contract 不依赖 Netty |
| `BytesSlice.writeTo(BytesSink)` | value 直接流式写出 |
| `NativeBytesSlice` pin/unpin | 同步写出期间保持 allocator handle |
| `DirectBytesSink` 暴露内存地址 | 支持 direct/off-heap aware 写入 |
| `NettyByteBufSink.unwrap()` | 适配器边界使用 ByteBuf 能力 |
| `BulkStringSink` 逐元素输出 | collection range 边遍历边输出 |

### Fallback 合理场景

| 场景 | 原因 |
|------|------|
| Protocol snapshots | 需要稳定 argv 跨过 decoder 生命周期 |
| DB lifecycle lookup | ownership/lifetime 边界 |
| Transaction replay | 需要保存可重放的快照 |
| Explicit introspection | SCAN、snapshot、MEMORY 等输出 |
| Tests | 可读性和确定性的取舍 |
| Ownership-returning DB APIs | GET/HGET/pop 等返回 owned 数据 |

---

## 设计优势

| 特性 | 实现方式 | 价值 |
|------|----------|------|
| **无 Netty 依赖** | 纯接口抽象 | 存储层/协议层可独立演进 |
| **零拷贝写出** | `memoryAddress()` | 减少内存分配和复制 |
| **分块写入** | ThreadLocal 8KB 缓冲区 | 平衡内存占用和 I/O 效率 |
| **短生命周期** | 设计约束 | 避免被意外缓存导致内存泄漏 |
| **多实现支持** | 接口驱动 | 支持堆内存、堆外内存、测试等多种实现 |
| **渐进式优化** | Fast-path 可选 | 默认走抽象路径，需要时优化 |

---

## 与现有文档的关系

- **`bytes-and-fast-paths.md`**：详细描述 bytes 抽象的整体设计和 fast-path 机制
- **`netty-adapter-design.md`**（本文）：聚焦 Netty-free 设计和 Netty 适配层实现

两者互补，共同构成 Yierdis 高性能字节处理架构的完整文档。