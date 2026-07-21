# Common Bytes Contract Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `yierdis-common-bytes` to its four exercised contracts, make the default view-copy behavior range-safe, and align tests and current documentation with the bounded streaming path.

**Architecture:** Keep `BytesSource -> BytesView -> BytesSlice` as the read/stream hierarchy and `BytesSink` as the synchronous write port. Remove the unused raw-address/direct-buffer capability instead of replacing it, then verify the removal with an architecture guard. Preserve the current request snapshot, native storage, RESP encoding, reply reservation, and bounded `ByteBuf` chunk data flows.

**Tech Stack:** Java 25, Maven reactor, JUnit 4, Netty 4.1, existing architecture-test source/POM guards.

## Global Constraints

- Retain `BytesSource`, `BytesView`, `BytesSlice`, and `BytesSink`.
- Remove `DirectBytesSink`, `NettyByteBufSink`, and the common bytes raw-address methods without a compatibility shim or replacement fast path.
- Preserve RESP wire behavior, command behavior, request ownership, storage ownership, reply budgeting, and bounded chunk output.
- `BytesView.getBytes(...)` must keep `IllegalArgumentException` for a null destination or negative length and use `IndexOutOfBoundsException` for invalid ranges.
- Valid zero-length ranges include `index == length()` and `dstOff == dst.length`; invalid zero-length positions must fail.
- Added or rewritten Java comments are Chinese and describe only verified public contracts.
- Run all Maven commands with `/usr/lib/jvm/java-25-openjdk-amd64` through the repository's `use-jdk25` skill.
- Do not stage, rewrite, or revert unrelated worktree changes.

---

### Task 1: Make The Default `BytesView` Copy Contract Range-Safe

**Files:**
- Create: `yierdis-common/yierdis-common-bytes/src/test/java/yier/bubu/redis/bytes/BytesViewTest.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java`

**Interfaces:**
- Consumes: `BytesView.length()`, `BytesView.getByte(int)`.
- Produces: overflow-safe default `void getBytes(int index, byte[] dst, int dstOff, int len)` with the exception categories in the global constraints.

- [ ] **Step 1: Write the failing default-method tests**

Create `BytesViewTest.java` with a real `BytesView` that does not override `getBytes(...)`:

```java
package yier.bubu.redis.bytes;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class BytesViewTest {
    @Test
    public void copiesValidSubrangeIntoDestinationOffset() {
        BytesView view = arrayView(10, 20, 30, 40);
        byte[] destination = new byte[]{1, 2, 3, 4, 5};

        view.getBytes(1, destination, 2, 2);

        Assert.assertArrayEquals(new byte[]{1, 2, 20, 30, 5}, destination);
    }

    @Test
    public void acceptsZeroLengthRangesAtBothEnds() {
        BytesView view = arrayView(10, 20);
        byte[] destination = new byte[2];

        view.getBytes(view.length(), destination, destination.length, 0);
    }

    @Test
    public void rejectsNullDestination() {
        BytesView view = arrayView(10);

        Assert.assertThrows(IllegalArgumentException.class, () -> view.getBytes(0, null, 0, 1));
    }

    @Test
    public void rejectsNegativeLength() {
        BytesView view = arrayView(10);

        Assert.assertThrows(IllegalArgumentException.class, () -> view.getBytes(0, new byte[1], 0, -1));
    }

    @Test
    public void rejectsNegativeSourcePositionForZeroLengthCopy() {
        BytesView view = arrayView(10);

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> view.getBytes(-1, new byte[1], 0, 0));
    }

    @Test
    public void rejectsNegativeDestinationPositionForZeroLengthCopy() {
        BytesView view = arrayView(10);

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> view.getBytes(0, new byte[1], -1, 0));
    }

    @Test
    public void rejectsSourcePositionPastEndForZeroLengthCopy() {
        BytesView view = arrayView(10);

        Assert.assertThrows(IndexOutOfBoundsException.class, () -> view.getBytes(2, new byte[1], 0, 0));
    }

    @Test
    public void rejectsSourceRangeEvenWhenGetByteIsPermissive() {
        BytesView view = new BytesView() {
            @Override
            public int length() {
                return 1;
            }

            @Override
            public byte getByte(int index) {
                return 99;
            }
        };

        Assert.assertThrows(
                IndexOutOfBoundsException.class,
                () -> view.getBytes(Integer.MAX_VALUE, new byte[1], 0, 1)
        );
    }

    @Test
    public void rejectsOverflowingDestinationRangeBeforeReadingSource() {
        AtomicInteger reads = new AtomicInteger();
        BytesView view = new BytesView() {
            @Override
            public int length() {
                return 1;
            }

            @Override
            public byte getByte(int index) {
                reads.incrementAndGet();
                return 10;
            }
        };

        Assert.assertThrows(
                IndexOutOfBoundsException.class,
                () -> view.getBytes(0, new byte[1], Integer.MAX_VALUE, 1)
        );
        Assert.assertEquals(0, reads.get());
    }

    private static BytesView arrayView(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return new BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-common/yierdis-common-bytes -am \
  -Dtest=BytesViewTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because invalid zero-length positions return without validation, a permissive `getByte(...)` lets an invalid source range succeed, and the overflowed destination addition reads the source before the array access fails.

- [ ] **Step 3: Implement complete overflow-safe validation**

Add `java.util.Objects` and replace the default method body with:

```java
import java.util.Objects;

// ...

    @Override
    default void getBytes(int index, byte[] dst, int dstOff, int len) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        Objects.checkFromIndexSize(index, len, length());
        Objects.checkFromIndexSize(dstOff, len, dst.length);
        if (len == 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            dst[dstOff + i] = getByte(index + i);
        }
    }
```

- [ ] **Step 4: Run the focused test and verify GREEN**

Run the command from Step 2.

Expected: PASS with all `BytesViewTest` methods green.

- [ ] **Step 5: Commit the range fix**

```bash
git add \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java \
  yierdis-common/yierdis-common-bytes/src/test/java/yier/bubu/redis/bytes/BytesViewTest.java
git commit -m "fix: validate common byte view ranges"
```

---

### Task 2: Remove The Unused Direct And Raw-Address API

**Files:**
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java`
- Delete: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/DirectBytesSink.java`
- Delete: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Modify: `yierdis-networking/yierdis-networking-netty/pom.xml`

**Interfaces:**
- Consumes: the four retained common bytes interfaces.
- Produces: a source/POM architecture guard that prevents the removed capability from returning.

- [ ] **Step 1: Add a failing architecture guard**

Add this test immediately after `byteViewAndKeyHandleMustNotExposeLegacyAliases()`:

```java
    @Test
    public void commonBytesMustNotExposeUnusedDirectMemoryApis() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录", repoRoot);

        Path bytesSource = repoRoot.resolve(
                "yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java"
        ).normalize();
        Path directBytesSink = repoRoot.resolve(
                "yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/DirectBytesSink.java"
        ).normalize();
        Path nettyByteBufSink = repoRoot.resolve(
                "yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java"
        ).normalize();
        Path commandSupport = repoRoot.resolve(
                "yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java"
        ).normalize();
        Path networkingNettyPom = repoRoot.resolve(
                "yierdis-networking/yierdis-networking-netty/pom.xml"
        ).normalize();

        Assert.assertTrue("缺少 BytesSource.java", Files.isRegularFile(bytesSource));
        String source = Files.readString(bytesSource, StandardCharsets.UTF_8);
        Assert.assertFalse("BytesSource must not expose hasMemoryAddress", source.contains("hasMemoryAddress("));
        Assert.assertFalse("BytesSource must not expose memoryAddress", source.contains("memoryAddress("));
        Assert.assertFalse("DirectBytesSink must be removed", Files.exists(directBytesSink));
        Assert.assertFalse("NettyByteBufSink must be removed", Files.exists(nettyByteBufSink));

        Assert.assertTrue("缺少 CommandSupport.java", Files.isRegularFile(commandSupport));
        String commandSource = Files.readString(commandSupport, StandardCharsets.UTF_8);
        Assert.assertFalse("command slice must not expose hasMemoryAddress", commandSource.contains("hasMemoryAddress("));
        Assert.assertFalse("command slice must not expose memoryAddress", commandSource.contains("memoryAddress("));
        Assert.assertFalse(
                "networking-netty must not keep an unused common-bytes dependency",
                pomHasProductionDependency(networkingNettyPom, "yierdis-common-bytes")
        );
    }
```

- [ ] **Step 2: Run the architecture guard and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ArchitectureBoundaryTest#commonBytesMustNotExposeUnusedDirectMemoryApis \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL at `BytesSource must not expose hasMemoryAddress` while the old API is still present.

- [ ] **Step 3: Remove the dead capability and its direct dependency**

Make these exact production changes:

1. Delete `DirectBytesSink.java`.
2. Delete `NettyByteBufSink.java`.
3. Remove both default address methods from `BytesSource`, leaving only:

```java
public interface BytesSource {
    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);
}
```

4. Remove the `hasMemoryAddress()` and `memoryAddress()` overrides at the end of `CommandArgBytesSlice`; keep its `frame` bulk-read path.
5. Remove this dependency block from `yierdis-networking-netty/pom.xml`:

```xml
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-common-bytes</artifactId>
        </dependency>
```

- [ ] **Step 4: Run the guard and compile affected consumers**

Run the command from Step 2, then run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl \
yierdis-command/yierdis-command-builtin,yierdis-networking/yierdis-networking-netty \
  -am -DskipTests compile
```

Expected: the architecture guard passes and both consumer module graphs compile.

- [ ] **Step 5: Commit the API removal**

```bash
git add \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java \
  yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java \
  yierdis-networking/yierdis-networking-netty/pom.xml
git add -u \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/DirectBytesSink.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/bytes/netty/NettyByteBufSink.java
git commit -m "refactor: remove unused direct bytes api"
```

---

### Task 3: Publish The Synchronous Ownership Contract

**Files:**
- Create: `yierdis-common/yierdis-common-bytes/src/test/java/yier/bubu/redis/bytes/BytesSinkTest.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSlice.java`
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSink.java`
- Modify: `yierdis-common/yierdis-common-bytes/pom.xml`

**Interfaces:**
- Consumes: the retained four-interface hierarchy from Task 2.
- Produces: Chinese public Javadocs for lifetime, synchronous consumption, ownership, and copy semantics; characterization coverage for `BytesSink.writeBytes(byte[])`.

- [ ] **Step 1: Characterize the whole-array sink helper**

Create `BytesSinkTest.java`:

```java
package yier.bubu.redis.bytes;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class BytesSinkTest {
    @Test
    public void wholeArrayWriteDelegatesTheCompleteRangeSynchronously() {
        byte[] source = new byte[]{10, 20, 30};
        AtomicBoolean called = new AtomicBoolean();
        BytesSink sink = (actual, offset, length) -> {
            Assert.assertSame(source, actual);
            Assert.assertEquals(0, offset);
            Assert.assertEquals(source.length, length);
            called.set(true);
        };

        sink.writeBytes(source);

        Assert.assertTrue(called.get());
    }

    @Test
    public void wholeArrayWriteRejectsNullSource() {
        BytesSink sink = (source, offset, length) -> { };

        Assert.assertThrows(IllegalArgumentException.class, () -> sink.writeBytes((byte[]) null));
    }
}
```

Run the Task 1 Maven command with `-Dtest=BytesViewTest,BytesSinkTest`.

Expected: PASS. This is a characterization test for unchanged helper behavior; this task adds no new sink execution behavior.

- [ ] **Step 2: Replace the public type comments with verified contracts**

Use these contracts, keeping implementation details out of the public API:

`BytesSource`:

```java
/**
 * 与存储介质无关的最小随机访问只读字节源。
 * <p>
 * 该接口不转移底层数据所有权，也不承诺线程安全、连续内存布局或对象生命周期；
 * 调用方只能在具体所有者规定的有效期内访问其有效索引和范围。
 */
public interface BytesSource {
```

`BytesView`:

```java
/**
 * 带长度的短生命周期只读 bytes 视图。
 * <p>
 * 主要用于 key 等请求级 lookup。调用方不得把视图本身保存到 DB、队列或跨线程状态；
 * 需要跨出当前操作生命周期时，必须先取得独立所有权。
 */
public interface BytesView extends BytesSource {
    /**
     * 返回当前视图的非负长度。
     */
    int length();

    /**
     * 把当前视图的有效范围同步复制到目标数组。
     *
     * @throws IllegalArgumentException 目标数组为 {@code null} 或 {@code len} 为负数
     * @throws IndexOutOfBoundsException 源范围或目标范围无效
     */
```

`BytesSlice`:

```java
/**
 * 可随机读取并同步流式写出的短生命周期 bytes 片段。
 * <p>
 * {@link #writeTo(BytesSink)} 返回前必须完成本次消费；该调用不转移 slice、底层数据或
 * sink 的所有权，也不承诺写出过程没有复制。
 */
public interface BytesSlice extends BytesView {
    void writeTo(BytesSink out);
}
```

`BytesSink`:

```java
/**
 * 与协议、存储介质和 I/O 框架无关的最小 bytes 写入端口。
 * <p>
 * 实现必须在 {@link #writeBytes(byte[], int, int)} 返回前消费指定范围，且不得保留或修改
 * 传入数组。该接口不承诺线程安全、缓冲策略、零拷贝或资源所有权转移。
 */
public interface BytesSink {
```

Do not add comments that restate the loop or null check inside method bodies.

- [ ] **Step 3: Update the module description**

Replace the POM description with:

```xml
    <description>Neutral byte access and synchronous streaming contracts shared across Yierdis layers.</description>
```

- [ ] **Step 4: Verify compilation and focused tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-common/yierdis-common-bytes -am \
  -Dtest=BytesViewTest,BytesSinkTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS without warnings or Javadoc syntax errors.

- [ ] **Step 5: Commit the contract documentation and test**

```bash
git add \
  yierdis-common/yierdis-common-bytes/pom.xml \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSource.java \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSlice.java \
  yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSink.java \
  yierdis-common/yierdis-common-bytes/src/test/java/yier/bubu/redis/bytes/BytesSinkTest.java
git commit -m "docs: define common bytes ownership contract"
```

---

### Task 4: Align Current Documentation With Bounded Streaming

**Files:**
- Modify: `docs/project-docs/bytes-and-fast-paths.md`
- Modify: `docs/project-docs/offheap-copy-behavior.md`
- Modify: `docs/project-docs/glossary.md`
- Modify: `docs/project-docs/development-navigation.md`
- Modify: `docs/project-docs/code-logic-coverage.md`
- Modify: `docs/project-docs/netty-adapter-design.md`

**Interfaces:**
- Consumes: retained common bytes contracts and the current `ReplyReservationSink -> BoundedChunkedReplySink -> ByteBuf chunks` path.
- Produces: current documentation that does not advertise the deleted types or an active zero-copy path.

- [ ] **Step 1: Rewrite the common bytes overview around exercised capabilities**

In `bytes-and-fast-paths.md`:

1. Keep the existing protocol snapshot, DB lookup, write-path, fallback, and native-memory facts.
2. Replace the core-interface section with descriptions of only `BytesSource`, `BytesView`, `BytesSlice`, and `BytesSink`.
3. State this ownership contract exactly:

```text
`BytesView` / `BytesSlice` 是短生命周期只读输入；跨命令、跨线程、跨队列或持久保存前必须取得独立所有权。
`BytesSink.writeBytes(...)` 在返回前同步消费输入范围，不保留传入数组，也不表示 source ownership 转移。
```

4. Replace the reply flow with:

```text
CommandExecutor / fast command handler
  -> RedisReplyWriterFactory
  -> RespReplyWriter
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> bounded ByteBuf chunks
  -> ConnectionReplySequencer
  -> channel.write(...)
```

5. Rename the fast-path section to `## 流式路径和 materialization fallback` and state that `BytesSlice.writeTo(...)` avoids whole-result materialization but may use bounded heap scratch copies.
6. Link `netty-adapter-design.md` for reply reservation and Netty ownership details.

- [ ] **Step 2: Correct copy-boundary, glossary, navigation, and coverage text**

Make these exact semantic replacements:

- In `offheap-copy-behavior.md`, replace `Off-heap -> off-heap / direct -> direct` with `Off-heap -> bounded streaming output`. Use this flow:

```text
NativeBytesSlice
  -> writeTo(BytesSink)
  -> reusable bounded heap scratch
  -> ReplyReservationSink / BoundedChunkedReplySink
  -> bounded ByteBuf chunks
```

- State that this path avoids a complete heap result but currently performs bounded copies.
- Remove the `DirectBytesSink` glossary entry.
- Define `BytesSink` as synchronously consuming the supplied array range without retaining it.
- Change development navigation line 29 to `bytes 流式写出和 materialize 边界` and line 30 to `Netty 入站、reply reservation 和有界分块写回`.
- Change the `code-logic-coverage.md` bytes row from `heap/off-heap/direct` and `direct fast path` to `heap/off-heap materialization 边界` and `bounded streaming path`; add `BytesViewTest` and `BytesSinkTest` to its tests column.

- [ ] **Step 3: Rewrite `netty-adapter-design.md` as a current-path document**

Keep the file and its navigation role introduced by commit `2153003b`, but replace its content with these sections and verified facts:

```markdown
# Netty 适配边界与有界写回

本文说明 Netty 被限制在哪些模块，以及请求和回复在 Netty 对象、稳定 heap 请求、
中立 bytes contract 与有界 `ByteBuf` chunk 之间如何转换。

## 模块边界

- `yierdis-networking-netty` 负责入站 decoder、连接 handler 和 Netty pipeline 适配。
- `yierdis-networking-resp` 通过 `BytesSink` 编码 RESP，不依赖 `ByteBuf`。
- command、storage 和 native value 通过 `BytesView` / `BytesSlice` 工作，不导入 Netty。
- `yierdis-server-main` 把 reply reservation、chunk allocation、顺序写回和 channel lifecycle 接到一起。

## 入站路径

```text
ByteBuf fragments
  -> AccountedRespCumulator
  -> RespRequestDecoder
  -> retained heap argv + RequestMemoryLease
  -> ExecutionRequest
  -> executor / fast command handler
```

请求跨过 decoder 生命周期前会 materialize 成稳定 heap argv；这是 ownership 和 admission
边界，不是零拷贝路径。

## 回复路径

```text
command / storage result
  -> RedisReplyWriter
  -> RespReplyWriter
  -> ReplyReservationSink
  -> BoundedChunkedReplySink
  -> fixed-capacity ByteBuf chunks
  -> ConnectionReplySequencer
  -> Channel.write(...)
```

`RespReplyWriter` 只看到 `BytesSink`。`BoundedChunkedReplySink` 在 allocator 调用前把已预留
额度转换成 allocated credit，再创建固定上限 chunk 并登记到 `ReplySlot`。回复按 slot sequence
写回；资源和额度由唯一 cleanup owner 收敛。

## BytesSlice 输出

`RespReplyWriter.bulkString(BytesSlice)` 先写 bulk header，再同步调用 `slice.writeTo(out)`，最后
写 CRLF。native slice 当前通过可复用的 8 KiB heap scratch 分块读取，再写入有界 reply chunk。
它避免完整结果 materialization，但不承诺零拷贝。

## 生命周期和背压

- request lease 覆盖请求排队和执行生命周期。
- reply plan 在生成受控回复字节前申请容量。
- `ReplySlot` 持有 chunk、source owner 和 outbound lease，直到写回或终止清理完成。
- `BytesView` / `BytesSlice` 不得代替这些 retained owner 跨队列保存。

## 验证入口

- 入站：`RespRequestDecoderTest`, `RespIngressAdmissionTest`, `RespIngressLifecycleIntegrationTest`。
- RESP 编码：`RespReplyWriterTest`。
- 有界回复：`BoundedChunkedReplySinkTest`, `ReplyCapacityBlockedSchedulingTest`。
- 顺序与清理：`ConnectionReplySequencerTest`, `ReplyShutdownTest`, `NettyExecutionAdapterIntegrationTest`。
```

- [ ] **Step 4: Scan current docs for stale capability claims**

Run:

```bash
rg -n 'DirectBytesSink|NettyByteBufSink|hasMemoryAddress|memoryAddress' \
  README.md docs/project-docs
```

Expected: no output. References in historical `docs/superpowers/specs` and `docs/superpowers/plans` are intentionally retained.

Run:

```bash
rg -n 'zero-copy|零拷贝|direct-aware|direct fast path' docs/project-docs
```

Expected: remaining matches describe non-goals, misconceptions, or copy boundaries; none claims an active production zero-copy implementation.

- [ ] **Step 5: Commit current documentation convergence**

```bash
git add \
  docs/project-docs/bytes-and-fast-paths.md \
  docs/project-docs/offheap-copy-behavior.md \
  docs/project-docs/glossary.md \
  docs/project-docs/development-navigation.md \
  docs/project-docs/code-logic-coverage.md \
  docs/project-docs/netty-adapter-design.md
git commit -m "docs: align bytes design with bounded streaming"
```

---

### Task 5: Verify The Cross-Module Contract

**Files:**
- Verify only; modify a task-owned file only if a verification command exposes a defect caused by Tasks 1-4.

**Interfaces:**
- Consumes: all outputs from Tasks 1-4.
- Produces: fresh compile, test, source-scan, and documentation-scan evidence.

- [ ] **Step 1: Verify removed API references and Maven dependency**

Run:

```bash
rg -n 'DirectBytesSink|NettyByteBufSink' \
  --glob '*.java' --glob 'pom.xml' -g '!**/target/**' .
rg -n 'hasMemoryAddress|memoryAddress' \
  yierdis-common/yierdis-common-bytes/src \
  yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java
rg -n -C 2 'yierdis-common-bytes' yierdis-networking/yierdis-networking-netty/pom.xml
```

Expected: all three commands produce no matches. Historical Markdown is outside this source scan.

- [ ] **Step 2: Run focused contract and integration tests on JDK 25**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl \
yierdis-common/yierdis-common-bytes,\
yierdis-command/yierdis-command-builtin,\
yierdis-networking/yierdis-networking-resp,\
yierdis-networking/yierdis-networking-netty,\
yierdis-db/yierdis-db-memory,\
yierdis-server/yierdis-server-main,\
yierdis-tests/yierdis-integration-tests,\
yierdis-tests/yierdis-architecture-tests \
  -am \
  -Dtest=BytesViewTest,BytesSinkTest,ArchitectureBoundaryTest,RespReplyWriterTest,NativeBytesSliceTest,StringCommandTest,NettyExecutionAdapterIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: BUILD SUCCESS and every named test present in its owning module passes.

- [ ] **Step 3: Run the full affected-module test suite on JDK 25**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl \
yierdis-common/yierdis-common-bytes,\
yierdis-command/yierdis-command-builtin,\
yierdis-networking/yierdis-networking-resp,\
yierdis-networking/yierdis-networking-netty,\
yierdis-db/yierdis-db-memory,\
yierdis-server/yierdis-server-main,\
yierdis-tests/yierdis-integration-tests,\
yierdis-tests/yierdis-architecture-tests \
  -am test
```

Expected: BUILD SUCCESS with no test failure or error.

- [ ] **Step 4: Check patch hygiene and final documentation claims**

Run:

```bash
git diff --check HEAD~4..HEAD
git status --short
rg -n 'DirectBytesSink|NettyByteBufSink|hasMemoryAddress|memoryAddress' \
  README.md docs/project-docs
```

Expected: no whitespace errors, a clean worktree apart from pre-existing unrelated changes, and no stale current-doc references.

- [ ] **Step 5: Record verification evidence**

Do not create an empty verification commit. Report the exact Maven commands, BUILD SUCCESS results, source/doc scan results, commit identifiers, and any unrelated worktree paths left untouched.
