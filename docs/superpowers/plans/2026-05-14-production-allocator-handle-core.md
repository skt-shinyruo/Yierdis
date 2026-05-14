# Production Allocator Stable Handle Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first executable foundation for the production allocator spec: stable handle encoding, allocator contracts, a non-moving stable object allocator, stale-handle checks, pin/quarantine behavior, realloc semantics, stats, and tests.

**Architecture:** Add stable handle and native-object allocator contracts to `yierdis-memory-api`, then implement a first non-moving FFM-backed stable allocator in `yierdis-memory-ffm`. This first milestone deliberately does not migrate `EntryTable`, type roots, page size classes, or active defrag; those require separate follow-up plans after the handle ABI is proven green.

**Tech Stack:** Java 25, Maven, JUnit 4, existing `YierdisFfmMemoryRuntime`, existing `OffHeapAllocator` / `OffHeapBuf`.

---

## Scope Check

The production allocator spec covers several independent subsystems:

- stable handle ABI
- object table
- size-class/page allocator
- realloc
- pins/epochs
- active defrag
- DB storage integration
- scan/snapshot/AOF/RDB safety

This plan implements only the first usable core:

```text
stable handle ABI
+ native allocator contracts
+ non-moving object table facade
+ generation stale-handle checks
+ pin/quarantine behavior
+ stable-handle realloc
+ allocator stats
```

Follow-up plans should cover:

- replacing `EntryTable` / `ValueHandle` with production handles
- page and size-class allocator internals
- active defrag movement
- snapshot/scan integration

## File Structure

Create API contracts in `yierdis-memory-api`:

- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java`
  Encodes and decodes the 64-bit stable handle ABI.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandleDomain.java`
  Names handle domain codes.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
  Names initial object kind codes and their domains.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAccessMode.java`
  Names read/read-write resolve modes.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeReallocPolicy.java`
  Names realloc behavior.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
  Public stable object allocator contract.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectView.java`
  Bounded resolved object view.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java`
  Stats snapshot.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeMemoryException.java`
  Base allocator exception.
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StaleNativeHandleException.java`
  Generation and freed-slot stale handle exception.

Create FFM implementation in `yierdis-memory-ffm`:

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
  Stable handle allocator backed by existing FFM buffers. It is non-moving in this milestone.

Create tests:

- `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java`
- `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

## Task 1: Stable Handle ABI

**Files:**

- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandleDomain.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java`
- Test: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java`

- [ ] **Step 1: Write the failing handle tests**

Create `NativeHandleTest.java`:

```java
package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeHandleTest {
    @Test
    public void nullHandleIsOnlyZeroRawValue() {
        Assert.assertTrue(NativeHandle.NULL.isNull());
        Assert.assertEquals(0L, NativeHandle.NULL.raw());
    }

    @Test
    public void encodesAndDecodesHandleFields() {
        NativeHandle handle = NativeHandle.of(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        Assert.assertFalse(handle.isNull());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, handle.domain());
        Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());
        Assert.assertEquals(123456789L, handle.slotId());
        Assert.assertEquals(77, handle.generation());
        Assert.assertEquals(3, handle.flags());
    }

    @Test
    public void rejectsOutOfRangeFields() {
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, -1, 1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1L << 40, 1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, -1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 4096, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 1, -1));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 1, 16));
    }

    @Test
    public void rejectsNonZeroReservedDomain() {
        long raw = 0x0000_0000_0000_0010L;
        assertIllegal(() -> NativeHandle.fromRaw(raw));
    }

    private static void assertIllegal(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }
}
```

- [ ] **Step 2: Run the API test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeHandleTest test
```

Expected: FAIL with compilation errors for missing `NativeHandle`, `NativeHandleDomain`, and `NativeObjectKind`.

- [ ] **Step 3: Add handle domain enum**

Create `NativeHandleDomain.java`:

```java
package yier.bubu.redis.memory.api;

public enum NativeHandleDomain {
    RESERVED(0),
    STORAGE_OBJECT(1),
    ENTRY_OBJECT(2),
    KEY_BYTES(3),
    TYPE_ROOT(4),
    INDEX_NODE(5),
    ALLOCATOR_METADATA(6);

    private final int code;

    NativeHandleDomain(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static NativeHandleDomain fromCode(int code) {
        for (NativeHandleDomain domain : values()) {
            if (domain.code == code) {
                return domain;
            }
        }
        throw new IllegalArgumentException("unknown native handle domain: " + code);
    }
}
```

- [ ] **Step 4: Add object kind enum**

Create `NativeObjectKind.java`:

```java
package yier.bubu.redis.memory.api;

public enum NativeObjectKind {
    GENERIC(0, NativeHandleDomain.STORAGE_OBJECT),
    STRING_BYTES(1, NativeHandleDomain.STORAGE_OBJECT),
    ENTRY_RECORD(1, NativeHandleDomain.ENTRY_OBJECT),
    KEY_BYTES(1, NativeHandleDomain.KEY_BYTES),
    LIST_NODE(2, NativeHandleDomain.TYPE_ROOT),
    HASH_NODE(3, NativeHandleDomain.TYPE_ROOT),
    SET_NODE(4, NativeHandleDomain.TYPE_ROOT),
    ZSET_NODE(5, NativeHandleDomain.TYPE_ROOT),
    INDEX_NODE(1, NativeHandleDomain.INDEX_NODE),
    METADATA_RECORD(1, NativeHandleDomain.ALLOCATOR_METADATA);

    private final int code;
    private final NativeHandleDomain domain;

    NativeObjectKind(int code, NativeHandleDomain domain) {
        this.code = code;
        this.domain = domain;
    }

    public int code() {
        return code;
    }

    public NativeHandleDomain domain() {
        return domain;
    }
}
```

- [ ] **Step 5: Add NativeHandle**

Create `NativeHandle.java`:

```java
package yier.bubu.redis.memory.api;

import java.util.Objects;

public record NativeHandle(long raw) {
    public static final NativeHandle NULL = new NativeHandle(0L);

    private static final int DOMAIN_SHIFT = 60;
    private static final int KIND_SHIFT = 56;
    private static final int SLOT_SHIFT = 16;
    private static final int GENERATION_SHIFT = 4;

    private static final long FOUR_BIT_MASK = 0x0fL;
    private static final long SLOT_MASK = (1L << 40) - 1L;
    private static final long GENERATION_MASK = (1L << 12) - 1L;

    public NativeHandle {
        if (raw != 0L && ((raw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK) == NativeHandleDomain.RESERVED.code()) {
            throw new IllegalArgumentException("non-zero handle cannot use reserved domain");
        }
    }

    public static NativeHandle fromRaw(long raw) {
        return new NativeHandle(raw);
    }

    public static NativeHandle of(
            NativeHandleDomain domain,
            NativeObjectKind kind,
            long slotId,
            int generation,
            int flags
    ) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(kind, "kind");
        if (domain == NativeHandleDomain.RESERVED) {
            throw new IllegalArgumentException("domain must not be reserved");
        }
        if (slotId < 0 || slotId > SLOT_MASK) {
            throw new IllegalArgumentException("slotId out of range: " + slotId);
        }
        if (generation < 0 || generation > GENERATION_MASK) {
            throw new IllegalArgumentException("generation out of range: " + generation);
        }
        if (flags < 0 || flags > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("flags out of range: " + flags);
        }
        if (kind.code() < 0 || kind.code() > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("kind code out of range: " + kind.code());
        }
        long raw = ((long) domain.code() << DOMAIN_SHIFT)
                | ((long) kind.code() << KIND_SHIFT)
                | (slotId << SLOT_SHIFT)
                | ((long) generation << GENERATION_SHIFT)
                | flags;
        return new NativeHandle(raw);
    }

    public boolean isNull() {
        return raw == 0L;
    }

    public NativeHandleDomain domain() {
        return NativeHandleDomain.fromCode((int) ((raw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK));
    }

    public int kindCode() {
        return (int) ((raw >>> KIND_SHIFT) & FOUR_BIT_MASK);
    }

    public long slotId() {
        return (raw >>> SLOT_SHIFT) & SLOT_MASK;
    }

    public int generation() {
        return (int) ((raw >>> GENERATION_SHIFT) & GENERATION_MASK);
    }

    public int flags() {
        return (int) (raw & FOUR_BIT_MASK);
    }

    public void requireNonNull() {
        if (isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
    }
}
```

- [ ] **Step 6: Run the handle tests and verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeHandleTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandleDomain.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java \
        yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java
git commit -m "feat(memory): add stable native handle ABI"
```

## Task 2: Native Allocator API Contracts

**Files:**

- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAccessMode.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeReallocPolicy.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectView.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeMemoryException.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StaleNativeHandleException.java`
- Test: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java`

- [ ] **Step 1: Write contract compilation tests**

Create `NativeAllocatorContractTest.java`:

```java
package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeAllocatorContractTest {
    @Test
    public void statsRecordExposesAllocatorCounters() {
        NativeAllocatorStats stats = new NativeAllocatorStats(
                10,
                64,
                2,
                1,
                3,
                4,
                5,
                6
        );

        Assert.assertEquals(10L, stats.logicalUsedBytes());
        Assert.assertEquals(64L, stats.reservedBytes());
        Assert.assertEquals(2L, stats.liveObjects());
        Assert.assertEquals(1L, stats.pinnedObjects());
        Assert.assertEquals(3L, stats.quarantinedObjects());
        Assert.assertEquals(4L, stats.staleHandleDetections());
        Assert.assertEquals(5L, stats.reallocInPlaceCount());
        Assert.assertEquals(6L, stats.reallocMovedCount());
    }

    @Test
    public void exceptionTypesCarryMessages() {
        NativeMemoryException base = new NativeMemoryException("base");
        StaleNativeHandleException stale = new StaleNativeHandleException("stale");

        Assert.assertEquals("base", base.getMessage());
        Assert.assertEquals("stale", stale.getMessage());
    }
}
```

- [ ] **Step 2: Run the API tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeAllocatorContractTest test
```

Expected: FAIL with missing native allocator contract types.

- [ ] **Step 3: Add access mode and realloc policy enums**

Create `NativeAccessMode.java`:

```java
package yier.bubu.redis.memory.api;

public enum NativeAccessMode {
    READ_ONLY,
    READ_WRITE
}
```

Create `NativeReallocPolicy.java`:

```java
package yier.bubu.redis.memory.api;

public enum NativeReallocPolicy {
    PRESERVE_PREFIX,
    NO_MOVE,
    MAY_MOVE_SAME_HANDLE
}
```

- [ ] **Step 4: Add allocator and view contracts**

Create `NativeAllocator.java`:

```java
package yier.bubu.redis.memory.api;

public interface NativeAllocator extends AutoCloseable {
    NativeHandle allocate(NativeObjectKind kind, int size);

    NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    void free(NativeHandle handle);

    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    NativeAllocatorStats stats();

    @Override
    void close();
}
```

Create `NativeObjectView.java`:

```java
package yier.bubu.redis.memory.api;

public interface NativeObjectView extends AutoCloseable {
    NativeHandle handle();

    int size();

    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    @Override
    void close();
}
```

- [ ] **Step 5: Add stats and exception types**

Create `NativeAllocatorStats.java`:

```java
package yier.bubu.redis.memory.api;

public record NativeAllocatorStats(
        long logicalUsedBytes,
        long reservedBytes,
        long liveObjects,
        long pinnedObjects,
        long quarantinedObjects,
        long staleHandleDetections,
        long reallocInPlaceCount,
        long reallocMovedCount
) {
}
```

Create `NativeMemoryException.java`:

```java
package yier.bubu.redis.memory.api;

public class NativeMemoryException extends RuntimeException {
    public NativeMemoryException(String message) {
        super(message);
    }

    public NativeMemoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

Create `StaleNativeHandleException.java`:

```java
package yier.bubu.redis.memory.api;

public final class StaleNativeHandleException extends NativeMemoryException {
    public StaleNativeHandleException(String message) {
        super(message);
    }
}
```

- [ ] **Step 6: Run API module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAccessMode.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeReallocPolicy.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectView.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeMemoryException.java \
        yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StaleNativeHandleException.java \
        yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java
git commit -m "feat(memory): add native allocator contracts"
```

## Task 3: Non-Moving Stable FFM Allocator

**Files:**

- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

- [ ] **Step 1: Write allocation, resolve, free, and stale-handle tests**

Create `YierdisStableNativeAllocatorTest.java`:

```java
package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class YierdisStableNativeAllocatorTest {
    @Test
    public void allocatesResolvesAndFreesObject() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertFalse(handle.isNull());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.domain(), handle.domain());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                Assert.assertEquals(8, view.size());
                Assert.assertEquals(8, view.capacity());
                view.setByte(0, (byte) 42);
            }

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(42, view.getByte(0));
            }

            NativeAllocatorStats beforeFree = allocator.stats();
            Assert.assertEquals(8L, beforeFree.logicalUsedBytes());
            Assert.assertEquals(1L, beforeFree.liveObjects());

            allocator.free(handle);

            NativeAllocatorStats afterFree = allocator.stats();
            Assert.assertEquals(0L, afterFree.logicalUsedBytes());
            Assert.assertEquals(0L, afterFree.liveObjects());
        }
    }

    @Test
    public void detectsUseAfterFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            allocator.free(handle);

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }
}
```

- [ ] **Step 2: Run the FFM allocator test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest test
```

Expected: FAIL with missing `YierdisStableNativeAllocator`.

- [ ] **Step 3: Implement the non-moving allocator**

Create `YierdisStableNativeAllocator.java`:

```java
package yier.bubu.redis.memory.foreign;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Objects;

public final class YierdisStableNativeAllocator implements NativeAllocator {
    private static final int INITIAL_GENERATION = 1;

    private final OffHeapAllocator payloadAllocator;
    private final Slot[] slots;
    private final ArrayDeque<Integer> freeSlots = new ArrayDeque<>();

    private boolean closed;
    private long logicalUsedBytes;
    private long liveObjects;
    private long staleHandleDetections;
    private long reallocInPlaceCount;
    private long reallocMovedCount;

    public YierdisStableNativeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        this(new YierdisForeignOffHeapAllocator(Objects.requireNonNull(runtime, "runtime"), 0), maxSlots);
    }

    public YierdisStableNativeAllocator(OffHeapAllocator payloadAllocator, int maxSlots) {
        this.payloadAllocator = Objects.requireNonNull(payloadAllocator, "payloadAllocator");
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        this.slots = new Slot[maxSlots + 1];
        for (int i = 1; i < slots.length; i++) {
            slots[i] = new Slot(i);
            freeSlots.addLast(i);
        }
    }

    @Override
    public synchronized NativeHandle allocate(NativeObjectKind kind, int size) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        Integer slotId = freeSlots.pollFirst();
        if (slotId == null) {
            throw new NativeMemoryException("native object slot limit exceeded");
        }
        Slot slot = slots[slotId];
        OffHeapBuf buffer = null;
        boolean ok = false;
        try {
            buffer = payloadAllocator.allocate(size);
            slot.allocate(kind, buffer, size);
            logicalUsedBytes += size;
            liveObjects++;
            ok = true;
            return NativeHandle.of(kind.domain(), kind, slotId, slot.generation, 0);
        } finally {
            if (!ok) {
                if (buffer != null) {
                    buffer.close();
                }
                slot.markFreeAfterFailedAllocation();
                freeSlots.addFirst(slotId);
            }
        }
    }

    @Override
    public synchronized NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        ensureOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0");
        }
        Slot slot = requireLiveSlot(handle);
        if (newSize <= slot.capacity) {
            logicalUsedBytes += newSize - slot.size;
            slot.size = newSize;
            reallocInPlaceCount++;
            return handle;
        }
        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("native object cannot grow in place");
        }

        OffHeapBuf next = payloadAllocator.allocate(newSize);
        boolean ok = false;
        try {
            byte[] copy = new byte[Math.min(slot.size, newSize)];
            slot.buffer.getBytes(0, copy, 0, copy.length);
            next.setBytes(0, copy, 0, copy.length);
            OffHeapBuf previous = slot.buffer;
            slot.buffer = next;
            slot.size = newSize;
            slot.capacity = newSize;
            logicalUsedBytes += newSize - copy.length;
            previous.close();
            reallocMovedCount++;
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                next.close();
            }
        }
    }

    @Override
    public synchronized void free(NativeHandle handle) {
        ensureOpen();
        Slot slot = requireLiveSlot(handle);
        if (slot.pinCount > 0) {
            slot.quarantined = true;
            return;
        }
        releaseSlot(slot);
    }

    @Override
    public synchronized NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        Objects.requireNonNull(mode, "mode");
        Slot slot = requireLiveSlot(handle);
        return new StableObjectView(handle, slot, mode);
    }

    @Override
    public synchronized NativeAllocatorStats stats() {
        return new NativeAllocatorStats(
                logicalUsedBytes,
                payloadAllocator.usedBytes(),
                liveObjects,
                pinnedObjects(),
                quarantinedObjects(),
                staleHandleDetections,
                reallocInPlaceCount,
                reallocMovedCount
        );
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        RuntimeException failure = null;
        for (int i = 1; i < slots.length; i++) {
            Slot slot = slots[i];
            if (slot.live && slot.buffer != null) {
                try {
                    slot.buffer.close();
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
                slot.clearWithoutGenerationChange();
            }
        }
        try {
            payloadAllocator.close();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        closed = true;
        if (failure != null) {
            throw failure;
        }
    }

    private Slot requireLiveSlot(NativeHandle handle) {
        if (handle == null || handle.isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        long slotId = handle.slotId();
        if (slotId <= 0 || slotId >= slots.length) {
            return stale("stale native handle: unknown slot " + slotId);
        }
        Slot slot = slots[(int) slotId];
        if (!slot.live || slot.generation != handle.generation()) {
            return stale("stale native handle: slot=" + slotId + " generation=" + handle.generation());
        }
        if (slot.kind.code() != handle.kindCode() || slot.kind.domain() != handle.domain()) {
            throw new NativeMemoryException("native handle kind/domain mismatch: " + handle.raw());
        }
        return slot;
    }

    private Slot stale(String message) {
        staleHandleDetections++;
        throw new StaleNativeHandleException(message);
    }

    private void releaseSlot(Slot slot) {
        logicalUsedBytes -= slot.size;
        liveObjects--;
        slot.buffer.close();
        slot.buffer = null;
        slot.size = 0;
        slot.capacity = 0;
        slot.live = false;
        slot.quarantined = false;
        slot.generation = nextGeneration(slot.generation);
        freeSlots.addLast(slot.slotId);
    }

    private long pinnedObjects() {
        return Arrays.stream(slots).filter(slot -> slot != null && slot.pinCount > 0).count();
    }

    private long quarantinedObjects() {
        return Arrays.stream(slots).filter(slot -> slot != null && slot.quarantined).count();
    }

    private static int nextGeneration(int current) {
        int next = (current + 1) & 0x0fff;
        return next == 0 ? INITIAL_GENERATION : next;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("stable native allocator is closed");
        }
    }

    private static final class Slot {
        private final int slotId;
        private int generation = INITIAL_GENERATION;
        private NativeObjectKind kind;
        private OffHeapBuf buffer;
        private int size;
        private int capacity;
        private int pinCount;
        private boolean live;
        private boolean quarantined;

        private Slot(int slotId) {
            this.slotId = slotId;
        }

        private void allocate(NativeObjectKind kind, OffHeapBuf buffer, int size) {
            this.kind = kind;
            this.buffer = buffer;
            this.size = size;
            this.capacity = buffer.capacity();
            this.live = true;
            this.quarantined = false;
        }

        private void markFreeAfterFailedAllocation() {
            this.kind = null;
            this.buffer = null;
            this.size = 0;
            this.capacity = 0;
            this.live = false;
            this.quarantined = false;
        }

        private void clearWithoutGenerationChange() {
            this.kind = null;
            this.buffer = null;
            this.size = 0;
            this.capacity = 0;
            this.pinCount = 0;
            this.live = false;
            this.quarantined = false;
        }
    }

    private final class StableObjectView implements NativeObjectView {
        private final NativeHandle handle;
        private final Slot slot;
        private final NativeAccessMode mode;
        private boolean closedView;

        private StableObjectView(NativeHandle handle, Slot slot, NativeAccessMode mode) {
            this.handle = handle;
            this.slot = slot;
            this.mode = mode;
        }

        @Override
        public NativeHandle handle() {
            ensureViewOpen();
            return handle;
        }

        @Override
        public int size() {
            ensureViewOpen();
            return slot.size;
        }

        @Override
        public int capacity() {
            ensureViewOpen();
            return slot.capacity;
        }

        @Override
        public byte getByte(int index) {
            ensureViewOpen();
            checkRange(index, 1);
            return slot.buffer.getByte(index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureWritable();
            checkRange(index, 1);
            slot.buffer.setByte(index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            ensureViewOpen();
            checkRange(index, len);
            slot.buffer.getBytes(index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            ensureWritable();
            checkRange(index, len);
            slot.buffer.setBytes(index, src, srcOff, len);
        }

        @Override
        public void close() {
            closedView = true;
        }

        private void ensureWritable() {
            ensureViewOpen();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new NativeMemoryException("resolved object is read-only");
            }
        }

        private void ensureViewOpen() {
            if (closedView) {
                throw new IllegalStateException("native object view is closed");
            }
        }

        private void checkRange(int index, int len) {
            if (len < 0 || index < 0 || index + len > slot.size) {
                throw new IndexOutOfBoundsException();
            }
        }
    }
}
```

- [ ] **Step 4: Run the FFM test and verify it passes**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java
git commit -m "feat(memory): add stable FFM native allocator"
```

## Task 4: Realloc And Read-Only Enforcement

**Files:**

- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`

- [ ] **Step 1: Add realloc and read-only tests**

Append these tests to `YierdisStableNativeAllocatorTest`:

```java
    @Test
    public void reallocPreservesHandleAndPrefixWhenMoved() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, new byte[]{1, 2, 3, 4}, 0, 4);
            }

            NativeHandle resized = allocator.realloc(handle, 8, yier.bubu.redis.memory.api.NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle, resized);

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                Assert.assertEquals(8, view.size());
                byte[] bytes = new byte[4];
                view.getBytes(0, bytes, 0, 4);
                Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, bytes);
                view.setByte(7, (byte) 9);
            }

            Assert.assertEquals(1L, allocator.stats().reallocMovedCount());
        }
    }

    @Test
    public void readOnlyViewRejectsMutation() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                try {
                    view.setByte(0, (byte) 1);
                    Assert.fail("expected read-only failure");
                } catch (yier.bubu.redis.memory.api.NativeMemoryException expected) {
                    Assert.assertTrue(expected.getMessage().contains("read-only"));
                }
            }
        }
    }
```

- [ ] **Step 2: Run tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest test
```

Expected: PASS if Task 3 implementation already included realloc and read-only behavior. If it fails, update only `YierdisStableNativeAllocator` to match the behavior in the tests.

- [ ] **Step 3: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java
git commit -m "test(memory): cover stable allocator realloc behavior"
```

## Task 5: Pin And Quarantine Foundation

**Files:**

- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

- [ ] **Step 1: Add pin API to the contract**

Modify `NativeAllocator.java` to:

```java
package yier.bubu.redis.memory.api;

public interface NativeAllocator extends AutoCloseable {
    NativeHandle allocate(NativeObjectKind kind, int size);

    NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    void free(NativeHandle handle);

    void pin(NativeHandle handle);

    void unpin(NativeHandle handle);

    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    NativeAllocatorStats stats();

    @Override
    void close();
}
```

- [ ] **Step 2: Add pin/quarantine test**

Append this test to `YierdisStableNativeAllocatorTest`:

```java
    @Test
    public void freePinnedObjectQuarantinesUntilUnpin() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            allocator.pin(handle);
            allocator.free(handle);

            Assert.assertEquals(1L, allocator.stats().pinnedObjects());
            Assert.assertEquals(1L, allocator.stats().quarantinedObjects());
            Assert.assertEquals(1L, allocator.stats().liveObjects());

            allocator.unpin(handle);

            Assert.assertEquals(0L, allocator.stats().pinnedObjects());
            Assert.assertEquals(0L, allocator.stats().quarantinedObjects());
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest#freePinnedObjectQuarantinesUntilUnpin test
```

Expected: FAIL until `pin` and `unpin` are implemented.

- [ ] **Step 4: Implement pin/unpin**

Add these methods to `YierdisStableNativeAllocator`:

```java
    @Override
    public synchronized void pin(NativeHandle handle) {
        ensureOpen();
        Slot slot = requireLiveSlot(handle);
        slot.pinCount++;
    }

    @Override
    public synchronized void unpin(NativeHandle handle) {
        ensureOpen();
        Slot slot = requireLiveSlot(handle);
        if (slot.pinCount <= 0) {
            throw new NativeMemoryException("native object is not pinned");
        }
        slot.pinCount--;
        if (slot.pinCount == 0 && slot.quarantined) {
            releaseSlot(slot);
        }
    }
```

Keep the existing `free(...)` behavior from Task 3:

```java
    @Override
    public synchronized void free(NativeHandle handle) {
        ensureOpen();
        Slot slot = requireLiveSlot(handle);
        if (slot.pinCount > 0) {
            slot.quarantined = true;
            return;
        }
        releaseSlot(slot);
    }
```

- [ ] **Step 5: Run FFM tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java
git commit -m "feat(memory): add native object pin quarantine"
```

## Task 6: Full Memory Module Verification And Documentation

**Files:**

- Modify: `docs/project-docs/ffm-usage.md`
- Test: existing memory tests

- [ ] **Step 1: Add stable handle allocator note to FFM docs**

Append this section near the end of `docs/project-docs/ffm-usage.md`:

```markdown
### Stable native object allocator

`YierdisStableNativeAllocator` is the first implementation of the production stable-handle ABI described in `docs/superpowers/specs/2026-05-14-production-allocator-handle-design.md`.

It provides:

- 64-bit `NativeHandle` values with domain, kind, slot id, generation, and flags
- generation checks for stale handle detection
- bounded resolved object views
- stable-handle realloc semantics
- pin/quarantine behavior for future snapshot and defrag work

This allocator is intentionally non-moving in its first milestone. Page size classes, object-table-native metadata compaction, DB `EntryTable` migration, and active defrag are follow-up work.
```

- [ ] **Step 2: Run memory module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory -am test
```

Expected: PASS.

- [ ] **Step 3: Run architecture tests if memory module passes**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add docs/project-docs/ffm-usage.md
git commit -m "docs: describe stable native allocator milestone"
```

## Task 7: Final Review

**Files:**

- Inspect: all files changed by Tasks 1-6

- [ ] **Step 1: Check for unresolved markers**

Run:

```bash
rg -n "T[B]D|T[O]DO|F[I]XME" yierdis-memory docs/project-docs/ffm-usage.md
```

Expected: no matches introduced by this work.

- [ ] **Step 2: Check git status**

Run:

```bash
git status --short
```

Expected: no unstaged changes.

- [ ] **Step 3: Run final focused verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory -am test
```

Expected: PASS.

- [ ] **Step 4: Summarize remaining follow-up work**

Write the implementation summary with these explicit follow-ups:

```text
Follow-up allocator work:
- migrate EntryTable and ValueHandle to NativeHandle
- replace simple payload allocation with page and size-class allocation
- add native object metadata table storage for all production metadata fields
- add epoch reclamation around snapshot/scan cursors
- add active defrag movement and validation hooks
```

Do not claim active defrag or DB storage migration is complete after this plan. This plan completes only the stable handle core.

## Self-Review

Spec coverage for this plan:

- Stable 64-bit handle ABI: Task 1.
- Allocator contracts and resolved views: Task 2.
- Non-moving object table facade and generation checks: Task 3.
- Realloc stable-handle behavior: Task 4.
- Pin/quarantine foundation: Task 5.
- Metrics: Tasks 2-5 through `NativeAllocatorStats`.
- Documentation and verification: Tasks 6-7.

Intentional gaps for follow-up plans:

- page and size-class allocator internals
- native metadata table expansion beyond this first facade
- DB `EntryTable` / `ValueHandle` migration
- snapshot/scan epoch integration
- active defrag

No task in this plan should modify `yierdis-db-memory`, command handling, networking, or server runtime.
