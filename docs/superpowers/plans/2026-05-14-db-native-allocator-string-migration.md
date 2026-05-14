# DB Native Allocator String Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move DB entry records and string payloads onto one DB-owned `NativeAllocator` namespace while preserving string/HLL behavior and native memory accounting.

**Architecture:** Add zero-length logical object support to the stable allocator because Redis empty strings need stable handles that can later grow. Convert `StringRoot` from `OffHeapAllocator` slots to allocator-backed `STRING_BYTES` handles. Wire one shared `NativeAllocator` through DB component assembly, resource ownership, memory reporting, and tests.

**Tech Stack:** Java 25, Maven, JUnit 4, JDK FFM-backed memory runtime, `NativeAllocator`, `YierdisStableNativeAllocator`.

---

## Scope

This plan implements the first independently testable slice of `docs/superpowers/specs/2026-05-14-db-native-allocator-unification-design.md`:

- Phase 1: DB allocator ownership.
- Phase 2: `StringRoot` migration.
- Documentation update for key bytes remaining blob-store-owned.

This plan does not migrate key bytes, collection roots, DB defrag maintenance, scan/snapshot epochs, or benchmarks. Those are separate plans after string migration is stable.

## File Map

- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
  - Allow logical zero-length objects while still allocating at least one physical byte.
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
  - Allow object metadata size `0` for allocate, location update, and moved publish paths.
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
  - Cover zero-length allocate, grow, shrink, view bounds, free, stale handle, and runtime cleanup.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
  - Replace off-heap slot map with allocator-backed `STRING_BYTES` handles.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
  - Add package-private allocator accessor for assembly tests.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
  - Create one shared `YierdisStableNativeAllocator` and pass it to `EntryTable` and `StringRoot`.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
  - Own and close the shared `NativeAllocator` exactly once.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
  - Pass the shared native allocator to lifecycle/reporting collaborators.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
  - Store the shared native allocator and expose package-private access for memory reporting/tests.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
  - Count shared allocator logical bytes once and stop separately counting `EntryTable.nativeBytes()`.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java`
  - Keep API unchanged; no required code change unless tests reveal double-counting through `offHeapAllocator`.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java`
  - Cover stable string handle semantics.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java`
  - Assert production assembly uses one shared allocator for entries and strings.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
  - Extend churn assertions to allocator-backed string bytes.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`
  - Rename assertions away from deprecated off-heap string ownership and toward runtime/stable allocator ownership.
- Docs: `docs/project-docs/native-allocator-and-handles.md`
  - Update current-state section: `ValueHandle` for strings is allocator-backed after this plan.
- Docs: `docs/project-docs/ffm-usage.md`
  - Update current-state section: keys remain blob-store-owned; strings no longer use `OffHeapAllocator`.

All Maven commands must use JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

---

### Task 1: Support Zero-Length Logical Native Objects

**Files:**
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

- [ ] **Step 1: Write the failing zero-length allocator test**

Append this test to `YierdisStableNativeAllocatorTest`:

```java
@Test
public void zeroLengthObjectHasStableHandleAndCanGrowShrinkAndFree() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-zero-length");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 16)) {
        NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 0);
        try (NativeObjectView initial = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(0, initial.size());
        }

        try (NativeObjectView empty = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(0, empty.size());
            Assert.assertTrue(empty.capacity() > 0);
            empty.getBytes(0, new byte[0], 0, 0);
        }

        NativeHandle grown = allocator.realloc(handle, 3, NativeReallocPolicy.PRESERVE_PREFIX);
        Assert.assertEquals(handle.raw(), grown.raw());
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setBytes(0, new byte[] { 'a', 'b', 'c' }, 0, 3);
        }

        NativeHandle shrunk = allocator.realloc(handle, 0, NativeReallocPolicy.PRESERVE_PREFIX);
        Assert.assertEquals(handle.raw(), shrunk.raw());
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(0, view.size());
            view.getBytes(0, new byte[0], 0, 0);
            try {
                view.getByte(0);
                Assert.fail("zero-length object should reject byte reads");
            } catch (IndexOutOfBoundsException expected) {
                // expected
            }
        }

        allocator.free(handle);
        try {
            allocator.resolve(handle, NativeAccessMode.READ_ONLY);
            Assert.fail("expected stale handle after free");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("stale native handle"));
        }
    }
}
```

Required imports if missing:

```java
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest#zeroLengthObjectHasStableHandleAndCanGrowShrinkAndFree test
```

Expected: FAIL with an `IllegalArgumentException` containing `size must be > 0` or `newSize must be > 0`.

- [ ] **Step 3: Allow logical size zero in `YierdisStableNativeAllocator`**

In `YierdisStableNativeAllocator`, add this helper near the other private helpers:

```java
private static int physicalAllocationBytes(int logicalSize) {
    if (logicalSize < 0) {
        throw new IllegalArgumentException("size must be >= 0");
    }
    return Math.max(1, logicalSize);
}
```

Change `allocate(...)` to validate `size >= 0` and allocate physical bytes with the helper:

```java
@Override
public synchronized NativeHandle allocate(NativeObjectKind kind, int size) {
    ensureOpen();
    Objects.requireNonNull(kind, "kind");
    if (size < 0) {
        throw new IllegalArgumentException("size must be >= 0");
    }

    long startedNanos = System.nanoTime();
    YierdisNativeBlock block = pageAllocator.allocate(physicalAllocationBytes(size));
    boolean allocated = false;
    try {
        NativeHandle handle = objectTable.allocate(
                kind,
                size,
                block.capacity(),
                packedAddress(block),
                block.pageClass().ordinal(),
                epochManager.nextEpoch()
        );
        Allocation allocation = new Allocation(block);
        allocation.lastHandle = handle;
        allocations.put(handle.slotId(), allocation);
        logicalUsedBytes += size;
        reservedBytes += block.capacity();
        liveObjects++;
        allocated = true;
        return handle;
    } finally {
        recordAllocationLatency(System.nanoTime() - startedNanos);
        if (!allocated) {
            block.close();
        }
    }
}
```

Change the start of `realloc(...)`:

```java
if (newSize < 0) {
    throw new IllegalArgumentException("newSize must be >= 0");
}
```

Change moved realloc allocation:

```java
YierdisNativeBlock next = pageAllocator.allocate(physicalAllocationBytes(newSize));
```

Change defrag target allocation in `moveLiveObject(...)`:

```java
target = pageAllocator.allocate(physicalAllocationBytes(sourceMeta.size()));
```

Keep `copyPrefix(...)` unchanged; it already copies zero bytes safely because the loop does not execute when `len == 0`.

- [ ] **Step 4: Allow logical size zero in `YierdisNativeObjectTable`**

In `YierdisNativeObjectTable.allocate(...)`, replace the size check with:

```java
if (size < 0) {
    throw new IllegalArgumentException("size must be >= 0");
}
```

In `YierdisNativeObjectTable.updateLocation(...)`, replace the size check with:

```java
if (size < 0) {
    throw new IllegalArgumentException("size must be >= 0");
}
```

In `YierdisNativeObjectTable.publishMoved(...)`, replace the size check with:

```java
if (size < 0) {
    throw new IllegalArgumentException("size must be >= 0");
}
```

Do not change the `capacity < size` checks.

- [ ] **Step 5: Run allocator tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest test
```

Expected: PASS.

- [ ] **Step 6: Commit allocator zero-length support**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java
git commit -m "feat(memory): support zero-length native objects"
```

---

### Task 2: Migrate `StringRoot` To `NativeAllocator`

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLogTest.java`

- [ ] **Step 1: Add failing stable-handle tests for strings**

Append these tests to `StringRootTest`:

```java
@Test
public void appendPreservesStableValueHandleWhenReallocMovesObject() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-stable-append");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
         StringRoot root = new StringRoot(allocator)) {
        ValueHandle handle = root.store(new byte[] { 'a' });
        long raw = handle.raw();

        byte[] suffix = new byte[64 * 1024];
        for (int i = 0; i < suffix.length; i++) {
            suffix[i] = 'b';
        }

        Assert.assertEquals(1 + suffix.length, root.append(handle, suffix));
        Assert.assertEquals(raw, handle.raw());
        Assert.assertEquals(1 + suffix.length, root.length(handle));
        Assert.assertEquals('a', root.byteAt(handle, 0));
        Assert.assertEquals('b', root.byteAt(handle, suffix.length));
        Assert.assertTrue(allocator.stats().reallocMovedCount() > 0L);
    }
}

@Test
public void releasedStringHandleFailsThroughAllocatorStaleHandleDetection() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-stale");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
         StringRoot root = new StringRoot(allocator)) {
        ValueHandle handle = root.store(new byte[] { 'x' });
        root.release(handle);

        try {
            root.copy(handle);
            Assert.fail("expected stale string handle");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("stale native handle"));
        }
        Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));
    }
}

@Test
public void reusedStringSlotDoesNotReviveOldHandle() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-generation");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
         StringRoot root = new StringRoot(allocator)) {
        ValueHandle first = root.store(new byte[] { 'a' });
        root.release(first);

        ValueHandle second = root.store(new byte[] { 'b' });
        Assert.assertNotEquals(first.raw(), second.raw());
        Assert.assertArrayEquals(new byte[] { 'b' }, root.copy(second));

        try {
            root.copy(first);
            Assert.fail("expected old handle to remain stale");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("stale native handle"));
        }
    }
}

@Test
public void emptyStringHandleCanGrowAndShrinkWithoutChangingHandle() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-empty");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
         StringRoot root = new StringRoot(allocator)) {
        ValueHandle handle = root.store(new byte[0]);
        long raw = handle.raw();

        Assert.assertEquals(0, root.length(handle));
        Assert.assertArrayEquals(new byte[0], root.copy(handle));

        Assert.assertEquals(3, root.append(handle, new byte[] { 'a', 'b', 'c' }));
        Assert.assertEquals(raw, handle.raw());
        Assert.assertArrayEquals(new byte[] { 'a', 'b', 'c' }, root.copy(handle));

        root.overwrite(handle, new byte[0]);
        Assert.assertEquals(raw, handle.raw());
        Assert.assertEquals(0, root.length(handle));
        Assert.assertArrayEquals(new byte[0], root.copy(handle));
    }
}
```

Add imports:

```java
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
```

- [ ] **Step 2: Run string tests and verify failure**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=StringRootTest,YierdisHyperLogLogTest test
```

Expected: FAIL because `StringRoot` has no `NativeAllocator` constructor and released string handles are rejected by the root-local map, not by allocator stale-handle checks.

- [ ] **Step 3: Replace `StringRoot` storage fields and constructors**

In `StringRoot.java`, replace off-heap imports with native allocator imports:

```java
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.OffHeapSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
```

Remove these imports:

```java
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.foreign.YierdisForeignOffHeapAllocator;
```

Replace the fields:

```java
private final NativeAllocator allocator;
private final boolean ownsAllocator;
private final java.util.HashSet<Long> liveHandles = new java.util.HashSet<>();
private boolean closed;
```

Replace constructors:

```java
public StringRoot(YierdisFfmMemoryRuntime runtime) {
    this(new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
}

public StringRoot(NativeAllocator allocator) {
    this(allocator, false);
}

private StringRoot(NativeAllocator allocator, boolean ownsAllocator) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    this.ownsAllocator = ownsAllocator;
}

NativeAllocator allocator() {
    return allocator;
}
```

- [ ] **Step 4: Replace `StringRoot` public methods with allocator-backed implementations**

Use these method bodies in `StringRoot.java`:

```java
public synchronized ValueEncoding encoding(ValueHandle handle) {
    requireStringHandle(handle);
    return ValueEncoding.STRING_RAW;
}

public synchronized boolean contains(ValueHandle handle) {
    ensureOpen();
    return handle != null && liveHandles.contains(handle.raw());
}

public synchronized ValueHandle store(byte[] value) {
    ensureOpen();
    int len = value == null ? 0 : value.length;
    NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, len);
    boolean ok = false;
    try {
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, value, 0, len);
            }
        }
        liveHandles.add(handle.raw());
        ok = true;
        return ValueHandle.fromNativeHandle(handle);
    } finally {
        if (!ok) {
            allocator.free(handle);
        }
    }
}

public synchronized ValueHandle store(BytesSlice value) {
    ensureOpen();
    int len = value == null ? 0 : value.length();
    NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, len);
    boolean ok = false;
    try {
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, value, 0, len);
            }
        }
        liveHandles.add(handle.raw());
        ok = true;
        return ValueHandle.fromNativeHandle(handle);
    } finally {
        if (!ok) {
            allocator.free(handle);
        }
    }
}

public synchronized void overwrite(ValueHandle handle, byte[] value) {
    ensureOpen();
    int len = value == null ? 0 : value.length;
    resizePreservingHandle(handle, len);
    if (len > 0) {
        try (NativeObjectView view = allocator.resolve(handle.nativeHandle(), NativeAccessMode.READ_WRITE)) {
            view.setBytes(0, value, 0, len);
        }
    }
}

public synchronized void overwrite(ValueHandle handle, BytesSlice value) {
    ensureOpen();
    int len = value == null ? 0 : value.length();
    resizePreservingHandle(handle, len);
    if (len > 0) {
        try (NativeObjectView view = allocator.resolve(handle.nativeHandle(), NativeAccessMode.READ_WRITE)) {
            view.setBytes(0, value, 0, len);
        }
    }
}

public synchronized int append(ValueHandle handle, byte[] suffix) {
    ensureOpen();
    if (suffix == null || suffix.length == 0) {
        return length(handle);
    }
    int oldLen = length(handle);
    int nextLen = Math.addExact(oldLen, suffix.length);
    resizePreservingHandle(handle, nextLen);
    try (NativeObjectView view = allocator.resolve(handle.nativeHandle(), NativeAccessMode.READ_WRITE)) {
        view.setBytes(oldLen, suffix, 0, suffix.length);
    }
    return nextLen;
}

public synchronized int append(ValueHandle handle, BytesSlice suffix) {
    ensureOpen();
    if (suffix == null || suffix.length() == 0) {
        return length(handle);
    }
    int oldLen = length(handle);
    int suffixLen = suffix.length();
    int nextLen = Math.addExact(oldLen, suffixLen);
    resizePreservingHandle(handle, nextLen);
    try (NativeObjectView view = allocator.resolve(handle.nativeHandle(), NativeAccessMode.READ_WRITE)) {
        view.setBytes(oldLen, suffix, 0, suffixLen);
    }
    return nextLen;
}

public synchronized void ensureLength(ValueHandle handle, int requiredLen) {
    ensureOpen();
    if (requiredLen < 0) {
        throw new IllegalArgumentException("requiredLen must be >= 0");
    }
    int oldLen = length(handle);
    if (requiredLen <= oldLen) {
        return;
    }
    resizePreservingHandle(handle, requiredLen);
    try (NativeObjectView view = allocator.resolve(handle.nativeHandle(), NativeAccessMode.READ_WRITE)) {
        zeroFill(view, oldLen, requiredLen);
    }
}

public synchronized byte byteAt(ValueHandle handle, int index) {
    ensureOpen();
    try (NativeObjectView view = allocator.resolve(requireStringHandle(handle), NativeAccessMode.READ_ONLY)) {
        return view.getByte(index);
    }
}

public synchronized void setByteAt(ValueHandle handle, int index, byte value) {
    ensureOpen();
    try (NativeObjectView view = allocator.resolve(requireStringHandle(handle), NativeAccessMode.READ_WRITE)) {
        view.setByte(index, value);
    }
}

public synchronized OffHeapSlice slice(ValueHandle handle) {
    byte[] bytes = copy(handle);
    if (bytes.length == 0) {
        return EMPTY_SLICE;
    }
    return new HeapBackedOffHeapSlice(bytes);
}

public synchronized byte[] copy(ValueHandle handle) {
    ensureOpen();
    try (NativeObjectView view = allocator.resolve(requireStringHandle(handle), NativeAccessMode.READ_ONLY)) {
        int len = view.size();
        if (len == 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        view.getBytes(0, out, 0, len);
        return out;
    }
}

public synchronized int length(ValueHandle handle) {
    ensureOpen();
    try (NativeObjectView view = allocator.resolve(requireStringHandle(handle), NativeAccessMode.READ_ONLY)) {
        return view.size();
    }
}

@Override
public synchronized long estimatedBytes(ValueHandle handle) {
    ensureOpen();
    try (NativeObjectView view = allocator.resolve(requireStringHandle(handle), NativeAccessMode.READ_ONLY)) {
        return view.capacity();
    }
}

@Override
public synchronized void release(ValueHandle handle) {
    if (handle == null) {
        return;
    }
    allocator.free(requireStringHandle(handle));
    liveHandles.remove(handle.raw());
}
```

- [ ] **Step 5: Replace `StringRoot` cleanup and helper methods**

Replace `clear()` and `close()` with:

```java
@Override
public synchronized void clear() {
    ensureOpen();
    RuntimeException failure = null;
    Long[] handles = liveHandles.toArray(Long[]::new);
    for (long raw : handles) {
        try {
            allocator.free(NativeHandle.fromRaw(raw));
            liveHandles.remove(raw);
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
    }
    if (failure != null) {
        throw failure;
    }
}

@Override
public synchronized void close() {
    if (closed) {
        return;
    }
    RuntimeException failure = null;
    try {
        clear();
    } catch (RuntimeException e) {
        failure = e;
    }
    if (ownsAllocator) {
        try {
            allocator.close();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
    }
    closed = true;
    if (failure != null) {
        throw failure;
    }
}
```

Delete the old `allocate`, `overwrite(Slot, ...)`, `ensureCapacity`, `copy(OffHeapBuf, ...)`, `nextCapacity`, `closeSlotBuffer`, `requireSlot`, `newHandle`, and `Slot` helper code.

Add these helpers:

```java
private NativeHandle requireStringHandle(ValueHandle handle) {
    Objects.requireNonNull(handle, "handle");
    NativeHandle nativeHandle = handle.nativeHandle();
    if (nativeHandle.domain() != NativeObjectKind.STRING_BYTES.domain()
            || nativeHandle.kindCode() != NativeObjectKind.STRING_BYTES.code()) {
        throw new IllegalArgumentException("string value handle kind mismatch: " + nativeHandle.raw());
    }
    return nativeHandle;
}

private void resizePreservingHandle(ValueHandle handle, int len) {
    NativeHandle nativeHandle = requireStringHandle(handle);
    NativeHandle resized = allocator.realloc(nativeHandle, len, NativeReallocPolicy.PRESERVE_PREFIX);
    if (resized.raw() != nativeHandle.raw()) {
        throw new IllegalStateException("native string realloc changed stable handle");
    }
}

private static void zeroFill(NativeObjectView view, int from, int toExclusive) {
    int remaining = toExclusive - from;
    int offset = from;
    while (remaining > 0) {
        int chunk = Math.min(remaining, ZERO_BUF.length);
        view.setBytes(offset, ZERO_BUF, 0, chunk);
        offset += chunk;
        remaining -= chunk;
    }
}

private static final class HeapBackedOffHeapSlice implements OffHeapSlice {
    private final byte[] bytes;

    private HeapBackedOffHeapSlice(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
    }

    @Override
    public int length() {
        return bytes.length;
    }

    @Override
    public byte getByte(int index) {
        if (index < 0 || index >= bytes.length) {
            throw new IndexOutOfBoundsException();
        }
        return bytes[index];
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        Objects.requireNonNull(dst, "dst");
        if (len < 0 || index < 0 || dstOff < 0 || index > bytes.length - len || dstOff > dst.length - len) {
            throw new IndexOutOfBoundsException();
        }
        System.arraycopy(bytes, index, dst, dstOff, len);
    }

    @Override
    public void writeTo(BytesSink out) {
        Objects.requireNonNull(out, "out");
        out.writeBytes(bytes, 0, bytes.length);
    }
}
```

- [ ] **Step 6: Run string and HLL tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=StringRootTest,YierdisHyperLogLogTest test
```

Expected: PASS.

- [ ] **Step 7: Commit string root migration**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java
git commit -m "feat(db): store string payloads in native allocator"
```

---

### Task 3: Wire One Shared DB Native Allocator

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java`

- [ ] **Step 1: Write the failing shared allocator assembly test**

Add this test to `YierdisDbConstructionTest`:

```java
@Test
public void storageComponentsShareOneNativeAllocatorForEntriesAndStrings() {
    YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(null, null, false, false);
    try {
        Assert.assertNotNull(storage.nativeAllocator);
        Assert.assertSame(storage.nativeAllocator, storage.entries.allocator());
        Assert.assertSame(storage.nativeAllocator, storage.stringRoot.allocator());

        storage.stringRoot.store(bytes("value"));
        storage.entries.allocate(new EntryRecord(
                1L,
                valueHandle(2L),
                3,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        ));

        Assert.assertTrue(storage.nativeAllocator.stats().objectCount(NativeObjectKind.ENTRY_RECORD) > 0L);
        Assert.assertTrue(storage.nativeAllocator.stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
    } finally {
        storage.resources.releaseAll(
                storage.expires,
                storage.entries,
                storage.keyDirectory,
                storage.stringRoot,
                storage.listRoot,
                storage.hashRoot,
                storage.setRoot,
                storage.zsetRoot
        );
    }
}
```

The file already imports `NativeObjectKind`, `EntryRecord`, `ValueType`, and `ValueEncoding`.

- [ ] **Step 2: Add an allocator accessor to `EntryTable`**

Add this package-private method near `runtime()`:

```java
NativeAllocator allocator() {
    return allocator;
}
```

- [ ] **Step 3: Add native allocator ownership to `YierdisDbOwnedResources`**

Add import:

```java
import yier.bubu.redis.memory.api.NativeAllocator;
```

Add fields:

```java
private final NativeAllocator nativeAllocator;
private final boolean ownsNativeAllocator;
```

Replace the constructor with:

```java
YierdisDbOwnedResources(
        YierdisFfmMemoryRuntime memoryRuntime,
        OffHeapAllocator offHeapAllocator,
        NativeAllocator nativeAllocator,
        boolean ownsMemoryRuntime,
        boolean ownsOffHeapAllocator,
        boolean ownsNativeAllocator
) {
    this.memoryRuntime = memoryRuntime;
    this.offHeapAllocator = offHeapAllocator;
    this.nativeAllocator = nativeAllocator;
    this.ownsMemoryRuntime = ownsMemoryRuntime;
    this.ownsOffHeapAllocator = ownsOffHeapAllocator;
    this.ownsNativeAllocator = ownsNativeAllocator;
}
```

In `close()`, close the stable native allocator after the legacy off-heap allocator and before the runtime:

```java
if (ownsNativeAllocator && nativeAllocator != null) {
    try {
        nativeAllocator.close();
    } catch (Throwable t) {
        failure = recordFailure(failure, t);
    }
}
```

Keep the existing off-heap and runtime close blocks.

- [ ] **Step 4: Create the shared allocator in `YierdisDbStorageComponents`**

Add imports:

```java
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
```

Add field:

```java
final NativeAllocator nativeAllocator;
```

Add constructor parameter and assignment:

```java
NativeAllocator nativeAllocator,
```

```java
this.nativeAllocator = nativeAllocator;
```

In `create(...)`, after resolving `resolvedRuntime`, create:

```java
NativeAllocator nativeAllocator = new YierdisStableNativeAllocator(
        resolvedRuntime,
        ENTRY_TABLE_NATIVE_SLOT_CAPACITY
);
```

Create resources with the new constructor:

```java
YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
        resolvedRuntime,
        resolvedAllocator,
        nativeAllocator,
        resolvedOwnsRuntime,
        resolvedOwnsAllocator,
        true
);
```

Replace entry table creation:

```java
EntryTable entries = new EntryTable(resolvedRuntime, nativeAllocator);
```

Replace string root creation:

```java
StringRoot stringRoot = new StringRoot(nativeAllocator);
```

Pass `nativeAllocator` to the `YierdisDbStorageComponents` constructor result.

- [ ] **Step 5: Pass native allocator through `YierdisDbKeyLifecycle`**

In `YierdisDbKeyLifecycle`, import:

```java
import yier.bubu.redis.memory.api.NativeAllocator;
```

Add field:

```java
private final NativeAllocator nativeAllocator;
```

Add constructor parameter immediately after `OffHeapAllocator offHeapAllocator`:

```java
NativeAllocator nativeAllocator,
```

Assign:

```java
this.nativeAllocator = java.util.Objects.requireNonNull(nativeAllocator, "nativeAllocator");
```

Add accessor near `offHeapAllocator()`:

```java
NativeAllocator nativeAllocator() {
    return nativeAllocator;
}
```

In `YierdisDbComponentFactory`, update construction:

```java
YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
        storage.expires,
        storage.offHeapAllocator,
        storage.nativeAllocator,
        storage.memoryRuntime,
        storage.entries,
        storage.keyDirectory,
        storage.stringRoot,
        storage.listRoot,
        storage.hashRoot,
        storage.setRoot,
        storage.zsetRoot,
        owner::nextLruClock,
        owner::adjustUsedBytes
);
```

- [ ] **Step 6: Run construction tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisDbConstructionTest,EntryTableContractTest test
```

Expected: PASS.

- [ ] **Step 7: Commit shared allocator wiring**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java
git commit -m "feat(db): share native allocator across entries and strings"
```

---

### Task 4: Fix Memory Accounting And DB Regression Tests

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java`

- [ ] **Step 1: Add failing accounting assertions**

In `NativeStorageRegressionTest.nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent`, after `YierdisMemoryStats populated = db.memory().memoryStats();`, add:

```java
Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes() > 0L);
```

Add import:

```java
import yier.bubu.redis.memory.api.NativeObjectKind;
```

After deleting all keys and asserting memory stats are zero, add:

```java
Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes());
```

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent test
```

Expected: FAIL if memory reporter still double-counts entry bytes or if string deletes do not free allocator objects.

- [ ] **Step 2: Count shared allocator logical bytes once**

In `YierdisDbMemoryReporter.directNativeBytes()`, start the method with shared allocator logical bytes:

```java
private long directNativeBytes() {
    long total = safeNativeAllocatorLogicalBytes();
    if (expires instanceof YierdisFfmExpireIndex ffmExpires) {
        total = addSaturating(total, ffmExpires.nativeBytes());
    }
    NativeKeyDirectory keyDirectory = keyLifecycle.keyDirectory();
    if (keyDirectory != null) {
        total = addSaturating(total, keyDirectory.nativeBytes());
    }
    ListRoot listRoot = keyLifecycle.listRoot();
    if (listRoot != null) {
        total = addSaturating(total, listRoot.nativeBytes());
    }
    HashRoot hashRoot = keyLifecycle.hashRoot();
    if (hashRoot != null) {
        total = addSaturating(total, hashRoot.nativeBytes());
    }
    SetRoot setRoot = keyLifecycle.setRoot();
    if (setRoot != null) {
        total = addSaturating(total, setRoot.nativeBytes());
    }
    ZSetRoot zsetRoot = keyLifecycle.zsetRoot();
    if (zsetRoot != null) {
        total = addSaturating(total, zsetRoot.nativeBytes());
    }
    return total;
}
```

Add helper:

```java
private long safeNativeAllocatorLogicalBytes() {
    var allocator = keyLifecycle.nativeAllocator();
    if (allocator == null) {
        return 0L;
    }
    try {
        return Math.max(0L, allocator.stats().logicalUsedBytes());
    } catch (Throwable ignored) {
        return 0L;
    }
}
```

Remove the `EntryTable entryTable = ... entryTable.nativeBytes()` block from `directNativeBytes()` because entry records are included in the shared allocator logical bytes.

- [ ] **Step 3: Update off-heap string tests to assert stable allocator ownership**

In `OffHeapStringStorageTest`, rename `setGetUsesFfmSliceAndDelFrees` to:

```java
public void setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes()
```

Inside that test, after SET, add:

```java
Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
```

After DEL, add:

```java
Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
```

In `expiredKeyStringPayloadIsReleasedWhenOverwrittenByOtherCommand`, replace the custom `YierdisForeignOffHeapAllocator` construction with:

```java
YierdisDb db = new YierdisDb();
```

Replace allocator assertions with:

```java
Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
```

and after list overwrite:

```java
Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
```

Replace `overwriteReusesFfmBufferUnderHardCap` with this test:

```java
@Test
public void overwritePreservesStableStringHandle() {
    YierdisDb db = new YierdisDb();
    try {
        db.bindToCurrentThread();
        byte[] key = b("k");
        byte[] v1 = b("hello");
        byte[] v2 = b("world");

        Assert.assertTrue(db.writes().strings().setString(key, v1, SetMode.NORMAL, null).value());
        long raw = db.keyLifecycle().liveEntryRecord(key).valueHandle().raw();

        Assert.assertTrue(db.writes().strings().setString(key, v2, SetMode.NORMAL, null).value());
        Assert.assertEquals(raw, db.keyLifecycle().liveEntryRecord(key).valueHandle().raw());

        RecordingBulkOutput out = new RecordingBulkOutput();
        db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
        Assert.assertTrue(out.usedOffHeapSlice);
        Assert.assertArrayEquals(v2, out.bytes);
    } finally {
        db.shutdown();
    }
}
```

Replace `ffmMaxBytesRejectsOversizedSet` with this deprecated-constructor compatibility test:

```java
@Test
public void deprecatedOffHeapAllocatorDoesNotOwnStringPayloadsAfterStableAllocatorMigration() {
    YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(4);
    YierdisDb db = new YierdisDb(allocator, 0, "noeviction", 5, 5, 5);
    try {
        db.bindToCurrentThread();
        Assert.assertTrue(db.writes().strings().setString(b("k"), b("hello"), SetMode.NORMAL, null).value());
        Assert.assertEquals(0L, allocator.usedBytes());
        Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
    } finally {
        db.shutdown();
    }
}
```

Add import:

```java
import yier.bubu.redis.memory.api.NativeObjectKind;
```

- [ ] **Step 4: Run DB regression and memory tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,OffHeapStringStorageTest,YierdisDbMemoryReporterTest,YierdisDbConstructionTest test
```

Expected: PASS.

- [ ] **Step 5: Commit accounting and DB tests**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java
git commit -m "test(db): verify stable allocator string accounting"
```

---

### Task 5: Update Native Memory Documentation

**Files:**
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/ffm-usage.md`

- [ ] **Step 1: Update allocator handle docs**

In `docs/project-docs/native-allocator-and-handles.md`, replace the paragraph that says only `EntryHandle` is object-table-backed with:

```markdown
注意：当前 `EntryHandle` 和 string `ValueHandle` 是 object-table-backed allocator handle。`EntryHandle` 包装 `ENTRY_RECORD`；string `ValueHandle` 包装 `STRING_BYTES`。集合 roots 的 `ValueHandle` 仍然是 root-local typed identity，除非对应 root 明确迁移到 `NativeAllocator`，不能把任意集合 `ValueHandle` 直接拿去 `NativeAllocator.resolve(...)`。
```

In the value/root section, add:

```markdown
`StringRoot` 已迁移到 `NativeAllocator`：`store` 分配 `STRING_BYTES`，`append`/`ensureLength` 通过 `realloc(..., PRESERVE_PREFIX)` 保持 stable handle，`slice`/`copy`/byte access 只在方法内部短暂 resolve `NativeObjectView`，`release` 调用 allocator free。
```

- [ ] **Step 2: Update FFM usage docs**

In `docs/project-docs/ffm-usage.md`, replace the current-state note that says `StringRoot` keeps a heap slot map and off-heap buffer with:

```markdown
- `StringRoot` 的 payload 已由 DB 级 shared `NativeAllocator` 管理，kind 为 `STRING_BYTES`。`ValueHandle` 对 string 来说是 stable allocator handle；append/ensureLength 使用 allocator realloc 保持 handle 不变。`slice` 对外仍返回 `OffHeapSlice` 接口，但不会暴露可逃逸的 allocator view。
```

Add this key isolation note near the key directory section:

```markdown
- key bytes 仍由 `YierdisFfmBlobStore` / `NativeKeyDirectory` 管理，不属于 stable allocator object table。active defrag 和 allocator stats 只覆盖 allocator-backed entry/string 对象；key bytes 会在后续迁移到 `NativeObjectKind.KEY_BYTES`。
```

- [ ] **Step 3: Check docs for stale statements**

Run:

```bash
rg -n "StringRoot.*OffHeapAllocator|StringRoot.*slot map|only `EntryHandle`|只有 `EntryHandle`|string root.*OffHeap" docs/project-docs docs/superpowers/specs
```

Expected: no remaining current-state statements that claim strings are still `OffHeapAllocator`-owned. Historical plan files may still mention old behavior; leave historical files unchanged.

- [ ] **Step 4: Commit docs**

```bash
git add docs/project-docs/native-allocator-and-handles.md docs/project-docs/ffm-usage.md
git commit -m "docs: mark string payloads allocator-backed"
```

---

### Task 6: Run Full Verification For This Slice

**Files:**
- No source edits unless verification fails.

- [ ] **Step 1: Run allocator module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am test
```

Expected: PASS.

- [ ] **Step 2: Run DB memory module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 3: Run integration leak regression tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=OffHeapLeakRegressionTest test
```

Expected: PASS.

- [ ] **Step 4: Verify runtime used bytes return to zero in targeted tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent,OffHeapStringStorageTest#setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes test
```

Expected: PASS and no native leak assertion failures.

- [ ] **Step 5: Inspect git status**

Run:

```bash
git status --short
```

Expected: clean except for unrelated user-created files such as `yierdis.md`.

- [ ] **Step 6: Commit any verification-only fixes**

If verification required source or test fixes, commit only those changed files:

```bash
git add <changed-files-from-this-slice>
git commit -m "fix(db): stabilize native string allocator migration"
```

If no fixes were required, do not create an empty commit.

## Self-Review Notes

- Spec coverage: this plan covers DB allocator ownership, `StringRoot` migration, string stale handle tests, generation reuse, memory accounting, shutdown cleanup, and documentation of key isolation.
- Intentional gaps: key bytes migration, collection root migration, DB defrag maintenance, scan/snapshot epochs, stress suite expansion, and benchmarks are not in this plan because they are separate subsystems.
- Type consistency: the plan uses existing `NativeAllocator`, `NativeObjectView`, `NativeAccessMode`, `NativeReallocPolicy`, `NativeAllocatorStats`, `ValueHandle.nativeHandle()`, and `NativeObjectKind.STRING_BYTES` APIs.
