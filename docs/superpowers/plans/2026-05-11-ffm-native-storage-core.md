# FFM Native Storage Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status:** Planned; the design was approved on 2026-05-11.

**Goal:** Replace the current object-backed DB storage core with an entry-table-and-handle FFM model, starting with slab-backed allocation and then migrating string, collection, and introspection paths without changing the command-facing APIs.

**Architecture:** Add a slab allocator in `yierdis-memory-ffm` so the storage layer stops treating every payload as its own region. Build a native `EntryTable` plus `NativeKeyDirectory` in `yierdis-db-memory`, then move each data type onto `TypeRoot` implementations while `YierdisObject` becomes a compatibility adapter only. Keep the existing DB API, command API, and transport layers stable while the storage internals change.

**Tech Stack:** Java 25, JDK 25 FFM API, Maven, JUnit 4, existing `YierdisFfmMemoryRuntime`, `OffHeapAllocator`, `DbReads`, `DbWrites`, and the current `yierdis-db-memory` mutation ledger.

---

## File Structure

Create or modify these groups:

- `yierdis-memory/yierdis-memory-ffm`: slab allocator and allocator tests.
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry`: `EntryHandle`, `EntryRecord`, `EntryTable`, `ValueHandle`, `TypeRoot`, and type-specific roots.
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace`: native key directory wiring and rehash behavior.
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory`: DB component wiring, lifecycle, estimation, memory reporting, and introspection.
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value`: compatibility adapters for the old object wrappers.
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory`: regression tests for string, list, set, hash, zset, HLL, TTL, memory, and rehash behavior.
- `docs/db-internals.md` and `docs/ffm-usage.md`: only after code changes land, to describe the new canonical storage model.

---

### Task 1: Add A Slab-Backed FFM Allocator

**Files:**
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlab.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocatorTest.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocatorTest.java`

- [ ] **Step 1: Write the failing slab allocator test**

```java
package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.OffHeapBuf;

public class YierdisFfmSlabAllocatorTest {
    @Test
    public void slabAllocatorSuballocatesAndReleasesToZero() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slab-test")) {
            YierdisFfmSlabAllocator allocator = new YierdisFfmSlabAllocator(runtime, 64);
            try {
                OffHeapBuf first = allocator.allocate(16);
                OffHeapBuf second = allocator.allocate(16);
                first.setByte(0, (byte) 7);
                second.setByte(0, (byte) 9);
                Assert.assertEquals(7, first.getByte(0));
                Assert.assertEquals(9, second.getByte(0));
                first.close();
                second.close();
                Assert.assertEquals(0L, allocator.usedBytes());
            } finally {
                allocator.close();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
```

- [ ] **Step 2: Run the new allocator test and verify it fails**

Run: `mvn -pl yierdis-memory/yierdis-memory-ffm test -Dtest=YierdisFfmSlabAllocatorTest`

Expected: FAIL because the slab allocator classes do not exist yet.

- [ ] **Step 3: Add the slab allocator implementation**

Create the allocator as a small internal pool that:

```java
public final class YierdisFfmSlabAllocator implements AutoCloseable {
    public OffHeapBuf allocate(int capacity) {
        return null;
    }

    public long usedBytes() {
        return 0L;
    }

    public long maxBytes() {
        return 0L;
    }

    public void close() {
    }
}
```

Back it with `YierdisFfmSlab` records that hold one large runtime region and a free-list of sub-blocks. Update `YierdisForeignOffHeapAllocator.allocate(...)` to delegate to the slab allocator instead of opening one runtime region per buffer.

- [ ] **Step 4: Run the allocator tests again**

Run: `mvn -pl yierdis-memory/yierdis-memory-ffm test -Dtest=YierdisFfmSlabAllocatorTest,YierdisForeignOffHeapAllocatorTest`

Expected: PASS, and the legacy allocator test still proves the public `OffHeapAllocator` contract did not change.

- [ ] **Step 5: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlab.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocatorTest.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocatorTest.java
git commit -m "feat: add slab-backed ffm allocator"
```

---

### Task 2: Add Native Entry Table And Key Directory

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/TypeRoot.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponents.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandleContractTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireIndexTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java`

- [ ] **Step 1: Write the failing entry contract tests**

```java
package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.ValueEncoding;
import yier.bubu.redis.storage.api.ValueType;

public class EntryTableContractTest {
    @Test
    public void entryRecordCarriesNativeMetadata() {
        EntryRecord record = new EntryRecord(
                11L,
                new ValueHandle(22L),
                33,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                7,
                99L,
                123L,
                456L
        );

        Assert.assertEquals(11L, record.keyHandle());
        Assert.assertEquals(22L, record.valueHandle().raw());
        Assert.assertEquals(ValueType.STRING, record.type());
        Assert.assertEquals(ValueEncoding.STRING_RAW, record.encoding());
    }

    @Test
    public void entryTableAllocatesAndReleasesHandles() {
        try (yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime runtime =
                     new yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime("entry-test")) {
            EntryTable table = new EntryTable(runtime, 64);
            EntryHandle handle = table.allocate(new EntryRecord(
                    1L, new ValueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            Assert.assertNotNull(table.get(handle));
            table.release(handle);
            Assert.assertNull(table.get(handle));
        }
    }
}
```

- [ ] **Step 2: Run the new entry tests and verify they fail**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=EntryHandleContractTest,EntryTableContractTest,NativeKeyDirectoryTest`

Expected: FAIL because the new native entry classes do not exist yet.

- [ ] **Step 3: Introduce the native key directory and entry table**

Implement the minimal API surface:

```java
public record EntryHandle(long raw) {
}

public record ValueHandle(long raw) {
}

public record EntryRecord(
    long keyHandle,
    ValueHandle valueHandle,
    int keyHash,
    ValueType type,
    ValueEncoding encoding,
    int flags,
    long expireAtMillis,
    long version,
    long lruOrLfu
) {
}

public interface TypeRoot extends AutoCloseable {
    ValueType type();
    ValueEncoding encoding();
    long estimatedBytes(ValueHandle handle);
    void release(ValueHandle handle);
}

public final class EntryTable implements AutoCloseable {
    public EntryHandle allocate(EntryRecord record) {
        return null;
    }

    public EntryRecord get(EntryHandle handle) {
        return null;
    }

    public EntryRecord replace(EntryHandle handle, EntryRecord record) {
        return null;
    }

    public void release(EntryHandle handle) {
    }
}
```

`NativeKeyDirectory` should own the key blob storage and map key bytes to `EntryHandle`, not to `YierdisObject`. It should expose a rehashable lookup/compute API so `YierdisDbKeyLifecycle` can keep its single-threaded mutation gate without exposing heap objects.

- [ ] **Step 4: Rewire DB assembly and lifecycle to the new model**

Update `YierdisDbStorageComponents` and `YierdisDbComponents` to carry `EntryTable` and `NativeKeyDirectory`. Update `YierdisDbComponentFactory` so the storage graph now builds:

```text
YierdisFfmMemoryRuntime
  -> slab allocator
  -> EntryTable
  -> NativeKeyDirectory
  -> YierdisDbKeyLifecycle
  -> type ops
```

Change `YierdisDbKeyLifecycle` remapping methods to operate on `EntryRecord` instead of `YierdisObject`, and add explicit `entryHandle(...)`, `entryRecord(...)`, `liveEntryRecord(...)`, and `unlinkEntry(...)` methods. Update `YierdisDbMemoryEstimator`, `YierdisDbMemoryReporter`, and `YierdisDbIntrospection` so they read native metadata instead of reading `YierdisObject.estimatedBytes` and object fields directly.

- [ ] **Step 5: Run the native directory and lifecycle tests**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=EntryHandleContractTest,EntryTableContractTest,NativeKeyDirectoryTest,YierdisDbConstructionTest,ExpireIndexTest,MemoryStatsAccountingConsistencyTest,YierdisDbMemoryEstimatorTest,YierdisFfmRehashConsistencyTest`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponents.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireIndexTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java
git commit -m "feat: add native entry directory"
```

---

### Task 3: Migrate String And HyperLogLog Onto Value Handles

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapBytesViewTtlRegressionTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java`

- [ ] **Step 1: Write the failing string-root tests**

```java
package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueEncoding;

public class StringRootTest {
    @Test
    public void stringRootOverwritesWithoutReintroducingHeapPayloads() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root")) {
            StringRoot root = new StringRoot(runtime);
            ValueHandle handle = root.store(new byte[] { 'h', 'e', 'l', 'l', 'o' });
            Assert.assertEquals(ValueEncoding.STRING_RAW, root.encoding(handle));

            BytesSlice slice = root.slice(handle);
            byte[] copy = new byte[slice.length()];
            slice.getBytes(0, copy, 0, copy.length);
            Assert.assertArrayEquals(new byte[] { 'h', 'e', 'l', 'l', 'o' }, copy);

            root.overwrite(handle, new byte[] { 'w', 'o', 'r', 'l', 'd' });
            Assert.assertArrayEquals(new byte[] { 'w', 'o', 'r', 'l', 'd' }, root.copy(handle));
        }
    }
}
```

- [ ] **Step 2: Run the string tests and verify they fail**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=StringRootTest,OffHeapStringStorageTest,OffHeapBytesViewTtlRegressionTest,YierdisDbMemoryEstimatorTest`

Expected: FAIL because `StringRoot` and the new handle-based entry path do not exist yet.

- [ ] **Step 3: Move string reads and writes onto `StringRoot`**

Introduce `StringRoot` as the canonical native storage for string payloads:

```java
public final class StringRoot implements TypeRoot {
    public ValueHandle store(byte[] value) {
        return null;
    }

    public ValueHandle store(BytesSlice value) {
        return null;
    }

    public void overwrite(ValueHandle handle, BytesSlice value) {
    }

    public OffHeapSlice slice(ValueHandle handle) {
        return null;
    }

    public byte[] copy(ValueHandle handle) {
        return null;
    }
}
```

Update `YierdisStringOps` so `set`, `append`, `setBit`, `getStringValue`, and `getStringBytes` resolve an `EntryRecord`, then operate on the record’s `ValueHandle` instead of on `YierdisObject.payload`. Keep `YierdisObject` only as a compatibility adapter that delegates to `StringRoot`.

Update `YierdisHyperLogLog` so `pfadd`, `pfcount`, and `pfmerge` work directly on the root-backed string bytes and only use `YierdisObject` as a temporary compatibility layer if a call path has not yet been migrated.

- [ ] **Step 4: Run the string tests again**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=StringRootTest,OffHeapStringStorageTest,OffHeapBytesViewTtlRegressionTest,YierdisDbMemoryEstimatorTest`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapStringStorageTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapBytesViewTtlRegressionTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java
git commit -m "feat: route string storage through native roots"
```

---

### Task 4: Migrate List Storage Onto Native Roots

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ListRootTest.java`

- [ ] **Step 1: Write the failing list-root tests**

```java
package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.util.List;

public class ListRootTest {
    @Test
    public void listRootSupportsPushPopAndStreaming() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-root")) {
            ListRoot root = new ListRoot(runtime);
            ValueHandle handle = root.create();
            root.rpush(handle, List.of("a".getBytes(), "b".getBytes(), "c".getBytes()));
            Assert.assertEquals(3, root.size(handle));
            Assert.assertArrayEquals("a".getBytes(), root.lpop(handle, 1).get(0));
            Assert.assertEquals(2, root.size(handle));
        }
    }
}
```

- [ ] **Step 2: Run the list tests and verify they fail**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=ListRootTest,ListValueTest,OffHeapCollectionReadStreamingTest`

Expected: FAIL because `ListRoot` does not exist and `YierdisListOps` still reads from `ListValue`.

- [ ] **Step 3: Move list operations onto `ListRoot`**

Implement `ListRoot` as the canonical native layout for packed and quicklist-like list storage:

```java
public final class ListRoot implements TypeRoot {
    public ValueHandle create() {
        return null;
    }

    public void lpush(ValueHandle handle, List<byte[]> values) {
    }

    public void rpush(ValueHandle handle, List<byte[]> values) {
    }

    public List<byte[]> lpop(ValueHandle handle, int count) {
        return null;
    }

    public List<byte[]> rpop(ValueHandle handle, int count) {
        return null;
    }

    public BulkStringSequence range(ValueHandle handle, int start, int stop) {
        return null;
    }
}
```

Update `YierdisListOps` so all writes and reads use the entry’s `ValueHandle` and `ListRoot`. Keep `ListValue` only as a compatibility adapter until the old object-based path is gone.

- [ ] **Step 4: Run the list tests again**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=ListRootTest,ListValueTest,OffHeapCollectionReadStreamingTest`

Expected: PASS, with streaming still producing `OffHeapSlice` values.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ListRootTest.java
git commit -m "feat: route list storage through native roots"
```

---

### Task 5: Migrate Hash, Set, And ZSet Onto Native Roots

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java`
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/HashValueTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ZSetValueTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/SetValueTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/CollectionRootTest.java`

- [ ] **Step 1: Write the failing collection-root tests**

```java
package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.util.List;

public class CollectionRootTest {
    @Test
    public void hashSetAndZsetRootsRoundTripMembers() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root")) {
            HashRoot hash = new HashRoot(runtime);
            SetRoot set = new SetRoot(runtime);
            ZSetRoot zset = new ZSetRoot(runtime);

            ValueHandle hashHandle = hash.create();
            ValueHandle setHandle = set.create();
            ValueHandle zsetHandle = zset.create();

            hash.hset(hashHandle, "field".getBytes(), "value".getBytes());
            set.sadd(setHandle, List.of("alpha".getBytes(), "beta".getBytes()));
            zset.zadd(zsetHandle, List.of("1".getBytes(), "m1".getBytes(), "2".getBytes(), "m2".getBytes()));

            Assert.assertArrayEquals("value".getBytes(), hash.hget(hashHandle, "field".getBytes()));
            Assert.assertEquals(2, set.size(setHandle));
            Assert.assertEquals(2, zset.size(zsetHandle));
        }
    }
}
```

- [ ] **Step 2: Run the collection tests and verify they fail**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=CollectionRootTest,HashValueTest,SetValueTest,ZSetValueTest,OffHeapCollectionReadStreamingTest`

Expected: FAIL because the new native collection roots do not exist yet.

- [ ] **Step 3: Move hash, set, and zset operations onto native roots**

Implement the three native roots and switch the existing ops to them:

```java
public final class HashRoot implements TypeRoot {
}

public final class SetRoot implements TypeRoot {
}

public final class ZSetRoot implements TypeRoot {
}
```

`HashValue`, `SetValue`, and `ZSetValue` should become compatibility adapters around those roots. `YierdisHashOps`, `YierdisSetOps`, and `YierdisZSetOps` should fetch the entry’s `ValueHandle`, route through the new roots, and only fall back to the adapters for old call paths that have not been removed yet.

Update `YierdisDbMemoryReporter`, `YierdisDbIntrospection`, and `YierdisDbMemoryEstimator` so they derive `OBJECT ENCODING`, `MEMORY USAGE`, and size estimates from entry metadata plus root state rather than from the old object payload graph.

- [ ] **Step 4: Run the collection tests again**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=CollectionRootTest,HashValueTest,SetValueTest,ZSetValueTest,OffHeapCollectionReadStreamingTest`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbIntrospection.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimator.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/HashValueTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/SetValueTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ZSetValueTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapCollectionReadStreamingTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/CollectionRootTest.java
git commit -m "feat: route collection storage through native roots"
```

---

### Task 6: Retire Compatibility Assumptions, Update Docs, And Run Full Regression

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapKeysToggleTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapKeyspaceTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireIndexTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java`
- Modify: `docs/db-internals.md`
- Modify: `docs/ffm-usage.md`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`

- [ ] **Step 1: Write the failing end-to-end regression test**

```java
package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.testkit.TestBytes;

import java.util.List;

public class NativeStorageRegressionTest {
    @Test
    public void allNativeRootsReleaseToZeroAfterDelete() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(TestBytes.b("s"), TestBytes.b("v"), SetMode.NORMAL, null);
            db.writes().lists().rpush(TestBytes.b("l"), List.of(TestBytes.b("a")));
            db.writes().hashes().hset(TestBytes.b("h"), List.of(TestBytes.b("f"), TestBytes.b("v"))).value();
            db.writes().sets().sadd(TestBytes.b("set"), List.of(TestBytes.b("m"))).value();
            db.writes().zsets().zadd(TestBytes.b("z"), List.of(TestBytes.b("1"), TestBytes.b("m"))).value();
            db.writes().hll().pfadd(TestBytes.b("hll"), List.of(TestBytes.b("x"))).value();

            Assert.assertTrue(db.size() >= 6);
            db.writes().keyspace().del(List.of(TestBytes.b("s"), TestBytes.b("l"), TestBytes.b("h"), TestBytes.b("set"), TestBytes.b("z"), TestBytes.b("hll"))).value();
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.memory().memoryStats().usedBytes());
        } finally {
            db.shutdown();
        }
    }
}
```

- [ ] **Step 2: Run the regression suite and verify it fails**

Run: `mvn -pl yierdis-db/yierdis-db-memory test -Dtest=NativeStorageRegressionTest,YierdisDbMemoryEstimatorTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,ExpireIndexTest,MemoryStatsAccountingConsistencyTest,YierdisFfmRehashConsistencyTest`

Expected: FAIL if any compatibility adapter still owns a hot path or if the docs and tests still describe the old object-backed model.

- [ ] **Step 3: Strip the remaining hot-path object assumptions**

Make `YierdisObject` a compatibility adapter only. Its job after this task is to bridge old helper code to `EntryRecord` and `TypeRoot`; it must not own canonical storage state. Remove any last `YierdisObject`-based assumptions from the estimator, reporter, introspection, TTL cleanup, and delete paths.

Update the two internal docs so they describe the new model:

```text
FFM slabs
+ 64-bit handles
+ EntryTable
+ NativeKeyDirectory
+ TypeRoot per data type
+ compatibility-only YierdisObject
```

- [ ] **Step 4: Run the full module regression**

Run:

```bash
mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
mvn -pl yierdis-tests/yierdis-architecture-tests -am test -Dsurefire.failIfNoSpecifiedTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/OffHeapKeysToggleTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapDbSmokeTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/UnsafeOffHeapKeyspaceTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireIndexTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java \
        docs/db-internals.md docs/ffm-usage.md
git commit -m "feat: finish native storage core migration"
```
