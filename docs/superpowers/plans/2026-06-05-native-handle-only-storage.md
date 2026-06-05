# Native Handle Only Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove heap and legacy blob-ref/offheap storage support and make Yierdis storage use one FFM-backed stable native handle model end to end.

**Architecture:** The only storage memory model is `YierdisFfmMemoryRuntime` plus `YierdisStableNativeAllocator`, with stable `NativeHandle` identities for keys, entries, strings, collection roots, collection internal objects, TTL index entries, and streamed reply slices. Public `storage.api.KeyHandle` remains an opaque bytes view, but `yierdis-db-memory` provides only native-handle-backed key handles. `BytesSlice` remains the streaming API; legacy `OffHeapAllocator`, `OffHeapBuf`, `OffHeapSlice`, blob refs, and heap storage helpers are removed.

**Tech Stack:** Java 25, Maven, JUnit 4, Yierdis FFM memory runtime, stable native allocator, RESP reply streaming via `BytesSlice`.

---

## Confirmed Decisions

- Public `yier.bubu.redis.storage.api.KeyHandle` remains an abstract bytes-view SPI.
- Internal `yierdis-db-memory` `KeyHandle` only supports stable native handles.
- Delete heap key handles, legacy FFM key handles, heap keyspace, legacy FFM keyspace, and heap expire index.
- Delete old `YierdisFfmBytesRef` / `YierdisFfmBlobStore` / blob-ref collection internals after native replacements exist.
- Delete `OffHeapAllocator`, `OffHeapBuf`, `OffHeapSlice`, `YierdisForeignOffHeapAllocator`, and `YierdisFfmSlabAllocator`.
- Keep `BytesSlice` and `bulkString(BytesSlice)` for streaming output.
- Implement native-backed `BytesSlice` using `NativeAllocator.pin(handle)` / `unpin(handle)` during synchronous `writeTo`.
- Keep current list/hash/set/zset behavior and encoding strategy; migrate memory representation, not Redis algorithms.
- Native handle graph and defrag validation must cover collection internal native handles.
- Public deprecated old offheap constructors/config bridges may be removed without compatibility shims.

## File Structure

### Memory API

- Modify `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
  - Add explicit object kinds for collection internals.
- Delete:
  - `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapAllocator.java`
  - `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapBuf.java`
  - `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapSlice.java`
- Delete or rewrite tests:
  - Delete `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/OffHeapContractsSmokeTest.java`

### Memory FFM

- Keep:
  - `YierdisFfmMemoryRuntime`
  - `YierdisStableNativeAllocator`
  - `YierdisNativePageAllocator`
  - `YierdisNativeObjectTable`
  - native defrag/epoch/page support
- Delete:
  - `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java`
  - `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java`
- Delete or replace tests:
  - `YierdisForeignOffHeapAllocatorTest`
  - `YierdisFfmSlabAllocatorTest`

### DB Key Identity And Keyspace

- Modify:
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/AllocatorKeyHandle.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandleAccess.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- Delete:
  - `HeapKeyHandle.java`
  - `FfmKeyHandle.java`
  - `ByteArrayKeyspace.java`
  - `YierdisFfmKeyspace.java`
- Replace tests:
  - `KeyHandleContractTest`
  - `NativeKeyDirectoryTest`
  - delete `ByteArrayKeyspaceTest`
  - delete `YierdisFfmKeyspaceTest`
  - delete `YierdisFfmRehashConsistencyTest` or move behavior coverage to `NativeKeyDirectoryTest`

### Expire Index

- Modify:
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisExpireIndex.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java`
- Delete:
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisHeapExpireIndex.java`
- Replace tests:
  - `ExpireIndexContractTest`
  - `ExpireKeySharingTest`

### DB Construction And Accounting

- Modify:
  - `YierdisDb.java`
  - `YierdisDbComponentFactory.java`
  - `YierdisDbStorageComponents.java`
  - `YierdisDbOwnedResources.java`
  - `YierdisDbKeyLifecycle.java`
  - `YierdisDbMemoryEstimator.java`
  - `YierdisDbMemoryReporter.java`
  - `YierdisDbRuntimeState.java`
  - `YierdisInstanceConfig.java`
- Remove `OffHeapAllocator`, `ownsOffHeapAllocator`, and `offHeapKeysEnabled` API/config paths.

### Native Bytes Streaming

- Create:
  - `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeBytesSlice.java`
- Modify:
  - list/hash/set/zset/string output paths that currently emit `YierdisFfmBytesRefSlice` or `OffHeapSlice`.
- Keep:
  - `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesSlice.java`
  - `bulkString(BytesSlice)` APIs in `ReplySink` and `BulkStringSink`.

### Collection Values

- Modify:
  - `ListRoot.java`, `ListValue.java`
  - `HashRoot.java`, `HashValue.java`
  - `SetRoot.java`, `SetValue.java`
  - `ZSetRoot.java`, `ZSetValue.java`
  - `YierdisFfmListpack.java`
  - `YierdisFfmByteMap.java`
  - `YierdisFfmZSet.java`
- Preferred outcome:
  - Rename legacy `YierdisFfm*` classes after conversion, or replace with new `NativeListpack`, `NativeByteMap`, `NativeZSet` classes.
- Delete after replacement:
  - `YierdisFfmBytesRef.java`
  - `YierdisFfmBytesRefSlice.java`
  - `YierdisFfmBlobStore.java`

### Native Graph And Defrag

- Modify:
  - `YierdisDbNativeHandleGraph.java`
  - collection root/value classes to expose internal handles for graph traversal.
- Tests:
  - Extend `YierdisDbNativeHandleGraphTest`
  - Extend `NativeStorageRegressionTest`
  - Extend `OffHeapLeakRegressionTest` or rename it to native leak regression.

### Architecture And Docs

- Modify:
  - `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java`
  - `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  - `docs/project-docs/native-memory-runtime.md`
  - `docs/project-docs/native-allocator-and-handles.md`
  - `docs/project-docs/bytes-and-fast-paths.md`
  - `docs/project-docs/ttl-and-expiration-lifecycle.md`
  - `docs/project-docs/maxmemory-and-eviction.md`
  - `docs/project-docs/code-logic-coverage.md`
  - `docs/project-docs/development-navigation.md`

---

## Task 1: Add Native Object Kinds For Collection Internals

**Files:**
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
- Test: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java`

- [ ] **Step 1: Add a failing test for distinct native object kinds**

Add this test method to `NativeHandleTest`:

```java
@Test
public void collectionNativeObjectKindsHaveDistinctCodesInsideTheirDomains() {
    java.util.EnumMap<NativeHandleDomain, java.util.HashSet<Integer>> seen = new java.util.EnumMap<>(NativeHandleDomain.class);
    for (NativeObjectKind kind : NativeObjectKind.values()) {
        java.util.HashSet<Integer> codes = seen.computeIfAbsent(kind.domain(), ignored -> new java.util.HashSet<>());
        Assert.assertTrue("duplicate kind code " + kind.code() + " in domain " + kind.domain(), codes.add(kind.code()));
    }

    Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.LIST_ROOT.domain());
    Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.HASH_ROOT.domain());
    Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.SET_ROOT.domain());
    Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.ZSET_ROOT.domain());
    Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.LISTPACK_BYTES.domain());
    Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.HASH_FIELD_BYTES.domain());
    Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.HASH_VALUE_BYTES.domain());
    Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.SET_MEMBER_BYTES.domain());
    Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.ZSET_MEMBER_BYTES.domain());
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeHandleTest#collectionNativeObjectKindsHaveDistinctCodesInsideTheirDomains test
```

Expected: FAIL because the new enum constants do not exist.

- [ ] **Step 3: Replace `NativeObjectKind` with explicit kinds**

Update `NativeObjectKind` to keep existing kinds and add collection internals with unique codes per domain:

```java
public enum NativeObjectKind {
    GENERIC(0, NativeHandleDomain.STORAGE_OBJECT),
    STRING_BYTES(1, NativeHandleDomain.STORAGE_OBJECT),
    LISTPACK_BYTES(2, NativeHandleDomain.STORAGE_OBJECT),
    HASH_FIELD_BYTES(3, NativeHandleDomain.STORAGE_OBJECT),
    HASH_VALUE_BYTES(4, NativeHandleDomain.STORAGE_OBJECT),
    SET_MEMBER_BYTES(5, NativeHandleDomain.STORAGE_OBJECT),
    ZSET_MEMBER_BYTES(6, NativeHandleDomain.STORAGE_OBJECT),
    SCORE_BYTES(7, NativeHandleDomain.STORAGE_OBJECT),

    ENTRY_RECORD(1, NativeHandleDomain.ENTRY_OBJECT),
    KEY_BYTES(1, NativeHandleDomain.KEY_BYTES),

    LIST_ROOT(1, NativeHandleDomain.TYPE_ROOT),
    HASH_ROOT(2, NativeHandleDomain.TYPE_ROOT),
    SET_ROOT(3, NativeHandleDomain.TYPE_ROOT),
    ZSET_ROOT(4, NativeHandleDomain.TYPE_ROOT),
    LIST_NODE(5, NativeHandleDomain.TYPE_ROOT),
    HASH_TABLE(6, NativeHandleDomain.TYPE_ROOT),
    SET_TABLE(7, NativeHandleDomain.TYPE_ROOT),
    ZSET_TABLE(8, NativeHandleDomain.TYPE_ROOT),
    ZSET_NODE(9, NativeHandleDomain.TYPE_ROOT),

    INDEX_NODE(1, NativeHandleDomain.INDEX_NODE),
    METADATA_RECORD(1, NativeHandleDomain.ALLOCATOR_METADATA);
```

Keep the existing constructor and accessors.

- [ ] **Step 4: Update roots to use root kinds**

Replace:

```java
NativeObjectKind.LIST_NODE
NativeObjectKind.HASH_NODE
NativeObjectKind.SET_NODE
NativeObjectKind.ZSET_NODE
```

in root table construction with:

```java
NativeObjectKind.LIST_ROOT
NativeObjectKind.HASH_ROOT
NativeObjectKind.SET_ROOT
NativeObjectKind.ZSET_ROOT
```

- [ ] **Step 5: Run memory API and db-memory compile**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -DskipTests compile
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry
git commit -m "refactor: add native object kinds for collection storage"
```

---

## Task 2: Remove Legacy OffHeap Memory API

**Files:**
- Delete:
  - `OffHeapAllocator.java`
  - `OffHeapBuf.java`
  - `OffHeapSlice.java`
  - `OffHeapContractsSmokeTest.java`
- Modify:
  - `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/package-info.java` if it mentions old offheap APIs.

- [ ] **Step 1: Delete legacy API files**

Delete:

```text
yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapAllocator.java
yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapBuf.java
yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/OffHeapSlice.java
yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/OffHeapContractsSmokeTest.java
```

- [ ] **Step 2: Remove package documentation references**

If `package-info.java` lists old offheap APIs, remove those entries and describe `NativeAllocator`, `NativeHandle`, and `NativeObjectView` as the supported memory abstraction.

- [ ] **Step 3: Run compile and verify expected downstream failures**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -am -DskipTests compile
```

Expected: PASS for memory-api.

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -DskipTests compile
```

Expected: FAIL with references to `OffHeapAllocator`, `OffHeapBuf`, or `OffHeapSlice`; these are fixed in later tasks.

- [ ] **Step 4: Commit**

```bash
git add yierdis-memory/yierdis-memory-api
git commit -m "refactor: remove legacy offheap memory api"
```

---

## Task 3: Remove Legacy FFM OffHeap Allocators

**Files:**
- Delete:
  - `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java`
  - `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java`
  - their tests.
- Modify:
  - any module docs that mention old slab/offheap allocator as supported.

- [ ] **Step 1: Delete old allocator implementations and tests**

Delete:

```text
yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocator.java
yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocator.java
yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisForeignOffHeapAllocatorTest.java
yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisFfmSlabAllocatorTest.java
```

- [ ] **Step 2: Run memory-ffm compile**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -DskipTests compile
```

Expected: PASS after DB references are not included; if memory-ffm compile still sees old class references, remove those references.

- [ ] **Step 3: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm
git commit -m "refactor: remove legacy ffm offheap allocators"
```

---

## Task 4: Collapse DB Construction To Native Runtime Only

**Files:**
- Modify:
  - `YierdisDb.java`
  - `YierdisDbComponentFactory.java`
  - `YierdisDbStorageComponents.java`
  - `YierdisDbOwnedResources.java`
  - `YierdisDbKeyLifecycle.java`
  - `YierdisDbMemoryEstimator.java`
  - `YierdisDbMemoryReporter.java`
  - `DbMemoryAccounting.java`
  - `YierdisInstanceConfig.java`
- Tests:
  - `YierdisDbConstructionTest`
  - runtime config tests.

- [ ] **Step 1: Write failing construction tests**

In `YierdisDbConstructionTest`, replace old offheap-constructor tests with:

```java
@Test
public void defaultDbCreatesNativeRuntimeAndStableNativeStorage() {
    try (YierdisDb db = new YierdisDb()) {
        Assert.assertNotNull(db.runtimeInternalsForTests().keyLifecycle().nativeAllocator());
        Assert.assertNotNull(db.runtimeInternalsForTests().keyLifecycle().memoryRuntime());
    }
}

@Test
public void sharedRuntimeDbUsesProvidedRuntime() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("shared-runtime-construction");
         YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5)) {
        Assert.assertSame(runtime, db.runtimeInternalsForTests().keyLifecycle().memoryRuntime());
    }
}
```

Remove tests that construct `YierdisDb` with `OffHeapAllocator`.

- [ ] **Step 2: Remove old constructor and field types**

In `YierdisDb`, remove:

```java
final OffHeapAllocator offHeapAllocator;
public YierdisDb(OffHeapAllocator offHeapAllocator)
public YierdisDb(OffHeapAllocator offHeapAllocator, ...)
```

Keep:

```java
public YierdisDb()
public static YierdisDb createWithSharedFfmRuntime(...)
public static YierdisDb createWithOwnedFfmRuntime(...)
```

The private constructor should accept only:

```java
YierdisFfmMemoryRuntime memoryRuntime,
boolean ownsMemoryRuntime,
long maxmemoryBytes,
MaxmemoryPolicy maxmemoryPolicy,
int maxmemorySamples,
long evictionTimeLimitMillis,
long expireCleanupTimeLimitMillis,
NativeDefragOptions nativeDefragOptions,
int dbIndex
```

- [ ] **Step 3: Simplify storage components**

In `YierdisDbStorageComponents`, remove all `OffHeapAllocator` fields and parameters. Creation should:

```java
YierdisFfmMemoryRuntime resolvedRuntime = memoryRuntime == null ? new YierdisFfmMemoryRuntime("db") : memoryRuntime;
boolean resolvedOwnsRuntime = memoryRuntime == null || ownsMemoryRuntime;
NativeAllocator nativeAllocator = new YierdisStableNativeAllocator(resolvedRuntime, sharedNativeSlotCapacity());
```

Then create:

```java
EntryTable entries = new EntryTable(resolvedRuntime, nativeAllocator);
NativeKeyDirectory keyDirectory = new NativeKeyDirectory(nativeAllocator);
StringRoot stringRoot = new StringRoot(nativeAllocator);
ListRoot listRoot = new ListRoot(nativeAllocator);
HashRoot hashRoot = new HashRoot(nativeAllocator);
SetRoot setRoot = new SetRoot(nativeAllocator);
ZSetRoot zSetRoot = new ZSetRoot(nativeAllocator);
YierdisExpireIndex expires = new YierdisFfmExpireIndex(resolvedRuntime, nativeAllocator);
```

- [ ] **Step 4: Simplify owned resources**

`YierdisDbOwnedResources` should close:

1. storage roots/tables
2. `NativeAllocator`
3. `YierdisFfmMemoryRuntime` only when owned

Remove old `OffHeapAllocator` ownership fields.

- [ ] **Step 5: Remove old config bridge methods**

In `YierdisInstanceConfig.Builder`, delete:

```java
offHeapAllocator(Object ignored)
ownsOffHeapAllocator(boolean ignored)
offHeapKeysEnabled(boolean ignored)
```

Update tests that called them to use native defaults.

- [ ] **Step 6: Simplify accounting**

In `DbMemoryAccounting.snapshot`, remove `OffHeapAllocator offHeapAllocator` and `safeOffHeapUsedBytes`. `offHeapUsedBytes` becomes `directNativeUsedBytes`.

In `YierdisDbMemoryEstimator`, remove the no-op `OffHeapAllocator` constructor argument.

- [ ] **Step 7: Run construction/accounting tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-server/yierdis-server-runtime-api -am -Dtest=YierdisDbConstructionTest,YierdisInstanceConfigTest test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-db/yierdis-db-memory yierdis-server/yierdis-server-runtime-api
git commit -m "refactor: collapse db construction to native runtime"
```

---

## Task 5: Make Internal KeyHandle Native-Only

**Files:**
- Modify:
  - `KeyHandle.java`
  - `AllocatorKeyHandle.java`
  - `KeyHandleAccess.java`
  - `NativeKeyDirectory.java`
  - `YierdisDbKeyLifecycle.java`
- Delete:
  - `HeapKeyHandle.java`
  - `FfmKeyHandle.java`
- Tests:
  - `KeyHandleContractTest`
  - `NativeKeyDirectoryTest`
  - `YierdisDbMemoryEstimatorTest`

- [ ] **Step 1: Replace key handle tests**

Rewrite `KeyHandleContractTest` to create a native key handle through `NativeKeyDirectory`:

```java
@Test
public void nativeKeyHandleIsReadOnlyBytesViewWithStableDictHash() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-key-handle-contract");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
         NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
        byte[] key = "hello".getBytes(StandardCharsets.US_ASCII);
        EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
        directory.compute(key, (ignored, old) -> entry);

        KeyHandle handle = directory.getKeyHandle(key);
        Assert.assertEquals(key.length, handle.len());
        for (int i = 0; i < key.length; i++) {
            Assert.assertEquals(key[i], handle.byteAt(i));
        }
        Assert.assertEquals(handle.dictHash(), directory.getKeyHandle(key).dictHash());
        allocator.free(entry.nativeHandle());
    }
}
```

- [ ] **Step 2: Remove factory methods for heap and legacy FFM**

In internal `KeyHandle`, keep:

```java
public static KeyHandle forNative(NativeAllocator allocator, NativeHandle handle, int dictHash)
```

Delete:

```java
forHeap(...)
forFfm(...)
hashBytesView(...)
```

Keep `len()`, `byteAt()`, `length()`, `getByte()`, and `dictHash()`.

- [ ] **Step 3: Narrow `KeyHandleAccess`**

Replace it with:

```java
public final class KeyHandleAccess {
    private KeyHandleAccess() {
    }

    public static boolean isAllocator(KeyHandle handle) {
        return handle instanceof AllocatorKeyHandle;
    }

    public static NativeHandle allocatorNativeHandle(KeyHandle handle) {
        NativeHandle nativeHandle = allocatorNativeHandleOrNull(handle);
        if (nativeHandle == null) {
            throw new IllegalArgumentException("expected allocator-backed KeyHandle: "
                    + (handle == null ? "null" : handle.getClass().getName()));
        }
        return nativeHandle;
    }

    public static NativeHandle allocatorNativeHandleOrNull(KeyHandle handle) {
        if (handle instanceof AllocatorKeyHandle h) {
            return h.nativeHandle();
        }
        return null;
    }
}
```

- [ ] **Step 4: Remove temporary heap handle from new-key remapping**

In `YierdisDbKeyLifecycle.computeWithHandle`, do not call remapping before the key has a native handle. Allocate and publish the entry/key handle first, then call the remapping function with `keyDirectory.getKeyHandle(keyBytes)`. If remapping returns null or fails, remove the newly inserted mapping and release the allocated entry.

The new-key flow should be:

```java
EntryHandle createdEntry = entryTable.allocate(EntryRecord.placeholder());
keyDirectory.compute(keyBytes, (key, oldHandle) -> {
    if (oldHandle != null) {
        throw new IllegalStateException("native entry appeared during remapping");
    }
    return createdEntry;
});
KeyHandle keyHandle = keyDirectory.getKeyHandle(keyBytes);
EntryRecord newRecord = remappingFunction.apply(keyHandle, null).record();
```

If `EntryRecord.placeholder()` does not exist, create a package-private placeholder factory in `EntryRecord` with zero raw handles and `ValueType.STRING` only for temporary unpublished entry table slots, or introduce an `EntryTable.reserve()` method that allocates an entry handle without writing a record. Prefer `EntryTable.reserve()` because it avoids fake record state.

- [ ] **Step 5: Delete old classes**

Delete:

```text
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/HeapKeyHandle.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/FfmKeyHandle.java
```

- [ ] **Step 6: Run key tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -Dtest=KeyHandleContractTest,NativeKeyDirectoryTest,YierdisDbMemoryEstimatorTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/key yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace
git commit -m "refactor: make key handles native only"
```

---

## Task 6: Delete Heap And Legacy FFM Keyspaces

**Files:**
- Delete:
  - `ByteArrayKeyspace.java`
  - `YierdisFfmKeyspace.java`
  - related tests.
- Modify:
  - architecture guards.

- [ ] **Step 1: Delete old keyspace classes and tests**

Delete:

```text
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/ByteArrayKeyspace.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmKeyspace.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/ByteArrayKeyspaceTest.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmKeyspaceTest.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmRehashConsistencyTest.java
```

- [ ] **Step 2: Move useful behavior coverage to `NativeKeyDirectoryTest`**

Add tests for:

```java
@Test public void nativeDirectorySurvivesRehashAndFindsAllKeys()
@Test public void nativeDirectoryScanResumesAfterCursor()
@Test public void nativeDirectoryRandomKeyHandleReturnsLiveNativeHandle()
@Test public void nativeDirectoryRemoveByEntryHandleReleasesKeyHandle()
```

Each test should use `YierdisStableNativeAllocator`, insert enough keys to trigger rehash, and verify `copy(KeyHandle)` returns expected bytes.

- [ ] **Step 3: Update architecture guard**

In `YierdisDbArchitectureGuardTest`, add forbidden source strings:

```java
"new ByteArrayKeyspace",
"new YierdisFfmKeyspace",
"KeyHandle.forHeap",
"KeyHandle.forFfm",
"HeapKeyHandle",
"FfmKeyHandle"
```

- [ ] **Step 4: Run keyspace tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-architecture-tests -am -Dtest=NativeKeyDirectoryTest,YierdisDbArchitectureGuardTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory yierdis-tests/yierdis-architecture-tests
git commit -m "refactor: remove legacy keyspace implementations"
```

---

## Task 7: Make Expire Index Native-Only

**Files:**
- Modify:
  - `YierdisExpireIndex.java`
  - `YierdisFfmExpireIndex.java`
  - `DbMemoryAccounting.java`
- Delete:
  - `YierdisHeapExpireIndex.java`
- Tests:
  - `ExpireIndexContractTest`
  - `ExpireKeySharingTest`

- [ ] **Step 1: Rewrite expire tests around native directory**

`ExpireIndexContractTest` should create:

```java
try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-expire-contract");
     YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
     NativeKeyDirectory keyspace = new NativeKeyDirectory(allocator)) {
    YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
    ...
}
```

Use `keyspace.getKeyHandle(key)` as the only input to `setExpireAtMillis`, `get`, and `removeExpire`.

- [ ] **Step 2: Delete heap expire index**

Delete:

```text
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisHeapExpireIndex.java
```

- [ ] **Step 3: Remove blob-ref mode from `YierdisFfmExpireIndex`**

Delete:

```java
private final YierdisFfmBlobStore blobStore;
public YierdisFfmExpireIndex(YierdisFfmBlobStore blobStore)
FfmKeyRef
KeyHandleAccess.ffmBytesRefOrNull(...)
KeyHandle.forFfm(...)
```

Keep only:

```java
private final YierdisFfmMemoryRuntime memoryRuntime;
private final NativeAllocator nativeAllocator;
private static final class AllocatorKeyRef
```

All stored expire keys should point to `NativeHandle` from the key directory. The expire index must not allocate its own key bytes.

- [ ] **Step 4: Simplify expire interface**

Remove the store-dependent method from `YierdisExpireIndex`:

```java
void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store);
```

Keep byte-array lookup/removal only when command paths still start from bytes and resolve to a key handle first in lifecycle. Prefer lifecycle methods to resolve `KeyHandle` before touching expire index.

- [ ] **Step 5: Simplify accounting**

Remove heap-expire-specific accounting branches:

```java
if (expires instanceof YierdisHeapExpireIndex ...)
estimateLongObjectBytes(...)
```

Only inspect `YierdisFfmExpireIndex` capacities/overhead.

- [ ] **Step 6: Run expire tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -Dtest=ExpireIndexContractTest,ExpireKeySharingTest,NativeStorageRegressionTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java yierdis-db/yierdis-db-memory/src/test/java
git commit -m "refactor: make expire index native only"
```

---

## Task 8: Add NativeBytesSlice With Pin/Unpin Streaming

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeBytesSlice.java`
- Tests:
  - Create `NativeBytesSliceTest.java`
  - Update `OffHeapCollectionReadStreamingTest` or rename it to `NativeCollectionReadStreamingTest`

- [ ] **Step 1: Write `NativeBytesSliceTest`**

Create:

```java
package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class NativeBytesSliceTest {
    @Test
    public void writesNativeBytesAndAllowsDefragAfterWriteCompletes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-slice-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 5);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < bytes.length; i++) {
                    view.setByte(i, bytes[i]);
                }
            }

            NativeBytesSlice slice = new NativeBytesSlice(allocator, handle, 1, 3);
            CollectingSink sink = new CollectingSink();
            slice.writeTo(sink);

            Assert.assertEquals("ell", sink.asString());
            allocator.defragOne(handle, 1024);
            allocator.free(handle);
        }
    }

    private static final class CollectingSink implements BytesSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void writeByte(byte b) {
            out.write(b);
        }

        @Override
        public void writeBytes(byte[] bytes, int off, int len) {
            out.write(bytes, off, len);
        }

        String asString() {
            return out.toString(StandardCharsets.US_ASCII);
        }
    }
}
```

- [ ] **Step 2: Implement `NativeBytesSlice`**

```java
package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;

import java.util.Objects;

public final class NativeBytesSlice implements BytesSlice {
    private final NativeAllocator allocator;
    private final NativeHandle handle;
    private final int offset;
    private final int length;

    public NativeBytesSlice(NativeAllocator allocator, NativeHandle handle, int offset, int length) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.handle = Objects.requireNonNull(handle, "handle");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte getByte(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + length);
        }
        allocator.pin(handle);
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return view.getByte(offset + index);
        } finally {
            allocator.unpin(handle);
        }
    }

    @Override
    public void writeTo(BytesSink out) {
        Objects.requireNonNull(out, "out");
        allocator.pin(handle);
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            if (offset > view.size() || length > view.size() - offset) {
                throw new IndexOutOfBoundsException("offset=" + offset + ", len=" + length + ", size=" + view.size());
            }
            for (int i = 0; i < length; i++) {
                out.writeByte(view.getByte(offset + i));
            }
        } finally {
            allocator.unpin(handle);
        }
    }
}
```

- [ ] **Step 3: Run slice tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -Dtest=NativeBytesSliceTest test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeBytesSlice.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/NativeBytesSliceTest.java
git commit -m "feat: add native bytes slice streaming"
```

---

## Task 9: Replace Legacy Blob Store With Native Byte Storage Helpers

**Files:**
- Create:
  - `NativeByteStore.java`
  - `NativeByteMap.java`
  - `NativeListpack.java`
- Delete after replacement:
  - `YierdisFfmBlobStore.java`
  - `YierdisFfmBytesRef.java`
  - `YierdisFfmBytesRefSlice.java`
  - `YierdisFfmByteMap.java`
  - `YierdisFfmListpack.java`

- [ ] **Step 1: Write native byte store tests**

Create `NativeByteStoreTest` with:

```java
@Test
public void storesComparesStreamsAndReleasesNativeBytes() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-store");
         NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
        NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_VALUE_BYTES);
        NativeHandle handle = store.store(bytes("abc"));
        Assert.assertTrue(store.equalsBytes(handle, bytes("abc")));
        Assert.assertFalse(store.equalsBytes(handle, bytes("abd")));
        Assert.assertArrayEquals(bytes("abc"), store.toByteArray(handle));
        Assert.assertEquals(3, store.length(handle));
        store.release(handle);
    }
}
```

- [ ] **Step 2: Implement `NativeByteStore`**

Required methods:

```java
NativeHandle store(byte[] bytes)
NativeHandle store(byte[] bytes, NativeObjectKind kind)
void release(NativeHandle handle)
byte[] toByteArray(NativeHandle handle)
boolean equalsBytes(NativeHandle handle, byte[] bytes)
boolean equalsBytes(NativeHandle handle, BytesView view)
int length(NativeHandle handle)
BytesSlice slice(NativeHandle handle)
```

Implementation must allocate with a specific `NativeObjectKind`, write bytes through `NativeObjectView`, compare through `READ_ONLY` resolve, and return `NativeBytesSlice` for streaming.

- [ ] **Step 3: Implement `NativeByteMap<V>`**

Use `NativeHandle[] keys`, `int[] hashes`, `byte[] states`, and `Object[] values`, mirroring old `YierdisFfmByteMap` behavior but storing `NativeHandle` instead of `YierdisFfmBytesRef`. It must:

```java
V put(byte[] keyBytes, V value)
V get(byte[] keyBytes)
V remove(byte[] keyBytes)
void forEach(EntryConsumer<V> consumer)
void clear()
long nativeBytes()
```

On key replacement/removal, release key handles using `NativeByteStore`.

- [ ] **Step 4: Implement `NativeListpack`**

Use `ArrayList<NativeHandle>` initially to preserve existing behavior. It must support old listpack operations:

```java
void addFirst(byte[] value)
void addLast(byte[] value)
byte[] removeFirst()
byte[] removeLast()
int size()
Cursor cursor()
void clear()
long estimatedBytes()
```

`Cursor` must stream entries via `NativeBytesSlice`, not heap copy, for reply paths.

- [ ] **Step 5: Run helper tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -Dtest=NativeByteStoreTest,NativeByteMapTest,NativeListpackTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value
git commit -m "feat: add native byte storage helpers"
```

---

## Task 10: Migrate ListValue To Native Handles

**Files:**
- Modify:
  - `ListRoot.java`
  - `ListValue.java`
- Tests:
  - `ListValueTest`
  - `CollectionDirectOpsTest`
  - `OffHeapCollectionReadStreamingTest` renamed to `NativeCollectionReadStreamingTest`

- [ ] **Step 1: Update `ListRoot` construction**

Remove `YierdisFfmMemoryRuntime runtime` field and runtime constructors. Keep only:

```java
public ListRoot(NativeAllocator allocator)
```

`newListValue` should call:

```java
return new ListValue(lists.allocator(), rootHandle);
```

- [ ] **Step 2: Update `ListValue` constructor**

Replace:

```java
YierdisFfmBlobStore ffmBlobStore
YierdisFfmListpack listpackFfm
```

with:

```java
NativeAllocator allocator
NativeByteStore byteStore
NativeListpack listpack
```

Use `NativeObjectKind.LISTPACK_BYTES` for packed entries and `NativeObjectKind.LIST_NODE` for nodes.

- [ ] **Step 3: Preserve streaming output**

Every `rangeInto` branch that previously called:

```java
out.bulkString(new YierdisFfmBytesRefSlice(ref));
```

must call:

```java
out.bulkString(new NativeBytesSlice(allocator, handle, 0, length));
```

or `NativeByteStore.slice(handle)`.

- [ ] **Step 4: Preserve pop return semantics**

`lpop` and `rpop` still return `List<byte[]>`; these paths may copy because Redis command return ownership leaves storage. Streaming range paths must not copy.

- [ ] **Step 5: Run list tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=ListValueTest,CollectionDirectOpsTest,ListCommandTest,NativeCollectionReadStreamingTest test
```

Expected: PASS; streaming test must assert `BytesSlice` path is used.

- [ ] **Step 6: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java yierdis-db/yierdis-db-memory/src/test/java yierdis-tests/yierdis-integration-tests/src/test/java
git commit -m "refactor: migrate list values to native handles"
```

---

## Task 11: Migrate HashValue To Native Handles

**Files:**
- Modify:
  - `HashRoot.java`
  - `HashValue.java`
- Tests:
  - `HashValueTest`
  - hash command integration tests
  - native streaming tests

- [ ] **Step 1: Update `HashRoot` construction**

Remove runtime constructors and `runtime` field. Keep only:

```java
public HashRoot(NativeAllocator allocator)
```

`newHashValue` should call:

```java
return new HashValue(hashes.allocator());
```

- [ ] **Step 2: Replace hash internals**

In `HashValue`, replace:

```java
YierdisFfmBlobStore ffmBlobStore
YierdisFfmListpack packedFfm
YierdisFfmByteMap<YierdisFfmBytesRef> mapFfm
ByteArrayHashMap<byte[]> map
```

with:

```java
NativeAllocator allocator
NativeByteStore fieldStore
NativeByteStore valueStore
NativeListpack packed
NativeByteMap<NativeHandle> map
```

Use:

```java
NativeObjectKind.HASH_FIELD_BYTES
NativeObjectKind.HASH_VALUE_BYTES
NativeObjectKind.HASH_TABLE
```

- [ ] **Step 3: Preserve packed-to-map conversion**

Keep current threshold behavior. Conversion must move all packed pairs into `NativeByteMap<NativeHandle>`, release old packed handles, and retain only map-owned field/value handles.

- [ ] **Step 4: Preserve streaming output**

`hgetallPairsInto` must stream fields and values through `NativeBytesSlice`; it must not call `toByteArray` except for APIs that explicitly return `byte[]`.

- [ ] **Step 5: Run hash tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=HashValueTest,HashCommandTest,NativeCollectionReadStreamingTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java yierdis-db/yierdis-db-memory/src/test/java yierdis-tests/yierdis-integration-tests/src/test/java
git commit -m "refactor: migrate hash values to native handles"
```

---

## Task 12: Migrate SetValue To Native Handles

**Files:**
- Modify:
  - `SetRoot.java`
  - `SetValue.java`
- Tests:
  - `SetValueTest`
  - set command integration tests

- [ ] **Step 1: Update `SetRoot` construction**

Remove runtime constructors and `runtime` field. Keep only:

```java
public SetRoot(NativeAllocator allocator)
```

`newSetValue` should call:

```java
return new SetValue(sets.allocator());
```

- [ ] **Step 2: Replace set internals**

In `SetValue`, replace:

```java
ByteArrayHashSet hashset
YierdisFfmByteMap<Object> hashsetFfm
YierdisFfmBlobStore ffmBlobStore
```

with:

```java
NativeAllocator allocator
NativeByteStore memberStore
NativeByteMap<Object> members
```

Use `NativeObjectKind.SET_MEMBER_BYTES` and `NativeObjectKind.SET_TABLE`.

- [ ] **Step 3: Preserve intset behavior if present**

If `SetValue` has integer compact encoding, keep it. Only replace byte-member map storage. Conversion from intset to native member map must allocate native member bytes.

- [ ] **Step 4: Preserve streaming output**

`membersInto` must call `out.bulkString(NativeBytesSlice)` for native member bytes.

- [ ] **Step 5: Run set tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=SetValueTest,SetCommandTest,NativeCollectionReadStreamingTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java yierdis-db/yierdis-db-memory/src/test/java yierdis-tests/yierdis-integration-tests/src/test/java
git commit -m "refactor: migrate set values to native handles"
```

---

## Task 13: Migrate ZSetValue To Native Handles

**Files:**
- Modify:
  - `ZSetRoot.java`
  - `ZSetValue.java`
  - replace or delete `YierdisFfmZSet.java`
- Tests:
  - `ZSetValueTest`
  - zset command integration tests

- [ ] **Step 1: Update `ZSetRoot` construction**

Remove runtime constructors and `runtime` field. Keep only:

```java
public ZSetRoot(NativeAllocator allocator)
```

`newZSetValue` should call:

```java
return new ZSetValue(zsets.allocator());
```

- [ ] **Step 2: Replace zset internals**

In `ZSetValue`, replace:

```java
YierdisFfmZSet ffm
ByteArrayHashMap<ZSkipList.Node> byMember
```

with native member storage:

```java
NativeAllocator allocator
NativeByteStore memberStore
NativeByteMap<ZSkipList.Node> byMember
```

Use:

```java
NativeObjectKind.ZSET_MEMBER_BYTES
NativeObjectKind.ZSET_TABLE
NativeObjectKind.ZSET_NODE
```

- [ ] **Step 3: Preserve skiplist semantics**

Keep score ordering, lex compare, rank ranges, score ranges, reverse ranges, and `WITHSCORES` output behavior. Only member byte storage changes.

- [ ] **Step 4: Preserve streaming output**

Range outputs must stream member bytes via `NativeBytesSlice`. Score strings can remain heap encoded ASCII bytes because they are computed values, not stored value bytes.

- [ ] **Step 5: Delete or replace `YierdisFfmZSet`**

After `ZSetValue` no longer references it, delete:

```text
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmZSet.java
```

If splitting is cleaner, create `NativeZSet.java` in `internal/value` and move the native implementation there.

- [ ] **Step 6: Run zset tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=ZSetValueTest,ZSetCommandTest,NativeCollectionReadStreamingTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value yierdis-db/yierdis-db-memory/src/test/java yierdis-tests/yierdis-integration-tests/src/test/java
git commit -m "refactor: migrate zset values to native handles"
```

---

## Task 14: Delete Legacy Blob-Ref Classes And Heap Collection Helpers

**Files:**
- Delete:
  - `YierdisFfmBytesRef.java`
  - `YierdisFfmBytesRefSlice.java`
  - `YierdisFfmBlobStore.java`
  - `YierdisFfmByteMap.java`
  - `YierdisFfmListpack.java`
  - `ByteArrayHashMap.java`
  - `ByteArrayHashSet.java`
- Delete or rewrite tests:
  - `YierdisFfmBlobStoreTest`
  - `YierdisFfmBlobStore` related tests
  - `ByteArrayHashMapTest`
  - `ByteArrayHashSetTest`

- [ ] **Step 1: Verify no production references remain**

Run:

```bash
rg -n "YierdisFfmBytesRef|YierdisFfmBytesRefSlice|YierdisFfmBlobStore|YierdisFfmByteMap|YierdisFfmListpack|ByteArrayHashMap|ByteArrayHashSet" yierdis-db/yierdis-db-memory/src/main/java
```

Expected: no output.

- [ ] **Step 2: Delete old classes and tests**

Delete the files listed above.

- [ ] **Step 3: Run full db-memory tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "refactor: remove legacy blob and heap collection helpers"
```

---

## Task 15: Extend Native Handle Graph And Defrag Coverage

**Files:**
- Modify:
  - `YierdisDbNativeHandleGraph.java`
  - `ListValue.java`
  - `HashValue.java`
  - `SetValue.java`
  - `ZSetValue.java`
  - root classes if traversal is rooted there.
- Tests:
  - `YierdisDbNativeHandleGraphTest`
  - `NativeStorageRegressionTest`
  - `OffHeapLeakRegressionTest` renamed or rewritten.

- [ ] **Step 1: Add handle enumeration interface**

Create package-private interface:

```java
package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeHandle;
import java.util.function.Consumer;

interface NativeHandleOwner {
    void forEachNativeHandle(Consumer<NativeHandle> consumer);
}
```

Make native collection values implement it.

- [ ] **Step 2: Implement traversal in values**

For each collection value:

- list: emit root, node, and entry byte handles.
- hash: emit root, table, field byte, value byte handles.
- set: emit root, table, member byte handles.
- zset: emit root, table, skiplist node, member byte handles.

- [ ] **Step 3: Extend `YierdisDbNativeHandleGraph`**

When visiting an `EntryRecord` with collection `ValueHandle`, resolve the root adapter through the relevant root and call `forEachNativeHandle`.

- [ ] **Step 4: Add graph tests**

Add tests that create one key of every type, call graph traversal, and assert it includes:

```java
NativeObjectKind.KEY_BYTES
NativeObjectKind.ENTRY_RECORD
NativeObjectKind.STRING_BYTES
NativeObjectKind.LIST_ROOT
NativeObjectKind.HASH_ROOT
NativeObjectKind.SET_ROOT
NativeObjectKind.ZSET_ROOT
NativeObjectKind.LISTPACK_BYTES
NativeObjectKind.HASH_FIELD_BYTES
NativeObjectKind.HASH_VALUE_BYTES
NativeObjectKind.SET_MEMBER_BYTES
NativeObjectKind.ZSET_MEMBER_BYTES
```

- [ ] **Step 5: Add defrag regression**

In `NativeStorageRegressionTest`, create list/hash/set/zset values, run `db.performNativeDefragCycleForTests(...)`, then verify all reads return the same bytes and no stale handles are reported.

- [ ] **Step 6: Run native graph/defrag tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -Dtest=YierdisDbNativeHandleGraphTest,NativeStorageRegressionTest,OffHeapLeakRegressionTest test
```

Expected: PASS. Rename `OffHeapLeakRegressionTest` if desired, then update command accordingly.

- [ ] **Step 7: Commit**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "refactor: include collection handles in native graph"
```

---

## Task 16: Update Architecture Guards

**Files:**
- Modify:
  - `YierdisDbArchitectureGuardTest.java`
  - `ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add forbidden legacy symbols**

Architecture guards should fail on production references to:

```text
OffHeapAllocator
OffHeapBuf
OffHeapSlice
YierdisForeignOffHeapAllocator
YierdisFfmSlabAllocator
HeapKeyHandle
FfmKeyHandle
KeyHandle.forHeap
KeyHandle.forFfm
ByteArrayKeyspace
YierdisFfmKeyspace
YierdisHeapExpireIndex
YierdisFfmBytesRef
YierdisFfmBytesRefSlice
YierdisFfmBlobStore
YierdisFfmByteMap
YierdisFfmListpack
ByteArrayHashMap
ByteArrayHashSet
```

Allow only docs/history if the guard already excludes docs.

- [ ] **Step 2: Add positive native-only assertions**

Assert DB memory production sources contain:

```text
YierdisStableNativeAllocator
NativeKeyDirectory
NativeBytesSlice
KeyHandle.forNative
```

- [ ] **Step 3: Run architecture tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-tests/yierdis-architecture-tests
git commit -m "test: enforce native handle only storage architecture"
```

---

## Task 17: Update Documentation

**Files:**
- Modify docs listed in File Structure.

- [ ] **Step 1: Update native runtime docs**

In `native-memory-runtime.md` and `native-allocator-and-handles.md`, describe:

- Stable native handles are the only supported storage memory identity.
- `NativeHandle` generation/domain/kind checks are mandatory.
- Defrag and graph traversal cover collection internals.

- [ ] **Step 2: Update fast path docs**

In `bytes-and-fast-paths.md`, replace old heap/offheap/blob-ref language with:

- command input may arrive as `BytesView`
- storage key identity is native handle
- reply output streams through `NativeBytesSlice`
- heap copies are allowed for ownership-returning APIs like pop/snapshot, not for streaming range/hgetall/members/zrange paths.

- [ ] **Step 3: Update TTL and maxmemory docs**

Document that TTL index and maxmemory candidates use native key handles only.

- [ ] **Step 4: Update coverage docs**

Remove coverage rows for deleted helpers. Add coverage rows for:

- `NativeBytesSlice`
- `NativeByteStore`
- `NativeByteMap`
- `NativeListpack`
- native collection graph traversal

- [ ] **Step 5: Run docs/architecture smoke**

Run:

```bash
rg -n "OffHeapAllocator|OffHeapBuf|OffHeapSlice|HeapKeyHandle|FfmKeyHandle|ByteArrayKeyspace|YierdisFfmKeyspace|YierdisFfmBytesRef|YierdisFfmBlobStore" docs yierdis-db/yierdis-db-memory/src/main/java yierdis-memory
```

Expected: no production/doc references except historical plan files under `docs/superpowers`.

- [ ] **Step 6: Commit**

```bash
git add docs
git commit -m "docs: document native handle only storage"
```

---

## Task 18: Full Verification

**Files:**
- No planned edits.

- [ ] **Step 1: Verify no legacy symbols remain in production**

Run:

```bash
rg -n "OffHeapAllocator|OffHeapBuf|OffHeapSlice|YierdisForeignOffHeapAllocator|YierdisFfmSlabAllocator|HeapKeyHandle|FfmKeyHandle|KeyHandle\\.forHeap|KeyHandle\\.forFfm|ByteArrayKeyspace|YierdisFfmKeyspace|YierdisHeapExpireIndex|YierdisFfmBytesRef|YierdisFfmBytesRefSlice|YierdisFfmBlobStore|YierdisFfmByteMap|YierdisFfmListpack|ByteArrayHashMap|ByteArrayHashSet" yierdis-db yierdis-memory yierdis-server yierdis-command yierdis-networking yierdis-tests
```

Expected: no references except deleted-file mentions in git history are not searched by `rg`.

- [ ] **Step 2: Run full Maven test suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS.

- [ ] **Step 3: Run focused regression suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests,yierdis-tests/yierdis-architecture-tests -am -Dtest=KeyHandleContractTest,NativeKeyDirectoryTest,ExpireIndexContractTest,NativeBytesSliceTest,NativeStorageRegressionTest,OffHeapLeakRegressionTest,CollectionDirectOpsTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,YierdisDbArchitectureGuardTest test
```

Expected: PASS. If `OffHeapLeakRegressionTest` has been renamed, use the new native leak regression class name.

- [ ] **Step 4: Commit final verification-only updates if any**

```bash
git status --short
```

Expected: clean. If small test/docs fixes were required:

```bash
git add <changed-files>
git commit -m "test: verify native handle only storage"
```

---

## Rollback Points

- After Task 4, DB construction should compile with native runtime only.
- After Task 7, keyspace and TTL are native-only even before collection values are fully migrated.
- After Task 10, list streaming should prove `NativeBytesSlice` performance path.
- After Task 14, no legacy heap/blob-ref helpers remain in db-memory.
- After Task 16, architecture tests prevent reintroducing old storage models.

## Self-Review

Spec coverage:

- Native-only key identity: Tasks 5-7 and 16.
- Delete compatibility constructors/config: Task 4.
- Delete old memory API and old allocators: Tasks 2-3.
- Preserve `BytesSlice` streaming without heap copy: Tasks 8, 10-13.
- Migrate collection value internals: Tasks 9-13.
- Delete blob-ref and heap helpers: Task 14.
- Native handle graph and defrag for collection internals: Task 15.
- Tests, architecture guard, docs: Tasks 16-18.

Placeholder scan:

- No task is intentionally deferred. Where implementation details depend on current class internals, the task names exact files, target methods, required behavior, and verification commands.

Type consistency:

- `NativeBytesSlice`, `NativeByteStore`, `NativeByteMap`, and `NativeListpack` are introduced before collection migrations use them.
- `NativeObjectKind` additions are introduced before native helpers use them.
- Legacy API deletion is paired with DB construction cleanup before full compile is expected.
