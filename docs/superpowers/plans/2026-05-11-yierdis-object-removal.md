# YierdisObject Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `YierdisObject` from `yierdis-db-memory` production implementation. The memory DB must use `EntryRecord`, `ValueHandle`, and the matching `TypeRoot` as the authoritative storage path for reads, writes, scans, expiry, maxmemory accounting, and cleanup.

**Architecture:** `YierdisFfmMemoryRuntime` owns native allocation. `NativeKeyDirectory` maps retained key bytes to `EntryHandle`. `EntryTable` stores `EntryRecord`. Each `EntryRecord` carries `ValueType`, `ValueEncoding`, expiry metadata, accounting metadata, and a `ValueHandle` owned by the type-specific root (`StringRoot`, `ListRoot`, `HashRoot`, `SetRoot`, `ZSetRoot`). `YierdisDbKeyLifecycle` becomes the single native lifecycle facade and no longer delegates to `YierdisKeyspace<YierdisObject>`.

**Tech Stack:** Java 25, Maven, JUnit 4, FFM APIs, existing `yierdis-memory-ffm` and `yierdis-db-memory` test suites.

---

## File Map

Production files to change:

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponents.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbInternals.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisStringValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisZSetValue.java`

Test files to change or add:

- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java`

Verification commands use:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-tests/yierdis-architecture-tests -am test -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Task 1: Add a Failing Architecture Guard

- [ ] Remove the `YierdisObject` import from `YierdisDbArchitectureGuardTest`.
- [ ] Replace the existing reflective guard that references `YierdisObject.class` with a source-level guard that does not import the class.
- [ ] Add a production-source scan guard that fails when any file under `yierdis-db/yierdis-db-memory/src/main/java` contains the token `YierdisObject`.

Add this test method:

```java
@Test
public void dbMemoryProductionMustNotReferenceYierdisObject() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Path mainRoot = repoRoot.resolve("yierdis-db/yierdis-db-memory/src/main/java");
    List<String> offenders = new ArrayList<>();

    int scanned = scanForForbiddenText(repoRoot, mainRoot, offenders, "YierdisObject");

    Assert.assertTrue("expected to scan yierdis-db-memory production sources", scanned > 0);
    if (!offenders.isEmpty()) {
        Assert.fail("yierdis-db-memory production sources must not reference YierdisObject:\n"
                + String.join("\n", offenders));
    }
}
```

Use or add these helpers in the same test class:

```java
private static int scanForForbiddenText(
        Path repoRoot,
        Path root,
        List<String> offenders,
        String forbidden
) throws IOException {
    try (Stream<Path> files = Files.walk(root)) {
        List<Path> javaFiles = files
                .filter(path -> Files.isRegularFile(path))
                .filter(path -> path.toString().endsWith(".java"))
                .toList();
        for (Path file : javaFiles) {
            scanFileForForbiddenText(repoRoot, file, offenders, forbidden);
        }
        return javaFiles.size();
    }
}

private static void scanFileForForbiddenText(
        Path repoRoot,
        Path file,
        List<String> offenders,
        String forbidden
) throws IOException {
    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    for (int i = 0; i < lines.size(); i++) {
        if (lines.get(i).contains(forbidden)) {
            offenders.add(repoRoot.relativize(file) + ":" + (i + 1));
        }
    }
}
```

- [ ] Run the architecture guard and confirm RED:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=YierdisDbArchitectureGuardTest test -Dsurefire.failIfNoSpecifiedTests=false
```

Expected result: the new guard fails and lists current production `YierdisObject` references.

- [ ] Commit after this task:

```bash
git add yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java
git commit -m "test: forbid yierdis object in db memory implementation"
```

---

## Task 2: Make NativeKeyDirectory the Key Iteration Surface

- [ ] Add tests to `NativeKeyDirectoryTest` for these behaviors:
  - `getKeyHandle(byte[])` returns a retained key handle for an existing native entry.
  - `randomKeyHandle()` returns an existing key handle when the directory is non-empty and returns `null` when empty.
  - `scan(ScanCursorV2.start(), count, consumer)` visits only live entries and returns `ScanCursorV2.start()` after the last bucket.
  - `forEachEntry(consumer)` visits each live `KeyHandle` and `EntryHandle` pair exactly once.

Test shape:

```java
@Test
public void exposesKeyHandlesForIterationAndRandomSelection() {
    NativeKeyDirectory directory = new NativeKeyDirectory(allocator);
    byte[] first = "first".getBytes(StandardCharsets.UTF_8);
    byte[] second = "second".getBytes(StandardCharsets.UTF_8);

    directory.compute(first, (key, previous) -> new EntryHandle(11L));
    directory.compute(second, (key, previous) -> new EntryHandle(12L));

    KeyHandle firstHandle = directory.getKeyHandle(first);
    Assert.assertNotNull(firstHandle);
    Assert.assertArrayEquals(first, firstHandle.copyBytes());
    Assert.assertNotNull(directory.randomKeyHandle());

    Map<String, Long> seen = new HashMap<>();
    ScanCursorV2 cursor = directory.scan(ScanCursorV2.start(), 16, (keyHandle, entryHandle) -> {
        seen.put(new String(keyHandle.copyBytes(), StandardCharsets.UTF_8), entryHandle.value());
        return true;
    });

    Assert.assertEquals(ScanCursorV2.start(), cursor);
    Assert.assertEquals(Map.of("first", 11L, "second", 12L), seen);
}
```

- [ ] Extend `NativeKeyDirectory` with a constructor that accepts a `YierdisFfmBlobStore` so the directory and `YierdisFfmExpireIndex` can share the same key-retention store:

```java
public NativeKeyDirectory(YierdisFfmMemoryAllocator allocator) {
    this(allocator, new YierdisFfmBlobStore(allocator));
}

public NativeKeyDirectory(YierdisFfmMemoryAllocator allocator, YierdisFfmBlobStore keyStore) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    this.keyStore = Objects.requireNonNull(keyStore, "keyStore");
    this.keyRefs = new YierdisFfmBytesRef[DEFAULT_CAPACITY];
    this.hashes = new int[DEFAULT_CAPACITY];
    this.values = new EntryHandle[DEFAULT_CAPACITY];
}
```

- [ ] Add these native key methods:

```java
public synchronized KeyHandle getKeyHandle(byte[] key) {
    int index = findSlot(key, smear(Arrays.hashCode(key)));
    if (index < 0) {
        return null;
    }
    return KeyHandle.forFfm(keyRefs[index], hashes[index]);
}

public synchronized KeyHandle randomKeyHandle() {
    if (size == 0) {
        return null;
    }
    int start = ThreadLocalRandom.current().nextInt(keyRefs.length);
    for (int step = 0; step < keyRefs.length; step++) {
        int index = (start + step) & (keyRefs.length - 1);
        if (keyRefs[index] != null) {
            return KeyHandle.forFfm(keyRefs[index], hashes[index]);
        }
    }
    return null;
}

public synchronized void forEachEntry(EntryConsumer consumer) {
    Objects.requireNonNull(consumer, "consumer");
    for (int i = 0; i < keyRefs.length; i++) {
        if (keyRefs[i] != null) {
            consumer.accept(KeyHandle.forFfm(keyRefs[i], hashes[i]), values[i]);
        }
    }
}

public synchronized ScanCursorV2 scan(ScanCursorV2 cursor, int count, ScanConsumer consumer) {
    Objects.requireNonNull(cursor, "cursor");
    Objects.requireNonNull(consumer, "consumer");
    if (count <= 0 || size == 0) {
        return ScanCursorV2.start();
    }
    int index = Math.toIntExact(cursor.value());
    int visited = 0;
    while (index < keyRefs.length && visited < count) {
        if (keyRefs[index] != null) {
            visited++;
            boolean keepGoing = consumer.accept(KeyHandle.forFfm(keyRefs[index], hashes[index]), values[index]);
            if (!keepGoing) {
                return new ScanCursorV2(index + 1L);
            }
        }
        index++;
    }
    return index >= keyRefs.length ? ScanCursorV2.start() : new ScanCursorV2(index);
}

@FunctionalInterface
public interface EntryConsumer {
    void accept(KeyHandle keyHandle, EntryHandle entryHandle);
}

@FunctionalInterface
public interface ScanConsumer {
    boolean accept(KeyHandle keyHandle, EntryHandle entryHandle);
}
```

- [ ] Update storage construction so `NativeKeyDirectory` and `YierdisFfmExpireIndex` share the same native key blob store.

Implementation target in `YierdisDb.createStorageComponents`:

```java
YierdisFfmBlobStore keyStore = new YierdisFfmBlobStore(nativeAllocator);
YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(keyStore);
NativeKeyDirectory keyDirectory = new NativeKeyDirectory(nativeAllocator, keyStore);
```

- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeKeyDirectoryTest,ExpireIndexTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java
git commit -m "feat: expose native key directory iteration"
```

---

## Task 3: Move KeyLifecycle Read, Scan, Random, and Expiry to Native Entries

- [ ] Add lifecycle tests in `NativeStorageRegressionTest` that verify:
  - `randomKey()` returns keys inserted through the public API after the compatibility store is not consulted.
  - `scan()` returns keys from `NativeKeyDirectory`.
  - expired native entries are removed from `NativeKeyDirectory`, `EntryTable`, and the matching type root.
  - `dbsize()` uses native entry count.

- [ ] Change `YierdisDbKeyLifecycle` constructor fields from object-store ownership to native ownership:

```java
private final NativeKeyDirectory keyDirectory;
private final EntryTable entryTable;
private final YierdisNativeValueRoots roots;
private final YierdisDbMemoryLedger memoryLedger;
private final LongSupplier touchClock;
```

- [ ] Add native accessors:

```java
KeyHandle keyHandle(byte[] key) {
    purgeExpired(key);
    return keyDirectory.getKeyHandle(key);
}

EntryRecord getLiveEntry(byte[] key) {
    EntryHandle handle = keyDirectory.get(key);
    return getLiveEntry(key, handle);
}

EntryRecord getLiveEntry(KeyHandle keyHandle) {
    EntryHandle handle = entryHandle(keyHandle);
    return getLiveEntry(keyHandle.copyBytes(), handle);
}

EntryRecord getLiveEntry(byte[] key, EntryHandle handle) {
    if (handle == null) {
        return null;
    }
    EntryRecord record = entryTable.get(handle);
    if (record == null) {
        return null;
    }
    if (isExpired(record, currentTimeMillis())) {
        removeEntry(key, handle, record);
        return null;
    }
    touchEntry(handle, record);
    return record;
}
```

- [ ] Add native mutation helpers:

```java
EntryRecord computeEntryWithHandle(
        byte[] key,
        ValueType expectedType,
        BiFunction<KeyHandle, EntryRecord, EntryRecord> remapping
) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(remapping, "remapping");
    return keyDirectory.compute(key, (keyHandle, existingHandle) -> {
        EntryRecord existing = liveRecordOrNull(key, existingHandle);
        if (existing != null && expectedType != null && existing.type() != expectedType) {
            throw new WrongTypeException();
        }
        EntryRecord updated = remapping.apply(keyHandle, existing);
        return writeComputedEntry(keyHandle, existingHandle, existing, updated);
    });
}

private EntryHandle writeComputedEntry(
        KeyHandle keyHandle,
        EntryHandle existingHandle,
        EntryRecord existing,
        EntryRecord updated
) {
    if (updated == null) {
        if (existingHandle != null && existing != null) {
            removeEntry(keyHandle, existingHandle, existing);
        }
        return null;
    }
    EntryHandle handle = existingHandle == null ? entryTable.allocate() : existingHandle;
    EntryRecord record = updated.withKeyHandle(keyHandle.rawHandle()).withVersion(estimatedBytes(keyHandle, updated));
    entryTable.put(handle, record);
    refreshEstimatedBytes(keyHandle, existing, record);
    return handle;
}
```

Use concrete method names already present in the current `EntryTable`, `EntryRecord`, and `KeyHandle` APIs. If `EntryRecord` has no `with...` helpers, construct a new `EntryRecord` with the same field order.

- [ ] Add native removal helpers:

```java
boolean removeEntry(KeyHandle keyHandle, EntryRecord expected) {
    EntryHandle handle = entryHandle(keyHandle);
    if (handle == null) {
        return false;
    }
    EntryRecord current = entryTable.get(handle);
    if (!Objects.equals(current, expected)) {
        return false;
    }
    removeEntry(keyHandle, handle, current);
    return true;
}

void removeEntry(KeyHandle keyHandle, EntryHandle handle, EntryRecord record) {
    releaseValue(record);
    entryTable.remove(handle);
    keyDirectory.remove(keyHandle);
    memoryLedger.onRemove(record.version());
}

private void releaseValue(EntryRecord record) {
    if (record == null || record.valueHandle() == null || record.valueHandle().raw() == 0L) {
        return;
    }
    roots.root(record.type()).release(record.valueHandle());
}
```

- [ ] Replace lifecycle `size`, `randomKeyHandle`, `forEachKeyHandle`, `scan`, expiry purge, and maxmemory candidate traversal with `NativeKeyDirectory` and `EntryTable`.
- [ ] Keep the old object methods temporarily only as compile shims during this task. They must delegate to native methods or be unused by the end of Task 7.
- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,ExpireIndexTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java
git commit -m "feat: make native entries key lifecycle authority"
```

---

## Task 4: Migrate String and HyperLogLog Ops to StringRoot

- [ ] Add regression tests for native-only string behavior:
  - `SET`/`GET`/`GETDEL`/`GETEX`/`STRLEN`
  - `INCRBY` and `INCRBYFLOAT`
  - `APPEND`
  - `SET` with NX, XX, GET, KEEPTTL, EX, PX
  - wrong-type behavior after list/hash/set/zset values exist
  - memory stats stay balanced after overwrite, delete, expire, and failed type conversion

- [ ] Add HLL tests:
  - `PFADD` creates a `ValueType.STRING` record with `ValueEncoding.STRING_RAW`
  - `PFCOUNT` reads the sketch through `StringRoot`
  - `PFMERGE` releases overwritten destination handles

- [ ] Refactor `YierdisStringOps`:
  - Replace every `YierdisObject` branch with `EntryRecord`.
  - Read bytes with `stringRoot.get(record.valueHandle())`.
  - Write bytes with `stringRoot.put(byte[])`.
  - Preserve integer-looking string encoding by setting `ValueEncoding.STRING_INT` while storing canonical decimal bytes in `StringRoot`.
  - Release old handles on overwrite using lifecycle `releaseValue`.
  - Store estimated bytes in `EntryRecord.version()`.

Factory helper:

```java
private EntryRecord stringRecord(
        KeyHandle keyHandle,
        ValueHandle valueHandle,
        ValueEncoding encoding,
        long expireAtMillis
) {
    return new EntryRecord(
            keyHandle.rawHandle(),
            valueHandle,
            keyHandle.hash(),
            ValueType.STRING,
            encoding,
            0,
            expireAtMillis,
            0L,
            touchClock.getAsLong());
}
```

- [ ] Refactor `YierdisHyperLogLog` so all helpers accept `EntryRecord` and `ValueHandle`:

```java
private byte[] readSketch(EntryRecord record) {
    if (record == null) {
        return EMPTY_HLL;
    }
    if (record.type() != ValueType.STRING) {
        throw new WrongTypeException();
    }
    return stringRoot.get(record.valueHandle());
}
```

- [ ] Remove all `YierdisObject` imports from `YierdisStringOps` and `YierdisHyperLogLog`.
- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,YierdisDbMemoryEstimatorTest,MemoryStatsAccountingConsistencyTest,StringRootTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryEstimatorTest.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java
git commit -m "refactor: route string ops through native records"
```

---

## Task 5: Migrate List and Hash Ops to Native Roots

- [ ] Add or extend regression tests for:
  - `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`, `LLEN`
  - `HSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`, `HSCAN`
  - expiry and delete releasing list/hash handles
  - overwrite from list to string and from hash to string releasing native handles

- [ ] Refactor `YierdisListOps`:
  - Replace `YierdisObject` reads with `EntryRecord` reads.
  - Use `ListRoot` for all list mutation and readback.
  - On an empty result after pop/trim, return `null` from lifecycle compute so the key is removed.
  - Keep `ValueEncoding.LIST_PACKED` for current native list representation.

- [ ] Refactor `YierdisHashOps`:
  - Replace `YierdisObject` reads with `EntryRecord`.
  - Use `HashRoot` for all hash mutation and readback.
  - On empty hash after delete, remove the native entry.
  - Keep `ValueEncoding.HASH_TABLE` for current native hash representation.

- [ ] Remove all `YierdisObject` imports from list and hash packages.
- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,ListRootTest,CollectionRootTest,HashValueTest,OffHeapCollectionReadStreamingTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java
git commit -m "refactor: route list and hash ops through native records"
```

---

## Task 6: Migrate Set and ZSet Ops to Native Roots

- [ ] Add or extend regression tests for:
  - `SADD`, `SREM`, `SPOP`, `SCARD`, `SMEMBERS`, `SSCAN`
  - `ZADD`, `ZREM`, `ZCARD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZSCAN`
  - empty set/zset mutation removing the native entry
  - overwrite and expiry releasing set/zset handles

- [ ] Refactor `YierdisSetOps`:
  - Replace `YierdisObject` reads with `EntryRecord`.
  - Use `SetRoot` for all set mutation and readback.
  - Return `null` from lifecycle compute when the set becomes empty.
  - Keep `ValueEncoding.SET_TABLE`.

- [ ] Refactor `YierdisZSetOps`:
  - Replace `YierdisObject` reads with `EntryRecord`.
  - Use `ZSetRoot` for all sorted-set mutation and readback.
  - Return `null` from lifecycle compute when the sorted set becomes empty.
  - Keep `ValueEncoding.ZSET_SKIPLIST`.

- [ ] Remove all `YierdisObject` imports from set and zset packages.
- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,SetValueTest,ZSetValueTest,CollectionRootTest,OffHeapCollectionReadStreamingTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java
git commit -m "refactor: route set and zset ops through native records"
```

---

## Task 7: Remove Compatibility Store Wiring

- [ ] Remove `YierdisKeyspace<YierdisObject>` fields from:
  - `YierdisDbStorageComponents`
  - `YierdisDbComponents`
  - `YierdisDb`
  - `YierdisDbMemoryReporter`
  - `YierdisDbOwnedResources`
  - `YierdisDbComponentFactory`
  - `YierdisDbKeyLifecycle`

- [ ] Remove `OwnerCallbacks.touch(YierdisObject)` and replace it with a native clock or native touch callback:

```java
@FunctionalInterface
interface NativeTouchClock {
    long nextAccessValue();
}
```

- [ ] Remove compatibility lifecycle methods:
  - `getLiveObject`
  - `getStoredObject`
  - `computeObjectWithHandle`
  - `computeObjectIfPresentWithHandle`
  - `removeObject`
  - `estimatedBytesForRemoval(KeyHandle, YierdisObject)`
  - `toEntryRecord(KeyHandle, YierdisObject, ...)`

- [ ] Replace reporter and internals object accounting with native accounting:

```java
long estimatedBytes() {
    return memoryLedger.usedBytes();
}

long keyCount() {
    return keyDirectory.size();
}

long nativeBytes() {
    return keyDirectory.nativeBytes() + entryTable.nativeBytes() + roots.nativeBytes();
}
```

- [ ] Ensure resource cleanup closes only native owners:
  - `NativeKeyDirectory`
  - `EntryTable`
  - `StringRoot`
  - `ListRoot`
  - `HashRoot`
  - `SetRoot`
  - `ZSetRoot`
  - `YierdisFfmExpireIndex`
  - `YierdisFfmMemoryRuntime`

- [ ] Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,YierdisDbMemoryEstimatorTest,MemoryStatsAccountingConsistencyTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,ExpireIndexTest,YierdisFfmRehashConsistencyTest test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal
git commit -m "refactor: remove yierdis object store wiring"
```

---

## Task 8: Delete YierdisObject Value Layer

- [ ] Confirm no production source outside `internal/value` references `YierdisObject`:

```bash
rg "YierdisObject" yierdis-db/yierdis-db-memory/src/main/java
```

Expected result before deletion: matches only in the compatibility value classes.

- [ ] Delete unused compatibility value files:

```bash
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisObject.java
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisStringValue.java
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisListValue.java
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHashValue.java
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisSetValue.java
git rm yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisZSetValue.java
```

- [ ] Remove imports and test references for deleted classes.
- [ ] Run architecture guard and confirm GREEN:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=YierdisDbArchitectureGuardTest test -Dsurefire.failIfNoSpecifiedTests=false
```

- [ ] Run full DB memory regression:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
```

- [ ] Commit:

```bash
git add yierdis-db/yierdis-db-memory/src/main/java yierdis-db/yierdis-db-memory/src/test/java yierdis-tests/yierdis-architecture-tests/src/test/java
git commit -m "refactor: delete yierdis object compatibility layer"
```

---

## Task 9: Final Verification and PR Sync

- [ ] Run final verification:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-tests/yierdis-architecture-tests -am test -Dsurefire.failIfNoSpecifiedTests=false
```

- [ ] Confirm no production reference remains:

```bash
rg "YierdisObject|YierdisStringValue|YierdisListValue|YierdisHashValue|YierdisSetValue|YierdisZSetValue" yierdis-db/yierdis-db-memory/src/main/java
```

Expected result: no output.

- [ ] Confirm branch status:

```bash
git status --short
git log --oneline --decorate -8
```

- [ ] Push the branch backing PR #24:

```bash
git push origin ffm-native-storage-core
```

- [ ] Update PR #24 with a Chinese summary of the removal:

```bash
gh pr edit 24 --body-file /tmp/yierdis-pr-24-body.md
```

Include these points in the PR body:

- 新增 native storage core 的 slab allocator、entry directory、entry table、type roots。
- `YierdisObject` 兼容层已从 `yierdis-db-memory` 实现中删除。
- 新 hot path 使用 `EntryRecord`、`ValueHandle` 和对应 `TypeRoot`。
- key lifecycle、scan、random key、expire、maxmemory accounting、resource cleanup 均以 native entry 为准。
- 验证命令和结果。

- [ ] Commit PR body file only if it is intentionally stored in the repository. Otherwise keep it under `/tmp`.

---

## Self-Review Checklist

- [ ] `YierdisObject` is absent from `yierdis-db/yierdis-db-memory/src/main/java`.
- [ ] `YierdisKeyspace<YierdisObject>` is absent from production source.
- [ ] Public DB behavior for string, HLL, list, hash, set, and zset tests is unchanged.
- [ ] Expired entries release their `TypeRoot` handles exactly once.
- [ ] Overwrites release old handles exactly once.
- [ ] `dbsize`, `scan`, and `randomKey` use native key directory state.
- [ ] `usedBytes`, native memory stats, and eviction estimates stay stable after create, overwrite, delete, expire, and close.
- [ ] Architecture guard fails before removal and passes after removal.
- [ ] PR #24 points at branch `ffm-native-storage-core` and contains the final Chinese description.
