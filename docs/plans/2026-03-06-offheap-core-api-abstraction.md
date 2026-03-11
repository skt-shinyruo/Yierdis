# Off-Heap Core API Abstraction (Dependency Inversion) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move the off-heap allocator boundary into `yierdis-core-api` as a neutral SPI so core modules stop leaking `Object`/implementation types across boundaries, and so `yierdis-memory-*` becomes a pluggable implementation layer.

**Architecture:** Introduce a neutral `yier.bubu.redis.offheap.api` contract (`OffHeapAllocator`, `OffHeapBuf`, `OffHeapSlice`, `OffHeapAddressAllocator`, etc.) in `yierdis-core-api`. Make `yierdis-memory-api` types (`YierdisOffHeapAllocator`, `YierdisOffHeapBuf`, …) extend these contracts, keeping backend selection/ServiceLoader in `yierdis-memory-api`. Then migrate core modules to depend only on core-api off-heap contracts, including making `DbEngineFactory` type-safe by accepting `OffHeapAllocator` instead of `Object`.

**Tech Stack:** Java 17, Maven multi-module build, JUnit4 tests.

---

### Task 1: Add neutral off-heap contracts to `yierdis-core-api`

**Files:**
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAllocator.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapBuf.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapSlice.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapBlock.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAddressAllocator.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapOutOfMemoryException.java`
- Test: `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/offheap/api/OffHeapContractsSmokeTest.java`

**Step 1: Write a failing compile-time smoke test**

Create `OffHeapContractsSmokeTest` that:
- defines a tiny in-test `OffHeapAllocator` implementation and `OffHeapBuf`/`OffHeapSlice` stubs
- asserts `OffHeapSlice` is-a `yier.bubu.redis.bytes.BytesSlice`
- asserts `OffHeapAllocator.allocate()` returns a non-null `OffHeapBuf` and can be sliced

Expected: compile fails (contracts not defined yet).

**Step 2: Implement `OffHeapSlice`**

Create:

```java
package yier.bubu.redis.offheap.api;

/**
 * Neutral off-heap bytes slice view.
 * <p>
 * This is intentionally minimal and extends the SSOT bytes abstraction.
 */
public interface OffHeapSlice extends yier.bubu.redis.bytes.BytesSlice {
}
```

**Step 3: Implement `OffHeapBuf`**

Create:

```java
package yier.bubu.redis.offheap.api;

import yier.bubu.redis.bytes.BytesSource;

public interface OffHeapBuf extends AutoCloseable {
    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    void setBytes(int index, BytesSource src, int srcIndex, int len);

    OffHeapSlice slice(int index, int len);

    @Override
    void close();
}
```

**Step 4: Implement `OffHeapAllocator`**

Create:

```java
package yier.bubu.redis.offheap.api;

public interface OffHeapAllocator extends AutoCloseable {
    OffHeapBuf allocate(int capacity);

    long usedBytes();

    /**
     * Hard cap in bytes, or 0 when unlimited.
     */
    long maxBytes();

    @Override
    void close();
}
```

**Step 5: Implement address capability (`OffHeapAddressAllocator` + `OffHeapBlock`)**

Create:

```java
package yier.bubu.redis.offheap.api;

/**
 * Optional capability interface for off-heap allocators that expose raw address-based memory operations.
 */
public interface OffHeapAddressAllocator extends OffHeapAllocator {
    OffHeapBlock allocateBlock(int capacity);

    long allocateAddress(int capacity);

    void freeAddress(long address, int capacity);

    byte getByte(long address);

    void putByte(long address, byte value);

    void setMemory(long address, long bytes, byte value);

    void copyMemory(long srcAddress, long dstAddress, long bytes);

    void copyMemory(byte[] src, int srcIndex, long dstAddress, int len);

    void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len);
}
```

And:

```java
package yier.bubu.redis.offheap.api;

/**
 * A raw off-heap memory block backed by an address with deterministic free via {@link #close()}.
 */
public interface OffHeapBlock extends AutoCloseable {
    long address();

    int capacity();

    @Override
    void close();
}
```

**Step 6: Add neutral OOM exception**

Create:

```java
package yier.bubu.redis.offheap.api;

/**
 * Off-heap allocation failure (hard cap exceeded or backend cannot reserve).
 */
public class OffHeapOutOfMemoryException extends RuntimeException {
    public OffHeapOutOfMemoryException(String message) {
        super(message);
    }
}
```

**Step 7: Run core-api tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-api test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

**Step 8: Commit (optional)**

Run:
`git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/offheap/api/OffHeapContractsSmokeTest.java`  
`git commit -m "feat(core-api): add neutral off-heap SPI contracts"`

---

### Task 2: Make `yierdis-memory-api` extend the neutral core-api contracts

**Files:**
- Modify: `yierdis-memory/api/pom.xml`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocator.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapBuf.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapSlice.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAddressAllocator.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapBlock.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapOutOfMemoryException.java`

**Step 1: Add dependency to core-api**

In `yierdis-memory/api/pom.xml`, add:

```xml
<dependency>
  <groupId>yier.bubu.redis</groupId>
  <artifactId>yierdis-core-api</artifactId>
</dependency>
```

Expected: offheap-api compiles with access to `yier.bubu.redis.offheap.api.*`.

**Step 2: Bridge allocator/buf/slice via `extends` + covariant returns**

Update `YierdisOffHeapAllocator`:
- `extends yier.bubu.redis.offheap.api.OffHeapAllocator`
- redeclare `YierdisOffHeapBuf allocate(int capacity);` (covariant return)
- keep `YierdisOffHeapBackend backend();` as implementation-specific metadata

Update `YierdisOffHeapBuf`:
- `extends yier.bubu.redis.offheap.api.OffHeapBuf`
- redeclare `YierdisOffHeapSlice slice(int index, int len);`

Update `YierdisOffHeapSlice`:
- `extends yier.bubu.redis.offheap.api.OffHeapSlice` (do not duplicate `BytesSlice` linkage)

Update address/block:
- `YierdisOffHeapAddressAllocator extends yier.bubu.redis.offheap.api.OffHeapAddressAllocator, YierdisOffHeapAllocator`
- `YierdisOffHeapBlock extends yier.bubu.redis.offheap.api.OffHeapBlock`

**Step 3: Make offheap OOM exception a subtype of the neutral one**

Change `YierdisOffHeapOutOfMemoryException`:
- `extends yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException`
- keep constructor `public YierdisOffHeapOutOfMemoryException(String message) { super(message); }`

**Step 4: Run offheap-api tests**

Run: `mvn -q -pl yierdis-memory/api test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

**Step 5: Commit (optional)**

Run:
`git add yierdis-memory/api/pom.xml yierdis-memory/api/src/main/java/yier/bubu/redis/db/offheap/api`  
`git commit -m "refactor(offheap-api): extend neutral core-api off-heap contracts"`

---

### Task 3: Make `DbEngineFactory` type-safe by using `OffHeapAllocator`

**Files:**
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java`

**Step 1: Update SPI signature**

In `DbEngineFactory`, change:
- `Object offHeapAllocator` → `yier.bubu.redis.offheap.api.OffHeapAllocator offHeapAllocator`

Expected: compile fails in all implementers/callers until updated.

**Step 2: Update default factory**

In `YierdisDbEngineFactory#create`:
- update signature to match
- remove `instanceof YierdisOffHeapAllocator` casting (parameter already typed)
- pass allocator through to `new YierdisDb(...)` (after Task 4 updates `YierdisDb` signatures)

**Step 3: Update runtime assembly call site**

In `YierdisInstance#create`, update the `engineFactory.create(...)` call to pass `OffHeapAllocator` (already from config after Task 5), not `Object`.

**Step 4: Update injection test**

In `DbEngineFactoryInjectionTest`, update the anonymous `DbEngineFactory` implementation signature to accept `OffHeapAllocator`.

**Step 5: Run runtime tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 4: Migrate `yierdis-core-db` to the neutral off-heap contracts

**Files:**
- Modify: `yierdis-core/yierdis-core-db/pom.xml`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/DbMemoryAccounting.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/*Value.java` (Hash/List/Set/ZSet as needed)
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/*` (KeyHandle, OffHeapKeyHandle, KeyHandleAccess)
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/offheap/*` (unsafe off-heap internals that currently import `yierdis-memory-api` types)

**Step 1: Switch imports from `yier.bubu.redis.db.memory.api.*` to `yier.bubu.redis.offheap.api.*`**

Mechanical change across the module:
- `YierdisOffHeapAllocator` → `OffHeapAllocator`
- `YierdisOffHeapBuf` → `OffHeapBuf`
- `YierdisOffHeapSlice` → `OffHeapSlice`
- `YierdisOffHeapAddressAllocator` → `OffHeapAddressAllocator`
- `YierdisOffHeapBlock` → `OffHeapBlock`
- `YierdisOffHeapOutOfMemoryException` → `OffHeapOutOfMemoryException`

**Step 2: Update `YierdisDb` constructor and feature gates**

In `YierdisDb`:
- change `offHeapAllocator` field type to `OffHeapAllocator`
- change the `offHeapKeysEnabled` guard to require `OffHeapAddressAllocator` (neutral)
- keep behavior: if enabled and allocator is address-capable, use unsafe keyspace/expire index; else use heap structures

**Step 3: Update OOM handling**

In `YierdisDbValueOps` and any other code catching off-heap OOM:
- catch `OffHeapOutOfMemoryException` (neutral)
- keep user-visible error messages stable (existing tests assert message fragments)

**Step 4: Drop `yierdis-memory-api` dependency**

In `yierdis-core-db/pom.xml` remove:

```xml
<dependency>
  <groupId>yier.bubu.redis</groupId>
  <artifactId>yierdis-memory-api</artifactId>
</dependency>
```

Expected: core-db still compiles because it now only references core-api neutral contracts.

**Step 5: Run core-db tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-db test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 5: Migrate `yierdis-core-runtime` config to the neutral allocator type

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/pom.xml`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java` (and any other allocator-typed tests)

**Step 1: Switch `YierdisInstanceConfig.offHeapAllocator` to `OffHeapAllocator`**

In `YierdisInstanceConfig`:
- field type: `OffHeapAllocator`
- builder setter: `offHeapAllocator(OffHeapAllocator allocator)`

**Step 2: Update `YierdisInstance` fields and shared usage computation**

In `YierdisInstance`:
- field type: `OffHeapAllocator`
- shared usage source remains `allocator.usedBytes()`

**Step 3: Drop runtime main dependency on offheap-api**

In `yierdis-core-runtime/pom.xml` remove:

```xml
<dependency>
  <groupId>yier.bubu.redis</groupId>
  <artifactId>yierdis-memory-api</artifactId>
</dependency>
```

Keep any offheap backends only in test scope if tests directly instantiate them.

**Step 4: Run runtime tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 6: Remove core-command dependency on offheap-api (catch neutral OOM)

**Files:**
- Modify: `yierdis-core/yierdis-core-command/pom.xml`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`

**Step 1: Change the catch clause**

In `YierdisFastCommandProcessor`:
- replace `catch (YierdisOffHeapOutOfMemoryException e)` with `catch (OffHeapOutOfMemoryException e)`
- keep the output string stable: `OOM off-heap memory limit exceeded`

**Step 2: Remove the offheap-api dependency**

In `yierdis-core-command/pom.xml` remove:

```xml
<dependency>
  <groupId>yier.bubu.redis</groupId>
  <artifactId>yierdis-memory-api</artifactId>
</dependency>
```

**Step 3: Run command tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-command test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 7: Update server/bootstrap wiring (compile-only)

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

**Step 1: Fix `YierdisInstanceConfig.builder().offHeapAllocator(...)` call sites**

After Task 5, the setter takes `OffHeapAllocator`:
- keep server code constructing allocator via `YierdisOffHeapAllocators.create(...)`
- pass it directly into the config builder

Expected: no semantic change; only type changes to satisfy compilation.

**Step 2: Build server module**

Run: `mvn -q -pl yierdis-server -DskipTests package -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 8: Full build verification

**Step 1: Run full test suite**

Run: `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

**Step 2: (Optional) `mvn -q -DskipTests package`**

Run: `mvn -q -DskipTests package -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.
