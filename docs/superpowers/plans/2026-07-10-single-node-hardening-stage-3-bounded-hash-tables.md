# Bounded Hash Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make key, member, and expiry hash tables collision resistant, compactable, shrinkable, and incrementally rehashed with actual-slot and elapsed-time work bounds, while replacing KEYS/SCAN result lists with replayable constant-size windows.

**Architecture:** Give every instance a random 128-bit SipHash key and inject a fixed key in tests. `NativeKeyDirectory`, `NativeByteMap`, and the FFM expiry index share one capacity policy and an active/old-table rehash model. Replacement arrays are allocated through Stage 2 preparation; publishing a resize and migrating individual occupied slots are allocation-free. SCAN carries generation, phase, and position while counting empty and tombstone slots against its budget. KEYS/SCAN discover count and encoded length in one bounded pass and synchronously replay the same logical window in a second pass without retaining key arrays.

**Tech Stack:** Java 25, Maven, JUnit 4, Stage 2 prepared mutations, heap primitive/reference arrays, FFM expiry tables, Redis-style weak SCAN semantics.

## Global Constraints

- Execute after Stage 2 fault-injection and full-suite gates pass.
- Grow when filled slots exceed 75% of capacity.
- Compact at the same capacity when tombstones exceed `max(size, capacity / 8)`.
- Shrink exactly one power-of-two level when size is below 12.5% of capacity; never shrink below 16.
- `clear()` returns every table to initial capacity 16 and releases all native key/member handles.
- A rehash owns an active table, an old table, and a cursor; lookups inspect both and new writes target active.
- Migrated old slots remain non-authoritative SCAN shadows until old-table retirement; lookup and mutation paths ignore them.
- Every inspected empty, tombstone, or filled slot consumes one unit of SCAN and rehash work.
- A complete SCAN does not omit a key that exists for the entire iteration; duplicates during rehash are allowed.
- That no-omission guarantee applies only while one iteration spans fewer than `2^29` structural generations; the finite cursor token is not a durable bookmark.
- KEYS/SCAN capture one expiry-evaluation timestamp, perform no rehash migration or physical expiry deletion during discovery/replay, and charge both passes to their work/time budget.
- Production key enumeration never returns or fills `List<byte[]>`.
- Byte hashing uses SipHash-2-4 directly over bytes and never materializes a `String`.
- Every Maven/Java command uses the explicit JDK 25 prefix.

---

## File Structure

Create:

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashSeed.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/SipHash24.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashCapacityPolicy.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashTableWorkBudget.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashTableWorkResult.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashTableMetrics.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashTableMaintenanceRegistry.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/hash/SipHash24Test.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/hash/HashCapacityPolicyTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/HashTableMaintenanceTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/HashTableMillionOperationChurnTest.java`
- `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/ScanCursorV2Test.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/KeyScanWindow.java`
- `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/KeyScanWindowTest.java`

Modify:

- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ScanCursorV2.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/KeyspaceReadOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeState.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbDataMaintenance.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeByteMap.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisKeyspaceOps.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/NativeByteMapTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/expire/ExpireIndexContractTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/KeysBudgetTest.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`

## Stable Interfaces Produced By This Stage

```java
public record HashSeed(long key0, long key1) {
    public static HashSeed random();
}

public final class SipHash24 {
    public static long hash(HashSeed seed, byte[] value);
    public static long hash(HashSeed seed, BytesView value);
    public static long hash(HashSeed seed, NativeObjectView value);
    public static int foldToInt(long hash);
}
```

```java
public record HashTableWorkBudget(long maxInspectedSlots, long timeLimitNanos) {
    public static HashTableWorkBudget of(long maxInspectedSlots, long timeLimitNanos);
}

public record HashTableWorkResult(
        long inspectedSlots,
        long migratedSlots,
        boolean rehashComplete,
        StopReason stopReason
) {
    public enum StopReason { COMPLETE, SLOT_LIMIT, TIME_LIMIT, NOT_REHASHING }
}
```

```java
public record HashTableMetrics(
        int capacity,
        int size,
        int filledSlots,
        int tombstones,
        boolean rehashing,
        int oldCapacity,
        int rehashCursor,
        long generation,
        long completedRehashes,
        int maximumProbeLength
) {}
```

```java
public final class HashCapacityPolicy {
    public enum Action { NONE, GROW, COMPACT, SHRINK }
    public record Decision(Action action, int targetCapacity) {}

    public static Decision nextAction(
            int capacity,
            int size,
            int filledSlots,
            int tombstones
    );
}
```

`NativeKeyDirectory`, `NativeByteMap`, and `YierdisFfmExpireIndex` expose `metrics()` and `advanceRehash(HashTableWorkBudget)`. Stage 4 reads these metrics; do not rename them.

The SCAN cursor uses the non-negative 63-bit layout below:

```text
bits  0..31  position
bits 32..33  phase (0 active, 1 old)
bits 34..62  table generation
```

`ScanCursorV2` adds `of(int generation, int phase, long position)` and `generation()` while retaining `start()`, `of(long)`, `value()`, `phase()`, `position()`, and decimal bulk-string output. It accepts generation values from zero through `0x1fffffff`, phase zero or one, and position from zero through `0xffffffffL`; all other inputs fail before bit packing. A table encodes the low 29 bits of its monotonic `long` generation. The token may repeat after `2^29` structural changes, so the documented continuously-present-key guarantee is scoped to an iteration shorter than that horizon; no implementation or test may treat the token as globally unique for the DB lifetime.

Key enumeration uses this exact API instead of caller-owned result lists:

```java
public interface KeyScanWindow extends BulkStringSequence, AutoCloseable {
    ScanCursorV2 nextCursor();
    long encodedElementBytes();
    long inspectedSlots();
    long tableGeneration();
    long expiryEvaluationMillis();
    boolean current();
    @Override void close();
}

public interface KeyspaceReadOps {
    KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);
    KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count);
}
```

Construction performs the discovery pass and stores only cursor/window bounds, counts, saturating payload totals, the full table generation, and one expiry-evaluation timestamp. `emitTo` replays matching native keys synchronously and may be called once after discovery; it allocates no key array and never converts a key to `String`. `current()` compares the full generation before replay. `close()` is idempotent and releases any pinned directory view, but never owns key handles. Stage 7 discards and recreates a window if reply-capacity waiting allowed another command to invalidate it.

---

### Task 1: Implement And Verify SipHash-2-4

**Interfaces:** Produces `HashSeed` and `SipHash24` above.

- [ ] **Step 1: Add official-vector tests**

Use the 64 reference outputs for messages containing byte values from zero through `n - 1`, for every `n` from zero through 63, with the 16-byte key encoded as hexadecimal `000102030405060708090a0b0c0d0e0f`. The first four assertions are:

```java
Assert.assertEquals(0x726fdb47dd0e0e31L, hash(0));
Assert.assertEquals(0x74f839c593dc67fdL, hash(1));
Assert.assertEquals(0x0d6c8009d9a94f5aL, hash(2));
Assert.assertEquals(0x85676696d7fb7e2dL, hash(3));
```

Also assert byte array, `BytesView`, and native object view overloads produce identical results for binary data containing zero and `0xff`.

- [ ] **Step 2: Run and verify missing hash types**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=SipHash24Test -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `SipHash24` and `HashSeed` do not exist.

- [ ] **Step 3: Implement the reference algorithm**

Read little-endian 8-byte words, execute two compression rounds per word and four finalization rounds, and incorporate the input length in the high byte of the last word. `HashSeed.random()` uses `SecureRandom` once during instance/factory construction, never per lookup.

- [ ] **Step 4: Run hash tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=SipHash24Test -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: all 64 vectors and all three input paths PASS.

- [ ] **Step 5: Commit keyed hashing**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/hash
git commit -m "feat: add keyed siphash for database keys"
```

Expected: PASS and the SipHash commit succeeds.

### Task 2: Lock The Shared Capacity Policy

**Interfaces:** `HashCapacityPolicy.nextAction(capacity, size, filledSlots, tombstones)` returns the exact nested `Decision`/`Action` types above.

- [ ] **Step 1: Add exact threshold tests**

```java
@Test
public void policyUsesApprovedThresholds() {
    Assert.assertEquals(Action.NONE, next(16, 10, 12, 2).action());
    Assert.assertEquals(Action.GROW, next(16, 11, 13, 2).action());
    Assert.assertEquals(Action.COMPACT, next(64, 20, 41, 21).action());
    Assert.assertEquals(64, next(64, 20, 41, 21).targetCapacity());
    Assert.assertEquals(Action.SHRINK, next(64, 7, 7, 0).action());
    Assert.assertEquals(32, next(64, 7, 7, 0).targetCapacity());
    Assert.assertEquals(Action.NONE, next(16, 1, 1, 0).action());
}
```

- [ ] **Step 2: Run and verify missing policy**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=HashCapacityPolicyTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement policy with overflow checks**

Capacity must be a power of two in `[16, 1 << 30]`. Compare integer products rather than floats. At equal eligibility, grow takes precedence, then compact, then shrink. A grow beyond the maximum throws `NativeCapacityExceededException` during prepare.

- [ ] **Step 4: Run tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=HashCapacityPolicyTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/hash
git commit -m "feat: define bounded hash table capacity policy"
```

Expected: PASS with every threshold, precedence, and overflow case green.

### Task 3: Make `NativeKeyDirectory` Incremental

**Interfaces:** Produces `metrics()`, `advanceRehash`, fixed-seed constructor `NativeKeyDirectory(NativeAllocator, HashSeed)`, and prepared resize publication used by Stage 2 `PreparedEntryMutation`.

- [ ] **Step 1: Add failing grow/lookup/work-budget tests**

Add to `NativeKeyDirectoryTest`:

```java
@Test
public void growPublishesTwoTablesAndMigratesAtMostBudgetedSlots() {
    try (Fixture f = fixture(16, FIXED_SEED)) {
        for (int i = 0; i < 13; i++) f.put(key(i), entry(i));
        Assert.assertTrue(f.directory().metrics().rehashing());
        HashTableWorkResult step = f.directory().advanceRehash(HashTableWorkBudget.of(3, Long.MAX_VALUE));
        Assert.assertEquals(3L, step.inspectedSlots());
        for (int i = 0; i < 13; i++) Assert.assertEquals(entry(i), f.directory().get(key(i)));
    }
}

@Test
public void emptyAndTombstoneSlotsConsumeMigrationBudget() {
    try (Fixture f = sparseRehashFixture()) {
        HashTableWorkResult step = f.directory().advanceRehash(HashTableWorkBudget.of(1, Long.MAX_VALUE));
        Assert.assertEquals(1L, step.inspectedSlots());
        Assert.assertEquals(0L, step.migratedSlots());
    }
}
```

- [ ] **Step 2: Run and observe monolithic rehash**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeKeyDirectoryTest#growPublishesTwoTablesAndMigratesAtMostBudgetedSlots+emptyAndTombstoneSlotsConsumeMigrationBudget -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the 13th insert migrates the whole table immediately.

- [ ] **Step 3: Add active/old table state**

Represent one table with a focused holder containing key handles, entry handles, hashes, states, size, filled, and capacity. States distinguish EMPTY, FILLED, TOMBSTONE, and MIGRATED_SCAN_SHADOW. Stage 2 preparation includes every replacement primitive/reference array byte in `MutationMemoryEstimator`, then allocates the replacement active table. Commit changes `active`, `old`, `rehashCursor`, and generation only. New insertions target active; lookup/removal check active then authoritative old FILLED slots. Deletion from old leaves a tombstone until migration reaches it.

- [ ] **Step 4: Implement bounded migration and metrics**

Every loop iteration increments inspected before checking state. Move a filled slot into active without allocating or copying its native key, then mark the old slot MIGRATED_SCAN_SHADOW rather than empty/tombstone. The shadow owns no separate handle and is ignored by lookup, size, filled-slot policy, and future migration; SCAN may read it while the shared key handle remains live. Deleting the authoritative active entry invalidates its matching shadow before freeing the handle. Complete by releasing old arrays, incrementing `completedRehashes`, and clearing old/cursor. Track the longest observed lookup/insert probe.

- [ ] **Step 5: Run directory, key-handle, and Stage 2 fault tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeKeyDirectoryTest,KeyHandleContractTest,MutationFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; resize allocation failure leaves the old table authoritative.

- [ ] **Step 6: Commit directory rehash**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "feat: incrementally rehash the key directory"
```

Expected: PASS and the incremental key-directory commit succeeds.

### Task 4: Compact And Shrink `NativeKeyDirectory`

**Interfaces:** Uses the shared capacity policy after deletes and on maintenance ticks.

- [ ] **Step 1: Add churn, compaction, shrink, and clear tests**

Insert 1,024 keys, remove 900, advance maintenance to completion, and assert capacity is lower than peak, tombstones are below the compact threshold, remaining keys resolve, removed keys do not, and native live key handles equal directory size. Clear and assert capacity 16, size/filled/tombstones zero.

- [ ] **Step 2: Run and observe retained capacity/tombstones**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeKeyDirectoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Start one-level shrink or same-size compact through prepared resize**

Do not start a second resize while one is active. Reevaluate policy after finishing. A delete itself remains allocation-free; it records maintenance debt. The next user write includes the replacement arrays in its prepared mutation, while a maintenance tick submits an internal no-logical-change mutation through `YierdisDbMutationExecutor`, reserves the exact heap-array peak with `MutationMemoryEstimator`, publishes the replacement table allocation-free, and reports `MutationOutcome.NONE`. Stage 6 marks this plan `requiresCommitStream() == false`, so sink failure does not block representation maintenance or create an event. If memory admission is unavailable, keep the debt and stop that family for the tick without degrading the DB.

- [ ] **Step 4: Run directory tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeKeyDirectoryTest,HashTableMaintenanceTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db/yierdis-db-memory
git commit -m "feat: compact and shrink the key directory"
```

Expected: PASS; compaction/shrink remains bounded and preserves every live key.

### Task 5: Apply The Same Model To `NativeByteMap`

**Interfaces:** Adds fixed-seed constructor `NativeByteMap(NativeByteStore, NativeObjectKind, HashSeed)`, `metrics()`, `advanceRehash`, and prepared put/delete methods consumed by Hash/Set/ZSet prepared mutations.

- [ ] **Step 1: Add collision, replacement, grow, compact, and shrink tests**

Use a package-private hash-injection constructor to force every test key to the same folded hash. Verify 512 colliding binary keys remain distinguishable, replacing one key does not change size, removing 480 keys shrinks after bounded maintenance, and native key-handle counts equal live members.

- [ ] **Step 2: Run and observe no incremental metrics/shrink**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeByteMapTest,HashValueTest,SetValueTest,ZSetValueTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement the active/old map**

Reuse `HashCapacityPolicy` and identical work accounting. Preserve typed values without copying. During rehash, an updated old-table key is removed from old and inserted into active exactly once. A prepared put owns its new native key/value handles until commit; resize-array allocation occurs before bucket visibility.

- [ ] **Step 4: Thread one instance seed into all collection roots**

Generate one `HashSeed` in `YierdisDbEngineFactory` and pass it through `YierdisDbStorageComponents`, `HashRoot`, `SetRoot`, `ZSetRoot`, and their value constructors. Retain existing constructors as package-private fixed-seed test adapters only where current tests require them.

- [ ] **Step 5: Run collection and fault tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeByteMapTest,HashValueTest,SetValueTest,ZSetValueTest,MutationFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit member tables**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "feat: bound collection member hash tables"
```

Expected: PASS and the bounded native byte-map commit succeeds.

### Task 6: Bound FFM Expiry Rehash Work

**Interfaces:** `YierdisFfmExpireIndex` uses the shared thresholds and `advanceRehash`; it continues reporting native region bytes for Stage 4.

- [ ] **Step 1: Add sparse expiry-table work tests**

Create an expiry table with a long empty/tombstone prefix, start rehash, advance with budget 4, and assert `inspectedSlots == 4` even when `migratedSlots == 0`. Verify grow/compact/shrink targets match `HashCapacityPolicy`.

- [ ] **Step 2: Run and reproduce unbounded empty-slot skipping**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ExpireIndexContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because current `rehashStep()` skips empty slots without decrementing work.

- [ ] **Step 3: Replace `rehashStep()` with budgeted advancement**

Ordinary access may request a one-slot budget; maintenance supplies its configured budget. Retired FFM tables close only after they are no longer authoritative and all accounting is updated. Replacement regions are allocated in prepared TTL mutation paths.

- [ ] **Step 4: Run expiry tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ExpireIndexContractTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db/yierdis-db-memory
git commit -m "fix: account actual slots in expiry rehash"
```

Expected: PASS; inspected-slot accounting never exceeds the supplied work budget.

### Task 7: Make KEYS/SCAN Generation-Aware Replayable Windows

**Interfaces:** Produces the exact `ScanCursorV2`, `KeyScanWindow`, and `KeyspaceReadOps` contracts above; no production API accepts or returns a key result list.

- [ ] **Step 1: Add cursor round-trip and sparse SCAN tests**

```java
@Test
public void cursorRoundTripsGenerationPhaseAndPosition() {
    ScanCursorV2 cursor = ScanCursorV2.of(12345, 1, 0xfedcba98L);
    ScanCursorV2 parsed = ScanCursorV2.of(Long.parseLong(new String(cursor.toBulkStringAscii(), US_ASCII)));
    Assert.assertEquals(12345, parsed.generation());
    Assert.assertEquals(1, parsed.phase());
    Assert.assertEquals(0xfedcba98L, parsed.position());
}
```

For sparse SCAN, make slot zero empty and slot seven filled, scan with budget one, and assert the cursor advances to position one without returning the key. Add a rehash case where SCAN has already passed the destination active slot before a key migrates; assert the later old phase returns the MIGRATED_SCAN_SHADOW. Complete the old table before the next cursor call and assert stale-generation restart still returns every continuously present key, allowing duplicates.

Add `KeyScanWindowTest` and `KeysBudgetTest` cases with three matching native keys, tombstones, and an expired key. Assert discovery stores count `3`, the exact saturating encoded element bytes, next cursor, full generation, and one expiry timestamp while the Java object retains no `List`, `Collection`, or key `byte[][]`. Emit into a recording `BulkStringSink` and assert the same three keys and order. Assert both passes count empty/tombstone/filled slots against one shared time/slot budget, do not advance rehash, and do not physically delete expiry entries. Mutate the directory generation before emission and assert `current()` becomes false and no stale window is emitted.

Add API boundary cases for generation `0x1fffffff` and rejection of
`0x20000000`. Add a contract test that injects full generations separated by
exactly `1L << 29`, demonstrates that their encoded tokens are equal, and
asserts the public documentation explicitly limits cursor validity rather than
claiming lifetime uniqueness. Do not add an impossible assertion that an
ancient cursor can be distinguished after token reuse without server-side
cursor state.

- [ ] **Step 2: Run and observe that only filled slots consume the current budget**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ScanCursorV2Test,NativeKeyDirectoryTest,KeysBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because current `NativeKeyDirectory.scan` skips empty-slot work and `KeyspaceReadOps` still fills `List<byte[]>`.

- [ ] **Step 3: Implement two-table weak iteration**

Count each slot before inspecting it. Traverse active phase then old phase for the cursor generation; old-phase scanning returns both authoritative FILLED slots and MIGRATED_SCAN_SHADOW slots. If a cursor generation is stale because its old table retired, restart at the current active phase; this may duplicate but cannot permanently skip a key that remains throughout a complete iteration inside the documented generation horizon. Return zero only after both relevant phases finish. Cursor validation rejects positions outside the encoded 32-bit range before narrowing to an array index. Keep the full monotonic generation in metrics and use only its low 29 bits for the wire token.

Implement one internal scan-window descriptor containing pattern reference, active/old bounds, start/end cursor, full generation, captured evaluation millis, count, encoded element bytes, and discovery work totals. Discovery and `emitTo` share the same matcher and logical-expiry predicate. Neither path calls migration or physical deletion. Sum both pass budgets before returning; a time/slot limit terminates at a well-defined cursor rather than returning a header count that replay cannot reproduce. `KeyCommands` writes the array header from `count()`, then calls `emitTo(new BulkStringReplyAdapter(out))` inside try-with-resources. SCAN writes `[nextCursor, keys]` from the same window. Delete `ArrayList<byte[]>` construction and both old `KeyspaceReadOps` list signatures.

- [ ] **Step 4: Run API, directory, KEYS, and integration SCAN tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-command/yierdis-command-builtin,yierdis-tests/yierdis-integration-tests -am -Dtest=ScanCursorV2Test,KeyScanWindowTest,NativeKeyDirectoryTest,KeysBudgetTest,ScanCursorContractTest,OffHeapKeysCommandSmokeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with unchanged decimal RESP cursors.

- [ ] **Step 5: Commit SCAN behavior**

```bash
git add yierdis-db yierdis-command/yierdis-command-builtin
git commit -m "fix: budget scan by inspected hash slots"
```

Expected: PASS and the replayable key-window commit succeeds.

### Task 8: Integrate Bounded Rehash Maintenance And Metrics

**Interfaces:** `YierdisDbDataMaintenance.rehashMaintenance(HashTableWorkBudget)` returns aggregate work, every table/registry exposes an O(1) conservative `heapEstimatedBytes()`, and `YierdisMemoryStats` exposes pending table count and last stop reason.

- [ ] **Step 1: Add a one-tick budget test**

Create 10,000 idle collection tables plus simultaneous key, expiry, hash, set, and zset rehash debt. Run one maintenance tick with 12 slots and a large time limit. Assert the registry contains only the five debt-bearing participants, the tick does not inspect the idle tables, aggregate inspected slots are at most 12, debt remains, and the reported stop reason is `SLOT_LIMIT`. Repeated ticks must clear all debt and return the registry to empty. Close a table while registered and assert it is removed in O(1) without leaving a stale node. For each active/old table transition, assert `heapEstimatedBytes()` changes from precomputed capacity formulas without walking entries; registry bytes include its fixed object plus intrusive participant references and return to the idle baseline after removal.

- [ ] **Step 2: Run and verify no shared rehash maintenance exists**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=HashTableMaintenanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement fair bounded advancement**

Implement `HashTableMaintenanceRegistry` as an owner-thread-confined intrusive rotating list: each table participant stores registered/previous/next fields, registration and removal allocate nothing and are O(1), and duplicate registration is an invariant failure. A table registers when it starts rehash or records resize debt and unregisters when all debt clears or it closes. Keep a rotating family/participant cursor so a large key directory cannot starve collection or expiry rehash, and never scan idle collection roots to discover work. Pass remaining slot and time budgets to each registered table, aggregate exact work, and record pending tables plus the first exhausted limit. Start pending compact/shrink/grow replacements only through the internal prepared-maintenance path above; expected maxmemory/native-capacity refusal leaves the current table authoritative and records a capacity stop reason. Each table holder computes retained heap bytes from fixed object estimates and active/old primitive/reference-array capacities, and the registry computes its own fixed/intrusive bytes without iteration. Stage 4 aggregates these O(1) values. Do not use size as a proxy for inspected slots.

- [ ] **Step 4: Run maintenance tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=HashTableMaintenanceTest,YierdisDbDefragMaintenanceTest,KeysBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit maintenance integration**

```bash
git add yierdis-db
git commit -m "feat: schedule bounded hash table maintenance"
```

Expected: PASS and the bounded maintenance/metrics commit succeeds.

### Task 9: Run Million-Operation Churn And Full Verification

**Interfaces:** Completes Stage 3 acceptance.

- [ ] **Step 1: Add deterministic million-operation churn**

Use a fixed seed and 4,096-key/member working set across key directory, Hash, Set, and ZSet. Execute exactly 1,000,000 insert/update/delete operations, advance bounded maintenance every 256 operations, then delete all data and drain maintenance. Assert every table returns to capacity 16, no tombstones or rehash debt remain, and live native handles return to the fixture baseline.

- [ ] **Step 2: Run the churn and focused reactors**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=HashTableMillionOperationChurnTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS without increasing the test heap to hide retained tables.

- [ ] **Step 3: Run architecture and full suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS.

- [ ] **Step 4: Commit Stage 3 acceptance**

```bash
git add yierdis-db
git commit -m "test: prove bounded hash table churn"
```
