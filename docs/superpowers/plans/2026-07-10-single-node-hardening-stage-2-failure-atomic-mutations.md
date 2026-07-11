# Failure-Atomic Mutations And OOM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee that every expected ledger, allocator, slot, and native-capacity failure leaves logical data and resource accounting unchanged, translate all such failures to the existing Redis OOM reply, degrade the DB on true invariant failures, and transfer mutation-returned values through closeable zero-copy reply views.

**Architecture:** Replace apply-in-place `MutationPlan` with prepared storage changes whose commit path cannot allocate. A mutation executor reserves memory, prepares every key/value/entry/TTL resource invisibly, reconciles allocator heap/native growth plus non-allocator staging, commits pointer and counter changes, then either releases superseded resources or transfers returned old/popped values to closeable reply owners. A deterministic fail-on-allocation wrapper drives every allocation boundary. Capacity failures share one exception hierarchy; lifecycle corruption remains a distinct invariant failure, and any exception after commit starts is handled without logical rollback.

**Tech Stack:** Java 25, Maven, JUnit 4, Stage 1 segmented allocator and `MemoryUsageSnapshot`, existing DB capability interfaces, existing Redis command error mapping.

## Global Constraints

- Execute this plan only after the Stage 1 full suite passes.
- Preserve RESP command names, argument validation, success replies, `MutationOutcome`, and command-by-command MULTI/EXEC behavior.
- Expected ledger, allocator hard-limit, native slot, page-region, and FFM allocation failures return exactly `OOM command not allowed when used memory > 'maxmemory'.` and do not close a healthy connection.
- `NativeMemoryException` remains an invariant/lifecycle exception and is never translated to OOM.
- Unexpected storage invariant failures mark the DB degraded; reads remain available and later writes return exactly `MISCONF DB is in a degraded state; writes are disabled`.
- `PreparedMutation.commit()` must not reserve memory, grow a table, create a native object, enqueue an event, or perform any other capacity-sensitive operation.
- `PreparedMutation.abort()` and `close()` are idempotent and restore all staged resource counters.
- Deletion-only reclamation bypasses recursive cleanup/eviction/governor admission, has zero positive persistent growth, and commits only a non-positive delta.
- `MutationPlan.upperBoundBytes()` is a conservative peak physical-growth estimate, including new native metadata segments/pages, FFM replacement regions, and heap arrays; it is not merely the logical value length.
- Aborting preparation restores the pre-command `MemoryUsageSnapshot`, including provisional metadata segments and warm pages created by the failed command.
- SET GET and counted LPOP/RPOP do not copy returned payloads into detached arrays or `List<byte[]>`; commit transfers native-value ownership to an idempotently closeable result and abort transfers nothing.
- Every Java/Maven command uses the explicit JDK 25 prefix from Stage 1.

---

## File Structure

Create:

- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeCapacityExceededException.java`
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocationScope.java`
- `yierdis-memory/yierdis-memory-testkit/pom.xml`
- `yierdis-memory/yierdis-memory-testkit/src/main/java/yier/bubu/redis/memory/testkit/FailOnAllocationNativeAllocator.java`
- `yierdis-memory/yierdis-memory-testkit/src/test/java/yier/bubu/redis/memory/testkit/FailOnAllocationNativeAllocatorTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/NativeAllocationScopeTest.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbHealthSnapshot.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/PostCommitMutationException.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/PoppedValueSequence.java`
- `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/OwnedReplyValueTest.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedMutation.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedDbMutation.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/AbstractPreparedMutation.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/MutationMemoryEstimator.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedEntryMutation.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbHealth.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MutationFaultInjectionTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/NativeCapacityOomRecoveryTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/DegradedDbWriteRejectionTest.java`

Modify:

- `pom.xml`
- `yierdis-memory/pom.xml`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectSegment.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageDirectory.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngine.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/StringWriteOps.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ListWriteOps.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringValue.java`
- `yierdis-db/yierdis-db-memory/pom.xml`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/MemoryLedger.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMemoryLedger.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbInternals.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeState.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSkipList.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeListpack.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHllOps.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ledger/MutationExecutorReservationTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

## Stable Interfaces Produced By This Stage

Use the approved contract verbatim:

```java
public interface PreparedMutation<T> extends AutoCloseable {
    long actualDeltaBytes();
    long stagedNonNativeGrowthBytes();
    T commit();
    void releaseSuperseded();
    void abort();

    @Override
    default void close() {
        abort();
    }
}
```

DB mutations additionally expose the already-computed logical outcome without allocating during commit:

```java
public interface PreparedDbMutation<T> extends PreparedMutation<T> {
    MutationOutcome outcome();
}
```

The executor consumes this exact plan shape through Stage 5; Stage 6 adds commit admission to it without changing `PreparedMutation`:

```java
public interface MutationPlan<T> {
    enum AdmissionMode { NORMAL, RECLAMATION }

    long upperBoundBytes();
    default AdmissionMode admissionMode() { return AdmissionMode.NORMAL; }
    PreparedDbMutation<T> prepare();
}

public <T> T execute(MutationPlan<T> plan);
```

`MemoryLedger` gains:

```java
void reconcile(MemoryReservation reservation, long requiredBytes);
MemoryReservation beginReclamation();
```

Reconciliation may reduce a reservation. `requiredBytes > reservation.reservedBytes()` is an invariant failure because every caller must provide a conservative upper bound. The required peak is `addSaturating(nativeScope.growth().effectiveBytes(), prepared.stagedNonNativeGrowthBytes())`; `actualDeltaBytes()` remains the steady-state ledger delta used only by `ledger.commit`. `beginReclamation()` returns a zero-byte token without invoking expiration cleanup, eviction, or a global coordinator. A `MutationPlan` adds `AdmissionMode admissionMode()` with `NORMAL` as the default and `RECLAMATION` for known deletion-only work. Before commit, the executor requires a reclamation plan's upper bound, measured native growth, and staged persistent growth to be zero; its committed delta must be non-positive. `PreparedMutation` moves `PREPARED -> COMMITTING -> COMMITTED -> RELEASED` on success or `PREPARED -> ABORTED` before commit starts. A throwable in COMMITTING is failure-terminal, degrades the DB, and never makes `abort()` legal again.

Native staging uses this exact contract:

```java
public interface NativeAllocationScope extends AutoCloseable {
    NativeAllocationGrowth growth();
    void promote();
    void abort();

    @Override
    default void close() { abort(); }
}
```

`NativeAllocator.beginAllocationScope()` permits exactly one active scope on its owner thread. Every allocation made before `promote()` belongs to that scope. `growth()` returns allocator-owned heap-directory, metadata-segment, and data-page growth using Stage 1's `NativeAllocationGrowth`. `abort()` frees remaining scoped handles and closes metadata segments and data pages created solely by the scope; `promote()` is allocation-free, non-throwing, and makes those segments/pages permanent under Stage 1 retention rules. Both terminal methods are idempotent.

Every mutation computes its peak admission value with one helper:

```java
static long peakAdditionalBytes(
        NativeAllocator allocator,
        long ffmRegionGrowthBytes,
        long heapGrowthBytes,
        int... nativeAllocationSizes
);
```

`MutationMemoryEstimator` rejects negative inputs and saturating-adds `allocator.estimateAdditionalGrowth(nativeAllocationSizes).effectiveBytes()`, exact new non-allocator FFM region capacity, and conservative new heap-array/object estimates. A saturated result is passed to `MutationPlan.upperBoundBytes()` and therefore rejects admission.

Mutation-returned values use these exact Stage 7-ready ownership contracts while retaining all existing non-owning factories:

```java
public final class BulkStringValue implements AutoCloseable {
    public static BulkStringValue nullValue();
    public static BulkStringValue bytes(byte[] data);
    public static BulkStringValue bytes(byte[] data, int off, int len);
    public static BulkStringValue slice(BytesSlice slice);
    public static BulkStringValue longAscii(long value);
    public static BulkStringValue owned(
            BytesSlice slice,
            int payloadLength,
            long retainedMemoryBytes,
            AutoCloseable owner
    );

    public boolean isNull();
    public int payloadLength();
    public long retainedMemoryBytes();
    public void writeTo(BulkStringSink out);
    @Override public void close();
}

public interface PoppedValueSequence extends BulkStringSequence, AutoCloseable {
    boolean isNull();
    long encodedElementBytes();
    long retainedMemoryBytes();
    @Override void close();
}
```

`payloadLength()` returns `-1` for null and the exact bulk payload length otherwise. `encodedElementBytes()` is the saturating sum of each `$<length>\r\n<payload>\r\n` element and excludes only the outer aggregate header; Stage 7 adds that header, retained-source, chunk, and fixed component charges. Non-owning `BulkStringValue` factories have zero retained bytes and no-op idempotent close. `owned` rejects negative lengths/retained bytes, retains no channel/runtime object, and closes its owner exactly once. A popped sequence owns detached native values in reply order and closes every remaining value exactly once after emission, cancellation, or disconnect.

`StringWriteOps.SetStringValue` becomes `record SetStringValue(boolean applied, BulkStringValue oldValue) implements AutoCloseable`, normalizes a null component to `BulkStringValue.nullValue()`, and delegates `close()` to it. `ListWriteOps.lpop/rpop` return `WriteResult<PoppedValueSequence>`. The prepared result wrapper and every ownership node are allocated before visibility commit; commit only transfers already-created ownership, and abort closes it without exposing it.

Post-visibility failures cross layers through one marker that Stage 7 treats as an unknown client result:

```java
public final class PostCommitMutationException extends RuntimeException {
    public PostCommitMutationException(String message, Throwable cause);
}
```

Only `YierdisDbMutationExecutor` constructs this type, after `commitStarted` is true and conservative ledger/scope settlement plus DB degradation have completed. Expected resource failures and all pre-commit failures never use it. Command error mapping must not translate it to OOM, BUSY, MISCONF, or a normal internal-error reply.

---

### Task 1: Unify Native Capacity Failures

**Interfaces:** Produces `NativeCapacityExceededException extends OffHeapOutOfMemoryException`; no invariant exception changes parent type.

- [ ] **Step 1: Add failing hierarchy tests**

Add to `NativeAllocatorContractTest` and `YierdisNativeObjectTableTest`:

```java
@Test
public void nativeCapacityExceptionIsAnOffHeapOom() {
    Assert.assertTrue(OffHeapOutOfMemoryException.class
            .isAssignableFrom(NativeCapacityExceededException.class));
    Assert.assertFalse(NativeMemoryException.class
            .isAssignableFrom(NativeCapacityExceededException.class));
}

@Test
public void slotExhaustionUsesCapacityHierarchy() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slot-capacity");
         YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 1, 0)) {
        table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 1, 1, 0);
        Assert.assertThrows(NativeCapacityExceededException.class,
                () -> table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 2, 1, 0));
    }
}
```

- [ ] **Step 2: Run and observe the wrong slot exception**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeAllocatorContractTest,YierdisNativeObjectTableTest#slotExhaustionUsesCapacityHierarchy -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because slot exhaustion currently throws `NativeMemoryException`.

- [ ] **Step 3: Translate only expected capacity boundaries**

Add message and message/cause constructors to `NativeCapacityExceededException`. Use it for slot capacity, checked page-id exhaustion, and configured allocator limits. At `YierdisFfmMemoryRuntime.allocateRegion`, catch only native allocation `OutOfMemoryError`, wrap it with the requested region size, and rethrow unrelated `Error` values. Keep stale handles, double free, invalid pin, corrupt state, and illegal movement as `NativeMemoryException` or its existing subtype.

- [ ] **Step 4: Run all allocator tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory -am test
```

Expected: PASS.

- [ ] **Step 5: Commit the taxonomy**

```bash
git add yierdis-memory
git commit -m "fix: unify native capacity failure taxonomy"
```

Expected: PASS and the native-capacity taxonomy commit succeeds.

### Task 2: Add Deterministic Allocation Fault Injection

**Interfaces:** Produces `FailOnAllocationNativeAllocator`, which delegates all operations including growth estimates/scopes and can fail the Nth `allocate` or moving `realloc` with `NativeCapacityExceededException`.

- [ ] **Step 1: Add the failing wrapper contract test**

```java
@Test
public void failsExactlyTheConfiguredAllocationAndCanBeReset() {
    NativeAllocator delegate = new RecordingAllocator();
    FailOnAllocationNativeAllocator allocator = new FailOnAllocationNativeAllocator(delegate);
    allocator.failOnAllocation(2);
    allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
    Assert.assertThrows(NativeCapacityExceededException.class,
            () -> allocator.allocate(NativeObjectKind.STRING_BYTES, 1));
    Assert.assertEquals(2L, allocator.allocationAttempts());
    allocator.disableFailures();
    allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
}
```

- [ ] **Step 2: Run and verify the new testkit module is absent**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-testkit -am -Dtest=FailOnAllocationNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the module and class do not exist.

- [ ] **Step 3: Implement the wrapper**

Use `AtomicLong` for the attempt counter so test behavior remains deterministic even when the synchronized adapter is exercised. Count `allocate` and every `realloc` whose requested size exceeds current capacity; expose `failOnAllocation(long oneBasedIndex)`, `disableFailures()`, `resetAttempts()`, and `allocationAttempts()`. Delegate bind, resolve, pin, epochs, `estimateAdditionalGrowth`, allocation scopes, trim, statistics, and close unchanged.

- [ ] **Step 4: Run wrapper tests and architecture rules**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-testkit -am -Dtest=FailOnAllocationNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; production modules do not depend on `yierdis-memory-testkit`.

- [ ] **Step 5: Commit the testkit**

```bash
git add pom.xml yierdis-memory yierdis-tests/yierdis-architecture-tests
git commit -m "test: add deterministic native allocation failures"
```

Expected: PASS and the deterministic fault-injection commit succeeds.

### Task 3: Introduce Prepared Mutation And Ledger Reconciliation

**Interfaces:** Produces the exact `PreparedMutation`, `PreparedDbMutation`, `MutationPlan`, `NativeAllocationScope`, and `MemoryLedger.reconcile` signatures above.

- [ ] **Step 1: Replace reservation tests with red prepared-mutation cases**

Add to `MutationExecutorReservationTest`:

```java
@Test
public void preparationFailureAbortsAndRollsBackReservation() {
    AtomicBoolean aborted = new AtomicBoolean();
    PreparedDbMutation<String> prepared = prepared(7, 0, MutationOutcome.VALUE_CHANGED,
            () -> "ok", () -> {}, () -> aborted.set(true));
    YierdisDbMutationExecutor executor = executorWithLedger(10);

    YierdisCommandException failure = Assert.assertThrows(YierdisCommandException.class,
            () -> executor.execute(new MutationPlan<>() {
        public long upperBoundBytes() { return 10; }
        public PreparedDbMutation<String> prepare() {
            prepared.abort();
            throw new NativeCapacityExceededException("injected");
        }
    }));
    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, failure.getMessage());
    Assert.assertTrue(aborted.get());
    Assert.assertEquals(0L, ledger.reservedBytes());
}

@Test
public void commitRunsAfterReconcileAndCannotBeRepeated() {
    List<String> order = new ArrayList<>();
    PreparedDbMutation<String> prepared = prepared(7, 0, MutationOutcome.VALUE_CHANGED,
            () -> { order.add("commit"); return "ok"; },
            () -> order.add("release"), () -> order.add("abort"));
    Assert.assertEquals("ok", executor.execute(plan(10, prepared)));
    Assert.assertEquals(List.of("commit", "release"), order);
    Assert.assertEquals(7L, ledger.usedBytes());
    Assert.assertEquals(0L, ledger.reservedBytes());
    Assert.assertThrows(IllegalStateException.class, prepared::commit);
    prepared.releaseSuperseded();
    Assert.assertEquals(List.of("commit", "release"), order);
}

@Test
public void abortedNativeScopeRestoresCommittedSnapshot() {
    MemoryUsageSnapshot before = allocator.memoryUsage();
    try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
        allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
        allocator.allocate(NativeObjectKind.ENTRY_RECORD, 56);
        Assert.assertTrue(scope.growth().effectiveBytes() > 0);
        scope.abort();
    }
    Assert.assertEquals(before, allocator.memoryUsage());
    Assert.assertEquals(0L, allocator.stats().liveObjects());
}

@Test
public void reclamationAdmissionDoesNotReenterCleanupOrGovernor() {
    PreparedDbMutation<String> prepared = prepared(-7, 0, MutationOutcome.VALUE_CHANGED,
            () -> "removed", () -> {}, () -> {});
    MutationPlan<String> plan = new MutationPlan<>() {
        public long upperBoundBytes() { return 0; }
        public AdmissionMode admissionMode() { return AdmissionMode.RECLAMATION; }
        public PreparedDbMutation<String> prepare() { return prepared; }
    };
    Assert.assertEquals("removed", executor.execute(plan));
    Assert.assertEquals(1, ledger.reclamationBegins());
    Assert.assertEquals(0, ledger.normalReservations());
    Assert.assertEquals(0, cleanupCalls.get());
    Assert.assertEquals(0, governorCalls.get());
}
```

Add rejection cases for a reclamation plan with a positive upper bound,
positive measured native/staged growth, or positive `actualDeltaBytes()`. Each
must fail before visibility, abort its staging, and leave the ledger unchanged.

Add these exact helper signatures to the rewritten test fixture so every call
above has one unambiguous parameter order:

```java
private static <T> PreparedDbMutation<T> prepared(
        long actualDeltaBytes,
        long stagedNonNativeGrowthBytes,
        MutationOutcome outcome,
        Supplier<T> commit,
        Runnable releaseSuperseded,
        Runnable abort
);

private static <T> MutationPlan<T> plan(
        long upperBoundBytes,
        PreparedDbMutation<T> prepared
);
```

The fixture owns `MemoryLedger ledger`, `YierdisStableNativeAllocator allocator`,
and `YierdisDbMutationExecutor executor`; `@After` closes the allocator/runtime
and asserts no reservation or active allocation scope remains.

- [ ] **Step 2: Run and verify compilation fails on the new contract**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationExecutorReservationTest,NativeAllocationScopeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because prepared-mutation, native-scope, and reconciliation contracts are absent.

- [ ] **Step 3: Implement the lifecycle state machine**

`AbstractPreparedMutation` has states `PREPARED`, `COMMITTING`, `COMMITTED`, `RELEASED`, and `ABORTED`. `commit()` validates every invariant while PREPARED, transitions to COMMITTING before the subclass allocation-free visible switch, and reaches COMMITTED only after that switch returns. A throwable in COMMITTING is a DB-degrading invariant failure and can never make `abort()` legal again. `releaseSuperseded()` invokes allocation-free idempotent cleanup while COMMITTED and transitions to RELEASED only after all cleanup succeeds; it is a no-op when already RELEASED. `abort()` releases staged resources only on PREPARED and is otherwise a no-op. Each subclass clears an owned-resource marker before releasing that resource so a cleanup failure cannot cause a later retry to double-free it. `YierdisDbMutationExecutor` performs:

```java
MemoryReservation reservation = null;
NativeAllocationScope allocations = null;
PreparedDbMutation<T> prepared = null;
boolean commitStarted = false;
try {
    reservation = plan.admissionMode() == AdmissionMode.RECLAMATION
            ? ledger.beginReclamation()
            : ledger.reserve(Math.max(0L, plan.upperBoundBytes()));
    allocations = nativeAllocator.beginAllocationScope();
    prepared = Objects.requireNonNull(plan.prepare(), "prepared mutation");
    long preparedPeakBytes = MemoryUsageSnapshot.addSaturating(
            allocations.growth().effectiveBytes(),
            prepared.stagedNonNativeGrowthBytes());
    if (plan.admissionMode() == AdmissionMode.RECLAMATION) {
        requireReclamationInvariants(plan.upperBoundBytes(), preparedPeakBytes,
                prepared.actualDeltaBytes());
    } else {
        ledger.reconcile(reservation, preparedPeakBytes);
    }
    commitStarted = true;
    T result = prepared.commit();
    allocations.promote();
    ledger.commit(reservation, prepared.actualDeltaBytes());
    prepared.releaseSuperseded();
    return result;
} catch (MemoryLedgerOutOfMemoryException | NativeCapacityExceededException expected) {
    if (commitStarted) throw new IllegalStateException("capacity failure after commit started", expected);
    if (prepared != null) prepared.abort();
    if (allocations != null) allocations.abort();
    ledger.rollback(reservation);
    throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
} catch (RuntimeException | Error failure) {
    if (!commitStarted) {
        if (prepared != null) prepared.abort();
        if (allocations != null) allocations.abort();
        ledger.rollback(reservation);
    } else {
        allocations.promote();
        ledger.commit(reservation, prepared.actualDeltaBytes());
    }
    throw failure;
}
```

The allocator scope records handles, newly committed metadata segments/pages, and their allocator-owned heap structures without boxed per-allocation maps. Freeing a scoped handle removes it from the scope. Abort first frees all remaining scoped handles, then closes only provisional empty pages/segments and their provisional directories; promotion clears provisional ownership without scanning configured slot capacity. Every prepared subtype returns its actual non-allocator staged FFM region capacity plus conservative heap object/array bytes from `stagedNonNativeGrowthBytes()`. `beginReclamation()` is implemented by every ledger, never invokes its normal cleanup/eviction/coordinator callbacks, and still yields a single-use token so a negative commit is settled exactly once. Mark physical expiry deletion, eviction, DEL/UNLINK, and other statically deletion-only plans as `RECLAMATION`; mixed updates remain `NORMAL`. Ensure `ledger.commit` validates arithmetic during reconcile so its post-storage update is a non-throwing counter assignment. Task 8 adds degradation for any invariant failure once `commitStarted`; it must never roll back the committed ledger or call `abort()` after commit began.

- [ ] **Step 4: Run ledger tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MemoryLedgerContractTest,MutationExecutorReservationTest,NativeAllocationScopeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit the coordinator core**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ledger
git commit -m "refactor: add prepared database mutations"
```

Expected: PASS and the prepared-mutation/ledger commit succeeds.

### Task 4: Prepare Key, Entry, TTL, And String Changes

**Interfaces:** Produces `PreparedEntryMutation<T>` as the sole coordinator for key-directory, entry-table, TTL-index, and value-root visibility.

- [ ] **Step 1: Add string and structural fault cases**

In `MutationFaultInjectionTest`, run new-key SET, existing-key SET, SET GET, APPEND, SETBIT growth, INCRBY, TTL add/replace/remove, the 13th key that grows an initial table, and slot-capacity exhaustion. For each failed allocation assert the old string bytes, old TTL, key count, `MemoryUsageSnapshot`, ledger used/reserved bytes, and live-handle counts equal the before snapshot. Add `OwnedReplyValueTest` cases proving an applied SET GET result reads the superseded bytes after commit, the DB exposes the replacement bytes, close releases the old native handle once, double-close is harmless, and an aborted/non-applied result owns no old handle.

Use this bounded loop:

```java
for (long failAt = 1; failAt <= 128; failAt++) {
    try (FaultFixture fixture = fixture(caseUnderTest.setup())) {
        DbStateSnapshot before = fixture.snapshot();
        fixture.allocator().failOnAllocation(failAt);
        try {
            caseUnderTest.mutate(fixture.db());
            Assert.assertTrue("success must occur after every allocation point was covered",
                    failAt > fixture.allocator().allocationAttempts());
            break;
        } catch (YierdisCommandException expected) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, expected.getMessage());
            Assert.assertEquals(before, fixture.snapshot());
            Assert.assertArrayEquals(before.primaryValue(), fixture.readPrimaryValue());
        }
    }
}
```

- [ ] **Step 2: Run the SET replacement probe and observe visible-state drift**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#stringMutationFamilyIsFailureAtomic -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on at least one injected point with changed value, TTL, or handles.

- [ ] **Step 3: Implement prepared structural changes**

Add prepare/commit/release/abort operations to `EntryTable`, `NativeKeyDirectory`, and `YierdisFfmExpireIndex`. Preparation allocates replacement arrays/regions and new key/entry/value handles but does not publish them. Before executor admission, enumerate every planned native object size and exact replacement FFM/heap table growth through `MutationMemoryEstimator`; the result is the plan's `upperBoundBytes()`. `PreparedEntryMutation.commit()` publishes preallocated directory and TTL tables, swaps the entry handle/record, then marks new resources owned. `releaseSuperseded()` frees old handles without allocation only after the executor commits accounting. Abort closes replacement tables and frees only staged handles. Pure key deletion, passive/active expiry removal, and maxmemory eviction use `AdmissionMode.RECLAMATION`; their prepared form cannot allocate a replacement table or report positive persistent growth. This prevents the cleanup callback invoked by a normal reservation from recursively entering that same reservation path.

- [ ] **Step 4: Convert every string write to replacement staging**

SET, APPEND, SETBIT, and INCRBY allocate a complete replacement string during prepare even when the old capacity could be changed in place. For SET GET, preparation also allocates the `BulkStringValue` ownership wrapper and records the old handle's exact payload/retained capacity. Commit swaps the `ValueHandle` and transfers the old value to `SetStringValue`; `releaseSuperseded()` must not free that transferred handle. SET without GET releases the old handle normally. Commit never calls `realloc`, `store`, `ensureLength`, or a reply factory. Update `StringCommands` to encode an applied old value through `BulkStringValue.writeTo(new BulkStringReplyAdapter(out))` inside try-with-resources. Preserve integer/embstr/raw encoding and existing wire replies.

- [ ] **Step 5: Run string and TTL fault matrix**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-command/yierdis-command-builtin -am -Dtest=MutationFaultInjectionTest#stringMutationFamilyIsFailureAtomic,OwnedReplyValueTest,StringDirectOpsTest,OffHeapStringStorageTest,TtlLifecycleDirectOpsTest,StringCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit structural and string staging**

```bash
git add yierdis-db yierdis-command/yierdis-command-builtin
git commit -m "fix: make string and entry mutations failure atomic"
```

Expected: PASS and the failure-atomic key/string mutation commit succeeds.

### Task 5: Prepare List Mutations

**Interfaces:** `ListValue.preparePush(List<byte[]> values, boolean left)` returns `PreparedMutation<PreparedListResult>`; `preparePop(int count, boolean left)` returns `PreparedMutation<PoppedValueSequence>`. `ListWriteOps.lpop/rpop` return `WriteResult<PoppedValueSequence>`; commit only links/unlinks existing nodes, transfers the detached values to the preallocated sequence owner, and updates head, tail, and length.

- [ ] **Step 1: Add new/existing LPUSH, RPUSH, LPOP, and RPOP fault cases**

Add the cases under `MutationFaultInjectionTest#listMutationFamilyIsFailureAtomic`. The snapshot compares ordered list contents, type encoding, TTL, memory counters, and native `LIST_NODE` counts at every injected allocation. In `OwnedReplyValueTest`, pop one and many values from both ends; assert order, `count()`, null-versus-empty semantics, exact payload/retained byte counters, successful emission after the list root changed, release after close, and idempotent double-close.

- [ ] **Step 2: Run the list matrix and observe partial linking**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#listMutationFamilyIsFailureAtomic -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL when a multi-value push allocates at least one node before a later allocation fails.

- [ ] **Step 3: Implement list staging**

Enumerate every new root, listpack, node, primitive ownership array, and `PoppedValueSequence` wrapper through `MutationMemoryEstimator`, then allocate and populate every new quicklist/listpack node during prepare. Record final head, tail, length, and detached value handles in primitive arrays. Do not copy popped payloads. Commit performs only link writes, root counter updates, and an allocation-free ownership transfer into the prepared sequence. Abort leaves the visible chain unchanged and closes the prepared owner. `releaseSuperseded()` releases only structural nodes not transferred to the sequence. Update `ListCommands` to write null/single/array shapes from the sequence via `BulkStringReplyAdapter` and close it in `finally`; remove its `List<byte[]>` response helper.

- [ ] **Step 4: Run list tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-command/yierdis-command-builtin -am -Dtest=MutationFaultInjectionTest#listMutationFamilyIsFailureAtomic,OwnedReplyValueTest,CollectionDirectOpsTest,ListValueTest,ListRootTest,NativeListpackTest,ListCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit list staging**

```bash
git add yierdis-db yierdis-command/yierdis-command-builtin
git commit -m "fix: stage list mutations before linking"
```

Expected: PASS and the failure-atomic list mutation commit succeeds.

### Task 6: Prepare Hash And Set Mutations

**Interfaces:** `HashValue.prepareSet`, `HashValue.prepareDelete`, `SetValue.prepareAdd`, and `SetValue.prepareRemove` return prepared mutations. Encoding promotion builds a complete replacement representation before commit.

- [ ] **Step 1: Add HSET/HDEL/SADD/SREM fault cases**

Add the cases under `MutationFaultInjectionTest#hashAndSetMutationFamiliesAreFailureAtomic`. Cover new key, existing member replacement, multiple fields/members, listpack-to-hashtable promotion, and table growth. Compare all fields/members and `HASH_*`/`SET_*` native handle counts after each failure.

- [ ] **Step 2: Run and observe partial multi-member changes**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#hashAndSetMutationFamiliesAreFailureAtomic -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL in current `hsetMany` or multi-member set paths.

- [ ] **Step 3: Implement prepared collection changes**

Use `MutationMemoryEstimator` for every new member/value/root handle plus exact replacement-array/listpack growth. Allocate those handles and arrays before changing buckets. A same-encoding update records target bucket writes and old handles; a promotion builds the complete target map and swaps one root reference. Commit never invokes `NativeByteMap.put`, `rehash`, or `NativeListpack.addLast`; it applies precomputed bucket/root writes. Abort releases all staged handles and arrays.

- [ ] **Step 4: Run hash/set tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#hashAndSetMutationFamiliesAreFailureAtomic,HashValueTest,SetValueTest,CollectionRootTest,CollectionDirectOpsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit hash/set staging**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "fix: stage hash and set mutations"
```

Expected: PASS and the failure-atomic hash/set mutation commit succeeds.

### Task 7: Prepare ZSet And HLL Mutations

**Interfaces:** `ZSetValue.prepareAdd` chooses skiplist levels and allocates member/map/node capacity before unlinking; `YierdisHyperLogLog.prepareAdd` and `prepareMerge` build replacement register state.

- [ ] **Step 1: Add ZADD and PFADD/PFMERGE fault cases**

Add the cases under `MutationFaultInjectionTest#zsetAndHllMutationFamiliesAreFailureAtomic`. Include the reproduced existing-member ZADD score update. On every failure assert cardinality, scores, ordered members, HLL count tolerance input state, TTL, accounting, and native handles equal the pre-command snapshot.

- [ ] **Step 2: Run the reproduced ZSet probe**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#zsetAndHllMutationFamiliesAreFailureAtomic -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: before implementation, FAIL because a `NativeCapacityExceededException` can leave the ZSet size or member map changed.

- [ ] **Step 3: Implement prepared ZSet changes**

Use `MutationMemoryEstimator` for member, skiplist node, map replacement, and HLL replacement growth. Choose the random skiplist level once during prepare. Allocate the member handle, skiplist node, and any map replacement array first. Compute predecessor/update ranks without unlinking. Commit publishes prepared map capacity, unlinks the old node when present, links the prepared node, and updates span/length counters. Abort frees only the prepared member/node/array. `releaseSuperseded()` releases the old node/member only after accounting is committed.

- [ ] **Step 4: Implement HLL replacement staging**

PFADD and PFMERGE calculate registers into a prepared native representation. Commit swaps the root handle; no register buffer allocation occurs during commit. Preserve no-op `MutationOutcome.NONE` when registers do not change.

- [ ] **Step 5: Run ZSet/HLL tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest#zsetAndHllMutationFamiliesAreFailureAtomic,ZSetValueTest,YierdisHyperLogLogTest,CollectionDirectOpsTest,NativeStorageRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit ZSet/HLL staging**

```bash
git add yierdis-db/yierdis-db-memory
git commit -m "fix: stage zset and hll mutations"
```

Expected: PASS and the failure-atomic ZSet/HLL mutation commit succeeds.

### Task 8: Add DB Degraded State

**Interfaces:** `DbEngine.health()` returns `DbHealthSnapshot`; write entry points call `YierdisDbHealth.requireWritable()` before reservation; failures after visibility may have changed are rethrown as `PostCommitMutationException`.

- [ ] **Step 1: Add failing degraded-state tests**

Inject a `NativeMemoryException("corrupt metadata")` during `releaseSuperseded()` after a replacement SET has committed. Assert the first command fails through the internal-error path, `health().degraded()` is true with the first cause retained, GET returns the newly committed value, ledger usage reflects that committed value, and the next SET throws the exact MISCONF message. Separately inject an invariant throwable after `AbstractPreparedMutation` enters COMMITTING; assert `abort()` is never invoked, the native allocation scope is promoted rather than aborted, the ledger reservation is settled once, and shutdown reclaims any conservatively retained staged handles.

- [ ] **Step 2: Run and verify writes currently continue**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=DegradedDbWriteRejectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because no health state exists.

- [ ] **Step 3: Implement health transitions**

`YierdisDbHealth` is owner-thread confined and records only the first invariant failure type/message/timestamp. The mutation executor degrades on `NativeMemoryException` and internal commit/release `IllegalStateException`, but not on command validation, wrong type, OOM, or user cancellation. Pre-commit invariant failures abort staging and roll back the reservation; post-commit failures preserve the committed ledger and visible state, record degradation, never call `abort()`, and throw `PostCommitMutationException` only after conservative settlement completes. Every write and maintenance deletion calls `requireWritable`; reads do not.

- [ ] **Step 4: Run health and direct-operation tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=DegradedDbWriteRejectionTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest,StringDirectOpsTest,CollectionDirectOpsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; no expected allocation failure degrades a DB.

- [ ] **Step 5: Commit degraded-state support**

```bash
git add yierdis-db
git commit -m "feat: disable writes after database invariant failure"
```

Expected: PASS and the degraded-state commit succeeds.

### Task 9: Verify OOM Connection Recovery And The Full Matrix

**Interfaces:** Completes the Stage 2 acceptance boundary; Stage 6 will repeat the matrix with commit-sequence assertions.

- [ ] **Step 1: Add the server recovery test**

Start a server with a native slot capacity that the test can exhaust. Send a mutating command that requires another slot, assert the exact Redis OOM reply, then send GET and PING on the same socket and assert both succeed. Assert no partial key exists.

- [ ] **Step 2: Run integration recovery and all fault cases**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=NativeCapacityOomRecoveryTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MutationFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS. Every failed attempt restores used, reserved, committed, reclaimable, and live-handle counters.

- [ ] **Step 3: Run architecture and full tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS with no ignored or weakened maxmemory assertions.

- [ ] **Step 4: Commit Stage 2 acceptance**

```bash
git add yierdis-tests/yierdis-integration-tests yierdis-db/yierdis-db-memory
git commit -m "test: prove mutation failure atomicity"
```
