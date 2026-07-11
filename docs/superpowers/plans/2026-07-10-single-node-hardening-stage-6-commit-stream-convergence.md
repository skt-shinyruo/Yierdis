# Reliable Commit Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish every committed user, expiry, and eviction change through an ordered, bounded, observable in-process stream with no silent loss, then remove obsolete change-recording adapters and enforce the DB/runtime module boundary.

**Architecture:** Add an owned immutable command-record contract plus a non-retainable callback view below server, command, DB, and runtime modules. The command engine establishes the current execution record, but the DB mutation coordinator remains the only commit authority: before visibility it reserves and prefills a fixed stream slot with a candidate sequence; after storage and ledger commit it publishes that slot through an allocation-free state transition. Runtime supplies a single-producer/single-consumer instance `CommitStream`; sink failure or an ambiguous post-commit invariant retains the affected slot, fails the stream, and disables writes while reads continue.

**Tech Stack:** Java 25, Maven, JUnit 4, Stage 2 prepared mutations, Stage 5 retainable request leases, one command-owner producer, one dedicated sink worker.

## Global Constraints

- Execute only after Stages 1-5 and the full JDK 25 suite pass.
- Preserve RESP behavior and existing CLI options.
- Events are globally ordered across DBs and carry sequence, db index, kind, immutable execution record, committed memory delta, and `commitAttemptTimestampMillis`.
- Default queue limits are exactly 8192 events and 64 MiB retained bytes; embedded config may override either positive value.
- Default graceful shutdown drain timeout is exactly five seconds.
- NOOP sink creates no worker, no queue, and no write reservation.
- No-op commands and aborted mutations consume no capacity and no sequence.
- `CommitStream.publish` performs no allocation, record retention, collection growth, or capacity check after storage visibility changes.
- An unpublished reservation is never canceled after commit starts; post-commit failure converts it to a held failed slot and fails the stream without allocation.
- Queue-full or failed stream rejects writes with exactly `BUSY commit stream unavailable` before storage visibility changes.
- Sink acknowledgment occurs only after `onChange` returns successfully.
- Sink exception retains the head event, records the first failure, transitions to FAILED, and stops accepting writes.
- Reads remain available when the stream fails; writes stop.
- Expired keys are logically invisible at deadline even if physical event-backed deletion waits; eviction never deletes without an event reservation.
- The commit stream is in-process and non-durable across crashes.
- A shutdown timeout is observable, returns failure, and never closes an event that is still inside a sink callback.
- Sink callbacks receive only an owner-thread callback-scoped read view; they cannot retain or close the queued record.
- Stage 7 owns final operational documentation, soak, performance gates, and full-program acceptance after bounded reply egress exists.
- Every Maven and Java command uses the explicit JDK 25 prefix.

---

## File Structure

Create:

- `yierdis-common/yierdis-common-command/pom.xml`
- `yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/CommandRecordView.java`
- `yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/ImmutableCommandRecord.java`
- `yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/ByteArrayCommandRecord.java`
- `yierdis-common/yierdis-common-command/src/main/java/yier/bubu/redis/common/command/CommandRecordScope.java`
- `yierdis-common/yierdis-common-command/src/test/java/yier/bubu/redis/common/command/CommandRecordScopeTest.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitKind.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitEvent.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitReservation.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitPublisher.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbCommitStreamUnavailableException.java`
- `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/DbCommitPublisherTest.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/CommitStream.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/CommitStreamState.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/CommitStreamStats.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/CommitStreamTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/CommitAwareMutationFaultInjectionTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/CommitStreamIntegrationTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/CommitStreamExpirationEvictionTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/CommitStreamShutdownTest.java`

Modify:

- `pom.xml`
- `yierdis-common/pom.xml`
- `yierdis-server/yierdis-server-api/pom.xml`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRequest.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRecord.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`
- `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ExecutionRequestContractTest.java`
- `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/CoreContractSmokeTest.java`
- `yierdis-db/yierdis-db-api/pom.xml`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/RuntimeDbEngine.java`
- `yierdis-db/yierdis-db-memory/pom.xml`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/PreparedDbMutation.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeState.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java`
- `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorArchitectureTest.java`
- `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`
- `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java`
- `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java`
- `yierdis-server/yierdis-server-runtime-api/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceResources.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/ExpireIndexTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisChangeEmissionTest.java`

Delete:

- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandChangeEmitter.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandChangeObserver.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisCommandProcessorOptions.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChange.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChangeContext.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChangeKind.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbChangeListener.java`
- `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEventBridge.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RuntimeChangeSinkCommandChangeObserver.java`
- `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CollectionCommandsMutationRecordingTest.java`
- `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/KeyspaceZSetCommandsMutationRecordingTest.java`
- `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/string/StringCommandsMutationRecordingTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RuntimeChangeSinkCommandChangeObserverTest.java`

## Stable Interfaces Produced By This Stage

The new low-level view and owned record contain no storage, command, runtime,
protocol, or Netty type:

```java
public interface CommandRecordView {
    int argc();
    boolean isNull(int index);
    int len(int index);
    byte byteAt(int index, int offset);
    void copyToByteArray(int index, byte[] dst, int dstOff);
    default byte[] toByteArray(int index) {
        if (isNull(index)) return null;
        byte[] copy = new byte[len(index)];
        copyToByteArray(index, copy, 0);
        return copy;
    }
    long retainedMemoryBytes();
}

public interface ImmutableCommandRecord extends CommandRecordView, AutoCloseable {
    ImmutableCommandRecord retain();
    @Override void close();
}
```

`retainedMemoryBytes()` is stable and non-negative for the lifetime of every record. `ByteArrayCommandRecord.copyOf(byte[]... argv)` owns immutable array copies, uses saturating arithmetic for the array/reference/payload estimate, and implements `retain()` with idempotent reference-counted views. It is the common-layer record used for bounded synthetic commands.

`ExecutionRequest extends ImmutableCommandRecord` while retaining its existing scheduling method `int retainedBytes()`. It implements the distinct memory contract as follows, so Java return types and the two accounting meanings never conflict:

```java
@Override
default long retainedMemoryBytes() {
    return admittedMemoryBytes();
}

@Override
ExecutionRequest retain();
```

Stage 5 retained requests implement `retain()` by sharing immutable argv and retaining the inbound lease. Ownership never crosses the sink API. `ExecutionRecord.request()` changes to the read-only `CommandRecordView` type and `ExecutionRecord` has no `retain()`/`close()` path. Preserve the existing public `ExecutionRecord(int dbIndex, ExecutionRequest request)` constructor: it normalizes a negative DB index to zero and makes a detached defensive heap copy, so standalone replay records keep their current immutable-snapshot behavior. Add `ExecutionRecord.borrowed(int dbIndex, CommandRecordView request)` for runtime delivery; it stores the supplied non-owning view without copying or retaining. The runtime supplies that factory with a generation-checked view usable only by the sink worker while the callback is active. Byte access after return or from another thread throws `IllegalStateException`.

```java
public enum DbCommitKind { USER, EXPIRED, EVICTED }

public interface DbCommitEvent {
    long sequence();
    int dbIndex();
    DbCommitKind kind();
    CommandRecordView record();
    long committedMemoryDelta();
    long commitAttemptTimestampMillis();
    default boolean synthetic() { return kind() != DbCommitKind.USER; }
}
```

```java
public interface DbCommitReservation extends AutoCloseable {
    DbCommitReservation NOOP = new DbCommitReservation() {
        @Override public long reservedMemoryBytes() { return 0L; }
        @Override public boolean noop() { return true; }
        @Override public void close() {}
    };

    long reservedMemoryBytes();
    boolean noop();
    @Override void close();
}

public interface DbCommitPublisher {
    DbCommitPublisher NOOP = new DbCommitPublisher() {
        @Override public DbCommitReservation reserve(int dbIndex, DbCommitKind kind,
                ImmutableCommandRecord record, long committedMemoryDelta,
                long commitAttemptTimestampMillis) {
            if (dbIndex < 0) throw new IllegalArgumentException("dbIndex must be non-negative");
            java.util.Objects.requireNonNull(kind, "kind");
            java.util.Objects.requireNonNull(record, "record");
            if (record.retainedMemoryBytes() < 0) {
                throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
            }
            return DbCommitReservation.NOOP;
        }
        @Override public long publish(DbCommitReservation reservation) {
            java.util.Objects.requireNonNull(reservation, "reservation");
            return 0L;
        }
        @Override public void failAfterCommit(DbCommitReservation reservation) {
            java.util.Objects.requireNonNull(reservation, "reservation");
        }
        @Override public boolean enabled() { return false; }
        @Override public boolean available() { return true; }
    };

    DbCommitReservation reserve(
            int dbIndex,
            DbCommitKind kind,
            ImmutableCommandRecord record,
            long committedMemoryDelta,
            long commitAttemptTimestampMillis
    );

    /** Allocation-free and non-retaining for a live reservation from this publisher. */
    long publish(DbCommitReservation reservation);
    /** Allocation-free and non-throwing; keeps the reservation-owned record after commit began. */
    void failAfterCommit(DbCommitReservation reservation);
    boolean enabled();
    boolean available();
}
```

`CommitStream` allocates a fixed ring of internal slots during construction.
`reserve(...)` validates capacity and sequence exhaustion, retains the record,
and prefills the tail slot with all event fields plus the candidate sequence.
The returned reservation carries the slot index and slot generation so a stale
close cannot affect a later reuse. `publish(...)` only changes that slot from
RESERVED to QUEUED, advances `lastAssignedSequence`, and wakes the consumer; it
does not allocate, retain, grow a collection, or perform capacity arithmetic.
An internal queued slot implements `DbCommitEvent` for the worker while it is
QUEUED or IN_FLIGHT.

`failAfterCommit(...)` consumes a still-RESERVED token, clears the producer
reservation marker, keeps the slot/record charged, records the constant failure
type `DbPostCommitInvariantFailure`, transitions the stream to FAILED, and
signals shutdown. It assigns no sequence and invokes no sink. It is idempotent
for the token generation and cannot cancel a slot after visibility may have
changed.

`CommandRecordScope.open(ImmutableCommandRecord)` is owner-thread scoped and nestable. `current()` returns the current record or null. It is context only; it does not publish or infer mutation outcomes.

Stage 6 extends Stage 2's internal `MutationPlan<T>` with the event identity, while `PreparedDbMutation<T>` continues to expose only its measured delta, logical outcome, commit, and abort lifecycle:

```java
default DbCommitKind commitKind() {
    return DbCommitKind.USER;
}

default boolean requiresCommitStream() {
    return true;
}

default ImmutableCommandRecord retainCommitRecord() {
    ImmutableCommandRecord current = CommandRecordScope.current();
    if (current == null) {
        throw new DbCommitStreamUnavailableException();
    }
    return current.retain();
}
```

The executor calls `retainCommitRecord()` only after an invisible prepared mutation reports `outcome().changedAny()`, `requiresCommitStream()` is true, and `publisher.enabled()` is true. Expiry and eviction plans override the record/kind methods and return a newly owned `ByteArrayCommandRecord` containing exactly `DEL` and the key. Stage 3 representation-only rehash/compact maintenance overrides `requiresCommitStream()` to false and must report `MutationOutcome.NONE`; it remains available after sink failure because it creates no logical commit. The executor closes its owned record view after reservation or abort; the reserved ring slot owns a separate retained view until cancellation or acknowledgment. A real publisher reports `available() == false` only after failure or while draining/closed, not merely because its ring is currently full; count/byte exhaustion is reported by `reserve(...)`.

`YierdisChangeEvent` becomes an `AutoCloseable` callback-scoped compatibility projection of `DbCommitEvent`. Its constructor receives scalar fields plus a borrowed `CommandRecordView`; it does not retain the underlying record. It gains `sequence()`, `committedMemoryDelta()`, and `commitAttemptTimestampMillis()` while retaining `record()`, `kind()`, `synthetic()`, `dbIndex()`, and `request()`; `request()` returns `CommandRecordView`. `CommitStream` closes this projection after `onChange` returns or throws, invalidating the view before the ring slot can be reused. Consumers that need data after the callback must copy it during `onChange`; there is no API that can retain the inbound lease through the event.

```java
public record CommitStreamStats(
        CommitStreamState state,
        int queuedEvents,
        long queuedBytes,
        long lastAssignedSequence,
        long lastAcknowledgedSequence,
        long rejectedWrites,
        boolean shutdownTimedOut,
        String firstFailureType,
        String firstFailureMessage
) {}
```

```java
public enum CommitStreamState {
    DISABLED,
    RUNNING,
    DRAINING,
    FAILED,
    CLOSED
}
```

For capacity observability, `queuedEvents` and `queuedBytes` include the single
RESERVED producer slot as well as QUEUED/IN_FLIGHT slots. This matches the
limits enforced by `reserve(...)`; `lastAssignedSequence` still advances only
when that reservation is published.

The state machine is exact: DISABLED shutdown is an idempotent success;
RUNNING accepts reservations; shutdown changes RUNNING to DRAINING; complete
acknowledgment and worker termination change DRAINING to CLOSED; a callback,
projection, or drain-timeout failure changes RUNNING/DRAINING to FAILED.
Repeated shutdown returns the original success for DISABLED/CLOSED and false
for FAILED without erasing diagnostics.

Internal slots use exactly `EMPTY`, `RESERVED`, `QUEUED`, and `IN_FLIGHT`.
Every transition is under the stream lock. A callback-active marker and a
single terminal-cleanup owner (`NONE`, `WORKER`, or `SHUTDOWN`) prevent
shutdown and worker paths from closing the same slot twice. `SHUTDOWN` may
claim cleanup only while no callback is active and no slot is `IN_FLIGHT`;
otherwise the worker claims it after its callback `finally` invalidates the
borrowed view. An outstanding `RESERVED` slot when orderly shutdown begins is
an invariant because command-owner work must already be drained.

---

### Task 1: Add Owned Records And Borrowed Views

**Interfaces:** Produces `CommandRecordView`, `ImmutableCommandRecord`, `ByteArrayCommandRecord`, `CommandRecordScope`, detached and borrowed `ExecutionRecord` construction paths, and an `ExecutionRequest` subtype relationship.

- [ ] **Step 1: Add failing retain/scope tests**

```java
@Test
public void scopeIsNestedAndDoesNotCopyRecord() {
    TestRecord first = record("SET", "a", "1");
    TestRecord second = record("DEL", "a");
    Assert.assertNull(CommandRecordScope.current());
    try (CommandRecordScope.Scope ignored = CommandRecordScope.open(first)) {
        Assert.assertSame(first, CommandRecordScope.current());
        try (CommandRecordScope.Scope nested = CommandRecordScope.open(second)) {
            Assert.assertSame(second, CommandRecordScope.current());
        }
        Assert.assertSame(first, CommandRecordScope.current());
    }
    Assert.assertNull(CommandRecordScope.current());
}
```

Also assert `ExecutionRequest.class.getInterfaces()` includes `ImmutableCommandRecord`; `ByteArrayCommandRecord.copyOf` isolates caller mutations, saturates its retained-memory estimate, shares immutable argv across retained views, and releases each owned view idempotently; `CommandRecordView` declares no `retain`/`close`; and `ExecutionRecord.request()` is typed as `CommandRecordView`. Keep the existing `ExecutionRequestContractTest` assertions that the public constructor normalizes a negative DB index, copies argv, and remains readable after the source request closes or mutates. Add a borrowed-factory case proving it returns the exact guarded view without a copy or retain. Retained Stage 5 requests share argv while incrementing exactly one request-lease reference.

- [ ] **Step 2: Run and observe the common-command module is absent**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-common/yierdis-common-command -am -Dtest=CommandRecordScopeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Add the neutral module and adapt requests**

`yierdis-common-command` may depend only on `yierdis-common-bytes`. Implement the read-only `CommandRecordView`, owned `ByteArrayCommandRecord`, and a nestable `ThreadLocal` scope with idempotent close and owner-thread validation. Reject a negative retained-memory result at record/publisher boundaries. Make `ExecutionRequest` extend `ImmutableCommandRecord`, preserve `int retainedBytes()` for scheduling, and implement `long retainedMemoryBytes()` by delegating to `admittedMemoryBytes()`. Update `ByteArrayExecutionRequest` and `RetainedRespExecutionRequest` to retain shared immutable storage and reference-counted leases. Change `ExecutionRecord.request()` to `CommandRecordView`, preserve the public constructor's defensive-copy behavior, and add the non-copying borrowed factory used only with callback-guarded views. Only the stream ring owns a retained event record.

- [ ] **Step 4: Run common/server-api/ingress tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-common/yierdis-common-command,yierdis-server/yierdis-server-api,yierdis-networking/yierdis-networking-netty -am -Dtest=CommandRecordScopeTest,ReferenceCountedRequestMemoryLeaseTest,RespIngressAdmissionTest -Dsurefire.failIfNoSpecifiedTests=false test
git add pom.xml yierdis-common yierdis-server/yierdis-server-api yierdis-networking/yierdis-networking-netty yierdis-tests/yierdis-architecture-tests
git commit -m "feat: add retainable immutable command records"
```

Expected: PASS with owned records reference-counted and borrowed views exposing no ownership API.

### Task 2: Add DB Commit Publication Contracts

**Interfaces:** Produces the exact DB API event view, reservation, and publisher above; `yierdis-db-api` preserves its common-bytes dependency and adds only common-command/common-memory, with no server/runtime dependency.

- [ ] **Step 1: Add API lifecycle tests**

Use a package-private fake `DbCommitEvent` to assert `synthetic()` maps from non-USER kinds and the timestamp accessor is explicitly `commitAttemptTimestampMillis()`. Assert its `record()` type is `CommandRecordView`, which exposes no ownership method. Assert `DbCommitReservation.NOOP` has zero bytes and can be closed repeatedly. Assert `DbCommitPublisher.NOOP` is disabled but available, validates negative DB indexes/null kind/null record, never retains a record, returns sequence zero, and treats `failAfterCommit(NOOP)` as an idempotent allocation-free no-op.

- [ ] **Step 2: Run and verify missing contracts**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-api -am -Dtest=DbCommitPublisherTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement contracts and BUSY exception**

`DbCommitStreamUnavailableException` extends `YierdisCommandException` and always uses `BUSY commit stream unavailable`. A real reservation is single-use before commit: publish consumes it and close cancels it; once commit begins, only `publish` or `failAfterCommit` may consume it. A stale slot-generation token cannot affect a reused ring slot. The NOOP publisher never retains a record and returns sequence zero. Document `publish` and `failAfterCommit` as allocation-free/non-retaining operations for a valid live reservation; the latter keeps the ring-owned record and fails the stream rather than withdrawing an ambiguous commit.

- [ ] **Step 4: Add publisher attachment to `RuntimeDbEngine`**

Add `attachCommitPublisher(DbCommitPublisher publisher, int dbIndex)` with a NOOP default for test doubles. DB implementation stores publisher/index before thread binding and rejects rebinding after writes begin.

- [ ] **Step 5: Run DB API tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-api -am -Dtest=DbCommitPublisherTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db/yierdis-db-api
git commit -m "feat: define database commit publication contracts"
```

Expected: PASS with the NOOP publisher allocation-free and every public signature matching the stable interfaces above.

### Task 3: Implement The Bounded Ordered `CommitStream`

**Interfaces:** Runtime `CommitStream` implements `DbCommitPublisher`; the owner thread is the only producer and a dedicated worker is the only consumer.

- [ ] **Step 1: Add order, no-op, capacity, and sequence tests**

Reserve/publish events from DB 1, DB 0, and DB 1. Assert sink order and sequences `1,2,3`. Assert canceled reservation and NOOP publisher consume no sequence, and the next reservation reuses the canceled candidate. With event limit two and byte limit ten, fill each limit and assert the next reserve throws exact BUSY and increments `rejectedWrites`. Keep a canceled reservation reference until its ring slot is reused, close the stale reference again, and assert the newer reservation is unaffected. Construct a package-private test stream whose last assigned sequence is `Long.MAX_VALUE`; its next reservation must fail before publication with exact BUSY and must never wrap sequence to zero or negative.

Reserve a record and call `failAfterCommit` while its slot is still `RESERVED`.
Assert the stream becomes FAILED with first failure type exactly
`DbPostCommitInvariantFailure`, the candidate sequence remains unassigned, the
slot count/bytes and record lease remain charged, the sink is never invoked,
and a repeated call with the same token is an idempotent no-op. Terminal
shutdown cleanup must eventually close the held record exactly once; the stale
cancellation-token case above must not affect it.

Inside a sink callback, capture `event.record()` and `event.request()`. Assert
neither view exposes `retain()` or `close()`, all byte access from a second
thread fails with `IllegalStateException`, and every accessor fails after the
callback returns. Copying argv during the callback remains valid afterward.
Block a callback while shutdown times out and race worker/shutdown cleanup;
assert the `IN_FLIGHT` record is not closed while the callback is active and
every non-empty slot is closed exactly once after the worker claims cleanup.

- [ ] **Step 2: Run and verify missing stream**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime -am -Dtest=CommitStreamTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement reserved ring slots**

Allocate a fixed `CommitStreamSlot[capacity]` ring at construction. Reservation rejects a negative DB index, null kind/record, negative or unstable `record.retainedMemoryBytes()`, an individual record larger than the byte limit, count/byte exhaustion, a second outstanding producer reservation, non-RUNNING state, and sequence exhaustion. It computes the candidate with checked arithmetic, retains the record, verifies the retained view reports the same byte count, fills the current tail slot, increments reserved event/byte counters, and returns a reservation token containing stream identity, slot index, and slot generation. If any prefill/retain step fails, clear the slot and counters before rethrowing. Publish validates only the prevalidated token, changes RESERVED to QUEUED, advances tail and `lastAssignedSequence`, clears the producer-reservation marker, and signals the worker; the implementation contains no `new`, record retain, collection growth, byte arithmetic, or user callback on this path. Before commit starts, cancel/close clears a still-RESERVED slot, closes its retained record, and releases count/bytes. After commit starts the executor never closes that token: `failAfterCommit` consumes a still-RESERVED token, keeps its slot/record charged, assigns no sequence, records constant failure type `DbPostCommitInvariantFailure`, clears the producer marker, transitions to FAILED, and signals the worker/shutdown without allocating or throwing. Use a condition or semaphore for the worker; do not busy-spin.

- [ ] **Step 4: Implement acknowledgment and failure retention**

Worker peeks the head slot and, under the stream lock, marks it IN_FLIGHT and `callbackActive=true`. It creates a closeable `YierdisChangeEvent` projection whose generation-checked borrowed `CommandRecordView` validates the worker thread and active callback generation before every accessor. It calls `YierdisChangeSink.onChange`, then closes the projection in `finally` before clearing `callbackActive`. Only after a successful return while no shutdown timeout or terminal failure has been recorded does it advance head, update `lastAcknowledgedSequence`, decrement queued bytes/count, close the slot's retained record, and clear the slot for reuse. On projection allocation failure or any sink `Throwable`, keep the head lease charged, store only the exception class name and at most 512 message characters (never retain the `Throwable`), transition RUNNING/DRAINING to FAILED, signal waiters/shutdown, and stop consuming. After an ordinary callback failure returns, change the retained head from IN_FLIGHT back to QUEUED so a later shutdown thread may claim cleanup. If shutdown timed out while the callback was active, leave it IN_FLIGHT until the projection is invalidated, then have the worker claim cleanup before any shutdown thread can do so. Terminal cleanup is performed only by the thread that claims `cleanupOwner` under the lock.

- [ ] **Step 5: Run stream tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime -am -Dtest=CommitStreamTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-runtime
git commit -m "feat: add ordered bounded commit stream"
```

Expected: PASS for ordering, capacity, fail-after-commit retention, borrowed-view invalidation, and cleanup ownership races.

### Task 4: Reserve And Publish From The DB Mutation Authority

**Interfaces:** Stage 2 `MutationPlan<T>` gains `requiresCommitStream()`, `commitKind()`, and `retainCommitRecord()`; changed logical outcomes prefill event capacity before visible commit, and commit publishes allocation-free after ledger commit but before superseded resource release.

- [ ] **Step 1: Add commit-aware fault matrix tests**

Wrap every Stage 2 mutation family with a recording publisher. For each allocation, record-retain, or queue reservation failure assert state/TTL/accounting/handles unchanged, `lastAssignedSequence` unchanged, and no event queued. For success assert exactly one sequence/event when `MutationOutcome.changedAny()` and none for a no-op. Instrument the real publisher so a post-commit publish call fails the test if it allocates, retains, invokes user code, or changes byte capacity. With the default NOOP publisher, assert a direct embedded DB write does not inspect `CommandRecordScope` or allocate a synthetic record. With an enabled publisher, assert a changed USER write without a scope aborts invisibly with exact BUSY, while a scoped direct write publishes normally. Fail the stream, then assert a user no-op write returns BUSY but a pending Stage 3 representation-only rehash tick still advances within its work budget and emits no event.

Inject failures immediately before and after storage commit, native-scope
promotion, ledger commit, `publisher.publish`, and superseded release. Every
pre-commit failure must abort the prepared mutation, allocation scope, ledger
reservation, and event reservation exactly once. Once storage commit starts,
the catch path must never call `PreparedDbMutation.abort()`,
`NativeAllocationScope.abort()`, ledger rollback, or
`DbCommitReservation.close()`. If publication has not completed, it must call
`publisher.failAfterCommit(reservation)`, retain the held RESERVED slot without
assigning a sequence, and degrade the DB. If publication reached QUEUED before
a later invariant failure, the event and sequence remain authoritative while
the DB degrades. Assert record and reservation close counts across the full
matrix.

- [ ] **Step 2: Run and verify current manual path cannot satisfy sequence assertions**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=CommitAwareMutationFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Extend `MutationPlan` with event identity**

Add the exact defaults from the stable-interface section:

```java
default DbCommitKind commitKind() { return DbCommitKind.USER; }
default boolean requiresCommitStream() { return true; }
default ImmutableCommandRecord retainCommitRecord() {
    ImmutableCommandRecord current = CommandRecordScope.current();
    if (current == null) throw new DbCommitStreamUnavailableException();
    return current.retain();
}
```

The executor never invokes record retention for NOOP publication, a no-op prepared outcome, or a plan with `requiresCommitStream() == false`. User writes retain `CommandRecordScope.current()` after invisible storage preparation. Synthetic expiry/eviction plans override the record/kind methods and create a bounded two-argument `ByteArrayCommandRecord` only on the enabled, changed path. Representation-only maintenance overrides `requiresCommitStream()` to false and is rejected as an invariant if it reports a changed logical outcome. A changed USER direct write without a scope throws exact `BUSY commit stream unavailable` before storage visibility; direct writes with the default NOOP publisher retain their current behavior and have no commit-stream work.

- [ ] **Step 4: Change executor order**

The DB mutation executor performs owner/health checks; when `requiresCommitStream()` and a real publisher are both true it rejects unavailable FAILED/draining state before preparation. It reserves peak physical memory, opens the Stage 2 native allocation scope, prepares storage invisibly, and reconciles `allocations.growth().effectiveBytes() + prepared.stagedNonNativeGrowthBytes()` before any event work. It then branches on `prepared.outcome().changedAny()`. Only the changed, stream-required, enabled branch obtains one owned record view and calls `publisher.reserve(dbIndex, plan.commitKind(), record, prepared.actualDeltaBytes(), clock.millis())`; reserve prefills the candidate sequence and retains its own record view. The executor closes its owned record immediately after reserve succeeds. It sets `commitStarted = true`, commits storage, promotes the native scope through its non-throwing transition, commits the ledger, calls allocation-free `publisher.publish(reservation)`, clears the local reservation reference immediately after publication, and performs allocation-free superseded release. A user-command no-op checks FAILED/draining write-stop state but never reserves event count or bytes; representation-only maintenance bypasses the stream entirely. On expected capacity or BUSY before `commitStarted`, abort storage, the provisional native scope, event reservation, and memory reservation, then close the owned record. Once commit starts, no catch path invokes `prepared.abort()`, `allocations.abort()`, ledger rollback, or `reservation.close()`. An unexpected post-commit invariant promotes/settles only work not already completed, degrades the DB, and calls allocation-free `publisher.failAfterCommit(reservation)` when the local unpublished reservation is still present. If publication already completed, the cleared local reference prevents withdrawal and the QUEUED event remains authoritative.

- [ ] **Step 5: Run fault matrix and direct ops**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=CommitAwareMutationFaultInjectionTest,MutationFaultInjectionTest,CollectionDirectOpsTest,StringDirectOpsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit DB publication**

```bash
git add yierdis-db
git commit -m "feat: publish events from database commits"
```

Expected: PASS and the DB-authoritative publication commit succeeds.

### Task 5: Establish User Execution Records Without Inferring Changes

**Interfaces:** `DefaultYierdisEngine.execute` opens `CommandRecordScope`; `YierdisInstance.create` composes and attaches the default-limited stream before command-layer observers are removed; command core no longer checks `changedAny()` for event publication.

- [ ] **Step 1: Add command execution scope and user-stream tests**

In `DefaultYierdisEngineTest`, use a handler that reads `CommandRecordScope.current()` and assert it is the exact request during command execution and null afterward, including exception paths and queued MULTI/EXEC replay. Create the user-command cases in `CommitStreamIntegrationTest`: with the existing embedded `changeSink` option, assert SET/HSET/ZADD commits are delivered through `CommitStream` exactly once, a no-op consumes no slot/sequence, and closing a normally drained instance leaves no worker or request lease.

- [ ] **Step 2: Run and verify no execution scope exists**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-core -am -Dtest=DefaultYierdisEngineTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Open scope in engine and transaction replay**

`DefaultYierdisEngine` wraps `commandProcessor.execute` in the scope. `QueuedCommandReplayer` retains each queued request as today and opens its own scope around each EXEC command, preserving command-by-command atomicity and event order.

- [ ] **Step 4: Compose and attach the default stream before cutover**

In `YierdisInstance.create`, construct `CommitStream` from the existing `changeSink` using the exact default limits `8192`, `64 * 1024 * 1024`, and five seconds. A NOOP sink yields the DISABLED singleton path with no worker/ring. Attach the publisher and DB index to every engine before binding or accepting commands, then start the worker only after all attachments succeed. Add the stream to `YierdisInstanceResources`; normal empty/fast-sink close must terminate it without leaking a record. Task 7 replaces these constants with validated config fields and adds observability, while Task 8 hardens blocked/failed drain reporting.

- [ ] **Step 5: Remove manual mutation recording**

Delete `CommandChangeEmitter`, `CommandChangeObserver`, the observer-only `YierdisCommandProcessorOptions` type, `CommandSupport.recordMutation`, `CommandSupport.recordWriteValue`, the direct FLUSHDB recording call, and `CommandContext` mutation booleans/accessors. Simplify `YierdisFastCommandProcessor` and `ServerCommandComposition` constructors after removing the options parameter. Replace every handler's `recordWriteValue(ctx, result)` use with `result.value()` only after the Stage 6 DB publisher tests are green. Delete the three source-scanning mutation-recording tests; Task 6 deletes the runtime observer test together with the remaining DB bridge. Command handlers no longer decide event emission.

- [ ] **Step 6: Run command, composition, and user-stream tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command,yierdis-server,yierdis-tests/yierdis-integration-tests -am -Dtest=DefaultYierdisEngineTest,YierdisServerBootstrapCommandWiringTest,CommitStreamIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS, including no-op, score-update, PFMerge, and MULTI/EXEC event cases through the DB publisher.

- [ ] **Step 7: Commit command convergence and stream composition**

```bash
git add yierdis-command yierdis-server yierdis-tests/yierdis-integration-tests
git commit -m "refactor: make database commits authoritative for changes"
```

Expected: PASS and the command/stream convergence commit succeeds.

### Task 6: Route Expiry And Eviction Through The Same Stream

**Interfaces:** Synthetic deletes use `DbCommitKind.EXPIRED` or `EVICTED`, a bounded immutable `DEL key` record, and Stage 2 `AdmissionMode.RECLAMATION`.

- [ ] **Step 1: Add expiry/eviction pressure tests**

Assert passive and active expiry produce one EXPIRED event, eviction produces one EVICTED event, synthetic records contain exactly `DEL` and the key, and global ordering interleaves user/expiry/eviction commits. Fill or fail the stream, let a key deadline pass, and assert GET still returns nil without BUSY while physical key count may remain until capacity returns. Fill the stream before eviction and assert the incoming write returns exactly `BUSY commit stream unavailable` atomically rather than deleting without an event or translating stream pressure to OOM. Instrument ledger cleanup, eviction, and global-governor callbacks while expiration and eviction run from inside a normal maxmemory admission attempt; assert each physical delete uses one zero-byte reclamation token, commits a non-positive delta, and never recursively invokes those callbacks.

- [ ] **Step 2: Run and observe current best-effort `DbChangeContext` behavior**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=CommitStreamExpirationEvictionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on queue pressure or sink failure.

- [ ] **Step 3: Implement logical expiry and deferred physical deletion**

All reads continue checking deadline and return absent. Cleanup and eviction plans declare `AdmissionMode.RECLAMATION`, obtain `MemoryLedger.beginReclamation()`, and must report zero upper-bound/native/staged persistent growth plus a non-positive actual delta. That admission path never invokes cleanup, eviction, or the global governor. Cleanup prepares `PreparedEntryMutation` deletion invisibly, then retains the synthetic record and reserves its event before commit. If event capacity is unavailable, abort the prepared deletion and reclamation token, increment `expiredEntriesAwaitingPhysicalDeletion`, and leave the physical entry unchanged for a future tick. Eviction likewise reserves event capacity before committing a victim deletion; reservation failure leaves storage and ledger unchanged.

- [ ] **Step 4: Remove legacy DB change delivery**

Delete `DbChange*`, `DbChangeContext`, bridge, runtime observer, and associated wiring. `YierdisInstanceRuntimeAccess.maintenanceTick()` invokes engines directly; DB publisher handles events. Update prior event tests to assert stream delivery and acknowledgment.

- [ ] **Step 5: Run expiry/eviction tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=MutationExecutorReservationTest,CommitAwareMutationFaultInjectionTest,CommitStreamIntegrationTest,CommitStreamExpirationEvictionTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db yierdis-server yierdis-tests/yierdis-integration-tests
git commit -m "fix: publish expiry and eviction commits reliably"
```

Expected: PASS; reclamation admission does not recurse and no physical deletion occurs without an event reservation.

### Task 7: Compose Configuration, Failure State, And Metrics

**Interfaces:** `YierdisInstanceConfig` adds embedded-only `commitStreamMaxEvents`, `commitStreamMaxRetainedBytes`, and `commitStreamShutdownTimeoutMillis`; `YierdisInstanceObservability` exposes `CommitStreamStats`.

- [ ] **Step 1: Add default/validation tests**

Add the config cases to `YierdisChangeSinkTest` and the runtime cases to `YierdisInstanceTest`. Assert defaults `8192`, `64 * 1024 * 1024`, and `5000 ms`; exercise every builder getter/copy path and reject non-positive overrides. Assert NOOP sink creates `CommitStreamState.DISABLED`, no worker thread, zero queue capacity reservation, and no DB write overhead.

- [ ] **Step 2: Run and verify fields are absent**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server -am -Dtest=YierdisChangeSinkTest,CommitStreamTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Replace composition constants with config and expose state**

Replace Task 5's default literals in `YierdisInstance.create` with the validated config getters, preserving construction-before-attachment and start-after-all-attachments order. A failed stream makes `DbCommitPublisher.available()` false, so all later write preparations return BUSY; reads bypass it. Expose stream state, reserved-or-queued count/bytes, sequences, rejects, shutdown-timeout flag, and sanitized first failure in STATS/INFO and embedded observability.

- [ ] **Step 4: Adapt `YierdisChangeEvent` compatibility**

Project `DbCommitEvent` through `ExecutionRecord.borrowed(...)` to the exact closeable `YierdisChangeEvent`/`ExecutionRecord` contracts above without copying argv. Keep current kind/synthetic semantics and add `sequence()`, `committedMemoryDelta()`, and `commitAttemptTimestampMillis()`. Preserve standalone public event constructors by routing them through the detached `ExecutionRecord` path. Update sinks and tests that currently retain stream-created `YierdisChangeEvent` objects so they copy the fields they need inside `onChange`; callback-scoped request views are closed by the stream and must not be read afterward.

- [ ] **Step 5: Run runtime config/observability tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server -am -Dtest=YierdisChangeSinkTest,CommitStreamTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server
git commit -m "feat: configure and observe the commit stream"
```

Expected: PASS with exact defaults, DISABLED behavior, callback-scoped events, and complete stream metrics.

### Task 8: Implement Graceful Drain And Failure-Reporting Shutdown

**Interfaces:** `CommitStream.shutdownGracefully(Duration)` returns true only when every published event is acknowledged before timeout, the stream is not failed, and the worker terminates.

- [ ] **Step 1: Add shutdown tests**

Cover DISABLED, empty RUNNING queue, delayed successful sink, an interruptible sink blocked beyond timeout, a sink that temporarily ignores interruption, failed sink, an outstanding RESERVED producer slot, and idempotent second close. Assert shutdown first rejects new reservations, drains acknowledged events within five seconds by default, joins the worker on successful paths, closes acknowledged event leases, and reports false for timed-out/failed streams while retaining failure diagnostics and `shutdownTimedOut=true` where applicable. An outstanding RESERVED slot after command-owner drain fails shutdown with first failure type `CommitStreamOutstandingReservation`; it is not silently treated as ordinary queued work. After a failed callback has returned, the shutdown thread claims terminal cleanup and closes every queued/held event without advancing acknowledgment. While a callback is still executing, shutdown cannot claim cleanup and neither it nor another thread closes the `IN_FLIGHT` head event. Retain a Stage 5 request in the head event and assert its detached inbound lease stays charged through timeout, then reaches zero when the callback exits and worker-owned terminal cleanup runs without retaining the closed channel. Count closes for every slot and assert worker/shutdown races never double-close a record.

- [ ] **Step 2: Run and observe ordinary resource close cannot express delivery failure**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=CommitStreamShutdownTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Order instance/server shutdown**

Stop transport acceptance, mark the inbound budget closed to new reservations and cancel partial-request waiters, drain command-owner work, stop DB maintenance, transition the stream RUNNING -> DRAINING, and await acknowledgment. Under the stream lock, represent terminal cleanup ownership as exactly `NONE`, `WORKER`, or `SHUTDOWN`. The shutdown thread may CAS/claim `SHUTDOWN` only when `callbackActive == false` and no slot is `IN_FLIGHT`; otherwise it leaves ownership as NONE so the worker claims `WORKER` after its callback `finally` first closes and invalidates the borrowed projection. Only the cleanup owner changes remaining `RESERVED`/`QUEUED`/`IN_FLIGHT` slots to EMPTY, decrements counters, and closes their records, with slot generation preventing later reservation-token close from touching reused storage.

If every slot is acknowledged before the deadline, the worker terminates and state becomes CLOSED; only then close DB/native resources and assert inbound reservations are zero. The worker is a named daemon because Java cannot safely kill arbitrary user callback code. At timeout, atomically record `shutdownTimedOut`, first failure type `CommitStreamDrainTimeout`, and FAILED state, interrupt the worker, and return/report drain failure. If it remains inside `onChange`, leave the IN_FLIGHT head lease exclusively worker-owned and allow the already-closed inbound budget to receive its eventual cross-thread release. When the callback exits, the worker claims cleanup and closes that head plus all remaining unacknowledged slots without advancing `lastAcknowledgedSequence`. Failed callbacks that have already returned allow the shutdown thread to claim cleanup immediately. DB/native resources may close after the owner thread is drained because event records own only immutable request bytes, but the lightweight inbound budget object must remain reachable from outstanding leases until they release.

Zero inbound reservations is an immediate invariant only on the successful drain path. A failed or timed-out path reports the stream failure and may retain bytes that are provably owned by held event slots or an active callback; it must not misreport those bytes as an inbound leak. Their eventual cleanup must still converge the budget to zero. Aggregate independent DB/resource, genuine inbound-leak, and stream-drain failures with suppressed exceptions. Do not claim success when slots remain, the worker is alive, state is FAILED, or inbound reservations remain after an otherwise successful drain.

- [ ] **Step 4: Run shutdown and bootstrap close tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server,yierdis-tests/yierdis-integration-tests -am -Dtest=CommitStreamShutdownTest,YierdisServerBootstrapCloseTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit shutdown behavior**

```bash
git add yierdis-server yierdis-tests/yierdis-integration-tests
git commit -m "fix: drain commit events during graceful shutdown"
```

Expected: PASS and the graceful commit-stream shutdown commit succeeds.

### Task 9: Remove The DB-To-Runtime Dependency And Strengthen Guards

**Interfaces:** `yierdis-db-memory` production dependencies are exactly DB API, common bytes/memory/command, memory API, and memory FFM; runtime API is absent.

- [ ] **Step 1: Tighten the architecture test first**

Change the storage-memory policy and `ArchitectureBoundaryTest` to assert:

```java
Assert.assertFalse(pomHasProductionDependency(storageMemoryPom, "yierdis-server-runtime-api"));
```

Scan DB-memory main sources for imports/references to command, protocol, executor, Netty, server runtime API, and server runtime implementation. Add guards that manual change types, `recordMutation`, `changedAny` emission, and `DbChangeContext` do not exist in production.

- [ ] **Step 2: Run and verify the current unused dependency fails**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: before POM cleanup, FAIL because DB-memory declares `yierdis-server-runtime-api`.

- [ ] **Step 3: Remove dependency and stale adapters**

Delete the dependency from DB-memory POM and allowed policy. Remove every temporary Stage 1-5 compatibility adapter whose final consumer has migrated, including `usedBytesForMaxmemory` callbacks if no consumer remains and old RESP command adapters already replaced in Stage 5.

- [ ] **Step 4: Run all architecture tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
```

Expected: all existing rules plus the new DB/runtime rule PASS together.

- [ ] **Step 5: Commit boundary convergence**

```bash
git add yierdis-db/yierdis-db-memory/pom.xml yierdis-tests/yierdis-architecture-tests yierdis-db yierdis-command yierdis-networking
git commit -m "refactor: enforce database runtime independence"
```

Expected: PASS and the DB/runtime boundary commit succeeds.

## Stage Exit

Run the focused commit-stream, shutdown, and architecture commands from Tasks 1-9, then run the full JDK 25 Maven suite. Stage 6 is complete only when commit-stream slots and retained request leases converge under both successful and failed shutdown paths and `yierdis-db-memory` has no runtime dependency. Do not run or claim the final program soak, performance, documentation, or acceptance gates here; Stage 7 owns them after bounded ordered reply egress is implemented.
