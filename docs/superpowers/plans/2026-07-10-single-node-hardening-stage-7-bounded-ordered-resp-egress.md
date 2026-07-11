# Bounded Ordered RESP Egress And Final Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve per-connection reply order across commands, rejection, and protocol failures while bounding every reply slot, source, and `ByteBuf` allocation globally, per connection, and per top-level reply, then complete documentation, soak, performance, and full-program acceptance.

**Architecture:** Register one connection-local reply slot before the decoder emits each complete request or terminal protocol error. The command owner performs side-effect-free reply preflight, reserves the whole reply charge, synchronously encodes fixed-size chunks, and hands READY slots to an event-loop-confined sequencer; Netty write futures own chunk leases until completion. FAIR scheduling defers only the blocked connection, GLOBAL preserves FIFO by pausing at its head. DB modules expose closeable/replayable value sources and never depend on RESP or Netty.

**Tech Stack:** Java 25, Maven, JUnit 4, Netty 4.1, Stage 2 owned mutation results and post-commit marker, Stage 3 replayable scan windows, Stage 5 inbound handoff gate, Stage 6 commit stream, existing benchmark suite.

## Global Constraints

- Execute only after Stages 1-6 and their full JDK 25 suites pass.
- Defaults are exactly: global reply capacity 256 MiB, per-connection capacity 128 MiB, single-reply total charge 64 MiB, chunk payload 64 KiB, per-slot control reservation 4 KiB, reply drain timeout 5000 ms.
- Require `replyControlReservationBytes <= replyMaxTotalBytes <= replyPerConnectionCapacityBytes <= replyGlobalCapacityBytes`; all values and `replyChunkPayloadBytes`/`replyDrainTimeoutMillis` are positive.
- The single-reply limit includes slot/source estimates, retained transferred values, encoded bytes, actual `ByteBuf.capacity()`, and fixed queue/promise/listener/component overhead.
- Reserve global, connection, and single-reply capacity before allocating slot state, source wrappers, buffers, promises, listeners, or queue nodes.
- Register a slot in receive order before executor submission; command, BUSY, protocol-error, internal-error, and close-after-reply output share one sequencer.
- No production handler calls `Channel.write`, `writeAndFlush`, or `channel.alloc().buffer()` outside the sequencer/chunk sink.
- `clientOutputBufferLimitBytes` and `clientOutputBufferOverLimitMillis` remain soft Netty writability/slow-client controls and do not replace hard reply admission.
- The command-owner thread never waits or spins for reply capacity. FAIR rotates other connections; GLOBAL pauses at the blocked FIFO head.
- `inputPausedByReply` is independent from ingress, executor backlog, Netty writability, and closing pause reasons; input resumes only when every reason clears.
- A write or EXEC that may have committed never receives replacement BUSY/OOM/internal/limit output after egress failure; close the connection and treat the result as unknown.
- An oversized reply closes without a replacement error. If exact preflight detects it before mutation, the mutation does not execute.
- Production command APIs do not create unbounded `List<byte[]>`, detached HGET/SET GET/pop arrays, or a single growable reply `ByteBuf`.
- Every reply source, slot cleanup, buffer lease, and request view closes idempotently and exactly once under success, failure, disconnect, and shutdown races.
- GET, SET, HSET, and ZADD median throughput must each remain at least 90% of the immutable pre-change baseline.
- Every Maven, Java, smoke, soak, and benchmark command uses `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`.

---

## File Structure

Create:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyPlan.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyPlans.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyReservationSink.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyCapacityUnavailableException.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyTooLargeException.java`
- `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ReplyPlansTest.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionReply.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionAttempt.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudget.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudgetStats.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundConnectionMemory.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryLease.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlot.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlotState.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplyCleanupOwner.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplySequencer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RegisteredRespMessage.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGate.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/BoundedChunkedReplySink.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ChildChannelRegistry.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OutboundMemoryBudgetTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionReplySequencerTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/BoundedChunkedReplySinkTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyPipelineTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ReplyShutdownTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyServerInfoProviderTest.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/MeasuredBulkStringSequence.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapMetrics.java`
- `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/MeasuredReplySourceTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/OrderedReplyIntegrationTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/OutboundReplyPressureTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/ReplyResultUnknownTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/ProductionHardeningSoakTest.java`
- `scripts/production-hardening-soak.sh`
- `docs/project-docs/production-hardening-operations.md`

Modify:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplyWriterTest.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorTaskQueue.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorKeyState.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionIoAdapter.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorFairSchedulingTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCloseTest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/HashReadOps.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ListReadOps.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringValue.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSequence.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/PoppedValueSequence.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/KeyScanWindow.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdPolicy.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdEvaluator.java`
- `README.md`
- `docs/project-docs/native-allocator-and-handles.md`
- `docs/project-docs/native-memory-runtime.md`
- `docs/project-docs/db-internals.md`
- `docs/project-docs/maxmemory-and-eviction.md`
- `docs/project-docs/executor-and-backpressure.md`
- `docs/project-docs/protocol-reference.md`
- `docs/project-docs/change-event-and-proxy-logic.md`
- `docs/project-docs/configuration-and-operations.md`
- `docs/project-docs/testing-and-debugging.md`
- `docs/project-docs/core-logic-index.md`

Delete after all replacement tests pass:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyReplyFlushBatch.java`
- Production method `RedisReplyWriter.bulkStringArray(List<byte[]>)` and all production/test overrides of that signature.

## Stable Interfaces Produced By This Stage

Commands request capacity through protocol-independent byte totals; RESP framing helpers produce those totals before mutation:

```java
public record ReplyPlan(
        long encodedUpperBoundBytes,
        long retainedSourceBytes,
        boolean reserveMaximum
) {
    public static ReplyPlan exact(long encodedUpperBoundBytes, long retainedSourceBytes);
    public static ReplyPlan maximum();
}

public final class ReplyPlans {
    public static ReplyPlan bulkString(int payloadLength, long retainedSourceBytes);
    public static ReplyPlan bulkStringArray(
            int count,
            long encodedElementBytes,
            long retainedSourceBytes
    );
    public static ReplyPlan raw(long encodedUpperBoundBytes, long retainedSourceBytes);
}

public interface ReplyReservationSink extends BytesSink {
    void require(ReplyPlan plan)
            throws ReplyCapacityUnavailableException, ReplyTooLargeException;
    long writtenBytes();
}
```

`ReplyPlans` uses saturating arithmetic. `bulkString(-1, 0)` plans the protocol null value; non-null payloads include `$`, decimal length, both CRLF pairs, payload, and retained-source bytes. `bulkStringArray` adds the exact outer array header to the already-complete encoded element total. `ReplyPlan.maximum()` requests the remaining configured single-reply capacity and is used for EXEC and result shapes that cannot be safely sized in O(1) before mutation.

`RedisReplyWriter` adds `default void requireReply(ReplyPlan plan) {}` for detached tests. `RespReplyWriter` overrides it and delegates only to a `ReplyReservationSink`; a production writer created with any other sink is an invariant failure. Its ordinary scalar/error methods fit the pre-reserved control allowance. Error normalization remains capped at 512 UTF-8 bytes.

Executor tasks carry a transport-neutral reply owner:

```java
public interface ExecutionReply extends AutoCloseable {
    BytesSink sink();
    void markReady(boolean closeAfterReply);
    void cancel();
    boolean hasWrittenBytes();
    @Override void close();
}

public enum ExecutionAttempt {
    COMPLETED,
    REPLY_CAPACITY_BLOCKED,
    CONNECTION_CLOSED
}
```

`CommandExecutor.trySubmit(connection, request, reply)` transfers both request and reply ownership only on acceptance. Rejection leaves both with the caller. `ReplyCapacityUnavailableException` is caught only when `reply.hasWrittenBytes()` is false; the task remains queued and is retried. `ReplyTooLargeException`, `PostCommitMutationException`, or any capacity exception after bytes/mutation may have become visible cancels the slot and closes the connection.

The Netty implementation has states `REGISTERED`, `WAITING_CAPACITY`, `PRODUCING`, `READY`, `WRITING`, `COMPLETED`, `CANCELLED`, and `FAILED`. Exactly one terminal cleanup owner is selected from `SEQUENCER`, `FINAL_WRITE_FUTURE`, `CONNECTION_CLOSE`, or `SHUTDOWN`. The sequencer and ordered queue are event-loop confined; global/connection counters and cleanup claims are thread-safe.

Measured DB sources use:

```java
public interface MeasuredBulkStringSequence extends BulkStringSequence, AutoCloseable {
    long encodedElementBytes();
    long retainedMemoryBytes();
    @Override void close();
}

public record BulkStringMapMetrics(
        int pairCount,
        long encodedElementBytes,
        long retainedMemoryBytes
) {}
```

Stage 2 `PoppedValueSequence` and Stage 3 `KeyScanWindow` extend `MeasuredBulkStringSequence`. Existing LRANGE, SMEMBERS, ZRANGE, and HGETALL visitor results expose the same exact metrics without materializing elements.

---

### Task 1: Add Reply Configuration And Atomic Capacity Accounting

**Interfaces:** Produces the six exact config fields, `OutboundMemoryBudget`, per-connection accounts, idempotent leases, and complete budget stats.

- [ ] **Step 1: Write failing config and three-level budget tests**

In `YierdisServerArgsTest`, round-trip all six CLI options and assert defaults `268435456`, `134217728`, `67108864`, `65536`, `4096`, and `5000`. Reject zero/negative values, control greater than single, single greater than connection, connection greater than global, and chunk/control plus fixed overhead greater than single.

In `OutboundMemoryBudgetTest`, create a 1024-byte global budget, 600-byte connection cap, and 400-byte single cap. Reserve two 200-byte slots on one connection, reject its next 300-byte connection projection, admit 300 bytes on a second connection, reject global overflow, and prove release restores capacity exactly once. Cover `Long.MAX_VALUE` additions, negative inputs, close-with-active-leases, canceled waiters, and peak/reserved/allocated separation.

```java
@Test
public void enforcesGlobalConnectionAndSingleLimitsBeforeAllocation() {
    OutboundMemoryBudget budget = new OutboundMemoryBudget(1024);
    OutboundConnectionMemory a = budget.openConnection(600);
    OutboundConnectionMemory b = budget.openConnection(600);
    OutboundMemoryLease a1 = a.reserve(200, 400).orElseThrow();
    OutboundMemoryLease a2 = a.reserve(200, 400).orElseThrow();
    Assert.assertTrue(a.reserve(300, 400).isEmpty());
    OutboundMemoryLease b1 = b.reserve(300, 400).orElseThrow();
    Assert.assertTrue(b.reserve(200, 400).isEmpty());
    a1.close();
    a1.close();
    Assert.assertEquals(500, budget.stats().reservedBytes());
    a2.close();
    b1.close();
}
```

- [ ] **Step 2: Run the red tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,OutboundMemoryBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because reply options and outbound budget types do not exist.

- [ ] **Step 3: Implement config validation and budget/lease state**

Add constants, Picocli fields, copy/toArgv/runtime-record fields, exact defaults, and validation in the same style as ingress options. `OutboundMemoryBudget` owns one lock, global counters, FIFO capacity waiters, and connection accounts. A connection account owns reserved/allocated counters and active-slot count but no `Channel`. Reserve calculates projected single, connection, and global totals with saturation before constructing a lease. Lease conversion from unallocated reservation to allocated chunk capacity changes gauges but not total admitted bytes. Closing the budget rejects new reservations/cancels waiters while existing leases remain valid until their final close.

- [ ] **Step 4: Run focused tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,OutboundMemoryBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-main
git commit -m "feat: add bounded reply capacity accounting"
```

Expected: PASS with no negative/saturated counter admission and exact defaults.

### Task 2: Register Ordered Reply Slots Before Decoder Handoff

**Interfaces:** Produces `ReplySlot`, `ConnectionReplySequencer`, `RegisteredRespMessage`, and the Stage 5 gate implementation; every request/error receives one sequence before downstream emission.

- [ ] **Step 1: Add red slot ordering and gate tests**

Use an `EmbeddedChannel` and a deterministic 4 KiB control reservation. Feed two valid requests followed by a terminal protocol error. Assert wrappers have sequences 0, 1, 2; make slot 1 READY first and assert no outbound write; make slot 0 READY and assert 0 then 1 are submitted; make slot 2 close-after-reply and assert later registration is rejected. Exhaust global control capacity and assert Stage 5 decoder remains `WAITING_FOR_HANDOFF`, retains one inbound lease, and emits no later pipelined request until a release callback runs.

Add race tests where producer handoff, channel close, and shutdown all call terminal cleanup. Count request/source/slot closes and assert exactly one `cleanupOwner` wins while every submitted chunk remains write-future-owned.

- [ ] **Step 2: Run and observe unordered direct paths**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=ConnectionReplySequencerTest,OrderedReplyPipelineTest,RespDecodedMessageGateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because decoded messages carry no slot and BUSY/protocol paths can write directly.

- [ ] **Step 3: Implement event-loop-confined sequencing and gate wrapping**

At channel initialization create one `OutboundConnectionMemory` and `ConnectionReplySequencer`. `NettyReplyDecodedMessageGate.tryAdmit` reserves the 4 KiB control charge before allocating a slot/wrapper. ADMITTED returns `RegisteredRespMessage(payload, slot)`; WAITING registers one budget callback and leaves the original message in Stage 5's decoder; CLOSED closes an execution request. Slot registration appends monotonically and detects sequence overflow before allocation. Only the event loop advances the ordered queue or invokes `Channel.write`.

Implement exact state transitions and the atomic cleanup-owner claim. A close-after-reply READY slot cancels later slots, disables all input, and attaches channel close to its final promise. Unattributed pipeline failure may append one terminal internal-error slot only when control reservation succeeds; otherwise close without a reply.

- [ ] **Step 4: Run slot/gate tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am -Dtest=RespDecodedMessageGateTest,ConnectionReplySequencerTest,OrderedReplyPipelineTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-networking/yierdis-networking-netty yierdis-server/yierdis-server-main
git commit -m "feat: register ordered reply slots at RESP handoff"
```

Expected: PASS; no later wrapper is emitted while the head waits for control capacity.

### Task 3: Encode Replies Into Pre-Reserved Fixed Chunks

**Interfaces:** Produces `ReplyPlan`, `ReplyPlans`, `ReplyReservationSink`, and `BoundedChunkedReplySink`; removes growable pending reply buffers.

- [ ] **Step 1: Add red plan arithmetic and chunk allocation tests**

Test null, empty, 9/10/99/100-byte bulk framing boundaries, array header boundaries, retained-source addition, and saturated totals in `ReplyPlansTest`. In `BoundedChunkedReplySinkTest`, reserve an exact 150 KiB encoded reply with 64 KiB chunks; assert three fixed-max-capacity buffers, no composite, no capacity growth, and total actual `ByteBuf.capacity()` plus fixed component charges never exceeds the slot reservation. Make the allocator return a capacity larger than converted credit and assert the buffer is released, slot fails, and no retroactive reserve occurs.

```java
@Test
public void exactReservationConvertsToFixedChunkLeases() {
    ReplySlot slot = fixture.slotWithReservation(200 * 1024);
    BoundedChunkedReplySink sink = fixture.sink(slot, 64 * 1024);
    sink.require(ReplyPlan.exact(150 * 1024, 0));
    sink.writeBytes(new byte[150 * 1024], 0, 150 * 1024);
    sink.finish();
    Assert.assertEquals(List.of(65536, 65536, 22528), fixture.capacities());
    Assert.assertTrue(fixture.totalAdmittedCharge() <= 200 * 1024);
}
```

- [ ] **Step 2: Run and observe the growable `ByteBuf` path**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-main -am -Dtest=ReplyPlansTest,RespReplyWriterTest,BoundedChunkedReplySinkTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `NettyExecutionIoAdapter.newReplySink()` allocates one unbounded buffer and reply planning is absent.

- [ ] **Step 3: Implement exact reservation conversion and writer delegation**

Implement the stable interfaces verbatim. A slot begins with control credit. `require` computes total charge using configured chunk size and fixed per-chunk/source components, checks single/connection/global projections, then expands the slot reservation or throws `ReplyCapacityUnavailableException` without writing. `ReplyTooLargeException` is used only when the total can never fit the single limit. Allocation converts one fixed chunk charge before calling `alloc.buffer(initialCapacity, maxCapacity)` with both capacities equal to the selected payload capacity. Record actual capacity, return only conservative surplus, and forbid composite buffers or `ensureWritable` growth.

`finish` marks the final chunk, returns unconverted credit, and publishes READY. Each chunk gets a real promise; its listener releases only that chunk lease. Remove `PENDING_REPLY_BUFFER`, void promises, and flush batching. `RespReplyWriter.requireReply` delegates to the reservation sink; production construction rejects an ordinary `NettyByteBufSink`.

- [ ] **Step 4: Run writer/chunk tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-main -am -Dtest=ReplyPlansTest,RespReplyWriterTest,BoundedChunkedReplySinkTest,ConnectionReplySequencerTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-api yierdis-networking/yierdis-networking-resp yierdis-server/yierdis-server-main
git commit -m "feat: encode replies into bounded fixed chunks"
```

Expected: PASS with every allocation preceded by an equal-or-larger converted lease.

### Task 4: Defer Reply-Blocked Tasks Without Blocking The Owner

**Interfaces:** Executor tasks own `ExecutionReply`; `CommandExecutorExecutionSupport.execute` returns `ExecutionAttempt`; FAIR keeps one blocked head per connection and GLOBAL keeps one blocked FIFO head.

- [ ] **Step 1: Add FAIR/GLOBAL red scheduling tests**

For FAIR, queue A1/A2 and B1/B2, make A1 preflight throw `ReplyCapacityUnavailableException`, and assert B1/B2 execute while A1 remains before A2. Release capacity and assert execution order B1, B2, A1, A2 with each request/reply closed once. For GLOBAL, queue A1 then B1, block A1, and assert B1 does not execute until A1 wakes. Run 10,000 wakeup attempts and assert one scheduled drain and no spin.

Add closing tests: disconnect a blocked connection, assert its task/request/slot release and FAIR continues; stop executor with a GLOBAL blocked head and assert drain-leftover closes it.

- [ ] **Step 2: Run and observe tasks cannot be put back at the head**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor -am -Dtest=CommandExecutorFairSchedulingTest,CommandExecutorTest,CommandExecutorBackpressureTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `poll()` removes a task permanently and execution has no reply-blocked outcome.

- [ ] **Step 3: Add blocked-head state and independent pause reasons**

Store FAIR `blockedHead` in each `ExecutorKeyState`; `pollFairTask` skips that key until its one-shot reply wakeup clears the field, while preserving A1 before A2. Store GLOBAL blocked head in `ExecutorTaskQueue`, and return no later task. `ReplyCapacityUnavailableException` is legal only before sink bytes and mutation visibility; execution support leaves request/reply open, records `inputPausedByReply`, and returns `REPLY_CAPACITY_BLOCKED`. Capacity release schedules one owner drain. Extend connection pause state so ingress, executor, reply, writability, and closing flags compose; no component directly enables auto-read without checking all flags.

- [ ] **Step 4: Run scheduling tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=CommandExecutorFairSchedulingTest,CommandExecutorTest,CommandExecutorBackpressureTest,NettyExecutionAdapterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-executor yierdis-server/yierdis-server-main
git commit -m "fix: rotate connections waiting for reply capacity"
```

Expected: PASS; FAIR makes progress, GLOBAL preserves FIFO, and neither path spins.

### Task 5: Route Every Reply Through One Ordered Exit

**Interfaces:** `RegisteredRespMessage` transfers its slot to exactly one command, rejection, protocol-error, or internal-error producer; only `ConnectionReplySequencer` submits outbound writes.

- [ ] **Step 1: Add red mixed-origin ordering tests**

In `OrderedReplyPipelineTest`, register five messages in receive order and complete them out of order as command success, executor BUSY rejection, terminal protocol error, internal execution failure, and close-after-reply. Assert wire order remains 0 through 4, the close occurs only after slot 4's final write future, and no producer calls the channel directly. Inject a write failure at slot 1 and assert slots 1-4 are canceled, every source/chunk/slot closes once, and no replacement reply is appended.

In `ReplyResultUnknownTest`, inject `PostCommitMutationException` after a SET and after the first mutating command in EXEC. Assert the connection closes without BUSY/OOM/internal output, the client-visible result is classified as unknown, and the committed value/event is not rolled back or duplicated.

- [ ] **Step 2: Run and observe the direct BUSY/protocol/error paths**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=OrderedReplyPipelineTest,ReplyResultUnknownTest,CommandExecutorBackpressureTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because BUSY and protocol errors bypass command reply ordering and execution failures can synthesize replacement output.

- [ ] **Step 3: Replace every direct write with slot completion**

Change `YierdisFastCommandHandler` to submit the registered request and its slot together. On executor rejection, retain ownership and encode the existing BUSY reply into that same slot. Change `RespProtocolErrorReplyHandler` to consume the registered terminal-error wrapper, encode through its slot, mark close-after-reply, and reject later input. Change `CommandExecutorExecutionSupport` so ordinary pre-execution failures finish the task's existing reply while post-commit/visibility-unknown failures cancel it and close the connection without output.

Remove direct `Channel.write`, `writeAndFlush`, and growable buffer allocation from these handlers. An unexpected exception without an attributable slot may register one terminal internal-error slot only through the Stage 5 gate and only if its control reservation succeeds; otherwise close. Preserve the existing exact RESP error strings and sanitization limits.

- [ ] **Step 4: Add architecture guards, run the green tests, and commit**

Extend `ArchitectureBoundaryTest` to scan production handlers and permit outbound `Channel.write`/`writeAndFlush` and `channel.alloc().buffer()` only in `ConnectionReplySequencer` and `BoundedChunkedReplySink`. Then run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests,yierdis-tests/yierdis-architecture-tests -am -Dtest=OrderedReplyPipelineTest,ReplyResultUnknownTest,CommandExecutorBackpressureTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-networking/yierdis-networking-netty yierdis-server yierdis-tests/yierdis-integration-tests yierdis-tests/yierdis-architecture-tests
git commit -m "fix: unify ordered reply completion paths"
```

Expected: PASS with command, rejection, protocol, and failure replies serialized by one connection-local queue and no replacement output after an ambiguous commit.

### Task 6: Transfer Owned Scalar And Mutation Reply Sources

**Interfaces:** HGET and Stage 2 mutation results return closeable `BulkStringValue`/`PoppedValueSequence`; request-backed PING/ECHO replies transfer retained views; EXEC reserves its maximum before replay.

- [ ] **Step 1: Add red ownership and preflight tests**

Add DB and command tests that close every HGET, SET GET, ECHO, and counted LPOP/RPOP result under success, reply-capacity rejection, oversized reply, disconnect, and encoder exception. Assert HGET and SET GET return closeable views without detached `byte[]`; ECHO retains the admitted request slice until its final write future; counted pop measures the exact array reply before mutation and returns values in Redis order without `List<byte[]>`.

Configure the single-reply limit one byte below each exact SET GET and counted-pop plan. Assert SET leaves the old value unchanged, pop leaves the list unchanged, no commit event is published, and no reply bytes are written. Queue a MULTI containing a write followed by a maximum-sized reply, deny `ReplyPlan.maximum()` at EXEC, and assert no queued command executes.

- [ ] **Step 2: Run and observe detached scalar/list results**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=OwnedReplyValueTest,HashValueTest,ListValueTest,StringCommandTest,ListCommandTest,OrderedReplyIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because HGET/SET GET/pop/ECHO reply ownership is detached or unmeasured and EXEC has no whole-reply reservation.

- [ ] **Step 3: Implement preflight-before-mutation and ownership transfer**

Change `HashReadOps.hget` and `YierdisHashOps` to return `BulkStringValue`; the nil singleton remains zero-charge and closeable. Make ECHO and PING-with-message transfer a retained Stage 5 request slice into the reply slot rather than copying it; ordinary PING uses the static PONG scalar. The slot's cleanup owner closes the view after final write or cancellation.

For SET GET, acquire the old `BulkStringValue`, calculate its exact RESP and retained-source charge, call `requireReply`, and only then prepare/commit the mutation. For counted LPOP/RPOP, Stage 2 discovery returns an exact `PoppedValueSequence`; reserve its complete measured array charge before commit, then transfer it to the slot. If preflight fails, close the source and abort without visibility. Once commit starts, capacity exceptions are invariant failures and use result-unknown close semantics.

Before replaying any EXEC command, require `ReplyPlan.maximum()` for the outer array and all nested output; capacity failure leaves the transaction queued state intact for normal abort cleanup and executes none of it. Replace all production uses of `bulkStringArray(List<byte[]>)` with source/visitor overloads, then delete that method and every override after compilation proves no consumer remains.

- [ ] **Step 4: Run scalar/mutation tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db,yierdis-command,yierdis-server,yierdis-tests/yierdis-integration-tests -am -Dtest=OwnedReplyValueTest,HashValueTest,ListValueTest,StringCommandTest,ListCommandTest,TransactionCommandTest,OrderedReplyIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db yierdis-command yierdis-server yierdis-networking/yierdis-networking-resp yierdis-tests/yierdis-integration-tests
git commit -m "fix: preflight owned mutation replies"
```

Expected: PASS with zero detached scalar/pop copies, no mutation before failed preflight, and every transferred source closed exactly once.

### Task 7: Stream Replayable Aggregate Replies Without Result Lists

**Interfaces:** `MeasuredBulkStringSequence` and `BulkStringMapMetrics` provide exact two-pass measurement/replay; `KeyScanWindow` replays one stable discovery window with the same expiry timestamp and generation.

- [ ] **Step 1: Add red aggregate measurement and replay tests**

In `MeasuredReplySourceTest`, cover empty, null, binary, large, and integer-boundary payloads for LRANGE, HGETALL, SMEMBERS, ZRANGE, KEYS, and SCAN. Assert discovery returns count, exact `encodedElementBytes`, retained bytes, and a replay function while allocating no result `List` or detached element arrays. Instrument storage visitation and assert both passes consume the configured work/time budget.

For KEYS/SCAN, make expiry and rehash maintenance eligible between passes. Assert discovery and replay use one captured expiry timestamp, neither pass advances rehash or physically expires entries, replay visits exactly the discovered generation, and a generation mismatch before output causes a bounded rediscovery rather than partial output. Assert an oversized aggregate is rejected before the first reply byte and releases every retained source.

- [ ] **Step 2: Run and observe aggregate materialization**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db,yierdis-tests/yierdis-integration-tests -am -Dtest=MeasuredReplySourceTest,KeyScanWindowTest,KeysBudgetTest,ListValueTest,HashValueTest,SetValueTest,ZSetValueTest,OffHeapKeysCommandSmokeTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because aggregate commands still construct result lists or cannot replay an exactly measured stable source.

- [ ] **Step 3: Implement measured discovery/replay visitors**

Make LRANGE, SMEMBERS, and ZRANGE expose `MeasuredBulkStringSequence`; make HGETALL expose `BulkStringMapMetrics` plus a replay visitor whose pair count and element bytes match the measurement. Use saturating arithmetic and reject count overflow before reservation. The command first obtains metrics, requires the exact outer-array plan, then replays directly into fixed chunks. Close the measured source in the slot cleanup path.

Use Stage 3 `KeyScanWindow` for KEYS/SCAN. Discovery captures cursor/window generation and `nowMillis`; replay validates that generation before writing and uses the same timestamp. A mismatch before any sink byte releases the plan and retries within the existing work/time budget; exhaustion requests a pessimistic maximum or fails cleanly before output. Once bytes begin, mismatch is an invariant failure and closes the connection. Do not run rehash, compaction, or physical expiry from either pass.

Delete remaining production aggregate `List<byte[]>` result paths and update `RedisReplyWriter`/`RespReplyWriter` with measured-sequence and measured-map visitor methods only.

- [ ] **Step 4: Run aggregate tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db,yierdis-networking/yierdis-networking-resp,yierdis-tests/yierdis-integration-tests -am -Dtest=MeasuredReplySourceTest,KeyScanWindowTest,KeysBudgetTest,ListValueTest,HashValueTest,SetValueTest,ZSetValueTest,OffHeapKeysCommandSmokeTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,RespReplyWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-db yierdis-command/yierdis-command-builtin yierdis-server/yierdis-server-api yierdis-networking/yierdis-networking-resp
git commit -m "refactor: stream measured aggregate replies"
```

Expected: PASS with exact reservation before replay and no unbounded aggregate result container.

### Task 8: Drain Child Channels, Reply Slots, And Leases On Shutdown

**Interfaces:** `ChildChannelRegistry` tracks every accepted child through close; bootstrap shutdown reports success only after command ownership, ordered writes, and outbound leases converge.

- [ ] **Step 1: Add red shutdown and metric tests**

In `ReplyShutdownTest` and `YierdisServerBootstrapCloseTest`, cover an idle child, READY slots, a reply blocked on capacity, a slow non-writable child, a failed write, a producer racing close, and a child accepted while server shutdown starts. Assert shutdown order is: stop parent acceptance, reject/disable child input, cancel or drain executor ownership, finish already READY ordered writes, close children, await final write futures, then close budgets/resources.

With the 5000 ms default and a short test override, assert success only when all child channels close and global/per-connection reserved, allocated, slot, source, and buffer counters reach zero. A timeout must return/report failure with live-child/slot/lease diagnostics; it must not overwrite counters with zero. Assert eventual write-future completion still releases leases after the timeout.

- [ ] **Step 2: Run and observe parent-only shutdown**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=ReplyShutdownTest,YierdisServerBootstrapCloseTest,NettyServerInfoProviderTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because accepted child channels and their reply/write-future leases are not part of one reported drain.

- [ ] **Step 3: Implement registry, shutdown order, and observability**

Register each child before command input is enabled and remove it only from its close-future listener. Closing the registry atomically prevents late registration, disables all child input, and returns a future that completes only when the registry is empty. Tell the executor to reject new work, cancel non-started slots, and drain started owners. Let the sequencer flush READY heads in order; cancel capacity-waiting or not-yet-producing slots. Close each channel after its final ordered promise or immediately when no reply remains.

Expose INFO/STATS fields for configured limits, current/peak global reserved and allocated bytes, active connections/slots/chunks/sources, capacity rejects, oversized replies, deferred FAIR/GLOBAL heads, canceled/failed slots, write failures, result-unknown closes, shutdown timeouts, and live child channels. Per-connection accounts disappear only at zero active leases. Aggregate shutdown failures with Stage 5 ingress and Stage 6 commit-stream failures using suppressed exceptions.

- [ ] **Step 4: Run shutdown/observability tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=ReplyShutdownTest,YierdisServerBootstrapCloseTest,NettyServerInfoProviderTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-main
git commit -m "fix: drain child reply ownership on shutdown"
```

Expected: PASS with truthful timeout reporting and zero outbound ownership on every successful shutdown.

### Task 9: Prove The End-To-End Failure And Ordering Matrix

**Interfaces:** Integration tests exercise the public RESP/TCP boundary and assert wire order, mutation visibility, hard limits, scheduler policy, and terminal ownership together.

- [ ] **Step 1: Add the complete red integration matrix**

In `OrderedReplyIntegrationTest`, pipeline small/large commands, BUSY rejection, protocol error, internal error, and QUIT/close-after-reply; delay completions and assert receive order exactly matches request order. In `OutboundReplyPressureTest`, independently hit the global, per-connection, and single-reply limits with fast, slow, and disconnecting clients under FAIR and GLOBAL modes. Assert counters never exceed configured hard limits, FAIR clients make progress around one blocked connection, and GLOBAL does not pass its blocked FIFO head.

In `ReplyResultUnknownTest`, inject allocator overflow, source-measure overflow, chunk allocation mismatch, write failure, disconnect during output, and post-commit failure. Assert pre-mutation exact-limit failures leave state unchanged; visibility-unknown cases close without a replacement reply; no response is reordered; and all request/source/slot/chunk leases eventually reach zero.

- [ ] **Step 2: Run the matrix before final fixes**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL until every cross-module race and fault-injection assertion uses the bounded ordered path.

- [ ] **Step 3: Fix only matrix-proven ownership and state defects**

Trace each failure to the earliest violated slot transition, capacity projection, mutation boundary, scheduler wakeup, or cleanup-owner claim. Preserve the interfaces and semantics from Tasks 1-8; do not add fallback direct writes, retroactive reservation, unbounded queues, or copied aggregate results. Add a focused regression assertion beside every correction.

- [ ] **Step 4: Run the matrix repeatedly and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest -Dsurefire.failIfNoSpecifiedTests=false -Dsurefire.rerunFailingTestsCount=3 test
git add yierdis-server yierdis-networking yierdis-command yierdis-db yierdis-tests/yierdis-integration-tests
git commit -m "test: cover ordered reply failure matrix"
```

Expected: PASS on all repetitions with every outbound and inbound lease gauge returning to zero.

### Task 10: Publish Operations And Architecture Documentation

**Interfaces:** Documentation describes the final seven-stage architecture, exact defaults, limit semantics, result-unknown behavior, observability, and failure-safe operating procedures.

- [ ] **Step 1: Add a failing documentation contract test**

Extend `ArchitectureBoundaryTest` with a documentation contract that requires `production-hardening-operations.md` and verifies exact reply, ingress, commit-stream, maxmemory, shutdown, soak, and performance terms. Require README and the core logic index to link it. Check that legacy text does not claim output is one growable `ByteBuf`, that command handlers publish changes, or that a successful close can ignore active leases.

- [ ] **Step 2: Run the documentation guard**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the final operations guide and reply-egress descriptions do not exist.

- [ ] **Step 3: Update the complete documentation set**

Document startup validation, the six reply defaults, capacity accounting, FAIR/GLOBAL behavior, soft versus hard output limits, slot ordering, owned/replayable sources, preflight-before-mutation, result-unknown disconnects, commit-stream pressure, INFO/STATS fields, graceful shutdown sequence, timeout diagnosis, leak triage, and operator-safe recovery. Add exact JDK 25 smoke, soak, and benchmark commands plus expected success criteria.

Update allocator/native memory, DB internals, maxmemory, executor/backpressure, protocol, change-event, configuration/operations, testing/debugging, and core-index documents so each names its owning module and cross-links the new guide. Keep implementation details consistent with the final code and avoid promises of crash durability for the in-process commit stream.

- [ ] **Step 4: Run the guard and commit docs**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
git add README.md docs/project-docs yierdis-tests/yierdis-architecture-tests
git commit -m "docs: publish production hardening operations guide"
```

Expected: PASS with every documented default and lifecycle statement guarded by the architecture suite.

### Task 11: Run A Ten-Minute Bounded Production Soak

**Interfaces:** `scripts/production-hardening-soak.sh` runs a deterministic ten-minute workload and emits machine-readable samples plus a non-zero exit on any limit, ordering, error, or cleanup violation.

- [ ] **Step 1: Add a red soak harness contract**

Create `ProductionHardeningSoakTest` to invoke a short deterministic test mode. Require mixed GET/SET/HSET/ZADD, pipelined large replies, KEYS/SCAN, counted pop, expiry, eviction, commit sink delay, slow-reader disconnect, and graceful shutdown. Sample heap/native/maxmemory, ingress reservations, commit slots/bytes, outbound reserved/allocated bytes, reply slots/sources/buffers, child channels, rejects, and ordering sequence.

Assert the harness fails on any configured hard-limit overshoot, negative counter, missing sample, wire-order mismatch, unexpected command error, worker death, shutdown timeout, or non-zero final ownership counter. Preserve the random seed, exact argv, JDK, OS, commit, elapsed time, and peak values in its report.

- [ ] **Step 2: Run the short red contract**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=ProductionHardeningSoakTest -Dyierdis.soak.durationSeconds=15 -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the bounded soak harness and its final counter assertions do not exist.

- [ ] **Step 3: Implement and run the full soak**

Implement the test fixture and shell wrapper without external Redis dependencies. Use explicit low limits to force admission and drain paths while keeping the workload deterministic. The shell script packages current artifacts, runs exactly 600 seconds, stores logs/samples under `target/production-hardening-soak`, and propagates the test exit status.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710
```

Expected: PASS after ten minutes; all hard limits are respected, reply order is intact, no unexpected errors occur, and every final lease/slot/source/buffer/child counter is zero.

- [ ] **Step 4: Commit the reproducible soak harness**

```bash
git add scripts/production-hardening-soak.sh yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/ProductionHardeningSoakTest.java
git commit -m "test: add bounded production hardening soak"
```

Expected: the commit contains the harness and assertions, while generated soak reports remain untracked.

### Task 12: Enforce Four-Command Throughput Gates

**Interfaces:** The release suite compares immutable baseline `fb857980^` to current artifacts and fails when median GET, SET, HSET, or ZADD throughput is below 90%; large pipelined reply measurements are diagnostic only.

- [ ] **Step 1: Add red suite-profile and threshold tests**

Extend `SuiteProfileFactoryTest` and `SuiteThresholdEvaluatorTest` so the release profile contains isolated GET, SET, HSET, and ZADD median-throughput comparisons with a `0.90` minimum ratio. Assert one `0.8999` ratio fails the suite, `0.90` passes, missing/errored samples fail, and all four commands are required. Add a diagnostic large pipelined reply scenario that records outbound reserved/allocated peaks, capacity waits, and write failures without changing the four mandatory thresholds.

- [ ] **Step 2: Run the red benchmark tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteProfileFactoryTest,SuiteThresholdEvaluatorTest,SuiteRunnerOrchestrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the release policy does not yet enforce all four production-hardening ratios or expose reply-egress diagnostics.

- [ ] **Step 3: Implement policy and run the immutable comparison**

Add the four workload definitions and exact threshold policy. Extend observation snapshots/reports with outbound reply gauges while preserving existing report compatibility. Build the current server/benchmark jars and use the separately preserved baseline server jar built from `fb857980^`; never rebuild or mutate that baseline during the comparison.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -DskipTests package
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar --suite --suiteProfile release --baselineServerJar artifacts/baseline/yierdis-server-main-0.1.0-SNAPSHOT.jar --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --reportDir target/benchmark-reports/production-hardening
```

Expected: PASS only when GET, SET, HSET, and ZADD current median throughput are each at least 90% of baseline; the report also includes non-gating large-reply outbound metrics.

- [ ] **Step 4: Commit benchmark gates**

```bash
git add yierdis-benchmark
git commit -m "perf: enforce production hardening throughput gates"
```

Expected: the release profile, threshold tests, and diagnostic reply scenario are committed; generated benchmark artifacts remain untracked.

### Task 13: Execute Final Seven-Stage Acceptance

**Interfaces:** Acceptance metadata records exact commit/artifact identities and proves focused behavior, architecture, full suite, smoke, soak, throughput, and terminal ownership on the same candidate.

- [ ] **Step 1: Run all focused production-hardening suites**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db,yierdis-networking,yierdis-command,yierdis-server,yierdis-tests/yierdis-integration-tests -am -Dtest=OutboundMemoryBudgetTest,ConnectionReplySequencerTest,BoundedChunkedReplySinkTest,OrderedReplyPipelineTest,ReplyShutdownTest,OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest,CommitStreamIntegrationTest,CommitStreamShutdownTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with no skipped specified class and no retained ownership after each fixture.

- [ ] **Step 2: Run architecture and the complete JDK 25 suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS with all module boundaries, source guards, and repository tests green.

- [ ] **Step 3: Run smoke, soak, and performance gates on one candidate**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH ./scripts/smoke.sh
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar --suite --suiteProfile release --baselineServerJar artifacts/baseline/yierdis-server-main-0.1.0-SNAPSHOT.jar --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --reportDir target/benchmark-reports/production-hardening-final
```

Expected: PASS for smoke and ten-minute soak; every hard limit and final counter assertion holds; all four throughput ratios are at least 0.90.

- [ ] **Step 4: Record acceptance metadata and commit**

Record candidate commit, baseline commit/artifact checksum, current artifact checksum, JDK/OS/CPU, exact commands, focused/full test totals, soak seed/peaks/final counters, and four throughput medians/ratios in `docs/project-docs/production-hardening-operations.md`. Do not claim acceptance if any command was rerun against a different candidate artifact.

```bash
git add docs/project-docs/production-hardening-operations.md
git commit -m "docs: record production hardening acceptance"
```

Expected: the committed record identifies one fully verified candidate and contains no failed, skipped, unknown, or below-threshold gate.

## Stage Exit

Stage 7 and the production-hardening program are complete only after Tasks 1-13 pass on one candidate. Per-connection reply order must hold across every output origin; global, connection, and single-reply limits must remain hard; preflight failures must remain failure-atomic; visibility-unknown failures must close without replacement output; FAIR/GLOBAL scheduling must match configuration; and successful shutdown must finish with zero inbound, commit-stream, reply-slot, source, chunk, write-future, and child-channel ownership. Preserve the soak and benchmark artifacts referenced by the committed acceptance record.
