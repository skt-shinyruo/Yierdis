# Backend Network And Executor Contract Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace split transport/executor admission, untyped reply-capacity waits, replay-on-capacity execution, and binary read pausing with one ownership-safe handoff, typed capacity registrations, prepared one-shot execution, and composable read-pause tokens.

**Architecture:** The RESP decoder retains at most one complete message and hands it to a Netty coordinator that acquires a reply-control lease and an executor backlog admission before publishing one ordered task. The executor owns the serial command thread and keeps the same `PreparedCommand` and protocol-produced `ReplyPlan` while reply capacity is unavailable; mutation begins only after reservation and is never replayed. Netty networking owns reply budgets, ordered slots, wire-buffer allocation, and read-pause state; `server-main` only adapts those contracts and wires them together.

**Tech Stack:** Java 25, Maven reactor, JUnit 4, Netty 4.1, existing RESP adapter and executor queues.

## Global Constraints

- This plan consumes the coordinated contract rewrite's `CommandSession`, `PreparedCommand`, `ValidationResult`, `CommandExecutionContext`, `ReplyShape`, `ReplyPlan`, and `ReplySizer`; it does not create compatibility adapters for `Session`, handler-driven reply preflight, or the old executor submission API. Network/executor Task 1 is the deliberate contract-first exception: it runs before prepared execution exists and therefore retains the then-current `execute(...)` engine boundary only until command/runtime Task 3 atomically replaces it and deletes `execute(...)`.
- After command/runtime Task 3, `CommandExecutionEngine` has exactly one abstract method: `PreparedCommand prepare(CommandSession session, ExecutionRequest request)`. Network/executor Task 1 must neither introduce `prepare(...)` early nor retain `execute(...)` after that command/runtime checkpoint.
- `PreparedCommand` has exactly `ReplyShape replyShape()`, `ValidationResult validateBeforeExecute()`, `void execute(CommandExecutionContext context)`, and `void close()`; `ValidationResult` has `VALID` and `STALE`.
- `ReplySizer.plan(CommandSession, ReplyShape)` is the sole producer of a task's `ReplyPlan`; command, storage, executor, and reply-slot code do not calculate RESP framing.
- `ReplyPlan` contains `encodedUpperBoundBytes` and `retainedSourceBytes`; Netty may add configured control and chunk-allocation overhead when reserving its outbound budget.
- Every submitted command, maintenance action, writable recovery, startup bind, and shutdown drain executes through one `SerialOwnerExecutor` whose actions never overlap and always use the same physical thread.
- The decoder retains at most one fully decoded message that has not completed handoff. `server-main` has no second pending-command queue.
- A normal request is admitted only after an ordered reply-control lease and executor backlog reservation are both held and the executor task owns the request and reply slot.
- One connection may own one control-admission waiter plus one expansion waiter per live reply lease. Expansion work for the earliest admitted lease has same-connection priority; grants rotate across connections.
- Failure to retain a capacity waiter is not permission to silently cancel an admitted slot. A live connection receives a terminal error or is explicitly closed.
- Mutation starts only after `ReplyPlan` reservation. Capacity waiting retains the same prepared object. Stale validation closes and recreates the prepared object before mutation.
- A prepared mutation executes at most once. Failure after mutation begins closes the connection when complete output or mutation visibility cannot be proved; it never re-enters preparation or execution for that task.
- Read pauses use independently owned tokens tagged `INGRESS_BUDGET`, `EXECUTOR_QUEUE`, `REPLY_CAPACITY`, `TRANSPORT_UNWRITABLE`, or `CLOSING`. Closing one token cannot clear another token, including another token with the same reason.
- A physical `ChannelHandlerContext.read()` occurs only when the connection is open and active, no read is already in flight or scheduled, no outstanding read credit exists, no inbound-memory credit acquisition is pending, and no pause token remains.
- Request, admission, waiter, prepared command, reply slot, lease, chunks, and retained reply sources each have one explicit owner and close idempotently exactly once on success, rejection, stale re-preparation, disconnect, and shutdown.
- All Java and Maven commands use JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`.
- Do not stage, rewrite, or revert unrelated worktree changes.

---

## File Structure

Create in `yierdis-server-api`:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CapacityRegistration.java`: transport-neutral, idempotent cancellation ownership.
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyReservationResult.java`: reply reservation outcome consumed by executor-core.
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionReply.java`: semantic executor-to-reply-owner contract.

Create in `yierdis-server-executor`:

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/SerialOwnerExecutor.java`: one-thread, non-overlapping execution contract.
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmission.java`: unpublished backlog reservation and ownership-transfer token.
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmissionAttempt.java`: acquired, temporary-capacity, and terminal-rejection result.

Create in `yierdis-networking-netty`:

- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundPauseReason.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundPauseToken.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundWaitKind.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundCapacityRegistration.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/InboundReadCreditHandlerTest.java`

Create in `server-main`:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettySerialOwnerExecutor.java`: adapts exactly one Netty `EventExecutor`.

Move from `server-executor` to `server-api` and change the package to `yier.bubu.redis.execution.api`:

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionReply.java`

Move from `server-main` to package `yier.bubu.redis.protocol.resp.netty` in `networking-netty`:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudget.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudgetStats.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundConnectionMemory.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryLease.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlot.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlotState.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplyCleanupOwner.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplySequencer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplyEgressStats.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/BoundedChunkedReplySink.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OutboundMemoryBudgetTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionReplySequencerTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/BoundedChunkedReplySinkTest.java`

Delete after replacement tests pass:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RegisteredRespMessage.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyCapacityUnavailableException.java`

Modify server-api contracts:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ConnectionStatsView.java`

Modify executor production files:

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionEngine.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudget.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureIo.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureRuntime.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionIoAdapter.java`

Modify networking production files:

- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundReadControl.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundReadCreditHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespDecodedMessageGate.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`

Modify composition and adapters:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGate.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ChildChannelRegistry.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`

Modify focused tests and support:

- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorAdmissionTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/PreparedCommandExecutionTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudgetTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespIngressAdmissionTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGateTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyPipelineTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyTestFixture.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespIngressLifecycleIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/ProductionHardeningSoakTest.java`
- `docs/project-docs/executor-and-backpressure.md`

---

### Task 1: Introduce Serial Ownership And Two-Phase Executor Admission

**Files:**
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CapacityRegistration.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyReservationResult.java`
- Move/Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionReply.java`
- Delete: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionReply.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/SerialOwnerExecutor.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmission.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmissionAttempt.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettySerialOwnerExecutor.java`
- Create: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorAdmissionTest.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudget.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlot.java`

**Interfaces:**
- Consumes: the current `CommandExecutionEngine.execute(...)`, `ExecutionRequest`, `ReplyPlan`, `RedisReplyWriterFactory`, and `ExecutionConnection` contracts. This task deliberately precedes the command/runtime plan's prepared-execution task and does not reference `PreparedCommand`, `ReplyShape`, or `ReplySizer`.
- Produces: `SerialOwnerExecutor`, an unpublished `ExecutorAdmission<C>`, `CommandExecutor.tryAcquire(C, int)`, and `CommandExecutor.onAdmissionAvailable(int, Runnable)`.
- Ownership rule: `tryAcquire` never owns the request or reply. `ExecutorAdmission.publish(...)` always consumes both; `ExecutorAdmission.close()` releases only an unpublished reservation.

- [ ] **Step 1: Add failing admission ownership tests**

Create `ExecutorAdmissionTest.java` with these tests and helpers:

```java
package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationResult;

import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorAdmissionTest {
    @Test
    public void unpublishedAdmissionReservesCapacityButNotRequestOwnership() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = newExecutor(owner, 1, 64L);
        TestConnection connection = ExecutorCoreTestSupport.newConnection("admission");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("PING");
        TrackingReply reply = new TrackingReply();
        try {
            ExecutorAdmissionAttempt<TestConnection> first = executor.tryAcquire(connection, 32);
            Assert.assertTrue(first instanceof ExecutorAdmissionAttempt.Acquired<TestConnection>);
            Assert.assertEquals(1, executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0, connection.context().pending());
            Assert.assertEquals(0, request.closeCalls());
            Assert.assertEquals(0, reply.cancelCalls());

            ExecutorAdmissionAttempt<TestConnection> second = executor.tryAcquire(connection, 32);
            Assert.assertEquals(
                    ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS,
                    ((ExecutorAdmissionAttempt.Unavailable<TestConnection>) second).reason()
            );

            ExecutorAdmission<TestConnection> admission =
                    ((ExecutorAdmissionAttempt.Acquired<TestConnection>) first).admission();
            admission.close();
            admission.close();

            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
            Assert.assertEquals(0, request.closeCalls());
            Assert.assertEquals(0, reply.cancelCalls());
        } finally {
            request.close();
            reply.close();
            executor.close();
            owner.runAll();
        }
    }

    @Test
    public void publishTransfersRequestAndReplyExactlyOnce() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = newExecutor(owner, 1, 64L);
        TestConnection connection = ExecutorCoreTestSupport.newConnection("publish");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("PING");
        TrackingReply reply = new TrackingReply();

        ExecutorAdmission<TestConnection> admission = acquired(executor.tryAcquire(connection, request.retainedBytes()));
        admission.publish(request, reply);

        Assert.assertEquals(1, connection.context().pending());
        Assert.assertThrows(IllegalStateException.class, () -> admission.publish(request, reply));
        admission.close();
        Assert.assertEquals(0, request.closeCalls());
        Assert.assertEquals(0, reply.cancelCalls());

        executor.close();
        owner.runAll();

        Assert.assertEquals(1, request.closeCalls());
        Assert.assertEquals(1, reply.cancelCalls());
        Assert.assertEquals(0, connection.context().pending());
        Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
    }

    @Test
    public void capacityRegistrationWakesOnceAndCanBeCancelled() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = newExecutor(owner, 1, 64L);
        TestConnection connection = ExecutorCoreTestSupport.newConnection("wait");
        ExecutorAdmission<TestConnection> held = acquired(executor.tryAcquire(connection, 32));
        AtomicInteger wakeups = new AtomicInteger();

        CapacityRegistration registration = executor.onAdmissionAvailable(32, wakeups::incrementAndGet);
        Assert.assertEquals(0, wakeups.get());

        held.close();
        Assert.assertEquals(1, wakeups.get());
        registration.cancel();
        registration.cancel();
        Assert.assertEquals(1, wakeups.get());

        executor.close();
        owner.runAll();
    }

    private static <C extends ExecutionConnection> ExecutorAdmission<C> acquired(
            ExecutorAdmissionAttempt<C> attempt
    ) {
        Assert.assertTrue(attempt instanceof ExecutorAdmissionAttempt.Acquired<C>);
        return ((ExecutorAdmissionAttempt.Acquired<C>) attempt).admission();
    }

    private static CommandExecutor<TestConnection> newExecutor(
            ManualOwnerExecutor owner,
            int queueCapacity,
            long queueMaxBytes
    ) {
        return new CommandExecutor<>(
                () -> { },
                ExecutorCoreTestSupport.simpleCommandEngine(),
                owner,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                new RecordingIoAdapter(),
                new CommandExecutorConfig(
                        queueCapacity,
                        queueMaxBytes,
                        queueCapacity,
                        0,
                        0,
                        0,
                        128,
                        10,
                        SchedulingPolicy.FAIR
                )
        );
    }

    private static final class TrackingReply implements ExecutionReply {
        private final AtomicInteger cancelCalls = new AtomicInteger();

        @Override
        public ReplyReservationResult tryReserve(ReplyPlan plan) {
            return ReplyReservationResult.RESERVED;
        }

        @Override
        public CapacityRegistration onCapacityAvailable(Runnable wakeup) {
            return CapacityRegistration.NONE;
        }

        @Override
        public BytesSink sink() {
            return (source, sourceIndex, length) -> { };
        }

        @Override
        public void markReady(boolean closeAfterReply) {
        }

        @Override
        public void cancel() {
            cancelCalls.incrementAndGet();
        }

        @Override
        public boolean hasWrittenBytes() {
            return false;
        }

        @Override
        public void markResultUnknown() {
        }

        @Override
        public void close() {
            cancel();
        }

        int cancelCalls() {
            return cancelCalls.get();
        }
    }
}
```

Add these assertions to `CommandExecutorTest`:

```java
    @Test
    public void ownerContractRejectsCallsFromTheWrongThread() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();

        Assert.assertFalse(owner.inOwnerThread());
        Assert.assertThrows(IllegalStateException.class, owner::requireOwnerThread);

        owner.execute(() -> {
            Assert.assertTrue(owner.inOwnerThread());
            owner.requireOwnerThread();
        });
        owner.runAll();
    }
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor -am \
  -Dtest=ExecutorAdmissionTest,CommandExecutorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: compilation fails because `SerialOwnerExecutor`, `ExecutorAdmission`, `ExecutorAdmissionAttempt`, `CapacityRegistration`, and `CommandExecutor.tryAcquire(...)` do not exist.

- [ ] **Step 3: Add the stable shared and serial-owner contracts**

Create `CapacityRegistration.java`:

```java
package yier.bubu.redis.execution.api;

@FunctionalInterface
public interface CapacityRegistration extends AutoCloseable {
    CapacityRegistration NONE = () -> { };

    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
```

Create `ReplyReservationResult.java`:

```java
package yier.bubu.redis.execution.api;

public enum ReplyReservationResult {
    RESERVED,
    WAITING,
    TOO_LARGE,
    CLOSED
}
```

Move `ExecutionReply` to server-api and replace it with:

```java
package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionReply extends AutoCloseable {
    ReplyReservationResult tryReserve(ReplyPlan plan);

    CapacityRegistration onCapacityAvailable(Runnable wakeup);

    BytesSink sink();

    void markReady(boolean closeAfterReply);

    void cancel();

    boolean hasWrittenBytes();

    void markResultUnknown();

    @Override
    void close();
}
```

`CapacityRegistration.NONE` means that no callback ownership was retained. Adapt the current `ReplySlot` before the later package move by replacing `awaitCapacity(...)` with these methods:

```java
    @Override
    public ReplyReservationResult tryReserve(ReplyPlan plan) {
        try {
            BytesSink current = sink();
            if (!(current instanceof ReplyReservationSink reservationSink)) {
                throw new IllegalStateException("reply sink does not support reservation");
            }
            reservationSink.require(Objects.requireNonNull(plan, "plan"));
            return ReplyReservationResult.RESERVED;
        } catch (ReplyCapacityUnavailableException unavailable) {
            return ReplyReservationResult.WAITING;
        } catch (ReplyTooLargeException tooLarge) {
            return ReplyReservationResult.TOO_LARGE;
        } catch (IllegalStateException closedSlot) {
            if (cleanupOwner.get() == ReplyCleanupOwner.NONE) {
                throw closedSlot;
            }
            return ReplyReservationResult.CLOSED;
        }
    }

    @Override
    public CapacityRegistration onCapacityAvailable(Runnable wakeup) {
        Objects.requireNonNull(wakeup, "wakeup");
        CapacityWait waiting = capacityWait.get();
        if (waiting == null || cleanupOwner.get() != ReplyCleanupOwner.NONE || isTerminal(state.get())) {
            return CapacityRegistration.NONE;
        }
        AtomicBoolean active = new AtomicBoolean(true);
        boolean retained = lease.awaitAdditionalCapacity(
                waiting.additionalBytes(),
                waiting.singleReplyLimitBytes(),
                () -> {
                    if (active.compareAndSet(true, false)) {
                        wakeup.run();
                    }
                }
        );
        if (!retained) {
            active.set(false);
            return CapacityRegistration.NONE;
        }
        return () -> {
            if (active.compareAndSet(true, false)) {
                cancelCapacityWait();
            }
        };
    }
```

Add the `CapacityRegistration`, `ReplyReservationResult`, `ReplyCapacityUnavailableException`, and `ReplyTooLargeException` imports from `yier.bubu.redis.execution.api`, then remove the old `awaitCapacity(...)` override. Use these exact fake implementations in `CommandExecutorTest.TrackingReply`:

```java
        @Override
        public ReplyReservationResult tryReserve(ReplyPlan plan) {
            return ReplyReservationResult.RESERVED;
        }

        @Override
        public CapacityRegistration onCapacityAvailable(Runnable wakeup) {
            return CapacityRegistration.NONE;
        }

        @Override
        public void markResultUnknown() {
        }
```

Use these implementations in `ReplyCapacityBlockedSchedulingTest.BlockingReply`, replacing `awaitCapacity(...)` and adding `capacityWaitActive`:

```java
        private final AtomicBoolean capacityWaitActive = new AtomicBoolean();

        @Override
        public ReplyReservationResult tryReserve(ReplyPlan plan) {
            return capacityAvailable.get()
                    ? ReplyReservationResult.RESERVED
                    : ReplyReservationResult.WAITING;
        }

        @Override
        public CapacityRegistration onCapacityAvailable(Runnable callback) {
            Objects.requireNonNull(callback, "callback");
            capacityWaitActive.set(true);
            wakeup = () -> {
                if (capacityWaitActive.compareAndSet(true, false)) {
                    callback.run();
                }
            };
            return () -> capacityWaitActive.set(false);
        }

        @Override
        public void markResultUnknown() {
        }
```

Teach `CommandExecutorTask` to own that registration independently from request/reply ownership:

```java
    private CapacityRegistration capacityRegistration = CapacityRegistration.NONE;

    void ownCapacityRegistration(CapacityRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        CapacityRegistration previous = capacityRegistration;
        capacityRegistration = registration;
        previous.cancel();
    }

    void capacityRegistrationSignalled() {
        capacityRegistration = CapacityRegistration.NONE;
    }

    void cancelCapacityRegistration() {
        CapacityRegistration registration = capacityRegistration;
        capacityRegistration = CapacityRegistration.NONE;
        registration.cancel();
    }
```

Replace `CommandExecutorDrainLoop.registerBlockedReplyTask(...)` registration with the shared handle:

```java
        CapacityRegistration registration;
        try {
            registration = task.reply.onCapacityAvailable(() -> ownerExecutor.execute(() -> {
                task.capacityRegistrationSignalled();
                if (taskQueue.resumeBlocked(task.connection, task)) {
                    scheduleDrain();
                }
            }));
        } catch (Throwable ignored) {
            registration = CapacityRegistration.NONE;
        }
        if (registration == CapacityRegistration.NONE) {
            if (taskQueue.cancelBlocked(task.connection, task)) {
                executionSupport.recycleAndRelease(task);
            }
            return;
        }
        task.ownCapacityRegistration(registration);
```

Call `task.cancelCapacityRegistration()` at the start of `CommandExecutorExecutionSupport.recycleAndRelease(...)`. The connection-close callback uses the same cancellation path, so shutdown, disconnect, and an explicit task cancel cannot retain a stale wakeup.

Create `SerialOwnerExecutor.java`:

```java
package yier.bubu.redis.execution.executor;

import java.util.concurrent.Executor;

public interface SerialOwnerExecutor extends Executor {
    boolean inOwnerThread();

    default void requireOwnerThread() {
        if (!inOwnerThread()) {
            throw new IllegalStateException("not on the serial owner thread");
        }
    }
}
```

Create `ExecutorAdmissionAttempt.java`:

```java
package yier.bubu.redis.execution.executor;

import java.util.Objects;

public sealed interface ExecutorAdmissionAttempt<C extends ExecutionConnection>
        permits ExecutorAdmissionAttempt.Acquired,
                ExecutorAdmissionAttempt.Unavailable,
                ExecutorAdmissionAttempt.Rejected {
    record Acquired<C extends ExecutionConnection>(ExecutorAdmission<C> admission)
            implements ExecutorAdmissionAttempt<C> {
        public Acquired {
            Objects.requireNonNull(admission, "admission");
        }
    }

    record Unavailable<C extends ExecutionConnection>(BlockReason reason)
            implements ExecutorAdmissionAttempt<C> {
        public Unavailable {
            Objects.requireNonNull(reason, "reason");
        }
    }

    record Rejected<C extends ExecutionConnection>(CommandExecutor.SubmitRejectReason reason)
            implements ExecutorAdmissionAttempt<C> {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    enum BlockReason {
        QUEUE_SLOTS,
        QUEUE_BYTES
    }
}
```

Create `NettySerialOwnerExecutor.java`:

```java
package yier.bubu.redis.app.server;

import io.netty.util.concurrent.EventExecutor;
import yier.bubu.redis.execution.executor.SerialOwnerExecutor;

import java.util.Objects;

final class NettySerialOwnerExecutor implements SerialOwnerExecutor {
    private final EventExecutor delegate;

    NettySerialOwnerExecutor(EventExecutor delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(Objects.requireNonNull(command, "command"));
    }

    @Override
    public boolean inOwnerThread() {
        return delegate.inEventLoop();
    }
}
```

- [ ] **Step 4: Implement the unpublished executor admission lease**

Create `ExecutorAdmission.java`:

```java
package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class ExecutorAdmission<C extends ExecutionConnection> implements AutoCloseable {
    private enum State {
        OPEN,
        PUBLISHED,
        CLOSED
    }

    private final CommandExecutorSubmitter<C> owner;
    private final C connection;
    private final int retainedBytes;
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    ExecutorAdmission(CommandExecutorSubmitter<C> owner, C connection, int retainedBytes) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.connection = Objects.requireNonNull(connection, "connection");
        this.retainedBytes = retainedBytes;
    }

    public C connection() {
        return connection;
    }

    public int retainedBytes() {
        return retainedBytes;
    }

    public void publish(ExecutionRequest request, ExecutionReply reply) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(reply, "reply");
        if (!state.compareAndSet(State.OPEN, State.PUBLISHED)) {
            throw new IllegalStateException("executor admission is no longer publishable");
        }
        owner.publish(this, request, reply);
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.OPEN, State.CLOSED)) {
            owner.releaseUnpublished(this);
        }
    }
}
```

Replace the separate count/byte reservation in `ExecutorBacklogBudget` with the following package-private operation while retaining the existing counters and high/low-watermark queries:

```java
    ExecutorAdmissionAttempt.BlockReason tryReserve(int retainedBytes) {
        if (!tryReserveSlot()) {
            return ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS;
        }
        if (!tryReserveQueuedBytes(retainedBytes)) {
            releaseSlot();
            return ExecutorAdmissionAttempt.BlockReason.QUEUE_BYTES;
        }
        return null;
    }

    void release(int retainedBytes) {
        try {
            releaseQueuedBytes(retainedBytes);
        } finally {
            releaseSlot();
        }
    }

    CapacityRegistration onCapacityAvailable(int retainedBytes, Runnable callback) {
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        CapacityWaiter waiter = new CapacityWaiter(retainedBytes, callback);
        capacityWaiters.offer(waiter);
        signalCapacityWaiters();
        return waiter;
    }
```

Make `CapacityWaiter` implement `yier.bubu.redis.execution.api.CapacityRegistration`. Its existing atomic one-shot `signal()` and `cancel()` bodies remain the implementation; remove the nested `CommandExecutor.CapacityRegistration` type.

Replace submission entry points in `CommandExecutorSubmitter` with these concrete methods:

```java
    ExecutorAdmissionAttempt<C> tryAcquire(C connection, int retainedBytes) {
        Objects.requireNonNull(connection, "connection");
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        ExecutionConnectionContext context = connection.context();
        if (!running.getAsBoolean()) {
            context.recordCommandRejected();
            submitRejectedNotRunning.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.NOT_RUNNING);
        }
        if (context.isClosing()) {
            context.recordCommandRejected();
            submitRejectedClosing.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING);
        }
        if (!backlogBudget.canEverReserveQueuedBytes(retainedBytes)) {
            context.recordCommandRejected();
            submitRejectedRequestTooLarge.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.REQUEST_TOO_LARGE);
        }
        ExecutorAdmissionAttempt.BlockReason blocked = backlogBudget.tryReserve(retainedBytes);
        if (blocked != null) {
            if (blocked == ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS) {
                submitRejectedQueueFull.increment();
            } else {
                submitRejectedBytesBudget.increment();
            }
            return new ExecutorAdmissionAttempt.Unavailable<>(blocked);
        }
        return new ExecutorAdmissionAttempt.Acquired<>(new ExecutorAdmission<>(this, connection, retainedBytes));
    }

    // 成功从 OPEN 切换到 PUBLISHED 后，request 和 reply 的唯一所有者就是此方法；
    // 任何入队、计数或调度故障都不能把它们重新交给 decoder。
    void publish(
            ExecutorAdmission<C> admission,
            ExecutionRequest request,
            ExecutionReply reply
    ) {
        C connection = admission.connection();
        int retainedBytes = admission.retainedBytes();
        CommandExecutorTask<C> task = new CommandExecutorTask<>(connection, request, retainedBytes, reply);
        boolean recorded = false;
        boolean offered = false;
        try {
            connection.context().recordCommandEnqueued(retainedBytes);
            recorded = true;
            if (!taskQueue.offer(connection, task)) {
                throw new IllegalStateException("reserved executor admission could not be published");
            }
            offered = true;
            submitAccepted.increment();
            scheduleDrain.run();
            return;
        } catch (Throwable ignored) {
            // A task removed by a concurrent owner drain has already become executor-owned;
            // leave its accounting intact and let the normal terminal path release it.
            boolean removed = !offered || taskQueue.remove(connection, task);
            if (!removed) {
                markClosingAfterPublishFailure(connection);
                return;
            }
            if (recorded) {
                try {
                    connection.context().rollbackCommandEnqueued(retainedBytes);
                } catch (Throwable ignoredRollback) {
                }
            }
            closeAcceptedTask(task);
            markClosingAfterPublishFailure(connection);
        }
    }

    void releaseUnpublished(ExecutorAdmission<C> admission) {
        backlogBudget.release(admission.retainedBytes());
    }

    private void closeAcceptedTask(CommandExecutorTask<C> task) {
        try {
            task.request.close();
        } catch (Throwable ignored) {
        }
        try {
            task.reply.cancel();
        } catch (Throwable ignored) {
        }
        try {
            backlogBudget.release(task.retainedBytes);
        } catch (Throwable ignored) {
        }
    }

    private static <C extends ExecutionConnection> void markClosingAfterPublishFailure(C connection) {
        try {
            connection.markClosing();
        } catch (Throwable ignored) {
        }
    }
```

`ExecutorAdmission.publish(...)` may reject only before it changes its state
(null input or a second call). Once it has changed `OPEN -> PUBLISHED`, it never
throws a publication failure to its caller: `CommandExecutorSubmitter.publish`
consumes the request and reply on every branch. A failed offer, accounting
record, enqueue counter, or drain scheduling either removes the unpublished task,
rolls back its accounting, independently closes request/reply/releases backlog,
and marks the connection closing, or observes that a concurrent owner drain has
already taken the task and leaves that owner intact. Add fault-injection tests for
`recordCommandEnqueued`, queue offer, `scheduleDrain`, reply cancellation,
backlog release, and `markClosing`; each test asserts one request close, one reply
cancel, zero queued tasks/bytes after the branch resolves, and no decoder-side
second close.

Pass `drainLoop::scheduleDrain` into the `CommandExecutorSubmitter` constructor as a stored `Runnable scheduleDrain`; remove both `trySubmit` methods and their call-time scheduling argument.

- [ ] **Step 5: Expose only two-phase admission and enforce the serial owner**

Change the constructor's owner parameter and field from `Executor` to `SerialOwnerExecutor`; the constructor remains on the current execute-style engine surface in this task:

```java
    public CommandExecutor(
            Runnable bindToCurrentThread,
            CommandExecutionEngine commandProcessor,
            SerialOwnerExecutor ownerExecutor,
            RedisReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            CommandExecutorConfig config
    ) {
        this.bindToCurrentThread = Objects.requireNonNull(bindToCurrentThread, "bindToCurrentThread");
        Objects.requireNonNull(config, "config");
        this.ownerExecutor = Objects.requireNonNull(ownerExecutor, "ownerExecutor");
        this.schedulingPolicy = config.schedulingPolicy();
        this.backlogBudget = new ExecutorBacklogBudget(config.queueCapacity(), config.queueMaxBytes());
        this.backpressureController = new ExecutorBackpressureController<>(
                this.ownerExecutor,
                backlogBudget,
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                backpressureIo(ioAdapter),
                backpressureRuntime(),
                backpressureObserver(),
                () -> running
        );
        ArrayBlockingQueue<CommandExecutorTask<C>> globalQueue =
                this.schedulingPolicy == SchedulingPolicy.GLOBAL
                        ? new ArrayBlockingQueue<>(config.queueCapacity())
                        : null;
        this.taskQueue = new ExecutorTaskQueue<>(
                this.schedulingPolicy,
                globalQueue,
                CommandExecutor::queueStateFor
        );
        this.executionSupport = new CommandExecutorExecutionSupport<>(
                Objects.requireNonNull(commandProcessor, "commandProcessor"),
                Objects.requireNonNull(replyWriterFactory, "replyWriterFactory"),
                Objects.requireNonNull(ioAdapter, "ioAdapter"),
                backlogBudget,
                backpressureController,
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                () -> running
        );
        this.drainLoop = new CommandExecutorDrainLoop<>(
                this.ownerExecutor,
                taskQueue,
                executionSupport,
                config.maxDrainCommands(),
                TimeUnit.MILLISECONDS.toNanos(config.drainTimeLimitMillis()),
                () -> running
        );
        this.submitter = new CommandExecutorSubmitter<>(
                taskQueue,
                backlogBudget,
                backpressureController,
                config.backpressureHighWatermark(),
                config.backpressureBytesHighWatermark(),
                () -> running,
                drainLoop::scheduleDrain
        );
    }

    private ExecutorBackpressureIo<C> backpressureIo(ExecutionIoAdapter<C> ioAdapter) {
        return new ExecutorBackpressureIo<>() {
            @Override public boolean isActive(C key) { return ioAdapter.isActive(key); }
            @Override public boolean isWritable(C key) { return ioAdapter.isWritable(key); }
            @Override public void disableAutoRead(C key) { ioAdapter.disableInput(key); }
            @Override public void enableAutoRead(C key) { ioAdapter.enableInput(key); }
            @Override public void onClose(C key, Runnable callback) { ioAdapter.onClose(key, callback); }
        };
    }

    private ExecutorBackpressureRuntime<C> backpressureRuntime() {
        return new ExecutorBackpressureRuntime<>() {
            @Override public int pending(C key) { return key.context().pending(); }
            @Override public long pendingBytes(C key) { return key.context().pendingBytes(); }
            @Override public boolean isClosing(C key) { return key.context().isClosing(); }
            @Override public boolean markAutoReadDisabledByExecutor(C key) {
                return key.context().markInputDisabledByExecutor();
            }
            @Override public boolean autoReadDisabledByExecutor(C key) {
                return key.context().autoReadDisabledByExecutor();
            }
            @Override public boolean clearAutoReadDisabledByExecutor(C key) {
                return key.context().clearAutoReadDisabledByExecutor();
            }
            @Override public boolean inputPausedByReply(C key) {
                return key.context().inputPausedByReply();
            }
        };
    }

    private ExecutorBackpressureObserver<C> backpressureObserver() {
        return new ExecutorBackpressureObserver<>() {
            @Override
            public void onEnter(C key) {
                key.context().recordBackpressureEnter();
                backpressureEnter.increment();
            }

            @Override
            public void onExit(C key) {
                key.context().recordBackpressureExit();
                backpressureExit.increment();
            }
        };
    }

    public ExecutorAdmissionAttempt<C> tryAcquire(C connection, int retainedBytes) {
        return submitter.tryAcquire(connection, retainedBytes);
    }

    public CapacityRegistration onAdmissionAvailable(int retainedBytes, Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        if (!running) {
            callback.run();
            return CapacityRegistration.NONE;
        }
        return backlogBudget.onCapacityAvailable(retainedBytes, callback);
    }
```

The three private adapter methods above are a mechanical extraction of the current anonymous adapters. Do not retain `trySubmit(C, ExecutionRequest)`, `trySubmit(C, ExecutionRequest, ExecutionReply)`, or `onCapacityAvailable(ExecutionRequest, Runnable)`.

Change every owner-executor field and constructor parameter in `CommandExecutor`, `CommandExecutorDrainLoop`, and `ExecutorBackpressureController` from `Executor` to `SerialOwnerExecutor`. Add `ownerExecutor.requireOwnerThread()` at the start of `CommandExecutorDrainLoop.drainLoop()`, `drainLeftoverCommands()`, every scheduled maintenance body, writable recovery body, and shutdown drain body. Implement `close()` by setting `running = false`, waking capacity waiters, and submitting `drainLoop::drainLeftoverCommands` to `ownerExecutor`; when already in the owner thread it may invoke the drain directly. Tests using `ManualOwnerExecutor` call `owner.runAll()` after `close()`.

In `ExecutorCoreTestSupport`, make `ManualOwnerExecutor` implement the new contract with this complete owner-state behavior:

```java
final class ManualOwnerExecutor implements SerialOwnerExecutor {
    private final List<Runnable> tasks = new ArrayList<>();
    private Thread ownerThread;
    private boolean runningAction;

    @Override
    public void execute(Runnable command) {
        tasks.add(Objects.requireNonNull(command, "command"));
    }

    @Override
    public boolean inOwnerThread() {
        return runningAction && Thread.currentThread() == ownerThread;
    }

    int pendingTasks() {
        return tasks.size();
    }

    void runAll() {
        Thread caller = Thread.currentThread();
        if (ownerThread == null) {
            ownerThread = caller;
        } else if (ownerThread != caller) {
            throw new IllegalStateException("manual owner changed physical thread");
        }
        while (!tasks.isEmpty()) {
            List<Runnable> pending = new ArrayList<>(tasks);
            tasks.clear();
            for (Runnable task : pending) {
                if (runningAction) {
                    throw new IllegalStateException("serial owner actions overlapped");
                }
                runningAction = true;
                try {
                    task.run();
                } finally {
                    runningAction = false;
                }
            }
        }
    }
}
```

Update Bootstrap construction from `commandGroup.next()` to:

```java
        SerialOwnerExecutor commandOwner = new NettySerialOwnerExecutor(commandGroup.next());
        executor = new CommandExecutor<>(
                runtimeAccess::bindToCurrentThread,
                executionEngine,
                commandOwner,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                executorConfig
        );
```

- [ ] **Step 6: Run focused tests and verify GREEN**

Run the command from Step 2.

Expected: PASS; unpublished admission changes global budget counts but not connection pending counts, publish transfers request/reply once, cancellation wakes capacity waiters once, and the deterministic owner assertion passes.

- [ ] **Step 7: Run executor regression tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor -am test
```

Expected: PASS after all old `trySubmit` test call sites are migrated to `tryAcquire(...).admission().publish(...)`; there are no deprecated adapters.

- [ ] **Step 8: Commit the serial admission contract**

```bash
git add \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CapacityRegistration.java \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyReservationResult.java \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionReply.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/SerialOwnerExecutor.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmission.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorAdmissionAttempt.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudget.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorAdmissionTest.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettySerialOwnerExecutor.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlot.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java
git add -u yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionReply.java
git commit -m "refactor: make executor admission and ownership explicit"
```

---

### Task 2: Harden Prepared-Task Reservation And One-Reply-Or-Close Semantics

**Files:**
- Create: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/PreparedCommandExecutionTest.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionAttempt.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`

**Interfaces:**
- Consumes: Task 1 of this plan, including `ExecutionReply.tryReserve(ReplyPlan)`, `ExecutionReply.onCapacityAvailable(Runnable)`, and the `CapacityRegistration.NONE` failure sentinel.
- Consumes: Task 3 of `docs/superpowers/plans/2026-07-21-backend-command-runtime-rewrite.md`, including `PreparedCommand`, `ValidationResult`, `CommandExecutionContext`, `ReplySizer`, `CommandExecutionEngine.prepare(...)`, the prepared fields added to `CommandExecutorTask`, and the `ReplySizer` constructor parameter added to `CommandExecutor`.
- Produces: an executor-owned lifecycle in which `REPREPARE` is distinct from capacity blocking, a live admitted task can never disappear when waiter registration fails, and any exception after `PreparedCommand.execute(...)` begins closes rather than replays.
- Ownership rule: a task owns exactly one request and one reply for its full lifetime, at most one prepared command, its matching reply plan, and at most one capacity registration. Only terminal cleanup releases backlog and connection-pending accounting.

- [ ] **Step 1: Add RED lifecycle tests for both scheduling policies**

Create `PreparedCommandExecutionTest.java`. The first test must exercise the same blocked head under `FAIR` and `GLOBAL`, including repeated wakeups:

```java
package yier.bubu.redis.execution.executor;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;

public class PreparedCommandExecutionTest {
    @Test
    public void capacityWaitRetainsOnePreparationAndOnePlanUnderFairAndGlobal() {
        for (SchedulingPolicy policy : SchedulingPolicy.values()) {
            ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
            RecordingIoAdapter io = new RecordingIoAdapter();
            AtomicInteger aPrepares = new AtomicInteger();
            AtomicInteger aExecutes = new AtomicInteger();
            AtomicInteger aCloses = new AtomicInteger();
            AtomicInteger bExecutes = new AtomicInteger();
            CommandExecutionEngine engine = (session, request) -> {
                String name = new String(request.toByteArray(0), java.nio.charset.StandardCharsets.US_ASCII);
                if (name.equals("A")) {
                    aPrepares.incrementAndGet();
                    return prepared(ValidationResult.VALID, aExecutes, aCloses);
                }
                return prepared(ValidationResult.VALID, bExecutes, new AtomicInteger());
            };
            CommandExecutor<TestConnection> executor = newExecutor(owner, io, engine, policy);
            ScriptedReply blocked = new ScriptedReply(
                    List.of(ReplyReservationResult.WAITING, ReplyReservationResult.RESERVED),
                    false,
                    true
            );
            try {
                submit(executor, ExecutorCoreTestSupport.newConnection("a"),
                        TrackingExecutionRequest.ofUtf8("A"), blocked);
                submit(executor, ExecutorCoreTestSupport.newConnection("b"),
                        TrackingExecutionRequest.ofUtf8("B"), ScriptedReply.reserved());

                owner.runAll();
                Assert.assertEquals(policy == SchedulingPolicy.FAIR ? 1 : 0, bExecutes.get());
                Assert.assertEquals(1, aPrepares.get());
                Assert.assertEquals(0, aExecutes.get());
                Assert.assertEquals(0, aCloses.get());

                blocked.wake(10_000);
                Assert.assertEquals(1, owner.pendingTasks());
                owner.runAll();

                Assert.assertEquals(1, aPrepares.get());
                Assert.assertEquals(1, aExecutes.get());
                Assert.assertEquals(1, aCloses.get());
                Assert.assertEquals(1, bExecutes.get());
                Assert.assertEquals(1, blocked.readyCalls());
                Assert.assertEquals(0, blocked.cancelCalls());
            } finally {
                executor.close();
                owner.runAll();
            }
        }
    }

    @Test
    public void stalePreparationIsClosedAndReplacedBeforeExecution() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        AtomicInteger prepares = new AtomicInteger();
        AtomicInteger executes = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        CommandExecutor<TestConnection> executor = newExecutor(owner, io, (session, request) -> prepared(
                prepares.getAndIncrement() == 0 ? ValidationResult.STALE : ValidationResult.VALID,
                executes,
                closes
        ), SchedulingPolicy.FAIR);
        try {
            submit(executor, ExecutorCoreTestSupport.newConnection("stale"),
                    TrackingExecutionRequest.ofUtf8("INCR"), ScriptedReply.reserved());
            owner.runAll();

            Assert.assertEquals(2, prepares.get());
            Assert.assertEquals(1, executes.get());
            Assert.assertEquals(2, closes.get());
        } finally {
            executor.close();
            owner.runAll();
        }
    }

    @Test
    public void outputAndResultUnknownMarkerFailuresCloseWithoutReplay() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = ExecutorCoreTestSupport.newConnection("post-mutation");
        AtomicInteger prepares = new AtomicInteger();
        AtomicInteger mutations = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        CommandExecutor<TestConnection> executor = newExecutor(owner, io, (session, request) -> {
            prepares.incrementAndGet();
            return prepared(ValidationResult.VALID, mutations, closes);
        }, SchedulingPolicy.FAIR);
        ScriptedReply reply = new ScriptedReply(
                List.of(ReplyReservationResult.RESERVED),
                true,
                true,
                false,
                true
        );
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("SET");
        try {
            submit(executor, connection, request, reply);
            owner.runAll();
            owner.runAll();

            Assert.assertEquals(1, prepares.get());
            Assert.assertEquals(1, mutations.get());
            Assert.assertEquals(1, closes.get());
            Assert.assertEquals(1, reply.resultUnknownCalls());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(0, reply.readyCalls());
            Assert.assertEquals(1, io.closeCalls(connection));
            Assert.assertTrue(connection.context().isClosing());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
            Assert.assertEquals(0, connection.context().pending());
        } finally {
            executor.close();
            owner.runAll();
        }
    }

    @Test
    public void failedWaitRegistrationClosesALiveAdmittedConnection() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = ExecutorCoreTestSupport.newConnection("missing-waiter");
        AtomicInteger executes = new AtomicInteger();
        CommandExecutor<TestConnection> executor = newExecutor(
                owner,
                io,
                (session, request) -> prepared(
                        ValidationResult.VALID,
                        executes,
                        new AtomicInteger()
                ),
                SchedulingPolicy.FAIR
        );
        ScriptedReply reply = new ScriptedReply(
                List.of(ReplyReservationResult.WAITING),
                false,
                false
        );
        try {
            submit(executor, connection, TrackingExecutionRequest.ofUtf8("GET"), reply);
            owner.runAll();

            Assert.assertEquals(0, executes.get());
            Assert.assertTrue(connection.context().isClosing());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(1, io.closeCalls(connection));
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
        } finally {
            executor.close();
            owner.runAll();
        }
    }

    @Test
    public void registrationCancelFailureDoesNotStrandOtherOwnedState() {
        assertBlockedCleanupSurvives(true, false);
    }

    @Test
    public void preparedCloseFailureDoesNotStrandOtherOwnedState() {
        assertBlockedCleanupSurvives(false, true);
    }

    @Test
    public void everyTerminalCleanupFaultStillReleasesAllOtherOwnersOnce() {
        for (CleanupFault fault : CleanupFault.values()) {
            CleanupProbe probe = runTerminalCleanupFault(fault);

            Assert.assertEquals(fault.name(), 1, probe.waiterCancelCalls());
            Assert.assertEquals(fault.name(), 1, probe.preparedCloseCalls());
            Assert.assertEquals(fault.name(), 1, probe.requestCloseCalls());
            Assert.assertEquals(fault.name(), 1, probe.resultUnknownCalls());
            Assert.assertEquals(fault.name(), 1, probe.markClosingCalls());
            Assert.assertEquals(fault.name(), 1, probe.disableInputCalls());
            Assert.assertEquals(fault.name(), 1, probe.replyCancelCalls());
            Assert.assertEquals(fault.name(), 1, probe.transportCloseCalls());
            Assert.assertEquals(fault.name(), 1, probe.finishTaskCalls());
            Assert.assertEquals(fault.name(), 0, probe.executor().statsSnapshot().queuedTasks());
            Assert.assertEquals(fault.name(), 0L, probe.executor().statsSnapshot().queuedBytes());
            Assert.assertEquals(fault.name(), 0, probe.connection().context().pending());

            probe.closeAgainAndDrain();
            Assert.assertEquals(fault.name(), 1, probe.effectiveRequestReleases());
            Assert.assertEquals(fault.name(), 1, probe.effectiveReplyReleases());
            Assert.assertEquals(fault.name(), 1, probe.effectiveBacklogReleases());
        }
    }
```

Define `CleanupFault` with `WAITER_CANCEL`, `PREPARED_CLOSE`, `REQUEST_CLOSE`,
`RESULT_UNKNOWN`, `MARK_CLOSING`, `DISABLE_INPUT`, `REPLY_CANCEL`,
`TRANSPORT_CLOSE`, and `FINISH_TASK`. `runTerminalCleanupFault(...)` uses the
real executor state machine plus one throwing test double at the named callback;
all other doubles are non-throwing and non-idempotent invocation counters.
`CleanupProbe` exposes both invocation counts and effective-release counts so a
second close can prove idempotence without confusing "called twice" with
"released twice". Exercise the matrix once with a pre-mutation blocked task and
once with `executionStarted == true`; the post-mutation matrix requires exactly
one `markResultUnknown()` attempt and never re-prepares or re-executes.

Complete that test class with these concrete helpers:

```java
    private static CommandExecutor<TestConnection> newExecutor(
            ManualOwnerExecutor owner,
            RecordingIoAdapter io,
            CommandExecutionEngine engine,
            SchedulingPolicy policy
    ) {
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                owner,
                (session, shape) -> ReplyPlan.exact(64L, shape.retainedSourceBytes()),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 1_000, policy)
        );
        ExecutorCoreTestSupport.startExecutor(executor, owner);
        return executor;
    }

    private static void submit(
            CommandExecutor<TestConnection> executor,
            TestConnection connection,
            TrackingExecutionRequest request,
            ExecutionReply reply
    ) {
        ExecutorAdmissionAttempt<TestConnection> attempt =
                executor.tryAcquire(connection, request.retainedBytes());
        Assert.assertTrue(attempt instanceof ExecutorAdmissionAttempt.Acquired<TestConnection>);
        ((ExecutorAdmissionAttempt.Acquired<TestConnection>) attempt).admission().publish(request, reply);
    }

    private static PreparedCommand prepared(
            ValidationResult validation,
            AtomicInteger executes,
            AtomicInteger closes
    ) {
        return prepared(validation, executes, closes, false);
    }

    private static PreparedCommand prepared(
            ValidationResult validation,
            AtomicInteger executes,
            AtomicInteger closes,
            boolean failClose
    ) {
        return new PreparedCommand() {
            @Override
            public yier.bubu.redis.execution.api.ReplyShape replyShape() {
                return ReplyShapes.simpleString("OK");
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return validation;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                executes.incrementAndGet();
                context.reply().simpleString("OK");
            }

            @Override
            public void close() {
                closes.incrementAndGet();
                if (failClose) {
                    throw new IllegalStateException("injected prepared close failure");
                }
            }
        };
    }

    private static void assertBlockedCleanupSurvives(
            boolean failRegistrationCancel,
            boolean failPreparedClose
    ) {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = ExecutorCoreTestSupport.newConnection("cleanup-failure");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("GET");
        AtomicInteger preparedCloses = new AtomicInteger();
        CommandExecutor<TestConnection> executor = newExecutor(
                owner,
                io,
                (session, ignored) -> prepared(
                        ValidationResult.VALID,
                        new AtomicInteger(),
                        preparedCloses,
                        failPreparedClose
                ),
                SchedulingPolicy.FAIR
        );
        ScriptedReply reply = new ScriptedReply(
                List.of(ReplyReservationResult.WAITING),
                false,
                true,
                failRegistrationCancel
        );
        try {
            submit(executor, connection, request, reply);
            owner.runAll();

            io.fireClosed(connection);
            owner.runAll();

            Assert.assertEquals(1, reply.registrationCancelCalls());
            Assert.assertEquals(1, preparedCloses.get());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
            Assert.assertEquals(0, connection.context().pending());

            executor.close();
            owner.runAll();
            Assert.assertEquals(1, reply.registrationCancelCalls());
            Assert.assertEquals(1, preparedCloses.get());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(1, reply.cancelCalls());
        } finally {
            executor.close();
            owner.runAll();
        }
    }

    private static final class ScriptedReply implements ExecutionReply {
        private final ArrayDeque<ReplyReservationResult> reservations;
        private final boolean failWrites;
        private final boolean retainWaiter;
        private final boolean failRegistrationCancel;
        private final boolean failMarkResultUnknown;
        private final AtomicBoolean waiterActive = new AtomicBoolean();
        private final AtomicInteger readyCalls = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();
        private final AtomicInteger registrationCancelCalls = new AtomicInteger();
        private final AtomicInteger resultUnknownCalls = new AtomicInteger();
        private Runnable wakeup;
        private long writtenBytes;

        ScriptedReply(
                List<ReplyReservationResult> reservations,
                boolean failWrites,
                boolean retainWaiter
        ) {
            this(reservations, failWrites, retainWaiter, false, false);
        }

        ScriptedReply(
                List<ReplyReservationResult> reservations,
                boolean failWrites,
                boolean retainWaiter,
                boolean failRegistrationCancel
        ) {
            this(reservations, failWrites, retainWaiter, failRegistrationCancel, false);
        }

        ScriptedReply(
                List<ReplyReservationResult> reservations,
                boolean failWrites,
                boolean retainWaiter,
                boolean failRegistrationCancel,
                boolean failMarkResultUnknown
        ) {
            this.reservations = new ArrayDeque<>(reservations);
            this.failWrites = failWrites;
            this.retainWaiter = retainWaiter;
            this.failRegistrationCancel = failRegistrationCancel;
            this.failMarkResultUnknown = failMarkResultUnknown;
        }

        static ScriptedReply reserved() {
            return new ScriptedReply(List.of(ReplyReservationResult.RESERVED), false, true);
        }

        @Override
        public ReplyReservationResult tryReserve(ReplyPlan plan) {
            ReplyReservationResult result = reservations.peekFirst();
            if (reservations.size() > 1) {
                reservations.removeFirst();
            }
            return Objects.requireNonNull(result, "scripted reservation");
        }

        @Override
        public CapacityRegistration onCapacityAvailable(Runnable callback) {
            if (!retainWaiter) {
                return CapacityRegistration.NONE;
            }
            waiterActive.set(true);
            wakeup = () -> {
                if (waiterActive.compareAndSet(true, false)) {
                    callback.run();
                }
            };
            return () -> {
                registrationCancelCalls.incrementAndGet();
                if (waiterActive.compareAndSet(true, false)) {
                    if (failRegistrationCancel) {
                        throw new IllegalStateException("injected registration cancel failure");
                    }
                }
            };
        }

        @Override
        public BytesSink sink() {
            return (source, sourceIndex, length) -> {
                if (failWrites) {
                    throw new IllegalStateException("injected post-mutation write failure");
                }
                writtenBytes += length;
            };
        }

        @Override public void markReady(boolean closeAfterReply) { readyCalls.incrementAndGet(); }
        @Override public boolean hasWrittenBytes() { return writtenBytes > 0L; }
        @Override
        public void markResultUnknown() {
            resultUnknownCalls.incrementAndGet();
            if (failMarkResultUnknown) {
                throw new IllegalStateException("injected result-unknown marker failure");
            }
        }

        @Override
        public void cancel() {
            cancelCalls.incrementAndGet();
        }

        @Override public void close() { cancel(); }

        void wake(int attempts) {
            Assert.assertNotNull("capacity callback must be registered", wakeup);
            for (int index = 0; index < attempts; index++) {
                wakeup.run();
            }
        }

        int readyCalls() { return readyCalls.get(); }
        int cancelCalls() { return cancelCalls.get(); }
        int resultUnknownCalls() { return resultUnknownCalls.get(); }
        int registrationCancelCalls() { return registrationCancelCalls.get(); }
    }
}
```

The failing sink throws only after `executes.incrementAndGet()` and `PreparedCommand.execute(...)` enters reply rendering. Do not weaken the test by throwing before execution begins. Each cleanup-failure test closes the executor a second time and repeats the ownership assertions so a failed external cleanup cannot make later shutdown release any request, reply, waiter, prepared command, or backlog reservation twice.

- [ ] **Step 2: Extend blocked-task cleanup tests**

Add this parameterized cleanup test and helper to `ReplyCapacityBlockedSchedulingTest`:

```java
    @Test
    public void blockedPreparedTaskReleasesExactlyOnceOnDisconnectAndShutdown() {
        for (SchedulingPolicy policy : SchedulingPolicy.values()) {
            assertBlockedPreparedCleanup(policy, false);
            assertBlockedPreparedCleanup(policy, true);
        }
    }

    private static void assertBlockedPreparedCleanup(
            SchedulingPolicy policy,
            boolean shutdown
    ) {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = ExecutorCoreTestSupport.newConnection(
                policy + (shutdown ? "-shutdown" : "-disconnect"));
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("GET");
        AtomicInteger preparedCloses = new AtomicInteger();
        CommandExecutionEngine engine = (session, ignored) -> new PreparedCommand() {
            @Override public ReplyShape replyShape() { return ReplyShapes.bulkString(8L, 0L); }
            @Override public ValidationResult validateBeforeExecute() { return ValidationResult.VALID; }
            @Override public void execute(CommandExecutionContext context) {
                throw new AssertionError("capacity-blocked command must not execute");
            }
            @Override public void close() { preparedCloses.incrementAndGet(); }
        };
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                owner,
                (session, shape) -> ReplyPlan.exact(64L, shape.retainedSourceBytes()),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 1_000, policy)
        );
        BlockingReply reply = new BlockingReply(false);
        ExecutorCoreTestSupport.startExecutor(executor, owner);
        ExecutorAdmissionAttempt<TestConnection> attempt =
                executor.tryAcquire(connection, request.retainedBytes());
        ((ExecutorAdmissionAttempt.Acquired<TestConnection>) attempt)
                .admission().publish(request, reply);
        owner.runAll();

        if (shutdown) {
            CompletableFuture<Void> stopped = executor.shutdownGracefully();
            owner.runAll();
            Assert.assertTrue(stopped.isDone());
        } else {
            io.fireClosed(connection);
            owner.runAll();
        }

        Assert.assertEquals(1, request.closeCalls());
        Assert.assertEquals(1, preparedCloses.get());
        Assert.assertEquals(1, reply.cancelCalls());
        Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
        Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
        Assert.assertEquals(0, connection.context().pending());

        executor.close();
        owner.runAll();
        Assert.assertEquals(1, request.closeCalls());
        Assert.assertEquals(1, preparedCloses.get());
        Assert.assertEquals(1, reply.cancelCalls());
    }
```

Each helper call builds a fresh fixture, so disconnect and shutdown independently prove one terminal claim instead of sharing idempotent test doubles.

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor -am \
  -Dtest=PreparedCommandExecutionTest,ReplyCapacityBlockedSchedulingTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the provisional prepared flow does not distinguish stale re-preparation from capacity blocking, does not close a live connection when it cannot retain a waiter, and can route post-execution output failure through a retry-capable path.

- [ ] **Step 4: Give `CommandExecutorTask` one explicit prepared lifecycle**

Replace the provisional prepared fields from the command/runtime task with these fields and methods; retain the existing final connection/request/reply/backlog fields and constructor:

```java
    PreparedCommand prepared;
    ReplyPlan replyPlan;
    private CapacityRegistration capacityRegistration = CapacityRegistration.NONE;
    private boolean executionStarted;
    private boolean terminalCleanupClaimed;

    void installPreparation(PreparedCommand nextPrepared, ReplyPlan nextPlan) {
        if (prepared != null || replyPlan != null || executionStarted) {
            throw new IllegalStateException("task already owns a preparation");
        }
        prepared = Objects.requireNonNull(nextPrepared, "nextPrepared");
        replyPlan = Objects.requireNonNull(nextPlan, "nextPlan");
    }

    void discardStalePreparation() {
        if (executionStarted) {
            throw new IllegalStateException("cannot reprepare after execution started");
        }
        PreparedCommand stale = prepared;
        prepared = null;
        replyPlan = null;
        if (stale != null) {
            stale.close();
        }
    }

    void markExecutionStarted() {
        if (prepared == null || replyPlan == null || executionStarted) {
            throw new IllegalStateException("task is not ready to execute");
        }
        executionStarted = true;
    }

    boolean executionStarted() {
        return executionStarted;
    }

    void ownCapacityRegistration(CapacityRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        CapacityRegistration previous = capacityRegistration;
        capacityRegistration = registration;
        previous.cancel();
    }

    void capacityRegistrationSignalled() {
        capacityRegistration = CapacityRegistration.NONE;
    }

    boolean claimTerminalCleanup() {
        if (terminalCleanupClaimed) {
            return false;
        }
        terminalCleanupClaimed = true;
        return true;
    }

    void closeOwnedState() {
        CapacityRegistration registration = capacityRegistration;
        PreparedCommand ownedPrepared = prepared;
        capacityRegistration = CapacityRegistration.NONE;
        prepared = null;
        replyPlan = null;

        Throwable failure = null;
        try {
            registration.cancel();
        } catch (Throwable registrationFailure) {
            failure = registrationFailure;
        }
        try {
            if (ownedPrepared != null) {
                ownedPrepared.close();
            }
        } catch (Throwable preparedFailure) {
            if (failure == null) {
                failure = preparedFailure;
            } else if (failure != preparedFailure) {
                failure.addSuppressed(preparedFailure);
            }
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("task-owned cleanup failed", failure);
        }
    }
```

`claimTerminalCleanup()` is owner-thread confined and makes request closure, reply disposition, pending accounting, and backlog release a one-time transition. `closeOwnedState()` first detaches every field, then attempts registration and prepared cleanup independently; it preserves the first failure and suppresses the second. Its caller catches that aggregate only after both cleanup actions ran, then continues releasing the request, reply, and budgets.

Every cleanup method in this task follows the same detach-first rule. The
terminal path must attempt, in independent guarded blocks, task-owned waiter and
prepared cleanup, result-unknown marking when required, connection-closing
marking, input disable, reply cancellation, transport close, request close, and
pending/backlog accounting. A failure in one block is recorded or suppressed but
cannot skip a later block. Accounting is claimed before callbacks and released
from a `finally`-equivalent path so even a throwing observer cannot strand queue
slots or bytes. Do not rely on callback implementations being idempotent; the
task's `claimTerminalCleanup()` is the single exactly-once guard.

- [ ] **Step 5: Harden the existing re-preparation, reservation-waiting, and terminal-execution paths**

`Command/runtime` Task 3 already adds `REPREPARE` to `ExecutionAttempt` and the provisional owner-drain requeue. Retain that transition and replace its provisional handling with the guarded ownership behavior below; do not introduce a second enum value or a compatibility branch:

```java
public enum ExecutionAttempt {
    COMPLETED,
    REPREPARE,
    REPLY_CAPACITY_BLOCKED,
    CONNECTION_CLOSED
}
```

Replace the provisional prepared execution body in `CommandExecutorExecutionSupport` with this state transition. `replySizer` is the constructor field added by the command/runtime task:

```java
    private ExecutionAttempt executeWithReply(CommandExecutorTask<C> task) {
        C connection = task.connection;
        if (connection == null) {
            terminalCleanup(task, false, true);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }
        ExecutionConnectionContext context = connection.context();
        if (context.isClosing()) {
            context.recordSkippedClosing();
            commandsSkippedClosing.increment();
            terminalCleanup(task, false, true);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        try {
            if (task.prepared == null) {
                PreparedCommand prepared = commandProcessor.prepare(connection.session(), task.request);
                ReplyPlan plan;
                try {
                    plan = replySizer.plan(connection.session(), prepared.replyShape());
                } catch (Throwable sizingFailure) {
                    prepared.close();
                    throw sizingFailure;
                }
                task.installPreparation(prepared, plan);
            }

            ReplyReservationResult reservation = task.reply.tryReserve(task.replyPlan);
            switch (reservation) {
                case WAITING -> {
                    context.markInputPausedByReply();
                    backpressureController.disableAutoRead(connection);
                    return ExecutionAttempt.REPLY_CAPACITY_BLOCKED;
                }
                case TOO_LARGE, CLOSED -> {
                    closeAdmittedConnection(task, false);
                    return ExecutionAttempt.CONNECTION_CLOSED;
                }
                case RESERVED -> {
                }
            }

            if (task.prepared.validateBeforeExecute() == ValidationResult.STALE) {
                task.discardStalePreparation();
                return ExecutionAttempt.REPREPARE;
            }

            RedisReplyWriter writer = replyWriterFactory.newWriter(
                    connection.session(),
                    task.reply.sink()
            );
            task.markExecutionStarted();
            try (CommandExecutionContext commandContext = CommandExecutionContext.forRequest(
                    connection.session(),
                    writer,
                    task.request
            )) {
                task.prepared.execute(commandContext);
            }
            if (writer.closeAfterReplyRequested()) {
                context.recordCloseAfterReply();
                closeAfterReply.increment();
                connection.markClosing();
            }
            task.reply.markReady(writer.closeAfterReplyRequested());
            // markReady 成功后，reply 的 slot/lease 所有权转交给 sequencer。
            commandsExecuted.increment();
            terminalCleanup(task, true, false);
            return ExecutionAttempt.COMPLETED;
        } catch (Throwable failure) {
            if (task.executionStarted()) {
                commandsExecuted.increment();
                closeAdmittedConnection(task, true);
            } else {
                closeAdmittedConnection(task, false);
            }
            return ExecutionAttempt.CONNECTION_CLOSED;
        }
    }
```

Use these terminal helpers in the same class and make every old cleanup branch delegate to them:

```java
    void failLiveAdmittedTask(CommandExecutorTask<C> task) {
        if (task == null || task.connection == null || task.connection.context().isClosing()) {
            recycleAndRelease(task);
            return;
        }
        closeAdmittedConnection(task, task.executionStarted());
    }

    private void closeAdmittedConnection(CommandExecutorTask<C> task, boolean resultUnknown) {
        if (task == null || !task.claimTerminalCleanup()) {
            return;
        }
        C connection = task.connection;
        if (resultUnknown) {
            try {
                task.reply.markResultUnknown();
            } catch (Throwable ignored) {
            }
        }
        try {
            connection.markClosing();
        } catch (Throwable ignored) {
        }
        try {
            backpressureController.disableAutoRead(connection);
        } catch (Throwable ignored) {
        }
        try {
            task.reply.cancel();
        } catch (Throwable ignored) {
        }
        try {
            ioAdapter.closeConnection(connection);
        } catch (Throwable ignored) {
        }
        releaseClaimedTask(task, task.executionStarted(), false);
    }

    private void terminalCleanup(
            CommandExecutorTask<C> task,
            boolean executed,
            boolean cancelReply
    ) {
        if (task == null || !task.claimTerminalCleanup()) {
            return;
        }
        releaseClaimedTask(task, executed, cancelReply);
    }

    private void releaseClaimedTask(
            CommandExecutorTask<C> task,
            boolean executed,
            boolean cancelReply
    ) {
        try {
            task.closeOwnedState();
        } catch (Throwable ignored) {
        }
        try {
            task.request.close();
        } catch (Throwable ignored) {
        }
        if (cancelReply) {
            try {
                task.reply.cancel();
            } catch (Throwable ignored) {
            }
        }
        if (task.connection == null) {
            try {
                backlogBudget.release(task.retainedBytes);
            } catch (Throwable ignored) {
            }
            return;
        }
        try {
            task.connection.context().clearInputPausedByReply();
        } catch (Throwable ignored) {
        }
        try {
            finishTask(task.connection, task.retainedBytes, executed);
        } catch (Throwable ignored) {
        }
    }
```

Delete the old catch-and-retry branches for `ReplyCapacityUnavailableException`; pre-execution capacity is represented only by `ReplyReservationResult.WAITING`. Do not use `MutationContext.none()`. Do not catch a post-execution failure and render a replacement error into the same slot. A successful `markReady(...)` is the ownership-transfer point: the sequencer then owns the reply slot, lease, chunks, and retained reply sources, so `terminalCleanup(task, true, false)` releases only executor-owned state and must not call `cancel()` or `close()` on the reply.

- [ ] **Step 6: Preserve ordering while yielding stale and capacity-blocked heads**

In `CommandExecutorDrainLoop.drainLoop()`, handle the two non-terminal results independently:

```java
            ExecutionAttempt attempt = executionSupport.execute(task, touchedConnections);
            if (attempt == ExecutionAttempt.REPREPARE) {
                if (!taskQueue.block(task.connection, task)
                        || !taskQueue.resumeBlocked(task.connection, task)) {
                    executionSupport.failLiveAdmittedTask(task);
                }
            } else if (attempt == ExecutionAttempt.REPLY_CAPACITY_BLOCKED) {
                registerBlockedReplyTask(task);
            }
```

This reuses the queue's blocked-head representation: `GLOBAL` keeps the task at the global head; `FAIR` keeps it ahead of later work on the same connection while allowing another connection to run.

Replace the no-registration branch in `registerBlockedReplyTask(...)` with:

```java
        CapacityRegistration registration;
        try {
            registration = task.reply.onCapacityAvailable(() -> ownerExecutor.execute(() -> {
                task.capacityRegistrationSignalled();
                if (taskQueue.resumeBlocked(task.connection, task)) {
                    scheduleDrain();
                }
            }));
        } catch (Throwable ignored) {
            registration = CapacityRegistration.NONE;
        }
        if (registration == CapacityRegistration.NONE) {
            if (taskQueue.cancelBlocked(task.connection, task)) {
                executionSupport.failLiveAdmittedTask(task);
            }
            return;
        }
        task.ownCapacityRegistration(registration);
```

The connection-close callback and `drainLeftoverCommands()` call `recycleAndRelease(...)`, which first calls `task.closeOwnedState()`. A failed waiter registration therefore closes a live connection; disconnect and explicit shutdown may cancel without output because the connection is already terminal.

- [ ] **Step 7: Run focused and executor regression tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor -am \
  -Dtest=PreparedCommandExecutionTest,ReplyCapacityBlockedSchedulingTest,CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS for both scheduling policies; capacity waits preserve object identity, stale preparation yields without reordering, post-execution failures close exactly once, and all terminal paths return request/prepared/reply/backlog ownership.

- [ ] **Step 8: Commit the prepared-task lifecycle**

```bash
git add \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionAttempt.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/PreparedCommandExecutionTest.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java
git commit -m "fix: make prepared execution one shot"
```

---

### Task 3: Move Ordered Egress To Netty And Add Typed Fair Capacity Waiters

**Files:**
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundWaitKind.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundCapacityRegistration.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudget.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudgetStats.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundConnectionMemory.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryLease.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplySlot.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplySlotState.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplyCleanupOwner.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ConnectionReplySequencer.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplyEgressStats.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/BoundedChunkedReplySink.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudgetTest.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/ConnectionReplySequencerTest.java`
- Move/Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/BoundedChunkedReplySinkTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGate.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyPipelineTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyTestFixture.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/OutboundReplyPressureTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/ReplyResultUnknownTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/ProductionHardeningSoakTest.java`

**Interfaces:**
- Consumes: Task 1's shared `CapacityRegistration`, `ExecutionReply`, and `ReplyReservationResult`, plus the command/runtime task's final `ReplyPlan`.
- Produces: public Netty-owned outbound budget, connection account, lease, reply slot, sequencer, egress stats, and chunk sink types under `yier.bubu.redis.protocol.resp.netty`.
- Produces: `OutboundConnectionMemory.onControlCapacityAvailable(...)` and `OutboundMemoryLease.onAdditionalCapacityAvailable(...)`, both returning `Optional<OutboundCapacityRegistration>`.
- Fairness rule: the budget grants at most one retry at a time; a connection selects its lowest-sequence live lease expansion before its control waiter, and each successful grant rotates that connection behind all other waiting connections.

- [ ] **Step 1: Add RED tests for typed waiter coexistence and fairness**

Move `OutboundMemoryBudgetTest` to the Netty module/package, preserve its accounting tests, and add these tests:

```java
    @Test
    public void oneConnectionRetainsControlAndPerLeaseExpansionWaitersTogether() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory connection = budget.openConnection(400L);
        OutboundConnectionMemory holder = budget.openConnection(400L);
        OutboundMemoryLease admitted = connection.reserve(100L, 400L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(300L, 400L).orElseThrow();
        List<String> wakeups = new ArrayList<>();

        OutboundCapacityRegistration control = connection.onControlCapacityAvailable(
                100L, 400L, () -> wakeups.add("control")
        ).orElseThrow();
        OutboundCapacityRegistration expansion = admitted.onAdditionalCapacityAvailable(
                100L, 400L, () -> wakeups.add("expansion")
        ).orElseThrow();

        Assert.assertEquals(OutboundWaitKind.CONTROL_ADMISSION, control.kind());
        Assert.assertEquals(OutboundWaitKind.LEASE_EXPANSION, expansion.kind());
        Assert.assertTrue(control.active());
        Assert.assertTrue(expansion.active());
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());
        Assert.assertEquals(1, budget.stats().expansionWaiters());
        Assert.assertEquals(2, budget.stats().totalWaiters());

        pressure.close();
        Assert.assertEquals(List.of("expansion"), wakeups);
        Assert.assertTrue(admitted.tryReserveAdditional(100L, 400L));
        Assert.assertEquals(List.of("expansion", "control"), wakeups);
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());
        Assert.assertEquals(0, budget.stats().expansionWaiters());

        OutboundMemoryLease controlLease = connection.reserve(100L, 400L).orElseThrow();
        Assert.assertFalse(control.active());
        Assert.assertFalse(expansion.active());
        controlLease.close();
        admitted.close();
        holder.close();
        connection.close();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void earliestAdmittedExpansionBeatsAnOlderControlWait() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(300L);
        OutboundConnectionMemory connection = budget.openConnection(300L);
        OutboundConnectionMemory holder = budget.openConnection(300L);
        OutboundMemoryLease admitted = connection.reserve(100L, 300L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(200L, 300L).orElseThrow();
        List<String> wakeups = new ArrayList<>();

        connection.onControlCapacityAvailable(50L, 300L, () -> wakeups.add("control"))
                .orElseThrow();
        admitted.onAdditionalCapacityAvailable(50L, 300L, () -> wakeups.add("expansion"))
                .orElseThrow();
        pressure.close();

        Assert.assertEquals(List.of("expansion"), wakeups);
        Assert.assertTrue(admitted.tryReserveAdditional(50L, 300L));
        Assert.assertEquals(List.of("expansion", "control"), wakeups);

        connection.reserve(50L, 300L).orElseThrow().close();
        admitted.close();
        holder.close();
        connection.close();
    }

    @Test
    public void grantsRotateAcrossConnectionsAfterAdmittedProgress() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(300L);
        OutboundConnectionMemory a = budget.openConnection(300L);
        OutboundConnectionMemory b = budget.openConnection(300L);
        OutboundConnectionMemory holder = budget.openConnection(300L);
        OutboundMemoryLease a1 = a.reserve(50L, 300L).orElseThrow();
        OutboundMemoryLease a2 = a.reserve(50L, 300L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(200L, 300L).orElseThrow();
        List<String> wakeups = new ArrayList<>();

        a1.onAdditionalCapacityAvailable(50L, 300L, () -> wakeups.add("a1")).orElseThrow();
        a2.onAdditionalCapacityAvailable(50L, 300L, () -> wakeups.add("a2")).orElseThrow();
        b.onControlCapacityAvailable(50L, 300L, () -> wakeups.add("b")).orElseThrow();

        Assert.assertEquals(2, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());
        Assert.assertEquals(2, budget.stats().expansionWaiters());
        Assert.assertEquals(3, budget.stats().totalWaiters());

        pressure.close();
        Assert.assertEquals(List.of("a1"), wakeups);
        Assert.assertTrue(a1.tryReserveAdditional(50L, 300L));
        Assert.assertEquals(List.of("a1", "b"), wakeups);
        OutboundMemoryLease b1 = b.reserve(50L, 300L).orElseThrow();
        Assert.assertEquals(List.of("a1", "b", "a2"), wakeups);
        Assert.assertTrue(a2.tryReserveAdditional(50L, 300L));

        b1.close();
        a1.close();
        a2.close();
        holder.close();
        a.close();
        b.close();
    }

    @Test
    public void throwingGrantCallbackDoesNotBlockTheNextConnection() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(200L);
        OutboundConnectionMemory a = budget.openConnection(200L);
        OutboundConnectionMemory b = budget.openConnection(200L);
        OutboundConnectionMemory holder = budget.openConnection(200L);
        OutboundMemoryLease pressure = holder.reserve(200L, 200L).orElseThrow();
        List<String> callbacks = new ArrayList<>();

        a.onControlCapacityAvailable(50L, 200L, () -> {
            callbacks.add("a");
            throw new IllegalStateException("injected callback failure");
        }).orElseThrow();
        b.onControlCapacityAvailable(50L, 200L, () -> callbacks.add("b")).orElseThrow();

        pressure.close();

        Assert.assertEquals(List.of("a", "b"), callbacks);
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());
        OutboundMemoryLease admitted = b.reserve(50L, 200L).orElseThrow();
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(0, budget.stats().totalWaiters());

        admitted.close();
        holder.close();
        a.close();
        b.close();
    }

    @Test
    public void grantedRegistrationCanBeCancelledWithoutBlockingTheNextConnection() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(200L);
        OutboundConnectionMemory a = budget.openConnection(200L);
        OutboundConnectionMemory b = budget.openConnection(200L);
        OutboundConnectionMemory holder = budget.openConnection(200L);
        OutboundMemoryLease pressure = holder.reserve(200L, 200L).orElseThrow();
        AtomicReference<OutboundCapacityRegistration> aRegistration = new AtomicReference<>();
        List<String> callbacks = new ArrayList<>();

        aRegistration.set(a.onControlCapacityAvailable(
                50L, 200L, () -> callbacks.add("a")
        ).orElseThrow());
        b.onControlCapacityAvailable(50L, 200L, () -> callbacks.add("b")).orElseThrow();

        pressure.close();
        Assert.assertEquals(List.of("a"), callbacks);
        Assert.assertEquals(2, budget.stats().waitingConnections());
        Assert.assertEquals(2, budget.stats().controlWaiters());

        aRegistration.get().cancel();
        Assert.assertEquals(List.of("a", "b"), callbacks);
        Assert.assertFalse(aRegistration.get().active());
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());

        b.reserve(50L, 200L).orElseThrow().close();
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(0, budget.stats().totalWaiters());
        holder.close();
        a.close();
        b.close();
    }

    @Test
    public void closingConnectionCancelsAllItsWaitersAndHandsOffTheGrant() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(300L);
        OutboundConnectionMemory closing = budget.openConnection(300L);
        OutboundConnectionMemory next = budget.openConnection(300L);
        OutboundConnectionMemory holder = budget.openConnection(300L);
        OutboundMemoryLease admitted = closing.reserve(50L, 300L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(250L, 300L).orElseThrow();
        List<String> callbacks = new ArrayList<>();

        closing.onControlCapacityAvailable(50L, 300L, () -> callbacks.add("control"))
                .orElseThrow();
        admitted.onAdditionalCapacityAvailable(50L, 300L, () -> callbacks.add("expansion"))
                .orElseThrow();
        next.onControlCapacityAvailable(50L, 300L, () -> callbacks.add("next"))
                .orElseThrow();

        pressure.close();
        Assert.assertEquals(List.of("expansion"), callbacks);
        closing.close();
        Assert.assertEquals(List.of("expansion", "next"), callbacks);
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(1, budget.stats().controlWaiters());
        Assert.assertEquals(0, budget.stats().expansionWaiters());

        next.reserve(50L, 300L).orElseThrow().close();
        admitted.close();
        holder.close();
        next.close();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
        Assert.assertEquals(0, budget.stats().totalWaiters());
    }

    @Test
    public void oneLeaseCannotOwnTwoExpansionWaiters() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(200L);
        OutboundConnectionMemory connection = budget.openConnection(200L);
        OutboundConnectionMemory holder = budget.openConnection(200L);
        OutboundMemoryLease lease = connection.reserve(100L, 200L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(100L, 200L).orElseThrow();
        lease.onAdditionalCapacityAvailable(1L, 200L, () -> { }).orElseThrow();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> lease.onAdditionalCapacityAvailable(1L, 200L, () -> { })
        );
        lease.close();
        pressure.close();
        holder.close();
        connection.close();
    }

    @Test
    public void replySlotInstallsWaitOwnershipBeforeAZeroLatencyGrantCanRetry() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(200L);
        OutboundConnectionMemory connection = budget.openConnection(200L);
        OutboundMemoryLease lease = connection.reserve(100L, 200L).orElseThrow();
        EmbeddedChannel channel = new EmbeddedChannel();
        ReplySlot slot = newReplySlot(channel, connection, lease);
        AtomicInteger wakeups = new AtomicInteger();

        slot.expectAdditionalCapacity(50L, 200L);
        CapacityRegistration registration = slot.onCapacityAvailable(wakeups::incrementAndGet);

        Assert.assertNotSame(CapacityRegistration.NONE, registration);
        Assert.assertEquals(0, wakeups.get());
        Assert.assertEquals(1, budget.stats().expansionWaiters());
        channel.runPendingTasks();
        Assert.assertEquals(1, wakeups.get());
        registration.cancel();
        Assert.assertEquals(0, budget.stats().expansionWaiters());

        slot.cancel();
        channel.finishAndReleaseAll();
        connection.close();
        budget.close();
    }
```

`newReplySlot(...)` is the existing real-slot fixture extended to keep the real
sequencer. `OutboundMemoryBudget` may invoke a
grant signal synchronously after releasing its lock, but a signal is never the
retry itself. `ReplySlot.onCapacityAvailable(...)` passes
`() -> sequencer.scheduleLaterOnEventLoop(wakeup)` to the lease, and the gate uses
`() -> ctx.executor().execute(resume)` for control admission. Thus the returned
registration and matching pause token are installed before the later event-loop
turn can retry. No budget callback may call `tryReserveAdditional(...)`, decoder
resume, or executor drain directly.

Add this `ConnectionReplySequencerTest` case. The callbacks enqueue their real owner actions so the test distinguishes a capacity signal from admission/execution itself:

```java
    @Test
    public void admittedHeadProgressesBeforeSameConnectionControlAdmission() {
        EmbeddedChannel channel = new EmbeddedChannel();
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory connection = budget.openConnection(400L);
        OutboundConnectionMemory holder = budget.openConnection(400L);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(
                channel, connection, () -> { }, slot -> {
                    throw new AssertionError("test installs chunks directly");
                });
        OutboundMemoryLease headLease = connection.reserve(100L, 400L).orElseThrow();
        OutboundMemoryLease secondLease = connection.reserve(100L, 400L).orElseThrow();
        OutboundMemoryLease pressure = holder.reserve(200L, 400L).orElseThrow();
        ReplySlot head = sequencer.register(headLease).orElseThrow();
        ReplySlot second = sequencer.register(secondLease).orElseThrow();
        ArrayDeque<Runnable> executorWakeups = new ArrayDeque<>();
        ArrayDeque<Runnable> controlWakeups = new ArrayDeque<>();
        AtomicReference<ReplySlot> third = new AtomicReference<>();

        headLease.onAdditionalCapacityAvailable(50L, 400L, () -> executorWakeups.add(() -> {
            Assert.assertTrue(headLease.tryReserveAdditional(50L, 400L));
            head.addChunk(Unpooled.copiedBuffer("head", StandardCharsets.US_ASCII));
            head.markReady(false);
        })).orElseThrow();
        connection.onControlCapacityAvailable(50L, 400L, () -> controlWakeups.add(() -> {
            OutboundMemoryLease lease = connection.reserve(50L, 400L).orElseThrow();
            third.set(sequencer.register(lease).orElseThrow());
        })).orElseThrow();

        pressure.close();
        Assert.assertEquals(1, executorWakeups.size());
        Assert.assertTrue(controlWakeups.isEmpty());
        executorWakeups.removeFirst().run();
        channel.runPendingTasks();

        ByteBuf written = channel.readOutbound();
        Assert.assertEquals("head", written.toString(StandardCharsets.US_ASCII));
        written.release();
        Assert.assertNotEquals(ReplySlotState.CANCELLED, head.state());
        Assert.assertNotEquals(ReplySlotState.CANCELLED, second.state());
        Assert.assertNull(third.get());
        Assert.assertEquals(1, controlWakeups.size());

        controlWakeups.removeFirst().run();
        Assert.assertEquals(2L, third.get().sequence());
        sequencer.close();
        channel.finishAndReleaseAll();
        holder.close();
        budget.close();
    }
```

- [ ] **Step 2: Run the outbound RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty -am \
  -Dtest=OutboundMemoryBudgetTest,ConnectionReplySequencerTest,BoundedChunkedReplySinkTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because outbound owners still live in `server-main`, a connection still has one untyped waiter slot, and waiter cancellation is connection-wide.

- [ ] **Step 3: Move the outbound ownership files without compatibility wrappers**

Move the production and test files, then change every moved package declaration to `yier.bubu.redis.protocol.resp.netty`:

```bash
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudget.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudget.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryBudgetStats.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudgetStats.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundConnectionMemory.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundConnectionMemory.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/OutboundMemoryLease.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryLease.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlot.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplySlot.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplySlotState.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplySlotState.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplyCleanupOwner.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplyCleanupOwner.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplySequencer.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ConnectionReplySequencer.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ReplyEgressStats.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplyEgressStats.java
git mv yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/BoundedChunkedReplySink.java yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/BoundedChunkedReplySink.java
git mv yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OutboundMemoryBudgetTest.java yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/OutboundMemoryBudgetTest.java
git mv yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionReplySequencerTest.java yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/ConnectionReplySequencerTest.java
git mv yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/BoundedChunkedReplySinkTest.java yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/BoundedChunkedReplySinkTest.java
```

Make `OutboundMemoryBudget`, `OutboundMemoryBudgetStats`, `OutboundConnectionMemory`, `OutboundMemoryLease`, `ReplySlot`, `ConnectionReplySequencer`, and `ReplyEgressStats` public. Make the constructors/factories and snapshot methods used by `server-main` public. Keep `ReplySlotState`, `ReplyCleanupOwner`, chunk records, cleanup owners, sink internals, and waiter implementation package-private. Update `server-main` and integration-test imports directly; do not leave forwarding types in `yier.bubu.redis.app.server`.

Expand the moved stats record so connection cardinality cannot be mistaken for waiter cardinality:

```java
public record OutboundMemoryBudgetStats(
        long capacityBytes,
        long reservedBytes,
        long allocatedBytes,
        long peakReservedBytes,
        long peakAllocatedBytes,
        long capacityRejectedReservations,
        int waitingConnections,
        int controlWaiters,
        int expansionWaiters,
        int activeConnections,
        long activeSlots,
        boolean closed
) {
    public int totalWaiters() {
        return Math.addExact(controlWaiters, expansionWaiters);
    }
}
```

Migrate `NettyServerInfoProvider` and `ProductionHardeningSoakTest` to `capacityRejectedReservations()` and delete the old `rejectedReservations()` and `capacityRejects()` aliases; this breaking rewrite does not retain metric-accessor adapters.

- [ ] **Step 4: Define the typed waiter handles**

Create `OutboundWaitKind.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

public enum OutboundWaitKind {
    CONTROL_ADMISSION,
    LEASE_EXPANSION
}
```

Create `OutboundCapacityRegistration.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import yier.bubu.redis.execution.api.CapacityRegistration;

public interface OutboundCapacityRegistration extends CapacityRegistration {
    OutboundWaitKind kind();

    boolean active();
}
```

Change the public wait surfaces to return owned registrations:

```java
// OutboundConnectionMemory
public Optional<OutboundCapacityRegistration> onControlCapacityAvailable(
        long bytes,
        long singleReplyLimitBytes,
        Runnable callback
) {
    return budget.registerControlWaiter(this, bytes, singleReplyLimitBytes, callback);
}

// OutboundMemoryLease
public Optional<OutboundCapacityRegistration> onAdditionalCapacityAvailable(
        long bytes,
        long singleReplyLimitBytes,
        Runnable callback
) {
    return budget.registerExpansionWaiter(this, bytes, singleReplyLimitBytes, callback);
}
```

Delete `OutboundConnectionMemory.awaitCapacity()`, `cancelWaiter()`, `OutboundMemoryLease.awaitAdditionalCapacity()`, and `cancelAdditionalCapacityWaiter()`. Closing a returned registration is the only waiter-cancellation API.

- [ ] **Step 5: Replace the one-waiter map with a lease-aware connection ring**

Replace `OutboundMemoryBudget.waiters`, `waitersByConnection`, and the old `Waiter` with these fields and state types:

```java
    private final ArrayDeque<OutboundConnectionMemory> waiterRing = new ArrayDeque<>();
    private final Map<OutboundConnectionMemory, ConnectionWaiters> waitsByConnection =
            new IdentityHashMap<>();
    private WaitRegistration grantedWaiter;
    private long nextLeaseSequence;

    private static final class ConnectionWaiters {
        private WaitRegistration control;
        private final java.util.TreeMap<Long, WaitRegistration> expansions = new java.util.TreeMap<>();
        private boolean inRing;

        private boolean empty() {
            return control == null && expansions.isEmpty();
        }

        private WaitRegistration priorityWaiter() {
            return expansions.isEmpty() ? control : expansions.firstEntry().getValue();
        }
    }

    private record WaiterCounts(int connections, int control, int expansions) {
    }

    private final class WaitRegistration implements OutboundCapacityRegistration {
        private final OutboundConnectionMemory connection;
        private final OutboundMemoryLease lease;
        private final OutboundWaitKind kind;
        private final long bytes;
        private final long singleReplyLimitBytes;
        private final Runnable callback;
        private volatile boolean active = true;
        private boolean granted;

        private WaitRegistration(
                OutboundConnectionMemory connection,
                OutboundMemoryLease lease,
                OutboundWaitKind kind,
                long bytes,
                long singleReplyLimitBytes,
                Runnable callback
        ) {
            this.connection = connection;
            this.lease = lease;
            this.kind = kind;
            this.bytes = bytes;
            this.singleReplyLimitBytes = singleReplyLimitBytes;
            this.callback = callback;
        }

        @Override public OutboundWaitKind kind() { return kind; }
        @Override public boolean active() { return active; }
        @Override public void cancel() { cancelRegistration(this); }
    }
```

Build each snapshot from the registrations, not from connection-map size alone:

```java
    public OutboundMemoryBudgetStats stats() {
        synchronized (lock) {
            WaiterCounts waiters = waiterCountsLocked();
            return new OutboundMemoryBudgetStats(
                    capacityBytes,
                    reservedBytes,
                    allocatedBytes,
                    peakReservedBytes,
                    peakAllocatedBytes,
                    capacityRejectedReservations,
                    waiters.connections(),
                    waiters.control(),
                    waiters.expansions(),
                    activeConnections,
                    activeSlots,
                    closed
            );
        }
    }

    private WaiterCounts waiterCountsLocked() {
        int connections = 0;
        int control = 0;
        int expansions = 0;
        for (ConnectionWaiters state : waitsByConnection.values()) {
            int liveExpansions = Math.toIntExact(state.expansions.values().stream()
                    .filter(WaitRegistration::active)
                    .count());
            int liveControl = state.control != null && state.control.active() ? 1 : 0;
            if (liveControl + liveExpansions > 0) {
                connections++;
                control += liveControl;
                expansions = Math.addExact(expansions, liveExpansions);
            }
        }
        return new WaiterCounts(connections, control, expansions);
    }
```

Assign every new lease a monotonic sequence under the budget lock:

```java
            reserveLocked(connection, bytes);
            lease = new OutboundMemoryLease(this, connection, bytes, nextLeaseSequence++);
```

Add `long admissionSequence()` to `OutboundMemoryLease` as a package-private accessor. Use overflow-safe unsigned ordering by rejecting creation after `nextLeaseSequence == Long.MAX_VALUE`; a process cannot silently wrap lease identity.

Implement the two registration methods as follows:

```java
    Optional<OutboundCapacityRegistration> registerControlWaiter(
            OutboundConnectionMemory connection,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        return registerWaiter(connection, null, OutboundWaitKind.CONTROL_ADMISSION,
                bytes, singleReplyLimitBytes, callback);
    }

    Optional<OutboundCapacityRegistration> registerExpansionWaiter(
            OutboundMemoryLease lease,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        Objects.requireNonNull(lease, "lease");
        return registerWaiter(lease.connection(), lease, OutboundWaitKind.LEASE_EXPANSION,
                bytes, singleReplyLimitBytes, callback);
    }

    private Optional<OutboundCapacityRegistration> registerWaiter(
            OutboundConnectionMemory connection,
            OutboundMemoryLease lease,
            OutboundWaitKind kind,
            long bytes,
            long singleReplyLimitBytes,
            Runnable callback
    ) {
        validateReservationArguments(bytes, singleReplyLimitBytes);
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(callback, "callback");
        WaitRegistration granted;
        WaitRegistration registration;
        synchronized (lock) {
            requireOwner(connection);
            if (closed || connection.closed() || (lease != null && lease.closed())) {
                return Optional.empty();
            }
            requireAttached(connection);
            if (!canEverFit(connection, lease, bytes, singleReplyLimitBytes)) {
                capacityRejectedReservations = saturatedAdd(capacityRejectedReservations, 1L);
                return Optional.empty();
            }
            ConnectionWaiters state = waitsByConnection.computeIfAbsent(
                    connection, ignored -> new ConnectionWaiters());
            if (kind == OutboundWaitKind.CONTROL_ADMISSION && state.control != null) {
                throw new IllegalStateException("connection already owns a control-admission waiter");
            }
            if (kind == OutboundWaitKind.LEASE_EXPANSION
                    && state.expansions.containsKey(lease.admissionSequence())) {
                throw new IllegalStateException("lease already owns an expansion waiter");
            }
            registration = new WaitRegistration(
                    connection, lease, kind, bytes, singleReplyLimitBytes, callback);
            if (kind == OutboundWaitKind.CONTROL_ADMISSION) {
                state.control = registration;
            } else {
                state.expansions.put(lease.admissionSequence(), registration);
            }
            enqueueConnectionLocked(connection, state);
            granted = grantOneWaiterLocked();
        }
        invokeGrant(granted);
        return Optional.of(registration);
    }
```

`canEverFit(...)` uses `fitsSingle(bytes, limit)` for control and `fitsWithin(lease.reservedBytes(), bytes, limit)` for expansion, then checks the connection/global hard capacities without considering current reservations.

Use this grant loop; it encodes both local priority and global rotation:

```java
    private WaitRegistration grantOneWaiterLocked() {
        if (grantedWaiter != null) {
            return null;
        }
        int candidates = waiterRing.size();
        while (candidates-- > 0) {
            OutboundConnectionMemory connection = waiterRing.removeFirst();
            ConnectionWaiters state = waitsByConnection.get(connection);
            if (state == null) {
                continue;
            }
            state.inRing = false;
            pruneClosedWaitersLocked(connection, state);
            if (state.empty()) {
                waitsByConnection.remove(connection);
                continue;
            }
            enqueueConnectionLocked(connection, state);
            WaitRegistration candidate = state.priorityWaiter();
            if (!currentlyFits(candidate)) {
                continue;
            }
            candidate.granted = true;
            grantedWaiter = candidate;
            return candidate;
        }
        return null;
    }

    private void enqueueConnectionLocked(
            OutboundConnectionMemory connection,
            ConnectionWaiters state
    ) {
        if (!state.inRing && !state.empty()) {
            waiterRing.addLast(connection);
            state.inRing = true;
        }
    }

    private void invokeGrant(WaitRegistration initialGrant) {
        WaitRegistration grant = initialGrant;
        while (grant != null) {
            try {
                grant.callback.run();
                return;
            } catch (Throwable callbackFailure) {
                synchronized (lock) {
                    if (grantedWaiter != grant) {
                        return;
                    }
                    removeRegistrationLocked(grant);
                    grant = closed ? null : grantOneWaiterLocked();
                }
            }
        }
    }
```

`currentlyFits(...)` applies the existing single-reply, per-connection, and global checks. `pruneClosedWaitersLocked(...)` removes inactive registrations, a closed control connection, and closed expansion leases. `removeRegistrationLocked(...)` sets the removed registration's `active = false` and `granted = false`, removes only its matching control or lease-expansion entry, and clears `grantedWaiter` when identities match. Therefore a throwing callback relinquishes the global grant before `invokeGrant(...)` selects and invokes the next eligible connection.

A granted registration remains present and counted in `waitingConnections`,
`controlWaiters`, or `expansionWaiters` until exactly one of these transitions:
its matching `reserve(...)`/`tryReserveAdditional(...)` consumes the grant,
`cancel()` removes it, its lease or connection closes, the budget closes, or its
callback throws. Each transition clears `grantedWaiter` and selects the next
eligible registration before leaving the budget operation. A callback that
returns successfully has only signalled its owner; it must later consume or
cancel the same registration. Gate and slot callbacks therefore enqueue a later
event-loop retry, and their close paths cancel the stored registration so a
signalled but abandoned retry cannot block global progress.

Update `reserve(...)` and `expandLease(...)` so a reservation can consume only its matching granted waiter while `grantedWaiter != null`. A fresh request cannot barge ahead of a granted retry. For `expandLease(...)`, an older expansion on the same connection also blocks a later lease; a pending control waiter does not block a fresh admitted-lease expansion. After a successful reservation, call:

```java
    private void consumeGrantedWaiterLocked(WaitRegistration registration) {
        if (registration == null) {
            return;
        }
        if (grantedWaiter != registration || !registration.granted || !registration.active) {
            throw new IllegalStateException("capacity retry does not own the current grant");
        }
        removeRegistrationLocked(registration);
    }
```

Replace every old `Runnable callback` local in reserve, expand, registration, cancellation, lease close, connection close, and budget-close paths with a `WaitRegistration grant`; call `grantOneWaiterLocked()` before leaving the lock and `invokeGrant(grant)` afterward. `cancelRegistration(...)`, lease close, connection close, and budget close remove only the targeted registration(s), never another lease's waiter. Each removal sets `active = false` exactly once and grants the next eligible connection when appropriate.

- [ ] **Step 6: Make reply reservation an explicit result instead of an exception retry**

In the moved `BoundedChunkedReplySink`, remove `ReplyReservationSink` from the implements list and replace `require(...)` with a package-private reservation method:

```java
    ReplyReservationResult tryReserve(ReplyPlan requestedPlan) {
        AtomicReference<ReplyReservationResult> result = new AtomicReference<>();
        slot.runProducerAction(() -> result.set(tryReserveLocked(requestedPlan)));
        return result.get();
    }

    private ReplyReservationResult tryReserveLocked(ReplyPlan requestedPlan) {
        Objects.requireNonNull(requestedPlan, "requestedPlan");
        ensureOpen();
        if (writtenBytes != 0L) {
            throw new IllegalStateException("reply reservation must precede output");
        }
        long target = reservationTarget(requestedPlan);
        if (target > maxTotalBytes) {
            recordTooLarge();
            return ReplyReservationResult.TOO_LARGE;
        }
        long currentReservation = slot.lease().reservedBytes();
        if (target > currentReservation
                && !slot.lease().tryReserveAdditional(target - currentReservation, maxTotalBytes)) {
            slot.expectAdditionalCapacity(target - currentReservation, maxTotalBytes);
            slot.markWaitingForCapacity();
            return ReplyReservationResult.WAITING;
        }
        slot.clearCapacityWaitAfterReservation();
        plan = requestedPlan;
        return ReplyReservationResult.RESERVED;
    }
```

The stale re-preparation path may call this method again before any bytes are written. Never shrink the lease: replace `plan` with the new plan and reserve only a positive delta, so a smaller replacement is safely over-reserved and a larger replacement expands monotonically.

In `ReplySlot`, keep the concrete sink internally and implement the shared reply contract directly:

```java
    @Override
    public ReplyReservationResult tryReserve(ReplyPlan plan) {
        synchronized (contentLock) {
            if (cleanupOwner.get() != ReplyCleanupOwner.NONE || isTerminal(state.get())) {
                return ReplyReservationResult.CLOSED;
            }
            return sinkInternal().tryReserve(Objects.requireNonNull(plan, "plan"));
        }
    }

    @Override
    public CapacityRegistration onCapacityAvailable(Runnable wakeup) {
        CapacityWait waiting = capacityWait.get();
        if (waiting == null || cleanupOwner.get() != ReplyCleanupOwner.NONE || isTerminal(state.get())) {
            return CapacityRegistration.NONE;
        }
        Optional<OutboundCapacityRegistration> retained = lease.onAdditionalCapacityAvailable(
                waiting.additionalBytes(),
                waiting.singleReplyLimitBytes(),
                () -> sequencer.scheduleLaterOnEventLoop(wakeup)
        );
        if (retained.isEmpty()) {
            return CapacityRegistration.NONE;
        }
        OutboundCapacityRegistration registration = retained.get();
        OutboundCapacityRegistration previous = expansionRegistration.getAndSet(registration);
        if (previous != null) {
            previous.cancel();
        }
        return registration;
    }
```

Add this package-private helper to `ConnectionReplySequencer`; unlike its normal
ready/drain helper, it always submits a later event-loop task even when called
from that event loop:

```java
    void scheduleLaterOnEventLoop(Runnable action) {
        try {
            channel.eventLoop().execute(Objects.requireNonNull(action, "action"));
        } catch (Throwable schedulingFailure) {
            failAfterEventLoopRejection(schedulingFailure);
        }
    }
```

`ReplySlot.onCapacityAvailable(...)` uses this helper around the executor
wakeup. The returned registration is consequently stored before a zero-latency
budget grant can resume the blocked executor task.

Add `AtomicReference<OutboundCapacityRegistration> expansionRegistration`. Rename `waitForAdditionalCapacity(...)` to `expectAdditionalCapacity(...)`. `clearCapacityWaitAfterReservation()` and terminal cleanup cancel and clear only this slot's expansion registration. Remove every import and use of `ReplyCapacityUnavailableException`, then delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyCapacityUnavailableException.java`.

- [ ] **Step 7: Update Netty composition and moved-type consumers**

Import the moved public types from `yier.bubu.redis.protocol.resp.netty` in `NettyReplyDecodedMessageGate`, `YierdisServerChannelInitializer`, `YierdisServerBootstrap`, `NettyServerInfoProvider`, their tests, and protocol integration tests. The construction remains structurally identical:

```java
OutboundConnectionMemory outboundConnection = outboundMemoryBudget.openConnection(
        config.replyPerConnectionCapacityBytes()
);
ConnectionReplySequencer replySequencer = new ConnectionReplySequencer(
        ch,
        outboundConnection,
        inboundReadCredit::pauseIngress,
        slot -> BoundedChunkedReplySink.forChannel(
                slot,
                ch,
                config.replyChunkPayloadBytes(),
                config.replyControlReservationBytes(),
                config.replyMaxTotalBytes(),
                resource -> closeReplyResourceOnOwner(executor, resource)
        ),
        replyEgressStats
);
```

This snippet is transitional until Task 4 replaces `pauseIngress` with a `CLOSING` token. Do not recreate package-private egress helpers in `server-main` to avoid changing imports.

- [ ] **Step 8: Run moved-unit and server integration tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=OutboundMemoryBudgetTest,ConnectionReplySequencerTest,BoundedChunkedReplySinkTest,OrderedReplyPipelineTest,OutboundReplyPressureTest,ReplyResultUnknownTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; one connection can retain both waiter kinds, expansion priority cannot delete a live slot, grants rotate across connections, and all moved reply ownership counters return to zero.

- [ ] **Step 9: Verify package ownership and obsolete capacity signaling**

Run:

```bash
rg -n 'class (OutboundMemoryBudget|OutboundConnectionMemory|OutboundMemoryLease|ReplySlot|ConnectionReplySequencer|BoundedChunkedReplySink)|enum ReplySlotState' \
  yierdis-server/yierdis-server-main/src/main/java
```

Expected: no output.

Run:

```bash
rg -n 'ReplyCapacityUnavailableException|awaitCapacity\(|cancelWaiter\(|awaitAdditionalCapacity\(|cancelAdditionalCapacityWaiter\(' \
  --glob '*.java' yierdis-server yierdis-networking
```

Expected: no output. Capacity waiting is represented by `ReplyReservationResult.WAITING` plus an owned typed registration.

- [ ] **Step 10: Commit Netty egress ownership and waiter fairness**

```bash
git add \
  yierdis-networking/yierdis-networking-netty \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api \
  yierdis-server/yierdis-server-main \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/OutboundReplyPressureTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/ReplyResultUnknownTest.java
git commit -m "refactor: move ordered reply capacity to Netty"
```

---

### Task 4: Tokenize Read Pauses And Publish Paired Admissions Directly

**Files:**
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundPauseReason.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundPauseToken.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/InboundReadCreditHandlerTest.java`
- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGateTest.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundReadControl.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundReadCreditHandler.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespDecodedMessageGate.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ConnectionReplySequencer.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ReplySlot.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ConnectionStatsView.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureRuntime.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureIo.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionIoAdapter.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyReplyDecodedMessageGate.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RegisteredRespMessage.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyPipelineTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/OrderedReplyTestFixture.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespIngressLifecycleIntegrationTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `docs/project-docs/executor-and-backpressure.md`

**Interfaces:**
- Consumes: Tasks 1-3 of this plan and Task 3 of the command/runtime plan, especially `ExecutorAdmission.publish(ExecutionRequest, ExecutionReply)`, Netty-owned `ReplySlot`, typed outbound registrations, and prepared one-shot execution.
- Produces: `InboundPauseToken InboundReadControl.pause(InboundPauseReason)`, direct decoder-to-executor paired admission, and no transport-side command queue.
- Pause ownership: decoder and read-credit acquisition own separate `INGRESS_BUDGET` tokens; executor backlog control and the paired gate own separate `EXECUTOR_QUEUE` tokens; each waiting reply slot and the paired gate own separate `REPLY_CAPACITY` tokens; the writability handler owns `TRANSPORT_UNWRITABLE`; connection terminal state owns `CLOSING`.
- Read invariant: a physical `ctx.read()` occurs only while the channel is open and active, no pause token exists, no read is scheduled or in flight, no outstanding read credit exists, and no pending inbound-memory credit exists.
- Handoff invariant: the decoder owns at most one complete message. The gate either consumes it by publishing request plus slot, retains no ownership and returns `WAITING`, or closes explicitly; `server-main` never queues another request.

- [ ] **Step 1: Add RED tests for token identity and the physical-read invariant**

Create `InboundReadCreditHandlerTest.java` with a read probe and these tests:

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class InboundReadCreditHandlerTest {
    @Test
    public void equalReasonTokensDoNotResumeUntilBothOwnersRelease() {
        Fixture fixture = new Fixture();
        try {
            InboundPauseToken first = fixture.credits.pause(InboundPauseReason.INGRESS_BUDGET);
            InboundPauseToken second = fixture.credits.pause(InboundPauseReason.INGRESS_BUDGET);
            fixture.completeCurrentRead();
            int pausedReads = fixture.reads.get();

            first.close();
            first.close();
            fixture.runTasks();
            Assert.assertEquals(pausedReads, fixture.reads.get());
            Assert.assertTrue(fixture.credits.paused(InboundPauseReason.INGRESS_BUDGET));

            second.close();
            fixture.runTasks();
            Assert.assertEquals(pausedReads + 1, fixture.reads.get());
            Assert.assertFalse(fixture.credits.paused(InboundPauseReason.INGRESS_BUDGET));
        } finally {
            fixture.close();
        }
    }

    @Test
    public void differentReasonsComposeWithoutOverwritingEachOther() {
        Fixture fixture = new Fixture();
        try {
            InboundPauseToken executor = fixture.credits.pause(InboundPauseReason.EXECUTOR_QUEUE);
            InboundPauseToken transport = fixture.credits.pause(InboundPauseReason.TRANSPORT_UNWRITABLE);
            fixture.completeCurrentRead();
            int pausedReads = fixture.reads.get();

            executor.close();
            fixture.runTasks();
            Assert.assertEquals(pausedReads, fixture.reads.get());
            Assert.assertTrue(fixture.credits.paused(InboundPauseReason.TRANSPORT_UNWRITABLE));

            transport.close();
            fixture.runTasks();
            Assert.assertEquals(pausedReads + 1, fixture.reads.get());
        } finally {
            fixture.close();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final AtomicInteger reads = new AtomicInteger();
        private final InboundMemoryBudget budget = new InboundMemoryBudget(64 * 1024L);
        private final InboundConnectionMemory memory = new InboundConnectionMemory(
                "read-token", 64 * 1024L, Runnable::run, () -> { });
        private final InboundReadCreditHandler credits = new InboundReadCreditHandler(budget, memory, 1024);
        private final EmbeddedChannel channel = new EmbeddedChannel(
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void read(ChannelHandlerContext ctx) {
                        reads.incrementAndGet();
                    }
                },
                credits
        );

        private Fixture() {
            runTasks();
            Assert.assertEquals(1, reads.get());
        }

        private void completeCurrentRead() {
            channel.pipeline().fireChannelReadComplete();
            runTasks();
        }

        private void runTasks() {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
            budget.close();
        }
    }
}
```

Add this third test to cover pending inbound-memory credit directly:

```java
    @Test
    public void grantedPendingCreditCannotReadWhileAnotherPauseOwnerRemains() {
        int receiveCapacity = 1024;
        long creditBytes = InboundBufferLease.chargeForCapacity(receiveCapacity);
        InboundMemoryBudget budget = new InboundMemoryBudget(creditBytes);
        InboundConnectionMemory holder = new InboundConnectionMemory(
                "holder", creditBytes, Runnable::run, () -> { });
        InboundConnectionMemory tested = new InboundConnectionMemory(
                "tested", creditBytes, Runnable::run, () -> { });
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED,
                budget.tryReserve(holder, creditBytes));
        AtomicInteger reads = new AtomicInteger();
        InboundReadCreditHandler credits = new InboundReadCreditHandler(
                budget, tested, receiveCapacity);
        EmbeddedChannel channel = new EmbeddedChannel(
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void read(ChannelHandlerContext ctx) {
                        reads.incrementAndGet();
                    }
                },
                credits
        );
        try {
            channel.runPendingTasks();
            Assert.assertTrue(credits.pendingReadCreditForTests());
            Assert.assertEquals(0L, credits.outstandingReadCreditBytes());
            Assert.assertEquals(0, reads.get());

            InboundPauseToken executor = credits.pause(InboundPauseReason.EXECUTOR_QUEUE);
            budget.release(holder, creditBytes);
            channel.runPendingTasks();

            Assert.assertFalse(credits.pendingReadCreditForTests());
            Assert.assertEquals(0, reads.get());
            executor.close();
            channel.runPendingTasks();
            Assert.assertEquals(1, reads.get());
        } finally {
            channel.finishAndReleaseAll();
            holder.close();
            tested.close();
            budget.close();
        }
    }
```

- [ ] **Step 2: Add RED tests for paired admission and single-message retention**

Create `NettyReplyDecodedMessageGateTest.java` with the existing `DefaultEventExecutorGroup`, `EmbeddedChannel`, real `CommandExecutor`, `OutboundMemoryBudget`, and `ConnectionReplySequencer` fixtures. Add these exact assertions:

#### Task 4 fixture migration

`NettyReplyDecodedMessageGateTest.Fixture` must explicitly own `channel`,
`context`, `ownerGroup`/`owner`, `executor`, `connection`, `inboundBudget`,
`inboundMemory`, `readCredits`, `outboundBudget`, `outboundConnection`,
`holderMemory`, `sequencer`, `gate`, and `resumeCalls`. Construct them in that
order: create the channel/connection, create and bind/start the executor, create
inbound credits, open both outbound connections, create the sequencer with
`readCredits`, create/bind the gate, then add `inboundReadCredit` and an inert
`gateTestContext` handler and retain that handler's `ChannelHandlerContext`.
`holderMemory` must have the global-budget-sized connection limit so it can
actually drive the global reply-control waiter path.

`runEventLoop()` repeats twice: it drains embedded pending and scheduled tasks,
then waits for one owner no-op task. Do not call it while `blockOwner(...)` is
intentionally holding the serial owner. `close()` is ordered: `gate.close()`,
start `gate.shutdownGracefully()`, run `runEventLoop()`, join executor graceful
shutdown, run `runEventLoop()` again, join egress shutdown, finish the channel,
close the sequencer, stop the owner group, then close holder/inbound memory and
outbound connection/budget resources. Tests obtain `fixture.context()` and use
`fixture::resume`, never pass a null context.

Use a request double that counts raw `close()` calls separately from the final
reference release. It must delegate immutable argv access to
`ByteArrayExecutionRequest`, create a fresh view from `retain()`, and make a
duplicate close observable without manufacturing a second release:

```java
private static final class TrackingRequest implements ExecutionRequest {
    private final ExecutionRequest delegate;
    private final State state;
    private final AtomicBoolean viewClosed = new AtomicBoolean();

    private TrackingRequest(ExecutionRequest delegate, State state) {
        this.delegate = delegate;
        this.state = state;
    }

    static TrackingRequest ping() {
        return new TrackingRequest(ByteArrayExecutionRequest.fromUtf8("PING", List.of()), new State());
    }

    int closeCalls() {
        return state.closeCalls.get();
    }

    int finalReleaseCalls() {
        return state.finalReleaseCalls.get();
    }

    @Override public int argc() { return delegate.argc(); }
    @Override public boolean isNull(int index) { return delegate.isNull(index); }
    @Override public int len(int index) { return delegate.len(index); }
    @Override public byte byteAt(int index, int offset) { return delegate.byteAt(index, offset); }
    @Override public void copyToByteArray(int index, byte[] dst, int dstOff) {
        delegate.copyToByteArray(index, dst, dstOff);
    }
    @Override public byte[] toByteArray(int index) { return delegate.toByteArray(index); }
    @Override public byte[] readOnlyByteArray(int index) { return delegate.readOnlyByteArray(index); }
    @Override public int retainedBytes() { return delegate.retainedBytes(); }
    @Override public long admittedMemoryBytes() { return delegate.admittedMemoryBytes(); }

    @Override
    public TrackingRequest retain() {
        state.references.incrementAndGet();
        return new TrackingRequest(delegate.retain(), state);
    }

    @Override
    public void close() {
        state.closeCalls.incrementAndGet();
        if (viewClosed.compareAndSet(false, true)) {
            delegate.close();
            if (state.references.decrementAndGet() == 0) {
                state.finalReleaseCalls.incrementAndGet();
            }
        }
    }

    private static final class State {
        private final AtomicInteger references = new AtomicInteger(1);
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicInteger finalReleaseCalls = new AtomicInteger();
    }
}
```

Create the fixture in `@Before` with `new Fixture(1)`, close it in `@After`,
and call `fixture.context()` rather than passing `null` to the gate. The direct
tests use `fixture::resume`; after releasing a waiter call only
`fixture.runEventLoop()` because the callback is deliberately scheduled back to
the embedded Netty loop. Any test which lets a published request finish must
also call `fixture.runAllLoops()` and assert both
`request.closeCalls() == 1` and `request.finalReleaseCalls() == 1`.

```java
    @Test
    public void executorSaturationReleasesThePartialOutboundLease() {
        ExecutorAdmission<NettyExecutionConnection> held = acquired(
                fixture.executor.tryAcquire(fixture.connection, 1)
        );
        TrackingRequest pending = TrackingRequest.ping();

        RespDecodedMessageGate.Admission result = fixture.gate.tryAdmit(
                fixture.context(), pending, fixture::resume
        );

        Assert.assertEquals(RespDecodedMessageGate.Status.WAITING, result.status());
        Assert.assertEquals(1, fixture.executor.statsSnapshot().queuedTasks());
        Assert.assertEquals(0L, fixture.outboundBudget.stats().reservedBytes());
        Assert.assertEquals(1, fixture.readCredits.activePauseCount(InboundPauseReason.EXECUTOR_QUEUE));
        Assert.assertEquals(0, pending.closeCalls());
        held.close();
        fixture.runEventLoop();
        Assert.assertEquals(1, fixture.resumeCalls.get());
    }

    @Test
    public void replyControlFailureNeverReservesExecutorBacklog() {
        OutboundMemoryLease pressure = fixture.holderMemory
                .reserve(fixture.outboundBudget.capacityBytes(), fixture.outboundBudget.capacityBytes())
                .orElseThrow();
        TrackingRequest pending = TrackingRequest.ping();

        RespDecodedMessageGate.Admission result = fixture.gate.tryAdmit(
                fixture.context(), pending, fixture::resume
        );

        Assert.assertEquals(RespDecodedMessageGate.Status.WAITING, result.status());
        Assert.assertEquals(0, fixture.executor.statsSnapshot().queuedTasks());
        Assert.assertEquals(0, fixture.connection.context().pending());
        Assert.assertEquals(1, fixture.outboundBudget.stats().controlWaiters());
        Assert.assertEquals(1, fixture.readCredits.activePauseCount(InboundPauseReason.REPLY_CAPACITY));
        pressure.close();
        fixture.runEventLoop();
        Assert.assertEquals(1, fixture.resumeCalls.get());
    }

    @Test
    public void pairedSuccessConsumesTheMessageAndPublishesOneTask() {
        TrackingRequest request = TrackingRequest.ping();

        RespDecodedMessageGate.Admission result = fixture.gate.tryAdmit(
                fixture.context(), request, fixture::resume
        );

        Assert.assertEquals(RespDecodedMessageGate.Status.ADMITTED, result.status());
        Assert.assertNull(result.forwardedMessage());
        Assert.assertEquals(1, fixture.connection.context().pending());
        Assert.assertEquals(1, fixture.outboundBudget.stats().activeSlots());
        Assert.assertEquals(0, request.closeCalls());
    }
```

Define the concrete `Fixture` promised above in the same test file; it uses the
real final Task 4 wiring rather than a mock handler or a second queue:

```java
    private static final class Fixture implements AutoCloseable {
        private static final long INBOUND_BYTES = 64 * 1024L;
        private static final long OUTBOUND_BYTES = 256 * 1024L;
        private static final long CONTROL_BYTES = 4_096L;
        private static final long MAX_REPLY_BYTES = 128 * 1024L;

        private final DefaultEventExecutorGroup ownerGroup = new DefaultEventExecutorGroup(1);
        private final EventExecutor owner = ownerGroup.next();
        private final EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() { });
        private final InboundMemoryBudget inboundBudget = new InboundMemoryBudget(INBOUND_BYTES);
        private final InboundConnectionMemory inboundMemory =
                new InboundConnectionMemory("gate", INBOUND_BYTES, Runnable::run, () -> { });
        private final InboundReadCreditHandler readCredits =
                new InboundReadCreditHandler(inboundBudget, inboundMemory, 8 * 1024);
        private final OutboundMemoryBudget outboundBudget = new OutboundMemoryBudget(OUTBOUND_BYTES * 2);
        private final OutboundConnectionMemory connectionMemory =
                outboundBudget.openConnection(OUTBOUND_BYTES);
        private final OutboundConnectionMemory holderMemory =
                outboundBudget.openConnection(OUTBOUND_BYTES);
        private final AtomicInteger resumeCalls = new AtomicInteger();
        private final NettyExecutionConnection connection =
                NettyExecutionConnection.getOrCreate(channel, 16, 1_024L);
        private final RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        private final CommandExecutor<NettyExecutionConnection> executor;
        private final ConnectionReplySequencer sequencer;
        private final NettyReplyDecodedMessageGate gate;

        private Fixture(int queueCapacity) {
            executor = newPreparedExecutor(owner, replyWriterFactory, queueCapacity);
            executor.start();
            connection.bindOwnerTaskExecutor(executor::executeOwnerTask);
            sequencer = new ConnectionReplySequencer(
                    channel,
                    connectionMemory,
                    readCredits,
                    slot -> BoundedChunkedReplySink.forChannel(
                            slot, channel, 64 * 1024, CONTROL_BYTES, MAX_REPLY_BYTES),
                    ReplyEgressStats.noop()
            );
            gate = new NettyReplyDecodedMessageGate(
                    CONTROL_BYTES,
                    MAX_REPLY_BYTES,
                    connectionMemory,
                    sequencer,
                    executor,
                    connection,
                    readCredits,
                    replyWriterFactory
            );
            connection.bindReplyGate(gate);
            channel.pipeline().addLast("readCredits", readCredits);
            channel.pipeline().addLast("gateContext", new ChannelInboundHandlerAdapter() { });
        }

        private ChannelHandlerContext context() {
            return channel.pipeline().context("gateContext");
        }

        private void resume() {
            resumeCalls.incrementAndGet();
        }

        private void runEventLoop() {
            for (int i = 0; i < 2; i++) {
                channel.runPendingTasks();
                channel.runScheduledPendingTasks();
                owner.submit(() -> { }).syncUninterruptibly();
            }
        }

        @Override
        public void close() {
            gate.close();
            CompletableFuture<Void> egress = gate.shutdownGracefully();
            runEventLoop();
            executor.shutdownGracefully().join();
            runEventLoop();
            egress.join();
            channel.finishAndReleaseAll();
            sequencer.close();
            ownerGroup.shutdownGracefully().syncUninterruptibly();
            inboundMemory.close();
            inboundBudget.close();
            holderMemory.close();
            connectionMemory.close();
            outboundBudget.close();
        }
    }

    private static ExecutorAdmission<NettyExecutionConnection> acquired(
            ExecutorAdmissionAttempt<NettyExecutionConnection> attempt
    ) {
        Assert.assertTrue(attempt instanceof ExecutorAdmissionAttempt.Acquired<NettyExecutionConnection>);
        return ((ExecutorAdmissionAttempt.Acquired<NettyExecutionConnection>) attempt).admission();
    }
```

Add imports for `CommandExecutionContext`, `PreparedCommand`, `ReplyShape`,
`ReplyShapes`, `ValidationResult`, `CommandExecutorConfig`, `SchedulingPolicy`,
and `RespReplySizer`. `newPreparedExecutor(...)` is a local helper in this test
file and must be this implementation:

```java
    private static CommandExecutor<NettyExecutionConnection> newPreparedExecutor(
            EventExecutor owner,
            RespReplyWriterFactory replyWriterFactory,
            int queueCapacity
    ) {
        return new CommandExecutor<>(
                () -> { },
                (session, request) -> new PreparedCommand() {
                    @Override
                    public ReplyShape replyShape() {
                        return ReplyShapes.simpleString("PONG");
                    }

                    @Override
                    public ValidationResult validateBeforeExecute() {
                        return ValidationResult.VALID;
                    }

                    @Override
                    public void execute(CommandExecutionContext context) {
                        context.reply().simpleString("PONG");
                    }

                    @Override
                    public void close() {
                    }
                },
                new NettySerialOwnerExecutor(owner),
                new RespReplySizer(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(
                        queueCapacity,
                        0,
                        queueCapacity,
                        0,
                        0,
                        0,
                        128,
                        10,
                        SchedulingPolicy.FAIR
                )
        );
    }
```

Do not use `YierdisFastCommandHandler` in this fixture. `runEventLoop()` is
deliberately a two-loop barrier: it drains the embedded channel, waits for one
owner turn, then drains the channel again; no test may use `Thread.sleep` to
wait for a retry.

Add this integration test to `RespIngressLifecycleIntegrationTest`; retain the decoder as a fixture field and make `awaitReplies(int)` drain the embedded event loop until it has read that many complete outbound RESP frames:

```java
    @Test
    public void executorSaturationKeepsOneDecoderMessageAndNoTransportQueue() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        ProtocolExecutorFixture fixture = new ProtocolExecutorFixture(1, executions);
        CountDownLatch ownerStarted = new CountDownLatch(1);
        CountDownLatch unblockOwner = new CountDownLatch(1);
        fixture.blockOwner(ownerStarted, unblockOwner);
        Assert.assertTrue(ownerStarted.await(1, TimeUnit.SECONDS));
        try {
            fixture.writePacket(
                    "*1\r\n$4\r\nPING\r\n"
                            + "*1\r\n$4\r\nPING\r\n"
                            + "*1\r\n$4\r\nPING\r\n"
            );

            Assert.assertEquals(1, fixture.decoder.pendingDecodedMessagesForTests());
            Assert.assertEquals(1, fixture.connection.context().pending());
            Assert.assertEquals(1, fixture.executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(1L, fixture.outboundBudget.stats().activeSlots());
            Assert.assertNull(fixture.channel.pipeline().context("commandHandler"));
            Assert.assertEquals(1,
                    fixture.readCredits.activePauseCount(InboundPauseReason.EXECUTOR_QUEUE));

            unblockOwner.countDown();
            Assert.assertEquals(
                    List.of("+PONG\r\n", "+PONG\r\n", "+PONG\r\n"),
                    fixture.awaitReplies(3)
            );
            Assert.assertEquals(3, executions.get());
            Assert.assertEquals(0, fixture.decoder.pendingDecodedMessagesForTests());
            Assert.assertEquals(0, fixture.connection.context().pending());
            Assert.assertEquals(0, fixture.executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0L, fixture.inboundBudget.stats().reservedBytes());
            Assert.assertEquals(0L, fixture.outboundBudget.stats().reservedBytes());
        } finally {
            unblockOwner.countDown();
            fixture.close();
        }
    }
```

Add disconnect and shutdown cases using a fresh lifecycle fixture per branch. After firing channel close or joining `executor.shutdownGracefully()` plus `gate.shutdownGracefully()`, run all owner/event-loop tasks twice and use this shared assertion body:

```java
Assert.assertEquals(1, lifecycle.requestCloseCalls());
Assert.assertEquals(1, lifecycle.admissionReleaseCalls());
Assert.assertEquals(1, lifecycle.waiterCancelCalls());
Assert.assertEquals(1, lifecycle.slotCleanupCalls());
Assert.assertEquals(1, lifecycle.leaseCloseCalls());
Assert.assertEquals(1, lifecycle.preparedCloseCalls());
Assert.assertEquals(1, lifecycle.retainedSourceCloseCalls());
Assert.assertEquals(0, lifecycle.executor.statsSnapshot().queuedTasks());
Assert.assertEquals(0L, lifecycle.outboundBudget.stats().reservedBytes());
Assert.assertEquals(0L, lifecycle.inboundBudget.stats().reservedBytes());
```

After a second `gate.close()` call, assert `lifecycle.gateCloseInvocations() == 2`
but keep every resource invocation/effective-release assertion above at `1`.
The lifecycle fixture records both values separately: a second public close is
observable, while the single gate-close transition must not call request close,
waiter cancellation, slot cleanup, lease close, prepared close, or retained
source close a second time.

Migrate `ProtocolExecutorFixture` in `RespIngressLifecycleIntegrationTest` to
hold final fields for `InboundReadCreditHandler readCredits`,
`RespRequestDecoder decoder`, `NettyReplyDecodedMessageGate gate`, and the
`ConnectionReplySequencer`; construct them in the Step 8 pipeline order and add
only `inboundReadCredit`, `inboundByteAccounting`, and `respRequestDecoder` to
the channel. Its `writePacket(String)` writes the ASCII buffer then calls
`runAllLoops()`. Its `blockOwner(...)` submits the latch task to `owner`. Its
`awaitReplies(int expected)` repeatedly calls `runAllLoops()`, drains outbound
`ByteBuf` frames into ASCII strings, and fails at a one-second monotonic deadline
unless exactly `expected` frames arrive. `runAllLoops()` performs the same
embedded-event-loop/owner/embedded-event-loop barrier as the gate fixture.

Migrate `OrderedReplyTestFixture` to expose `writePacket(String)` and
`awaitOutboundFrames(int)` through that same decoder/gate pipeline. Replace
every `register(...)`, `RegisteredRespMessage`, and direct
`YierdisFastCommandHandler` write in `OrderedReplyPipelineTest` with complete
RESP request frames, then assert the resulting RESP output order after
`awaitOutboundFrames`. Add a structural assertion that the initialized pipeline
has no `commandHandler` context and no `pendingSubmissions` field is referenced.
The fixture close order matches the concrete gate fixture above:
`gate.close()`, start `gate.shutdownGracefully()`, drain the event loop, join
`executor.shutdownGracefully()`, drain again, join egress shutdown,
`channel.finishAndReleaseAll()`, `sequencer.close()`, stop the owner group,
close inbound connection/budget resources, then close outbound connection/budget
resources. Each resource exposes an effective-close counter for
disconnect/shutdown assertions.

- [ ] **Step 3: Run the token and paired-admission tests to verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=InboundReadCreditHandlerTest,RespRequestDecoderTest,NettyReplyDecodedMessageGateTest,RespIngressLifecycleIntegrationTest,OrderedReplyPipelineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because read pauses are binary, admitted messages must be non-null downstream objects, the reply gate does not acquire executor admission, and the command handler owns a second pending queue.

- [ ] **Step 4: Introduce unique idempotent pause tokens and one read predicate**

Create `InboundPauseReason.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

public enum InboundPauseReason {
    INGRESS_BUDGET,
    EXECUTOR_QUEUE,
    REPLY_CAPACITY,
    TRANSPORT_UNWRITABLE,
    CLOSING
}
```

Create `InboundPauseToken.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class InboundPauseToken implements AutoCloseable {
    private final InboundPauseReason reason;
    private final Consumer<InboundPauseToken> releaser;
    private final AtomicBoolean active = new AtomicBoolean(true);

    InboundPauseToken(InboundPauseReason reason, Consumer<InboundPauseToken> releaser) {
        this.reason = Objects.requireNonNull(reason, "reason");
        this.releaser = Objects.requireNonNull(releaser, "releaser");
    }

    public InboundPauseReason reason() {
        return reason;
    }

    public boolean active() {
        return active.get();
    }

    @Override
    public void close() {
        if (active.compareAndSet(true, false)) {
            releaser.accept(this);
        }
    }
}
```

Replace `InboundReadControl` with:

```java
package yier.bubu.redis.protocol.resp.netty;

public interface InboundReadControl {
    InboundReadControl NOOP = reason -> {
        InboundPauseToken token = new InboundPauseToken(reason, ignored -> { });
        token.close();
        return token;
    };

    InboundPauseToken pause(InboundPauseReason reason);
}
```

In `InboundReadCreditHandler`, replace both booleans with a concurrent identity set and make every read path use the same predicate:

```java
    private final java.util.Set<InboundPauseToken> pauseTokens =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    private InboundPauseToken readCreditPause;

    @Override
    public InboundPauseToken pause(InboundPauseReason reason) {
        return acquirePause(reason, true);
    }

    public boolean paused(InboundPauseReason reason) {
        return pauseTokens.stream().anyMatch(token -> token.reason() == reason);
    }

    public int activePauseCount(InboundPauseReason reason) {
        return Math.toIntExact(pauseTokens.stream()
                .filter(token -> token.reason() == reason)
                .count());
    }

    boolean pendingReadCreditForTests() {
        return pendingReadCredit != null;
    }

    private InboundPauseToken acquirePause(InboundPauseReason reason, boolean cancelPendingCredit) {
        InboundPauseToken token = new InboundPauseToken(reason, this::releasePause);
        if (closed) {
            token.close();
            return token;
        }
        pauseTokens.add(token);
        if (closed) {
            token.close();
            return token;
        }
        if (cancelPendingCredit) {
            executeOnEventLoop(this::cancelPendingReadCredit);
        }
        return token;
    }

    private void releasePause(InboundPauseToken token) {
        if (pauseTokens.remove(token)) {
            executeOnEventLoop(this::scheduleReadIfAllowed);
        }
    }

    private boolean physicalReadAllowed(ChannelHandlerContext ctx) {
        return !closed
                && ctx != null
                && ctx.channel().isOpen()
                && ctx.channel().isActive()
                && ctx.channel().isWritable()
                && pauseTokens.isEmpty()
                && !readScheduled
                && !readInFlight
                && outstandingCreditBytes == 0L
                && pendingReadCredit == null;
    }
```

`scheduleReadIfAllowed()` checks every clause except `readScheduled`, sets it once, and schedules one event-loop action. That action clears `readScheduled`, calls `physicalReadAllowed(ctx)`, and installs exactly one `PendingReadCredit`. Both an immediate reservation and a waiter callback must compare-and-detach that exact pending object (`pendingReadCredit = null`) before calling `grantReadCredit(...)`; only after the detach can `grantReadCredit(...)` re-check `physicalReadAllowed(ctx)` and call `ctx.read()`. On `WAITING`, set `readCreditPause = acquirePause(INGRESS_BUDGET, false)` without cancelling that same pending credit. Claiming, rejecting, cancelling, channel close, and handler removal first detach and close `readCreditPause`, then release the matching credit. Cleanup closes a snapshot of all remaining tokens and never calls `ctx.read()` after `closed = true`.

- [ ] **Step 5: Make decoder pauses owned and allow consumed admission**

Change the gate result validation and factory methods:

```java
    record Admission(Status status, Object forwardedMessage) {
        public Admission {
            Objects.requireNonNull(status, "status");
            if (status != Status.ADMITTED && forwardedMessage != null) {
                throw new IllegalArgumentException("only admitted messages may be forwarded");
            }
        }

        public static Admission admitted(Object forwardedMessage) {
            return new Admission(Status.ADMITTED,
                    Objects.requireNonNull(forwardedMessage, "forwardedMessage"));
        }

        public static Admission consumed() {
            return new Admission(Status.ADMITTED, null);
        }

        public static Admission waiting() { return new Admission(Status.WAITING, null); }
        public static Admission closed() { return new Admission(Status.CLOSED, null); }
    }
```

`PASS_THROUGH` continues returning `Admission.admitted(decoded)`. In `RespRequestDecoder`, keep nullable `InboundPauseToken decoderIngressPause` and `InboundPauseToken closingPause`; decoder memory/consolidation waits acquire only `decoderIngressPause`, successful continuation closes only that token, and `enterClosing()` acquires only `closingPause`. A gate `WAITING` result does not acquire a decoder token because the gate already owns the precise executor/reply reason.

Replace the admitted tail of `forwardPendingDecodedMessage(...)` with:

```java
        pendingDecodedMessage = null;
        Object forwarded = admission.forwardedMessage();
        if (decoded instanceof RespProtocolError) {
            state = State.CLOSING;
            if (forwarded != null) {
                ctx.fireChannelRead(forwarded);
            }
            return false;
        }
        state = State.READ_COMMAND;
        closeDecoderIngressPause();
        if (forwarded != null) {
            ctx.fireChannelRead(forwarded);
        }
        return true;
```

Add `int pendingDecodedMessagesForTests() { return pendingDecodedMessage == null ? 0 : 1; }`. `cleanup()` closes both decoder-owned tokens after detaching them. No code clears pauses by enum value; only the exact token owner closes its handle.

- [ ] **Step 6: Acquire reply and executor capacity as one logical gate admission**

Give `NettyReplyDecodedMessageGate` the executor, execution connection, read control, reply writer factory, and these owned wait fields:

```java
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final NettyExecutionConnection executionConnection;
    private final InboundReadControl readControl;
    private final RedisReplyWriterFactory replyWriterFactory;
    private CapacityRegistration executorWait = CapacityRegistration.NONE;
    private OutboundCapacityRegistration replyWait;
    private InboundPauseToken executorPause;
    private InboundPauseToken replyPause;
    private boolean closed;
```

Replace `tryAdmit(...)` with the paired state machine:

```java
    @Override
    public Admission tryAdmit(ChannelHandlerContext ctx, Object decoded, Runnable resumeOnEventLoop) {
        Objects.requireNonNull(decoded, "decoded");
        Objects.requireNonNull(resumeOnEventLoop, "resumeOnEventLoop");
        cancelAdmissionWaits();
        if (closed || !sequencer.acceptingRegistrations() || connectionMemory.closed()) {
            return Admission.closed();
        }
        if (decoded instanceof RespProtocolError error) {
            return admitTerminal(ctx, error, resumeOnEventLoop);
        }
        if (!(decoded instanceof ExecutionRequest request)) {
            closeLiveConnection(ctx);
            return Admission.closed();
        }

        OutboundMemoryLease lease = connectionMemory
                .reserve(controlReservationBytes, singleReplyLimitBytes)
                .orElse(null);
        if (lease == null) {
            return waitForReplyControl(ctx, resumeOnEventLoop);
        }

        ExecutorAdmissionAttempt<NettyExecutionConnection> attempt =
                executor.tryAcquire(executionConnection, request.retainedBytes());
        if (attempt instanceof ExecutorAdmissionAttempt.Unavailable<NettyExecutionConnection>) {
            lease.close();
            return waitForExecutor(ctx, request.retainedBytes(), resumeOnEventLoop);
        }
        if (attempt instanceof ExecutorAdmissionAttempt.Rejected<NettyExecutionConnection> rejected) {
            if (rejected.reason() == CommandExecutor.SubmitRejectReason.REQUEST_TOO_LARGE) {
                return completeRequestTooLarge(ctx, request, lease);
            }
            lease.close();
            closeLiveConnection(ctx);
            return Admission.closed();
        }

        ExecutorAdmission<NettyExecutionConnection> admission =
                ((ExecutorAdmissionAttempt.Acquired<NettyExecutionConnection>) attempt).admission();
        ReplySlot slot;
        try {
            slot = sequencer.register(lease).orElse(null);
        } catch (Throwable registrationFailure) {
            // A throwing register call leaves lease ownership with this gate by contract.
            lease.close();
            admission.close();
            closeLiveConnection(ctx);
            return Admission.closed();
        }
        if (slot == null) {
            // An empty result has already closed the lease; the decoded request remains
            // decoder-owned until CLOSED makes the decoder release it.
            admission.close();
            closeLiveConnection(ctx);
            return Admission.closed();
        }
        // publish consumes request plus slot on every post-state-transition branch.
        admission.publish(request, slot);
        return Admission.consumed();
    }
```

`ConnectionReplySequencer.register(lease)` has a three-way ownership contract:
success returns a slot which owns `lease`; an empty result closes `lease` before
returning; a thrown exception leaves `lease` with the caller. Add an explicit
`registerFailureLeavesLeaseWithCaller` unit test and a paired-gate test for each
outcome. The paired-gate fault tests inject register-empty, register-throw,
publication accounting failure, queue-offer failure, and drain-scheduling
failure. They assert: the decoder closes its request exactly once only for the
pre-publish closed outcomes; publish failures close request/reply/lease/backlog
inside the executor exactly once and return `Admission.consumed()`; no branch
leaves a live slot, waiter, pause token, or queued byte.

Use these wait helpers; callbacks always enqueue a later decoder retry, avoiding inline re-entry before the returned registration is stored:

```java
    private Admission waitForReplyControl(ChannelHandlerContext ctx, Runnable resume) {
        InboundPauseToken pause = readControl.pause(InboundPauseReason.REPLY_CAPACITY);
        Optional<OutboundCapacityRegistration> retained = connectionMemory.onControlCapacityAvailable(
                controlReservationBytes,
                singleReplyLimitBytes,
                () -> ctx.executor().execute(resume)
        );
        if (retained.isEmpty()) {
            pause.close();
            closeLiveConnection(ctx);
            return Admission.closed();
        }
        replyPause = pause;
        replyWait = retained.get();
        return Admission.waiting();
    }

    private Admission waitForExecutor(ChannelHandlerContext ctx, int retainedBytes, Runnable resume) {
        InboundPauseToken pause = readControl.pause(InboundPauseReason.EXECUTOR_QUEUE);
        CapacityRegistration retained = executor.onAdmissionAvailable(
                retainedBytes,
                () -> ctx.executor().execute(resume)
        );
        if (retained == CapacityRegistration.NONE) {
            pause.close();
            closeLiveConnection(ctx);
            return Admission.closed();
        }
        executorPause = pause;
        executorWait = retained;
        return Admission.waiting();
    }
```

`cancelAdmissionWaits()` first detaches all four fields, then independently cancels each registration and closes each token even if another cleanup throws. `close()`, channel close, handler removal, and shutdown call that method exactly once. `completeRequestTooLarge(...)` registers the already-reserved lease, uses `RespProtocolErrorReplyHandler.completeError(...)`, closes the request, returns `Admission.consumed()`, and never publishes executor work. `admitTerminal(...)` waits only for reply control, registers a terminal slot, calls `executionConnection.markClosing()`, and uses `completeProtocolError(...)`, which marks the slot ready with close-after-reply before the gate returns consumed. If terminal waiter retention, registration, or rendering fails, cancel the slot/lease and close the channel explicitly.

Replace that lifecycle prose with this concrete detached cleanup implementation.
`RespDecodedMessageGate` extends `AutoCloseable` with a default no-op `close()`;
`RespRequestDecoder.cleanup()` invokes it after detaching its own retained decoded
message, and `PASS_THROUGH` keeps the default. `NettyReplyDecodedMessageGate`
installs `channel.closeFuture().addListener(ignored -> close())`; its
`shutdownGracefully()` calls `close()` before delegating to the sequencer. Thus
decoder removal, channel close, explicit gate close, and graceful shutdown share
one `closed.compareAndSet(false, true)` transition.

```java
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recordCleanupFailure(cancelAdmissionWaits());
    }

    private Throwable cancelAdmissionWaits() {
        CapacityRegistration detachedExecutorWait = executorWait;
        OutboundCapacityRegistration detachedReplyWait = replyWait;
        InboundPauseToken detachedExecutorPause = executorPause;
        InboundPauseToken detachedReplyPause = replyPause;
        executorWait = CapacityRegistration.NONE;
        replyWait = null;
        executorPause = null;
        replyPause = null;

        Throwable failure = null;
        failure = runCleanup(failure, detachedExecutorWait::cancel);
        failure = runCleanup(failure, () -> {
            if (detachedReplyWait != null) {
                detachedReplyWait.cancel();
            }
        });
        failure = runCleanup(failure, () -> {
            if (detachedExecutorPause != null) {
                detachedExecutorPause.close();
            }
        });
        failure = runCleanup(failure, () -> {
            if (detachedReplyPause != null) {
                detachedReplyPause.close();
            }
        });
        return failure;
    }

    private static Throwable runCleanup(Throwable previous, Runnable action) {
        try {
            action.run();
            return previous;
        } catch (Throwable failure) {
            if (previous == null) {
                return failure;
            }
            if (previous != failure) {
                previous.addSuppressed(failure);
            }
            return previous;
        }
    }

    private void recordCleanupFailure(Throwable failure) {
        if (failure != null) {
            cleanupFailures.increment();
        }
    }
```

Use `AtomicBoolean closed` rather than an event-loop-only boolean because a
channel close listener and graceful shutdown may race. `tryAdmit(...)` continues
to call `cancelAdmissionWaits()` before replacing a prior retry, but it first
checks `closed.get()` and never calls it after the gate-close transition. Every
capacity callback schedules `resumeOnEventLoop` rather than invoking it inline;
an already queued retry re-enters the decoder, observes `CLOSING` or the newly
stored waiter, and cannot recreate a detached old registration.

Add these lifecycle tests to `NettyReplyDecodedMessageGateTest` using a
`ThrowingLifecycleFixture` that can independently throw from executor-wait
cancel, reply-wait cancel, executor-token close, or reply-token close:

```java
    @Test
    public void cleanupDetachesEveryWaitOwnerBeforeIndependentFailures() {
        for (GateCleanupFault fault : GateCleanupFault.values()) {
            ThrowingLifecycleFixture fixture = ThrowingLifecycleFixture.waitingOnBothKinds(fault);
            try {
                fixture.gate.close();
                fixture.runAllLoops();

                Assert.assertEquals(fault.name(), 1, fixture.executorWaitCancelCalls());
                Assert.assertEquals(fault.name(), 1, fixture.replyWaitCancelCalls());
                Assert.assertEquals(fault.name(), 1, fixture.executorPauseCloseCalls());
                Assert.assertEquals(fault.name(), 1, fixture.replyPauseCloseCalls());
                Assert.assertEquals(fault.name(), 0, fixture.readCredits().activePauseCount(
                        InboundPauseReason.EXECUTOR_QUEUE));
                Assert.assertEquals(fault.name(), 0, fixture.readCredits().activePauseCount(
                        InboundPauseReason.REPLY_CAPACITY));
                Assert.assertEquals(fault.name(), 0, fixture.outboundBudget().stats().totalWaiters());

                fixture.gate.close();
                fixture.runAllLoops();
                Assert.assertEquals(fault.name(), 1, fixture.executorWaitCancelCalls());
                Assert.assertEquals(fault.name(), 1, fixture.replyWaitCancelCalls());
                Assert.assertEquals(fault.name(), 1, fixture.executorPauseCloseCalls());
                Assert.assertEquals(fault.name(), 1, fixture.replyPauseCloseCalls());
            } finally {
                fixture.close();
            }
        }
    }

    @Test
    public void queuedCapacityCallbackAfterCloseCannotReinstallAdmissionState() {
        ThrowingLifecycleFixture fixture = ThrowingLifecycleFixture.waitingForReply();
        try {
            fixture.fireStoredReplyCallback();
            fixture.gate.close();
            fixture.runAllLoops();

            Assert.assertEquals(0, fixture.outboundBudget().stats().totalWaiters());
            Assert.assertEquals(0, fixture.readCredits().activePauseCount(
                    InboundPauseReason.REPLY_CAPACITY));
            Assert.assertEquals(0, fixture.gate.activeAdmissionWaitsForTests());
            Assert.assertEquals(0, fixture.decoder().pendingDecodedMessagesForTests());
        } finally {
            fixture.close();
        }
    }
```

`ThrowingLifecycleFixture` records raw method invocation counts and separately
counts effective resource release; a repeated `gate.close()` must leave every
resource effect at one. Its `runAllLoops()` drains the Netty embedded event loop,
then the serial owner, then the Netty loop again, twice, so callback/close races
are deterministic rather than timeout based.

- [ ] **Step 7: Put each remaining pause under its real owner**

In moved `ReplySlot`, store `InboundReadControl` from `ConnectionReplySequencer`. When an expansion wait is retained, acquire a `REPLY_CAPACITY` token and return a composite `CapacityRegistration` that targets only that slot's registration and token:

```java
        InboundPauseToken pause = readControl.pause(InboundPauseReason.REPLY_CAPACITY);
        Optional<OutboundCapacityRegistration> retained = lease.onAdditionalCapacityAvailable(
                waiting.additionalBytes(), waiting.singleReplyLimitBytes(), wakeup);
        if (retained.isEmpty()) {
            pause.close();
            return CapacityRegistration.NONE;
        }
        OutboundCapacityRegistration registration = retained.get();
        expansionRegistration.set(registration);
        expansionPause.set(pause);
        AtomicBoolean active = new AtomicBoolean(true);
        return () -> {
            if (active.compareAndSet(true, false)) {
                if (expansionRegistration.compareAndSet(registration, null)) {
                    registration.cancel();
                }
                if (expansionPause.compareAndSet(pause, null)) {
                    pause.close();
                }
            }
        };
```

Successful re-reservation and every terminal slot cleanup call the same targeted cancellation helper. A capacity callback does not close the token early; the token stays active until the executor retry either reserves capacity or terminally cleans the task.

In `NettyExecutionConnection`, keep independent atomic references for one executor-backlog token and one terminal token. `NettyExecutionIoAdapter.disableInput(...)` acquires `EXECUTOR_QUEUE` only when the first reference is empty; `enableInput(...)` detaches and closes exactly that token. `markClosing()` acquires `CLOSING` once. In `WriteBufferBackpressureHandler`, hold an event-loop-confined `TRANSPORT_UNWRITABLE` token, acquire it on unwritable, and close it on writable/removal. Remove `CommandExecutor.onTransportUnwritable/onTransportWritable`; transport state no longer masquerades as executor backlog state.

Remove `inputPausedByReply` from `ConnectionStatsView`, `ExecutionConnectionContext`, its snapshot record, `ExecutorBackpressureRuntime`, and `CommandExecutor` adapters. Remove the reply-pause and writability guards from `ExecutorBackpressureController`: closing the executor's token is safe because any reply or transport token remains independently active. In Task 2's `CommandExecutorExecutionSupport`, delete `markInputPausedByReply()`, `clearInputPausedByReply()`, and the reply-capacity call to `backpressureController.disableAutoRead(...)`; `ReplySlot` now owns that exact pause lifetime.

- [ ] **Step 8: Delete the transport command queue and wire direct handoff**

Delete `YierdisFastCommandHandler.java` and `RegisteredRespMessage.java`. Remove the `"commandHandler"` pipeline entry. Construct the gate with both admission authorities and pass the same read control into the sequencer and decoder:

```java
ConnectionReplySequencer replySequencer = new ConnectionReplySequencer(
        ch,
        outboundConnection,
        inboundReadCredit,
        slot -> BoundedChunkedReplySink.forChannel(
                slot,
                ch,
                config.replyChunkPayloadBytes(),
                config.replyControlReservationBytes(),
                config.replyMaxTotalBytes(),
                resource -> closeReplyResourceOnOwner(executor, resource)
        ),
        replyEgressStats
);
NettyReplyDecodedMessageGate gate = new NettyReplyDecodedMessageGate(
        config.replyControlReservationBytes(),
        config.replyMaxTotalBytes(),
        outboundConnection,
        replySequencer,
        executor,
        executionConnection,
        inboundReadCredit,
        replyWriterFactory
);
RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
        config.protocolMaxBulkBytes(),
        config.protocolMaxArgs(),
        config.protocolMaxLineBytes(),
        config.protocolMaxCommandBytes(),
        inboundMemoryBudget,
        inboundConnection,
        gate
);
decoder.setReadControl(inboundReadCredit);

ch.pipeline()
        .addLast("inboundReadCredit", inboundReadCredit)
        .addLast("inboundByteAccounting", new InboundByteAccountingHandler(inboundReadCredit))
        .addLast("respRequestDecoder", decoder);
```

Bind `gate.close()` and the connection's terminal token to the channel close future. Add these networking-owned rendering helpers to `RespProtocolErrorReplyHandler`:

```java
    public static void completeProtocolError(
            RedisReplyWriterFactory factory,
            CommandSession session,
            ExecutionReply reply,
            String message
    ) {
        RedisReplyWriter writer = factory.newWriter(session, reply.sink());
        writer.protocolError(message);
        writer.requestCloseAfterReply();
        reply.markReady(true);
    }

    public static void completeError(
            RedisReplyWriterFactory factory,
            CommandSession session,
            ExecutionReply reply,
            String message,
            boolean closeAfterReply
    ) {
        RedisReplyWriter writer = factory.newWriter(session, reply.sink());
        writer.error(message);
        if (closeAfterReply) {
            writer.requestCloseAfterReply();
        }
        reply.markReady(closeAfterReply);
    }
```

The gate catches rendering failure, cancels the reply, and closes the channel; after either helper returns, reply ownership belongs to the sequencer. `server-main` coordinates these helpers but contains no RESP framing or command handler. Migrate `OrderedReplyTestFixture` and all pipeline tests to write directly through the decoder/gate or `ExecutorAdmission.publish(...)`.

- [ ] **Step 9: Run focused state-machine and lifecycle tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=InboundReadCreditHandlerTest,RespRequestDecoderTest,RespProtocolErrorReplyHandlerTest,ReplyCapacityBlockedSchedulingTest,NettyReplyDecodedMessageGateTest,RespIngressLifecycleIntegrationTest,OrderedReplyPipelineTest,NettyExecutionAdapterIntegrationTest,RespIngressPressureTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; equal-reason tokens coexist, all physical reads satisfy the single predicate, executor saturation retains one decoder message and no transport queue, partial acquisitions are released, pipelined requests each produce one ordered reply, and close/shutdown return every owner count to zero exactly once.

- [ ] **Step 10: Enforce the deleted surfaces and module ownership**

Add architecture assertions that both deleted files are absent and outbound owners exist only under `yierdis-networking-netty`. Run:

```bash
test ! -e yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java
test ! -e yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RegisteredRespMessage.java

rg -n 'YierdisFastCommandHandler|RegisteredRespMessage|pendingSubmissions|inputPausedByReply|pauseIngress\(|resumeIngress\(' \
  --glob '*.java' yierdis-server yierdis-networking yierdis-tests
```

Expected: all commands succeed and `rg` prints no matches. Update `docs/project-docs/executor-and-backpressure.md` with the five pause reasons, per-owner token table, paired lease/admission handoff, the one-decoder-message bound, and the physical-read predicate.

- [ ] **Step 11: Commit tokenized paired handoff**

```bash
git add \
  yierdis-networking/yierdis-networking-netty \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ConnectionStatsView.java \
  yierdis-server/yierdis-server-executor \
  yierdis-server/yierdis-server-main \
  yierdis-tests/yierdis-architecture-tests \
  yierdis-tests/yierdis-integration-tests \
  docs/project-docs/executor-and-backpressure.md
git commit -m "refactor: pair ingress admission with tokenized reads"
```

---

## Final Verification

- [ ] **Step 1: Run the complete JDK 25 test reactor**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn test
```

Expected: PASS with no ignored or disabled regression tests.

- [ ] **Step 2: Package every production module except benchmarks**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -q -pl '!yierdis-benchmark' -DskipTests package
```

Expected: exit 0.

- [ ] **Step 3: Run the existing server smoke check without rebuilding**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
SKIP_BUILD=1 ./scripts/smoke.sh
```

Expected: PASS; the server accepts RESP commands, preserves ordered replies, and shuts down without retained inbound or outbound ownership.
