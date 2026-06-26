# Connection Actor Session Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current RESP submit path with a connection actor/session architecture that owns request lifecycle, terminal transitions, reply gating, and connection statistics while keeping DB command execution on the single executor owner thread.

**Architecture:** Introduce explicit connection lifecycle state and typed executor submit/completion contracts first, then switch RESP decoding to emit final `ExecutionRequest` objects directly, add a Netty event-loop actor that becomes the only transport control plane, and finally cut the production pipeline over to the new actor path while deleting the old adapter/handler chain. Normal replies, busy rejects, protocol terminal replies, and internal terminal replies all route through one actor-owned reply gate; executor code no longer writes Netty channels directly.

**Tech Stack:** Java 25, Maven, Netty 4.1, JUnit 4, existing `ExecutionRequest` / `RedisReplyWriter` / `CommandExecutor` boundaries, explicit `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` command prefix for all Maven runs.

---

## File Structure

Create:

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionState.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitOutcome.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionCompletion.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionSuppressionReason.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/BufferedReplyBytesSink.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolViolation.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorHandler.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplyGate.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorCompletionBus.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NoopExecutionIoAdapter.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionActorHandlerTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ActorTestEnv.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorActorContractTest.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoderTest.java`

Modify:

- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnection.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutionConnectionContextTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- `docs/project-docs/request-execution-flow.md`
- `docs/project-docs/executor-and-backpressure.md`

Delete:

- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`

---

### Task 1: Add Explicit Connection State And Submit/Completion Contracts

**Files:**

- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionState.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitOutcome.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionCompletion.java`
- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionSuppressionReason.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnection.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutionConnectionContextTest.java`
- Create: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorActorContractTest.java`

- [ ] **Step 1: Write the failing state and contract tests**

```java
package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

public class ExecutionConnectionContextTest {
    @Test
    public void lifecycleStateStartsOpenAndTracksTerminalEpochAndReplySequence() {
        ExecutionConnectionContext context = new ExecutionConnectionContext();

        Assert.assertEquals(ExecutionConnectionState.OPEN, context.state());
        Assert.assertEquals(0L, context.terminalEpoch());
        Assert.assertEquals(1L, context.nextRequestSeq());
        Assert.assertEquals(1L, context.nextReplySeq());

        Assert.assertTrue(context.beginProtocolPoison());
        Assert.assertEquals(ExecutionConnectionState.PROTOCOL_POISONED, context.state());
        Assert.assertEquals(1L, context.terminalEpoch());
        Assert.assertFalse(context.beginInternalTermination());
    }
}
```

```java
package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

public class CommandExecutorActorContractTest {
    @Test
    public void submitOutcomeExposesAcceptedAndRejectFamiliesExplicitly() {
        Assert.assertEquals(CommandExecutorSubmitOutcome.ACCEPTED, CommandExecutorSubmitOutcome.ACCEPTED);
        Assert.assertTrue(CommandExecutorSubmitOutcome.REJECTED_QUEUE_FULL.isRejected());
        Assert.assertTrue(CommandExecutorSubmitOutcome.REJECTED_BYTES_BUDGET.isRejected());
        Assert.assertTrue(CommandExecutorSubmitOutcome.REJECTED_EXECUTOR_NOT_RUNNING.isRejected());
        Assert.assertTrue(CommandExecutorSubmitOutcome.REJECTED_CONNECTION_NOT_ACCEPTING.isRejected());
        Assert.assertTrue(CommandExecutorSubmitOutcome.REJECTED_INTERNAL.isRejected());
    }
}
```

- [ ] **Step 2: Run the executor tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test \
  -Dtest=ExecutionConnectionContextTest,CommandExecutorActorContractTest
```

Expected: FAIL with missing symbols for `ExecutionConnectionState`, `terminalEpoch()`, `nextRequestSeq()`, `nextReplySeq()`, and `CommandExecutorSubmitOutcome`.

- [ ] **Step 3: Add the new executor contract types**

Create `ExecutionConnectionState.java`:

```java
package yier.bubu.redis.execution.executor;

public enum ExecutionConnectionState {
    OPEN,
    CLOSE_AFTER_REPLY,
    INTERNAL_TERMINATING,
    PROTOCOL_POISONED,
    CLOSED
}
```

Create `CommandExecutorSubmitOutcome.java`:

```java
package yier.bubu.redis.execution.executor;

public enum CommandExecutorSubmitOutcome {
    ACCEPTED(false),
    REJECTED_QUEUE_FULL(true),
    REJECTED_BYTES_BUDGET(true),
    REJECTED_EXECUTOR_NOT_RUNNING(true),
    REJECTED_CONNECTION_NOT_ACCEPTING(true),
    REJECTED_INTERNAL(true);

    private final boolean rejected;

    CommandExecutorSubmitOutcome(boolean rejected) {
        this.rejected = rejected;
    }

    public boolean isRejected() {
        return rejected;
    }
}
```

Create `CommandExecutionSuppressionReason.java`:

```java
package yier.bubu.redis.execution.executor;

public enum CommandExecutionSuppressionReason {
    CONNECTION_NOT_ACCEPTING,
    PROTOCOL_POISONED,
    INTERNAL_TERMINATING,
    REPLY_SUPPRESSED_AFTER_TERMINAL
}
```

Create `CommandExecutionCompletion.java`:

```java
package yier.bubu.redis.execution.executor;

public sealed interface CommandExecutionCompletion<C extends ExecutionConnection>
        permits CommandExecutionCompletion.Success,
                CommandExecutionCompletion.Suppressed,
                CommandExecutionCompletion.InternalFailure {

    C connection();

    long seq();

    int retainedBytes();

    record Success<C extends ExecutionConnection>(
            C connection,
            long seq,
            byte[] replyBytes,
            boolean closeAfterReplyRequested,
            int retainedBytes,
            long terminalEpochSnapshot
    ) implements CommandExecutionCompletion<C> {
    }

    record Suppressed<C extends ExecutionConnection>(
            C connection,
            long seq,
            CommandExecutionSuppressionReason reason,
            int retainedBytes,
            long terminalEpochSnapshot
    ) implements CommandExecutionCompletion<C> {
    }

    record InternalFailure<C extends ExecutionConnection>(
            C connection,
            long seq,
            byte[] terminalReplyBytes,
            int retainedBytes,
            long terminalEpochSnapshot
    ) implements CommandExecutionCompletion<C> {
    }
}
```

- [ ] **Step 4: Expand `ExecutionConnectionContext` and simplify `ExecutionConnection`**

Update `ExecutionConnection.java`:

```java
package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.Session;

public interface ExecutionConnection {
    String connectionId();

    Session session();

    ExecutionConnectionContext context();
}
```

Add these members to `ExecutionConnectionContext.java`:

```java
private final java.util.concurrent.atomic.AtomicReference<ExecutionConnectionState> state =
        new java.util.concurrent.atomic.AtomicReference<>(ExecutionConnectionState.OPEN);
private final java.util.concurrent.atomic.AtomicLong terminalEpoch = new java.util.concurrent.atomic.AtomicLong(0L);
private final java.util.concurrent.atomic.AtomicLong nextRequestSeq = new java.util.concurrent.atomic.AtomicLong(1L);
private final java.util.concurrent.atomic.AtomicLong nextReplySeq = new java.util.concurrent.atomic.AtomicLong(1L);
private final java.util.concurrent.atomic.AtomicInteger inflight = new java.util.concurrent.atomic.AtomicInteger(0);

public ExecutionConnectionState state() {
    return state.get();
}

public long terminalEpoch() {
    return terminalEpoch.get();
}

public long nextRequestSeq() {
    return nextRequestSeq.get();
}

public long nextReplySeq() {
    return nextReplySeq.get();
}

public long allocateRequestSeq() {
    return nextRequestSeq.getAndIncrement();
}

public boolean beginCloseAfterReply() {
    return state.compareAndSet(ExecutionConnectionState.OPEN, ExecutionConnectionState.CLOSE_AFTER_REPLY);
}

public boolean beginInternalTermination() {
    if (state.compareAndSet(ExecutionConnectionState.OPEN, ExecutionConnectionState.INTERNAL_TERMINATING)
            || state.compareAndSet(ExecutionConnectionState.CLOSE_AFTER_REPLY, ExecutionConnectionState.INTERNAL_TERMINATING)) {
        terminalEpoch.incrementAndGet();
        return true;
    }
    return false;
}

public boolean beginProtocolPoison() {
    if (state.compareAndSet(ExecutionConnectionState.OPEN, ExecutionConnectionState.PROTOCOL_POISONED)
            || state.compareAndSet(ExecutionConnectionState.CLOSE_AFTER_REPLY, ExecutionConnectionState.PROTOCOL_POISONED)
            || state.compareAndSet(ExecutionConnectionState.INTERNAL_TERMINATING, ExecutionConnectionState.PROTOCOL_POISONED)) {
        terminalEpoch.incrementAndGet();
        return true;
    }
    return false;
}
```

- [ ] **Step 5: Re-run the focused executor tests and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test \
  -Dtest=ExecutionConnectionContextTest,CommandExecutorActorContractTest
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionState.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitOutcome.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionCompletion.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionSuppressionReason.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnection.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutionConnectionContextTest.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorActorContractTest.java
git commit -m "refactor: add connection lifecycle and executor contracts"
```

### Task 2: Refactor Executor Core To Emit Completions And Own Reject Cleanup

**Files:**

- Create: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/BufferedReplyBytesSink.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`

- [ ] **Step 1: Write the failing executor behavior tests**

Append to `CommandExecutorTest.java`:

```java
@Test
public void rejectPathClosesRequestAndRollsBackPendingBeforeReturning() {
    RecordingIoAdapter io = new RecordingIoAdapter();
    ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
    CommandExecutor<TestConnection> executor = new CommandExecutor<>(
            () -> {},
            ExecutorCoreTestSupport.simpleCommandEngine(),
            ownerExecutor,
            ExecutorCoreTestSupport.simpleReplyWriterFactory(),
            io,
            new CommandExecutorConfig(1, 4, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
    );
    ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

    TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
    TrackingExecutionRequest first = TrackingExecutionRequest.ofUtf8("PING", "ABCD");
    TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING", "EFGH");

    Assert.assertEquals(CommandExecutorSubmitOutcome.ACCEPTED, executor.submit(connection, first));
    Assert.assertEquals(CommandExecutorSubmitOutcome.REJECTED_BYTES_BUDGET, executor.submit(connection, rejected));
    Assert.assertEquals(1, rejected.closeCalls());
    Assert.assertEquals(1, connection.context().pending());
    Assert.assertEquals(first.retainedBytes(), connection.context().pendingBytes());
}
```

```java
@Test
public void internalTerminationSuppressesQueuedTaskBeforeExecuteAndReturnsCompletion() {
    RecordingIoAdapter io = new RecordingIoAdapter();
    ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
    java.util.List<CommandExecutionCompletion<TestConnection>> completions = new java.util.ArrayList<>();

    CommandExecutor<TestConnection> executor = new CommandExecutor<>(
            () -> {},
            ExecutorCoreTestSupport.simpleCommandEngine(),
            ownerExecutor,
            ExecutorCoreTestSupport.simpleReplyWriterFactory(),
            io,
            new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR),
            completions::add
    );
    ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

    TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
    TrackingExecutionRequest first = TrackingExecutionRequest.ofUtf8("PING");
    TrackingExecutionRequest second = TrackingExecutionRequest.ofUtf8("PING");

    long seq1 = connection.context().allocateRequestSeq();
    long seq2 = connection.context().allocateRequestSeq();
    Assert.assertEquals(CommandExecutorSubmitOutcome.ACCEPTED, executor.submit(connection, seq1, first));
    Assert.assertEquals(CommandExecutorSubmitOutcome.ACCEPTED, executor.submit(connection, seq2, second));

    Assert.assertTrue(connection.context().beginInternalTermination());
    ownerExecutor.runAll();

    Assert.assertEquals(2, completions.size());
    Assert.assertTrue(completions.get(1) instanceof CommandExecutionCompletion.Suppressed);
}
```

- [ ] **Step 2: Run the executor suite and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test \
  -Dtest=CommandExecutorTest
```

Expected: FAIL because `submit(...)` overloads, completion sink plumbing, and reject cleanup behavior do not exist.

- [ ] **Step 3: Add buffered reply bytes and task metadata**

Create `BufferedReplyBytesSink.java`:

```java
package yier.bubu.redis.execution.executor;

import yier.bubu.redis.bytes.BytesSink;

final class BufferedReplyBytesSink implements BytesSink {
    private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(128);

    @Override
    public void writeBytes(byte[] src, int srcIndex, int len) {
        out.write(src, srcIndex, len);
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }
}
```

Update `CommandExecutorTask.java`:

```java
final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final long seq;
    final long terminalEpochSnapshot;
    final ExecutionRequest request;
    final int retainedBytes;

    CommandExecutorTask(C connection, long seq, long terminalEpochSnapshot, ExecutionRequest request, int retainedBytes) {
        this.connection = connection;
        this.seq = seq;
        this.terminalEpochSnapshot = terminalEpochSnapshot;
        this.request = request;
        this.retainedBytes = retainedBytes;
    }
}
```

- [ ] **Step 4: Refactor submitter and execution support to use outcomes and completions**

Update the submit entrypoints in `CommandExecutor.java`:

```java
private final java.util.function.Consumer<CommandExecutionCompletion<C>> completionSink;

public CommandExecutor(
        Runnable bindToCurrentThread,
        CommandExecutionEngine commandProcessor,
        java.util.concurrent.Executor ownerExecutor,
        RedisReplyWriterFactory replyWriterFactory,
        ExecutionIoAdapter<C> ioAdapter,
        CommandExecutorConfig config,
        java.util.function.Consumer<CommandExecutionCompletion<C>> completionSink
) {
    // existing assignments ...
    this.completionSink = java.util.Objects.requireNonNull(completionSink, "completionSink");
}

public CommandExecutorSubmitOutcome submit(C connection, long seq, ExecutionRequest request) {
    return submitter.submit(connection, seq, request, drainLoop::scheduleDrain);
}

public CommandExecutorSubmitOutcome submit(C connection, ExecutionRequest request) {
    long seq = connection.context().allocateRequestSeq();
    return submit(connection, seq, request);
}
```

In `CommandExecutorSubmitter.java`, replace nullable reject returns with explicit outcomes and internal cleanup:

```java
CommandExecutorSubmitOutcome submit(C connection, long seq, ExecutionRequest request, Runnable scheduleDrain) {
    ExecutionConnectionContext context = connection.context();
    if (!running.getAsBoolean()) {
        closeQuietly(request);
        context.recordCommandRejected();
        return CommandExecutorSubmitOutcome.REJECTED_EXECUTOR_NOT_RUNNING;
    }
    if (context.state() != ExecutionConnectionState.OPEN && context.state() != ExecutionConnectionState.CLOSE_AFTER_REPLY) {
        closeQuietly(request);
        context.recordCommandRejected();
        return CommandExecutorSubmitOutcome.REJECTED_CONNECTION_NOT_ACCEPTING;
    }

    int retainedBytes = safeRetainedBytes(request);
    context.recordCommandEnqueued(retainedBytes);
    if (!backlogBudget.tryReserveSlot()) {
        context.recordCommandFinished(retainedBytes, false);
        closeQuietly(request);
        return CommandExecutorSubmitOutcome.REJECTED_QUEUE_FULL;
    }
    if (!backlogBudget.tryReserveQueuedBytes(retainedBytes)) {
        backlogBudget.releaseSlot();
        context.recordCommandFinished(retainedBytes, false);
        closeQuietly(request);
        return CommandExecutorSubmitOutcome.REJECTED_BYTES_BUDGET;
    }

    boolean accepted = taskQueue.offer(connection, new CommandExecutorTask<>(connection, seq, context.terminalEpoch(), request, retainedBytes));
    if (!accepted) {
        backlogBudget.releaseQueuedBytes(retainedBytes);
        backlogBudget.releaseSlot();
        context.recordCommandFinished(retainedBytes, false);
        closeQuietly(request);
        return CommandExecutorSubmitOutcome.REJECTED_INTERNAL;
    }

    scheduleDrain.run();
    return CommandExecutorSubmitOutcome.ACCEPTED;
}
```

In `CommandExecutorExecutionSupport.java`, encode replies into bytes and emit completions:

```java
void execute(CommandExecutorTask<C> task) {
    C connection = task.connection;
    ExecutionConnectionContext context = connection.context();
    if (context.state() == ExecutionConnectionState.PROTOCOL_POISONED
            || context.state() == ExecutionConnectionState.INTERNAL_TERMINATING && context.terminalEpoch() != task.terminalEpochSnapshot) {
        commandsSuppressed.increment();
        completionSink.accept(new CommandExecutionCompletion.Suppressed<>(
                connection,
                task.seq,
                CommandExecutionSuppressionReason.CONNECTION_NOT_ACCEPTING,
                task.retainedBytes,
                task.terminalEpochSnapshot
        ));
        closeRequest(task.request);
        finishTask(connection, task.retainedBytes, false);
        return;
    }

    BufferedReplyBytesSink sink = new BufferedReplyBytesSink();
    RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), sink);
    commandProcessor.execute(connection.session(), task.request, writer);
    completionSink.accept(new CommandExecutionCompletion.Success<>(
            connection,
            task.seq,
            sink.toByteArray(),
            writer.closeAfterReplyRequested(),
            task.retainedBytes,
            task.terminalEpochSnapshot
    ));
    closeRequest(task.request);
    finishTask(connection, task.retainedBytes, true);
}
```

- [ ] **Step 5: Re-run executor tests and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test \
  -Dtest=CommandExecutorTest,CommandExecutorActorContractTest,ExecutionConnectionContextTest
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/BufferedReplyBytesSink.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java
git commit -m "refactor: make executor emit actor completions"
```

### Task 3: Decode RESP Directly Into Final Retained Requests

**Files:**

- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoder.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolViolation.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoderTest.java`
- Delete later in task: `RespCommandAdapter.java`, `RespRequestDecoder.java`, `RespProtocolError.java`, `RespProtocolErrorReplyHandler.java`

- [ ] **Step 1: Write the failing decoder test**

Create `RespExecutionRequestDecoderTest.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

public class RespExecutionRequestDecoderTest {
    @Test
    public void decoderEmitsExecutionRequestDirectlyAndRetainsFrameUntilClose() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespExecutionRequestDecoder(1024, 16, 256));
        try {
            Assert.assertTrue(ch.writeInbound(io.netty.buffer.Unpooled.copiedBuffer(
                    "*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n",
                    java.nio.charset.StandardCharsets.US_ASCII
            )));

            Object inbound = ch.readInbound();
            Assert.assertTrue(inbound instanceof ExecutionRequest);
            ExecutionRequest request = (ExecutionRequest) inbound;
            Assert.assertEquals(2, request.argc());
            Assert.assertEquals(8, request.retainedBytes());
            request.close();
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
```

- [ ] **Step 2: Run the Netty RESP tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty test \
  -Dtest=RespExecutionRequestDecoderTest
```

Expected: FAIL because `RespExecutionRequestDecoder` and `RetainedRespExecutionRequest` do not exist.

- [ ] **Step 3: Add the retained request implementation**

Create `RetainedRespExecutionRequest.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.execution.api.ExecutionRequest;

final class RetainedRespExecutionRequest implements ExecutionRequest {
    private final ByteBuf frame;
    private final int[] offsets;
    private final int[] lengths;
    private final int retainedBytes;

    RetainedRespExecutionRequest(ByteBuf frame, int[] offsets, int[] lengths, int retainedBytes) {
        this.frame = frame.retain();
        this.offsets = offsets;
        this.lengths = lengths;
        this.retainedBytes = retainedBytes;
    }

    @Override
    public int argc() {
        return lengths.length;
    }

    @Override
    public boolean isNull(int index) {
        return false;
    }

    @Override
    public int len(int index) {
        return lengths[index];
    }

    @Override
    public byte byteAt(int index, int offset) {
        return frame.getByte(offsets[index] + offset);
    }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {
        frame.getBytes(offsets[index], dst, dstOff, lengths[index]);
    }

    @Override
    public byte[] toByteArray(int index) {
        byte[] out = new byte[lengths[index]];
        copyToByteArray(index, out, 0);
        return out;
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public void close() {
        ReferenceCountUtil.release(frame);
    }
}
```

- [ ] **Step 4: Replace the old decoder lane with a direct decoder**

Create `RespProtocolViolation.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

public record RespProtocolViolation(String message) {
}
```

Create `RespExecutionRequestDecoder.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.protocol.resp.InlineCommandParser;

public final class RespExecutionRequestDecoder extends ByteToMessageDecoder {
    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxInlineBytes;

    public RespExecutionRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes) {
        this.maxBulkBytes = maxBulkBytes;
        this.maxArgs = maxArgs;
        this.maxInlineBytes = maxInlineBytes;
    }

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, java.util.List<Object> out) {
        if (!in.isReadable()) {
            return;
        }
        int reader = in.readerIndex();
        if (in.getByte(reader) == '*') {
            decodeArray(in, out);
            return;
        }
        decodeInline(in, out);
    }

    private void decodeArray(ByteBuf in, java.util.List<Object> out) {
        int start = in.readerIndex();
        int argcLf = in.forEachByte(start, in.readableBytes(), value -> value != '\n');
        if (argcLf < 0) {
            return;
        }
        long argc = Long.parseLong(in.toString(start + 1, argcLf - start - 2, java.nio.charset.StandardCharsets.US_ASCII));
        if (argc < 0 || argc > maxArgs) {
            in.readerIndex(in.writerIndex());
            out.add(new RespProtocolViolation("ERR Protocol error: too many arguments"));
            return;
        }

        int[] offsets = new int[(int) argc];
        int[] lengths = new int[(int) argc];
        in.readerIndex(argcLf + 1);
        int retainedBytes = 0;
        for (int i = 0; i < argc; i++) {
            int bulkStart = in.readerIndex();
            int bulkLf = in.forEachByte(bulkStart, in.readableBytes(), value -> value != '\n');
            if (bulkLf < 0) {
                in.readerIndex(start);
                return;
            }
            int len = Integer.parseInt(in.toString(bulkStart + 1, bulkLf - bulkStart - 2, java.nio.charset.StandardCharsets.US_ASCII));
            if (len < 0 || len > maxBulkBytes || in.readableBytes() < (bulkLf + 1 - bulkStart) + len + 2) {
                in.readerIndex(in.writerIndex());
                out.add(new RespProtocolViolation("ERR Protocol error: invalid bulk length"));
                return;
            }
            in.readerIndex(bulkLf + 1);
            offsets[i] = in.readerIndex();
            lengths[i] = len;
            retainedBytes += len;
            in.readerIndex(in.readerIndex() + len + 2);
        }
        out.add(new RetainedRespExecutionRequest(in.retainedSlice(start, in.readerIndex() - start), offsets, lengths, retainedBytes));
    }

    private void decodeInline(ByteBuf in, java.util.List<Object> out) {
        int start = in.readerIndex();
        int lf = in.forEachByte(start, in.readableBytes(), value -> value != '\n');
        if (lf < 0) {
            return;
        }
        byte[] line = new byte[lf - start - 1];
        in.getBytes(start, line);
        InlineCommandParser.Decoded decoded = InlineCommandParser.parseUnlimited(line, 0, line.length);
        if (decoded.argc() > maxArgs) {
            in.readerIndex(in.writerIndex());
            out.add(new RespProtocolViolation("ERR Protocol error: too many arguments"));
            return;
        }

        int[] offsets = new int[decoded.argc()];
        int[] lengths = new int[decoded.argc()];
        int cursor = 0;
        byte[][] args = decoded.copyArgs();
        for (int i = 0; i < args.length; i++) {
            offsets[i] = cursor;
            lengths[i] = args[i].length;
            cursor += args[i].length;
        }
        io.netty.buffer.ByteBuf frame = io.netty.buffer.Unpooled.wrappedBuffer(args);
        in.readerIndex(lf + 1);
        out.add(new RetainedRespExecutionRequest(frame, offsets, lengths, decoded.retainedBytes()));
    }
}
```

Delete the old protocol lane classes in the same change:

```bash
rm yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java
rm yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java
rm yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java
rm yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java
```

- [ ] **Step 5: Re-run RESP Netty tests and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty test \
  -Dtest=RespExecutionRequestDecoderTest,RespRequestDecoderTest
```

Expected: PASS after porting the existing decoder assertions to `RespExecutionRequestDecoder`.

Commit:

```bash
git add \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoder.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolViolation.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespExecutionRequestDecoderTest.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java
git rm \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java
git commit -m "refactor: decode resp directly into retained execution requests"
```

### Task 4: Implement The Netty Connection Actor And Reply Gate

**Files:**

- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplyGate.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorHandler.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionActorHandlerTest.java`
- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ActorTestEnv.java`

- [ ] **Step 1: Write the failing actor tests**

Create `ConnectionActorHandlerTest.java`:

```java
package yier.bubu.redis.app.server;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConnectionActorHandlerTest {
    @Test
    public void protocolViolationWritesOneTerminalReplyAndSuppressesLaterCompletionReplies() {
        ActorTestEnv env = new ActorTestEnv();
        EmbeddedChannel channel = env.channel();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 16, 1024);
            channel.writeInbound(new RespProtocolViolation("ERR Protocol error: invalid bulk length"));
            Assert.assertArrayEquals(
                    "-ERR Protocol error: invalid bulk length\r\n".getBytes(StandardCharsets.US_ASCII),
                    env.readOutbound(channel)
            );
            Assert.assertEquals(yier.bubu.redis.execution.executor.ExecutionConnectionState.PROTOCOL_POISONED, connection.context().state());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void queueFullRejectWritesBusyButConnectionNotAcceptingSilentlyDrops() {
        ActorTestEnv env = new ActorTestEnv();
        EmbeddedChannel channel = env.channel();
        try {
            channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));
            Assert.assertArrayEquals("-ERR busy queue_full\r\n".getBytes(StandardCharsets.US_ASCII), env.readOutbound(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
```

- [ ] **Step 2: Run the server-main actor test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=ConnectionActorHandlerTest
```

Expected: FAIL because `ConnectionActorHandler`, `ConnectionReplyGate`, and the actor test harness do not exist.

- [ ] **Step 3: Add the reply gate**

Create `ConnectionReplyGate.java`:

```java
package yier.bubu.redis.app.server;

import yier.bubu.redis.execution.executor.ExecutionConnectionState;

final class ConnectionReplyGate {
    boolean canWriteNormalReply(ExecutionConnectionState state) {
        return state == ExecutionConnectionState.OPEN || state == ExecutionConnectionState.CLOSE_AFTER_REPLY;
    }

    boolean canWriteTerminalProtocolReply(ExecutionConnectionState state) {
        return state == ExecutionConnectionState.OPEN
                || state == ExecutionConnectionState.CLOSE_AFTER_REPLY
                || state == ExecutionConnectionState.INTERNAL_TERMINATING;
    }

    boolean canWriteTerminalInternalReply(ExecutionConnectionState state) {
        return state == ExecutionConnectionState.OPEN || state == ExecutionConnectionState.CLOSE_AFTER_REPLY;
    }
}
```

- [ ] **Step 4: Add the actor handler and upgrade `NettyExecutionConnection`**

Create `ConnectionActorHandler.java`:

```java
package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutionCompletion;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorSubmitOutcome;
import yier.bubu.redis.execution.executor.ExecutionConnectionState;
import yier.bubu.redis.protocol.resp.netty.RespProtocolViolation;

final class ConnectionActorHandler extends SimpleChannelInboundHandler<Object> {
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final RedisReplyWriterFactory replyWriterFactory;
    private final ConnectionReplyGate replyGate = new ConnectionReplyGate();

    ConnectionActorHandler(CommandExecutor<NettyExecutionConnection> executor, RedisReplyWriterFactory replyWriterFactory) {
        super(false);
        this.executor = executor;
        this.replyWriterFactory = replyWriterFactory;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ctx.channel(), 16, 1024);
        if (msg instanceof RespProtocolViolation violation) {
            beginProtocolPoison(ctx, connection, violation.message());
            return;
        }
        if (!(msg instanceof ExecutionRequest request)) {
            return;
        }

        CommandExecutorSubmitOutcome outcome = executor.submit(connection, request);
        if (outcome == CommandExecutorSubmitOutcome.ACCEPTED) {
            return;
        }
        if (outcome == CommandExecutorSubmitOutcome.REJECTED_QUEUE_FULL) {
            writeError(ctx, connection, "ERR busy queue_full", false);
            return;
        }
        if (outcome == CommandExecutorSubmitOutcome.REJECTED_BYTES_BUDGET) {
            writeError(ctx, connection, "ERR busy bytes_budget", false);
            return;
        }
        if (outcome == CommandExecutorSubmitOutcome.REJECTED_EXECUTOR_NOT_RUNNING
                || outcome == CommandExecutorSubmitOutcome.REJECTED_INTERNAL) {
            beginInternalTermination(ctx, connection);
        }
    }

    void onCompletion(ChannelHandlerContext ctx, CommandExecutionCompletion<NettyExecutionConnection> completion) {
        NettyExecutionConnection connection = completion.connection();
        if (completion instanceof CommandExecutionCompletion.Success<NettyExecutionConnection> success) {
            if (!replyGate.canWriteNormalReply(connection.context().state())) {
                connection.context().recordCommandCompletion(false);
                return;
            }
            ByteBuf out = ctx.alloc().buffer(success.replyBytes().length);
            out.writeBytes(success.replyBytes());
            if (success.closeAfterReplyRequested()) {
                connection.context().beginCloseAfterReply();
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.writeAndFlush(out);
            }
            connection.context().recordCommandCompletion(true);
            return;
        }
        if (completion instanceof CommandExecutionCompletion.Suppressed<NettyExecutionConnection>) {
            connection.context().recordCommandSuppressed();
            connection.context().recordCommandCompletion(false);
            return;
        }
        if (completion instanceof CommandExecutionCompletion.InternalFailure<NettyExecutionConnection> failure) {
            beginInternalTermination(ctx, connection);
            connection.context().recordCommandCompletion(false);
        }
    }

    private void beginProtocolPoison(ChannelHandlerContext ctx, NettyExecutionConnection connection, String message) {
        if (connection.context().beginProtocolPoison() && replyGate.canWriteTerminalProtocolReply(ExecutionConnectionState.OPEN)) {
            writeError(ctx, connection, message, true);
        }
    }

    private void beginInternalTermination(ChannelHandlerContext ctx, NettyExecutionConnection connection) {
        if (connection.context().beginInternalTermination()) {
            writeError(ctx, connection, "ERR internal error", true);
        }
    }

    private void writeError(ChannelHandlerContext ctx, NettyExecutionConnection connection, String message, boolean close) {
        ByteBuf out = ctx.alloc().buffer();
        RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), new NettyByteBufSink(out));
        writer.error(message);
        if (close) {
            ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.writeAndFlush(out);
        }
    }
}
```

Update `NettyExecutionConnection.java` to remove `markClosing()` and add actor-owned sequence helpers:

```java
long allocateRequestSeq() {
    return context.allocateRequestSeq();
}

void markClosed() {
    context.markClosed();
    session.discardTransaction();
}
```

Create `ActorTestEnv.java`:

```java
package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;

final class ActorTestEnv {
    final RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();

    EmbeddedChannel channel() {
        return new EmbeddedChannel(new ConnectionActorHandler(null, replyWriterFactory));
    }

    byte[] readOutbound(EmbeddedChannel channel) {
        ByteBuf out = channel.readOutbound();
        Assert.assertNotNull(out);
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }
}
```

- [ ] **Step 5: Re-run the actor test and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=ConnectionActorHandlerTest
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionReplyGate.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorHandler.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ConnectionActorHandlerTest.java
git commit -m "refactor: add netty connection actor and reply gate"
```

### Task 5: Cut The Production Pipeline Over To The Actor Path

**Files:**

- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorCompletionBus.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NoopExecutionIoAdapter.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`

- [ ] **Step 1: Write the failing pipeline integration assertions**

Update `NettyExecutionAdapterIntegrationTest.java` to expect the new pipeline:

```java
@Test
public void channelInitializerWiresDecoderDirectlyIntoConnectionActor() throws Exception {
    try (InitializerTestEnv env = new InitializerTestEnv()) {
        io.netty.channel.socket.nio.NioSocketChannel channel = new io.netty.channel.socket.nio.NioSocketChannel();
        try {
            new YierdisServerChannelInitializer(runtimeConfig(0, 0, 3, 2, 4), env.executor, env.replyWriterFactory)
                    .initChannel(channel);

            java.util.List<String> names = channel.pipeline().names();
            Assert.assertTrue(names.contains("respExecutionRequestDecoder"));
            Assert.assertTrue(names.contains("connectionActor"));
            Assert.assertFalse(names.contains("respCommandAdapter"));
            Assert.assertFalse(names.contains("respProtocolErrorReply"));
            Assert.assertFalse(names.contains("commandHandler"));
        } finally {
            channel.unsafe().closeForcibly();
        }
    }
}
```

- [ ] **Step 2: Run the server-main integration tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest,YierdisServerBootstrapCommandWiringTest
```

Expected: FAIL because the channel initializer and bootstrap still build the old handler chain.

- [ ] **Step 3: Switch the server bootstrap to actor completions**

In `YierdisServerBootstrap.java`, build the executor with an actor completion sink:

```java
ConnectionActorCompletionBus completionBus = new ConnectionActorCompletionBus();
executor = new CommandExecutor<>(
        runtimeAccess::bindToCurrentThread,
        commandEngine::execute,
        commandGroup.next(),
        replyWriterFactory,
        new NoopExecutionIoAdapter<>(),
        executorConfig,
        completionBus::publish
);
```

Add a small bus in server-main that schedules completions back onto `connection.channel().eventLoop()`:

```java
final class ConnectionActorCompletionBus {
    void publish(CommandExecutionCompletion<NettyExecutionConnection> completion) {
        NettyExecutionConnection connection = completion.connection();
        connection.channel().eventLoop().execute(() -> connection.actor().onCompletion(completion));
    }
}
```

Create `NoopExecutionIoAdapter.java`:

```java
package yier.bubu.redis.app.server;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.executor.ExecutionIoAdapter;

final class NoopExecutionIoAdapter<C extends yier.bubu.redis.execution.executor.ExecutionConnection> implements ExecutionIoAdapter<C> {
    @Override
    public boolean isActive(C connection) {
        return true;
    }

    @Override
    public boolean isWritable(C connection) {
        return true;
    }

    @Override
    public void disableInput(C connection) {
    }

    @Override
    public void enableInput(C connection) {
    }

    @Override
    public void onClose(C connection, Runnable callback) {
    }

    @Override
    public BytesSink newReplySink(C connection) {
        throw new UnsupportedOperationException("actor path buffers replies inside executor");
    }

    @Override
    public void writeBufferedReply(C connection, boolean closeAfterReply) {
    }

    @Override
    public void flushPending(Iterable<C> touchedConnections) {
    }
}
```

- [ ] **Step 4: Replace the old pipeline and delete obsolete handlers**

Update `YierdisServerChannelInitializer.java`:

```java
ch.pipeline()
        .addLast("respExecutionRequestDecoder", new RespExecutionRequestDecoder(
                config.protocolMaxBulkBytes(),
                config.protocolMaxArgs(),
                config.protocolMaxLineBytes()
        ))
        .addLast("connectionActor", new ConnectionActorHandler(executor, replyWriterFactory));
```

Delete the old server-main handler classes:

```bash
rm yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java
rm yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java
```

- [ ] **Step 5: Re-run pipeline/integration tests and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=ConnectionActorHandlerTest,NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest,YierdisServerBootstrapCommandWiringTest
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ConnectionActorCompletionBus.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NoopExecutionIoAdapter.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java
git rm \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java
git commit -m "refactor: switch server pipeline to connection actor"
```

### Task 6: Rebuild STATS And Backpressure Around Actor-Owned State

**Files:**

- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Write the failing stats assertions**

Append to `YierdisServerBootstrapCommandWiringTest.java`:

```java
@Test
public void statsExposeConnectionActorLifecycleFields() throws Exception {
    try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0")) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            java.io.OutputStream out = socket.getOutputStream();
            java.io.InputStream in = socket.getInputStream();

            java.util.Map<String, Object> stats = respMap(roundTrip(out, in, "STATS"));
            Assert.assertTrue(stats.containsKey("conn_state"));
            Assert.assertTrue(stats.containsKey("conn_inflight"));
            Assert.assertTrue(stats.containsKey("conn_next_seq"));
            Assert.assertTrue(stats.containsKey("conn_next_reply_seq"));
            Assert.assertTrue(stats.containsKey("terminal_protocol_total"));
            Assert.assertTrue(stats.containsKey("reply_suppressed_total"));
        }
    }
}
```

- [ ] **Step 2: Run the STATS tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=YierdisServerBootstrapCommandWiringTest
```

Expected: FAIL because the new actor/session stats keys are not emitted yet.

- [ ] **Step 3: Expand `ExecutionConnectionContext` metrics**

Add fields and counters to `ExecutionConnectionContext.java`:

```java
private final java.util.concurrent.atomic.AtomicLong commandsStarted = new java.util.concurrent.atomic.AtomicLong(0L);
private final java.util.concurrent.atomic.AtomicLong commandsCompleted = new java.util.concurrent.atomic.AtomicLong(0L);
private final java.util.concurrent.atomic.AtomicLong commandsSuppressed = new java.util.concurrent.atomic.AtomicLong(0L);
private final java.util.concurrent.atomic.AtomicLong repliesWritten = new java.util.concurrent.atomic.AtomicLong(0L);
private final java.util.concurrent.atomic.AtomicLong repliesSuppressed = new java.util.concurrent.atomic.AtomicLong(0L);

public void recordCommandStarted() {
    inflight.incrementAndGet();
    commandsStarted.incrementAndGet();
}

public void recordCommandCompletion(boolean replyWritten) {
    inflight.decrementAndGet();
    commandsCompleted.incrementAndGet();
    if (replyWritten) {
        repliesWritten.incrementAndGet();
    } else {
        repliesSuppressed.incrementAndGet();
    }
}

public void recordCommandSuppressed() {
    commandsSuppressed.incrementAndGet();
}
```

- [ ] **Step 4: Emit the new keys from `NettyServerInfoProvider`**

Update the `stats(...)` method in `NettyServerInfoProvider.java`:

```java
writePair(out, ascii("terminal_protocol_total"), s.terminalProtocol());
writePair(out, ascii("terminal_internal_total"), s.terminalInternal());
writePair(out, ascii("reply_suppressed_total"), s.replySuppressed());
writePair(out, ascii("conn_state"), ascii(String.valueOf(stats.state()).toLowerCase(java.util.Locale.ROOT)));
writePair(out, ascii("conn_inflight"), stats.inflight());
writePair(out, ascii("conn_next_seq"), stats.nextRequestSeq());
writePair(out, ascii("conn_next_reply_seq"), stats.nextReplySeq());
writePair(out, ascii("conn_terminal_epoch"), stats.terminalEpoch());
```

- [ ] **Step 5: Re-run stats/backpressure tests and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test \
  -Dtest=CommandExecutorBackpressureTest
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test \
  -Dtest=YierdisServerBootstrapCommandWiringTest
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java \
  yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: expose actor session stats and backpressure state"
```

### Task 7: Remove The Legacy Request Lane And Tighten Architecture Guards

**Files:**

- Delete: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
- Delete: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`
- Delete: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `docs/project-docs/executor-and-backpressure.md`

- [ ] **Step 1: Write the failing architecture and doc assertions**

Add to `ArchitectureBoundaryTest.java`:

```java
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java").normalize(),
        offenders,
        "RespCommandAdapter",
        "RespProtocolErrorReplyHandler",
        "YierdisFastCommandHandler"
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp").normalize(),
        offenders,
        "class RespCommandRequest",
        "class RespExecutionAdapter"
);
```

- [ ] **Step 2: Run the architecture tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests test \
  -Dtest=ArchitectureBoundaryTest
```

Expected: FAIL while the deleted classes and old pipeline names still exist.

- [ ] **Step 3: Delete the legacy request-adaptation classes**

Remove the old RESP adaptation lane:

```bash
rm yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java
rm yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java
rm yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java
```

- [ ] **Step 4: Update architecture guards and project docs**

Update `request-execution-flow.md` to describe:

```markdown
- `RespExecutionRequestDecoder` emits final `ExecutionRequest` objects directly.
- `ConnectionActorHandler` is the only I/O boundary for submit, reject, terminal reply, and reply suppression.
- `CommandExecutor` returns completions to the actor instead of writing the channel directly.
```

Update `executor-and-backpressure.md` to describe:

```markdown
- transport auto-read and reply gating are actor-owned
- executor backpressure outcomes are surfaced as typed submit outcomes
- terminal transitions suppress later replies according to actor state
```

- [ ] **Step 5: Re-run architecture/docs-linked suites and commit**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests test \
  -Dtest=ArchitectureBoundaryTest
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-resp test
```

Expected: PASS.

Commit:

```bash
git add \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  docs/project-docs/request-execution-flow.md \
  docs/project-docs/executor-and-backpressure.md
git rm \
  yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java \
  yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java \
  yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java
git commit -m "refactor: remove legacy resp adaptation lane"
```

### Task 8: Run The Full Focused Verification Suite

**Files:**

- No code changes. Verification only.

- [ ] **Step 1: Run networking tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking test
```

Expected: PASS.

- [ ] **Step 2: Run executor tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-executor test
```

Expected: PASS.

- [ ] **Step 3: Run server-main tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main test
```

Expected: PASS.

- [ ] **Step 4: Run architecture and integration suites**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests test
```

Expected: PASS.

- [ ] **Step 5: Create the final implementation commit**

Run:

```bash
git status --short
git add -A
git commit -m "refactor: rewrite connection io around actor sessions"
```

Expected: a clean working tree after the final commit.
