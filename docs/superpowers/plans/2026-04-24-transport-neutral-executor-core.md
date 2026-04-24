# Transport-Neutral Executor Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move command execution runtime out of `yierdis-server` into a transport-neutral `yierdis-executor-core`, then reduce `yierdis-server` to Netty protocol and transport adapters.

**Architecture:** The refactor introduces executor-core-owned connection/session/runtime abstractions plus a new `CommandExecutor` that owns submission, drain, backpressure, maintenance, and stats. `yierdis-server` will replace `NettyCommandExecutor` and `ServerConnectionContext` with thin Netty adapters that translate `Channel` events into executor-core interfaces while keeping reply authority with `ReplyWriterFactory(BytesSink)`.

**Tech Stack:** Java 25, Maven, Netty 4.1, JUnit 4, existing `ReplyWriter` / `ExecutionRequest` contracts

---

## File Structure

### Create

- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnection.java`
  Transport-neutral executor key and context holder.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionIoAdapter.java`
  Transport boundary for input control, reply buffering, flush, and close callbacks.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnectionContext.java`
  Pending counters, closing state, queue state, and per-connection stats snapshot.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/DefaultExecutionSession.java`
  Transport-neutral `ServerSession` implementation with transaction queue limits.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorConfig.java`
  Executor-core config record replacing server-owned config type.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
  Public executor runtime replacing `NettyCommandExecutor`.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorTask.java`
  Internal queued task payload for executor core.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorSubmitter.java`
  Submission, rejection, and backlog-budget path.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorDrainLoop.java`
  Single-thread execution loop and flush lifecycle.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
  `CommandContext`, session, reply writer, close-after-reply, and completion bookkeeping.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutionConnectionContextTest.java`
  New home for generic context and session tests.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`
  New home for queue, closing, and execution lifecycle tests.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java`
  New home for input disable/enable and byte-budget tests.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java`
  New home for fair scheduling tests.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java`
  Shared test doubles for requests, processors, reply writers, connections, and adapters.
- `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionConnection.java`
  Netty wrapper implementing `ExecutionConnection`.
- `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionIoAdapter.java`
  Netty adapter implementing `ExecutionIoAdapter<NettyExecutionConnection>`.
- `yierdis-server/src/main/java/yier/bubu/redis/CommandExecutorConfigs.java`
  Maps `YierdisServerRuntimeConfig` to executor-core `CommandExecutorConfig`.
- `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`
  Focused Netty adapter test that stays in server.

### Modify

- `yierdis-executor-core/pom.xml`
  Add dependencies on `yierdis-core-contract` and `yierdis-core-command`.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  Submit to `CommandExecutor<NettyExecutionConnection>` and stop depending on executor-owned reply helpers.
- `yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java`
  Use `ReplyWriterFactory` directly for protocol/internal error replies.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
  Create/attach `NettyExecutionConnection` instead of `ServerConnectionContext`.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  Construct `CommandExecutor`, Netty adapter, and new config mapper.
- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
  Bind to `CommandExecutor<?>` and fetch connection stats from executor-core session/context.
- `yierdis-server/src/main/java/yier/bubu/redis/NettyReplyFlushBatch.java`
  Retain as the flush coalescer used by `NettyExecutionIoAdapter`.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  Add guards for executor-core being Netty-free and server not re-owning runtime state.
- `docs/module-architecture.md`
- `docs/request-execution-flow.md`
- `docs/executor-and-backpressure.md`
- `docs/project-overview.md`
- `docs/main-path-walkthrough.md`
- `docs/configuration-and-operations.md`
- `docs/development-navigation.md`

### Delete

- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java`

### Test Commands Used Throughout

- `jdk25 mvn -pl yierdis-executor-core -Dtest=ExecutionConnectionContextTest test`
- `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest test`
- `jdk25 mvn -pl yierdis-server -Dtest=NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest test`
- `jdk25 mvn test`

### Task 1: Introduce Executor-Core Connection And Session Types

**Files:**
- Modify: `yierdis-executor-core/pom.xml`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnection.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnectionContext.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/DefaultExecutionSession.java`
- Test: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutionConnectionContextTest.java`

- [ ] **Step 1: Write the failing context/session tests**

```java
package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.List;

public class ExecutionConnectionContextTest {
    @Test
    public void connectionContextTracksPendingBytesAndClosing() {
        DefaultExecutionSession session = new DefaultExecutionSession(2, 32);
        ExecutionConnectionContext context = new ExecutionConnectionContext(session);

        Assert.assertSame(session, context.session());
        Assert.assertEquals(0, context.pending());
        Assert.assertEquals(0L, context.pendingBytes());
        Assert.assertFalse(context.isClosing());

        context.recordCommandEnqueued(12);
        context.recordCommandEnqueued(8);
        Assert.assertEquals(2, context.pending());
        Assert.assertEquals(20L, context.pendingBytes());

        context.recordCommandFinished(12, true);
        Assert.assertEquals(1, context.pending());
        Assert.assertEquals(8L, context.pendingBytes());

        Assert.assertTrue(context.markClosing());
        Assert.assertFalse(context.markClosing());
        Assert.assertTrue(context.isClosing());
    }

    @Test
    public void sessionTransactionHonorsQueueLimitsAndDiscardsOnClosing() {
        DefaultExecutionSession session = new DefaultExecutionSession(1, 16);
        ExecutionConnectionContext context = new ExecutionConnectionContext(session);
        session.transaction().begin();

        ExecutionRequest first = TestExecutionRequests.ofUtf8("SET", "k", "v");
        ExecutionRequest second = TestExecutionRequests.ofUtf8("PING");

        Assert.assertNull(session.transaction().tryEnqueue(first));
        Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(second));
        Assert.assertTrue(session.transaction().aborted());

        context.markClosing();
        Assert.assertFalse(session.transaction().active());
        Assert.assertEquals(List.of(), session.transaction().drain());
    }
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=ExecutionConnectionContextTest test`
Expected: FAIL with compilation errors for missing `DefaultExecutionSession`, `ExecutionConnectionContext`, and `TestExecutionRequests`.

- [ ] **Step 3: Write the minimal connection/session implementation**

```java
package yier.bubu.redis.executor;

public interface ExecutionConnection {
    String connectionId();
    ExecutionConnectionContext context();
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ServerSession;

public final class ExecutionConnectionContext {
    private final DefaultExecutionSession session;
    private final ExecutorKeyState queueState = new ExecutorKeyState();
    private int pending;
    private long pendingBytes;
    private boolean closing;
    private boolean inputDisabledByExecutor;
    private long commandsEnqueued;
    private long commandsExecuted;
    private long commandsRejected;
    private long commandsSkippedClosing;
    private long closeAfterReply;
    private long backpressureEnter;
    private long backpressureExit;

    public ExecutionConnectionContext(DefaultExecutionSession session) {
        this.session = session;
        this.session.attach(this);
    }

    public ServerSession session() {
        return session;
    }

    public ExecutorKeyState queueState() {
        return queueState;
    }

    public int pending() {
        return pending;
    }

    public long pendingBytes() {
        return pendingBytes;
    }

    public boolean isClosing() {
        return closing;
    }

    public boolean markClosing() {
        if (closing) {
            return false;
        }
        closing = true;
        session.discardTransaction();
        return true;
    }

    public void recordCommandEnqueued(int retainedBytes) {
        pending++;
        pendingBytes += Math.max(0, retainedBytes);
        commandsEnqueued++;
    }

    public void recordCommandFinished(int retainedBytes, boolean executed) {
        pending--;
        pendingBytes -= Math.max(0, retainedBytes);
        if (executed) {
            commandsExecuted++;
        }
    }

    public void recordCommandRejected() {
        commandsRejected++;
    }

    public void recordSkippedClosing() {
        commandsSkippedClosing++;
    }

    public void recordCloseAfterReply() {
        closeAfterReply++;
    }

    public void recordBackpressureEnter() {
        backpressureEnter++;
    }

    public void recordBackpressureExit() {
        backpressureExit++;
    }

    public boolean markInputDisabledByExecutor() {
        if (inputDisabledByExecutor) {
            return false;
        }
        inputDisabledByExecutor = true;
        return true;
    }

    public boolean clearInputDisabledByExecutor() {
        if (!inputDisabledByExecutor) {
            return false;
        }
        inputDisabledByExecutor = false;
        return true;
    }

    public boolean inputDisabledByExecutor() {
        return inputDisabledByExecutor;
    }

    public ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                pending,
                pendingBytes,
                inputDisabledByExecutor,
                closing,
                commandsEnqueued,
                commandsExecuted,
                commandsRejected,
                commandsSkippedClosing,
                closeAfterReply,
                backpressureEnter,
                backpressureExit
        );
    }

    public record ConnectionStatsSnapshot(
            int pending,
            long pendingBytes,
            boolean inputDisabledByExecutor,
            boolean closing,
            long commandsEnqueued,
            long commandsExecuted,
            long commandsRejected,
            long commandsSkippedClosing,
            long closeAfterReply,
            long backpressureEnter,
            long backpressureExit
    ) {
    }
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class DefaultExecutionSession implements ServerSession {
    private static final AtomicLong NEXT_CLIENT_ID = new AtomicLong(1);

    private final long clientId = NEXT_CLIENT_ID.getAndIncrement();
    private final DefaultTransactionState transaction;
    private ExecutionConnectionContext connectionContext;
    private int dbIndex;
    private String clientName;
    private boolean authenticated;

    public DefaultExecutionSession(int maxQueuedCommands, long maxQueuedBytes) {
        this.transaction = new DefaultTransactionState(maxQueuedCommands, maxQueuedBytes);
    }

    void attach(ExecutionConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
    }

    public ExecutionConnectionContext connectionContext() {
        return connectionContext;
    }

    void discardTransaction() {
        transaction.discard();
    }

    @Override public int dbIndex() { return dbIndex; }
    @Override public void setDbIndex(int dbIndex) { this.dbIndex = Math.max(0, dbIndex); }
    @Override public long clientId() { return clientId; }
    @Override public String clientName() { return clientName; }
    @Override public void setClientName(String clientName) { this.clientName = clientName; }
    @Override public boolean authenticated() { return authenticated; }
    @Override public void setAuthenticated(boolean authenticated) { this.authenticated = authenticated; }
    @Override public TransactionState transaction() { return transaction; }

    private static final class DefaultTransactionState implements TransactionState {
        private final int maxQueuedCommands;
        private final long maxQueuedBytes;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();
        private boolean active;
        private boolean aborted;
        private long queuedBytes;

        private DefaultTransactionState(int maxQueuedCommands, long maxQueuedBytes) {
            this.maxQueuedCommands = Math.max(0, maxQueuedCommands);
            this.maxQueuedBytes = Math.max(0, maxQueuedBytes);
        }

        @Override public synchronized boolean active() { return active; }
        @Override public synchronized boolean aborted() { return aborted; }
        @Override public synchronized void markAborted() { aborted = true; }
        @Override public synchronized void begin() { active = true; aborted = false; queuedBytes = 0; queue.clear(); }
        @Override public synchronized void discard() { active = false; aborted = false; queuedBytes = 0; queue.clear(); }
        @Override public synchronized void enqueue(ExecutionRequest request) { tryEnqueue(request); }
        @Override public synchronized int size() { return queue.size(); }

        @Override
        public synchronized String tryEnqueue(ExecutionRequest request) {
            if (maxQueuedCommands > 0 && queue.size() >= maxQueuedCommands) {
                aborted = true;
                return "ERR Transaction queue is full";
            }
            ExecutionRequest snapshot = ByteArrayExecutionRequest.copyOf(request);
            long requestBytes = Math.max(0L, snapshot.retainedBytes());
            if (maxQueuedBytes > 0 && queuedBytes + requestBytes > maxQueuedBytes) {
                aborted = true;
                snapshot.close();
                return "ERR Transaction queue is full";
            }
            queue.add(snapshot);
            queuedBytes += requestBytes;
            return null;
        }

        @Override
        public synchronized List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            queuedBytes = 0;
            return out;
        }
    }
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Arrays;

final class TestExecutionRequests {
    static ExecutionRequest ofUtf8(String... argv) {
        return ByteArrayExecutionRequest.fromUtf8(argv[0], Arrays.asList(Arrays.copyOfRange(argv, 1, argv.length)));
    }
}
```

```xml
<dependencies>
    <dependency>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-core-contract</artifactId>
    </dependency>
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

- [ ] **Step 4: Run the context/session test to verify it passes**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=ExecutionConnectionContextTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-executor-core/pom.xml \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnection.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnectionContext.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/DefaultExecutionSession.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutionConnectionContextTest.java
git commit -m "refactor: add executor core connection and session types"
```

### Task 2: Add Executor-Core I/O Boundary And Config

**Files:**
- Modify: `yierdis-executor-core/pom.xml`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionIoAdapter.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorConfig.java`
- Test: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`

- [ ] **Step 1: Write the failing config and I/O boundary test**

```java
package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

import java.util.concurrent.atomic.AtomicBoolean;

public class CommandExecutorTest {
    @Test
    public void configExposesQueueDrainAndBackpressureSettings() {
        CommandExecutorConfig config = new CommandExecutorConfig(32, 1024, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR);

        Assert.assertEquals(32, config.queueCapacity());
        Assert.assertEquals(1024L, config.queueMaxBytes());
        Assert.assertEquals(8, config.backpressureHighWatermark());
        Assert.assertEquals(4, config.backpressureLowWatermark());
        Assert.assertEquals(128L, config.backpressureBytesHighWatermark());
        Assert.assertEquals(64L, config.backpressureBytesLowWatermark());
        Assert.assertEquals(16, config.maxDrainCommands());
        Assert.assertEquals(10L, config.drainTimeLimitMillis());
        Assert.assertEquals(SchedulingPolicy.FAIR, config.schedulingPolicy());
    }

    @Test
    public void ioAdapterContractCanBufferFlushAndCloseOneConnection() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = new TestConnection("c-1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 128)));
        AtomicBoolean closed = new AtomicBoolean(false);

        io.onClose(connection, () -> closed.set(true));
        io.disableInput(connection);
        io.enableInput(connection);
        BytesSink sink = io.newReplySink(connection);
        sink.writeByte((byte) 'O');
        sink.writeByte((byte) 'K');
        io.writeBufferedReply(connection, true);
        io.fireClosed();

        Assert.assertEquals("OK", io.bufferedReply());
        Assert.assertTrue(io.closeAfterReply());
        Assert.assertTrue(closed.get());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorTest test`
Expected: FAIL with compilation errors for missing `ExecutionIoAdapter`, `CommandExecutorConfig`, `RecordingIoAdapter`, and `TestConnection`.

- [ ] **Step 3: Write the minimal config and I/O abstractions**

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

public interface ExecutionIoAdapter<C extends ExecutionConnection> {
    boolean isActive(C connection);
    boolean isWritable(C connection);
    void disableInput(C connection);
    void enableInput(C connection);
    void onClose(C connection, Runnable callback);
    BytesSink newReplySink(C connection);
    void writeBufferedReply(C connection, boolean closeAfterReply);
    void flushPending(Iterable<C> touchedConnections);
}
```

```java
package yier.bubu.redis.executor;

public record CommandExecutorConfig(
        int queueCapacity,
        long queueMaxBytes,
        int backpressureHighWatermark,
        int backpressureLowWatermark,
        long backpressureBytesHighWatermark,
        long backpressureBytesLowWatermark,
        int maxDrainCommands,
        long drainTimeLimitMillis,
        SchedulingPolicy schedulingPolicy
) {
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;

final class TestConnection implements ExecutionConnection {
    private final String connectionId;
    private final ExecutionConnectionContext context;

    TestConnection(String connectionId, ExecutionConnectionContext context) {
        this.connectionId = connectionId;
        this.context = context;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    @Override
    public ExecutionConnectionContext context() {
        return context;
    }
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class ExecutorCoreTestSupport {
    static ExecutionRequest ofUtf8(String... argv) {
        return ByteArrayExecutionRequest.fromUtf8(argv[0], Arrays.asList(Arrays.copyOfRange(argv, 1, argv.length)));
    }

    static YierdisFastCommandProcessor pingOnlyProcessor() {
        YierdisDbRouter router = dbIndexProvider -> { throw new AssertionError("db should not be used in ping-only tests"); };
        return new YierdisFastCommandProcessor(router, null);
    }

    static ReplyWriterFactory ndjsonReplyWriterFactory() {
        return out -> new ReplyWriter() {
            private boolean closeAfterReply;

            @Override public void simpleString(String value) { write("{\"ok\":true,\"result\":\"" + value + "\"}\n"); }
            @Override public void error(String message) { write("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"" + message + "\"}}\n"); }
            @Override public void protocolError(String message) { error(message); }
            @Override public void internalError(String message) { error(message); }
            @Override public void bulkString(byte[] value) { write(new String(value, StandardCharsets.UTF_8)); }
            @Override public void arrayHeader(int len) { }
            @Override public void mapHeader(int len) { }
            @Override public void integer(long value) { write(Long.toString(value)); }
            @Override public void nullBulkString() { write("null"); }
            @Override public void requestCloseAfterReply() { closeAfterReply = true; }
            @Override public boolean closeAfterReplyRequested() { return closeAfterReply; }

            private void write(String text) {
                byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                out.writeBytes(bytes, 0, bytes.length);
            }
        };
    }

}

final class TrackingExecutionRequest implements ExecutionRequest {
    private final ExecutionRequest delegate;
    private int closeCalls;

    private TrackingExecutionRequest(ExecutionRequest delegate) {
        this.delegate = delegate;
    }

    static TrackingExecutionRequest ofUtf8(String... argv) {
        return new TrackingExecutionRequest(ExecutorCoreTestSupport.ofUtf8(argv));
    }

    int closeCalls() {
        return closeCalls;
    }

    @Override public int argc() { return delegate.argc(); }
    @Override public boolean isNull(int index) { return delegate.isNull(index); }
    @Override public int len(int index) { return delegate.len(index); }
    @Override public byte byteAt(int index, int offset) { return delegate.byteAt(index, offset); }
    @Override public void copyToByteArray(int index, byte[] dst, int dstOff) { delegate.copyToByteArray(index, dst, dstOff); }
    @Override public byte[] toByteArray(int index) { return delegate.toByteArray(index); }
    @Override public int retainedBytes() { return delegate.retainedBytes(); }
    @Override public void close() { closeCalls++; delegate.close(); }
}

final class RecordingIoAdapter implements ExecutionIoAdapter<TestConnection> {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<String> order = new ArrayList<>();
    private Runnable closeCallback = () -> {};
    private boolean closeAfterReply;
    private boolean inputDisabled;
    private boolean inputEnabledAgain;

    @Override public boolean isActive(TestConnection connection) { return true; }
    @Override public boolean isWritable(TestConnection connection) { return true; }
    @Override public void disableInput(TestConnection connection) { inputDisabled = true; }
    @Override public void enableInput(TestConnection connection) { inputEnabledAgain = true; }
    @Override public void onClose(TestConnection connection, Runnable callback) { this.closeCallback = callback; }
    @Override public void writeBufferedReply(TestConnection connection, boolean closeAfterReply) {
        this.closeAfterReply = closeAfterReply;
        order.add(connection.connectionId());
    }
    @Override public void flushPending(Iterable<TestConnection> touchedConnections) { }

    @Override
    public BytesSink newReplySink(TestConnection connection) {
        return new BytesSink() {
            @Override public void writeByte(byte value) { bytes.write(value); }
            @Override public void writeBytes(byte[] src, int off, int len) { bytes.write(src, off, len); }
        };
    }

    String bufferedReply() { return bytes.toString(); }
    boolean closeAfterReply() { return closeAfterReply; }
    boolean inputDisabled() { return inputDisabled; }
    boolean inputEnabledAgain() { return inputEnabledAgain; }
    String executionOrder() { return String.join(",", order); }
    byte[] takeLastReply() { return bytes.toByteArray(); }
    void completePendingFlushes() { inputEnabledAgain = true; }
    void fireClosed() { closeCallback.run(); }
}
```

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-core-command</artifactId>
</dependency>
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-executor-core/pom.xml \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionIoAdapter.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorConfig.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java
git commit -m "refactor: add executor core io boundary and config"
```

### Task 3: Build The Transport-Neutral Command Executor Core

**Files:**
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorTask.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorSubmitter.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorDrainLoop.java`
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`

- [ ] **Step 1: Expand the failing executor test to cover submit, execute, and closing**

```java
@Test
public void queueFullAndClosingBehaviorsLiveInExecutorCore() throws Exception {
    RecordingIoAdapter io = new RecordingIoAdapter();
    TestConnection connection = new TestConnection("c-1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 1024)));

    CommandExecutor<TestConnection> executor = new CommandExecutor<>(
            () -> {},
            ExecutorCoreTestSupport.pingOnlyProcessor(),
            Runnable::run,
            ExecutorCoreTestSupport.ndjsonReplyWriterFactory(),
            io,
            new CommandExecutorConfig(1, 0, 32, 16, 0, 0, 32, 10, SchedulingPolicy.FAIR)
    );
    executor.start();

    Assert.assertNull(executor.trySubmit(connection, TestExecutionRequests.ofUtf8("PING")));
    Assert.assertArrayEquals(
            "{\"ok\":true,\"result\":\"PONG\"}\n".getBytes(StandardCharsets.UTF_8),
            io.takeLastReply()
    );

    connection.context().markClosing();
    TrackingExecutionRequest skipped = TrackingExecutionRequest.ofUtf8("PING");
    Assert.assertNull(executor.trySubmit(connection, skipped));
    Assert.assertEquals(1L, executor.statsSnapshot().commandsSkippedClosing());
    Assert.assertEquals(1, skipped.closeCalls());

    executor.close();
}
```

- [ ] **Step 2: Run the executor test to verify it fails**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorTest test`
Expected: FAIL with missing `CommandExecutor`, missing `statsSnapshot()`, and missing `trySubmit(...)`.

- [ ] **Step 3: Implement the minimal executor runtime**

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriterFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class CommandExecutor<C extends ExecutionConnection> implements AutoCloseable {
    public enum SubmitRejectReason {
        NOT_RUNNING("not_running"),
        QUEUE_FULL("queue_full"),
        BYTES_BUDGET("bytes_budget"),
        OFFER_FAILED("offer_failed");

        private final String code;

        SubmitRejectReason(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private final Executor ownerExecutor;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final CommandExecutorSubmitter<C> submitter;
    private final CommandExecutorDrainLoop<C> drainLoop;
    private final CommandExecutorExecutionSupport<C> executionSupport;
    private final CommandExecutorConfig config;
    private volatile boolean running = true;

    public CommandExecutor(
            Runnable bindToCurrentThread,
            YierdisFastCommandProcessor commandProcessor,
            Executor ownerExecutor,
            ReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            CommandExecutorConfig config
    ) {
        this.ownerExecutor = ownerExecutor;
        this.config = config;
        this.backlogBudget = new ExecutorBacklogBudget(config.queueCapacity(), config.queueMaxBytes());
        ArrayBlockingQueue<CommandExecutorTask<C>> fifo =
                config.schedulingPolicy() == SchedulingPolicy.GLOBAL ? new ArrayBlockingQueue<>(config.queueCapacity()) : null;
        this.taskQueue = new ExecutorTaskQueue<>(config.schedulingPolicy(), fifo, connection -> connection.context().queueState());
        this.executionSupport = new CommandExecutorExecutionSupport<>(commandProcessor, replyWriterFactory, ioAdapter, backlogBudget, () -> running);
        this.submitter = new CommandExecutorSubmitter<>(taskQueue, backlogBudget, () -> running, config);
        this.drainLoop = new CommandExecutorDrainLoop<>(ownerExecutor, taskQueue, executionSupport, config.maxDrainCommands(),
                TimeUnit.MILLISECONDS.toNanos(config.drainTimeLimitMillis()), () -> running);
        this.ownerExecutor.execute(bindToCurrentThread);
    }

    public void start() {
        drainLoop.markStarted();
    }

    public SubmitRejectReason trySubmit(C connection, ExecutionRequest request) {
        return submitter.trySubmit(connection, request, drainLoop::scheduleDrain);
    }

    public StatsSnapshot statsSnapshot() {
        return executionSupport.statsSnapshot(taskQueue.size(), backlogBudget.queuedBytes(), config.schedulingPolicy());
    }

    public CompletableFuture<Void> shutdownGracefully() {
        running = false;
        CompletableFuture<Void> future = new CompletableFuture<>();
        ownerExecutor.execute(() -> future.complete(null));
        return future;
    }

    public void executeMaintenance(Runnable task) {
        ownerExecutor.execute(() -> {
            if (running) {
                task.run();
            }
        });
    }

    @Override
    public void close() {
        running = false;
        drainLoop.drainLeftoverCommands();
    }

    public record StatsSnapshot(
            long commandsExecuted,
            long commandsSkippedClosing,
            int queuedTasks,
            long queuedBytes,
            SchedulingPolicy schedulingPolicy
    ) {}
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ExecutionRequest;

final class CommandExecutorTask<C extends ExecutionConnection> {
    final C connection;
    final ExecutionRequest request;
    final int retainedBytes;

    CommandExecutorTask(C connection, ExecutionRequest request, int retainedBytes) {
        this.connection = connection;
        this.request = request;
        this.retainedBytes = retainedBytes;
    }
}
```

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorExecutionSupport<C extends ExecutionConnection> {
    private final YierdisFastCommandProcessor commandProcessor;
    private final ReplyWriterFactory replyWriterFactory;
    private final ExecutionIoAdapter<C> ioAdapter;
    private final ExecutorBacklogBudget backlogBudget;
    private final BooleanSupplier running;
    private final LongAdder commandsExecuted = new LongAdder();
    private final LongAdder commandsSkippedClosing = new LongAdder();
    private CommandContext execCtx;

    CommandExecutorExecutionSupport(
            YierdisFastCommandProcessor commandProcessor,
            ReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            ExecutorBacklogBudget backlogBudget,
            BooleanSupplier running
    ) {
        this.commandProcessor = commandProcessor;
        this.replyWriterFactory = replyWriterFactory;
        this.ioAdapter = ioAdapter;
        this.backlogBudget = backlogBudget;
        this.running = running;
    }

    void execute(CommandExecutorTask<C> task) {
        C connection = task.connection;
        if (connection.context().isClosing()) {
            commandsSkippedClosing.increment();
            task.request.close();
            backlogBudget.releaseSlot();
            backlogBudget.releaseQueuedBytes(task.retainedBytes);
            return;
        }
        ReplyWriter writer = replyWriterFactory.newWriter(ioAdapter.newReplySink(connection));
        commandProcessor.execute(task.request, context(connection.context().session(), writer));
        ioAdapter.writeBufferedReply(connection, writer.closeAfterReplyRequested());
        commandsExecuted.increment();
        task.request.close();
        connection.context().recordCommandFinished(task.retainedBytes, true);
        backlogBudget.releaseSlot();
        backlogBudget.releaseQueuedBytes(task.retainedBytes);
    }

    CommandExecutor.StatsSnapshot statsSnapshot(int queuedTasks, long queuedBytes, SchedulingPolicy schedulingPolicy) {
        return new CommandExecutor.StatsSnapshot(
                commandsExecuted.sum(),
                commandsSkippedClosing.sum(),
                queuedTasks,
                queuedBytes,
                schedulingPolicy
        );
    }

    private CommandContext context(yier.bubu.redis.contract.Session session, ReplyWriter writer) {
        if (execCtx == null) {
            execCtx = new CommandContext(session, writer);
            return execCtx;
        }
        return execCtx.reset(session, writer);
    }
}
```

```java
// append to ExecutorCoreTestSupport.java in the same task
static CommandExecutor<TestConnection> immediate(RecordingIoAdapter io, int queueCapacity, int backpressureHigh, SchedulingPolicy policy) {
    CommandExecutor<TestConnection> executor = new CommandExecutor<>(
            () -> {},
            pingOnlyProcessor(),
            Runnable::run,
            ndjsonReplyWriterFactory(),
            io,
            new CommandExecutorConfig(queueCapacity, 0, backpressureHigh, Math.max(0, backpressureHigh / 2), 0, 0, 128, 10, policy)
    );
    executor.start();
    return executor;
}
```

- [ ] **Step 4: Run the executor test to verify it passes**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorTask.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorSubmitter.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorDrainLoop.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java
git commit -m "refactor: move command execution runtime into executor core"
```

### Task 4: Port Backpressure And Fair Scheduling Tests Into Executor-Core

**Files:**
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
- Create: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java`
- Create: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java`

- [ ] **Step 1: Write failing backpressure and fair scheduling tests**

```java
package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;

public class CommandExecutorBackpressureTest {
    @Test
    public void executorDisablesAndReEnablesInputBasedOnPendingThresholds() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = new TestConnection("c-1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 1024)));
        CommandExecutor<TestConnection> executor = ExecutorCoreTestSupport.immediate(io, 4, 2, SchedulingPolicy.FAIR);

        Assert.assertNull(executor.trySubmit(connection, ExecutorCoreTestSupport.ofUtf8("PING")));
        Assert.assertNull(executor.trySubmit(connection, ExecutorCoreTestSupport.ofUtf8("PING")));
        Assert.assertTrue(io.inputDisabled());

        io.completePendingFlushes();
        Assert.assertTrue(io.inputEnabledAgain());
    }
}
```

```java
package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;

public class CommandExecutorFairSchedulingTest {
    @Test
    public void fairSchedulingAlternatesAcrossConnections() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        CommandExecutor<TestConnection> executor = ExecutorCoreTestSupport.immediate(io, 16, 8, SchedulingPolicy.FAIR);
        TestConnection c1 = new TestConnection("c1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 1024)));
        TestConnection c2 = new TestConnection("c2", new ExecutionConnectionContext(new DefaultExecutionSession(4, 1024)));

        executor.trySubmit(c1, ExecutorCoreTestSupport.ofUtf8("PING"));
        executor.trySubmit(c1, ExecutorCoreTestSupport.ofUtf8("PING"));
        executor.trySubmit(c2, ExecutorCoreTestSupport.ofUtf8("PING"));

        Assert.assertEquals("c1,c2,c1", io.executionOrder());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest test`
Expected: FAIL because `RecordingIoAdapter` and `CommandExecutor` do not yet track input transitions or execution order.

- [ ] **Step 3: Implement the missing backpressure and scheduling behavior**

```java
// inside CommandExecutor constructor
ExecutorBackpressureIo<C> io = new ExecutorBackpressureIo<>() {
        @Override public boolean isActive(C key) { return ioAdapter.isActive(key); }
        @Override public boolean isWritable(C key) { return ioAdapter.isWritable(key); }
        @Override public void disableAutoRead(C key) { ioAdapter.disableInput(key); }
        @Override public void enableAutoRead(C key) { ioAdapter.enableInput(key); }
        @Override public void onClose(C key, Runnable callback) { ioAdapter.onClose(key, callback); }
};
ExecutorBackpressureRuntime<C> runtime = new ExecutorBackpressureRuntime<>() {
        @Override public int pending(C key) { return key.context().pending(); }
        @Override public long pendingBytes(C key) { return key.context().pendingBytes(); }
        @Override public boolean isClosing(C key) { return key.context().isClosing(); }
        @Override public boolean markAutoReadDisabledByExecutor(C key) { return key.context().markInputDisabledByExecutor(); }
        @Override public boolean autoReadDisabledByExecutor(C key) { return key.context().inputDisabledByExecutor(); }
        @Override public boolean clearAutoReadDisabledByExecutor(C key) { return key.context().clearInputDisabledByExecutor(); }
};
ExecutorBackpressureController<C> backpressure = new ExecutorBackpressureController<>(
        ownerExecutor,
        backlogBudget,
        config.backpressureLowWatermark(),
        config.backpressureBytesHighWatermark(),
        config.backpressureBytesLowWatermark(),
        io,
        runtime,
        new ExecutorBackpressureObserver<>() {},
        () -> running
);
```

```java
// inside CommandExecutorSubmitter.trySubmit(...)
connection.context().recordCommandEnqueued(retainedBytes);
if (!backlogBudget.tryReserveSlot()) {
    connection.context().recordCommandRejected();
    return CommandExecutor.SubmitRejectReason.QUEUE_FULL;
}
taskQueue.offer(connection, new CommandExecutorTask<>(connection, request, retainedBytes));
if (connection.context().pending() >= config.backpressureHighWatermark()) {
    connection.context().recordBackpressureEnter();
    backpressure.disableAutoRead(connection);
}
```

```java
// inside CommandExecutorExecutionSupport.execute(...)
ioAdapter.writeBufferedReply(connection, writer.closeAfterReplyRequested());
touchedConnections.add(connection);
...
if (running.getAsBoolean()
        && connection.context().pending() <= backpressureLowWatermark
        && backlogBudget.isGlobalBackpressureCleared()) {
    connection.context().recordBackpressureExit();
    backpressure.enableAutoReadIfWeDisabled(connection);
    backpressure.scheduleGlobalRecovery();
}
```

```java
// inside CommandExecutorDrainLoop
if (pendingAfterDrain) {
    executor.execute(this::drainLoop);
    return;
}
ioAdapter.flushPending(touchedConnections);
```

- [ ] **Step 4: Run the backpressure and fair scheduling tests to verify they pass**

Run: `jdk25 mvn -pl yierdis-executor-core -Dtest=CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java
git commit -m "refactor: port backpressure and fair scheduling into executor core"
```

### Task 5: Add Netty Execution Adapters And Rewire Handlers

**Files:**
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionConnection.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionIoAdapter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyReplyFlushBatch.java`
- Test: `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`

- [ ] **Step 1: Write the failing Netty adapter integration test**

```java
package yier.bubu.redis;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.executor.CommandExecutor;
import yier.bubu.redis.executor.CommandExecutorConfig;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.util.List;

public class NettyExecutionAdapterIntegrationTest {
    @Test
    public void handlerSubmitsThroughNettyExecutionConnection() throws Exception {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    processor,
                    Runnable::run,
                    new JsonLineReplyWriterFactory(),
                    ioAdapter,
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            EmbeddedChannel channel = new EmbeddedChannel(
                    new ProtocolCommandAdapter(),
                    new YierdisFastCommandHandler(executor, new JsonLineReplyWriterFactory())
            );
            try {
                channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));
                Assert.assertArrayEquals(
                        "{\"ok\":true,\"result\":\"PONG\"}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        readOutbound(channel)
                );
            } finally {
                channel.finishAndReleaseAll();
                executor.close();
            }
        }
    }

    private static byte[] readOutbound(EmbeddedChannel channel) {
        io.netty.buffer.ByteBuf out = channel.readOutbound();
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();
        return bytes;
    }
}
```

- [ ] **Step 2: Run the server integration test to verify it fails**

Run: `jdk25 mvn -pl yierdis-server -Dtest=NettyExecutionAdapterIntegrationTest test`
Expected: FAIL because `NettyExecutionConnection`, `NettyExecutionIoAdapter`, and the new `YierdisFastCommandHandler` constructor do not exist.

- [ ] **Step 3: Implement the Netty adapter layer and handler rewires**

```java
package yier.bubu.redis;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import yier.bubu.redis.executor.DefaultExecutionSession;
import yier.bubu.redis.executor.ExecutionConnection;
import yier.bubu.redis.executor.ExecutionConnectionContext;

final class NettyExecutionConnection implements ExecutionConnection {
    private static final AttributeKey<NettyExecutionConnection> KEY =
            AttributeKey.valueOf("yierdis.nettyExecutionConnection");

    static NettyExecutionConnection getOrCreate(Channel channel, int txMaxCommands, long txMaxBytes) {
        NettyExecutionConnection existing = channel.attr(KEY).get();
        if (existing != null) {
            return existing;
        }
        DefaultExecutionSession session = new DefaultExecutionSession(txMaxCommands, txMaxBytes);
        NettyExecutionConnection created = new NettyExecutionConnection(channel, new ExecutionConnectionContext(session));
        NettyExecutionConnection raced = channel.attr(KEY).setIfAbsent(created);
        return raced == null ? created : raced;
    }

    static NettyExecutionConnection get(Channel channel) {
        return channel.attr(KEY).get();
    }

    private final Channel channel;
    private final ExecutionConnectionContext context;

    private NettyExecutionConnection(Channel channel, ExecutionConnectionContext context) {
        this.channel = channel;
        this.context = context;
    }

    Channel channel() {
        return channel;
    }

    @Override public String connectionId() { return channel.id().asShortText(); }
    @Override public ExecutionConnectionContext context() { return context; }
}
```

```java
package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.executor.ExecutionIoAdapter;

import java.util.ArrayList;
import java.util.List;

final class NettyExecutionIoAdapter implements ExecutionIoAdapter<NettyExecutionConnection> {
    private static final AttributeKey<ByteBuf> REPLY_BUFFER_KEY =
            AttributeKey.valueOf("yierdis.nettyReplyBuffer");

    @Override public boolean isActive(NettyExecutionConnection connection) { return connection.channel().isActive(); }
    @Override public boolean isWritable(NettyExecutionConnection connection) { return connection.channel().isWritable(); }
    @Override public void disableInput(NettyExecutionConnection connection) { connection.channel().config().setAutoRead(false); }
    @Override public void enableInput(NettyExecutionConnection connection) { connection.channel().config().setAutoRead(true); }
    @Override public void onClose(NettyExecutionConnection connection, Runnable callback) { connection.channel().closeFuture().addListener(ignored -> callback.run()); }

    @Override
    public BytesSink newReplySink(NettyExecutionConnection connection) {
        ChannelHandlerContext ctx = connection.channel().pipeline().context(YierdisFastCommandHandler.class);
        ByteBuf out = ctx.alloc().buffer();
        connection.channel().attr(REPLY_BUFFER_KEY).set(out);
        return new NettyByteBufSink(out);
    }

    @Override
    public void writeBufferedReply(NettyExecutionConnection connection, boolean closeAfterReply) {
        ByteBuf out = connection.channel().attr(REPLY_BUFFER_KEY).getAndSet(null);
        ChannelHandlerContext ctx = connection.channel().pipeline().context(YierdisFastCommandHandler.class);
        if (closeAfterReply) {
            ctx.writeAndFlush(out).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
            connection.context().markClosing();
            connection.context().recordCloseAfterReply();
            return;
        }
        ctx.write(out, ctx.voidPromise());
    }

    @Override
    public void flushPending(Iterable<NettyExecutionConnection> touchedConnections) {
        for (NettyExecutionConnection connection : touchedConnections) {
            connection.channel().pipeline().context(YierdisFastCommandHandler.class).flush();
        }
    }
}
```

```java
public final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<ExecutionRequest> {
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final ReplyWriterFactory replyWriterFactory;

    public YierdisFastCommandHandler(CommandExecutor<NettyExecutionConnection> executor, ReplyWriterFactory replyWriterFactory) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ExecutionRequest msg) {
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        CommandExecutor.SubmitRejectReason reject = executor.trySubmit(connection, msg);
        if (reject == null) {
            return;
        }
        ByteBuf out = ctx.alloc().buffer();
        ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
        writer.error("ERR busy " + reject.code());
        ctx.writeAndFlush(out);
        msg.close();
    }
}
```

- [ ] **Step 4: Run the integration test to verify it passes**

Run: `jdk25 mvn -pl yierdis-server -Dtest=NettyExecutionAdapterIntegrationTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionConnection.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionIoAdapter.java \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java \
  yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java
git commit -m "refactor: add netty adapters for executor core"
```

### Task 6: Rewire Bootstrap And Observability To The New Executor

**Files:**
- Create: `yierdis-server/src/main/java/yier/bubu/redis/CommandExecutorConfigs.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`

- [ ] **Step 1: Write the failing bootstrap/observability tests**

```java
@Test
public void bootstrapBuildsCommandExecutorAndBindsItIntoInfoProvider() throws Exception {
    try (YierdisServerBootstrap bootstrap = YierdisServerBootstrap.start("--port", "0")) {
        NettyServerInfoProvider info = bootstrap.infoProviderForTests();
        Assert.assertNotNull(info.boundExecutorForTests());
        Assert.assertEquals("FAIR", info.boundExecutorForTests().statsSnapshot().schedulingPolicy().name());
    }
}
```

```java
@Test
public void infoProviderReadsConnectionStatsFromDefaultExecutionSession() {
    DefaultExecutionSession session = new DefaultExecutionSession(16, 1024);
    ExecutionConnectionContext context = new ExecutionConnectionContext(session);
    context.recordCommandEnqueued(12);
    Assert.assertEquals(1, session.connectionContext().statsSnapshot().pending());
}
```

- [ ] **Step 2: Run the bootstrap tests to verify they fail**

Run: `jdk25 mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest test`
Expected: FAIL because bootstrap still builds `NettyCommandExecutor`, and `NettyServerInfoProvider` still binds the old type.

- [ ] **Step 3: Implement the bootstrap and info rewires**

```java
package yier.bubu.redis;

import yier.bubu.redis.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.executor.CommandExecutorConfig;
import yier.bubu.redis.executor.SchedulingPolicy;

final class CommandExecutorConfigs {
    static CommandExecutorConfig from(YierdisServerRuntimeConfig config) {
        return new CommandExecutorConfig(
                config.executorQueueCapacity(),
                config.executorQueueMaxBytes(),
                config.backpressureHighWatermark(),
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                config.executorMaxDrainCommands(),
                config.executorDrainTimeLimitMillis(),
                switch (config.executorSchedulingPolicy()) {
                    case GLOBAL -> SchedulingPolicy.GLOBAL;
                    case FAIR -> SchedulingPolicy.FAIR;
                }
        );
    }
}
```

```java
// inside YierdisServerBootstrap.startInternal()
CommandExecutorConfig executorConfig = CommandExecutorConfigs.from(runtimeConfig);
NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
executor = new CommandExecutor<>(
        runtimeAccess::bindToCurrentThread,
        processor,
        commandGroup.next()::execute,
        new JsonLineReplyWriterFactory(),
        ioAdapter,
        executorConfig
);
infoProvider.bindExecutor(executor);
```

```java
// inside NettyServerInfoProvider
private volatile CommandExecutor<?> executor;

void bindExecutor(CommandExecutor<?> executor) {
    this.executor = Objects.requireNonNull(executor, "executor");
}

private static ExecutionConnectionContext connectionStats(CommandContext ctx) {
    ServerSession serverSession = ctx.serverSessionOrNull();
    if (serverSession instanceof DefaultExecutionSession session) {
        return session.connectionContext();
    }
    return null;
}
```

- [ ] **Step 4: Run the bootstrap tests to verify they pass**

Run: `jdk25 mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/CommandExecutorConfigs.java \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java
git commit -m "refactor: rewire bootstrap and stats to command executor core"
```

### Task 7: Delete Legacy Server-Owned Runtime And Move Semantic Tests

**Files:**
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Delete: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java`
- Delete: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Delete: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- Delete: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- Delete: `yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`

- [ ] **Step 1: Write the failing migrated executor-core tests that replace the server-owned ones**

```java
@Test
public void closeAfterReplyAndRejectedCountersNoLongerDependOnServerContext() throws Exception {
    RecordingIoAdapter io = new RecordingIoAdapter();
    CommandExecutor<TestConnection> executor = ExecutorCoreTestSupport.immediate(io, 16, 8, SchedulingPolicy.FAIR);
    TestConnection connection = new TestConnection("c1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 1024)));

    TrackingExecutionRequest ping = TrackingExecutionRequest.ofUtf8("PING");
    Assert.assertNull(executor.trySubmit(connection, ping));
    Assert.assertEquals(1L, executor.statsSnapshot().commandsExecuted());

    executor.close();
    Assert.assertEquals(CommandExecutor.SubmitRejectReason.NOT_RUNNING, executor.trySubmit(connection, TrackingExecutionRequest.ofUtf8("PING")));
}
```

- [ ] **Step 2: Run the focused module tests to verify they fail before cleanup**

Run: `jdk25 mvn -pl yierdis-executor-core,yierdis-server -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest test`
Expected: FAIL because legacy server runtime files and imports are still referenced.

- [ ] **Step 3: Remove the old runtime classes and update remaining integration tests**

```bash
git rm yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java \
  yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java \
  yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java
```

```java
// representative integration test update
CommandExecutor<NettyExecutionConnection> executor = TestNettyExecutors.immediate(instance, processor);
EmbeddedChannel ch = new EmbeddedChannel(
        new ProtocolCommandAdapter(),
        new YierdisFastCommandHandler(executor, new JsonLineReplyWriterFactory())
);
```

- [ ] **Step 4: Run the focused module tests to verify cleanup passes**

Run: `jdk25 mvn -pl yierdis-executor-core,yierdis-server -Dtest=CommandExecutorTest,CommandExecutorBackpressureTest,CommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: delete server-owned command runtime"
```

### Task 8: Update Guard Tests, Docs, And Full Verification

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `docs/module-architecture.md`
- Modify: `docs/request-execution-flow.md`
- Modify: `docs/executor-and-backpressure.md`
- Modify: `docs/project-overview.md`
- Modify: `docs/main-path-walkthrough.md`
- Modify: `docs/configuration-and-operations.md`
- Modify: `docs/development-navigation.md`

- [ ] **Step 1: Write the failing architecture guard changes**

```java
@Test
public void executorCoreMustNotImportNettyOrProtocolModules() throws IOException {
    Path repoRoot = resolveRepoRoot();
    List<String> offenders = new ArrayList<>();
    scanForForbiddenText(
            repoRoot,
            repoRoot.resolve("yierdis-executor-core/src/main/java"),
            offenders,
            "import io.netty.",
            "ChannelHandlerContext",
            "ByteBuf",
            "import yier.bubu.redis.protocol."
    );
    Assert.assertTrue("executor-core must stay transport-neutral", offenders.isEmpty());
}

@Test
public void serverMustNotReintroduceLegacyExecutionAuthorities() throws IOException {
    Path repoRoot = resolveRepoRoot();
    List<String> offenders = new ArrayList<>();
    scanForForbiddenText(
            repoRoot,
            repoRoot.resolve("yierdis-server/src/main/java"),
            offenders,
            "class NettyCommandExecutor",
            "class ServerConnectionContext",
            "class ServerSessionState"
    );
    Assert.assertTrue("server must stay adapter-only for command runtime", offenders.isEmpty());
}
```

- [ ] **Step 2: Run the guard and docs-adjacent tests to verify they fail**

Run: `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest test`
Expected: FAIL until the new forbidden-text checks and updated docs/wording are aligned with the codebase.

- [ ] **Step 3: Update the docs and architecture guards**

```markdown
## `yierdis-executor-core`

This module is no longer just a scheduling helper layer. It now owns the transport-neutral
command execution runtime:

- submission
- drain loop
- connection/session state
- backpressure lifecycle
- maintenance dispatch
```

```markdown
## `yierdis-server`

The server module now owns only:

- Netty pipeline
- protocol decode / encode
- Netty execution adapters

It no longer owns command executor runtime state.
```

```java
// update request flow wording
1. `YierdisServerBootstrap` assembles `YierdisInstance`, `YierdisFastCommandProcessor`,
   `CommandExecutor`, and Netty adapters.
2. `YierdisFastCommandHandler` submits `ExecutionRequest` plus `NettyExecutionConnection`
   into executor-core.
3. `CommandExecutorDrainLoop` executes on the owner thread.
```

- [ ] **Step 4: Run the full test suite to verify everything passes**

Run: `jdk25 mvn test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  docs/module-architecture.md \
  docs/request-execution-flow.md \
  docs/executor-and-backpressure.md \
  docs/project-overview.md \
  docs/main-path-walkthrough.md \
  docs/configuration-and-operations.md \
  docs/development-navigation.md
git commit -m "docs: align architecture with transport-neutral executor core"
```
