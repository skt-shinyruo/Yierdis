package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyReservationResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplyCapacityBlockedSchedulingTest {
    @Test
    public void fairRunsOtherConnectionsWhileTheBlockedHeadKeepsItsOwnConnectionOrder() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        List<String> completed = new ArrayList<>();
        CommandExecutor<TestConnection> executor = newExecutor(ownerExecutor, completed, SchedulingPolicy.FAIR);
        TestConnection a = ExecutorCoreTestSupport.newConnection("a");
        TestConnection b = ExecutorCoreTestSupport.newConnection("b");
        TrackingExecutionRequest a1 = TrackingExecutionRequest.ofUtf8("A1");
        TrackingExecutionRequest a2 = TrackingExecutionRequest.ofUtf8("A2");
        TrackingExecutionRequest b1 = TrackingExecutionRequest.ofUtf8("B1");
        TrackingExecutionRequest b2 = TrackingExecutionRequest.ofUtf8("B2");
        BlockingReply blocked = new BlockingReply(false);
        BlockingReply a2Reply = new BlockingReply(true);
        BlockingReply b1Reply = new BlockingReply(true);
        BlockingReply b2Reply = new BlockingReply(true);
        try {
            ExecutorCoreTestSupport.publish(executor, a, a1, blocked);
            ExecutorCoreTestSupport.publish(executor, a, a2, a2Reply);
            ExecutorCoreTestSupport.publish(executor, b, b1, b1Reply);
            ExecutorCoreTestSupport.publish(executor, b, b2, b2Reply);

            ownerExecutor.runAll();

            Assert.assertEquals(List.of("B1", "B2"), completed);
            Assert.assertEquals(0, a1.closeCalls());
            Assert.assertEquals(0, blocked.cancelCalls());
            Assert.assertTrue("reply-blocked input must remain paused", a.context().autoReadDisabledByExecutor());

            blocked.makeCapacityAvailable(10_000);
            Assert.assertEquals("repeated capacity wakeups schedule one owner drain", 1, ownerExecutor.pendingTasks());
            ownerExecutor.runAll();

            Assert.assertEquals(List.of("B1", "B2", "A1", "A2"), completed);
            Assert.assertEquals(1, a1.closeCalls());
            Assert.assertEquals(1, a2.closeCalls());
            Assert.assertEquals(1, b1.closeCalls());
            Assert.assertEquals(1, b2.closeCalls());
            Assert.assertEquals(1, blocked.readyCalls());
            Assert.assertEquals(1, a2Reply.readyCalls());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    @Test
    public void globalDoesNotPassTheBlockedHeadUntilItsCapacityWakeup() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        List<String> completed = new ArrayList<>();
        CommandExecutor<TestConnection> executor = newExecutor(ownerExecutor, completed, SchedulingPolicy.GLOBAL);
        TestConnection a = ExecutorCoreTestSupport.newConnection("a");
        TestConnection b = ExecutorCoreTestSupport.newConnection("b");
        BlockingReply blocked = new BlockingReply(false);
        BlockingReply b1Reply = new BlockingReply(true);
        try {
            ExecutorCoreTestSupport.publish(executor, a, TrackingExecutionRequest.ofUtf8("A1"), blocked);
            ExecutorCoreTestSupport.publish(executor, b, TrackingExecutionRequest.ofUtf8("B1"), b1Reply);

            ownerExecutor.runAll();
            Assert.assertTrue(completed.isEmpty());

            blocked.makeCapacityAvailable(1);
            ownerExecutor.runAll();

            Assert.assertEquals(List.of("A1", "B1"), completed);
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    @Test
    public void disconnectingABlockedFairConnectionReleasesItsTaskWithoutStoppingOtherConnections() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        RecordingIoAdapter io = new RecordingIoAdapter();
        List<String> completed = new ArrayList<>();
        CommandExecutor<TestConnection> executor = newExecutor(ownerExecutor, completed, SchedulingPolicy.FAIR, io);
        TestConnection blockedConnection = ExecutorCoreTestSupport.newConnection("blocked");
        TestConnection runnableConnection = ExecutorCoreTestSupport.newConnection("runnable");
        TrackingExecutionRequest blockedRequest = TrackingExecutionRequest.ofUtf8("A1");
        BlockingReply blockedReply = new BlockingReply(false);
        try {
            ExecutorCoreTestSupport.publish(executor, blockedConnection, blockedRequest, blockedReply);
            ExecutorCoreTestSupport.publish(
                    executor,
                    runnableConnection,
                    TrackingExecutionRequest.ofUtf8("B1"),
                    new BlockingReply(true)
            );

            ownerExecutor.runAll();
            Assert.assertEquals(List.of("B1"), completed);

            io.fireClosed(blockedConnection);
            ownerExecutor.runAll();

            Assert.assertEquals(1, blockedRequest.closeCalls());
            Assert.assertEquals(1, blockedReply.cancelCalls());
            Assert.assertFalse(blockedConnection.context().inputPausedByReply());
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    @Test
    public void globalShutdownReleasesTheBlockedHead() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        List<String> completed = new ArrayList<>();
        CommandExecutor<TestConnection> executor = newExecutor(ownerExecutor, completed, SchedulingPolicy.GLOBAL);
        TestConnection connection = ExecutorCoreTestSupport.newConnection("a");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("A1");
        BlockingReply reply = new BlockingReply(false);
        try {
            ExecutorCoreTestSupport.publish(executor, connection, request, reply);
            ownerExecutor.runAll();
            Assert.assertTrue(completed.isEmpty());

            CompletableFuture<Void> shutdown = executor.shutdownGracefully();
            ownerExecutor.runAll();

            Assert.assertTrue(shutdown.isDone());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    private static CommandExecutor<TestConnection> newExecutor(
            ManualOwnerExecutor ownerExecutor,
            List<String> completed,
            SchedulingPolicy policy
    ) {
        return newExecutor(ownerExecutor, completed, policy, new RecordingIoAdapter());
    }

    private static CommandExecutor<TestConnection> newExecutor(
            ManualOwnerExecutor ownerExecutor,
            List<String> completed,
            SchedulingPolicy policy,
            RecordingIoAdapter io
    ) {
        CommandExecutionEngine engine = (session, request, writer) -> {
            String command = new String(request.toByteArray(0), StandardCharsets.US_ASCII);
            writer.requireReply(ReplyPlan.exact(32L, 0L));
            writer.simpleString(command);
            completed.add(command);
        };
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 1_000, policy)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);
        return executor;
    }

    private static final class BlockingReply implements ExecutionReply {
        private final AtomicBoolean capacityAvailable;
        private final AtomicInteger readyCalls = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();
        private final BlockingSink sink = new BlockingSink();
        private final AtomicBoolean capacityWaitActive = new AtomicBoolean();
        private Runnable wakeup;

        private BlockingReply(boolean capacityAvailable) {
            this.capacityAvailable = new AtomicBoolean(capacityAvailable);
        }

        @Override
        public BytesSink sink() {
            return sink;
        }

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
        public void markReady(boolean closeAfterReply) {
            readyCalls.incrementAndGet();
        }

        @Override
        public void cancel() {
            cancelCalls.incrementAndGet();
        }

        @Override
        public boolean hasWrittenBytes() {
            return sink.writtenBytes() > 0L;
        }

        @Override
        public void markResultUnknown() {
        }

        @Override
        public void close() {
            cancel();
        }

        private void makeCapacityAvailable(int wakeupAttempts) {
            capacityAvailable.set(true);
            Runnable registeredWakeup = wakeup;
            Assert.assertNotNull("blocked reply must register a capacity wakeup", registeredWakeup);
            for (int i = 0; i < wakeupAttempts; i++) {
                registeredWakeup.run();
            }
        }

        private int readyCalls() {
            return readyCalls.get();
        }

        private int cancelCalls() {
            return cancelCalls.get();
        }

        private final class BlockingSink implements ReplyReservationSink {
            private long writtenBytes;

            @Override
            public void require(ReplyPlan plan) {
                if (!capacityAvailable.get()) {
                    throw new ReplyCapacityUnavailableException("test reply capacity is unavailable");
                }
            }

            @Override
            public void writeBytes(byte[] source, int sourceIndex, int length) {
                writtenBytes += length;
            }

            @Override
            public long writtenBytes() {
                return writtenBytes;
            }
        }
    }
}
