package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.ValidationResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplyCapacityBlockedSchedulingTest {
    @Test
    public void capacityWaitRetainsOnePreparedCommandAndExecutesItOnce() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        AtomicInteger prepares = new AtomicInteger();
        AtomicInteger executes = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        CommandExecutionEngine engine = (session, request) -> {
            prepares.incrementAndGet();
            return countingPrepared(ValidationResult.VALID, executes, closes);
        };
        CommandExecutor<TestConnection> executor = newLifecycleExecutor(ownerExecutor, engine);
        BlockingReply reply = new BlockingReply(false);
        try {
            ExecutorCoreTestSupport.publish(
                    executor,
                    ExecutorCoreTestSupport.newConnection("capacity"),
                    TrackingExecutionRequest.ofUtf8("GET"),
                    reply
            );

            ownerExecutor.runAll();
            Assert.assertEquals(1, prepares.get());
            Assert.assertEquals(0, executes.get());
            Assert.assertEquals(0, closes.get());

            reply.makeCapacityAvailable(10);
            ownerExecutor.runAll();
            Assert.assertEquals(1, prepares.get());
            Assert.assertEquals(1, executes.get());
            Assert.assertEquals(1, closes.get());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    @Test
    public void capacityWakeupRegistrationIsCancelledAfterTheReplyIsReserved() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = newLifecycleExecutor(
                ownerExecutor,
                (session, request) -> countingPrepared(
                        ValidationResult.VALID,
                        new AtomicInteger(),
                        new AtomicInteger()
                )
        );
        BlockingReply reply = new BlockingReply(false);
        try {
            ExecutorCoreTestSupport.publish(
                    executor,
                    ExecutorCoreTestSupport.newConnection("capacity-registration"),
                    TrackingExecutionRequest.ofUtf8("GET"),
                    reply
            );

            ownerExecutor.runAll();
            reply.makeCapacityAvailable(1);
            ownerExecutor.runAll();

            Assert.assertEquals(1, reply.capacityRegistrationCancelCalls());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

    @Test
    public void stalePreparedCommandIsClosedAndPreparedAgainBeforeMutation() {
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        AtomicInteger prepares = new AtomicInteger();
        AtomicInteger executes = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        CommandExecutionEngine engine = (session, request) -> countingPrepared(
                prepares.getAndIncrement() == 0 ? ValidationResult.STALE : ValidationResult.VALID,
                executes,
                closes
        );
        CommandExecutor<TestConnection> executor = newLifecycleExecutor(ownerExecutor, engine);
        try {
            ExecutorCoreTestSupport.publish(
                    executor,
                    ExecutorCoreTestSupport.newConnection("stale"),
                    TrackingExecutionRequest.ofUtf8("INCR"),
                    new BlockingReply(true)
            );

            ownerExecutor.runAll();
            Assert.assertEquals(2, prepares.get());
            Assert.assertEquals(1, executes.get());
            Assert.assertEquals(2, closes.get());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }

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
        CommandExecutionEngine engine = (session, request) -> {
            String command = new String(request.toByteArray(0), StandardCharsets.US_ASCII);
            return ExecutorCoreTestSupport.fixed(
                    ReplyShapes.simpleString(command),
                    context -> {
                        completed.add(command);
                        return CommandResult.reply(RedisReplies.simpleString(command));
                    }
            );
        };
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplySizer(),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 1_000, policy)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);
        return executor;
    }

    private static CommandExecutor<TestConnection> newLifecycleExecutor(
            ManualOwnerExecutor ownerExecutor,
            CommandExecutionEngine engine
    ) {
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                ownerExecutor,
                (session, shape) -> ReplyPlan.exact(64L, shape.retainedSourceBytes()),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                new RecordingIoAdapter(),
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 1_000, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);
        return executor;
    }

    private static PreparedCommand countingPrepared(
            ValidationResult validation,
            AtomicInteger executes,
            AtomicInteger closes
    ) {
        return new PreparedCommand() {
            private boolean closed;

            @Override
            public yier.bubu.redis.execution.api.ReplyShape reservationShape() {
                return ReplyShapes.bulkString(4096, 0L);
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return validation;
            }

            @Override
            public CommandResult execute(CommandExecutionContext context) {
                executes.incrementAndGet();
                return CommandResult.reply(RedisReplies.simpleString("OK"));
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    closes.incrementAndGet();
                }
            }
        };
    }

    private static final class BlockingReply implements ExecutionReply {
        private final AtomicBoolean capacityAvailable;
        private final AtomicInteger readyCalls = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();
        private final AtomicInteger capacityRegistrationCancelCalls = new AtomicInteger();
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
            return () -> {
                capacityRegistrationCancelCalls.incrementAndGet();
                capacityWaitActive.set(false);
            };
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

        private int capacityRegistrationCancelCalls() {
            return capacityRegistrationCancelCalls.get();
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
