package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.ReplyTooLargeException;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void configRejectsInvalidValues() {
        assertInvalidConfig(-1, 1024, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(0, 1024, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, -1, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, -1, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 0, 0, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, -1, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 8, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 9, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, -1, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, -1, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 0, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 128, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 129, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, -1, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 0, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 16, -1, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 16, 0, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 16, 10, null, NullPointerException.class);
        assertInvalidConfig(32, 1024, 8, 4, 0, 1, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);

        CommandExecutorConfig disabledByteBackpressure = new CommandExecutorConfig(32, 1024, 8, 4, 0, 0, 16, 10, SchedulingPolicy.FAIR);
        Assert.assertEquals(0L, disabledByteBackpressure.backpressureBytesHighWatermark());
        Assert.assertEquals(0L, disabledByteBackpressure.backpressureBytesLowWatermark());
    }

    @Test
    public void ioAdapterContractCanBufferFlushAndCloseOneConnection() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = new TestConnection("c-1", new ExecutionConnectionContext());
        AtomicBoolean closed = new AtomicBoolean(false);

        Assert.assertTrue(io.isActive(connection));
        Assert.assertTrue(io.isWritable(connection));
        io.setActive(connection, false);
        io.setWritable(connection, false);
        Assert.assertFalse(io.isActive(connection));
        Assert.assertFalse(io.isWritable(connection));
        io.setActive(connection, true);
        io.setWritable(connection, true);

        io.onClose(connection, () -> closed.set(true));
        io.disableInput(connection);
        Assert.assertTrue(io.inputDisabled(connection));
        io.enableInput(connection);
        Assert.assertTrue(io.inputEnabledAgain(connection));
        BytesSink sink = io.newReplySink(connection);
        sink.writeBytes(new byte[]{'O', 'K'});
        io.writeBufferedReply(connection, true);
        io.flushPending(java.util.List.of(connection));
        io.fireClosed(connection);

        Assert.assertEquals("OK", io.bufferedReply(connection));
        Assert.assertTrue(io.closeAfterReply(connection));
        Assert.assertEquals("c-1", io.lastFlushedConnectionId());
        Assert.assertEquals(1, io.flushCalls());
        Assert.assertTrue(closed.get());
    }

    @Test
    public void ioAdapterStateIsScopedPerConnectionAndFlushCallsCountInvocations() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection first = new TestConnection("c-1", new ExecutionConnectionContext());
        TestConnection second = new TestConnection("c-2", new ExecutionConnectionContext());
        AtomicBoolean firstClosed = new AtomicBoolean(false);
        AtomicBoolean secondClosed = new AtomicBoolean(false);

        io.setActive(first, false);
        io.setWritable(second, false);
        io.onClose(first, () -> firstClosed.set(true));
        io.onClose(second, () -> secondClosed.set(true));
        io.disableInput(first);
        io.enableInput(second);
        io.newReplySink(first).writeBytes(new byte[]{'O', 'N', 'E'});
        io.newReplySink(second).writeBytes(new byte[]{'T', 'W', 'O'});
        io.writeBufferedReply(first, true);
        io.writeBufferedReply(second, false);
        io.flushPending(java.util.List.of(first, second));

        Assert.assertFalse(io.isActive(first));
        Assert.assertTrue(io.isActive(second));
        Assert.assertTrue(io.isWritable(first));
        Assert.assertFalse(io.isWritable(second));
        Assert.assertTrue(io.inputDisabled(first));
        Assert.assertFalse(io.inputDisabled(second));
        Assert.assertFalse(io.inputEnabledAgain(first));
        Assert.assertTrue(io.inputEnabledAgain(second));
        Assert.assertEquals("ONE", io.bufferedReply(first));
        Assert.assertEquals("TWO", io.bufferedReply(second));
        Assert.assertTrue(io.closeAfterReply(first));
        Assert.assertFalse(io.closeAfterReply(second));
        Assert.assertEquals(java.util.List.of("c-1", "c-2"), io.lastFlushedConnectionIds());
        Assert.assertEquals(1, io.flushCalls());
        Assert.assertEquals(1, io.flushCount(first));
        Assert.assertEquals(1, io.flushCount(second));

        io.fireClosed(first);
        Assert.assertTrue(firstClosed.get());
        Assert.assertFalse(secondClosed.get());

        io.fireClosed(second);
        Assert.assertTrue(secondClosed.get());
    }

    @Test
    public void executorRunsCommandsSkipsClosingQueuedWorkAndStopsAcceptingNewSubmissions() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        TrackingExecutionRequest ping = TrackingExecutionRequest.ofUtf8("PING");

        Assert.assertNull(executor.trySubmit(connection, ping));
        Assert.assertEquals(1, ownerExecutor.pendingTasks());
        ownerExecutor.runAll();

        Assert.assertEquals("PONG\n", io.bufferedReply(connection));
        Assert.assertEquals(1, ping.closeCalls());
        Assert.assertEquals(1, io.flushCalls());

        TrackingExecutionRequest queuedButClosing = TrackingExecutionRequest.ofUtf8("PING");
        Assert.assertNull(executor.trySubmit(connection, queuedButClosing));
        connection.markClosing();
        ownerExecutor.runAll();

        Assert.assertEquals(1, queuedButClosing.closeCalls());
        Assert.assertEquals(2, connection.context().statsSnapshot().commandsEnqueued());
        Assert.assertEquals(1, connection.context().statsSnapshot().commandsExecuted());
        Assert.assertEquals(1, connection.context().statsSnapshot().commandsSkippedClosing());

        CommandExecutor.StatsSnapshot stats = executor.statsSnapshot();
        Assert.assertEquals(1L, stats.commandsExecuted());
        Assert.assertEquals(1L, stats.commandsSkippedClosing());
        Assert.assertEquals(0, stats.queuedTasks());
        Assert.assertEquals(0L, stats.queuedBytes());
        Assert.assertEquals(SchedulingPolicy.FAIR, stats.schedulingPolicy());
        Assert.assertEquals(2L, stats.submitAccepted());
        Assert.assertEquals(0L, stats.submitRejectedNotRunning());

        CompletableFuture<Void> shutdown = executor.shutdownGracefully();
        Assert.assertFalse(shutdown.isDone());
        ownerExecutor.runAll();
        Assert.assertTrue(shutdown.isDone());

        TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING");
        Assert.assertEquals(CommandExecutor.SubmitRejectReason.NOT_RUNNING, executor.trySubmit(connection, rejected));
        Assert.assertEquals(0, rejected.closeCalls());
        Assert.assertEquals(1L, executor.statsSnapshot().submitRejectedNotRunning());

        executor.close();
        rejected.close();
    }

    @Test
    public void schedulingRejectionAfterOfferRollsBackOwnershipAndAccounting() {
        for (SchedulingPolicy policy : SchedulingPolicy.values()) {
            AtomicBoolean rejectOwnerTask = new AtomicBoolean();
            RecordingIoAdapter io = new RecordingIoAdapter();
            java.util.concurrent.Executor ownerExecutor = task -> {
                if (rejectOwnerTask.get()) {
                    throw new RejectedExecutionException("injected owner rejection");
                }
                task.run();
            };
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> { },
                    ExecutorCoreTestSupport.simpleCommandEngine(),
                    ownerExecutor,
                    ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                    io,
                    new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, policy)
            );
            executor.start();

            TestConnection connection = ExecutorCoreTestSupport.newConnection("schedule-reject-" + policy);
            TrackingExecutionRequest rejectedRequest = TrackingExecutionRequest.ofUtf8("PING");
            TrackingReply rejectedReply = new TrackingReply();
            rejectOwnerTask.set(true);

            Assert.assertEquals(
                    CommandExecutor.SubmitRejectReason.OFFER_FAILED,
                    executor.trySubmit(connection, rejectedRequest, rejectedReply)
            );
            Assert.assertEquals(0, rejectedRequest.closeCalls());
            Assert.assertEquals(0, rejectedReply.cancelCalls());
            Assert.assertEquals(0, connection.context().pending());
            Assert.assertEquals(0L, connection.context().pendingBytes());
            Assert.assertEquals(0L, connection.context().statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
            Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
            Assert.assertEquals(0L, executor.statsSnapshot().submitAccepted());
            Assert.assertEquals(1L, executor.statsSnapshot().submitRejectedOfferFailed());

            rejectOwnerTask.set(false);
            TrackingExecutionRequest accepted = TrackingExecutionRequest.ofUtf8("PING");
            Assert.assertNull(executor.trySubmit(connection, accepted));
            Assert.assertEquals("PONG\n", io.bufferedReply(connection));
            Assert.assertEquals(1, accepted.closeCalls());
            Assert.assertEquals(1L, executor.statsSnapshot().commandsExecuted());
            Assert.assertEquals(1L, executor.statsSnapshot().submitAccepted());

            executor.close();
            rejectedRequest.close();
            rejectedReply.close();
            Assert.assertEquals(1, rejectedRequest.closeCalls());
            Assert.assertEquals(1, rejectedReply.cancelCalls());
        }
    }

    @Test
    public void drainRescheduleRejectionDoesNotStrandAcceptedTasks() {
        String retainedArgument = "012345678901234567890123456789012345";
        for (SchedulingPolicy policy : SchedulingPolicy.values()) {
            AtomicBoolean rejectOwnerTask = new AtomicBoolean();
            ManualOwnerExecutor delegate = ExecutorCoreTestSupport.manualOwnerExecutor();
            java.util.concurrent.Executor ownerExecutor = task -> {
                if (rejectOwnerTask.get()) {
                    throw new RejectedExecutionException("injected owner reschedule rejection");
                }
                delegate.execute(task);
            };
            RecordingIoAdapter io = new RecordingIoAdapter();
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> { },
                    ExecutorCoreTestSupport.simpleCommandEngine(),
                    ownerExecutor,
                    ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                    io,
                    new CommandExecutorConfig(4, 100, 8, 4, 0, 0, 1, 1_000, policy)
            );
            ExecutorCoreTestSupport.startExecutor(executor, delegate);

            TestConnection connection = ExecutorCoreTestSupport.newConnection("reschedule-reject-" + policy);
            TrackingExecutionRequest firstRequest = TrackingExecutionRequest.ofUtf8("PING", retainedArgument);
            TrackingExecutionRequest secondRequest = TrackingExecutionRequest.ofUtf8("PING", retainedArgument);
            TrackingReply firstReply = new TrackingReply();
            TrackingReply secondReply = new TrackingReply();
            try {
                Assert.assertNull(executor.trySubmit(connection, firstRequest, firstReply));
                Assert.assertNull(executor.trySubmit(connection, secondRequest, secondReply));
                Assert.assertEquals(1, delegate.pendingTasks());

                rejectOwnerTask.set(true);
                try {
                    // 第一轮 drain 已取走一项；重排被拒绝时，队列仍必须保留后一项的所有权。
                    delegate.runAll();
                    Assert.fail("expected owner reschedule rejection");
                } catch (RejectedExecutionException expected) {
                    Assert.assertEquals("injected owner reschedule rejection", expected.getMessage());
                }
                rejectOwnerTask.set(false);

                Assert.assertEquals(1, firstRequest.closeCalls());
                Assert.assertEquals(1, firstReply.readyCalls());
                Assert.assertEquals(0, firstReply.cancelCalls());
                Assert.assertEquals(0, secondRequest.closeCalls());
                Assert.assertEquals(0, secondReply.readyCalls());
                Assert.assertEquals(0, secondReply.cancelCalls());
                Assert.assertEquals(1, connection.context().pending());
                Assert.assertEquals(40L, connection.context().pendingBytes());
                Assert.assertEquals(1, executor.statsSnapshot().queuedTasks());
                Assert.assertEquals(40L, executor.statsSnapshot().queuedBytes());
                Assert.assertEquals(2L, executor.statsSnapshot().submitAccepted());
                Assert.assertEquals(0L, executor.statsSnapshot().submitRejectedOfferFailed());

                TrackingExecutionRequest thirdRequest = TrackingExecutionRequest.ofUtf8("PING", retainedArgument);
                TrackingReply thirdReply = new TrackingReply();
                Assert.assertNull(executor.trySubmit(connection, thirdRequest, thirdReply));
                Assert.assertEquals(1, delegate.pendingTasks());
                delegate.runAll();

                Assert.assertEquals(1, secondRequest.closeCalls());
                Assert.assertEquals(1, secondReply.readyCalls());
                Assert.assertEquals(0, secondReply.cancelCalls());
                Assert.assertEquals(1, thirdRequest.closeCalls());
                Assert.assertEquals(1, thirdReply.readyCalls());
                Assert.assertEquals(0, thirdReply.cancelCalls());
                Assert.assertEquals(0, connection.context().pending());
                Assert.assertEquals(0L, connection.context().pendingBytes());
                Assert.assertEquals(0, executor.statsSnapshot().queuedTasks());
                Assert.assertEquals(0L, executor.statsSnapshot().queuedBytes());
                Assert.assertEquals(3L, executor.statsSnapshot().commandsExecuted());
                Assert.assertEquals(3L, executor.statsSnapshot().submitAccepted());
            } finally {
                executor.close();
            }
        }
    }

    @Test
    public void executorInternalErrorWritesInternalErrorReplyMarksClosingAndSurvivesShutdown() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        TrackingExecutionRequest exploding = TrackingExecutionRequest.failingOnCommandRead("PING");
        TrackingExecutionRequest queued = TrackingExecutionRequest.ofUtf8("PING");

        Assert.assertNull(executor.trySubmit(connection, exploding));
        Assert.assertNull(executor.trySubmit(connection, queued));

        ownerExecutor.runAll();

        Assert.assertEquals("ERR internal error\n", io.bufferedReply(connection));
        Assert.assertTrue(io.closeAfterReply(connection));
        Assert.assertTrue(connection.context().statsSnapshot().closing());
        Assert.assertEquals(1, exploding.closeCalls());
        Assert.assertEquals(1, queued.closeCalls());
        Assert.assertEquals(2L, connection.context().statsSnapshot().commandsEnqueued());
        Assert.assertEquals(1L, connection.context().statsSnapshot().commandsExecuted());
        Assert.assertEquals(1L, connection.context().statsSnapshot().commandsSkippedClosing());
        Assert.assertEquals(2L, executor.statsSnapshot().submitAccepted());
        Assert.assertEquals(1L, executor.statsSnapshot().closeAfterReply());

        CompletableFuture<Void> shutdown = executor.shutdownGracefully();
        Assert.assertFalse(shutdown.isDone());
        ownerExecutor.runAll();
        Assert.assertTrue("executor should still accept shutdown after internal command failure", shutdown.isDone());

        executor.close();
    }

    @Test
    public void executorHandlesSuccessfulCloseAfterReplyCommand() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        TrackingExecutionRequest quit = TrackingExecutionRequest.ofUtf8("QUIT");

        Assert.assertNull(executor.trySubmit(connection, quit));
        ownerExecutor.runAll();

        Assert.assertEquals("OK\n", io.bufferedReply(connection));
        Assert.assertTrue(io.closeAfterReply(connection));
        Assert.assertTrue(connection.context().statsSnapshot().closing());
        Assert.assertEquals(1, quit.closeCalls());
        Assert.assertEquals(1L, executor.statsSnapshot().closeAfterReply());

        executor.close();
    }

    @Test
    public void resultUnknownFailureCancelsTheRegisteredReplyAndClosesWithoutReplacementReply() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutionEngine engine = (session, request, out) -> {
            throw new ResultUnknownException("mutation result may already be visible");
        };
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("SET", "key", "value");
        TrackingReply reply = new TrackingReply();
        try {
            Assert.assertNull(executor.trySubmit(connection, request, reply));

            ownerExecutor.runAll();

            Assert.assertTrue(connection.context().statsSnapshot().closing());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(0, reply.readyCalls());
            Assert.assertEquals(0, reply.writtenBytes());
            Assert.assertEquals(1, io.closeCalls(connection));
        } finally {
            executor.close();
        }
    }

    @Test
    public void oversizedReplyCancelsTheRegisteredReplyAndClosesWithoutReplacementReply() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutionEngine engine = (session, request, out) -> {
            throw new ReplyTooLargeException("reply exceeds configured single-reply capacity");
        };
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("GET", "large");
        TrackingReply reply = new TrackingReply();
        try {
            Assert.assertNull(executor.trySubmit(connection, request, reply));

            ownerExecutor.runAll();

            Assert.assertTrue(connection.context().statsSnapshot().closing());
            Assert.assertEquals(1, request.closeCalls());
            Assert.assertEquals(1, reply.cancelCalls());
            Assert.assertEquals(0, reply.readyCalls());
            Assert.assertEquals(0, reply.writtenBytes());
            Assert.assertEquals("", io.bufferedReply(connection));
            Assert.assertEquals(1, io.closeCalls(connection));
        } finally {
            executor.close();
        }
    }

    @Test
    public void executorRejectsAlreadyClosingConnectionBeforeReservingBacklogBudget() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");
        Assert.assertTrue(connection.markClosing());

        TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING");
        Assert.assertEquals(CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING, executor.trySubmit(connection, rejected));

        ExecutionConnectionContext.ConnectionStatsSnapshot connectionStats = connection.context().statsSnapshot();
        Assert.assertEquals(0, rejected.closeCalls());
        Assert.assertEquals(0, ownerExecutor.pendingTasks());
        Assert.assertEquals(0, connectionStats.pending());
        Assert.assertEquals(0L, connectionStats.pendingBytes());
        Assert.assertEquals(0L, connectionStats.commandsEnqueued());
        Assert.assertEquals(0L, connectionStats.commandsExecuted());
        Assert.assertEquals(1L, connectionStats.commandsRejected());
        Assert.assertEquals(0L, connectionStats.commandsSkippedClosing());

        CommandExecutor.StatsSnapshot stats = executor.statsSnapshot();
        Assert.assertEquals(0L, stats.submitAccepted());
        Assert.assertEquals(1L, stats.submitRejectedClosing());
        Assert.assertEquals(0, stats.queuedTasks());
        Assert.assertEquals(0L, stats.queuedBytes());
        Assert.assertEquals(0L, stats.commandsSkippedClosing());

        executor.close();
        rejected.close();
    }

    @Test
    public void startWaitsForOwnerThreadBindingBeforeReturning() throws Exception {
        ExecutorService ownerExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch bindStarted = new CountDownLatch(1);
        CountDownLatch releaseBind = new CountDownLatch(1);

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {
                    bindStarted.countDown();
                    try {
                        Assert.assertTrue(releaseBind.await(1, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                new RecordingIoAdapter(),
                new CommandExecutorConfig(4, 64, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );

        try {
            Thread startThread = new Thread(executor::start);
            startThread.start();

            Assert.assertTrue(bindStarted.await(1, TimeUnit.SECONDS));
            startThread.join(100);
            Assert.assertTrue("start should wait until owner-thread binding finishes", startThread.isAlive());

            releaseBind.countDown();
            startThread.join(1000);
            Assert.assertFalse("start should return after binding completes", startThread.isAlive());

            executor.close();
        } finally {
            releaseBind.countDown();
            ownerExecutor.shutdownNow();
            Assert.assertTrue("owner executor should terminate", ownerExecutor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private static void assertInvalidConfig(
            int queueCapacity,
            long queueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis,
            SchedulingPolicy schedulingPolicy,
            Class<? extends Throwable> expected
    ) {
        try {
            new CommandExecutorConfig(
                    queueCapacity,
                    queueMaxBytes,
                    backpressureHighWatermark,
                    backpressureLowWatermark,
                    backpressureBytesHighWatermark,
                    backpressureBytesLowWatermark,
                    maxDrainCommands,
                    drainTimeLimitMillis,
                    schedulingPolicy
            );
            Assert.fail("Expected " + expected.getSimpleName());
        } catch (Throwable error) {
            Assert.assertEquals(expected, error.getClass());
        }
    }

    private static final class TrackingReply implements ExecutionReply {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final AtomicInteger readyCalls = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();

        @Override
        public BytesSink sink() {
            return output::write;
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
            return output.size() > 0;
        }

        @Override
        public void close() {
            cancel();
        }

        private int readyCalls() {
            return readyCalls.get();
        }

        private int cancelCalls() {
            return cancelCalls.get();
        }

        private int writtenBytes() {
            return output.size();
        }
    }
}
