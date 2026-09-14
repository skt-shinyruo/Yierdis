package yier.bubu.redis.execution.executor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationResult;

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

        ExecutorAdmission<TestConnection> admission = acquired(
                executor.tryAcquire(connection, request.retainedBytes())
        );
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

        Runnable registration = executor.onAdmissionAvailable(32, wakeups::incrementAndGet);
        Assert.assertEquals(0, wakeups.get());

        held.close();
        Assert.assertEquals(1, wakeups.get());
        registration.run();
        registration.run();
        Assert.assertEquals(1, wakeups.get());

        executor.close();
        owner.runAll();
    }

    @Test
    public void capacityRegistrationAfterExecutorCloseWakesImmediately() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = newExecutor(owner, 1, 64L);
        AtomicInteger wakeups = new AtomicInteger();

        executor.close();
        owner.runAll();
        Runnable registration = executor.onAdmissionAvailable(32, wakeups::incrementAndGet);

        Assert.assertEquals(1, wakeups.get());
        registration.run();
        Assert.assertEquals(1, wakeups.get());
    }

    @Test
    public void bytesBudgetAndPendingBytesBackpressureUseTheHeapFootprintEstimate() {
        ManualOwnerExecutor owner = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                ExecutorCoreTestSupport.simpleCommandEngine(),
                owner,
                ExecutorCoreTestSupport.simpleReplySizer(),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                new RecordingIoAdapter(),
                new CommandExecutorConfig(8, 128L, 8, 0, 80L, 40L, 128, 10, SchedulingPolicy.FAIR)
        );
        TestConnection connection = ExecutorCoreTestSupport.newConnection("footprint");
        // PING 的 payload 只有 4 字节，但 retainedBytes 按 HeapRequestFootprint 口径为 80；
        // executor queued bytes、连接 pending bytes 和 bytes 背压水位都必须按新口径记账。
        ByteArrayExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("PING", List.of());
        TrackingReply reply = new TrackingReply();
        try {
            Assert.assertEquals(80, request.retainedBytes());

            ExecutorAdmission<TestConnection> admission = acquired(
                    executor.tryAcquire(connection, request.retainedBytes()));
            admission.publish(request, reply);

            Assert.assertEquals(80L, executor.statsSnapshot().queuedBytes());
            Assert.assertEquals(80L, connection.context().pendingBytes());
            Assert.assertTrue(connection.context().autoReadDisabledByExecutor());

            ExecutorAdmissionAttempt<TestConnection> second = executor.tryAcquire(connection, 80);
            Assert.assertEquals(
                    ExecutorAdmissionAttempt.BlockReason.QUEUE_BYTES,
                    ((ExecutorAdmissionAttempt.Unavailable<TestConnection>) second).reason()
            );
        } finally {
            executor.close();
            owner.runAll();
        }
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
                ExecutorCoreTestSupport.simpleReplySizer(),
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
        public Runnable onCapacityAvailable(Runnable wakeup) {
            return null;
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

        int cancelCalls() {
            return cancelCalls.get();
        }
    }
}
