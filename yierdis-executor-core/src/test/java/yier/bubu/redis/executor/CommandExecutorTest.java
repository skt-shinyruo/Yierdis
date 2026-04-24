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
    public void configRejectsInvalidValues() {
        assertInvalidConfig(-1, 1024, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, -1, 8, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, -1, 4, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, -1, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 9, 128, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, -1, 64, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, -1, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 129, 16, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, -1, 10, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 16, -1, SchedulingPolicy.FAIR, IllegalArgumentException.class);
        assertInvalidConfig(32, 1024, 8, 4, 128, 64, 16, 10, null, NullPointerException.class);

        CommandExecutorConfig disabledByteBackpressure = new CommandExecutorConfig(32, 1024, 8, 4, 0, 64, 16, 10, SchedulingPolicy.FAIR);
        Assert.assertEquals(0L, disabledByteBackpressure.backpressureBytesHighWatermark());
        Assert.assertEquals(64L, disabledByteBackpressure.backpressureBytesLowWatermark());
    }

    @Test
    public void ioAdapterContractCanBufferFlushAndCloseOneConnection() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        TestConnection connection = new TestConnection("c-1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 128)));
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
        TestConnection first = new TestConnection("c-1", new ExecutionConnectionContext(new DefaultExecutionSession(4, 128)));
        TestConnection second = new TestConnection("c-2", new ExecutionConnectionContext(new DefaultExecutionSession(4, 128)));
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
}
