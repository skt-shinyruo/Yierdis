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

        Assert.assertTrue(io.isActive(connection));
        Assert.assertTrue(io.isWritable(connection));
        io.setActive(false);
        io.setWritable(false);
        Assert.assertFalse(io.isActive(connection));
        Assert.assertFalse(io.isWritable(connection));
        io.setActive(true);
        io.setWritable(true);

        io.onClose(connection, () -> closed.set(true));
        io.disableInput(connection);
        Assert.assertTrue(io.inputDisabled());
        io.enableInput(connection);
        Assert.assertTrue(io.inputEnabledAgain());
        BytesSink sink = io.newReplySink(connection);
        sink.writeBytes(new byte[]{'O', 'K'});
        io.writeBufferedReply(connection, true);
        io.flushPending(java.util.List.of(connection));
        io.fireClosed();

        Assert.assertEquals("OK", io.bufferedReply());
        Assert.assertTrue(io.closeAfterReply());
        Assert.assertEquals("c-1", io.lastFlushedConnectionId());
        Assert.assertEquals(1, io.flushCalls());
        Assert.assertTrue(closed.get());
    }
}
