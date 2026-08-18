package yier.bubu.redis.execution.executor;

import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;

public class CommandExecutorBackpressureTest {
    @Test
    public void executorDisablesAndReEnablesInputBasedOnPendingThresholds() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        BiFunction<CommandSession, ExecutionRequest, PreparedCommand> engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplySizer(),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 2, 1, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");

        TrackingExecutionRequest first = TrackingExecutionRequest.ofUtf8("PING");
        TrackingExecutionRequest second = TrackingExecutionRequest.ofUtf8("PING");
        ExecutorCoreTestSupport.publish(executor, connection, first, ExecutorCoreTestSupport.ioReply(io, connection));
        ExecutorCoreTestSupport.publish(executor, connection, second, ExecutorCoreTestSupport.ioReply(io, connection));

        Assert.assertTrue(io.inputDisabled(connection));
        Assert.assertTrue(connection.context().autoReadDisabledByExecutor());
        Assert.assertEquals(1, executor.statsSnapshot().channelsAutoReadDisabled());
        Assert.assertEquals(1L, executor.statsSnapshot().backpressureEnter());
        Assert.assertEquals(1L, connection.context().statsSnapshot().backpressureEnter());

        ownerExecutor.runAll();

        Assert.assertTrue(io.inputEnabledAgain(connection));
        Assert.assertFalse(connection.context().autoReadDisabledByExecutor());
        Assert.assertEquals(0, executor.statsSnapshot().channelsAutoReadDisabled());
        Assert.assertEquals(1L, executor.statsSnapshot().backpressureExit());
        Assert.assertEquals(1L, connection.context().statsSnapshot().backpressureExit());

        executor.close();
        ownerExecutor.runAll();
    }

    @Test
    public void transportRecoveryWaitsForWritabilityAndClosedConnectionsLeaveTheTrackingSet() {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> { },
                ExecutorCoreTestSupport.simpleCommandEngine(),
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplySizer(),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);
        TestConnection connection = ExecutorCoreTestSupport.newConnection("transport");
        try {
            io.setWritable(connection, false);
            executor.onTransportUnwritable(connection);

            Assert.assertTrue(connection.context().autoReadDisabledByExecutor());
            Assert.assertEquals(1, executor.statsSnapshot().channelsAutoReadDisabled());
            executor.onTransportWritable(connection);
            Assert.assertEquals(1, ownerExecutor.pendingTasks());
            ownerExecutor.runAll();
            Assert.assertFalse(io.inputEnabledAgain(connection));
            Assert.assertEquals(0L, executor.statsSnapshot().backpressureExit());

            io.setWritable(connection, true);
            executor.onTransportWritable(connection);
            ownerExecutor.runAll();
            Assert.assertTrue(io.inputEnabledAgain(connection));
            Assert.assertFalse(connection.context().autoReadDisabledByExecutor());
            Assert.assertEquals(1L, executor.statsSnapshot().backpressureExit());

            executor.onTransportUnwritable(connection);
            Assert.assertEquals(1, executor.statsSnapshot().channelsAutoReadDisabled());
            io.fireClosed(connection);
            Assert.assertEquals(0, executor.statsSnapshot().channelsAutoReadDisabled());
        } finally {
            executor.close();
            ownerExecutor.runAll();
        }
    }
}
