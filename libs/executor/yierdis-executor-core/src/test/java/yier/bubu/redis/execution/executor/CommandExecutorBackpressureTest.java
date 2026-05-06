package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

public class CommandExecutorBackpressureTest {
    @Test
    public void executorDisablesAndReEnablesInputBasedOnPendingThresholds() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 2, 1, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection connection = ExecutorCoreTestSupport.newConnection("c-1");

        Assert.assertNull(executor.trySubmit(connection, TrackingExecutionRequest.ofUtf8("PING")));
        Assert.assertNull(executor.trySubmit(connection, TrackingExecutionRequest.ofUtf8("PING")));

        Assert.assertTrue(io.inputDisabled(connection));
        Assert.assertTrue(connection.context().autoReadDisabledByExecutor());

        ownerExecutor.runAll();

        Assert.assertTrue(io.inputEnabledAgain(connection));
        Assert.assertFalse(connection.context().autoReadDisabledByExecutor());
    }
}
