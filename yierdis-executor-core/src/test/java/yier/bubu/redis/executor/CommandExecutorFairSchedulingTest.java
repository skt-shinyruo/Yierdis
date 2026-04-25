package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CommandExecutorFairSchedulingTest {
    @Test
    public void fairSchedulingAlternatesAcrossConnections() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        try (ProcessorHandle handle = ExecutorCoreTestSupport.processorHandle()) {
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> {},
                    handle.processor(),
                    ownerExecutor,
                    ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                    io,
                    new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 2, 1000, SchedulingPolicy.FAIR)
            );
            ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

            TestConnection c1 = ExecutorCoreTestSupport.newConnection("c1");
            TestConnection c2 = ExecutorCoreTestSupport.newConnection("c2");

            Assert.assertNull(executor.trySubmit(c1, TrackingExecutionRequest.ofUtf8("PING")));
            Assert.assertNull(executor.trySubmit(c1, TrackingExecutionRequest.ofUtf8("PING")));
            Assert.assertNull(executor.trySubmit(c2, TrackingExecutionRequest.ofUtf8("PING")));

            ownerExecutor.runAll();

            Assert.assertEquals(List.of("c1", "c2", "c1"), io.executionOrder());
        }
    }
}
