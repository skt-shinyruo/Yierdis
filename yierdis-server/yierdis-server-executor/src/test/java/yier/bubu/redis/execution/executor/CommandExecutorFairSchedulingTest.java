package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CommandExecutorFairSchedulingTest {
    @Test
    public void fairSchedulingAlternatesAcrossConnections() throws Exception {
        RecordingIoAdapter io = new RecordingIoAdapter();
        ManualOwnerExecutor ownerExecutor = ExecutorCoreTestSupport.manualOwnerExecutor();

        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
                ownerExecutor,
                ExecutorCoreTestSupport.simpleReplySizer(),
                ExecutorCoreTestSupport.simpleReplyWriterFactory(),
                io,
                new CommandExecutorConfig(16, 0, 8, 4, 0, 0, 2, 1000, SchedulingPolicy.FAIR)
        );
        ExecutorCoreTestSupport.startExecutor(executor, ownerExecutor);

        TestConnection c1 = ExecutorCoreTestSupport.newConnection("c1");
        TestConnection c2 = ExecutorCoreTestSupport.newConnection("c2");

        TrackingExecutionRequest c1First = TrackingExecutionRequest.ofUtf8("PING");
        TrackingExecutionRequest c1Second = TrackingExecutionRequest.ofUtf8("PING");
        TrackingExecutionRequest c2First = TrackingExecutionRequest.ofUtf8("PING");
        ExecutorCoreTestSupport.publish(executor, c1, c1First, ExecutorCoreTestSupport.ioReply(io, c1));
        ExecutorCoreTestSupport.publish(executor, c1, c1Second, ExecutorCoreTestSupport.ioReply(io, c1));
        ExecutorCoreTestSupport.publish(executor, c2, c2First, ExecutorCoreTestSupport.ioReply(io, c2));

        ownerExecutor.runAll();

        Assert.assertEquals(List.of("c1", "c2", "c1"), io.executionOrder());

        executor.close();
        ownerExecutor.runAll();
    }
}
