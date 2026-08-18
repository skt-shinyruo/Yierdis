package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;

public class ExecutionConnectionContextTest {
    @Test
    public void connectionContextTracksPendingBytesAndClosing() {
        ExecutionConnectionContext context = new ExecutionConnectionContext();

        Assert.assertEquals(0, context.pending());
        Assert.assertEquals(0L, context.pendingBytes());
        Assert.assertFalse(context.isClosing());

        context.recordCommandEnqueued(12);
        context.recordCommandEnqueued(8);
        Assert.assertEquals(2, context.pending());
        Assert.assertEquals(20L, context.pendingBytes());

        context.recordCommandFinished(12, true);
        Assert.assertEquals(1, context.pending());
        Assert.assertEquals(8L, context.pendingBytes());

        Assert.assertTrue(context.markClosing());
        Assert.assertFalse(context.markClosing());
        Assert.assertTrue(context.isClosing());
    }
}
