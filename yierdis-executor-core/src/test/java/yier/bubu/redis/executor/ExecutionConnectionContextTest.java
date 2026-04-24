package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.List;

public class ExecutionConnectionContextTest {
    @Test
    public void connectionContextTracksPendingBytesAndClosing() {
        DefaultExecutionSession session = new DefaultExecutionSession(2, 32);
        ExecutionConnectionContext context = new ExecutionConnectionContext(session);

        Assert.assertSame(session, context.session());
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

    @Test
    public void sessionTransactionHonorsQueueLimitsAndDiscardsOnClosing() {
        DefaultExecutionSession session = new DefaultExecutionSession(1, 16);
        ExecutionConnectionContext context = new ExecutionConnectionContext(session);
        session.transaction().begin();

        ExecutionRequest first = TestExecutionRequests.ofUtf8("SET", "k", "v");
        ExecutionRequest second = TestExecutionRequests.ofUtf8("PING");

        Assert.assertNull(session.transaction().tryEnqueue(first));
        Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(second));
        Assert.assertTrue(session.transaction().aborted());

        context.markClosing();
        Assert.assertFalse(session.transaction().active());
        Assert.assertEquals(List.of(), session.transaction().drain());
    }
}
