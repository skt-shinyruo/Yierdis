package yier.bubu.redis.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.TransactionState;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    public void sessionTransactionRejectsCommandsThatOverflowQueuedBytes() {
        DefaultExecutionSession session = new DefaultExecutionSession(4, 5);
        session.transaction().begin();

        ExecutionRequest first = TestExecutionRequests.ofUtf8("SET", "a");
        ExecutionRequest second = TestExecutionRequests.ofUtf8("PING");

        Assert.assertNull(session.transaction().tryEnqueue(first));
        Assert.assertEquals(1, session.transaction().size());
        Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(second));
        Assert.assertTrue(session.transaction().aborted());
        Assert.assertEquals(1, session.transaction().size());
    }

    @Test
    public void transactionResetPathsCloseOwnedSnapshots() throws Exception {
        DefaultExecutionSession session = new DefaultExecutionSession(4, 64);
        ExecutionConnectionContext context = new ExecutionConnectionContext(session);
        AtomicInteger closedSnapshots = new AtomicInteger();

        session.transaction().begin();
        Assert.assertNull(session.transaction().tryEnqueue(TestExecutionRequests.ofUtf8("SET", "k", "v")));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));

        session.transaction().begin();
        Assert.assertEquals(1, closedSnapshots.get());
        Assert.assertTrue(session.transaction().active());
        Assert.assertEquals(0, session.transaction().size());

        Assert.assertNull(session.transaction().tryEnqueue(TestExecutionRequests.ofUtf8("GET", "k")));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));
        session.transaction().discard();
        Assert.assertEquals(2, closedSnapshots.get());
        Assert.assertFalse(session.transaction().active());
        Assert.assertEquals(0, session.transaction().size());

        session.transaction().begin();
        Assert.assertNull(session.transaction().tryEnqueue(TestExecutionRequests.ofUtf8("DEL", "k")));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));
        context.markClosing();
        Assert.assertEquals(3, closedSnapshots.get());
        Assert.assertFalse(session.transaction().active());
        Assert.assertEquals(List.of(), session.transaction().drain());
    }

    @SuppressWarnings("unchecked")
    private static void replaceQueuedSnapshot(TransactionState transaction, ExecutionRequest replacement) throws Exception {
        Field queueField = transaction.getClass().getDeclaredField("queue");
        queueField.setAccessible(true);
        ArrayList<ExecutionRequest> queue = (ArrayList<ExecutionRequest>) queueField.get(transaction);
        queue.set(0, replacement);
    }

    private static final class CloseTrackingExecutionRequest implements ExecutionRequest {
        private final AtomicInteger closeCounter;

        private CloseTrackingExecutionRequest(AtomicInteger closeCounter) {
            this.closeCounter = closeCounter;
        }

        @Override
        public int argc() {
            return 1;
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return 0;
        }

        @Override
        public byte byteAt(int index, int offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toByteArray(int index) {
            return new byte[0];
        }

        @Override
        public int retainedBytes() {
            return 0;
        }

        @Override
        public void close() {
            closeCounter.incrementAndGet();
        }
    }
}
