package yier.bubu.redis.execution.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineSessionTest {
    @Test
    public void engineSessionOwnsDbClientAndTransactionState() {
        EngineSession session = new EngineSession(1, 16);
        session.setDbIndex(3);
        session.setClientName("  worker  ");
        session.setAuthenticated(true);

        Assert.assertEquals(3, session.dbIndex());
        Assert.assertEquals("worker", session.clientName());
        Assert.assertTrue(session.authenticated());

        TransactionState tx = session.transaction();
        tx.begin();
        Assert.assertNull(tx.tryEnqueue(ByteArrayExecutionRequest.fromUtf8("SET", List.of("k", "v"))));
        Assert.assertEquals(
                "ERR Transaction queue is full",
                tx.tryEnqueue(ByteArrayExecutionRequest.fromUtf8("GET", List.of("k")))
        );
        Assert.assertTrue(tx.aborted());

        session.discardTransaction();

        Assert.assertFalse(tx.active());
        Assert.assertFalse(tx.aborted());
        Assert.assertEquals(0, tx.size());
    }

    @Test
    public void engineSessionTracksRespProtocolVersion() {
        EngineSession session = new EngineSession(1, 16);

        Assert.assertEquals(2, session.respVersion());

        session.setRespVersion(3);
        Assert.assertEquals(3, session.respVersion());

        Assert.assertThrows(IllegalArgumentException.class, () -> session.setRespVersion(4));
    }

    @Test
    public void transactionRejectsCommandsThatOverflowQueuedBytes() {
        EngineSession session = new EngineSession(4, 5);
        session.transaction().begin();

        ExecutionRequest first = ByteArrayExecutionRequest.fromUtf8("SET", List.of("a"));
        ExecutionRequest second = ByteArrayExecutionRequest.fromUtf8("PING", List.of());

        Assert.assertNull(session.transaction().tryEnqueue(first));
        Assert.assertEquals(1, session.transaction().size());
        Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(second));
        Assert.assertTrue(session.transaction().aborted());
        Assert.assertEquals(1, session.transaction().size());
    }

    @Test
    public void transactionRejectsWhenSnapshotBytesExceedBudgetEvenIfEstimatedBytesDoNot() {
        EngineSession session = new EngineSession(4, 6);
        session.transaction().begin();

        ExecutionRequest underestimated = new ExecutionRequest() {
            private final byte[][] argv = new byte[][]{
                    "SET".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                    "k".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                    "value".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
            };

            @Override
            public int argc() {
                return argv.length;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public int len(int index) {
                return argv[index].length;
            }

            @Override
            public byte byteAt(int index, int offset) {
                return argv[index][offset];
            }

            @Override
            public void copyToByteArray(int index, byte[] dst, int dstOff) {
                System.arraycopy(argv[index], 0, dst, dstOff, argv[index].length);
            }

            @Override
            public byte[] toByteArray(int index) {
                return argv[index].clone();
            }

            @Override
            public int retainedBytes() {
                return 0;
            }

            @Override
            public void close() {
            }
        };

        Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(underestimated));
        Assert.assertTrue(session.transaction().aborted());
        Assert.assertEquals(0, session.transaction().size());
    }

    @Test
    public void transactionKeepsOneRetainedRequestViewUntilDiscard() {
        EngineSession session = new EngineSession(4, 64);
        RetainedRequestState state = new RetainedRequestState();
        ExecutionRequest original = new RetainedTrackingExecutionRequest(state);

        session.transaction().begin();
        Assert.assertNull(session.transaction().tryEnqueue(original));
        Assert.assertEquals(1, state.retainCalls.get());

        original.close();
        Assert.assertEquals(0, state.finalReleases.get());

        session.transaction().discard();
        Assert.assertEquals(1, state.finalReleases.get());
    }

    @Test
    public void connectionStatsAreReadOnlyObservationNotOwnedSessionState() {
        EngineSession session = new EngineSession(4, 64);
        Assert.assertNull(session.connectionStats());

        ConnectionStatsView stats = new TestConnectionStats(2, 24, 3, 1);
        session.bindConnectionStatsSupplier(() -> stats);

        Assert.assertSame(stats, session.connectionStats());
        Assert.assertEquals(2, session.connectionStats().pending());
        Assert.assertEquals(24L, session.connectionStats().pendingBytes());
        Assert.assertEquals(3L, session.connectionStats().commandsEnqueued());
        Assert.assertEquals(1L, session.connectionStats().commandsExecuted());
    }

    private record TestConnectionStats(
            int pending,
            long pendingBytes,
            long commandsEnqueued,
            long commandsExecuted
    ) implements ConnectionStatsView {
        @Override
        public boolean inputDisabledByExecutor() {
            return false;
        }

        @Override
        public boolean closing() {
            return false;
        }

        @Override
        public long commandsRejected() {
            return 0;
        }

        @Override
        public long commandsSkippedClosing() {
            return 0;
        }

        @Override
        public long closeAfterReply() {
            return 0;
        }

        @Override
        public long backpressureEnter() {
            return 0;
        }

        @Override
        public long backpressureExit() {
            return 0;
        }
    }

    @Test
    public void transactionResetPathsCloseOwnedSnapshots() throws Exception {
        EngineSession session = new EngineSession(4, 64);
        AtomicInteger closedSnapshots = new AtomicInteger();

        session.transaction().begin();
        Assert.assertNull(session.transaction().tryEnqueue(ByteArrayExecutionRequest.fromUtf8("SET", List.of("k", "v"))));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));

        session.transaction().begin();
        Assert.assertEquals(1, closedSnapshots.get());
        Assert.assertTrue(session.transaction().active());
        Assert.assertEquals(0, session.transaction().size());

        Assert.assertNull(session.transaction().tryEnqueue(ByteArrayExecutionRequest.fromUtf8("GET", List.of("k"))));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));
        session.transaction().discard();
        Assert.assertEquals(2, closedSnapshots.get());
        Assert.assertFalse(session.transaction().active());
        Assert.assertEquals(0, session.transaction().size());

        session.transaction().begin();
        Assert.assertNull(session.transaction().tryEnqueue(ByteArrayExecutionRequest.fromUtf8("DEL", List.of("k"))));
        replaceQueuedSnapshot(session.transaction(), new CloseTrackingExecutionRequest(closedSnapshots));
        session.discardTransaction();
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

    private static final class RetainedRequestState {
        private final AtomicInteger references = new AtomicInteger(1);
        private final AtomicInteger retainCalls = new AtomicInteger();
        private final AtomicInteger finalReleases = new AtomicInteger();
    }

    private static final class RetainedTrackingExecutionRequest implements ExecutionRequest {
        private final RetainedRequestState state;
        private final AtomicBoolean closed = new AtomicBoolean();

        private RetainedTrackingExecutionRequest(RetainedRequestState state) {
            this.state = state;
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
            return 3;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return "GET".getBytes(java.nio.charset.StandardCharsets.US_ASCII)[offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] source = "GET".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            System.arraycopy(source, 0, dst, dstOff, source.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            return "GET".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        }

        @Override
        public int retainedBytes() {
            return 3;
        }

        @Override
        public ExecutionRequest retain() {
            state.retainCalls.incrementAndGet();
            state.references.incrementAndGet();
            return new RetainedTrackingExecutionRequest(state);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) && state.references.decrementAndGet() == 0) {
                state.finalReleases.incrementAndGet();
            }
        }
    }
}
