package yier.bubu.redis.app.server;

// 连接关闭清理回归：MULTI 期间关闭连接必须清空事务队列，避免大请求数据长期驻留。

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReferenceCountedRequestMemoryLease;
import yier.bubu.redis.execution.api.RequestMemoryLease;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionQueueCleanupTest {
    @Test
    public void closingConnectionDiscardsTransactionState() {
        EmbeddedChannel ch = new EmbeddedChannel();
        AtomicInteger finalReleases = new AtomicInteger();
        AtomicInteger retainedCloses = new AtomicInteger();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 1, 16);
            TransactionState tx = connection.session().transaction();

            tx.begin();
            Assert.assertTrue(tx.active());
            Assert.assertFalse(tx.aborted());
            Assert.assertEquals(0, tx.size());

            try (LeaseBackedRequest queued = leasedRequest(
                    finalReleases, retainedCloses, "SET", "k", "v");
                 ByteArrayExecutionRequest rejected = request("GET", "k")) {
                Assert.assertNull(tx.tryEnqueue(queued));
                Assert.assertEquals(1, tx.size());

                // 触发一次入队失败，确保 aborted 标记也能被清理。
                Assert.assertNotNull(tx.tryEnqueue(rejected));
                Assert.assertTrue(tx.aborted());

                Assert.assertTrue(connection.markClosing());
                Assert.assertFalse(tx.active());
                Assert.assertFalse(tx.aborted());
                Assert.assertEquals(0, tx.size());
                Assert.assertEquals(0, finalReleases.get());
                Assert.assertEquals(1, retainedCloses.get());
            }
            Assert.assertEquals(1, finalReleases.get());
            Assert.assertEquals(1, retainedCloses.get());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
    }

    private static LeaseBackedRequest leasedRequest(
            AtomicInteger finalReleases,
            AtomicInteger retainedCloses,
            String... args
    ) {
        ByteArrayExecutionRequest request = request(args);
        RequestMemoryLease lease = new ReferenceCountedRequestMemoryLease(
                request.admittedMemoryBytes(), ignored -> finalReleases.incrementAndGet());
        return new LeaseBackedRequest(request, lease, retainedCloses, false);
    }

    private static final class LeaseBackedRequest implements ExecutionRequest {
        private final ByteArrayExecutionRequest request;
        private final RequestMemoryLease lease;
        private final AtomicInteger retainedCloses;
        private final boolean retainedView;

        private LeaseBackedRequest(
                ByteArrayExecutionRequest request,
                RequestMemoryLease lease,
                AtomicInteger retainedCloses,
                boolean retainedView
        ) {
            this.request = request;
            this.lease = lease;
            this.retainedCloses = retainedCloses;
            this.retainedView = retainedView;
        }

        @Override
        public int argc() {
            return request.argc();
        }

        @Override
        public boolean isNull(int index) {
            return request.isNull(index);
        }

        @Override
        public int len(int index) {
            return request.len(index);
        }

        @Override
        public byte byteAt(int index, int offset) {
            return request.byteAt(index, offset);
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            request.copyToByteArray(index, dst, dstOff);
        }

        @Override
        public byte[] toByteArray(int index) {
            return request.toByteArray(index);
        }

        @Override
        public byte[] readOnlyByteArray(int index) {
            return request.readOnlyByteArray(index);
        }

        @Override
        public int retainedBytes() {
            return request.retainedBytes();
        }

        @Override
        public long admittedMemoryBytes() {
            return lease.reservedBytes();
        }

        @Override
        public LeaseBackedRequest retain() {
            return new LeaseBackedRequest(request, lease.retain(), retainedCloses, true);
        }

        @Override
        public void close() {
            if (retainedView) {
                retainedCloses.incrementAndGet();
            }
            lease.close();
        }
    }
}
