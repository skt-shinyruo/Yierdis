package yier.bubu.redis.app.server;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReferenceCountedRequestMemoryLease;
import yier.bubu.redis.execution.api.RequestMemoryLease;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.InboundConnectionMemory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RespIngressLifecycleIntegrationTest {
    @Test
    public void acceptedRequestKeepsLeaseUntilExecutorCompletion() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory("accepted", 1_024);
        AtomicInteger executions = new AtomicInteger();
        ExecutorFixture fixture = new ExecutorFixture(2, executions);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        LeaseBackedRequest request = admittedRequest(budget, memory, 128);
        try {
            fixture.write(request);

            Assert.assertEquals(128L, budget.stats().reservedBytes());
            Assert.assertEquals(0, executions.get());
            Assert.assertEquals(1, fixture.connection.context().statsSnapshot().pending());

            unblock.countDown();
            Assert.assertTrue(request.awaitFinalRelease());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(1, executions.get());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void rejectedAndClosingQueuedRequestsReleaseInboundLeases() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory("rejected", 1_024);
        AtomicInteger executions = new AtomicInteger();
        ExecutorFixture fixture = new ExecutorFixture(1, executions);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        LeaseBackedRequest queued = admittedRequest(budget, memory, 128);
        LeaseBackedRequest rejected = admittedRequest(budget, memory, 128);
        try {
            fixture.write(queued);
            fixture.write(rejected);

            Assert.assertTrue(rejected.awaitFinalRelease());
            Assert.assertEquals(128L, budget.stats().reservedBytes());

            fixture.connection.markClosing();
            unblock.countDown();

            Assert.assertTrue(queued.awaitFinalRelease());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(0, executions.get());
            Assert.assertEquals(1L, fixture.connection.context().statsSnapshot().commandsSkippedClosing());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void transactionAndDetachedRetainedViewReleaseExactlyOnce() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory("transaction", 1_024);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 4, 1_024);
            LeaseBackedRequest queued = admittedRequest(budget, memory, 128);

            connection.session().transaction().begin();
            Assert.assertNull(connection.session().transaction().tryEnqueue(queued));
            Assert.assertEquals(1, queued.retainCalls());
            queued.close();
            Assert.assertEquals(128L, budget.stats().reservedBytes());

            Assert.assertTrue(connection.markClosing());
            Assert.assertTrue(queued.awaitFinalRelease());
            Assert.assertEquals(0L, budget.stats().reservedBytes());

            LeaseBackedRequest original = admittedRequest(budget, memory, 128);
            ExecutionRequest retained = original.retain();
            original.close();
            memory.close();

            Thread finalizer = new Thread(retained::close, "inbound-lease-finalizer");
            finalizer.start();
            finalizer.join(1_000L);

            Assert.assertFalse(finalizer.isAlive());
            Assert.assertTrue(original.awaitFinalRelease());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static InboundConnectionMemory connectionMemory(String id, long hardLimitBytes) {
        return new InboundConnectionMemory(id, hardLimitBytes, Runnable::run, () -> { });
    }

    private static LeaseBackedRequest admittedRequest(
            InboundMemoryBudget budget,
            InboundConnectionMemory memory,
            long reservedBytes
    ) {
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(memory, reservedBytes));
        LeaseState state = new LeaseState();
        RequestMemoryLease lease = new ReferenceCountedRequestMemoryLease(reservedBytes, bytes -> {
            budget.release(memory, bytes);
            state.finalReleases.incrementAndGet();
            state.released.countDown();
        });
        return new LeaseBackedRequest(new byte[][]{ascii("PING")}, lease, state);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class ExecutorFixture implements AutoCloseable {
        private final DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        private final EventExecutor owner = group.next();
        private final CommandExecutor<NettyExecutionConnection> executor;
        private final OrderedReplyTestFixture replies;
        private final EmbeddedChannel channel;
        private final NettyExecutionConnection connection;

        private ExecutorFixture(int queueCapacity, AtomicInteger executions) {
            executor = new CommandExecutor<>(
                    () -> { },
                    (session, request, out) -> {
                        executions.incrementAndGet();
                        out.simpleString("OK");
                    },
                    owner,
                    new RespReplyWriterFactory(),
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(queueCapacity, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            replies = OrderedReplyTestFixture.open(executor, new RespReplyWriterFactory());
            channel = replies.channel();
            connection = replies.connection();
        }

        private void blockOwner(CountDownLatch started, CountDownLatch unblock) {
            owner.submit(() -> {
                started.countDown();
                unblock.await();
                return null;
            });
        }

        private void write(ExecutionRequest request) {
            replies.write(request);
        }

        @Override
        public void close() {
            executor.shutdownGracefully().join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
        }
    }

    private static final class LeaseState {
        private final AtomicInteger retainCalls = new AtomicInteger();
        private final AtomicInteger finalReleases = new AtomicInteger();
        private final CountDownLatch released = new CountDownLatch(1);
    }

    private static final class LeaseBackedRequest implements ExecutionRequest {
        private final byte[][] argv;
        private final RequestMemoryLease lease;
        private final LeaseState state;

        private LeaseBackedRequest(byte[][] argv, RequestMemoryLease lease, LeaseState state) {
            this.argv = argv;
            this.lease = lease;
            this.state = state;
        }

        private boolean awaitFinalRelease() throws InterruptedException {
            return state.released.await(1, TimeUnit.SECONDS);
        }

        private int retainCalls() {
            return state.retainCalls.get();
        }

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            return argv[index] == null;
        }

        @Override
        public int len(int index) {
            byte[] value = argv[index];
            return value == null ? -1 : value.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] value = argv[index];
            System.arraycopy(value, 0, dst, dstOff, value.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            byte[] value = argv[index];
            return value == null ? null : value.clone();
        }

        @Override
        public byte[] readOnlyByteArray(int index) {
            return argv[index];
        }

        @Override
        public int retainedBytes() {
            int total = 0;
            for (byte[] value : argv) {
                if (value != null) {
                    total += value.length;
                }
            }
            return total;
        }

        @Override
        public long admittedMemoryBytes() {
            return lease.reservedBytes();
        }

        @Override
        public ExecutionRequest retain() {
            state.retainCalls.incrementAndGet();
            return new LeaseBackedRequest(argv, lease.retain(), state);
        }

        @Override
        public void close() {
            lease.close();
        }
    }
}
