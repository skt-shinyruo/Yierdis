package yier.bubu.redis.app.server;

import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReferenceCountedRequestMemoryLease;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.RequestMemoryLease;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.protocol.resp.RespReplyWriter;
import yier.bubu.redis.protocol.resp.netty.InboundByteAccountingHandler;
import yier.bubu.redis.protocol.resp.netty.InboundConnectionMemory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RespIngressLifecycleIntegrationTest {
    @Test
    public void acceptedRequestKeepsLeaseUntilExecutorCompletion() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory(1_024);
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
    public void deferredAndClosingQueuedRequestsReleaseInboundLeases() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory(1_024);
        AtomicInteger executions = new AtomicInteger();
        ExecutorFixture fixture = new ExecutorFixture(1, executions);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        LeaseBackedRequest queued = admittedRequest(budget, memory, 128);
        LeaseBackedRequest deferred = admittedRequest(budget, memory, 128);
        try {
            fixture.write(queued);
            fixture.write(deferred);

            Assert.assertEquals(256L, budget.stats().reservedBytes());
            Assert.assertEquals(0L, fixture.connection.context().statsSnapshot().commandsRejected());

            fixture.connection.markClosing();
            unblock.countDown();

            Assert.assertTrue(queued.awaitFinalRelease());
            Assert.assertTrue(fixture.awaitFinalRelease(deferred));
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(0, executions.get());
            Assert.assertEquals(1L, fixture.connection.context().statsSnapshot().commandsSkippedClosing());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void shutdownRejectsDeferredRequestWithoutDroppingAnEarlierReadyReply() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory(1_024);
        ExecutorFixture fixture = new ExecutorFixture(1, new AtomicInteger());
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        LeaseBackedRequest queued = admittedRequest(budget, memory, 128);
        LeaseBackedRequest deferred = admittedRequest(budget, memory, 128);
        try {
            fixture.write(queued);
            ReplySlot ready = fixture.replies.registerReadyAscii("ready-before-shutdown");
            fixture.write(deferred);
            Assert.assertEquals(ReplySlotState.READY, ready.state());

            fixture.connection.markClosing();
            CompletableFuture<Void> executorDrained = fixture.executor.shutdownGracefully();
            fixture.replies.drain();

            Assert.assertTrue("shutdown must leave transport ownership to the sequencer", fixture.channel.isOpen());
            Assert.assertTrue(fixture.awaitFinalRelease(deferred));

            unblock.countDown();
            executorDrained.join();
            CompletableFuture<Void> repliesDrained = fixture.connection.shutdownReplyGracefully();
            fixture.replies.drain();

            Assert.assertEquals("ready-before-shutdown", readOutboundAscii(fixture.channel));
            Assert.assertEquals(ReplySlotState.COMPLETED, ready.state());
            Assert.assertTrue(repliesDrained.isDone());
            Assert.assertFalse(fixture.channel.isOpen());
            Assert.assertTrue(queued.awaitFinalRelease());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void samePacketProtocolErrorCompletesDeferredRequestBeforeTerminalReply() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        ProtocolExecutorFixture fixture = new ProtocolExecutorFixture(1, executions);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        try {
            fixture.writePacket(
                    "*1\r\n$4\r\nPING\r\n"
                            + "*1\r\n$4\r\nPING\r\n"
                            + "*1\r\nPING\r\n"
            );

            Assert.assertEquals(1L, fixture.executor.statsSnapshot().submitRejectedQueueFull());
            Assert.assertNull(fixture.channel.readOutbound());

            unblock.countDown();

            Assert.assertEquals("-ERR busy queue_full\r\n", awaitOutboundAscii(fixture.channel));
            String protocolError = awaitOutboundAscii(fixture.channel);
            Assert.assertTrue(protocolError, protocolError.startsWith("-ERR Protocol error"));
            Assert.assertTrue(awaitChannelClosed(fixture.channel));
            Assert.assertEquals(0, executions.get());
            Assert.assertEquals(1L, fixture.connection.context().statsSnapshot().commandsSkippedClosing());
            Assert.assertEquals(0L, fixture.inboundBudget.stats().reservedBytes());
            Assert.assertEquals(0L, fixture.outboundBudget.stats().reservedBytes());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void internalErrorCompletesDeferredRequestBeforeTerminalReply() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        ProtocolExecutorFixture fixture = new ProtocolExecutorFixture(1, executions);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        fixture.blockOwner(blockerStarted, unblock);
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        try {
            fixture.writePacket(
                    "*1\r\n$4\r\nPING\r\n"
                            + "*1\r\n$4\r\nPING\r\n"
            );
            Assert.assertEquals(1L, fixture.executor.statsSnapshot().submitRejectedQueueFull());

            fixture.channel.pipeline().fireExceptionCaught(new IllegalStateException("injected internal failure"));
            Assert.assertNull(fixture.channel.readOutbound());

            unblock.countDown();

            Assert.assertEquals("-ERR busy queue_full\r\n", awaitOutboundAscii(fixture.channel));
            Assert.assertEquals("-ERR internal error\r\n", awaitOutboundAscii(fixture.channel));
            Assert.assertTrue(awaitChannelClosed(fixture.channel));
            Assert.assertEquals(0, executions.get());
            Assert.assertEquals(1L, fixture.connection.context().statsSnapshot().commandsSkippedClosing());
            Assert.assertEquals(0L, fixture.inboundBudget.stats().reservedBytes());
            Assert.assertEquals(0L, fixture.outboundBudget.stats().reservedBytes());
        } finally {
            unblock.countDown();
            fixture.close();
        }
    }

    @Test
    public void transactionAndDetachedRetainedViewReleaseExactlyOnce() throws Exception {
        InboundMemoryBudget budget = new InboundMemoryBudget(1_024);
        InboundConnectionMemory memory = connectionMemory(1_024);
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

    private static InboundConnectionMemory connectionMemory(long hardLimitBytes) {
        return new InboundConnectionMemory(hardLimitBytes, Runnable::run, () -> { });
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

    private static String readOutboundAscii(EmbeddedChannel channel) {
        ByteBuf buffer = channel.readOutbound();
        Assert.assertNotNull("expected a drained ready reply", buffer);
        try {
            return buffer.toString(StandardCharsets.US_ASCII);
        } finally {
            buffer.release();
        }
    }

    private static String awaitOutboundAscii(EmbeddedChannel channel) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadlineNanos) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
            ByteBuf buffer = channel.readOutbound();
            if (buffer != null) {
                try {
                    return buffer.toString(StandardCharsets.US_ASCII);
                } finally {
                    buffer.release();
                }
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("timed out waiting for outbound reply");
    }

    private static boolean awaitChannelClosed(EmbeddedChannel channel) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (channel.isOpen() && System.nanoTime() < deadlineNanos) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
            Thread.sleep(1L);
        }
        return !channel.isOpen();
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
                    (session, request) -> okPrepared(executions),
                    new NettySerialOwnerExecutor(owner),
                    new RespReplySizer(),
                    RespReplyWriter::new,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(queueCapacity, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            replies = OrderedReplyTestFixture.open(executor, RespReplyWriter::new);
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

        private boolean awaitFinalRelease(LeaseBackedRequest request) throws InterruptedException {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (!request.isFinalReleased() && System.nanoTime() < deadlineNanos) {
                replies.drain();
                Thread.sleep(1L);
            }
            return request.isFinalReleased();
        }

        @Override
        public void close() {
            executor.shutdownGracefully().join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
        }
    }

    private static final class ProtocolExecutorFixture implements AutoCloseable {
        private static final long INBOUND_CAPACITY_BYTES = 64L * 1024L;
        private static final long OUTBOUND_CONNECTION_BYTES = 256L * 1024L;
        private static final long OUTBOUND_GLOBAL_BYTES = 512L * 1024L;

        private final DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        private final EventExecutor owner = group.next();
        private final CommandExecutor<NettyExecutionConnection> executor;
        private final InboundMemoryBudget inboundBudget = new InboundMemoryBudget(INBOUND_CAPACITY_BYTES);
        private final InboundConnectionMemory inboundMemory = connectionMemory(INBOUND_CAPACITY_BYTES);
        private final OutboundMemoryBudget outboundBudget = new OutboundMemoryBudget(OUTBOUND_GLOBAL_BYTES);
        private final EmbeddedChannel channel = new EmbeddedChannel();
        private final NettyExecutionConnection connection;
        private final ConnectionReplySequencer sequencer;

        private ProtocolExecutorFixture(int queueCapacity, AtomicInteger executions) {
            BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
            executor = new CommandExecutor<>(
                    () -> { },
                    (session, request) -> okPrepared(executions),
                    new NettySerialOwnerExecutor(owner),
                    new RespReplySizer(),
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(queueCapacity, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            connection = NettyExecutionConnection.getOrCreate(channel, 16, 1_024L);
            connection.bindOwnerTaskExecutor(task -> executor.executeOwnerTask(task));
            OutboundConnectionMemory outboundMemory = outboundBudget.openConnection(OUTBOUND_CONNECTION_BYTES);
            sequencer = new ConnectionReplySequencer(
                    channel,
                    outboundMemory,
                    () -> { },
                    slot -> BoundedChunkedReplySink.forChannel(
                            slot,
                            channel,
                            64 * 1024,
                            OrderedReplyTestFixture.CONTROL_BYTES,
                            OrderedReplyTestFixture.MAX_REPLY_BYTES
                    )
            );
            NettyReplyDecodedMessageGate gate = new NettyReplyDecodedMessageGate(
                    OrderedReplyTestFixture.CONTROL_BYTES,
                    OrderedReplyTestFixture.MAX_REPLY_BYTES,
                    outboundMemory,
                    sequencer
            );
            connection.bindReplyGate(gate);

            InboundReadCreditHandler readCredits = new InboundReadCreditHandler(
                    inboundBudget,
                    inboundMemory,
                    8 * 1024
            );
            RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                    1_024,
                    16,
                    1_024,
                    1_024,
                    inboundBudget,
                    inboundMemory,
                    gate
            );
            decoder.setReadControl(readCredits);
            channel.pipeline()
                    .addLast("inboundReadCredit", readCredits)
                    .addLast("inboundByteAccounting", new InboundByteAccountingHandler(readCredits))
                    .addLast("respRequestDecoder", decoder)
                    .addLast("executionRequestIngress", new NettyExecutionRequestIngress(executor, replyWriterFactory));
        }

        private void blockOwner(CountDownLatch started, CountDownLatch unblock) {
            owner.submit(() -> {
                started.countDown();
                unblock.await();
                return null;
            });
        }

        private void writePacket(String packet) {
            channel.writeInbound(Unpooled.copiedBuffer(packet, StandardCharsets.US_ASCII));
            channel.runPendingTasks();
        }

        @Override
        public void close() {
            executor.shutdownGracefully().join();
            group.shutdownGracefully().syncUninterruptibly();
            channel.finishAndReleaseAll();
            sequencer.close();
            inboundMemory.close();
            inboundBudget.close();
            outboundBudget.close();
        }
    }

    private static PreparedCommand okPrepared(AtomicInteger executions) {
        return new PreparedCommand() {
            @Override
            public ReplyShape reservationShape() {
                return ReplyShapes.simpleString("OK");
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public CommandResult execute(CommandSession context) {
                executions.incrementAndGet();
                return CommandResult.reply(RedisReplies.simpleString("OK"));
            }

            @Override
            public void close() {
            }
        };
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

        private boolean isFinalReleased() {
            return state.finalReleases.get() > 0;
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
