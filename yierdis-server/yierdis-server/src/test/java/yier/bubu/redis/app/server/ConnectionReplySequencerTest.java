package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionReplySequencerTest {
    @Test
    public void cleanupIncludesAnAsyncResourceTransferredWhileTheLeaseIsClosing() throws Exception {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        OutboundConnectionMemory waiterConnection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        CountDownLatch leaseCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseLeaseClose = new CountDownLatch(1);
        CompletableFuture<Void> resourceClose = new CompletableFuture<>();
        AtomicInteger closes = new AtomicInteger();
        Thread cleanup = null;
        try {
            Assert.assertTrue(waiterConnection.awaitCapacity(4_096L, 4_096L, () -> {
                leaseCloseEntered.countDown();
                try {
                    Assert.assertTrue(releaseLeaseClose.await(1, TimeUnit.SECONDS));
                } catch (InterruptedException failure) {
                    throw new AssertionError(failure);
                }
            }));

            cleanup = Thread.ofPlatform().start(slot::cancelNow);
            Assert.assertTrue(leaseCloseEntered.await(1, TimeUnit.SECONDS));
            slot.addOwnedResource(closes::incrementAndGet, resource -> resourceClose.thenRun(() -> {
                try {
                    resource.close();
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            }));
            releaseLeaseClose.countDown();
            cleanup.join(1_000L);

            Assert.assertFalse(cleanup.isAlive());
            Assert.assertFalse(slot.cleanupCompletion().isDone());
            Assert.assertEquals(0, closes.get());

            resourceClose.complete(null);

            Assert.assertEquals(1, closes.get());
            Assert.assertTrue(slot.cleanupCompletion().isDone());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            releaseLeaseClose.countDown();
            resourceClose.complete(null);
            if (cleanup != null) {
                cleanup.join(1_000L);
            }
            waiterConnection.close();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void cancellationAfterChunksAreClaimedKeepsTheLeaseUntilEveryChunkIsReleased() throws Exception {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        BlockingOutboundWrite blockedWrite = new BlockingOutboundWrite();
        EmbeddedChannel channel = new EmbeddedChannel(blockedWrite);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ByteBuf chunk = Unpooled.copiedBuffer("claimed", StandardCharsets.US_ASCII);
        Thread writer = null;
        try {
            slot.addChunk(chunk);
            writer = Thread.ofPlatform().start(() -> slot.markReady(false));
            Assert.assertTrue(blockedWrite.writeEntered.await(1, TimeUnit.SECONDS));

            Assert.assertTrue(slot.cancelNow());
            Assert.assertEquals(1, chunk.refCnt());
            Assert.assertFalse(slot.lease().closed());
            Assert.assertEquals(4_096L, budget.stats().reservedBytes());

            blockedWrite.releaseWrite.countDown();
            writer.join(1_000L);

            Assert.assertFalse(writer.isAlive());
            Assert.assertEquals(0, chunk.refCnt());
            Assert.assertTrue(slot.lease().closed());
            Assert.assertTrue(slot.cleanupCompletion().isDone());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            blockedWrite.releaseWrite.countDown();
            if (writer != null) {
                writer.join(1_000L);
            }
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void cleanupWaitsForAResourceTransferredWhileAnInFlightChunkKeepsTerminationOpen() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        DelayedOutboundWrites delayedWrites = new DelayedOutboundWrites();
        EmbeddedChannel channel = new EmbeddedChannel(delayedWrites);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        CompletableFuture<Void> resourceClose = new CompletableFuture<>();
        AtomicInteger closes = new AtomicInteger();
        try {
            slot.addChunk(Unpooled.copiedBuffer("reply", StandardCharsets.US_ASCII));
            slot.markReady(false);
            drain(channel);
            Assert.assertEquals(1, delayedWrites.pendingWriteCount());

            Assert.assertTrue(slot.cancelNow());
            slot.addOwnedResource(closes::incrementAndGet, resource -> resourceClose.thenRun(() -> {
                try {
                    resource.close();
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            }));
            delayedWrites.succeedAll();
            drain(channel);

            Assert.assertFalse(slot.cleanupCompletion().isDone());
            Assert.assertFalse(slot.lease().closed());
            Assert.assertEquals(4_096L, budget.stats().reservedBytes());

            resourceClose.complete(null);

            Assert.assertEquals(1, closes.get());
            Assert.assertTrue(slot.cleanupCompletion().isDone());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            resourceClose.complete(null);
            delayedWrites.failAll();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void terminationWaitsForOwnerResourceCloseAfterTheTransportHasClosed() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ReplyEgressStats stats = new ReplyEgressStats();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(
                channel,
                connection,
                () -> { },
                slot -> {
                    throw new IllegalStateException("test does not create a reply sink");
                },
                stats
        );
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        CompletableFuture<Void> ownerClose = new CompletableFuture<>();
        AtomicInteger closes = new AtomicInteger();
        try {
            slot.addOwnedResource(closes::incrementAndGet, resource -> ownerClose.thenRun(() -> {
                try {
                    resource.close();
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            }));
            slot.addChunk(Unpooled.copiedBuffer("reply", StandardCharsets.US_ASCII));
            slot.markReady(false);
            drain(channel);
            Assert.assertEquals("reply", readAscii(channel));

            CompletableFuture<Void> terminated = sequencer.shutdownGracefully();
            drain(channel);

            Assert.assertFalse(channel.isOpen());
            Assert.assertFalse(terminated.isDone());
            Assert.assertFalse(slot.lease().closed());
            Assert.assertEquals(1L, stats.snapshot().activeSources());
            Assert.assertEquals(4_096L, budget.stats().reservedBytes());

            ownerClose.complete(null);

            Assert.assertEquals(1, closes.get());
            Assert.assertTrue(terminated.isDone());
            Assert.assertEquals(0L, stats.snapshot().activeSources());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            ownerClose.complete(null);
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void shutdownWaitsForSourceCleanupWhenCancellationClosesTheTransport() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        CompletableFuture<Void> ownerClose = new CompletableFuture<>();
        AtomicInteger closes = new AtomicInteger();
        try {
            slot.addOwnedResource(closes::incrementAndGet, resource -> {
                channel.close();
                return ownerClose.thenRun(() -> {
                    try {
                        resource.close();
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    }
                });
            });

            CompletableFuture<Void> terminated = sequencer.shutdownGracefully();
            drain(channel);

            Assert.assertFalse(channel.isOpen());
            Assert.assertFalse(terminated.isDone());
            Assert.assertFalse(slot.lease().closed());
            Assert.assertEquals(4_096L, budget.stats().reservedBytes());

            ownerClose.complete(null);

            Assert.assertEquals(1, closes.get());
            Assert.assertTrue(terminated.isDone());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            ownerClose.complete(null);
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void submitsFollowingReadySlotsBeforeThePriorWriteFutureCompletes() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(8_192L);
        OutboundConnectionMemory connection = budget.openConnection(8_192L);
        DelayedOutboundWrites delayedWrites = new DelayedOutboundWrites();
        EmbeddedChannel channel = new EmbeddedChannel(delayedWrites);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot first = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ReplySlot second = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        try {
            second.addChunk(Unpooled.copiedBuffer("second", StandardCharsets.US_ASCII));
            second.markReady(false);
            first.addChunk(Unpooled.copiedBuffer("first", StandardCharsets.US_ASCII));
            first.markReady(false);
            drain(channel);

            Assert.assertEquals(2, delayedWrites.pendingWriteCount());
            Assert.assertEquals(1, delayedWrites.flushCount());
            Assert.assertEquals(ReplySlotState.WRITING, first.state());
            Assert.assertEquals(ReplySlotState.WRITING, second.state());

            delayedWrites.succeedAll();
            drain(channel);

            Assert.assertEquals(ReplySlotState.COMPLETED, first.state());
            Assert.assertEquals(ReplySlotState.COMPLETED, second.state());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            delayedWrites.failAll();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void failureOfAnyChunkFailsTheWholeSlotEvenWhenTheLastChunkSucceeds() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        DelayedOutboundWrites delayedWrites = new DelayedOutboundWrites();
        EmbeddedChannel channel = new EmbeddedChannel(delayedWrites);
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        try {
            slot.addChunk(Unpooled.copiedBuffer("first", StandardCharsets.US_ASCII));
            slot.addChunk(Unpooled.copiedBuffer("last", StandardCharsets.US_ASCII));
            slot.markReady(false);
            drain(channel);

            delayedWrites.failFirstAndSucceedRest();
            drain(channel);

            Assert.assertEquals(ReplySlotState.FAILED, slot.state());
            Assert.assertFalse(channel.isOpen());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            delayedWrites.failAll();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void cleanupWaitsForAnActiveProducerAndCannotAcceptLateChunks() throws Exception {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ByteBuf chunk = Unpooled.buffer(8, 8);
        CountDownLatch producerInside = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        CountDownLatch cleanupDone = new CountDownLatch(1);
        try {
            Thread producer = Thread.ofPlatform().start(() -> slot.runProducerAction(() -> {
                slot.addChunk(chunk);
                producerInside.countDown();
                try {
                    Assert.assertTrue(releaseProducer.await(1, TimeUnit.SECONDS));
                } catch (InterruptedException failure) {
                    throw new AssertionError(failure);
                }
                chunk.writeByte('x');
            }));
            Assert.assertTrue(producerInside.await(1, TimeUnit.SECONDS));
            Thread cleanup = Thread.ofPlatform().start(() -> {
                slot.cancelNow();
                cleanupDone.countDown();
            });

            Assert.assertFalse("cleanup must not release a producer-owned buffer", cleanupDone.await(100, TimeUnit.MILLISECONDS));
            Assert.assertEquals(1, chunk.refCnt());
            releaseProducer.countDown();
            producer.join(1_000L);
            cleanup.join(1_000L);

            Assert.assertFalse(producer.isAlive());
            Assert.assertFalse(cleanup.isAlive());
            Assert.assertEquals(0, chunk.refCnt());
            Assert.assertEquals(ReplySlotState.CANCELLED, slot.state());

            ByteBuf late = Unpooled.buffer(1, 1);
            Assert.assertThrows(IllegalStateException.class, () -> slot.addChunk(late));
            Assert.assertEquals(0, late.refCnt());
        } finally {
            releaseProducer.countDown();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void writesReadySlotsInRegistrationOrder() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(12_288L);
        OutboundConnectionMemory connection = budget.openConnection(12_288L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot first = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ReplySlot second = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        try {
            Assert.assertEquals(0L, first.sequence());
            Assert.assertEquals(1L, second.sequence());

            second.addChunk(Unpooled.copiedBuffer("second", StandardCharsets.US_ASCII));
            second.markReady(false);
            drain(channel);
            Assert.assertNull(channel.readOutbound());

            first.addChunk(Unpooled.copiedBuffer("first", StandardCharsets.US_ASCII));
            first.markReady(false);
            drain(channel);

            Assert.assertEquals(ReplySlotState.COMPLETED, first.state());
            Assert.assertEquals(2, channel.outboundMessages().size());
            Assert.assertEquals("first", readAscii(channel));
            Assert.assertEquals("second", readAscii(channel));
            Assert.assertEquals(ReplySlotState.COMPLETED, first.state());
            Assert.assertEquals(ReplySlotState.COMPLETED, second.state());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void closeAfterReplyRejectsLaterRegistrationAndCleansEverySlotOnce() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(12_288L);
        OutboundConnectionMemory connection = budget.openConnection(12_288L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot first = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ReplySlot later = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        AtomicInteger firstResourceCloses = new AtomicInteger();
        AtomicInteger laterResourceCloses = new AtomicInteger();
        first.addOwnedResource(firstResourceCloses::incrementAndGet);
        later.addOwnedResource(laterResourceCloses::incrementAndGet);
        try {
            first.addChunk(Unpooled.copiedBuffer("bye", StandardCharsets.US_ASCII));
            first.markReady(true);
            drain(channel);

            Assert.assertEquals("bye", readAscii(channel));
            Assert.assertFalse(sequencer.acceptingRegistrations());
            Assert.assertTrue(connection.reserve(4_096L, 4_096L).isEmpty());
            Assert.assertEquals(1, firstResourceCloses.get());
            Assert.assertEquals(1, laterResourceCloses.get());
            Assert.assertEquals(ReplySlotState.CANCELLED, later.state());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void racingTerminalCleanupClaimsOneOwnerAndReleasesTheLeaseOnce() throws Exception {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot slot = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        AtomicInteger closes = new AtomicInteger();
        slot.addOwnedResource(closes::incrementAndGet);
        try {
            Thread producer = Thread.ofPlatform().start(() -> slot.cancel());
            Thread closer = Thread.ofPlatform().start(() -> slot.cancel());
            producer.join();
            closer.join();

            Assert.assertTrue(slot.cleanupCompletion().isDone());
            Assert.assertEquals(1, closes.get());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(ReplySlotState.CANCELLED, slot.state());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void gracefulShutdownCancelsBlockedSlotsDrainsReadyHeadsAndThenClosesTheChild() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(12_288L);
        OutboundConnectionMemory connection = budget.openConnection(12_288L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot blocked = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ReplySlot ready = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        try {
            ready.addChunk(Unpooled.copiedBuffer("ready", StandardCharsets.US_ASCII));
            ready.markReady(false);

            CompletableFuture<Void> drained = sequencer.shutdownGracefully();
            drain(channel);

            Assert.assertEquals("ready", readAscii(channel));
            Assert.assertEquals(ReplySlotState.CANCELLED, blocked.state());
            Assert.assertEquals(ReplySlotState.COMPLETED, ready.state());
            Assert.assertTrue(drained.isDone());
            Assert.assertFalse(channel.isOpen());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void executionConnectionDelegatesGracefulReplyShutdownToItsGate() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(8_192L);
        OutboundConnectionMemory connectionMemory = budget.openConnection(8_192L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connectionMemory, () -> { });
        NettyReplyDecodedMessageGate gate = new NettyReplyDecodedMessageGate(
                4_096L,
                4_096L,
                connectionMemory,
                sequencer
        );
        NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 16, 1_024L);
        connection.bindReplyGate(gate);
        ReplySlot ready = sequencer.register(connectionMemory.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        try {
            ready.addChunk(Unpooled.copiedBuffer("ready", StandardCharsets.US_ASCII));
            ready.markReady(false);

            CompletableFuture<Void> drained = connection.shutdownReplyGracefully();
            drain(channel);

            Assert.assertEquals("ready", readAscii(channel));
            Assert.assertTrue(drained.isDone());
            Assert.assertFalse(channel.isOpen());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void egressStatsRetainTerminalCountsAndReleaseActiveOwnershipOnShutdown() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(12_288L);
        OutboundConnectionMemory connection = budget.openConnection(12_288L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ReplyEgressStats stats = new ReplyEgressStats();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(
                channel,
                connection,
                () -> { },
                slot -> {
                    throw new IllegalStateException("test does not create a reply sink");
                },
                stats
        );
        ReplySlot blocked = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        ReplySlot ready = sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
        AtomicInteger closes = new AtomicInteger();
        try {
            blocked.addOwnedResource(closes::incrementAndGet);
            ready.addOwnedResource(closes::incrementAndGet);
            ready.addChunk(Unpooled.copiedBuffer("ready", StandardCharsets.US_ASCII));
            blocked.recordOversizedReply();
            blocked.markResultUnknown();
            ready.markReady(false);

            ReplyEgressStats.Snapshot beforeShutdown = stats.snapshot();
            Assert.assertEquals(1L, beforeShutdown.activeChunks());
            Assert.assertEquals(2L, beforeShutdown.activeSources());

            sequencer.shutdownGracefully();
            drain(channel);

            Assert.assertEquals("ready", readAscii(channel));
            ReplyEgressStats.Snapshot afterShutdown = stats.snapshot();
            Assert.assertEquals(0L, afterShutdown.activeChunks());
            Assert.assertEquals(0L, afterShutdown.activeSources());
            Assert.assertEquals(1L, afterShutdown.cancelledSlots());
            Assert.assertEquals(1L, afterShutdown.oversizedReplies());
            Assert.assertEquals(1L, afterShutdown.resultUnknownCloses());
            Assert.assertEquals(2, closes.get());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    private static String readAscii(EmbeddedChannel channel) {
        ByteBuf buffer = channel.readOutbound();
        Assert.assertNotNull("expected an outbound reply chunk", buffer);
        try {
            return buffer.toString(StandardCharsets.US_ASCII);
        } finally {
            buffer.release();
        }
    }

    private static void drain(EmbeddedChannel channel) {
        for (int i = 0; i < 4; i++) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
        }
    }

    private static final class DelayedOutboundWrites extends ChannelOutboundHandlerAdapter {
        private final List<PendingWrite> pendingWrites = new ArrayList<>();
        private int flushCount;

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
            pendingWrites.add(new PendingWrite(message, promise));
        }

        @Override
        public void flush(ChannelHandlerContext context) {
            flushCount++;
        }

        private int pendingWriteCount() {
            return pendingWrites.size();
        }

        private int flushCount() {
            return flushCount;
        }

        private void succeedAll() {
            completeAll(null);
        }

        private void failAll() {
            completeAll(new IllegalStateException("test cleanup"));
        }

        private void failFirstAndSucceedRest() {
            List<PendingWrite> writes = List.copyOf(pendingWrites);
            pendingWrites.clear();
            for (int index = 0; index < writes.size(); index++) {
                PendingWrite write = writes.get(index);
                ReferenceCountUtil.release(write.message());
                if (index == 0) {
                    write.promise().setFailure(new IllegalStateException("injected first chunk failure"));
                } else {
                    write.promise().setSuccess();
                }
            }
        }

        private void completeAll(Throwable failure) {
            List<PendingWrite> writes = List.copyOf(pendingWrites);
            pendingWrites.clear();
            for (PendingWrite write : writes) {
                ReferenceCountUtil.release(write.message());
                if (failure == null) {
                    write.promise().setSuccess();
                } else {
                    write.promise().setFailure(failure);
                }
            }
        }

        private record PendingWrite(Object message, ChannelPromise promise) {
        }
    }

    private static final class BlockingOutboundWrite extends ChannelOutboundHandlerAdapter {
        private final CountDownLatch writeEntered = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
            writeEntered.countDown();
            try {
                if (!releaseWrite.await(1, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting to release outbound write");
                }
                ReferenceCountUtil.release(message);
                promise.setSuccess();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                ReferenceCountUtil.release(message);
                promise.setFailure(failure);
            }
        }
    }
}
