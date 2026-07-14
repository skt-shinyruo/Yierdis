package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionReplySequencerTest {
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
            Thread producer = Thread.ofPlatform().start(() -> slot.cancel(ReplyCleanupOwner.SEQUENCER));
            Thread closer = Thread.ofPlatform().start(() -> slot.cancel(ReplyCleanupOwner.CONNECTION_CLOSE));
            producer.join();
            closer.join();

            Assert.assertNotEquals(ReplyCleanupOwner.NONE, slot.cleanupOwner());
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
}
