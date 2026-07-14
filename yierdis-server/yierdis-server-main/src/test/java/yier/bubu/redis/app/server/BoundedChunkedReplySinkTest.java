package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedChunkedReplySinkTest {
    @Test
    public void exactReservationConvertsToFixedChunkLeases() {
        Fixture fixture = new Fixture(200 * 1024L);
        try {
            BoundedChunkedReplySink sink = fixture.sink(Unpooled::buffer);
            sink.require(ReplyPlan.exact(150L * 1024L, 0L));
            sink.writeBytes(new byte[150 * 1024], 0, 150 * 1024);
            sink.finish();
            fixture.slot.markReady(false);
            fixture.drain();

            Assert.assertEquals(List.of(65_536, 65_536, 22_528), fixture.outboundCapacities());
            Assert.assertTrue(fixture.peakReservedBytes() <= 200L * 1024L);
            Assert.assertEquals(0L, fixture.budget.stats().allocatedBytes());
            Assert.assertEquals(0L, fixture.budget.stats().reservedBytes());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void allocatorCapacityAboveConvertedCreditFailsWithoutRetroactiveAdmission() {
        Fixture fixture = new Fixture(16 * 1024L);
        try {
            BoundedChunkedReplySink sink = fixture.sink((initialCapacity, maxCapacity) ->
                    Unpooled.buffer(initialCapacity + 1, initialCapacity + 1)
            );
            sink.require(ReplyPlan.exact(512L, 0L));

            Assert.assertThrows(IllegalStateException.class, () -> sink.writeBytes(new byte[512], 0, 512));
            Assert.assertEquals(0L, fixture.budget.stats().allocatedBytes());
            Assert.assertEquals(0L, fixture.budget.stats().reservedBytes());
            Assert.assertEquals(ReplySlotState.FAILED, fixture.slot.state());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void unplannedControlReplyDoesNotReserveTheWholeSingleReplyLimit() {
        Fixture fixture = new Fixture(64L * 1024L * 1024L);
        try {
            BoundedChunkedReplySink sink = fixture.sink(Unpooled::buffer);
            byte[] pong = "+PONG\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

            sink.writeBytes(pong, 0, pong.length);

            Assert.assertEquals(4_096L, fixture.slot.lease().reservedBytes());
            Assert.assertEquals(4_096L, fixture.budget.stats().reservedBytes());
            sink.finish();
            fixture.slot.markReady(false);
            fixture.drain();
            Assert.assertEquals(0L, fixture.budget.stats().reservedBytes());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void transferredSourceRemainsOwnedUntilTheReplySlotFinishes() {
        Fixture fixture = new Fixture(16 * 1024L);
        AtomicInteger closes = new AtomicInteger();
        try {
            BoundedChunkedReplySink sink = fixture.sink(Unpooled::buffer);

            Assert.assertTrue(sink.transferOwnership(closes::incrementAndGet));
            sink.writeBytes(new byte[]{'O', 'K'}, 0, 2);
            sink.finish();
            fixture.slot.markReady(false);
            fixture.drain();

            Assert.assertEquals(1, closes.get());
        } finally {
            fixture.close();
        }
    }

    @Test
    public void blockedPreflightWakesTheSameSlotWhenAnotherLeaseReleasesCapacity() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(16 * 1024L);
        OutboundConnectionMemory waitingConnection = budget.openConnection(16 * 1024L);
        OutboundConnectionMemory holderConnection = budget.openConnection(16 * 1024L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, waitingConnection, () -> { });
        ReplySlot slot = sequencer.register(waitingConnection.reserve(4_096L, 16 * 1024L).orElseThrow()).orElseThrow();
        OutboundMemoryLease holder = holderConnection.reserve(12 * 1024L, 16 * 1024L).orElseThrow();
        BoundedChunkedReplySink sink = new BoundedChunkedReplySink(
                slot,
                Unpooled::buffer,
                64 * 1024,
                4_096L,
                16 * 1024L
        );
        AtomicInteger wakeups = new AtomicInteger();
        try {
            ReplyPlan plan = ReplyPlan.exact(512L, 0L);
            Assert.assertThrows(ReplyCapacityUnavailableException.class, () -> sink.require(plan));
            Assert.assertTrue(slot.awaitCapacity(wakeups::incrementAndGet));

            holder.close();

            Assert.assertEquals(1, wakeups.get());
            sink.require(plan);
            Assert.assertEquals(5_632L, slot.lease().reservedBytes());
            sink.writeBytes(new byte[512], 0, 512);
            sink.finish();
            slot.markReady(false);
            for (int i = 0; i < 4; i++) {
                channel.runPendingTasks();
                channel.runScheduledPendingTasks();
            }
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            holder.close();
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final OutboundMemoryBudget budget;
        private final EmbeddedChannel channel;
        private final ConnectionReplySequencer sequencer;
        private final ReplySlot slot;
        private final long singleReplyLimitBytes;

        private Fixture(long singleReplyLimitBytes) {
            this.singleReplyLimitBytes = singleReplyLimitBytes;
            budget = new OutboundMemoryBudget(singleReplyLimitBytes);
            OutboundConnectionMemory connection = budget.openConnection(singleReplyLimitBytes);
            channel = new EmbeddedChannel();
            sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
            slot = sequencer.register(connection.reserve(4_096L, singleReplyLimitBytes).orElseThrow()).orElseThrow();
        }

        private BoundedChunkedReplySink sink(BoundedChunkedReplySink.ChunkAllocator allocator) {
            return new BoundedChunkedReplySink(slot, allocator, 64 * 1024, 4_096L, singleReplyLimitBytes);
        }

        private void drain() {
            for (int i = 0; i < 4; i++) {
                channel.runPendingTasks();
                channel.runScheduledPendingTasks();
            }
        }

        private List<Integer> outboundCapacities() {
            List<Integer> capacities = new ArrayList<>();
            ByteBuf chunk;
            while ((chunk = channel.readOutbound()) != null) {
                try {
                    capacities.add(chunk.capacity());
                } finally {
                    chunk.release();
                }
            }
            return capacities;
        }

        private long peakReservedBytes() {
            return budget.stats().peakReservedBytes();
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }
}
