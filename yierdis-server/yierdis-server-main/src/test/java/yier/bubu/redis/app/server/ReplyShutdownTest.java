package yier.bubu.redis.app.server;

import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;

public class ReplyShutdownTest {
    @Test
    public void idleRegisteredChildDrainsThroughSequencerBeforeItsOutboundAccountCloses() {
        ChildChannelRegistry registry = new ChildChannelRegistry();
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory connectionMemory = budget.openConnection(4_096L);
        EmbeddedChannel child = new EmbeddedChannel();
        Assert.assertTrue(registry.register(child));
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(child, connectionMemory, () -> { });
        NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(child, 16, 1_024L);
        connection.bindReplyGate(new NettyReplyDecodedMessageGate(
                4_096L,
                4_096L,
                connectionMemory,
                sequencer
        ));
        try {
            registry.beginShutdown();
            connection.markClosing();
            CompletableFuture<Void> drained = connection.shutdownReplyGracefully();
            drain(child);

            Assert.assertTrue(drained.isDone());
            Assert.assertTrue(registry.drainedFuture().isDone());
            Assert.assertFalse(child.isOpen());
            Assert.assertEquals(0, registry.activeChannelCount());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            Assert.assertEquals(0L, budget.stats().allocatedBytes());
            Assert.assertEquals(0L, budget.stats().activeSlots());
            Assert.assertEquals(0, budget.stats().activeConnections());
        } finally {
            child.finishAndReleaseAll();
            sequencer.close();
        }
    }

    private static void drain(EmbeddedChannel channel) {
        for (int index = 0; index < 4; index++) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
        }
    }
}
