package yier.bubu.redis.app.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderedReplyPipelineTest {
    @Test
    public void decoderWaitsAtReplyAdmissionAndPreservesTheOriginalMessageUntilCapacityReturns() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(4_096L);
        OutboundConnectionMemory outboundConnection = budget.openConnection(4_096L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, outboundConnection, () -> { });
        NettyReplyDecodedMessageGate gate = new NettyReplyDecodedMessageGate(4_096L, 4_096L, outboundConnection, sequencer);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                1_024,
                16,
                1_024,
                1_024,
                null,
                null,
                gate
        );
        CapturingHandler capture = new CapturingHandler();
        channel.pipeline().addLast("decoder", decoder);
        channel.pipeline().addLast("capture", capture);
        try {
            Assert.assertFalse(channel.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n",
                    StandardCharsets.US_ASCII
            )));

            Assert.assertEquals(1, capture.messages.size());
            RegisteredRespMessage first = capture.messages.getFirst();
            Assert.assertTrue(first.message() instanceof ExecutionRequest);
            Assert.assertEquals(0L, first.slot().sequence());
            first.slot().cancel();
            channel.runPendingTasks();

            Assert.assertEquals(2, capture.messages.size());
            RegisteredRespMessage second = capture.messages.get(1);
            Assert.assertEquals(1L, second.slot().sequence());
            second.close();
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            for (RegisteredRespMessage message : capture.messages) {
                message.close();
            }
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void terminalProtocolErrorGetsTheNextRegisteredSlot() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(8_192L);
        OutboundConnectionMemory outboundConnection = budget.openConnection(8_192L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, outboundConnection, () -> { });
        NettyReplyDecodedMessageGate gate = new NettyReplyDecodedMessageGate(4_096L, 4_096L, outboundConnection, sequencer);
        RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                4,
                16,
                1_024,
                1_024,
                null,
                null,
                gate
        );
        CapturingHandler capture = new CapturingHandler();
        channel.pipeline().addLast("decoder", decoder);
        channel.pipeline().addLast("capture", capture);
        try {
            channel.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n*1\r\n$5\r\nabcde\r\n",
                    StandardCharsets.US_ASCII
            ));

            Assert.assertEquals(2, capture.messages.size());
            Assert.assertEquals(0L, capture.messages.get(0).slot().sequence());
            Assert.assertEquals(1L, capture.messages.get(1).slot().sequence());
            Assert.assertTrue(capture.messages.get(1).message() instanceof yier.bubu.redis.protocol.resp.netty.RespProtocolError);
        } finally {
            for (RegisteredRespMessage message : capture.messages) {
                message.close();
            }
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void mixedReplyProducersCompleteOutOfOrderButWriteInReceiveOrder() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(32_768L);
        OutboundConnectionMemory connection = budget.openConnection(32_768L);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(channel, connection, () -> { });
        ReplySlot command = register(sequencer, connection);
        ReplySlot busy = register(sequencer, connection);
        ReplySlot protocol = register(sequencer, connection);
        ReplySlot internal = register(sequencer, connection);
        ReplySlot closeAfterReply = register(sequencer, connection);
        try {
            ready(internal, "-ERR internal error\r\n", false);
            ready(busy, "-ERR busy queue_full\r\n", false);
            ready(closeAfterReply, "+OK\r\n", true);
            ready(protocol, "-ERR Protocol error\r\n", false);
            Assert.assertNull(channel.readOutbound());

            ready(command, "+PONG\r\n", false);
            drain(channel);

            Assert.assertEquals("+PONG\r\n", readAscii(channel));
            Assert.assertEquals("-ERR busy queue_full\r\n", readAscii(channel));
            Assert.assertEquals("-ERR Protocol error\r\n", readAscii(channel));
            Assert.assertEquals("-ERR internal error\r\n", readAscii(channel));
            Assert.assertEquals("+OK\r\n", readAscii(channel));
            Assert.assertFalse(channel.isOpen());
            Assert.assertEquals(0L, budget.stats().reservedBytes());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    @Test
    public void failedWriteCancelsTheFailedAndLaterSlotsWithoutAppendingAReplacementReply() {
        AtomicInteger writes = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (writes.incrementAndGet() == 2) {
                    ReferenceCountUtil.release(msg);
                    promise.setFailure(new IllegalStateException("injected write failure"));
                    return;
                }
                ctx.write(msg, promise);
            }
        });
        OutboundMemoryBudget budget = new OutboundMemoryBudget(32_768L);
        OutboundConnectionMemory connection = budget.openConnection(32_768L);
        ReplyEgressStats egressStats = new ReplyEgressStats();
        ConnectionReplySequencer sequencer = new ConnectionReplySequencer(
                channel,
                connection,
                () -> { },
                slot -> {
                    throw new IllegalStateException("test does not create a reply sink");
                },
                egressStats
        );
        List<ReplySlot> slots = List.of(
                register(sequencer, connection),
                register(sequencer, connection),
                register(sequencer, connection),
                register(sequencer, connection),
                register(sequencer, connection)
        );
        List<AtomicInteger> closes = new ArrayList<>();
        try {
            for (int index = 0; index < slots.size(); index++) {
                AtomicInteger closeCount = new AtomicInteger();
                closes.add(closeCount);
                slots.get(index).addOwnedResource(closeCount::incrementAndGet);
                slots.get(index).addChunk(Unpooled.copiedBuffer("+" + index + "\r\n", StandardCharsets.US_ASCII));
            }
            for (int index = slots.size() - 1; index >= 0; index--) {
                slots.get(index).markReady(false);
            }
            drain(channel);

            Assert.assertEquals("+0\r\n", readAscii(channel));
            Assert.assertNull(channel.readOutbound());
            Assert.assertFalse(channel.isOpen());
            for (int index = 0; index < slots.size(); index++) {
                Assert.assertEquals("slot resource should close once: " + index, 1, closes.get(index).get());
            }
            Assert.assertEquals(ReplySlotState.COMPLETED, slots.getFirst().state());
            for (int index = 1; index < slots.size(); index++) {
                Assert.assertTrue(
                        "failed or later slot should terminate: " + index,
                        slots.get(index).state() == ReplySlotState.FAILED || slots.get(index).state() == ReplySlotState.CANCELLED
                );
            }
            Assert.assertEquals(0L, budget.stats().reservedBytes());
            ReplyEgressStats.Snapshot stats = egressStats.snapshot();
            Assert.assertEquals(1L, stats.writeFailures());
            Assert.assertEquals(1L, stats.failedSlots());
            Assert.assertEquals(3L, stats.cancelledSlots());
            Assert.assertEquals(0L, stats.activeChunks());
            Assert.assertEquals(0L, stats.activeSources());
        } finally {
            channel.finishAndReleaseAll();
            sequencer.close();
        }
    }

    private static ReplySlot register(ConnectionReplySequencer sequencer, OutboundConnectionMemory connection) {
        return sequencer.register(connection.reserve(4_096L, 4_096L).orElseThrow()).orElseThrow();
    }

    private static void ready(ReplySlot slot, String reply, boolean closeAfterReply) {
        slot.addChunk(Unpooled.copiedBuffer(reply, StandardCharsets.US_ASCII));
        slot.markReady(closeAfterReply);
    }

    private static String readAscii(EmbeddedChannel channel) {
        io.netty.buffer.ByteBuf reply = channel.readOutbound();
        Assert.assertNotNull("expected outbound reply", reply);
        try {
            return reply.toString(StandardCharsets.US_ASCII);
        } finally {
            reply.release();
        }
    }

    private static void drain(EmbeddedChannel channel) {
        for (int index = 0; index < 4; index++) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
        }
    }

    private static final class CapturingHandler extends ChannelInboundHandlerAdapter {
        private final List<RegisteredRespMessage> messages = new ArrayList<>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Assert.assertTrue(msg instanceof RegisteredRespMessage);
            messages.add((RegisteredRespMessage) msg);
        }
    }
}
