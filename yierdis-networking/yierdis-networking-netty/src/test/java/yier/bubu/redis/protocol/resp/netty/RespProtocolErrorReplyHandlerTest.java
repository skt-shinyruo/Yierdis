package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RespProtocolErrorReplyHandlerTest {
    @Test
    public void closeAfterReplyRunsCallbackAndForwardsTheTerminalError() {
        AtomicInteger downstreamReads = new AtomicInteger();
        AtomicBoolean closeCallbackRan = new AtomicBoolean();
        EmbeddedChannel ch = new EmbeddedChannel(
                new RespProtocolErrorReplyHandler(new RespReplyWriterFactory(), ctx -> closeCallbackRan.set(true)),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        downstreamReads.incrementAndGet();
                    }
                }
        );
        try {
            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error", true)));
            Assert.assertTrue(closeCallbackRan.get());
            Assert.assertEquals(1, downstreamReads.get());
            Assert.assertNull(ch.readOutbound());
            Assert.assertTrue(ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void closeAfterReplyDropsLaterInboundMessagesOnceClosingStarts() {
        AtomicInteger downstreamReads = new AtomicInteger();
        TrackingCloseable laterInbound = new TrackingCloseable();
        EmbeddedChannel ch = new EmbeddedChannel(
                new RespProtocolErrorReplyHandler(
                        new RespReplyWriterFactory(),
                        ctx -> ctx.pipeline().fireChannelRead(laterInbound)
                ),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        downstreamReads.incrementAndGet();
                    }
                }
        );
        try {
            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error", true)));
            Assert.assertTrue(laterInbound.closed);
            Assert.assertEquals(1, downstreamReads.get());
            Assert.assertNull(ch.readOutbound());
            Assert.assertTrue(ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void protocolErrorIsForwardedWithoutAllocatingAReplyBuffer() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespProtocolErrorReplyHandler(
                new RespReplyWriterFactory(),
                ctx -> null,
                ctx -> false,
                ctx -> {}
        ));
        try {
            RespProtocolError error = new RespProtocolError("ERR Protocol error", true);
            Assert.assertTrue(ch.writeInbound(error));
            Assert.assertSame(error, ch.readInbound());
            Assert.assertNull(ch.readOutbound());
            Assert.assertTrue(ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void closingStateSignalDropsProtocolErrorWithoutWritingReply() {
        AtomicBoolean closeObserverCalled = new AtomicBoolean();
        EmbeddedChannel ch = new EmbeddedChannel(new RespProtocolErrorReplyHandler(
                new RespReplyWriterFactory(),
                ctx -> null,
                ctx -> true,
                ctx -> closeObserverCalled.set(true)
        ));
        try {
            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error", true)));
            Assert.assertNull(ch.readOutbound());
            Assert.assertTrue(ch.isOpen());
            Assert.assertFalse(closeObserverCalled.get());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static final class TrackingCloseable implements AutoCloseable {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

}
