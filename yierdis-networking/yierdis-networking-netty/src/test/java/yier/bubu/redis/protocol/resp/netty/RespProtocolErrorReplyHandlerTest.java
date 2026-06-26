package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ProtocolNegotiationSession;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RespProtocolErrorReplyHandlerTest {
    @Test
    public void closeAfterReplyRunsCallbackWritesErrorAndClosesChannel() {
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
            Assert.assertEquals(0, downstreamReads.get());
            Assert.assertArrayEquals(ascii("-ERR Protocol error\r\n"), readOutbound(ch));
            Assert.assertFalse(ch.isOpen());
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
            Assert.assertEquals(0, downstreamReads.get());
            Assert.assertArrayEquals(ascii("-ERR Protocol error\r\n"), readOutbound(ch));
            Assert.assertFalse(ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void protocolErrorUsesInjectedSessionReplyWriterWhenAvailable() {
        TestProtocolSession session = new TestProtocolSession();
        session.setRespVersion(3);
        EmbeddedChannel ch = new EmbeddedChannel(new RespProtocolErrorReplyHandler(
                new RespReplyWriterFactory(),
                ctx -> session,
                ctx -> false,
                ctx -> {}
        ));
        try {
            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error", true)));
            Assert.assertArrayEquals(ascii("-ERR Protocol error\r\n"), readOutbound(ch));
            Assert.assertFalse(ch.isOpen());
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
            Assert.assertNull(readOutbound(ch));
            Assert.assertTrue(ch.isOpen());
            Assert.assertFalse(closeObserverCalled.get());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        ByteBuf out = ch.readOutbound();
        if (out == null) {
            return null;
        }
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class TrackingCloseable implements AutoCloseable {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TestProtocolSession implements ProtocolNegotiationSession {
        private int respVersion = 2;

        @Override
        public int respVersion() {
            return respVersion;
        }

        @Override
        public void setRespVersion(int respVersion) {
            this.respVersion = respVersion;
        }
    }
}
