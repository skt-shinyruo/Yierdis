package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;

import java.lang.reflect.Proxy;
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
        RespProtocolErrorReplyHandler handler = new RespProtocolErrorReplyHandler(new RespReplyWriterFactory());
        TrackingChannelHandlerContext ctx = new TrackingChannelHandlerContext(downstreamReads);
        try {
            handler.channelRead(ctx.proxy(), new RespProtocolError("ERR Protocol error", true));
            handler.channelRead(ctx.proxy(), laterInbound);

            Assert.assertFalse(ctx.channel().config().isAutoRead());
            Assert.assertTrue(laterInbound.closed);
            Assert.assertEquals(0, downstreamReads.get());
            Assert.assertArrayEquals(ascii("-ERR Protocol error\r\n"), ctx.writtenBytes);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            ctx.channel().finishAndReleaseAll();
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

    private static final class TrackingChannelHandlerContext {
        private final EmbeddedChannel channel = new EmbeddedChannel();
        private final AtomicInteger downstreamReads;
        private final ChannelHandlerContext proxy;
        private byte[] writtenBytes;

        private TrackingChannelHandlerContext(AtomicInteger downstreamReads) {
            this.downstreamReads = downstreamReads;
            this.proxy = (ChannelHandlerContext) Proxy.newProxyInstance(
                    ChannelHandlerContext.class.getClassLoader(),
                    new Class<?>[]{ChannelHandlerContext.class},
                    (obj, method, args) -> switch (method.getName()) {
                        case "channel" -> channel;
                        case "alloc" -> ByteBufAllocator.DEFAULT;
                        case "executor" -> ImmediateEventExecutor.INSTANCE;
                        case "name" -> "tracking";
                        case "handler" -> null;
                        case "isRemoved" -> false;
                        case "fireChannelRead" -> {
                            downstreamReads.incrementAndGet();
                            yield obj;
                        }
                        case "writeAndFlush", "write" -> writeAndFlush(args[0]);
                        case "newPromise" -> new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
                        case "voidPromise" -> channel.voidPromise();
                        case "pipeline" -> channel.pipeline();
                        case "flush", "read", "fireChannelRegistered", "fireChannelUnregistered", "fireChannelActive",
                             "fireChannelInactive", "fireExceptionCaught", "fireUserEventTriggered",
                             "fireChannelReadComplete", "fireChannelWritabilityChanged" -> obj;
                        case "attr" -> channel.attr((io.netty.util.AttributeKey<Object>) args[0]);
                        case "hasAttr" -> channel.hasAttr((io.netty.util.AttributeKey<Object>) args[0]);
                        case "toString" -> "TrackingChannelHandlerContext";
                        case "hashCode" -> System.identityHashCode(obj);
                        case "equals" -> obj == args[0];
                        default -> throw new UnsupportedOperationException(method.toString());
                    }
            );
        }

        private ChannelHandlerContext proxy() {
            return proxy;
        }

        private EmbeddedChannel channel() {
            return channel;
        }

        private ChannelFuture writeAndFlush(Object msg) {
            if (msg instanceof ByteBuf out) {
                try {
                    writtenBytes = new byte[out.readableBytes()];
                    out.getBytes(out.readerIndex(), writtenBytes);
                } finally {
                    out.release();
                }
            }
            return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        }
    }
}
