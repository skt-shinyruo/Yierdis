package yier.bubu.redis.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.RespPush;
import yier.bubu.redis.protocol.RespSimpleString;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Resp3PushInterleaveTest {
    @Test
    public void pushFramesDoNotBreakRequestResponsePairing() throws Exception {
        try (PushThenReplyServer server = PushThenReplyServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try (RespFrame reply = client.execute(Arrays.asList(b("PING")), 1000)) {
                    RespObject obj = RespObjectParser.parse(reply);
                    Assert.assertTrue(obj instanceof RespSimpleString);
                    Assert.assertEquals("PONG", ((RespSimpleString) obj).value());
                }

                RespFrame pushFrame = client.pollPush(1000);
                Assert.assertNotNull("expected push frame", pushFrame);
                try (RespFrame push = pushFrame) {
                    RespObject obj = RespObjectParser.parse(push);
                    Assert.assertTrue(obj instanceof RespAttribute);
                    RespObject value = ((RespAttribute) obj).value();
                    Assert.assertTrue(value instanceof RespPush);
                    RespPush p = (RespPush) value;
                    Assert.assertEquals(2, p.values().size());
                    Assert.assertTrue(p.values().get(0) instanceof RespSimpleString);
                    Assert.assertTrue(p.values().get(1) instanceof RespSimpleString);
                    Assert.assertEquals("foo", ((RespSimpleString) p.values().get(0)).value());
                    Assert.assertEquals("bar", ((RespSimpleString) p.values().get(1)).value());
                }

                Assert.assertNull("expected no extra push messages", client.pollPush(50));
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class PushThenReplyServer implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup workers;
        private final Channel serverChannel;

        private PushThenReplyServer(EventLoopGroup boss, EventLoopGroup workers, Channel serverChannel) {
            this.boss = boss;
            this.workers = workers;
            this.serverChannel = serverChannel;
        }

        static PushThenReplyServer start() throws Exception {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup workers = new NioEventLoopGroup(1);
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                private boolean sent;

                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    try {
                                        if (sent) {
                                            return;
                                        }
                                        sent = true;

                                        byte[] push = ascii("|1\r\n+kind\r\n+notify\r\n>2\r\n+foo\r\n+bar\r\n");
                                        byte[] reply = ascii("+PONG\r\n");
                                        ctx.writeAndFlush(Unpooled.wrappedBuffer(concat(push, reply)));
                                    } finally {
                                        ReferenceCountUtil.release(msg);
                                    }
                                }
                            });
                        }
                    });

            Channel ch = b.bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            return new PushThenReplyServer(boss, workers, ch);
        }

        int port() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        @Override
        public void close() {
            try {
                serverChannel.close().syncUninterruptibly();
            } finally {
                boss.shutdownGracefully();
                workers.shutdownGracefully();
            }
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}

