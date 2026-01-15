package yier.bubu.redis.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandDecoder;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespWriter;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class YierdisClientTest {
    @Test
    public void pingWorks() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                RespObject resp = client.execute(Arrays.asList(b("PING")), 1000);
                Assert.assertTrue(resp instanceof RespSimpleString);
                Assert.assertEquals("PONG", ((RespSimpleString) resp).value());
            }
        }
    }

    @Test
    public void setGetAreBinarySafeOverTcp() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
                byte[] value = new byte[]{0, 1, (byte) 0xFF, '\n'};

                RespObject set = client.execute(Arrays.asList(b("SET"), key, value), 1000);
                Assert.assertTrue(set instanceof RespSimpleString);

                RespObject get = client.execute(Arrays.asList(b("GET"), key), 1000);
                Assert.assertTrue(get instanceof RespBulkString);
                Assert.assertArrayEquals(value, ((RespBulkString) get).data());
            }
        }
    }

    @Test
    public void unknownCommandReturnsRespError() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                RespObject resp = client.execute(Arrays.asList(b("NOPE")), 1000);
                Assert.assertTrue(resp instanceof RespError);
                Assert.assertTrue(((RespError) resp).message().startsWith("ERR unknown command"));
            }
        }
    }

    @Test
    public void executeRejectsNonPositiveTimeout() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try {
                    client.execute(Arrays.asList(b("PING")), 0);
                    Assert.fail("Expected IllegalArgumentException");
                } catch (IllegalArgumentException e) {
                    Assert.assertTrue(e.getMessage().contains("timeoutMillis"));
                }
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestServer implements AutoCloseable {
        private final EventLoopGroup bossGroup;
        private final EventLoopGroup workerGroup;
        private final Channel serverChannel;
        private final YierdisDb db;

        private TestServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Channel serverChannel, YierdisDb db) {
            this.bossGroup = bossGroup;
            this.workerGroup = workerGroup;
            this.serverChannel = serverChannel;
            this.db = db;
        }

        static TestServer start() throws InterruptedException {
            YierdisDb db = new YierdisDb();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);

            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup(1);
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                    .addLast("respCommandDecoder", new RespCommandDecoder())
                                    .addLast("commandHandler", new FastCommandHandler(processor));
                        }
                    });

            Channel serverChannel = bootstrap.bind(0).sync().channel();
            return new TestServer(bossGroup, workerGroup, serverChannel, db);
        }

        int port() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        @Override
        public void close() {
            try {
                serverChannel.close().syncUninterruptibly();
            } finally {
                db.shutdown();
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }

        private static final class FastCommandHandler extends SimpleChannelInboundHandler<RespCommand> {
            private final YierdisFastCommandProcessor processor;

            private FastCommandHandler(YierdisFastCommandProcessor processor) {
                this.processor = processor;
            }

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, RespCommand msg) {
                ByteBuf out = ctx.alloc().buffer();
                try {
                    processor.execute(msg, new RespWriter(out, ctx.channel()));
                    ctx.writeAndFlush(out);
                    out = null;
                } finally {
                    msg.recycle();
                    if (out != null) {
                        out.release();
                    }
                }
            }
        }
    }
}
