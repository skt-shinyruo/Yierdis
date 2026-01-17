package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.YierdisServerBootstrap;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespObjectParser;

import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class YierdisClientTest {
    @Test
    public void pingWorks() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try (RespFrame frame = client.execute(Arrays.asList(b("PING")), 1000)) {
                    RespObject resp = RespObjectParser.parse(frame);
                    Assert.assertTrue(resp instanceof RespSimpleString);
                    Assert.assertEquals("PONG", ((RespSimpleString) resp).value());
                }
            }
        }
    }

    @Test
    public void hello3ReturnsResp3MapAndNullIsDecoded() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                RespObject helloObj;
                try (RespFrame frame = client.execute(Arrays.asList(b("HELLO"), b("3")), 1000)) {
                    helloObj = RespObjectParser.parse(frame);
                }
                Assert.assertTrue(helloObj instanceof RespMap);
                RespMap map = (RespMap) helloObj;
                Assert.assertEquals(5, map.entries().size());

                java.util.HashMap<String, String> kv = new java.util.HashMap<>();
                for (RespMap.Entry e : map.entries()) {
                    kv.put(bulkUtf8(e.key()), bulkUtf8(e.value()));
                }
                Assert.assertEquals("yierdis", kv.get("server"));
                Assert.assertEquals("3", kv.get("proto"));
                Assert.assertEquals("standalone", kv.get("mode"));
                Assert.assertEquals("master", kv.get("role"));
                Assert.assertNotNull(kv.get("version"));

                try (RespFrame frame = client.execute(Arrays.asList(b("GET"), b("missing")), 1000)) {
                    RespObject missing = RespObjectParser.parse(frame);
                    Assert.assertTrue(missing instanceof RespNull);
                }
            }
        }
    }

    @Test
    public void setGetAreBinarySafeOverTcp() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
                byte[] value = new byte[]{0, 1, (byte) 0xFF, '\n'};

                try (RespFrame frame = client.execute(Arrays.asList(b("SET"), key, value), 1000)) {
                    RespObject set = RespObjectParser.parse(frame);
                    Assert.assertTrue(set instanceof RespSimpleString);
                }

                try (RespFrame frame = client.execute(Arrays.asList(b("GET"), key), 1000)) {
                    RespObject get = RespObjectParser.parse(frame);
                    Assert.assertTrue(get instanceof RespBulkString);
                    Assert.assertArrayEquals(value, ((RespBulkString) get).data());
                }
            }
        }
    }

    @Test
    public void unknownCommandReturnsRespError() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try (RespFrame frame = client.execute(Arrays.asList(b("NOPE")), 1000)) {
                    RespObject resp = RespObjectParser.parse(frame);
                    Assert.assertTrue(resp instanceof RespError);
                    Assert.assertTrue(((RespError) resp).message().startsWith("ERR unknown command"));
                }
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

    @Test
    public void timeoutClosesConnectionToPreventResponseDesync() throws Exception {
        try (BlackholeServer server = BlackholeServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try {
                    client.execute(Arrays.asList(b("PING")), 100);
                    Assert.fail("Expected IllegalStateException");
                } catch (IllegalStateException e) {
                    Assert.assertTrue(e.getMessage().contains("Timeout waiting for response"));
                }

                try {
                    client.execute(Arrays.asList(b("PING")), 1000);
                    Assert.fail("Expected IllegalStateException");
                } catch (IllegalStateException e) {
                    Assert.assertTrue(e.getMessage().toLowerCase().contains("closed"));
                }
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String bulkUtf8(RespObject obj) {
        Assert.assertTrue(obj instanceof RespBulkString);
        RespBulkString bulk = (RespBulkString) obj;
        Assert.assertFalse(bulk.isNull());
        return bulk.asString();
    }

    private static final class TestServer implements AutoCloseable {
        private final YierdisServerBootstrap server;

        private TestServer(YierdisServerBootstrap server) {
            this.server = server;
        }

        static TestServer start() throws Exception {
            // Bind port=0 for ephemeral port (avoids conflicts on CI/dev machines).
            YierdisServerBootstrap server = YierdisServerBootstrap.start(
                    "--port", "0",
                    "--ioThreads", "1",
                    "--noCleanup"
            );
            return new TestServer(server);
        }

        int port() {
            return server.port();
        }

        @Override
        public void close() {
            server.close();
        }
    }

    /**
     * A tiny TCP server that accepts connections but never replies.
     * <p>
     * Used to deterministically trigger client-side timeouts without modifying the real server implementation.
     */
    private static final class BlackholeServer implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup workers;
        private final Channel serverChannel;

        private BlackholeServer(EventLoopGroup boss, EventLoopGroup workers, Channel serverChannel) {
            this.boss = boss;
            this.workers = workers;
            this.serverChannel = serverChannel;
        }

        static BlackholeServer start() throws Exception {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup workers = new NioEventLoopGroup(1);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    // Consume and discard to keep the connection open.
                                    ReferenceCountUtil.release(msg);
                                }
                            });
                        }
                    });

            Channel ch = bootstrap.bind(0).sync().channel();
            return new BlackholeServer(boss, workers, ch);
        }

        int port() {
            if (serverChannel.localAddress() instanceof InetSocketAddress addr) {
                return addr.getPort();
            }
            throw new IllegalStateException("No local address");
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
}
