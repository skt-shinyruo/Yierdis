package yier.bubu.redis.app.client;

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
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class YierdisClientTest {
    @Test
    public void pingReturnsSimpleStringPongOverResp() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            YierdisClient.RespReply reply = client.executeUtf8(List.of("PING"), 1000);
            Assert.assertEquals(YierdisClient.RespReply.Kind.SIMPLE_STRING, reply.kind());
            Assert.assertEquals("PONG", reply.text());
        }
    }

    @Test
    public void helloReturnsMapAndNullIsDecoded() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Map<String, YierdisClient.RespReply> hello = replyMap(client.execute(Arrays.asList(b("HELLO")), 1000));
            Assert.assertEquals("yierdis", stringField(hello, "server"));
            Assert.assertNotNull(stringField(hello, "version"));
            Assert.assertEquals(2L, longField(hello, "proto"));
            Assert.assertEquals("standalone", stringField(hello, "mode"));
            Assert.assertEquals("master", stringField(hello, "role"));

            YierdisClient.RespReply missing = client.execute(Arrays.asList(b("GET"), b("missing")), 1000);
            Assert.assertTrue(missing.isNull());
        }
    }

    @Test
    public void infoAndStatsCommandsExposeServerObservabilityOverTcp() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            String infoText = stringResult(client.execute(Arrays.asList(b("INFO")), 1000));
            Assert.assertTrue(infoText.contains("# Server\r\n"));
            Assert.assertTrue(infoText.contains("redis_version:"));
            Assert.assertTrue(infoText.contains("# Stats\r\n"));
            Assert.assertTrue(infoText.contains("yierdis_queued_tasks:"));

            Map<String, YierdisClient.RespReply> stats = replyMap(client.execute(Arrays.asList(b("STATS")), 1000));
            Assert.assertTrue(longField(stats, "queued_tasks") >= 0);
            Assert.assertTrue(longField(stats, "commands_executed_total") >= 0);
            Assert.assertTrue(longField(stats, "conn_commands_enqueued") >= 0);
            Assert.assertTrue(longField(stats, "conn_commands_executed") >= 0);
        }
    }

    @Test
    public void setGetWorkOverTcpUsingUtf8Strings() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("OK", stringResult(client.execute(Arrays.asList(b("SET"), b("k"), b("v")), 1000)));
            Assert.assertEquals("v", stringResult(client.execute(Arrays.asList(b("GET"), b("k")), 1000)));
        }
    }

    @Test
    public void rawByteExecutePreservesBinaryArgsOverResp() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            YierdisClient.RespReply reply = client.execute(Arrays.asList(b("ECHO"), new byte[]{(byte) 0xFF}), 1000);

            Assert.assertArrayEquals(new byte[]{(byte) 0xFF}, bulkBytes(reply));
        }
    }

    @Test
    public void unknownCommandReturnsCommandErrorReply() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            YierdisClient.RespReply reply = client.execute(Arrays.asList(b("NOPE")), 1000);

            Assert.assertEquals(YierdisClient.RespReply.Kind.ERROR, reply.kind());
            Assert.assertTrue(reply.text().startsWith("ERR unknown command"));
        }
    }

    @Test
    public void memoryStatsHasStableKeySet() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Map<String, YierdisClient.RespReply> stats = replyMap(client.execute(Arrays.asList(b("MEMORY"), b("STATS")), 1000));

            HashSet<String> keys = new HashSet<>(stats.keySet());
            Assert.assertTrue(keys.contains("maxmemory_bytes"));
            Assert.assertTrue(keys.contains("used_bytes_for_maxmemory"));
            Assert.assertTrue(keys.contains("effective_used_bytes_for_maxmemory"));
            Assert.assertTrue(keys.contains("ledger_used_bytes"));
            Assert.assertTrue(keys.contains("ledger_reserved_bytes"));
            Assert.assertTrue(keys.contains("offheap_used_bytes"));
            Assert.assertTrue(keys.contains("offheap_included_in_maxmemory"));
            Assert.assertTrue(keys.contains("total_estimated_bytes"));
            Assert.assertTrue(keys.contains("keyspace_rehashing"));
            Assert.assertTrue(keys.contains("keyspace_table0_capacity"));
            Assert.assertTrue(keys.contains("expire_rehashing"));
            Assert.assertTrue(keys.contains("key_count"));
            Assert.assertTrue(keys.contains("expire_count"));
        }
    }

    @Test
    public void executeRejectsNonPositiveTimeout() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            try {
                client.execute(Arrays.asList(b("PING")), 0);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("timeoutMillis"));
            }
        }
    }

    @Test
    public void failedConnectDoesNotLeakEventLoopThreads() throws Exception {
        Set<String> before = threadNames();

        try {
            YierdisClient.connect("127.0.0.1", unusedPort());
            Assert.fail("Expected connection failure");
        } catch (Exception expected) {
            assertConnectException(expected);
        }

        waitForNoExtraEventLoopThreads(before, 3000);
    }

    @Test
    public void timeoutClosesConnectionToPreventResponseDesync() throws Exception {
        try (BlackholeServer server = BlackholeServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
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

    @Test
    public void serverCloseWakesExecuteWithoutTimeout() throws Exception {
        try (CloseOnReadServer server = CloseOnReadServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            try {
                client.execute(Arrays.asList(b("PING")), 5000);
                Assert.fail("Expected IllegalStateException");
            } catch (IllegalStateException e) {
                String msg = String.valueOf(e.getMessage());
                Assert.assertFalse(msg.contains("Timeout waiting for response"));
                Assert.assertTrue(msg.toLowerCase().contains("closed")
                        || (e.getCause() != null && String.valueOf(e.getCause().getMessage()).toLowerCase().contains("closed")));
            }
        }
    }

    @Test
    public void invalidRespReplyClosesConnection() throws Exception {
        try (FloodingServer server = FloodingServer.start(1);
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Assert.assertTrue(server.awaitFlood(1_000));

            try {
                client.execute(Arrays.asList(b("PING")), 1000);
                Assert.fail("Expected IllegalStateException");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("Invalid RESP reply"));
            }

            try {
                client.execute(Arrays.asList(b("PING")), 1000);
                Assert.fail("Expected IllegalStateException");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().toLowerCase().contains("closed"));
            }
        }
    }

    @Test
    public void respReplyBulkBytesAccessorReturnsDefensiveCopy() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            YierdisClient.RespReply reply = client.execute(Arrays.asList(b("ECHO"), b("original")), 1000);

            byte[] bytes = reply.bytes();
            bytes[0] = 'x';

            Assert.assertEquals("original", stringResult(reply));
        }
    }

    @Test
    public void respReplyPublicConstructorDefensivelyCopiesBytesAndValues() {
        byte[] bytes = b("scalar");
        YierdisClient.RespReply bulk = new YierdisClient.RespReply(
                YierdisClient.RespReply.Kind.BULK_STRING, null, bytes, null, null
        );
        bytes[0] = 'x';
        Assert.assertEquals("scalar", stringResult(bulk));

        ArrayList<YierdisClient.RespReply> values = new ArrayList<>();
        values.add(new YierdisClient.RespReply(YierdisClient.RespReply.Kind.INTEGER, null, null, 1L, null));
        YierdisClient.RespReply array = new YierdisClient.RespReply(
                YierdisClient.RespReply.Kind.ARRAY, null, null, null, values
        );
        values.clear();
        Assert.assertEquals(1, array.values().size());
    }

    private static Map<String, YierdisClient.RespReply> replyMap(YierdisClient.RespReply reply) {
        Assert.assertEquals(YierdisClient.RespReply.Kind.ARRAY, reply.kind());
        List<YierdisClient.RespReply> values = reply.values();
        Assert.assertNotNull(values);
        Assert.assertEquals("expected even RESP2 map array length", 0, values.size() % 2);
        Map<String, YierdisClient.RespReply> map = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i += 2) {
            map.put(stringResult(values.get(i)), values.get(i + 1));
        }
        return map;
    }

    private static String stringField(Map<String, YierdisClient.RespReply> map, String key) {
        return stringResult(map.get(key));
    }

    private static long longField(Map<String, YierdisClient.RespReply> map, String key) {
        YierdisClient.RespReply value = map.get(key);
        Assert.assertNotNull("expected integer field: " + key, value);
        Assert.assertEquals("expected integer field: " + key, YierdisClient.RespReply.Kind.INTEGER, value.kind());
        return value.integer();
    }

    private static String stringResult(YierdisClient.RespReply reply) {
        Assert.assertNotNull(reply);
        if (reply.kind() == YierdisClient.RespReply.Kind.SIMPLE_STRING) {
            return reply.text();
        }
        return new String(bulkBytes(reply), StandardCharsets.UTF_8);
    }

    private static byte[] bulkBytes(YierdisClient.RespReply reply) {
        Assert.assertNotNull(reply);
        Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, reply.kind());
        return reply.bytes();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Set<String> threadNames() {
        Set<String> names = new LinkedHashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread != null) {
                names.add(thread.getName());
            }
        }
        return names;
    }

    private static int unusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForNoExtraEventLoopThreads(Set<String> before, long timeoutMillis) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            Set<String> leaked = extraEventLoopThreads(before);
            if (leaked.isEmpty()) {
                return;
            }
            if (System.nanoTime() >= deadlineNanos) {
                Assert.fail("Expected no leaked nioEventLoopGroup threads, but found " + leaked);
            }
            Thread.sleep(25);
        }
    }

    private static Set<String> extraEventLoopThreads(Set<String> before) {
        Set<String> leaked = new LinkedHashSet<>();
        for (String name : threadNames()) {
            if (!before.contains(name) && name.contains("nioEventLoopGroup")) {
                leaked.add(name);
            }
        }
        return leaked;
    }

    private static void assertConnectException(Exception expected) {
        Throwable cursor = expected;
        while (cursor != null) {
            if (cursor instanceof ConnectException) {
                return;
            }
            cursor = cursor.getCause();
        }
        throw new AssertionError("Expected ConnectException but got " + expected, expected);
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

    /**
     * A TCP server that closes immediately after receiving any bytes.
     * <p>
     * Used to verify the client unblocks on close (without waiting for timeout).
     */
    private static final class CloseOnReadServer implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup workers;
        private final Channel serverChannel;

        private CloseOnReadServer(EventLoopGroup boss, EventLoopGroup workers, Channel serverChannel) {
            this.boss = boss;
            this.workers = workers;
            this.serverChannel = serverChannel;
        }

        static CloseOnReadServer start() throws Exception {
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
                                    ReferenceCountUtil.release(msg);
                                    ctx.close();
                                }
                            });
                        }
                    });

            Channel ch = bootstrap.bind(0).sync().channel();
            return new CloseOnReadServer(boss, workers, ch);
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

    /**
     * A server that sends invalid RESP on connect, used to verify malformed replies close the client.
     */
    private static final class FloodingServer implements AutoCloseable {
        private final EventLoopGroup boss;
        private final EventLoopGroup workers;
        private final Channel serverChannel;
        private final CountDownLatch flooded;

        private FloodingServer(EventLoopGroup boss, EventLoopGroup workers, Channel serverChannel, CountDownLatch flooded) {
            this.boss = boss;
            this.workers = workers;
            this.serverChannel = serverChannel;
            this.flooded = flooded;
        }

        static FloodingServer start(int lines) throws Exception {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup workers = new NioEventLoopGroup(1);
            CountDownLatch flooded = new CountDownLatch(1);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    byte[] invalid = "{not-resp}\n".getBytes(StandardCharsets.US_ASCII);
                                    for (int i = 0; i < lines; i++) {
                                        ctx.write(Unpooled.copiedBuffer(invalid));
                                    }
                                    ctx.flush();
                                    flooded.countDown();
                                }
                            });
                        }
                    });

            Channel ch = bootstrap.bind(0).sync().channel();
            return new FloodingServer(boss, workers, ch, flooded);
        }

        boolean awaitFlood(long timeoutMillis) throws InterruptedException {
            return flooded.await(timeoutMillis, TimeUnit.MILLISECONDS);
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
