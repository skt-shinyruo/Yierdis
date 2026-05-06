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
import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonLong;
import yier.bubu.redis.protocol.json.JsonNull;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class YierdisClientTest {
    @Test
    public void pingWorks() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("PING")), 1000);
                Assert.assertTrue(ok(reply.envelope()));
                Assert.assertEquals("PONG", stringResult(reply.envelope()));
            }
        }
    }

    @Test
    public void helloReturnsObjectAndNullIsDecoded() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("HELLO"), b("3")), 1000);
                Assert.assertTrue(ok(reply.envelope()));
                JsonObject result = objectResult(reply.envelope());
                Assert.assertEquals("yierdis", stringField(result, "server"));
                Assert.assertNotNull(stringField(result, "version"));
                Assert.assertEquals(1L, longField(result, "proto"));
                Assert.assertEquals("standalone", stringField(result, "mode"));
                Assert.assertEquals("master", stringField(result, "role"));

                YierdisClient.JsonReply missing = client.execute(Arrays.asList(b("GET"), b("missing")), 1000);
                Assert.assertTrue(ok(missing.envelope()));
                JsonValue v = resultValue(missing.envelope());
                Assert.assertTrue(v == null || v instanceof JsonNull);
            }
        }
    }

    @Test
    public void infoAndStatsCommandsExposeServerObservabilityOverTcp() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply info = client.execute(Arrays.asList(b("INFO")), 1000);
                Assert.assertTrue(ok(info.envelope()));
                String infoText = stringResult(info.envelope());
                Assert.assertTrue(infoText.contains("# Server\r\n"));
                Assert.assertTrue(infoText.contains("redis_version:"));
                Assert.assertTrue(infoText.contains("# Stats\r\n"));
                Assert.assertTrue(infoText.contains("yierdis_queued_tasks:"));

                YierdisClient.JsonReply stats = client.execute(Arrays.asList(b("STATS")), 1000);
                Assert.assertTrue(ok(stats.envelope()));
                JsonObject statsObject = objectResult(stats.envelope());
                Assert.assertTrue(longField(statsObject, "queued_tasks") >= 0);
                Assert.assertTrue(longField(statsObject, "commands_executed_total") >= 0);
                Assert.assertTrue(longField(statsObject, "conn_commands_enqueued") >= 0);
                Assert.assertTrue(longField(statsObject, "conn_commands_executed") >= 0);
            }
        }
    }

    @Test
    public void setGetWorkOverTcpUsingUtf8Strings() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply set = client.execute(Arrays.asList(b("SET"), b("k"), b("v")), 1000);
                Assert.assertTrue(ok(set.envelope()));
                Assert.assertEquals("OK", stringResult(set.envelope()));

                YierdisClient.JsonReply get = client.execute(Arrays.asList(b("GET"), b("k")), 1000);
                Assert.assertTrue(ok(get.envelope()));
                Assert.assertEquals("v", stringResult(get.envelope()));
            }
        }
    }

    @Test
    public void rawByteExecuteNormalizesMalformedUtf8ArgsBeforeSending() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("ECHO"), new byte[]{(byte) 0xFF}), 1000);

                Assert.assertTrue(ok(reply.envelope()));
                Assert.assertEquals("\uFFFD", stringResult(reply.envelope()));
            }
        }
    }

    @Test
    public void unknownCommandReturnsCommandErrorEnvelope() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("NOPE")), 1000);
                Assert.assertFalse(ok(reply.envelope()));
                JsonObject err = errorObject(reply.envelope());
                Assert.assertEquals("command", stringField(err, "kind"));
                Assert.assertTrue(stringField(err, "message").startsWith("ERR unknown command"));
            }
        }
    }

    @Test
    public void memoryStatsHasStableKeySet() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("MEMORY"), b("STATS")), 1000);
                Assert.assertTrue(ok(reply.envelope()));
                JsonObject stats = objectResult(reply.envelope());

                HashSet<String> keys = new HashSet<>(stats.values().keySet());
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

    @Test
    public void serverCloseWakesExecuteWithoutTimeout() throws Exception {
        try (CloseOnReadServer server = CloseOnReadServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
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
    }

    @Test
    public void responseQueueOverflowClosesConnection() throws Exception {
        try (FloodingServer server = FloodingServer.start(256)) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                Assert.assertTrue(server.awaitFlood(1_000));
                // Give the client a moment to decode/enqueue a few lines.
                Thread.sleep(50);

                try {
                    client.execute(Arrays.asList(b("PING")), 1000);
                    Assert.fail("Expected IllegalStateException");
                } catch (IllegalStateException e) {
                    String m1 = String.valueOf(e.getMessage()).toLowerCase();
                    String m2 = e.getCause() == null ? "" : String.valueOf(e.getCause().getMessage()).toLowerCase();
                    Assert.assertTrue(m1.contains("overflow") || m2.contains("overflow"));
                }
            }
        }
    }

    @Test
    public void jsonReplyLineAccessorReturnsDefensiveCopy() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                YierdisClient.JsonReply reply = client.execute(Arrays.asList(b("PING")), 1000);
                String original = reply.lineUtf8();

                byte[] line = reply.line();
                line[0] = 'x';

                Assert.assertEquals(original, reply.lineUtf8());
            }
        }
    }

    @Test
    public void decodeResultMapStringKeysReturnsLivePlainObjectValuesForClientCompatibility() {
        byte[] line = "{\"ok\":true,\"result\":{\"outer\":{\"x\":1}}}".getBytes(StandardCharsets.UTF_8);
        JsonValue envelope = JsonParser.parseStrictUtf8(line, 0, line.length, JsonLimits.DEFAULT);

        Map<String, JsonValue> result = CustomProtocolV1Replies.decodeResultMapStringKeys(envelope);
        JsonObject resultObject = (JsonObject) CustomProtocolV1Replies.resultValue(envelope);

        Assert.assertSame(resultObject.values(), result);
        result.put("evil", new JsonLong(2));
        ((JsonObject) result.get("outer")).values().put("y", new JsonLong(2));

        Assert.assertEquals(new JsonLong(2), resultObject.values().get("evil"));
        Assert.assertEquals(new JsonLong(2), ((JsonObject) resultObject.values().get("outer")).values().get("y"));
    }

    @Test
    public void decodeResultMapStringKeysReturnsMutableDecodedTaggedMapValuesForClientCompatibility() {
        byte[] line = "{\"ok\":true,\"result\":{\"$map\":[[\"outer\",{\"items\":[1]}]]}}".getBytes(StandardCharsets.UTF_8);
        JsonValue envelope = JsonParser.parseStrictUtf8(line, 0, line.length, JsonLimits.DEFAULT);

        Map<String, JsonValue> result = CustomProtocolV1Replies.decodeResultMapStringKeys(envelope);
        JsonObject outer = (JsonObject) result.get("outer");
        JsonArray items = (JsonArray) outer.values().get("items");

        result.put("evil", new JsonLong(2));
        items.values().add(new JsonLong(2));

        Assert.assertEquals(new JsonLong(2), result.get("evil"));
        Assert.assertEquals(2, items.values().size());
        Assert.assertEquals(new JsonLong(2), items.values().get(1));
    }

    @Test
    public void jsonReplyPublicConstructorAcceptsNonObjectEnvelopeForCompatibility() {
        byte[] line = "scalar".getBytes(StandardCharsets.UTF_8);
        JsonValue envelope = new JsonString("not-an-object");

        YierdisClient.JsonReply reply = new YierdisClient.JsonReply(line, envelope);

        Assert.assertSame(envelope, reply.envelope());
        Assert.assertEquals("scalar", reply.lineUtf8());
    }

    private static boolean ok(JsonValue envelope) {
        return CustomProtocolV1Replies.isOkEnvelope(envelope);
    }

    private static JsonValue resultValue(JsonValue envelope) {
        return CustomProtocolV1Replies.resultValue(envelope);
    }

    private static String stringResult(JsonValue envelope) {
        JsonValue v = resultValue(envelope);
        Assert.assertTrue(v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static JsonObject objectResult(JsonValue envelope) {
        return new JsonObject(CustomProtocolV1Replies.decodeResultMapStringKeys(envelope));
    }

    private static JsonObject errorObject(JsonValue envelope) {
        return CustomProtocolV1Replies.errorObject(envelope);
    }

    private static JsonObject envelopeObject(JsonValue envelope) {
        return CustomProtocolV1Replies.envelopeObject(envelope);
    }

    private static String stringField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertTrue("expected string field: " + key, v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static long longField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertTrue("expected long field: " + key, v instanceof JsonLong);
        return ((JsonLong) v).value();
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
     * A server that floods JSON reply lines on connect, used to trigger client response queue overflow.
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
                                    byte[] ok = "{\"ok\":true,\"result\":\"OK\"}\n".getBytes(StandardCharsets.US_ASCII);
                                    for (int i = 0; i < lines; i++) {
                                        ctx.write(Unpooled.copiedBuffer(ok));
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
