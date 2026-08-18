package yier.bubu.redis.app.client;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YierdisClientTest {
    @Test
    public void pingReturnsSimpleStringPongOverResp() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            RespClientCodec.RespReply reply = client.execute(List.of("PING".getBytes(StandardCharsets.UTF_8)), 1000);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.SIMPLE_STRING, reply.kind());
            Assert.assertEquals("PONG", reply.text());
        }
    }

    @Test
    public void helloReturnsMapAndNullIsDecoded() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Map<String, RespClientCodec.RespReply> hello = replyMap(client.execute(Arrays.asList(b("HELLO")), 1000));
            Assert.assertEquals("yierdis", stringField(hello, "server"));
            Assert.assertNotNull(stringField(hello, "version"));
            Assert.assertEquals(2L, longField(hello, "proto"));
            Assert.assertEquals("standalone", stringField(hello, "mode"));
            Assert.assertEquals("master", stringField(hello, "role"));

            RespClientCodec.RespReply missing = client.execute(Arrays.asList(b("GET"), b("missing")), 1000);
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

            Map<String, RespClientCodec.RespReply> stats = replyMap(client.execute(Arrays.asList(b("STATS")), 1000));
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
            RespClientCodec.RespReply reply = client.execute(Arrays.asList(b("ECHO"), new byte[]{(byte) 0xFF}), 1000);

            Assert.assertArrayEquals(new byte[]{(byte) 0xFF}, bulkBytes(reply));
        }
    }

    @Test
    public void unknownCommandReturnsCommandErrorReply() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            RespClientCodec.RespReply reply = client.execute(Arrays.asList(b("NOPE")), 1000);

            Assert.assertEquals(RespClientCodec.RespReply.Kind.ERROR, reply.kind());
            Assert.assertTrue(reply.text().startsWith("ERR unknown command"));
        }
    }

    @Test
    public void memoryStatsHasStableKeySet() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Map<String, RespClientCodec.RespReply> stats = replyMap(client.execute(Arrays.asList(b("MEMORY"), b("STATS")), 1000));

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
    public void timeoutClosesConnectionToPreventResponseDesync() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(socket -> {
            if (socket.getInputStream().read() < 0) {
                throw new AssertionError("client closed before sending a command");
            }
            try {
                while (socket.getInputStream().read() >= 0) {
                }
            } catch (IOException ignored) {
            }
        });
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
            server.assertSucceeded();
        }
    }

    @Test
    public void serverCloseWakesExecuteWithoutTimeout() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(socket -> socket.getInputStream().read());
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
            server.assertSucceeded();
        }
    }

    @Test
    public void invalidRespReplyClosesConnection() throws Exception {
        try (ScriptedSocketServer server = ScriptedSocketServer.start(socket -> {
            socket.getOutputStream().write("{not-resp}\n".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            while (socket.getInputStream().read() >= 0) {
            }
        });
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
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
            server.assertSucceeded();
        }
    }

    @Test
    public void respReplyBulkBytesAccessorReturnsDefensiveCopy() throws Exception {
        try (TestServer server = TestServer.start();
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            RespClientCodec.RespReply reply = client.execute(Arrays.asList(b("ECHO"), b("original")), 1000);

            byte[] bytes = reply.bytes();
            bytes[0] = 'x';

            Assert.assertEquals("original", stringResult(reply));
        }
    }

    @Test
    public void respReplyPublicConstructorDefensivelyCopiesBytesAndValues() {
        byte[] bytes = b("scalar");
        RespClientCodec.RespReply bulk = new RespClientCodec.RespReply(
                RespClientCodec.RespReply.Kind.BULK_STRING, null, bytes, null, null
        );
        bytes[0] = 'x';
        Assert.assertEquals("scalar", stringResult(bulk));

        ArrayList<RespClientCodec.RespReply> values = new ArrayList<>();
        values.add(new RespClientCodec.RespReply(RespClientCodec.RespReply.Kind.INTEGER, null, null, 1L, null));
        RespClientCodec.RespReply array = new RespClientCodec.RespReply(
                RespClientCodec.RespReply.Kind.ARRAY, null, null, null, values
        );
        values.clear();
        Assert.assertEquals(1, array.values().size());
    }

    private static Map<String, RespClientCodec.RespReply> replyMap(RespClientCodec.RespReply reply) {
        Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, reply.kind());
        List<RespClientCodec.RespReply> values = reply.values();
        Assert.assertNotNull(values);
        Assert.assertEquals("expected even RESP2 map array length", 0, values.size() % 2);
        Map<String, RespClientCodec.RespReply> map = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i += 2) {
            map.put(stringResult(values.get(i)), values.get(i + 1));
        }
        return map;
    }

    private static String stringField(Map<String, RespClientCodec.RespReply> map, String key) {
        return stringResult(map.get(key));
    }

    private static long longField(Map<String, RespClientCodec.RespReply> map, String key) {
        RespClientCodec.RespReply value = map.get(key);
        Assert.assertNotNull("expected integer field: " + key, value);
        Assert.assertEquals("expected integer field: " + key, RespClientCodec.RespReply.Kind.INTEGER, value.kind());
        return value.integer();
    }

    private static String stringResult(RespClientCodec.RespReply reply) {
        Assert.assertNotNull(reply);
        if (reply.kind() == RespClientCodec.RespReply.Kind.SIMPLE_STRING) {
            return reply.text();
        }
        return new String(bulkBytes(reply), StandardCharsets.UTF_8);
    }

    private static byte[] bulkBytes(RespClientCodec.RespReply reply) {
        Assert.assertNotNull(reply);
        Assert.assertEquals(RespClientCodec.RespReply.Kind.BULK_STRING, reply.kind());
        return reply.bytes();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
                    "--maxmemoryBytes", "0",
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

}
