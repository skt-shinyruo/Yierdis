package yier.bubu.redis.client;

// maxmemory 全局口径回归：验证 global/per-db 行为差异、跨 DB 淘汰，以及 off-heap 统计不双计。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.YierdisServerBootstrap;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MaxmemoryScopeTest {
    @Test
    public void globalScopeEvictsAcrossDbsUsingLru() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", "800",
                "--maxmemoryScope", "global",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                ok(client, b("SELECT"), b("1"));
                ok(client, b("SET"), b("b"), bytesOfLen(256, (byte) 'b'));

                ok(client, b("SELECT"), b("0"));
                ok(client, b("SET"), b("a"), bytesOfLen(256, (byte) 'a'));

                // 触发全局淘汰：应淘汰最旧的 key（DB1:b），而不是局限于当前 DB。
                ok(client, b("SET"), b("c"), bytesOfLen(256, (byte) 'c'));

                ok(client, b("SELECT"), b("1"));
                RespBulkString bVal = (RespBulkString) execute(client, b("GET"), b("b"));
                Assert.assertTrue(bVal.isNull());

                ok(client, b("SELECT"), b("0"));
                RespBulkString aVal = (RespBulkString) execute(client, b("GET"), b("a"));
                Assert.assertFalse(aVal.isNull());
                RespBulkString cVal = (RespBulkString) execute(client, b("GET"), b("c"));
                Assert.assertFalse(cVal.isNull());
            }
        }
    }

    @Test
    public void perDbScopeEvictsOnlyWithinSelectedDb() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", "800",
                "--maxmemoryScope", "per-db",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                ok(client, b("SELECT"), b("1"));
                ok(client, b("SET"), b("b"), bytesOfLen(256, (byte) 'b'));

                ok(client, b("SELECT"), b("0"));
                ok(client, b("SET"), b("a"), bytesOfLen(256, (byte) 'a'));

                // per-db 模式下，DB0 写入触发淘汰只会影响 DB0，本例应淘汰 a，保留 DB1:b。
                ok(client, b("SET"), b("c"), bytesOfLen(256, (byte) 'c'));

                ok(client, b("SELECT"), b("1"));
                RespBulkString bVal = (RespBulkString) execute(client, b("GET"), b("b"));
                Assert.assertFalse(bVal.isNull());

                ok(client, b("SELECT"), b("0"));
                RespBulkString aVal = (RespBulkString) execute(client, b("GET"), b("a"));
                Assert.assertTrue(aVal.isNull());
                RespBulkString cVal = (RespBulkString) execute(client, b("GET"), b("c"));
                Assert.assertFalse(cVal.isNull());
            }
        }
    }

    @Test
    public void globalMemoryStatsDoesNotDoubleCountOffHeap() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryScope", "global",
                "--maxmemoryBytes", "0",
                "--offheapBackend", "netty",
                "--offheapMaxBytes", "10485760"
        )) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                ok(client, b("SELECT"), b("0"));
                ok(client, b("SET"), b("k"), bytesOfLen(1024, (byte) 'x'));

                HashMap<String, Long> stats = parseMemoryStats(execute(client, b("MEMORY"), b("STATS")));
                long used = stats.getOrDefault("used_bytes_for_maxmemory", -1L);
                long heap = stats.getOrDefault("heap_data_bytes_estimate", -1L);
                long offHeap = stats.getOrDefault("offheap_used_bytes", -1L);

                Assert.assertTrue("offheap_used_bytes should be > 0 when offheapBackend=netty", offHeap > 0);
                Assert.assertEquals("used_bytes_for_maxmemory should equal heap + offheap in global mode", heap + offHeap, used);
            }
        }
    }

    private static HashMap<String, Long> parseMemoryStats(RespObject obj) {
        Assert.assertTrue(obj instanceof RespArray);
        RespArray arr = (RespArray) obj;
        Assert.assertNotNull(arr.values());
        List<RespObject> values = arr.values();
        HashMap<String, Long> out = new HashMap<>();
        for (int i = 0; i + 1 < values.size(); i += 2) {
            Assert.assertTrue(values.get(i) instanceof RespBulkString);
            Assert.assertTrue(values.get(i + 1) instanceof RespInteger);
            String k = ((RespBulkString) values.get(i)).asString();
            long v = ((RespInteger) values.get(i + 1)).value();
            out.put(k, v);
        }
        return out;
    }

    private static RespObject execute(YierdisClient client, byte[]... args) throws Exception {
        try (RespFrame frame = client.execute(Arrays.asList(args), 2000)) {
            return RespObjectParser.parse(frame);
        }
    }

    private static void ok(YierdisClient client, byte[]... args) throws Exception {
        RespObject obj = execute(client, args);
        Assert.assertTrue(obj instanceof RespSimpleString);
        Assert.assertEquals("OK", ((RespSimpleString) obj).value());
    }

    private static byte[] bytesOfLen(int len, byte fill) {
        byte[] out = new byte[Math.max(0, len)];
        Arrays.fill(out, fill);
        return out;
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestServer implements AutoCloseable {
        private final YierdisServerBootstrap server;

        private TestServer(YierdisServerBootstrap server) {
            this.server = server;
        }

        static TestServer startWithArgs(String... extraArgs) throws Exception {
            String[] base = new String[]{
                    "--port", "0",
                    "--ioThreads", "1",
                    "--noCleanup"
            };
            String[] argv = Arrays.copyOf(base, base.length + extraArgs.length);
            System.arraycopy(extraArgs, 0, argv, base.length, extraArgs.length);
            return new TestServer(YierdisServerBootstrap.start(argv));
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

