package yier.bubu.redis.app.client;

// maxmemory 全局口径回归：验证 global/per-db 行为差异、跨 DB 淘汰，以及 off-heap 统计不双计。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MaxmemoryScopeTest {
    private static final String PROBE_MAXMEMORY_BYTES = "100000000";
    private static final int EVICTION_VALUE_BYTES = 64 * 1024;

    @Test
    public void globalScopeEvictsAcrossDbsUsingLru() throws Exception {
        byte[] value = bytesOfLen(EVICTION_VALUE_BYTES, (byte) 'x');
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", Long.toString(globalBudgetThatFitsTwoKeysButNotThree(value)),
                "--maxmemoryScope", "global",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            ok(client, b("SELECT"), b("1"));
            ok(client, b("SET"), b("b"), value);

            ok(client, b("SELECT"), b("0"));
            ok(client, b("SET"), b("a"), value);

            // 触发全局淘汰：应淘汰最旧的 key（DB1:b），而不是局限于当前 DB。
            ok(client, b("SET"), b("c"), value);

            ok(client, b("SELECT"), b("1"));
            YierdisClient.RespReply bVal = execute(client, b("GET"), b("b"));

            ok(client, b("SELECT"), b("0"));
            YierdisClient.RespReply aVal = execute(client, b("GET"), b("a"));
            YierdisClient.RespReply cVal = execute(client, b("GET"), b("c"));

            Assert.assertTrue(bVal.isNull());
            Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, aVal.kind());
            Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, cVal.kind());
        }
    }

    @Test
    public void perDbScopeEvictsOnlyWithinSelectedDb() throws Exception {
        byte[] value = bytesOfLen(EVICTION_VALUE_BYTES, (byte) 'x');
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", Long.toString(perDbBudgetThatFitsOneKeyButNotTwo(value)),
                "--maxmemoryScope", "per-db",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            ok(client, b("SELECT"), b("1"));
            ok(client, b("SET"), b("b"), value);

            ok(client, b("SELECT"), b("0"));
            ok(client, b("SET"), b("a"), value);

            // per-db 模式下，DB0 写入触发淘汰只会影响 DB0，本例应淘汰 a，保留 DB1:b。
            ok(client, b("SET"), b("c"), value);

            ok(client, b("SELECT"), b("1"));
            YierdisClient.RespReply bVal = execute(client, b("GET"), b("b"));
            Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, bVal.kind());

            ok(client, b("SELECT"), b("0"));
            YierdisClient.RespReply aVal = execute(client, b("GET"), b("a"));
            Assert.assertTrue(aVal.isNull());
            YierdisClient.RespReply cVal = execute(client, b("GET"), b("c"));
            Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, cVal.kind());
        }
    }

    private static long globalBudgetThatFitsTwoKeysButNotThree(byte[] value) throws Exception {
        long usedAfterTwoKeys = probeGlobalUsedBytes(value, 1);
        long usedAfterThreeKeys = probeGlobalUsedBytes(value, 2);
        return midpointBudget(
                usedAfterTwoKeys,
                usedAfterThreeKeys,
                "global"
        );
    }

    private static long perDbBudgetThatFitsOneKeyButNotTwo(byte[] value) throws Exception {
        long usedAfterOneLocalKey = probePerDbDb0UsedBytes(value, 1);
        long usedAfterTwoLocalKeys = probePerDbDb0UsedBytes(value, 2);
        long perDbBudget = midpointBudget(
                usedAfterOneLocalKey,
                usedAfterTwoLocalKeys,
                "per-db"
        );
        return Math.multiplyExact(perDbBudget, 2L);
    }

    private static long probeGlobalUsedBytes(byte[] value, int localKeyCount) throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", PROBE_MAXMEMORY_BYTES,
                "--maxmemoryScope", "global",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            ok(client, b("SELECT"), b("1"));
            ok(client, b("SET"), b("b"), value);
            if (localKeyCount > 0) {
                ok(client, b("SELECT"), b("0"));
                ok(client, b("SET"), b("a"), value);
            }
            if (localKeyCount > 1) {
                ok(client, b("SET"), b("c"), value);
            }
            return globalUsedBytesForMaxmemory(client);
        }
    }

    private static long probePerDbDb0UsedBytes(byte[] value, int localKeyCount) throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryBytes", PROBE_MAXMEMORY_BYTES,
                "--maxmemoryScope", "per-db",
                "--maxmemoryPolicy", "allkeys-lru",
                "--maxmemorySamples", "1000"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            ok(client, b("SELECT"), b("1"));
            ok(client, b("SET"), b("b"), value);
            if (localKeyCount > 0) {
                ok(client, b("SELECT"), b("0"));
                ok(client, b("SET"), b("a"), value);
            }
            if (localKeyCount > 1) {
                ok(client, b("SET"), b("c"), value);
            }
            return memoryStats(client, 0).getOrDefault("used_bytes_for_maxmemory", -1L);
        }
    }

    private static long globalUsedBytesForMaxmemory(YierdisClient client) throws Exception {
        // Under server GLOBAL scope, MEMORY STATS is already aggregated across DBs by NettyServerInfoProvider.
        return memoryStats(client, 0).getOrDefault("used_bytes_for_maxmemory", -1L);
    }

    private static long midpointBudget(long lowerBound, long upperExclusive, String label) {
        Assert.assertTrue(label + " probe must leave room between fit and overflow budgets", upperExclusive > lowerBound);
        long span = upperExclusive - lowerBound;
        return lowerBound + Math.max(0L, (span - 1L) / 2L);
    }

    @Test
    public void globalMemoryStatsIncludesDefaultFfmNativeMemoryOnce() throws Exception {
        try (TestServer server = TestServer.startWithArgs(
                "--databases", "2",
                "--maxmemoryScope", "global",
                "--maxmemoryBytes", "0"
        );
             YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            ok(client, b("SELECT"), b("0"));
            ok(client, b("SET"), b("k"), bytesOfLen(1024, (byte) 'x'));

            HashMap<String, Long> stats = parseMemoryStats(execute(client, b("MEMORY"), b("STATS")));
            long used = stats.getOrDefault("used_bytes_for_maxmemory", -1L);
            long ledgerUsed = stats.getOrDefault("ledger_used_bytes", -1L);
            long offHeap = stats.getOrDefault("offheap_used_bytes", -1L);
            long offHeapIncluded = stats.getOrDefault("offheap_included_in_maxmemory", -1L);

            Assert.assertTrue("offheap_used_bytes should be > 0 under the default FFM memory model", offHeap > 0);
            Assert.assertEquals("offheap_included_in_maxmemory should be 1 in global mode", 1L, offHeapIncluded);
            Assert.assertEquals("used_bytes_for_maxmemory should equal ledger_used_bytes + offheap_used_bytes when included",
                    ledgerUsed + offHeap, used);
        }
    }

    private static HashMap<String, Long> memoryStats(YierdisClient client, int dbIndex) throws Exception {
        ok(client, b("SELECT"), b(Integer.toString(dbIndex)));
        return parseMemoryStats(execute(client, b("MEMORY"), b("STATS")));
    }

    private static HashMap<String, Long> parseMemoryStats(YierdisClient.RespReply reply) {
        Map<String, YierdisClient.RespReply> values = replyMap(reply);
        HashMap<String, Long> out = new HashMap<>();
        for (Map.Entry<String, YierdisClient.RespReply> e : values.entrySet()) {
            YierdisClient.RespReply v = e.getValue();
            if (v.kind() == YierdisClient.RespReply.Kind.INTEGER) {
                out.put(e.getKey(), v.integer());
            }
        }
        return out;
    }

    private static YierdisClient.RespReply execute(YierdisClient client, byte[]... args) throws Exception {
        return client.execute(Arrays.asList(args), 2000);
    }

    private static void ok(YierdisClient client, byte[]... args) throws Exception {
        Assert.assertEquals("OK", stringResult(execute(client, args)));
    }

    private static Map<String, YierdisClient.RespReply> replyMap(YierdisClient.RespReply reply) {
        Assert.assertEquals(YierdisClient.RespReply.Kind.ARRAY, reply.kind());
        List<YierdisClient.RespReply> values = reply.values();
        Assert.assertNotNull(values);
        Assert.assertEquals(0, values.size() % 2);
        Map<String, YierdisClient.RespReply> map = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i += 2) {
            map.put(stringResult(values.get(i)), values.get(i + 1));
        }
        return map;
    }

    private static String stringResult(YierdisClient.RespReply reply) {
        Assert.assertNotNull(reply);
        if (reply.kind() == YierdisClient.RespReply.Kind.SIMPLE_STRING) {
            return reply.text();
        }
        if (reply.kind() == YierdisClient.RespReply.Kind.ERROR) {
            Assert.fail("unexpected error reply: " + reply.text());
        }
        Assert.assertEquals(YierdisClient.RespReply.Kind.BULK_STRING, reply.kind());
        return new String(reply.bytes(), StandardCharsets.UTF_8);
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
