package yier.bubu.redis.client;

// maxmemory 全局口径回归：验证 global/per-db 行为差异、跨 DB 淘汰，以及 off-heap 统计不双计。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.YierdisServerBootstrap;
import yier.bubu.redis.protocol.json.JsonBoolean;
import yier.bubu.redis.protocol.json.JsonLong;
import yier.bubu.redis.protocol.json.JsonNull;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.v1.CustomProtocolV1TaggedValue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
                JsonValue bVal = resultValue(execute(client, b("GET"), b("b")));
                Assert.assertTrue(bVal == null || bVal instanceof JsonNull);

                ok(client, b("SELECT"), b("0"));
                JsonValue aVal = resultValue(execute(client, b("GET"), b("a")));
                Assert.assertTrue(aVal instanceof JsonString);
                JsonValue cVal = resultValue(execute(client, b("GET"), b("c")));
                Assert.assertTrue(cVal instanceof JsonString);
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
                JsonValue bVal = resultValue(execute(client, b("GET"), b("b")));
                Assert.assertTrue(bVal instanceof JsonString);

                ok(client, b("SELECT"), b("0"));
                JsonValue aVal = resultValue(execute(client, b("GET"), b("a")));
                Assert.assertTrue(aVal == null || aVal instanceof JsonNull);
                JsonValue cVal = resultValue(execute(client, b("GET"), b("c")));
                Assert.assertTrue(cVal instanceof JsonString);
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
                long ledgerUsed = stats.getOrDefault("ledger_used_bytes", -1L);
                long offHeap = stats.getOrDefault("offheap_used_bytes", -1L);
                long offHeapIncluded = stats.getOrDefault("offheap_included_in_maxmemory", -1L);

                Assert.assertTrue("offheap_used_bytes should be > 0 when offheapBackend=netty", offHeap > 0);
                Assert.assertEquals("offheap_included_in_maxmemory should be 1 in global mode", 1L, offHeapIncluded);
                Assert.assertEquals("used_bytes_for_maxmemory should equal ledger_used_bytes + offheap_used_bytes when included",
                        ledgerUsed + offHeap, used);
            }
        }
    }

    private static HashMap<String, Long> parseMemoryStats(JsonValue envelope) {
        Assert.assertTrue(okEnvelope(envelope));
        JsonValue result = resultValue(envelope);
        Assert.assertTrue(result instanceof JsonObject);
        JsonObject obj = (JsonObject) result;
        Map<String, JsonValue> values = CustomProtocolV1TaggedValue.isTaggedMap(obj)
                ? CustomProtocolV1TaggedValue.decodeTaggedMapToStringKeyedObject(obj)
                : obj.values();

        HashMap<String, Long> out = new HashMap<>();
        for (Map.Entry<String, JsonValue> e : values.entrySet()) {
            JsonValue v = e.getValue();
            if (v instanceof JsonLong l) {
                out.put(e.getKey(), l.value());
            }
        }
        return out;
    }

    private static JsonValue execute(YierdisClient client, byte[]... args) throws Exception {
        return client.execute(Arrays.asList(args), 2000).envelope();
    }

    private static void ok(YierdisClient client, byte[]... args) throws Exception {
        JsonValue env = execute(client, args);
        Assert.assertTrue(okEnvelope(env));
        Assert.assertEquals("OK", stringResult(env));
    }

    private static boolean okEnvelope(JsonValue envelope) {
        Assert.assertTrue(envelope instanceof JsonObject);
        JsonValue ok = ((JsonObject) envelope).values().get("ok");
        return ok instanceof JsonBoolean b && b.value();
    }

    private static JsonValue resultValue(JsonValue envelope) {
        Assert.assertTrue(envelope instanceof JsonObject);
        return ((JsonObject) envelope).values().get("result");
    }

    private static String stringResult(JsonValue envelope) {
        JsonValue v = resultValue(envelope);
        Assert.assertTrue(v instanceof JsonString);
        return ((JsonString) v).value();
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
