package yier.bubu.redis.client;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypedCommandTest {
    @Test
    public void surfaceOmitsHelloTransactionPipelineAndMget() {
        for (Method method : Connection.class.getMethods()) {
            if (method.getDeclaringClass() != Connection.class) {
                continue;
            }
            String name = method.getName();
            Assert.assertFalse(name.equalsIgnoreCase("hello"));
            Assert.assertFalse(name.equals("mget"));
            Assert.assertFalse(name.equals("pipeline"));
            Assert.assertFalse(name.equals("multi"));
            Assert.assertFalse(name.equals("exec"));
            Assert.assertFalse(name.equals("discard"));
        }
    }

    @Test
    public void connectionCommandsCoverSuccessAndServerErrors() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("PONG", connection.ping());
            Assert.assertEquals("PONG", connection.ping(1_000));
            Assert.assertEquals("hi", connection.ping("hi"));
            Assert.assertEquals("hi", connection.echo("hi"));

            long count = connection.commandCount();
            Assert.assertTrue(count > 0);
            Assert.assertEquals(count, connection.commandCount(1_000));
            List<CommandInfo> listed = connection.commandList();
            Assert.assertEquals(count, listed.size());
            Assert.assertTrue(listed.stream().anyMatch(info -> "ping".equals(info.name())));
            List<CommandInfo> info = connection.commandInfo("ping", "no-such-command");
            Assert.assertEquals(2, info.size());
            Assert.assertEquals("ping", info.get(0).name());
            Assert.assertNotNull(info.get(0).flags());
            Assert.assertNull(info.get(1));

            Assert.assertEquals("OK", connection.clientSetname("app"));
            Assert.assertEquals("app", connection.clientGetname());
            Assert.assertEquals("OK", connection.clientSetname(""));
            Assert.assertNull(connection.clientGetname());
            Assert.assertEquals("OK", connection.clientSetinfo("LIB-NAME", "yierdis-client"));
            Assert.assertEquals("OK", connection.clientSetinfo("LIB-VER", "0"));
            assertServerError(
                    () -> connection.clientSetinfo("NOPE", "x"),
                    "Unrecognized option"
            );
            assertServerError(
                    () -> connection.clientSetname("bad name"),
                    "Client names cannot contain spaces"
            );

            assertServerError(
                    () -> connection.auth("secret"),
                    "AUTH <password> called without any password configured"
            );
            assertServerError(
                    () -> connection.auth("default", "secret"),
                    "AUTH <password> called without any password configured"
            );

            Assert.assertEquals("OK", connection.set("gone", "v"));
            Assert.assertEquals("OK", connection.flushdb());
            Assert.assertNull(connection.get("gone"));
            Assert.assertEquals("OK", connection.set("gone", "v"));
            Assert.assertEquals("OK", connection.flushdb(FlushMode.ASYNC));
            Assert.assertNull(connection.get("gone"));
            Assert.assertEquals("OK", connection.flushdb(1_000, FlushMode.SYNC));

            Assert.assertEquals("OK", connection.select(2));
            Assert.assertEquals(2, connection.database());
            assertServerError(() -> connection.select(16), "DB index is out of range");
            Assert.assertEquals(2, connection.database());
            Assert.assertEquals("PONG", connection.ping());
        }
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.quit());
            try {
                connection.ping();
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void stringCommandsSplitSetReplyShapes() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", connection.set("k", "v1"));
            Assert.assertNull(connection.set("k", "v2", SetOptions.nx()));
            Assert.assertEquals("v1", connection.get("k"));
            Assert.assertEquals("v1", connection.setGet("k", "v2"));
            Assert.assertEquals("v2", connection.get("k"));
            Assert.assertEquals("v2", connection.setGet("k", "v3", SetGetOptions.nx()));
            Assert.assertEquals("v2", connection.get("k"));
            Assert.assertEquals("OK", connection.set("k", "v4", SetOptions.xx().andEx(60)));
            Assert.assertEquals("v4", connection.get(1_000, "k"));
            Assert.assertNull(connection.set("missing", "v", SetOptions.xx()));
            Assert.assertEquals("OK", connection.set("fresh", "v", SetOptions.nx().andPx(60_000)));

            try {
                connection.set("k", "next", SetOptions.xx().withGet());
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            Assert.assertEquals("v4", connection.get("k"));
            assertLocal(() -> SetOptions.nx().andXx());
            assertLocal(() -> SetOptions.ex(1).andKeepTtl());

            Assert.assertEquals(2L, connection.strlen("k"));
            Assert.assertEquals(3L, connection.append("k", "!"));
            Assert.assertEquals("v4!", connection.get("k"));
            Assert.assertEquals(0L, connection.setbit("bits", 0, 1));
            Assert.assertEquals(1L, connection.getbit("bits", 0));
            Assert.assertEquals(1L, connection.bitcount("bits"));
            Assert.assertEquals(1L, connection.bitcount("bits", 0, -1));
            Assert.assertEquals(1L, connection.incr("n"));
            Assert.assertEquals(0L, connection.decr("n"));

            Assert.assertEquals(1L, connection.lpush("list", "a"));
            assertServerError(() -> connection.get("list"), "WRONGTYPE");
            assertServerError(() -> connection.incr("k"), "not an integer");
            try {
                connection.get(0, "k");
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            Assert.assertEquals("v4!", connection.get("k"));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void hashCommandsKeepReplyOrder() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(2L, connection.hset("h", "a", "1", "b", "2"));
            Assert.assertEquals(1L, connection.hset(1_000, "h", "c", "3"));
            Assert.assertEquals("2", connection.hget("h", "b"));
            Assert.assertNull(connection.hget("h", "missing"));
            Map<String, String> all = connection.hgetall("h");
            Assert.assertEquals(List.of("a", "1", "b", "2", "c", "3"), flat(all));
            Object raw = connection.command("HGETALL", "h");
            Assert.assertTrue(raw instanceof List);
            Assert.assertFalse(raw instanceof Map);
            Assert.assertEquals(flat(all), raw);
            Assert.assertEquals(3L, connection.hlen("h"));
            Assert.assertEquals(1L, connection.hdel("h", "b", "missing"));
            Assert.assertEquals(List.of("a", "c"), new ArrayList<>(connection.hgetall("h").keySet()));

            HashScan scan = connection.hscan("h", "0", ScanOptions.count(100));
            Object rawScan = connection.command("HSCAN", "h", "0", "COUNT", "100");
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), flatEntries(scan.entries()));
            HashFieldScan fields = connection.hscanNoValues("h", "0", ScanOptions.match("a*").andCount(100));
            Assert.assertEquals(List.of("a"), fields.fields());
            assertLocal(() -> ScanOptions.count(10).withNoValues());
            Assert.assertEquals("1", connection.hget("h", "a"));

            Assert.assertEquals("OK", connection.set("s", "v"));
            assertServerError(() -> connection.hget("s", "a"), "WRONGTYPE");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void listCommandsSplitPopReplyShapes() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(1L, connection.rpush("l", "a"));
            Assert.assertEquals(3L, connection.rpush("l", "b", "c"));
            Assert.assertEquals(List.of("a", "b", "c"), connection.lrange("l", 0, -1));
            Assert.assertEquals("a", connection.lpop("l"));
            Assert.assertEquals(List.of("b", "c"), connection.lpop("l", 2));
            Assert.assertNull(connection.lpop("missing"));
            Assert.assertNull(connection.rpop("missing", 2));
            Assert.assertEquals(2L, connection.lpush("l", "d", "e"));
            Assert.assertEquals("d", connection.rpop("l"));
            Assert.assertEquals(List.of("e"), connection.rpop(1_000, "l", 1));
            assertServerError(() -> connection.lpop("l", -1), "positive");
            Assert.assertEquals("OK", connection.set("s", "v"));
            assertServerError(() -> connection.lpop("s"), "WRONGTYPE");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void setCommandsKeepMemberOrder() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(2L, connection.sadd("s", "a", "b"));
            Assert.assertEquals(1L, connection.sadd("s", "c"));
            Assert.assertEquals(1L, connection.srem("s", "b", "missing"));
            Set<String> members = connection.smembers("s");
            Object raw = connection.command("SMEMBERS", "s");
            Assert.assertEquals(raw, new ArrayList<>(members));
            Assert.assertEquals(1L, connection.sismember("s", "a"));
            Assert.assertEquals(0L, connection.sismember("s", "b"));
            Assert.assertEquals(2L, connection.scard("s"));
            SetScan scan = connection.sscan("s", "0", ScanOptions.count(100));
            Object rawScan = connection.command("SSCAN", "s", "0", "COUNT", "100");
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), new ArrayList<>(scan.members()));
            Assert.assertEquals("OK", connection.set("str", "v"));
            assertServerError(() -> connection.sismember("str", "a"), "WRONGTYPE");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void zsetCommandsSplitIncrAndScores() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(1L, connection.zadd("z", "1", "a"));
            Assert.assertEquals(2L, connection.zadd("z", "2", "b", "3", "c"));
            Assert.assertEquals(0L, connection.zadd("z", ZAddOptions.nx(), "9", "a"));
            Assert.assertEquals(1L, connection.zadd("z", ZAddOptions.ch(), "4", "c"));
            Assert.assertEquals("5", connection.zaddIncr("z", "1", "c"));
            Assert.assertNull(connection.zaddIncr("z", ZAddIncrOptions.xx(), "1", "missing"));

            Assert.assertEquals(List.of("a", "b", "c"), connection.zrange("z", 0, -1));
            Assert.assertEquals(List.of("c", "b", "a"), connection.zrange("z", 0, -1, ZRangeOptions.rev()));
            Assert.assertEquals(
                    List.of(new ScoredMember("a", "1"), new ScoredMember("b", "2"), new ScoredMember("c", "5")),
                    connection.zrangeWithScores("z", 0, -1)
            );
            Assert.assertEquals(List.of("c", "b", "a"), connection.zrevrange("z", 0, -1));
            Assert.assertEquals(
                    new ScoredMember("c", "5"),
                    connection.zrevrangeWithScores("z", 0, 0).get(0)
            );
            Assert.assertEquals(List.of("b"), connection.zrangeByScore("z", "2", "2"));
            Assert.assertEquals(
                    List.of("c"),
                    connection.zrangeByScore("z", "-inf", "+inf", ScoreRangeOptions.limit(2, 1))
            );
            Assert.assertEquals(
                    List.of(new ScoredMember("b", "2")),
                    connection.zrangeByScoreWithScores("z", "(1", "3")
            );
            Assert.assertEquals(List.of("c", "b", "a"), connection.zrevrangeByScore("z", "+inf", "-inf"));
            Assert.assertEquals(
                    List.of(new ScoredMember("a", "1")),
                    connection.zrevrangeByScoreWithScores("z", "2", "-inf", ScoreRangeOptions.limit(1, 1))
            );

            ZScan scan = connection.zscan("z", "0", ScanOptions.count(100));
            Object rawScan = connection.command("ZSCAN", "z", "0", "COUNT", "100");
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), flatScored(scan.entries()));

            Assert.assertEquals(1L, connection.zrem("z", "b"));
            Assert.assertEquals(1L, connection.zremrangeByScore("z", "5", "5"));
            Assert.assertEquals(1L, connection.zremrangeByRank("z", 0, 0));
            Assert.assertEquals(List.of(), connection.zrange("z", 0, -1));

            assertLocal(() -> ZAddOptions.nx().withIncr());
            assertLocal(() -> ZAddOptions.nx().andXx());
            assertLocal(() -> ZAddOptions.gt().andLt());
            assertLocal(() -> ZAddOptions.nx().andGt());
            assertLocal(() -> ZRangeOptions.rev().withScores());
            assertLocal(() -> ScoreRangeOptions.limit(0, 1).withScores());
            Assert.assertEquals(1L, connection.zadd(1_000, "z", "1", "a"));

            Assert.assertEquals("OK", connection.set("s", "v"));
            assertServerError(() -> connection.zrange("s", 0, -1), "WRONGTYPE");
            assertServerError(() -> connection.zadd("z", "nope", "m"), "not a valid float");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void keyCommandsCoverScanExpireAndMemory() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("none", connection.type("missing"));
            Assert.assertNull(connection.memoryUsage("missing"));
            Assert.assertNull(connection.objectEncoding("missing"));
            Assert.assertEquals("OK", connection.set("k", "v"));
            Assert.assertEquals("string", connection.type("k"));
            Assert.assertTrue(connection.memoryUsage("k") > 0);
            Assert.assertTrue(connection.memoryUsage("k", MemoryUsageOptions.samples(0)) > 0);
            Assert.assertTrue(connection.memoryStats().get("key_count") instanceof Long);
            Assert.assertNotNull(connection.objectEncoding("k"));
            Assert.assertTrue(connection.keys("k*").contains("k"));
            KeyScan scan = connection.scan("0", ScanOptions.match("k*").andCount(100));
            Object rawScan = connection.command("SCAN", "0", "MATCH", "k*", "COUNT", "100");
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), scan.keys());

            Assert.assertEquals(1L, connection.exists("k", "missing"));
            Assert.assertEquals(1L, connection.expire("k", 30));
            Assert.assertTrue(connection.ttl("k") > 0);
            Assert.assertEquals(1L, connection.pexpire("k", 60_000, ExpireOptions.xx().andGt()));
            Assert.assertTrue(connection.pttl("k") > 0);
            Assert.assertEquals(1L, connection.expireat("k", 4_102_444_800L));
            Assert.assertEquals(1L, connection.pexpireat("k", 4_102_444_800_000L, ExpireOptions.xx()));
            Assert.assertEquals(1L, connection.persist("k"));
            Assert.assertEquals(-1L, connection.ttl("k"));
            assertLocal(() -> ExpireOptions.nx().andXx());
            assertLocal(() -> ExpireOptions.gt().andLt());

            Assert.assertEquals(1L, connection.del("k", "missing"));
            Assert.assertEquals(0L, connection.exists("k"));
            assertServerError(() -> connection.scan("-1"), "not an integer");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void hyperLogLogCommandsAndWrongType() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(1L, connection.pfadd("h", "a", "b"));
            Assert.assertEquals(0L, connection.pfadd("h", "a"));
            Assert.assertTrue(connection.pfcount("h") >= 1L);
            Assert.assertEquals("OK", connection.pfmerge("out", "h"));
            Assert.assertTrue(connection.pfcount(1_000, "out") >= 1L);
            Assert.assertEquals("OK", connection.set("s", "v"));
            assertServerError(() -> connection.pfadd("s", "a"), "WRONGTYPE");
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void infoIsTextStatsCountersAreLongAndWrongShapeStaysUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            String info = connection.info();
            Assert.assertTrue(info.contains("# Server"));
            Assert.assertTrue(connection.info("server").contains("# Server"));
            Assert.assertTrue(connection.info(1_000).contains("redis_version"));
            try {
                connection.info("health");
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("PONG", connection.ping());
            Map<String, Object> health = connection.infoHealth();
            Assert.assertTrue(health.get("ready") instanceof Long);
            Assert.assertTrue(health.get("lifecycle_state") instanceof String);
            Map<String, Object> yierdis = connection.infoYierdis();
            Assert.assertEquals("yierdis", yierdis.get("server"));
            Assert.assertTrue(yierdis.get("port") instanceof Long);

            Map<String, Object> stats = connection.stats();
            Assert.assertTrue(stats.get("commands_executed_total") instanceof Long);
            Assert.assertTrue(stats.get("lifecycle_state") instanceof String);
            List<?> rawStats = (List<?>) connection.command("STATS");
            ArrayList<String> rawKeys = new ArrayList<>();
            for (int index = 0; index < rawStats.size(); index += 2) {
                rawKeys.add((String) rawStats.get(index));
            }
            Assert.assertEquals(rawKeys, new ArrayList<>(connection.stats().keySet()));

            Assert.assertEquals("OK", connection.ydreconcile());
            Assert.assertEquals("OK", connection.ydreconcile(1_000));
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    @Test
    public void rawMgetIsUnknownAndConnectionStaysUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            connection.hset("h", "f", "v");
            Object raw = connection.command("HGETALL", "h");
            Assert.assertEquals(List.of("f", "v"), raw);
            Assert.assertFalse(raw instanceof Map);
            try {
                connection.command("MGET", "h");
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'MGET'", e.getMessage());
            }
            Assert.assertEquals("PONG", connection.ping());
        }
    }

    private static Connection connect(TestServer server) {
        return Connection.connect("127.0.0.1", server.port());
    }

    private static void assertServerError(Runnable call, String text) {
        try {
            call.run();
            Assert.fail("expected ServerException");
        } catch (ServerException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(text));
        }
    }

    private static void assertLocal(Runnable call) {
        try {
            call.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    private static List<String> flat(Map<String, String> map) {
        ArrayList<String> flat = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            flat.add(entry.getKey());
            flat.add(entry.getValue());
        }
        return flat;
    }

    private static List<String> flatEntries(List<HashEntry> entries) {
        ArrayList<String> flat = new ArrayList<>();
        for (HashEntry entry : entries) {
            flat.add(entry.field());
            flat.add(entry.value());
        }
        return flat;
    }

    private static List<String> flatScored(List<ScoredMember> entries) {
        ArrayList<String> flat = new ArrayList<>();
        for (ScoredMember entry : entries) {
            flat.add(entry.member());
            flat.add(entry.score());
        }
        return flat;
    }

    private static Object scanRow(Object reply, int index) {
        Assert.assertTrue(reply instanceof List);
        return ((List<?>) reply).get(index);
    }
}
