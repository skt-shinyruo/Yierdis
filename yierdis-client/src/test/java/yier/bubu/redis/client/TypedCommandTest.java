package yier.bubu.redis.client;

import yier.bubu.redis.client.command.ExpireOptions;
import yier.bubu.redis.client.command.FlushMode;
import yier.bubu.redis.client.command.MemoryUsageOptions;
import yier.bubu.redis.client.command.ScanOptions;
import yier.bubu.redis.client.command.ScoreRangeOptions;
import yier.bubu.redis.client.command.SetGetOptions;
import yier.bubu.redis.client.command.SetOptions;
import yier.bubu.redis.client.command.ZAddIncrOptions;
import yier.bubu.redis.client.command.ZAddOptions;
import yier.bubu.redis.client.command.ZRangeOptions;
import yier.bubu.redis.client.exception.DecodeException;
import yier.bubu.redis.client.exception.ServerException;
import yier.bubu.redis.client.reply.CommandInfo;
import yier.bubu.redis.client.reply.HashEntry;
import yier.bubu.redis.client.reply.HashFieldScan;
import yier.bubu.redis.client.reply.HashScan;
import yier.bubu.redis.client.reply.KeyScan;
import yier.bubu.redis.client.reply.ScoredMember;
import yier.bubu.redis.client.reply.SetScan;
import yier.bubu.redis.client.reply.ZScan;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypedCommandTest {
    @Test
    public void surfaceOmitsHelloExecDiscardAndMget() {
        for (Method method : Connection.class.getMethods()) {
            if (method.getDeclaringClass() != Connection.class) {
                continue;
            }
            String name = method.getName();
            Assert.assertFalse(name.equalsIgnoreCase("hello"));
            Assert.assertFalse(name.equals("mget"));
            Assert.assertFalse(name.equals("exec"));
            Assert.assertFalse(name.equals("discard"));
        }
    }

    @Test
    public void connectionCommandsCoverSuccessAndServerErrors() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("PONG", Await.join(connection.ping()));
            Assert.assertEquals("PONG", Await.join(connection.ping(1_000)));
            Assert.assertEquals("hi", Await.join(connection.ping("hi")));
            Assert.assertEquals("hi", Await.join(connection.echo("hi")));

            long count = Await.join(connection.commandCount());
            Assert.assertTrue(count > 0);
            Assert.assertEquals(Long.valueOf(count), Await.join(connection.commandCount(1_000)));
            List<CommandInfo> listed = Await.join(connection.commandList());
            Assert.assertEquals(count, listed.size());
            Assert.assertTrue(listed.stream().anyMatch(info -> "ping".equals(info.name())));
            List<CommandInfo> info = Await.join(connection.commandInfo("ping", "no-such-command"));
            Assert.assertEquals(2, info.size());
            Assert.assertEquals("ping", info.get(0).name());
            Assert.assertNotNull(info.get(0).flags());
            Assert.assertNull(info.get(1));

            Assert.assertEquals("OK", Await.join(connection.clientSetname("app")));
            Assert.assertEquals("app", Await.join(connection.clientGetname()));
            Assert.assertEquals("OK", Await.join(connection.clientSetname("")));
            Assert.assertNull(Await.join(connection.clientGetname()));
            Assert.assertEquals("OK", Await.join(connection.clientSetinfo("LIB-NAME", "yierdis-client")));
            Assert.assertEquals("OK", Await.join(connection.clientSetinfo("LIB-VER", "0")));
            assertServerError(
                    () -> Await.join(connection.clientSetinfo("NOPE", "x")),
                    "Unrecognized option"
            );
            assertServerError(
                    () -> Await.join(connection.clientSetname("bad name")),
                    "Client names cannot contain spaces"
            );

            assertServerError(
                    () -> Await.join(connection.auth("secret")),
                    "AUTH <password> called without any password configured"
            );
            Assert.assertEquals("OK", Await.join(connection.auth("default", "secret")));
            assertServerError(
                    () -> Await.join(connection.auth("bob", "secret")),
                    "WRONGPASS invalid username-password pair or user is disabled."
            );

            Assert.assertEquals("OK", Await.join(connection.set("gone", "v")));
            Assert.assertEquals("OK", Await.join(connection.flushdb()));
            Assert.assertNull(Await.join(connection.get("gone")));
            Assert.assertEquals("OK", Await.join(connection.set("gone", "v")));
            Assert.assertEquals("OK", Await.join(connection.flushdb(FlushMode.ASYNC)));
            Assert.assertNull(Await.join(connection.get("gone")));
            Assert.assertEquals("OK", Await.join(connection.flushdb(1_000, FlushMode.SYNC)));

            Assert.assertEquals("OK", Await.join(connection.select(2)));
            Assert.assertEquals(2, connection.database());
            assertServerError(() -> Await.join(connection.select(16)), "DB index is out of range");
            Assert.assertEquals(2, connection.database());
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.quit()));
            try {
                Await.join(connection.ping());
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
            }
        }
    }

    @Test
    public void stringCommandsSplitSetReplyShapes() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("OK", Await.join(connection.set("k", "v1")));
            Assert.assertNull(Await.join(connection.set("k", "v2", SetOptions.nx())));
            Assert.assertEquals("v1", Await.join(connection.get("k")));
            Assert.assertEquals("v1", Await.join(connection.setGet("k", "v2")));
            Assert.assertEquals("v2", Await.join(connection.get("k")));
            Assert.assertEquals("v2", Await.join(connection.setGet("k", "v3", SetGetOptions.nx())));
            Assert.assertEquals("v2", Await.join(connection.get("k")));
            Assert.assertEquals("OK", Await.join(connection.set("k", "v4", SetOptions.xx().andEx(60))));
            Assert.assertEquals("v4", Await.join(connection.get(1_000, "k")));
            Assert.assertNull(Await.join(connection.set("missing", "v", SetOptions.xx())));
            Assert.assertEquals("OK", Await.join(connection.set("fresh", "v", SetOptions.nx().andPx(60_000))));

            try {
                Await.join(connection.set("k", "next", SetOptions.xx().withGet()));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            Assert.assertEquals("v4", Await.join(connection.get("k")));
            assertLocal(() -> SetOptions.nx().andXx());
            assertLocal(() -> SetOptions.ex(1).andKeepTtl());

            Assert.assertEquals(Long.valueOf(2), Await.join(connection.strlen("k")));
            Assert.assertEquals(Long.valueOf(3), Await.join(connection.append("k", "!")));
            Assert.assertEquals("v4!", Await.join(connection.get("k")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.setbit("bits", 0, 1)));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.getbit("bits", 0)));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.bitcount("bits")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.bitcount("bits", 0, -1)));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.incr("n")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.decr("n")));

            Assert.assertEquals(Long.valueOf(1), Await.join(connection.lpush("list", "a")));
            assertServerError(() -> Await.join(connection.get("list")), "WRONGTYPE");
            assertServerError(() -> Await.join(connection.incr("k")), "not an integer");
            try {
                Await.join(connection.get(0, "k"));
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("commandTimeoutMillis"));
            }
            Assert.assertEquals("v4!", Await.join(connection.get("k")));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void hashCommandsKeepReplyOrder() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(2), Await.join(connection.hset("h", "a", "1", "b", "2")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.hset(1_000, "h", "c", "3")));
            Assert.assertEquals("2", Await.join(connection.hget("h", "b")));
            Assert.assertNull(Await.join(connection.hget("h", "missing")));
            Map<String, String> all = Await.join(connection.hgetall("h"));
            Assert.assertEquals(List.of("a", "1", "b", "2", "c", "3"), flat(all));
            Object raw = Await.join(connection.command("HGETALL", "h"));
            Assert.assertTrue(raw instanceof List);
            Assert.assertFalse(raw instanceof Map);
            Assert.assertEquals(flat(all), raw);
            Assert.assertEquals(Long.valueOf(3), Await.join(connection.hlen("h")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.hdel("h", "b", "missing")));
            Assert.assertEquals(List.of("a", "c"), new ArrayList<>(Await.join(connection.hgetall("h")).keySet()));

            HashScan scan = Await.join(connection.hscan("h", "0", ScanOptions.count(100)));
            Object rawScan = Await.join(connection.command("HSCAN", "h", "0", "COUNT", "100"));
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), flatEntries(scan.entries()));
            HashFieldScan fields = Await.join(connection.hscanNoValues("h", "0", ScanOptions.match("a*").andCount(100)));
            Assert.assertEquals(List.of("a"), fields.fields());
            assertLocal(() -> ScanOptions.count(10).withNoValues());
            Assert.assertEquals("1", Await.join(connection.hget("h", "a")));

            Assert.assertEquals("OK", Await.join(connection.set("s", "v")));
            assertServerError(() -> Await.join(connection.hget("s", "a")), "WRONGTYPE");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void listCommandsSplitPopReplyShapes() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.rpush("l", "a")));
            Assert.assertEquals(Long.valueOf(3), Await.join(connection.rpush("l", "b", "c")));
            Assert.assertEquals(List.of("a", "b", "c"), Await.join(connection.lrange("l", 0, -1)));
            Assert.assertEquals("a", Await.join(connection.lpop("l")));
            Assert.assertEquals(List.of("b", "c"), Await.join(connection.lpop("l", 2)));
            Assert.assertNull(Await.join(connection.lpop("missing")));
            Assert.assertNull(Await.join(connection.rpop("missing", 2)));
            Assert.assertEquals(Long.valueOf(2), Await.join(connection.lpush("l", "d", "e")));
            Assert.assertEquals("d", Await.join(connection.rpop("l")));
            Assert.assertEquals(List.of("e"), Await.join(connection.rpop(1_000, "l", 1)));
            assertServerError(() -> Await.join(connection.lpop("l", -1)), "positive");
            Assert.assertEquals("OK", Await.join(connection.set("s", "v")));
            assertServerError(() -> Await.join(connection.lpop("s")), "WRONGTYPE");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void setCommandsKeepMemberOrder() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(2), Await.join(connection.sadd("s", "a", "b")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.sadd("s", "c")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.srem("s", "b", "missing")));
            Set<String> members = Await.join(connection.smembers("s"));
            Object raw = Await.join(connection.command("SMEMBERS", "s"));
            Assert.assertEquals(raw, new ArrayList<>(members));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.sismember("s", "a")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.sismember("s", "b")));
            Assert.assertEquals(Long.valueOf(2), Await.join(connection.scard("s")));
            SetScan scan = Await.join(connection.sscan("s", "0", ScanOptions.count(100)));
            Object rawScan = Await.join(connection.command("SSCAN", "s", "0", "COUNT", "100"));
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), new ArrayList<>(scan.members()));
            Assert.assertEquals("OK", Await.join(connection.set("str", "v")));
            assertServerError(() -> Await.join(connection.sismember("str", "a")), "WRONGTYPE");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void zsetCommandsSplitIncrAndScores() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zadd("z", "1", "a")));
            Assert.assertEquals(Long.valueOf(2), Await.join(connection.zadd("z", "2", "b", "3", "c")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.zadd("z", ZAddOptions.nx(), "9", "a")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zadd("z", ZAddOptions.ch(), "4", "c")));
            Assert.assertEquals("5", Await.join(connection.zaddIncr("z", "1", "c")));
            Assert.assertNull(Await.join(connection.zaddIncr("z", ZAddIncrOptions.xx(), "1", "missing")));

            Assert.assertEquals(List.of("a", "b", "c"), Await.join(connection.zrange("z", 0, -1)));
            Assert.assertEquals(List.of("c", "b", "a"), Await.join(connection.zrange("z", 0, -1, ZRangeOptions.rev())));
            Assert.assertEquals(
                    List.of(new ScoredMember("a", "1"), new ScoredMember("b", "2"), new ScoredMember("c", "5")),
                    Await.join(connection.zrangeWithScores("z", 0, -1))
            );
            Assert.assertEquals(List.of("c", "b", "a"), Await.join(connection.zrevrange("z", 0, -1)));
            Assert.assertEquals(
                    new ScoredMember("c", "5"),
                    Await.join(connection.zrevrangeWithScores("z", 0, 0)).get(0)
            );
            Assert.assertEquals(List.of("b"), Await.join(connection.zrangeByScore("z", "2", "2")));
            Assert.assertEquals(
                    List.of("c"),
                    Await.join(connection.zrangeByScore("z", "-inf", "+inf", ScoreRangeOptions.limit(2, 1)))
            );
            Assert.assertEquals(
                    List.of(new ScoredMember("b", "2")),
                    Await.join(connection.zrangeByScoreWithScores("z", "(1", "3"))
            );
            Assert.assertEquals(List.of("c", "b", "a"), Await.join(connection.zrevrangeByScore("z", "+inf", "-inf")));
            Assert.assertEquals(
                    List.of(new ScoredMember("a", "1")),
                    Await.join(connection.zrevrangeByScoreWithScores("z", "2", "-inf", ScoreRangeOptions.limit(1, 1)))
            );

            ZScan scan = Await.join(connection.zscan("z", "0", ScanOptions.count(100)));
            Object rawScan = Await.join(connection.command("ZSCAN", "z", "0", "COUNT", "100"));
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), flatScored(scan.entries()));

            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zrem("z", "b")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zremrangeByScore("z", "5", "5")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zremrangeByRank("z", 0, 0)));
            Assert.assertEquals(List.of(), Await.join(connection.zrange("z", 0, -1)));

            assertLocal(() -> ZAddOptions.nx().withIncr());
            assertLocal(() -> ZAddOptions.nx().andXx());
            assertLocal(() -> ZAddOptions.gt().andLt());
            assertLocal(() -> ZAddOptions.nx().andGt());
            assertLocal(() -> ZRangeOptions.rev().withScores());
            assertLocal(() -> ScoreRangeOptions.limit(0, 1).withScores());
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.zadd(1_000, "z", "1", "a")));

            Assert.assertEquals("OK", Await.join(connection.set("s", "v")));
            assertServerError(() -> Await.join(connection.zrange("s", 0, -1)), "WRONGTYPE");
            assertServerError(() -> Await.join(connection.zadd("z", "nope", "m")), "not a valid float");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void keyCommandsCoverScanExpireAndMemory() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals("none", Await.join(connection.type("missing")));
            Assert.assertNull(Await.join(connection.memoryUsage("missing")));
            Assert.assertNull(Await.join(connection.objectEncoding("missing")));
            Assert.assertEquals("OK", Await.join(connection.set("k", "v")));
            Assert.assertEquals("string", Await.join(connection.type("k")));
            Assert.assertTrue(Await.join(connection.memoryUsage("k")) > 0);
            Assert.assertTrue(Await.join(connection.memoryUsage("k", MemoryUsageOptions.samples(0))) > 0);
            Assert.assertTrue(Await.join(connection.memoryStats()).get("key_count") instanceof Long);
            Assert.assertNotNull(Await.join(connection.objectEncoding("k")));
            Assert.assertTrue(Await.join(connection.keys("k*")).contains("k"));
            KeyScan scan = Await.join(connection.scan("0", ScanOptions.match("k*").andCount(100)));
            Object rawScan = Await.join(connection.command("SCAN", "0", "MATCH", "k*", "COUNT", "100"));
            Assert.assertEquals(scanRow(rawScan, 0), scan.cursor());
            Assert.assertEquals(scanRow(rawScan, 1), scan.keys());

            Assert.assertEquals(Long.valueOf(1), Await.join(connection.exists("k", "missing")));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.expire("k", 30)));
            Assert.assertTrue(Await.join(connection.ttl("k")) > 0);
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.pexpire("k", 60_000, ExpireOptions.xx().andGt())));
            Assert.assertTrue(Await.join(connection.pttl("k")) > 0);
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.expireat("k", 4_102_444_800L)));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.pexpireat("k", 4_102_444_800_000L, ExpireOptions.xx())));
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.persist("k")));
            Assert.assertEquals(Long.valueOf(-1), Await.join(connection.ttl("k")));
            assertLocal(() -> ExpireOptions.nx().andXx());
            assertLocal(() -> ExpireOptions.gt().andLt());

            Assert.assertEquals(Long.valueOf(1), Await.join(connection.del("k", "missing")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.exists("k")));
            assertServerError(() -> Await.join(connection.scan("-1")), "invalid cursor");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void hyperLogLogCommandsAndWrongType() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Assert.assertEquals(Long.valueOf(1), Await.join(connection.pfadd("h", "a", "b")));
            Assert.assertEquals(Long.valueOf(0), Await.join(connection.pfadd("h", "a")));
            Assert.assertTrue(Await.join(connection.pfcount("h")) >= 1L);
            Assert.assertEquals("OK", Await.join(connection.pfmerge("out", "h")));
            Assert.assertTrue(Await.join(connection.pfcount(1_000, "out")) >= 1L);
            Assert.assertEquals("OK", Await.join(connection.set("s", "v")));
            assertServerError(() -> Await.join(connection.pfadd("s", "a")), "WRONGTYPE");
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void infoIsTextStatsCountersAreLongAndWrongShapeStaysUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            String info = Await.join(connection.info());
            Assert.assertTrue(info.contains("# Server"));
            Assert.assertTrue(Await.join(connection.info("server")).contains("# Server"));
            Assert.assertTrue(Await.join(connection.info(1_000)).contains("redis_version"));
            try {
                Await.join(connection.info("health"));
                Assert.fail("expected DecodeException");
            } catch (DecodeException expected) {
            }
            Assert.assertEquals("PONG", Await.join(connection.ping()));
            Map<String, Object> health = Await.join(connection.infoHealth());
            Assert.assertTrue(health.get("ready") instanceof Long);
            Assert.assertTrue(health.get("lifecycle_state") instanceof String);
            Map<String, Object> yierdis = Await.join(connection.infoYierdis());
            Assert.assertEquals("yierdis", yierdis.get("server"));
            Assert.assertTrue(yierdis.get("port") instanceof Long);

            Map<String, Object> stats = Await.join(connection.stats());
            Assert.assertTrue(stats.get("commands_executed_total") instanceof Long);
            Assert.assertTrue(stats.get("lifecycle_state") instanceof String);
            List<?> rawStats = (List<?>) Await.join(connection.command("STATS"));
            ArrayList<String> rawKeys = new ArrayList<>();
            for (int index = 0; index < rawStats.size(); index += 2) {
                rawKeys.add((String) rawStats.get(index));
            }
            Assert.assertEquals(rawKeys, new ArrayList<>(Await.join(connection.stats()).keySet()));

            Assert.assertEquals("OK", Await.join(connection.ydreconcile()));
            Assert.assertEquals("OK", Await.join(connection.ydreconcile(1_000)));
            Assert.assertEquals("PONG", Await.join(connection.ping()));
        }
    }

    @Test
    public void rawMgetIsUnknownAndConnectionStaysUsable() throws Exception {
        try (TestServer server = TestServer.start();
             Connection connection = connect(server)) {
            Await.join(connection.hset("h", "f", "v"));
            Object raw = Await.join(connection.command("HGETALL", "h"));
            Assert.assertEquals(List.of("f", "v"), raw);
            Assert.assertFalse(raw instanceof Map);
            try {
                Await.join(connection.command("MGET", "h"));
                Assert.fail("expected ServerException");
            } catch (ServerException e) {
                Assert.assertEquals("ERR unknown command 'MGET'", e.getMessage());
            }
            Assert.assertEquals("PONG", Await.join(connection.ping()));
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
