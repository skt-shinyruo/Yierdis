package yier.bubu.redis.integration.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.runDefaultFfm;

public class CollectionScanCommandTest {
    private static final String WRONG_TYPE =
            "WRONGTYPE Operation against a key holding the wrong kind of value";

    @Test
    public void collectionScansImplementCursorOptionsAndRedisReplyShapes() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                assertEmpty(scan(client, "HSCAN", "missing", "0"));
                assertEmpty(scan(client, "SSCAN", "missing", "0"));
                assertEmpty(scan(client, "ZSCAN", "missing", "0"));

                client.execute(cmd("SET", "string", "value"));
                assertError(client.execute(cmd("HSCAN", "string", "0")), WRONG_TYPE);
                assertError(client.execute(cmd("SSCAN", "string", "0")), WRONG_TYPE);
                assertError(client.execute(cmd("ZSCAN", "string", "0")), WRONG_TYPE);

                assertError(
                        client.execute(cmd("HSCAN", "hash", "8589934592")),
                        "ERR value is not an integer or out of range"
                );
                assertError(
                        client.execute(cmd("SSCAN", "set", "0", "COUNT", "0")),
                        "ERR value is not an integer or out of range"
                );
                assertError(client.execute(cmd("ZSCAN", "zset", "0", "NOVALUES")), "ERR syntax error");

                client.execute(cmd(
                        "HSET", "hash",
                        "field:1", "value:1",
                        "field:2", "value:2",
                        "other", "value:3"
                ));
                ScanReply hash = scan(client, "HSCAN", "hash", "0", "MATCH", "field:*", "COUNT", "10");
                Assert.assertEquals(Map.of("field:1", "value:1", "field:2", "value:2"), pairs(hash.elements()));

                ScanReply hashNoValues = scan(
                        client,
                        "HSCAN", "hash", "0", "NOVALUES", "MATCH", "field:*", "COUNT", "10"
                );
                Assert.assertEquals(Set.of("field:1", "field:2"), strings(hashNoValues.elements()));

                client.execute(cmd("SADD", "set", "1", "2", "3", "alpha"));
                ScanReply set = scan(client, "SSCAN", "set", "0", "MATCH", "[13]", "COUNT", "10");
                Assert.assertEquals(Set.of("1", "3"), strings(set.elements()));

                String oversizedMember = "x".repeat(65);
                client.execute(cmd(
                        "ZADD", "zset",
                        "1", "member:1",
                        "2.5", "member:2",
                        "3", "other",
                        "4", oversizedMember
                ));
                ScanReply zset = scan(client, "ZSCAN", "zset", "0", "MATCH", "member:*", "COUNT", "10");
                Assert.assertEquals(Map.of("member:1", "1", "member:2", "2.5"), pairs(zset.elements()));
            }
        });
    }

    @Test
    public void hashTableScanTerminatesAndCoversStableFields() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                int fieldCount = 513;
                List<byte[]> hset = new ArrayList<>(2 + fieldCount * 2);
                hset.add(b("HSET"));
                hset.add(b("large-hash"));
                for (int index = 0; index < fieldCount; index++) {
                    hset.add(b("field:" + index));
                    hset.add(b("value:" + index));
                }
                client.execute(hset);

                Map<String, String> seen = new HashMap<>();
                String cursor = "0";
                int iterations = 0;
                do {
                    ScanReply reply = scan(client, "HSCAN", "large-hash", cursor, "COUNT", "7");
                    seen.putAll(pairs(reply.elements()));
                    cursor = reply.cursor();
                    iterations++;
                    Assert.assertTrue("HSCAN cursor did not terminate", iterations < 2048);
                } while (!"0".equals(cursor));

                Assert.assertEquals(fieldCount, seen.size());
                for (int index = 0; index < fieldCount; index++) {
                    Assert.assertEquals("value:" + index, seen.get("field:" + index));
                }
            }
        });
    }

    @Test
    public void compactEncodingsCompleteInOneCallEvenWithSmallCountHint() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("HSET", "hash", "f1", "v1", "f2", "v2", "f3", "v3"));
                ScanReply hash = scan(client, "HSCAN", "hash", "0", "COUNT", "1");
                Assert.assertEquals("0", hash.cursor());
                Assert.assertEquals(Map.of("f1", "v1", "f2", "v2", "f3", "v3"), pairs(hash.elements()));

                client.execute(cmd("SADD", "set", "1", "2", "3"));
                ScanReply set = scan(client, "SSCAN", "set", "0", "COUNT", "1");
                Assert.assertEquals("0", set.cursor());
                Assert.assertEquals(Set.of("1", "2", "3"), strings(set.elements()));

                client.execute(cmd("ZADD", "zset", "1", "a", "2", "b", "3", "c"));
                ScanReply zset = scan(client, "ZSCAN", "zset", "0", "COUNT", "1");
                Assert.assertEquals("0", zset.cursor());
                Assert.assertEquals(Map.of("a", "1", "b", "2", "c", "3"), pairs(zset.elements()));
            }
        });
    }

    @Test
    public void hashTableScansKeepCoveringPersistentElementsAcrossDeletesAndScoreUpdates() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                int memberCount = 300;
                List<byte[]> sadd = new ArrayList<>(memberCount + 2);
                sadd.add(b("SADD"));
                sadd.add(b("set"));
                List<byte[]> zadd = new ArrayList<>(memberCount * 2 + 2);
                zadd.add(b("ZADD"));
                zadd.add(b("zset"));
                Set<String> expectedSet = new HashSet<>();
                for (int index = 0; index < memberCount; index++) {
                    String member = "member:" + index;
                    sadd.add(b(member));
                    zadd.add(b(Integer.toString(index)));
                    zadd.add(b(member));
                    expectedSet.add(member);
                }
                client.execute(sadd);
                client.execute(zadd);

                ScanReply firstSetPage = scan(client, "SSCAN", "set", "0", "COUNT", "1");
                Set<String> seenSet = strings(firstSetPage.elements());
                String deleted = firstUnseenMember(seenSet, memberCount);
                client.execute(cmd("SREM", "set", deleted));
                expectedSet.remove(deleted);
                collectSetScan(client, firstSetPage.cursor(), seenSet);
                Assert.assertEquals(expectedSet, seenSet);

                ScanReply firstZsetPage = scan(client, "ZSCAN", "zset", "0", "COUNT", "1");
                Map<String, String> seenZset = pairs(firstZsetPage.elements());
                String updated = firstUnseenMember(seenZset.keySet(), memberCount);
                client.execute(cmd("ZADD", "zset", "9999", updated));
                collectZsetScan(client, firstZsetPage.cursor(), seenZset);
                Assert.assertEquals(memberCount, seenZset.size());
                Assert.assertEquals("9999", seenZset.get(updated));
            }
        });
    }

    @Test
    public void hugeCountRemainsABoundedHintForHashTableEncoding() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                int memberCount = 1_500;
                List<byte[]> sadd = new ArrayList<>(memberCount + 2);
                sadd.add(b("SADD"));
                sadd.add(b("large-set"));
                for (int index = 0; index < memberCount; index++) {
                    sadd.add(b("member:" + index));
                }
                client.execute(sadd);

                ScanReply first = scan(
                        client,
                        "SSCAN", "large-set", "0", "COUNT", Integer.toString(Integer.MAX_VALUE)
                );

                Assert.assertEquals(1_024, first.elements().values().size());
                Assert.assertNotEquals("0", first.cursor());
            }
        });
    }

    private static void collectSetScan(FastTestClient client, String initialCursor, Set<String> seen) {
        String cursor = initialCursor;
        int iterations = 0;
        while (!"0".equals(cursor)) {
            ScanReply page = scan(client, "SSCAN", "set", cursor, "COUNT", "1");
            seen.addAll(strings(page.elements()));
            cursor = page.cursor();
            Assert.assertTrue("SSCAN cursor did not terminate after mutation", ++iterations < 4096);
        }
    }

    private static void collectZsetScan(FastTestClient client, String initialCursor, Map<String, String> seen) {
        String cursor = initialCursor;
        int iterations = 0;
        while (!"0".equals(cursor)) {
            ScanReply page = scan(client, "ZSCAN", "zset", cursor, "COUNT", "1");
            seen.putAll(pairs(page.elements()));
            cursor = page.cursor();
            Assert.assertTrue("ZSCAN cursor did not terminate after score update", ++iterations < 4096);
        }
    }

    private static String firstUnseenMember(Set<String> seen, int memberCount) {
        for (int index = 0; index < memberCount; index++) {
            String candidate = "member:" + index;
            if (!seen.contains(candidate)) {
                return candidate;
            }
        }
        throw new AssertionError("expected at least one member outside the first scan page");
    }

    private static ScanReply scan(FastTestClient client, String... args) {
        ReplyObject reply = client.execute(cmd(args));
        Assert.assertTrue("expected collection scan array", reply instanceof ReplyArray);
        ReplyArray outer = (ReplyArray) reply;
        Assert.assertEquals(2, outer.values().size());
        Assert.assertTrue(outer.values().get(0) instanceof ReplyBulkString);
        Assert.assertTrue(outer.values().get(1) instanceof ReplyArray);
        return new ScanReply(
                ((ReplyBulkString) outer.values().get(0)).asString(),
                (ReplyArray) outer.values().get(1)
        );
    }

    private static Map<String, String> pairs(ReplyArray elements) {
        Assert.assertEquals(0, elements.values().size() & 1);
        Map<String, String> pairs = new HashMap<>();
        for (int index = 0; index < elements.values().size(); index += 2) {
            pairs.put(bulk(elements, index), bulk(elements, index + 1));
        }
        return pairs;
    }

    private static Set<String> strings(ReplyArray elements) {
        Set<String> values = new HashSet<>();
        for (int index = 0; index < elements.values().size(); index++) {
            values.add(bulk(elements, index));
        }
        return values;
    }

    private static String bulk(ReplyArray elements, int index) {
        ReplyObject value = elements.values().get(index);
        Assert.assertTrue(value instanceof ReplyBulkString);
        return ((ReplyBulkString) value).asString();
    }

    private static void assertEmpty(ScanReply reply) {
        Assert.assertEquals("0", reply.cursor());
        Assert.assertTrue(reply.elements().values().isEmpty());
    }

    private static void assertError(ReplyObject reply, String message) {
        Assert.assertTrue(reply instanceof ReplyError);
        Assert.assertEquals(message, ((ReplyError) reply).message());
    }

    private record ScanReply(String cursor, ReplyArray elements) {
    }
}
