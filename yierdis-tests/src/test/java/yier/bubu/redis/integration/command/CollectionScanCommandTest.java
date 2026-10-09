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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                assertEmpty(scan(client, "HSCAN", "missing", "0"));
                assertEmpty(scan(client, "SSCAN", "missing", "0"));
                assertEmpty(scan(client, "ZSCAN", "missing", "0"));

                client.execute(cmd("SET", "string", "value"));
                assertError(client.execute(cmd("HSCAN", "string", "0")), WRONG_TYPE);
                assertError(client.execute(cmd("SSCAN", "string", "0")), WRONG_TYPE);
                assertError(client.execute(cmd("ZSCAN", "string", "0")), WRONG_TYPE);

                // 不透明 cursor：任意非负值（含 phase 位超出内部约定的值）都返回合法 scan 窗口，
                // 而不是命令错误；不存在的 key 一律回空窗口且结束 cursor 为 0。
                assertEmpty(scan(client, "HSCAN", "hash", "8589934592"));
                assertEmpty(scan(client, "SSCAN", "set", "12884901888"));
                assertEmpty(scan(client, "ZSCAN", "zset", "9223372036854775807"));
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
    public void scanMatchFollowsRedisCharacterClassesAndEmptyStars() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            List<String> members = new ArrayList<>();
            members.add("");
            members.add("!");
            members.add("a");
            members.add("]");
            members.add("b");
            members.add("x");
            for (int index = 0; index < 520; index++) {
                members.add("pad:" + index);
            }

            List<byte[]> hset = new ArrayList<>();
            hset.add(b("HSET"));
            hset.add(b("hash"));
            List<byte[]> sadd = new ArrayList<>();
            sadd.add(b("SADD"));
            sadd.add(b("set"));
            List<byte[]> zadd = new ArrayList<>();
            zadd.add(b("ZADD"));
            zadd.add(b("zset"));
            Map<String, String> hashValues = new HashMap<>();
            Map<String, String> scores = new HashMap<>();
            for (int index = 0; index < members.size(); index++) {
                String member = members.get(index);
                String hashValue = "v:" + member;
                String score = Integer.toString(index + 1);
                hashValues.put(member, hashValue);
                scores.put(member, score);
                hset.add(b(member));
                hset.add(b(hashValue));
                sadd.add(b(member));
                zadd.add(b(score));
                zadd.add(b(member));
            }
            client.execute(hset);
            client.execute(sadd);
            client.execute(zadd);

            Map<String, Set<String>> expected = Map.of(
                    "[!a]", Set.of("!", "a"),
                    "[]a]", Set.of(),
                    "[]]", Set.of(),
                    "[a-]", Set.of("]", "a"),
                    "[abc", Set.of("a", "b"),
                    "*", new HashSet<>(members),
                    "**", members.stream().filter(member -> !member.isEmpty()).collect(java.util.stream.Collectors.toSet())
            );
            for (String command : new String[]{"HSCAN", "SSCAN", "ZSCAN"}) {
                String key = switch (command) {
                    case "HSCAN" -> "hash";
                    case "SSCAN" -> "set";
                    default -> "zset";
                };
                for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
                    Assert.assertEquals(
                            command + " MATCH " + entry.getKey(),
                            entry.getValue(),
                            scanMatchingMembers(
                                    client,
                                    command,
                                    key,
                                    entry.getKey(),
                                    "HSCAN".equals(command) ? hashValues : scores
                            )
                    );
                }
            }
        });
    }

    @Test
    public void hashTableScanTerminatesAndCoversStableFields() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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
    public void collectionScansRestartFromArbitraryNonNegativeCursors() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                int fieldCount = 513;
                List<byte[]> hset = new ArrayList<>(2 + fieldCount * 2);
                hset.add(b("HSET"));
                hset.add(b("large-hash"));
                List<byte[]> sadd = new ArrayList<>(2 + fieldCount);
                sadd.add(b("SADD"));
                sadd.add(b("large-set"));
                List<byte[]> zadd = new ArrayList<>(2 + fieldCount * 2);
                zadd.add(b("ZADD"));
                zadd.add(b("large-zset"));
                for (int index = 0; index < fieldCount; index++) {
                    hset.add(b("field:" + index));
                    hset.add(b("value:" + index));
                    sadd.add(b("member:" + index));
                    zadd.add(b(Integer.toString(index)));
                    zadd.add(b("member:" + index));
                }
                client.execute(hset);
                client.execute(sadd);
                client.execute(zadd);

                // 不透明 cursor：phase 位非法值与极大值都必须按重启迭代处理（允许重复），
                // 不得报错，结束仍回 0；集合 scan 与 key SCAN 行为一致。
                for (String command : new String[]{"HSCAN", "SSCAN", "ZSCAN"}) {
                    String key = switch (command) {
                        case "HSCAN" -> "large-hash";
                        case "SSCAN" -> "large-set";
                        default -> "large-zset";
                    };
                    List<String> cursors = new ArrayList<>(List.of(
                            "8589934592",
                            "12884901888",
                            "9223372036854775807"
                    ));
                    // 伪造一个 generation 匹配但 phase 位非法的 cursor：改写该集合在线 cursor 的 phase 位。
                    String live = scan(client, command, key, "0", "COUNT", "1").cursor();
                    Assert.assertNotEquals("0", live);
                    cursors.add(Long.toString(Long.parseLong(live) | (2L << 32)));

                    for (String initial : cursors) {
                        Set<String> seen = new HashSet<>();
                        String cursor = initial;
                        int iterations = 0;
                        do {
                            ScanReply page = scan(client, command, key, cursor, "COUNT", "7");
                            seen.addAll(strings(page.elements()));
                            cursor = page.cursor();
                            Assert.assertTrue(
                                    command + " from opaque cursor " + initial + " did not terminate",
                                    ++iterations < 4096
                            );
                        } while (!"0".equals(cursor));
                        Assert.assertEquals(
                                command + " from opaque cursor " + initial + " must restart and cover every member",
                                fieldCount * ("HSCAN".equals(command) || "ZSCAN".equals(command) ? 2 : 1),
                                seen.size()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void compactEncodingsCompleteInOneCallEvenWithSmallCountHint() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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
    public void sscanTerminatesAndCoversBaseMembersWhileWritesOutpaceTheCursor() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            int baseMembers = 600;
            List<byte[]> sadd = new ArrayList<>(baseMembers + 2);
            sadd.add(b("SADD"));
            sadd.add(b("growing"));
            for (int i = 0; i < baseMembers; i++) {
                sadd.add(b("base:" + i));
            }
            client.execute(sadd);

            Set<String> seen = new HashSet<>();
            int inserted = 0;
            String cursor = "0";
            int rounds = 0;
            do {
                ScanReply reply = scan(client, "SSCAN", "growing", cursor, "COUNT", "20");
                seen.addAll(strings(reply.elements()));
                cursor = reply.cursor();
                List<byte[]> grow = new ArrayList<>(42);
                grow.add(b("SADD"));
                grow.add(b("growing"));
                for (int i = 0; i < 40; i++) {
                    grow.add(b("grow:" + inserted++));
                }
                client.execute(grow);
                Assert.assertTrue(
                        "SSCAN did not terminate while the set kept growing; inserted=" + inserted,
                        ++rounds < 1_000
                );
            } while (!"0".equals(cursor));

            for (int i = 0; i < baseMembers; i++) {
                Assert.assertTrue("SSCAN missed base member base:" + i, seen.contains("base:" + i));
            }
        });
    }

    @Test
    public void fullSscanImmediatelyAfterBulkInsertReturnsEveryMemberExactlyOnce() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            // 刚写入大集合后立刻全量扫：常落在 hashtable 扩容/双表期；严格 rehash 断言见 NativeByteMapTest。
            for (int memberCount : new int[]{200, 600}) {
                client.execute(cmd("DEL", "set"));
                List<byte[]> sadd = new ArrayList<>(memberCount + 2);
                sadd.add(b("SADD"));
                sadd.add(b("set"));
                for (int i = 0; i < memberCount; i++) {
                    sadd.add(b("m" + i));
                }
                client.execute(sadd);

                List<String> returned = new ArrayList<>();
                String cursor = "0";
                int rounds = 0;
                do {
                    ScanReply reply = scan(client, "SSCAN", "set", cursor, "COUNT", "10");
                    returned.addAll(listStrings(reply.elements()));
                    cursor = reply.cursor();
                    Assert.assertTrue("SSCAN did not terminate", ++rounds < 10_000);
                } while (!"0".equals(cursor));

                Assert.assertEquals(memberCount, new HashSet<>(returned).size());
                Assert.assertEquals(
                        "SSCAN over a quiescent set of " + memberCount + " members returned duplicates",
                        memberCount,
                        returned.size()
                );
            }
        });
    }

    @Test
    public void hscanAndZscanCursorSurvivesGrowthBetweenCalls() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            int baseFields = 520;
            List<byte[]> hset = new ArrayList<>(2 + baseFields * 2);
            hset.add(b("HSET"));
            hset.add(b("hash"));
            for (int i = 0; i < baseFields; i++) {
                hset.add(b("base:" + i));
                hset.add(b("v" + i));
            }
            client.execute(hset);

            Set<String> hashSeen = new HashSet<>();
            int hashInserted = 0;
            String hashCursor = "0";
            int hashRounds = 0;
            do {
                ScanReply reply = scan(client, "HSCAN", "hash", hashCursor, "COUNT", "20");
                hashSeen.addAll(pairs(reply.elements()).keySet());
                hashCursor = reply.cursor();
                List<byte[]> grow = new ArrayList<>(82);
                grow.add(b("HSET"));
                grow.add(b("hash"));
                for (int i = 0; i < 40; i++) {
                    grow.add(b("grow:" + hashInserted));
                    grow.add(b("v"));
                    hashInserted++;
                }
                client.execute(grow);
                Assert.assertTrue("HSCAN did not terminate while hash grew", ++hashRounds < 1_000);
            } while (!"0".equals(hashCursor));
            for (int i = 0; i < baseFields; i++) {
                Assert.assertTrue("HSCAN missed base:" + i, hashSeen.contains("base:" + i));
            }

            int baseMembers = 200;
            List<byte[]> zadd = new ArrayList<>(2 + baseMembers * 2);
            zadd.add(b("ZADD"));
            zadd.add(b("zset"));
            for (int i = 0; i < baseMembers; i++) {
                zadd.add(b(Integer.toString(i)));
                zadd.add(b("base:" + i));
            }
            client.execute(zadd);

            Set<String> zsetSeen = new HashSet<>();
            int zsetInserted = 0;
            String zsetCursor = "0";
            int zsetRounds = 0;
            do {
                ScanReply reply = scan(client, "ZSCAN", "zset", zsetCursor, "COUNT", "20");
                zsetSeen.addAll(pairs(reply.elements()).keySet());
                zsetCursor = reply.cursor();
                List<byte[]> grow = new ArrayList<>(82);
                grow.add(b("ZADD"));
                grow.add(b("zset"));
                for (int i = 0; i < 40; i++) {
                    grow.add(b(Integer.toString(1_000 + zsetInserted)));
                    grow.add(b("grow:" + zsetInserted));
                    zsetInserted++;
                }
                client.execute(grow);
                Assert.assertTrue("ZSCAN did not terminate while zset grew", ++zsetRounds < 1_000);
            } while (!"0".equals(zsetCursor));
            for (int i = 0; i < baseMembers; i++) {
                Assert.assertTrue("ZSCAN missed base:" + i, zsetSeen.contains("base:" + i));
            }
        });
    }

    @Test
    public void hugeCountRemainsABoundedHintForHashTableEncoding() {
        runDefaultFfm(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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

    private static Set<String> scanMatchingMembers(
            FastTestClient client,
            String command,
            String key,
            String pattern,
            Map<String, String> pairedValues
    ) {
        Set<String> seen = new HashSet<>();
        String cursor = "0";
        int iterations = 0;
        boolean cursorAdvanced = false;
        do {
            ScanReply page = scan(client, command, key, cursor, "MATCH", pattern, "COUNT", "7");
            if ("SSCAN".equals(command)) {
                seen.addAll(strings(page.elements()));
            } else {
                Map<String, String> pagePairs = pairs(page.elements());
                for (Map.Entry<String, String> entry : pagePairs.entrySet()) {
                    Assert.assertEquals(pairedValues.get(entry.getKey()), entry.getValue());
                }
                seen.addAll(pagePairs.keySet());
            }
            if (!"0".equals(page.cursor())) {
                cursorAdvanced = true;
            }
            cursor = page.cursor();
            Assert.assertTrue(command + " MATCH cursor did not terminate", ++iterations < 4096);
        } while (!"0".equals(cursor));
        Assert.assertTrue(command + " MATCH " + pattern + " must advance the cursor", cursorAdvanced);
        return seen;
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
        return new HashSet<>(listStrings(elements));
    }

    private static List<String> listStrings(ReplyArray elements) {
        List<String> values = new ArrayList<>(elements.values().size());
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
