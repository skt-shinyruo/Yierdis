package yier.bubu.redis.testutil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Assert;

import static yier.bubu.redis.testutil.TestBytes.b;

/**
 * 命令层 SCAN 族共用脚本：边写边扫覆盖基准元素，以及刚批量写入后立刻全量扫无系统性重复。
 */
public final class GrowingScanSupport {
    private GrowingScanSupport() {
    }

    public static void assertKeyScanCoversBaseWhileGrowing(FastTestClient client, String match, int baseKeys) {
        for (int i = 0; i < baseKeys; i++) {
            client.execute(List.of(b("SET"), b("base:" + i), b("v")));
        }
        Set<String> seen = runGrowingScan(client, cursor -> {
            List<byte[]> command = new ArrayList<>();
            command.add(b("SCAN"));
            command.add(b(cursor));
            if (match != null) {
                command.add(b("MATCH"));
                command.add(b(match));
            }
            command.add(b("COUNT"));
            command.add(b("20"));
            return command;
        }, false, inserted -> {
            for (int i = 0; i < 40; i++) {
                client.execute(List.of(b("SET"), b("grow:" + inserted.getAndIncrement()), b("v")));
            }
        });
        if (match == null) {
            for (int i = 0; i < baseKeys; i++) {
                Assert.assertTrue("SCAN missed base key base:" + i, seen.contains("base:" + i));
            }
        } else {
            Assert.assertEquals(baseKeys, seen.size());
        }
    }

    public static void assertSscanCoversBaseWhileGrowing(FastTestClient client, int baseMembers) {
        List<byte[]> sadd = new ArrayList<>(baseMembers + 2);
        sadd.add(b("SADD"));
        sadd.add(b("growing"));
        for (int i = 0; i < baseMembers; i++) {
            sadd.add(b("base:" + i));
        }
        client.execute(sadd);

        Set<String> seen = runGrowingScan(
                client,
                cursor -> List.of(b("SSCAN"), b("growing"), b(cursor), b("COUNT"), b("20")),
                false,
                inserted -> {
                    List<byte[]> grow = new ArrayList<>(42);
                    grow.add(b("SADD"));
                    grow.add(b("growing"));
                    for (int i = 0; i < 40; i++) {
                        grow.add(b("grow:" + inserted.getAndIncrement()));
                    }
                    client.execute(grow);
                }
        );
        for (int i = 0; i < baseMembers; i++) {
            Assert.assertTrue("SSCAN missed base member base:" + i, seen.contains("base:" + i));
        }
    }

    public static void assertHscanCoversBaseWhileGrowing(FastTestClient client, int baseFields) {
        List<byte[]> hset = new ArrayList<>(2 + baseFields * 2);
        hset.add(b("HSET"));
        hset.add(b("hash"));
        for (int i = 0; i < baseFields; i++) {
            hset.add(b("base:" + i));
            hset.add(b("v" + i));
        }
        client.execute(hset);

        Set<String> seen = runGrowingScan(
                client,
                cursor -> List.of(b("HSCAN"), b("hash"), b(cursor), b("COUNT"), b("20")),
                true,
                inserted -> {
                    List<byte[]> grow = new ArrayList<>(82);
                    grow.add(b("HSET"));
                    grow.add(b("hash"));
                    for (int i = 0; i < 40; i++) {
                        int n = inserted.getAndIncrement();
                        grow.add(b("grow:" + n));
                        grow.add(b("v"));
                    }
                    client.execute(grow);
                }
        );
        for (int i = 0; i < baseFields; i++) {
            Assert.assertTrue("HSCAN missed base:" + i, seen.contains("base:" + i));
        }
    }

    public static void assertZscanCoversBaseWhileGrowing(FastTestClient client, int baseMembers) {
        List<byte[]> zadd = new ArrayList<>(2 + baseMembers * 2);
        zadd.add(b("ZADD"));
        zadd.add(b("zset"));
        for (int i = 0; i < baseMembers; i++) {
            zadd.add(b(Integer.toString(i)));
            zadd.add(b("base:" + i));
        }
        client.execute(zadd);

        Set<String> seen = runGrowingScan(
                client,
                cursor -> List.of(b("ZSCAN"), b("zset"), b(cursor), b("COUNT"), b("20")),
                true,
                inserted -> {
                    List<byte[]> grow = new ArrayList<>(82);
                    grow.add(b("ZADD"));
                    grow.add(b("zset"));
                    for (int i = 0; i < 40; i++) {
                        int n = inserted.getAndIncrement();
                        grow.add(b(Integer.toString(1_000 + n)));
                        grow.add(b("grow:" + n));
                    }
                    client.execute(grow);
                }
        );
        for (int i = 0; i < baseMembers; i++) {
            Assert.assertTrue("ZSCAN missed base:" + i, seen.contains("base:" + i));
        }
    }

    /**
     * 刚批量写入后立刻全量 SCAN：外部契约是无遗漏、无系统性重复。
     * 双表期严格钉住见 {@code OpenAddressingTopologyCompatibilityTest.fullScanWhileGrowingRehashReturnsEachLiveHashExactlyOnce}。
     */
    public static void assertFullKeyScanAfterBulkInsertReturnsEachKeyExactlyOnce(
            FastTestClient client,
            int... keyCounts
    ) {
        for (int keyCount : keyCounts) {
            client.execute(List.of(b("FLUSHALL")));
            for (int i = 0; i < keyCount; i++) {
                client.execute(List.of(b("SET"), b("k" + i), b("v")));
            }
            List<String> returned = collectFullScan(
                    client,
                    cursor -> List.of(b("SCAN"), b(cursor), b("COUNT"), b("10")),
                    false
            );
            Assert.assertEquals(keyCount, new HashSet<>(returned).size());
            Assert.assertEquals(
                    "SCAN over a quiescent keyspace of " + keyCount + " keys returned duplicates",
                    keyCount,
                    returned.size()
            );
        }
    }

    /**
     * 刚写入大 set 后立刻全量 SSCAN：外部契约同上；hashtable 双表期严格断言见 topology / NativeByteMap 单测。
     */
    public static void assertFullSscanAfterBulkInsertReturnsEachMemberExactlyOnce(
            FastTestClient client,
            int... memberCounts
    ) {
        for (int memberCount : memberCounts) {
            client.execute(List.of(b("DEL"), b("set")));
            List<byte[]> sadd = new ArrayList<>(memberCount + 2);
            sadd.add(b("SADD"));
            sadd.add(b("set"));
            for (int i = 0; i < memberCount; i++) {
                sadd.add(b("m" + i));
            }
            client.execute(sadd);
            List<String> returned = collectFullScan(
                    client,
                    cursor -> List.of(b("SSCAN"), b("set"), b(cursor), b("COUNT"), b("10")),
                    false
            );
            Assert.assertEquals(memberCount, new HashSet<>(returned).size());
            Assert.assertEquals(
                    "SSCAN over a quiescent set of " + memberCount + " members returned duplicates",
                    memberCount,
                    returned.size()
            );
        }
    }

    private static Set<String> runGrowingScan(
            FastTestClient client,
            Function<String, List<byte[]>> commandForCursor,
            boolean pairElements,
            Consumer<AtomicInteger> growAfterRound
    ) {
        Set<String> seen = new HashSet<>();
        AtomicInteger inserted = new AtomicInteger();
        String cursor = "0";
        int rounds = 0;
        do {
            ReplyArray reply = (ReplyArray) client.execute(commandForCursor.apply(cursor));
            collectElements(seen, (ReplyArray) reply.values().get(1), pairElements);
            cursor = ((ReplyBulkString) reply.values().get(0)).asString();
            growAfterRound.accept(inserted);
            Assert.assertTrue(
                    "scan did not terminate while writes outpaced the cursor; inserted=" + inserted.get(),
                    ++rounds < 1_000
            );
        } while (!"0".equals(cursor));
        return seen;
    }

    private static List<String> collectFullScan(
            FastTestClient client,
            Function<String, List<byte[]>> commandForCursor,
            boolean pairElements
    ) {
        List<String> returned = new ArrayList<>();
        String cursor = "0";
        int rounds = 0;
        do {
            ReplyArray reply = (ReplyArray) client.execute(commandForCursor.apply(cursor));
            collectElements(returned, (ReplyArray) reply.values().get(1), pairElements);
            cursor = ((ReplyBulkString) reply.values().get(0)).asString();
            Assert.assertTrue("scan did not terminate after bulk insert", ++rounds < 10_000);
        } while (!"0".equals(cursor));
        return returned;
    }

    private static void collectElements(java.util.Collection<String> into, ReplyArray elements, boolean pairElements) {
        if (pairElements) {
            for (int index = 0; index < elements.values().size(); index += 2) {
                into.add(((ReplyBulkString) elements.values().get(index)).asString());
            }
        } else {
            for (ReplyObject element : elements.values()) {
                into.add(((ReplyBulkString) element).asString());
            }
        }
    }
}
