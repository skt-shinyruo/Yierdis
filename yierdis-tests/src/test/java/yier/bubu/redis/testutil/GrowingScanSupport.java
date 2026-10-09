package yier.bubu.redis.testutil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.Assert;

import static yier.bubu.redis.testutil.TestBytes.b;

/**
 * 命令层「边写边扫仍能结束并覆盖基准元素」共用脚本，避免 SCAN / SSCAN / HSCAN / ZSCAN 各写一套。
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
                client.execute(List.of(b("SET"), b("grow:" + inserted[0]++), b("v")));
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
                        grow.add(b("grow:" + inserted[0]++));
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
                        grow.add(b("grow:" + inserted[0]));
                        grow.add(b("v"));
                        inserted[0]++;
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
                        grow.add(b(Integer.toString(1_000 + inserted[0])));
                        grow.add(b("grow:" + inserted[0]));
                        inserted[0]++;
                    }
                    client.execute(grow);
                }
        );
        for (int i = 0; i < baseMembers; i++) {
            Assert.assertTrue("ZSCAN missed base:" + i, seen.contains("base:" + i));
        }
    }

    private static Set<String> runGrowingScan(
            FastTestClient client,
            java.util.function.Function<String, List<byte[]>> commandForCursor,
            boolean pairElements,
            Consumer<int[]> growAfterRound
    ) {
        Set<String> seen = new HashSet<>();
        int[] inserted = {0};
        String cursor = "0";
        int rounds = 0;
        do {
            ReplyArray reply = (ReplyArray) client.execute(commandForCursor.apply(cursor));
            ReplyArray elements = (ReplyArray) reply.values().get(1);
            if (pairElements) {
                for (int index = 0; index < elements.values().size(); index += 2) {
                    seen.add(((ReplyBulkString) elements.values().get(index)).asString());
                }
            } else {
                for (ReplyObject element : elements.values()) {
                    seen.add(((ReplyBulkString) element).asString());
                }
            }
            cursor = ((ReplyBulkString) reply.values().get(0)).asString();
            growAfterRound.accept(inserted);
            Assert.assertTrue(
                    "scan did not terminate while writes outpaced the cursor; inserted=" + inserted[0],
                    ++rounds < 1_000
            );
        } while (!"0".equals(cursor));
        return seen;
    }
}
