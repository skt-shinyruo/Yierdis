package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyMap;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class EmptyBulkStringCommandTest {
    @Test
    public void smembersAllowsEmptyMember() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("s-empty");
                byte[] empty = b("");

                ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("SADD"), key, empty));
                Assert.assertEquals(1, added.value());

                ReplyArray members = (ReplyArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
                Assert.assertTrue(containsBulkString(members, empty));
            }
        });
    }

    @Test
    public void hgetallAllowsEmptyField() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("h-empty");
                byte[] field = b("");
                byte[] value = b("v");

                ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("HSET"), key, field, value));
                Assert.assertEquals(1, added.value());

                ReplyMap all = (ReplyMap) client.execute(Arrays.asList(b("HGETALL"), key));
                Assert.assertEquals(1, all.entries().size());
                assertContainsPair(all, field, value);
            }
        });
    }

    @Test
    public void zrangeAllowsEmptyMemberWithScores() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("z-empty");
                byte[] empty = b("");

                ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), empty));
                Assert.assertEquals(1, added.value());

                ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
                Assert.assertEquals(1, range.values().size());
                Assert.assertTrue(range.values().get(0) instanceof ReplyBulkString);
                Assert.assertArrayEquals(empty, ((ReplyBulkString) range.values().get(0)).data());

                ReplyArray withScores = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1"), b("WITHSCORES")));
                Assert.assertEquals(2, withScores.values().size());
                Assert.assertArrayEquals(empty, ((ReplyBulkString) withScores.values().get(0)).data());
                Assert.assertEquals("1", ((ReplyBulkString) withScores.values().get(1)).asString());
            }
        });
    }

    private static boolean containsBulkString(ReplyArray array, byte[] expected) {
        for (ReplyObject o : array.values()) {
            if (o instanceof ReplyBulkString && Arrays.equals(expected, ((ReplyBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }

    private static void assertContainsPair(ReplyMap map, byte[] field, byte[] value) {
        for (ReplyMap.Entry e : map.entries()) {
            ReplyObject k = e.key();
            ReplyObject v = e.value();
            if (!(k instanceof ReplyBulkString) || !(v instanceof ReplyBulkString)) {
                continue;
            }
            if (Arrays.equals(field, ((ReplyBulkString) k).data()) && Arrays.equals(value, ((ReplyBulkString) v).data())) {
                return;
            }
        }
        Assert.fail("HGETALL 返回缺少预期 field/value pair");
    }
}

