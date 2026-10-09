package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ListCommandTest {
    @Test
    public void lpopRpopCountVariantsAndDeleteWhenEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = new byte[]{'l', 0, (byte) 0xFF};
            byte[] a = new byte[]{0};
            byte[] second = new byte[]{1};
            byte[] c = new byte[]{2};

            ReplyInteger len = (ReplyInteger) client.execute(Arrays.asList(b("RPUSH"), key, a, second, c));
            Assert.assertEquals(3, len.value());

            ReplyArray popped2 = (ReplyArray) client.execute(Arrays.asList(b("LPOP"), key, b("2")));
            Assert.assertEquals(2, popped2.values().size());
            Assert.assertArrayEquals(a, ((ReplyBulkString) popped2.values().get(0)).data());
            Assert.assertArrayEquals(second, ((ReplyBulkString) popped2.values().get(1)).data());

            ReplyArray remaining = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(1, remaining.values().size());
            Assert.assertArrayEquals(c, ((ReplyBulkString) remaining.values().get(0)).data());

            ReplyArray poppedAll = (ReplyArray) client.execute(Arrays.asList(b("RPOP"), key, b("10")));
            Assert.assertEquals(1, poppedAll.values().size());
            Assert.assertArrayEquals(c, ((ReplyBulkString) poppedAll.values().get(0)).data());

            ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            ReplySimpleString type = (ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());

            ReplyArray rangeEmpty = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(rangeEmpty.values().isEmpty());
            }
        });
    }

    @Test
    public void lrangeClampsIndicesAndHandlesOutOfRange() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("mylist");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            ReplyArray all = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(3, all.values().size());
            Assert.assertEquals("a", ((ReplyBulkString) all.values().get(0)).asString());
            Assert.assertEquals("b", ((ReplyBulkString) all.values().get(1)).asString());
            Assert.assertEquals("c", ((ReplyBulkString) all.values().get(2)).asString());

            ReplyArray clampStop = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("10")));
            Assert.assertEquals(3, clampStop.values().size());

            ReplyArray tail = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("-2"), b("-1")));
            Assert.assertEquals(2, tail.values().size());
            Assert.assertEquals("b", ((ReplyBulkString) tail.values().get(0)).asString());
            Assert.assertEquals("c", ((ReplyBulkString) tail.values().get(1)).asString());

            ReplyArray startTooLarge = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("5"), b("10")));
            Assert.assertTrue(startTooLarge.values().isEmpty());

            ReplyArray startAfterStop = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("2"), b("1")));
            Assert.assertTrue(startAfterStop.values().isEmpty());

            ReplyArray hugeNegativeStart = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("-10"), b("-1")));
            Assert.assertEquals(3, hugeNegativeStart.values().size());
            }
        });
    }

    @Test
    public void listUpgradesAfterManyElementsAndKeepsOrder() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("big-list");
            int n = 129; // > ListValue.LISTPACK_MAX_ENTRIES

            ArrayList<byte[]> args = new ArrayList<>(2 + n);
            args.add(b("RPUSH"));
            args.add(key);
            for (int i = 0; i < n; i++) {
                args.add(b("v" + i));
            }

            ReplyInteger len = (ReplyInteger) client.execute(args);
            Assert.assertEquals(n, len.value());

            ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(n, range.values().size());
            Assert.assertEquals("v0", ((ReplyBulkString) range.values().get(0)).asString());
            Assert.assertEquals("v" + (n - 1), ((ReplyBulkString) range.values().get(n - 1)).asString());

            ReplyArray popped = (ReplyArray) client.execute(Arrays.asList(b("LPOP"), key, b("2")));
            Assert.assertEquals(2, popped.values().size());
            Assert.assertEquals("v0", ((ReplyBulkString) popped.values().get(0)).asString());
            Assert.assertEquals("v1", ((ReplyBulkString) popped.values().get(1)).asString());

            ReplyBulkString last = (ReplyBulkString) client.execute(Arrays.asList(b("RPOP"), key));
            Assert.assertEquals("v" + (n - 1), last.asString());
            }
        });
    }

    @Test
    public void lrangeDoesNotOverflowOnHugePositiveIndices() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("list:huge-index");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            ReplyArray empty = (ReplyArray) client.execute(Arrays.asList(
                    b("LRANGE"), key,
                    b("9223372036854775807"), b("9223372036854775807")
            ));
            Assert.assertTrue(empty.values().isEmpty());
            }
        });
    }

    @Test
    public void lpopRpopRejectNegativeCountWithPositiveRangeError() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            client.execute(Arrays.asList(b("RPUSH"), b("list"), b("a")));

            client.execute(Arrays.asList(b("SET"), b("string"), b("v")));

            // Redis 8.9.241 的 getPositiveLongFromObjectOrReply 把非整数和负数都报成 "must be positive"，
            // 而且 count 先于查 key 解析，所以缺 key 和非 list key 也是这句。
            for (String command : List.of("LPOP", "RPOP")) {
                for (String key : List.of("list", "missing", "string")) {
                    for (String count : List.of("-1", "abc", "1.5", "", "-0", "+1", "007", " 1",
                            "9223372036854775808", "-9223372036854775808")) {
                        ReplyError failure = (ReplyError) client.execute(Arrays.asList(b(command), b(key), b(count)));
                        Assert.assertEquals(command + " " + key + " " + count,
                                "ERR value is out of range, must be positive", failure.message());
                    }
                }
            }
            Assert.assertEquals(1, ((ReplyArray) client.execute(
                    Arrays.asList(b("LRANGE"), b("list"), b("0"), b("-1")))).values().size());
            }
        });
    }

    @Test
    public void lpopRpopDoNotOverflowOnHugeCount() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("list:huge-count");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            ReplyArray popped = (ReplyArray) client.execute(Arrays.asList(b("RPOP"), key, b("9223372036854775807")));
            Assert.assertEquals(3, popped.values().size());
            Assert.assertEquals("c", ((ReplyBulkString) popped.values().get(0)).asString());
            Assert.assertEquals("b", ((ReplyBulkString) popped.values().get(1)).asString());
            Assert.assertEquals("a", ((ReplyBulkString) popped.values().get(2)).asString());

            Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key))).value());
            }
        });
    }

    @Test
    public void lpopRpopCountZeroChecksExistenceAndType() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            ReplyObject missingWithCount = client.execute(Arrays.asList(b("LPOP"), b("missing"), b("2")));
            Assert.assertTrue(missingWithCount instanceof ReplyNullArray);
            Assert.assertSame(missingWithCount, client.execute(Arrays.asList(b("LPOP"), b("missing"), b("0"))));
            Assert.assertSame(missingWithCount, client.execute(Arrays.asList(b("RPOP"), b("missing"), b("0"))));

            client.execute(Arrays.asList(b("SET"), b("str"), b("v")));
            assertWrongType(client.execute(Arrays.asList(b("LPOP"), b("str"), b("0"))));
            assertWrongType(client.execute(Arrays.asList(b("RPOP"), b("str"), b("0"))));

            client.execute(Arrays.asList(b("SADD"), b("set"), b("v")));
            assertWrongType(client.execute(Arrays.asList(b("LPOP"), b("set"), b("0"))));
            assertWrongType(client.execute(Arrays.asList(b("RPOP"), b("set"), b("0"))));

            client.execute(Arrays.asList(b("ZADD"), b("zset"), b("1"), b("v")));
            assertWrongType(client.execute(Arrays.asList(b("LPOP"), b("zset"), b("0"))));
            assertWrongType(client.execute(Arrays.asList(b("RPOP"), b("zset"), b("0"))));

            byte[] key = b("list");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b")));
            ReplyArray lpopZero = (ReplyArray) client.execute(Arrays.asList(b("LPOP"), key, b("0")));
            Assert.assertTrue(lpopZero.values().isEmpty());
            ReplyArray rpopZero = (ReplyArray) client.execute(Arrays.asList(b("RPOP"), key, b("0")));
            Assert.assertTrue(rpopZero.values().isEmpty());

            ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(2, range.values().size());
            Assert.assertEquals("a", ((ReplyBulkString) range.values().get(0)).asString());
            Assert.assertEquals("b", ((ReplyBulkString) range.values().get(1)).asString());
            }
        });
    }

    private static void assertWrongType(ReplyObject reply) {
        ReplyError error = (ReplyError) reply;
        Assert.assertEquals(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
                error.message()
        );
    }
}
