package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
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
}
