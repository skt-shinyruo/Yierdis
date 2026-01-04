package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ListCommandTest {
    @Test
    public void lpopRpopCountVariantsAndDeleteWhenEmpty() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{'l', 0, (byte) 0xFF};
            byte[] a = new byte[]{0};
            byte[] second = new byte[]{1};
            byte[] c = new byte[]{2};

            RespInteger len = (RespInteger) client.execute(Arrays.asList(b("RPUSH"), key, a, second, c));
            Assert.assertEquals(3, len.value());

            RespArray popped2 = (RespArray) client.execute(Arrays.asList(b("LPOP"), key, b("2")));
            Assert.assertEquals(2, popped2.values().size());
            Assert.assertArrayEquals(a, ((RespBulkString) popped2.values().get(0)).data());
            Assert.assertArrayEquals(second, ((RespBulkString) popped2.values().get(1)).data());

            RespArray remaining = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(1, remaining.values().size());
            Assert.assertArrayEquals(c, ((RespBulkString) remaining.values().get(0)).data());

            RespArray poppedAll = (RespArray) client.execute(Arrays.asList(b("RPOP"), key, b("10")));
            Assert.assertEquals(1, poppedAll.values().size());
            Assert.assertArrayEquals(c, ((RespBulkString) poppedAll.values().get(0)).data());

            RespInteger exists = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            RespSimpleString type = (RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());

            RespArray rangeEmpty = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(rangeEmpty.values().isEmpty());
            }
        });
    }

    @Test
    public void lrangeClampsIndicesAndHandlesOutOfRange() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("mylist");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            RespArray all = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(3, all.values().size());
            Assert.assertEquals("a", ((RespBulkString) all.values().get(0)).asString());
            Assert.assertEquals("b", ((RespBulkString) all.values().get(1)).asString());
            Assert.assertEquals("c", ((RespBulkString) all.values().get(2)).asString());

            RespArray clampStop = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("10")));
            Assert.assertEquals(3, clampStop.values().size());

            RespArray tail = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("-2"), b("-1")));
            Assert.assertEquals(2, tail.values().size());
            Assert.assertEquals("b", ((RespBulkString) tail.values().get(0)).asString());
            Assert.assertEquals("c", ((RespBulkString) tail.values().get(1)).asString());

            RespArray startTooLarge = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("5"), b("10")));
            Assert.assertTrue(startTooLarge.values().isEmpty());

            RespArray startAfterStop = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("2"), b("1")));
            Assert.assertTrue(startAfterStop.values().isEmpty());

            RespArray hugeNegativeStart = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("-10"), b("-1")));
            Assert.assertEquals(3, hugeNegativeStart.values().size());
            }
        });
    }

    @Test
    public void listUpgradesAfterManyElementsAndKeepsOrder() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("big-list");
            int n = 129; // > ListValue.LISTPACK_MAX_ENTRIES

            ArrayList<byte[]> args = new ArrayList<>(2 + n);
            args.add(b("RPUSH"));
            args.add(key);
            for (int i = 0; i < n; i++) {
                args.add(b("v" + i));
            }

            RespInteger len = (RespInteger) client.execute(args);
            Assert.assertEquals(n, len.value());

            RespArray range = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertEquals(n, range.values().size());
            Assert.assertEquals("v0", ((RespBulkString) range.values().get(0)).asString());
            Assert.assertEquals("v" + (n - 1), ((RespBulkString) range.values().get(n - 1)).asString());

            RespArray popped = (RespArray) client.execute(Arrays.asList(b("LPOP"), key, b("2")));
            Assert.assertEquals(2, popped.values().size());
            Assert.assertEquals("v0", ((RespBulkString) popped.values().get(0)).asString());
            Assert.assertEquals("v1", ((RespBulkString) popped.values().get(1)).asString());

            RespBulkString last = (RespBulkString) client.execute(Arrays.asList(b("RPOP"), key));
            Assert.assertEquals("v" + (n - 1), last.asString());
            }
        });
    }

    @Test
    public void lrangeDoesNotOverflowOnHugePositiveIndices() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("list:huge-index");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            RespArray empty = (RespArray) client.execute(Arrays.asList(
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("list:huge-count");
            client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

            RespArray popped = (RespArray) client.execute(Arrays.asList(b("RPOP"), key, b("9223372036854775807")));
            Assert.assertEquals(3, popped.values().size());
            Assert.assertEquals("c", ((RespBulkString) popped.values().get(0)).asString());
            Assert.assertEquals("b", ((RespBulkString) popped.values().get(1)).asString());
            Assert.assertEquals("a", ((RespBulkString) popped.values().get(2)).asString());

            Assert.assertEquals(0L, ((RespInteger) client.execute(Arrays.asList(b("EXISTS"), key))).value());
            }
        });
    }
}
