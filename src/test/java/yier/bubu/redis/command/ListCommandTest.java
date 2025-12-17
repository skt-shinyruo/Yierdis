package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ListCommandTest {
    @Test
    public void lpopRpopCountVariantsAndDeleteWhenEmpty() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = new byte[]{'l', 0, (byte) 0xFF};
        byte[] a = new byte[]{0};
        byte[] second = new byte[]{1};
        byte[] c = new byte[]{2};

        RespInteger len = (RespInteger) cp.execute(Arrays.asList(b("RPUSH"), key, a, second, c));
        Assert.assertEquals(3, len.value());

        RespArray popped2 = (RespArray) cp.execute(Arrays.asList(b("LPOP"), key, b("2")));
        Assert.assertEquals(2, popped2.values().size());
        Assert.assertArrayEquals(a, ((RespBulkString) popped2.values().get(0)).data());
        Assert.assertArrayEquals(second, ((RespBulkString) popped2.values().get(1)).data());

        RespArray remaining = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(1, remaining.values().size());
        Assert.assertArrayEquals(c, ((RespBulkString) remaining.values().get(0)).data());

        RespArray poppedAll = (RespArray) cp.execute(Arrays.asList(b("RPOP"), key, b("10")));
        Assert.assertEquals(1, poppedAll.values().size());
        Assert.assertArrayEquals(c, ((RespBulkString) poppedAll.values().get(0)).data());

        RespInteger exists = (RespInteger) cp.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

        RespSimpleString type = (RespSimpleString) cp.execute(Arrays.asList(b("TYPE"), key));
        Assert.assertEquals("none", type.value());

        RespArray rangeEmpty = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
        Assert.assertTrue(rangeEmpty.values().isEmpty());

        db.shutdown();
    }

    @Test
    public void lrangeClampsIndicesAndHandlesOutOfRange() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("mylist");
        cp.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b"), b("c")));

        RespArray all = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(3, all.values().size());
        Assert.assertEquals("a", ((RespBulkString) all.values().get(0)).asString());
        Assert.assertEquals("b", ((RespBulkString) all.values().get(1)).asString());
        Assert.assertEquals("c", ((RespBulkString) all.values().get(2)).asString());

        RespArray clampStop = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("10")));
        Assert.assertEquals(3, clampStop.values().size());

        RespArray tail = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("-2"), b("-1")));
        Assert.assertEquals(2, tail.values().size());
        Assert.assertEquals("b", ((RespBulkString) tail.values().get(0)).asString());
        Assert.assertEquals("c", ((RespBulkString) tail.values().get(1)).asString());

        RespArray startTooLarge = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("5"), b("10")));
        Assert.assertTrue(startTooLarge.values().isEmpty());

        RespArray startAfterStop = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("2"), b("1")));
        Assert.assertTrue(startAfterStop.values().isEmpty());

        RespArray hugeNegativeStart = (RespArray) cp.execute(Arrays.asList(b("LRANGE"), key, b("-10"), b("-1")));
        Assert.assertEquals(3, hugeNegativeStart.values().size());

        db.shutdown();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
