package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ZSetCommandTest {
    @Test
    public void zaddRejectsInvalidScores() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("z");

        RespObject err1 = cp.execute(Arrays.asList(b("ZADD"), key, b("NaN"), b("a")));
        Assert.assertTrue(err1 instanceof RespError);
        Assert.assertEquals("ERR value is not a valid float", ((RespError) err1).message());

        RespObject err2 = cp.execute(Arrays.asList(b("ZADD"), key, b("Infinity"), b("a")));
        Assert.assertTrue(err2 instanceof RespError);
        Assert.assertEquals("ERR value is not a valid float", ((RespError) err2).message());

        RespObject err3 = cp.execute(Arrays.asList(b("ZADD"), key, b("nope"), b("a")));
        Assert.assertTrue(err3 instanceof RespError);
        Assert.assertEquals("ERR value is not a valid float", ((RespError) err3).message());

        db.shutdown();
    }

    @Test
    public void zrangeTieBreakIsRawByteLexAndBoundsWork() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = new byte[]{'z', 0};

        byte[] m1 = new byte[]{0};
        byte[] m2 = new byte[]{0, 0};
        byte[] m3 = new byte[]{0, 1};
        byte[] m4 = new byte[]{(byte) 0xFF};

        RespInteger added = (RespInteger) cp.execute(Arrays.asList(
                b("ZADD"),
                key,
                b("1"), m4,
                b("1"), m3,
                b("1"), m2,
                b("1"), m1
        ));
        Assert.assertEquals(4, added.value());

        RespArray all = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(4, all.values().size());
        Assert.assertArrayEquals(m1, ((RespBulkString) all.values().get(0)).data());
        Assert.assertArrayEquals(m2, ((RespBulkString) all.values().get(1)).data());
        Assert.assertArrayEquals(m3, ((RespBulkString) all.values().get(2)).data());
        Assert.assertArrayEquals(m4, ((RespBulkString) all.values().get(3)).data());

        RespArray withScores = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("1"), b("WITHSCORES")));
        Assert.assertEquals(4, withScores.values().size());
        Assert.assertArrayEquals(m1, ((RespBulkString) withScores.values().get(0)).data());
        Assert.assertEquals("1", ((RespBulkString) withScores.values().get(1)).asString());
        Assert.assertArrayEquals(m2, ((RespBulkString) withScores.values().get(2)).data());
        Assert.assertEquals("1", ((RespBulkString) withScores.values().get(3)).asString());

        RespArray startTooLarge = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("10"), b("20")));
        Assert.assertTrue(startTooLarge.values().isEmpty());

        RespArray startAfterStop = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("2"), b("1")));
        Assert.assertTrue(startAfterStop.values().isEmpty());

        db.shutdown();
    }

    @Test
    public void zremDeletesKeyWhenEmpty() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = new byte[]{0, 'z'};
        byte[] member = new byte[]{0, 1, 2};

        RespInteger added = (RespInteger) cp.execute(Arrays.asList(b("ZADD"), key, b("1"), member));
        Assert.assertEquals(1, added.value());

        RespInteger removed = (RespInteger) cp.execute(Arrays.asList(b("ZREM"), key, member));
        Assert.assertEquals(1, removed.value());

        RespInteger exists = (RespInteger) cp.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

        RespSimpleString type = (RespSimpleString) cp.execute(Arrays.asList(b("TYPE"), key));
        Assert.assertEquals("none", type.value());

        db.shutdown();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

