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

import java.util.ArrayList;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

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

        RespArray stopHuge = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("9223372036854775807")));
        Assert.assertEquals(4, stopHuge.values().size());

        RespArray startHugeNegative = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("-9223372036854775808"), b("-1")));
        Assert.assertEquals(4, startHugeNegative.values().size());

        RespArray startAfterStop = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("2"), b("1")));
        Assert.assertTrue(startAfterStop.values().isEmpty());

        db.shutdown();
    }

    @Test
    public void zrevrangeAndZrangeRevReturnReverseOrder() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("zrev");

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

        RespArray rev = (RespArray) cp.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(4, rev.values().size());
        Assert.assertArrayEquals(m4, ((RespBulkString) rev.values().get(0)).data());
        Assert.assertArrayEquals(m3, ((RespBulkString) rev.values().get(1)).data());
        Assert.assertArrayEquals(m2, ((RespBulkString) rev.values().get(2)).data());
        Assert.assertArrayEquals(m1, ((RespBulkString) rev.values().get(3)).data());

        RespArray revViaZrange = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1"), b("REV")));
        Assert.assertEquals(4, revViaZrange.values().size());
        Assert.assertArrayEquals(m4, ((RespBulkString) revViaZrange.values().get(0)).data());
        Assert.assertArrayEquals(m3, ((RespBulkString) revViaZrange.values().get(1)).data());
        Assert.assertArrayEquals(m2, ((RespBulkString) revViaZrange.values().get(2)).data());
        Assert.assertArrayEquals(m1, ((RespBulkString) revViaZrange.values().get(3)).data());

        RespArray revWithScores = (RespArray) cp.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("1"), b("WITHSCORES")));
        Assert.assertEquals(4, revWithScores.values().size());
        Assert.assertArrayEquals(m4, ((RespBulkString) revWithScores.values().get(0)).data());
        Assert.assertEquals("1", ((RespBulkString) revWithScores.values().get(1)).asString());
        Assert.assertArrayEquals(m3, ((RespBulkString) revWithScores.values().get(2)).data());
        Assert.assertEquals("1", ((RespBulkString) revWithScores.values().get(3)).asString());

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

    @Test
    public void zsetUpgradesAfterManyElementsAndKeepsOrder() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("big-zset");
        int n = 129; // > ZSetValue.LISTPACK_MAX_ENTRIES

        ArrayList<byte[]> args = new ArrayList<>(2 + n * 2);
        args.add(b("ZADD"));
        args.add(key);
        for (int i = 0; i < n; i++) {
            args.add(b(Integer.toString(i))); // score
            args.add(b(String.format("m%03d", i))); // member
        }

        RespInteger added = (RespInteger) cp.execute(args);
        Assert.assertEquals(n, added.value());

        RespArray range = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(n, range.values().size());
        Assert.assertEquals("m000", ((RespBulkString) range.values().get(0)).asString());
        Assert.assertEquals("m128", ((RespBulkString) range.values().get(n - 1)).asString());

        RespArray rev = (RespArray) cp.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("1")));
        Assert.assertEquals(2, rev.values().size());
        Assert.assertEquals("m128", ((RespBulkString) rev.values().get(0)).asString());
        Assert.assertEquals("m127", ((RespBulkString) rev.values().get(1)).asString());

        db.shutdown();
    }

    @Test
    public void zsetUpgradesWhenMemberIsTooLargeForListpack() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = b("zset:big-member");
        byte[] small = b("a");
        byte[] big = new byte[65];
        Arrays.fill(big, (byte) 'x');

        RespInteger added = (RespInteger) cp.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), big,
                b("0"), small
        ));
        Assert.assertEquals(2, added.value());

        RespArray range = (RespArray) cp.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(2, range.values().size());
        Assert.assertArrayEquals(small, ((RespBulkString) range.values().get(0)).data());
        Assert.assertArrayEquals(big, ((RespBulkString) range.values().get(1)).data());

        db.shutdown();
    }
}
