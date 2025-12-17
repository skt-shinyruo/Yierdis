package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class CommandProcessorTest {
    @Test
    public void stringIsBinarySafe() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[]{0, (byte) 0xFF, 'a', '\n'};

        Assert.assertTrue(cp.execute(Arrays.asList(
                "SET".getBytes(StandardCharsets.UTF_8),
                key,
                value
        )) instanceof RespSimpleString);

        RespObject get = cp.execute(Arrays.asList(
                "GET".getBytes(StandardCharsets.UTF_8),
                key
        ));
        Assert.assertTrue(get instanceof RespBulkString);
        Assert.assertArrayEquals(value, ((RespBulkString) get).data());

        RespInteger len = (RespInteger) cp.execute(Arrays.asList(
                "STRLEN".getBytes(StandardCharsets.UTF_8),
                key
        ));
        Assert.assertEquals(value.length, len.value());

        byte[] extra = new byte[]{1, 2, 3};
        RespInteger newLen = (RespInteger) cp.execute(Arrays.asList(
                "APPEND".getBytes(StandardCharsets.UTF_8),
                key,
                extra
        ));
        Assert.assertEquals(value.length + extra.length, newLen.value());

        RespObject get2 = cp.execute(Arrays.asList(
                "GET".getBytes(StandardCharsets.UTF_8),
                key
        ));
        Assert.assertTrue(get2 instanceof RespBulkString);
        Assert.assertArrayEquals(new byte[]{0, (byte) 0xFF, 'a', '\n', 1, 2, 3}, ((RespBulkString) get2).data());

        db.shutdown();
    }

    @Test
    public void setGetIncrExpireTtl() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "a", "1")) instanceof RespSimpleString);
        RespObject get = cp.execute(cmd("GET", "a"));
        Assert.assertEquals("1", ((RespBulkString) get).asString());

        RespObject incr = cp.execute(cmd("INCR", "a"));
        Assert.assertEquals(2, ((RespInteger) incr).value());

        RespObject expire = cp.execute(cmd("EXPIRE", "a", "10"));
        Assert.assertEquals(1, ((RespInteger) expire).value());

        RespObject ttl = cp.execute(cmd("TTL", "a"));
        Assert.assertTrue(((RespInteger) ttl).value() >= 0);

        db.shutdown();
    }

    @Test
    public void setNxReturnsNilWhenKeyExists() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "k", "v")) instanceof RespSimpleString);
        RespObject res = cp.execute(cmd("SET", "k", "v2", "NX"));
        Assert.assertTrue(res instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) res).isNull());

        db.shutdown();
    }

    @Test
    public void listCommands() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        RespInteger len = (RespInteger) cp.execute(cmd("LPUSH", "mylist", "a", "b"));
        Assert.assertEquals(2, len.value());

        RespArray range = (RespArray) cp.execute(cmd("LRANGE", "mylist", "0", "-1"));
        Assert.assertEquals(2, range.values().size());

        RespObject pop = cp.execute(cmd("LPOP", "mylist"));
        Assert.assertTrue(pop instanceof RespBulkString);

        db.shutdown();
    }

    @Test
    public void setCommands() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        RespInteger added = (RespInteger) cp.execute(cmd("SADD", "s", "a", "b", "c"));
        Assert.assertEquals(3, added.value());

        RespInteger removed = (RespInteger) cp.execute(cmd("SREM", "s", "b", "x"));
        Assert.assertEquals(1, removed.value());

        RespInteger card = (RespInteger) cp.execute(cmd("SCARD", "s"));
        Assert.assertEquals(2, card.value());

        RespInteger isMember = (RespInteger) cp.execute(cmd("SISMEMBER", "s", "b"));
        Assert.assertEquals(0, isMember.value());

        RespArray members = (RespArray) cp.execute(cmd("SMEMBERS", "s"));
        Assert.assertEquals(2, members.values().size());

        db.shutdown();
    }

    @Test
    public void zsetCommands() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        RespInteger added = (RespInteger) cp.execute(cmd("ZADD", "myzset", "1", "b", "1", "a", "2", "c"));
        Assert.assertEquals(3, added.value());

        RespArray range = (RespArray) cp.execute(cmd("ZRANGE", "myzset", "0", "-1"));
        Assert.assertEquals(3, range.values().size());
        Assert.assertEquals("a", ((RespBulkString) range.values().get(0)).asString());
        Assert.assertEquals("b", ((RespBulkString) range.values().get(1)).asString());
        Assert.assertEquals("c", ((RespBulkString) range.values().get(2)).asString());

        RespInteger updated = (RespInteger) cp.execute(cmd("ZADD", "myzset", "0.5", "b"));
        Assert.assertEquals(0, updated.value());

        RespArray range2 = (RespArray) cp.execute(cmd("ZRANGE", "myzset", "0", "-1"));
        Assert.assertEquals("b", ((RespBulkString) range2.values().get(0)).asString());
        Assert.assertEquals("a", ((RespBulkString) range2.values().get(1)).asString());
        Assert.assertEquals("c", ((RespBulkString) range2.values().get(2)).asString());

        RespArray withScores = (RespArray) cp.execute(cmd("ZRANGE", "myzset", "0", "1", "WITHSCORES"));
        Assert.assertEquals(4, withScores.values().size());
        Assert.assertEquals("b", ((RespBulkString) withScores.values().get(0)).asString());
        Assert.assertEquals("0.5", ((RespBulkString) withScores.values().get(1)).asString());
        Assert.assertEquals("a", ((RespBulkString) withScores.values().get(2)).asString());
        Assert.assertEquals("1", ((RespBulkString) withScores.values().get(3)).asString());

        RespInteger zrem = (RespInteger) cp.execute(cmd("ZREM", "myzset", "a", "x"));
        Assert.assertEquals(1, zrem.value());

        RespArray range3 = (RespArray) cp.execute(cmd("ZRANGE", "myzset", "0", "-1"));
        Assert.assertEquals(2, range3.values().size());
        Assert.assertEquals("b", ((RespBulkString) range3.values().get(0)).asString());
        Assert.assertEquals("c", ((RespBulkString) range3.values().get(1)).asString());

        db.shutdown();
    }

    @Test
    public void wrongTypeReturnsError() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        cp.execute(cmd("SET", "a", "1"));
        RespObject err = cp.execute(cmd("LPUSH", "a", "x"));
        Assert.assertTrue(err instanceof RespError);
        Assert.assertTrue(((RespError) err).message().startsWith("WRONGTYPE"));

        db.shutdown();
    }

    private static java.util.List<byte[]> cmd(String... parts) {
        java.util.List<byte[]> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            out.add(p.getBytes(StandardCharsets.UTF_8));
        }
        return out;
    }
}
