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

import java.util.Arrays;

public class CommandProcessorTest {
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
    public void wrongTypeReturnsError() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        cp.execute(cmd("SET", "a", "1"));
        RespObject err = cp.execute(cmd("LPUSH", "a", "x"));
        Assert.assertTrue(err instanceof RespError);
        Assert.assertTrue(((RespError) err).message().startsWith("WRONGTYPE"));

        db.shutdown();
    }

    private static java.util.List<String> cmd(String... parts) {
        return Arrays.asList(parts);
    }
}
