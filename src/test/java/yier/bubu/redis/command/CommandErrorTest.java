package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class CommandErrorTest {
    @Test
    public void wrongTypeOnGetWhenKeyHoldsNonString() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] listKey = b("l");
        byte[] hashKey = b("h");
        byte[] setKey = b("s");
        byte[] zsetKey = b("z");

        Assert.assertTrue(cp.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("HSET"), hashKey, b("f"), b("v"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("SADD"), setKey, b("a"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("ZADD"), zsetKey, b("1"), b("a"))) instanceof RespInteger);

        assertWrongType(cp.execute(Arrays.asList(b("GET"), listKey)));
        assertWrongType(cp.execute(Arrays.asList(b("GET"), hashKey)));
        assertWrongType(cp.execute(Arrays.asList(b("GET"), setKey)));
        assertWrongType(cp.execute(Arrays.asList(b("GET"), zsetKey)));

        db.shutdown();
    }

    @Test
    public void wrongTypeOnHsetWhenKeyHoldsNonHash() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] stringKey = b("k:string");
        byte[] listKey = b("k:list");
        byte[] setKey = b("k:set");
        byte[] zsetKey = b("k:zset");

        Assert.assertTrue(cp.execute(Arrays.asList(b("SET"), stringKey, b("v"))) instanceof RespSimpleString);
        Assert.assertTrue(cp.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("SADD"), setKey, b("a"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("ZADD"), zsetKey, b("1"), b("a"))) instanceof RespInteger);

        assertWrongType(cp.execute(Arrays.asList(b("HSET"), stringKey, b("f"), b("v"))));
        assertWrongType(cp.execute(Arrays.asList(b("HSET"), listKey, b("f"), b("v"))));
        assertWrongType(cp.execute(Arrays.asList(b("HSET"), setKey, b("f"), b("v"))));
        assertWrongType(cp.execute(Arrays.asList(b("HSET"), zsetKey, b("f"), b("v"))));

        db.shutdown();
    }

    @Test
    public void wrongTypeOnSaddAndZaddWhenKeyHoldsNonMatchingType() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        byte[] stringKey = b("k:string");
        byte[] listKey = b("k:list");
        byte[] hashKey = b("k:hash");

        Assert.assertTrue(cp.execute(Arrays.asList(b("SET"), stringKey, b("v"))) instanceof RespSimpleString);
        Assert.assertTrue(cp.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof RespInteger);
        Assert.assertTrue(cp.execute(Arrays.asList(b("HSET"), hashKey, b("f"), b("v"))) instanceof RespInteger);

        assertWrongType(cp.execute(Arrays.asList(b("SADD"), stringKey, b("a"))));
        assertWrongType(cp.execute(Arrays.asList(b("SADD"), listKey, b("a"))));
        assertWrongType(cp.execute(Arrays.asList(b("SADD"), hashKey, b("a"))));

        assertWrongType(cp.execute(Arrays.asList(b("ZADD"), stringKey, b("1"), b("a"))));
        assertWrongType(cp.execute(Arrays.asList(b("ZADD"), listKey, b("1"), b("a"))));
        assertWrongType(cp.execute(Arrays.asList(b("ZADD"), hashKey, b("1"), b("a"))));

        db.shutdown();
    }

    @Test
    public void arityAndSyntaxErrorsMatchExpectedMessages() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        RespError hsetWrongArity = (RespError) cp.execute(Arrays.asList(b("HSET"), b("k"), b("f")));
        Assert.assertEquals("ERR wrong number of arguments for 'hset' command", hsetWrongArity.message());

        RespError zaddWrongArity = (RespError) cp.execute(Arrays.asList(b("ZADD"), b("k"), b("1"), b("a"), b("2")));
        Assert.assertEquals("ERR wrong number of arguments for 'zadd' command", zaddWrongArity.message());

        RespError zrangeSyntax = (RespError) cp.execute(Arrays.asList(b("ZRANGE"), b("k"), b("0"), b("-1"), b("WITHSCORESX")));
        Assert.assertEquals("ERR syntax error", zrangeSyntax.message());

        RespError setBadInt = (RespError) cp.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("abc")));
        Assert.assertEquals("ERR value is not an integer or out of range: seconds", setBadInt.message());

        db.shutdown();
    }

    @Test
    public void scoreRangeCommandsValidateArityAndLimitArguments() {
        YierdisDb db = new YierdisDb();
        CommandProcessor cp = new CommandProcessor(db);

        RespError zrangeByScoreWrongArity = (RespError) cp.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0")));
        Assert.assertEquals("ERR wrong number of arguments for 'zrangebyscore' command", zrangeByScoreWrongArity.message());

        RespError zrevrangeByScoreWrongArity = (RespError) cp.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("0")));
        Assert.assertEquals("ERR wrong number of arguments for 'zrevrangebyscore' command", zrevrangeByScoreWrongArity.message());

        RespError badOpt = (RespError) cp.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0"), b("1"), b("WITHSCORESX")));
        Assert.assertEquals("ERR syntax error", badOpt.message());

        RespError limitMissingCount = (RespError) cp.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0")));
        Assert.assertEquals("ERR syntax error", limitMissingCount.message());

        RespError negativeOffset = (RespError) cp.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0"), b("1"), b("LIMIT"), b("-1"), b("1")));
        Assert.assertEquals("ERR value is not an integer or out of range: offset", negativeOffset.message());

        RespError negativeCount = (RespError) cp.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0"), b("-1")));
        Assert.assertEquals("ERR value is not an integer or out of range: count", negativeCount.message());

        RespError badCount = (RespError) cp.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0"), b("x")));
        Assert.assertEquals("ERR value is not an integer or out of range: count", badCount.message());

        db.shutdown();
    }

    private static void assertWrongType(RespObject obj) {
        Assert.assertTrue(obj instanceof RespError);
        Assert.assertTrue(((RespError) obj).message().startsWith("WRONGTYPE"));
    }
}
