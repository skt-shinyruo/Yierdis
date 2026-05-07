package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandErrorTest {
    @Test
    public void wrongTypeOnGetWhenKeyHoldsNonString() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] listKey = b("l");
            byte[] hashKey = b("h");
            byte[] setKey = b("s");
            byte[] zsetKey = b("z");

            Assert.assertTrue(client.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("HSET"), hashKey, b("f"), b("v"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("SADD"), setKey, b("a"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("ZADD"), zsetKey, b("1"), b("a"))) instanceof ReplyInteger);

            assertWrongType(client.execute(Arrays.asList(b("GET"), listKey)));
            assertWrongType(client.execute(Arrays.asList(b("GET"), hashKey)));
            assertWrongType(client.execute(Arrays.asList(b("GET"), setKey)));
            assertWrongType(client.execute(Arrays.asList(b("GET"), zsetKey)));
            }
        });
    }

    @Test
    public void wrongTypeOnHsetWhenKeyHoldsNonHash() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] stringKey = b("k:string");
            byte[] listKey = b("k:list");
            byte[] setKey = b("k:set");
            byte[] zsetKey = b("k:zset");

            Assert.assertTrue(client.execute(Arrays.asList(b("SET"), stringKey, b("v"))) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("SADD"), setKey, b("a"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("ZADD"), zsetKey, b("1"), b("a"))) instanceof ReplyInteger);

            assertWrongType(client.execute(Arrays.asList(b("HSET"), stringKey, b("f"), b("v"))));
            assertWrongType(client.execute(Arrays.asList(b("HSET"), listKey, b("f"), b("v"))));
            assertWrongType(client.execute(Arrays.asList(b("HSET"), setKey, b("f"), b("v"))));
            assertWrongType(client.execute(Arrays.asList(b("HSET"), zsetKey, b("f"), b("v"))));
            }
        });
    }

    @Test
    public void wrongTypeOnSaddAndZaddWhenKeyHoldsNonMatchingType() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] stringKey = b("k:string");
            byte[] listKey = b("k:list");
            byte[] hashKey = b("k:hash");

            Assert.assertTrue(client.execute(Arrays.asList(b("SET"), stringKey, b("v"))) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(Arrays.asList(b("RPUSH"), listKey, b("a"))) instanceof ReplyInteger);
            Assert.assertTrue(client.execute(Arrays.asList(b("HSET"), hashKey, b("f"), b("v"))) instanceof ReplyInteger);

            assertWrongType(client.execute(Arrays.asList(b("SADD"), stringKey, b("a"))));
            assertWrongType(client.execute(Arrays.asList(b("SADD"), listKey, b("a"))));
            assertWrongType(client.execute(Arrays.asList(b("SADD"), hashKey, b("a"))));

            assertWrongType(client.execute(Arrays.asList(b("ZADD"), stringKey, b("1"), b("a"))));
            assertWrongType(client.execute(Arrays.asList(b("ZADD"), listKey, b("1"), b("a"))));
            assertWrongType(client.execute(Arrays.asList(b("ZADD"), hashKey, b("1"), b("a"))));
            }
        });
    }

    @Test
    public void arityAndSyntaxErrorsMatchExpectedMessages() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            ReplyError hsetWrongArity = (ReplyError) client.execute(Arrays.asList(b("HSET"), b("k"), b("f")));
            Assert.assertEquals("ERR wrong number of arguments for 'hset' command", hsetWrongArity.message());

            ReplyError hsetMissingValue = (ReplyError) client.execute(Arrays.asList(b("HSET"), b("k"), b("f1"), b("v1"), b("f2")));
            Assert.assertEquals("ERR wrong number of arguments for 'hset' command", hsetMissingValue.message());

            ReplyError zaddWrongArity = (ReplyError) client.execute(Arrays.asList(b("ZADD"), b("k"), b("1"), b("a"), b("2")));
            Assert.assertEquals("ERR wrong number of arguments for 'zadd' command", zaddWrongArity.message());

            ReplyError zrangeSyntax = (ReplyError) client.execute(Arrays.asList(b("ZRANGE"), b("k"), b("0"), b("-1"), b("WITHSCORESX")));
            Assert.assertEquals("ERR syntax error", zrangeSyntax.message());

            ReplyError scanMissingMatch = (ReplyError) client.execute(Arrays.asList(b("SCAN"), b("0"), b("MATCH")));
            Assert.assertEquals("ERR syntax error", scanMissingMatch.message());

            ReplyError scanBadCount = (ReplyError) client.execute(Arrays.asList(b("SCAN"), b("0"), b("COUNT"), b("x")));
            Assert.assertEquals("ERR value is not an integer or out of range", scanBadCount.message());

            ReplyObject zrangeDuplicate = client.execute(Arrays.asList(b("ZRANGE"), b("k"), b("0"), b("-1"), b("WITHSCORES"), b("WITHSCORES")));
            Assert.assertTrue(zrangeDuplicate instanceof ReplyError);
            Assert.assertEquals("ERR syntax error", ((ReplyError) zrangeDuplicate).message());

            ReplyError zrangeDuplicateUnknown = (ReplyError) client.execute(Arrays.asList(b("ZRANGE"), b("k"), b("0"), b("-1"), b("WITHSCORES"), b("BAD")));
            Assert.assertEquals("ERR syntax error", zrangeDuplicateUnknown.message());

            ReplyError setBadInt = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("abc")));
            Assert.assertEquals("ERR value is not an integer or out of range", setBadInt.message());

            ReplyError setConflictingModes = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")));
            Assert.assertEquals("ERR syntax error", setConflictingModes.message());

            ReplyError setMissingExpire = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX")));
            Assert.assertEquals("ERR syntax error", setMissingExpire.message());

            ReplyError setInvalidExpire = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("0")));
            Assert.assertEquals("ERR invalid expire time in 'set' command", setInvalidExpire.message());

            ReplyError setDuplicateGet = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("GET"), b("GET")));
            Assert.assertEquals("ERR syntax error", setDuplicateGet.message());

            }
        });
    }

    @Test
    public void scoreRangeCommandsValidateArityAndLimitArguments() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            ReplyError zrangeByScoreWrongArity = (ReplyError) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0")));
            Assert.assertEquals("ERR wrong number of arguments for 'zrangebyscore' command", zrangeByScoreWrongArity.message());

            ReplyError zrevrangeByScoreWrongArity = (ReplyError) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("0")));
            Assert.assertEquals("ERR wrong number of arguments for 'zrevrangebyscore' command", zrevrangeByScoreWrongArity.message());

            ReplyError badOpt = (ReplyError) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0"), b("1"), b("WITHSCORESX")));
            Assert.assertEquals("ERR syntax error", badOpt.message());

            ReplyError limitMissingCount = (ReplyError) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0")));
            Assert.assertEquals("ERR syntax error", limitMissingCount.message());

            ReplyError negativeOffset = (ReplyError) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), b("k"), b("0"), b("1"), b("LIMIT"), b("-1"), b("1")));
            Assert.assertEquals("ERR value is not an integer or out of range", negativeOffset.message());

            ReplyError negativeCount = (ReplyError) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0"), b("-1")));
            Assert.assertEquals("ERR value is not an integer or out of range", negativeCount.message());

            ReplyError badCount = (ReplyError) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), b("k"), b("1"), b("0"), b("LIMIT"), b("0"), b("x")));
            Assert.assertEquals("ERR value is not an integer or out of range", badCount.message());

            }
        });
    }

    private static void assertWrongType(ReplyObject obj) {
        Assert.assertTrue(obj instanceof ReplyError);
        Assert.assertTrue(((ReplyError) obj).message().startsWith("WRONGTYPE"));
    }
}
