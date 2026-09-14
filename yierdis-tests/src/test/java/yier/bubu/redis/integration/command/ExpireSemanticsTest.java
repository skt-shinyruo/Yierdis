package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyMap;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ExpireSemanticsTest {
    @Test
    public void expireZeroRemovesListAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("list");
            Assert.assertEquals(2L, ((ReplyInteger) client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b")))).value());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals(-2L, ((ReplyInteger) client.execute(Arrays.asList(b("TTL"), key))).value());
            Assert.assertEquals("none", ((ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(range.values().isEmpty());

	            Assert.assertTrue(client.execute(Arrays.asList(b("LPOP"), key)) instanceof ReplyNull);

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("RPUSH"), key, b("x")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesHashAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("hash");
            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("HSET"), key, b("f"), b("v")))).value());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

	            Assert.assertTrue(client.execute(Arrays.asList(b("HGET"), key, b("f"))) instanceof ReplyNull);

	            ReplyMap all = (ReplyMap) client.execute(Arrays.asList(b("HGETALL"), key));
	            Assert.assertTrue(all.entries().isEmpty());

            Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("HLEN"), key))).value());
            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("HSET"), key, b("f2"), b("v2")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesSetAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("set");
            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("SADD"), key, b("a")))).value());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("a")))).value());
            Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("SCARD"), key))).value());

            ReplyArray members = (ReplyArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
            Assert.assertTrue(members.values().isEmpty());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("SADD"), key, b("x")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesZsetAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

            byte[] key = b("zset");
            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), b("a")))).value());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(range.values().isEmpty());

            Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), b("x")))).value());
            }
        });
    }

    @Test
    public void ttlMinusTwoMeansKeyIsGoneAndNeverReturnedForLiveKeys() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

                // 仍存在的 key 不得回答 -2：persistent 为 -1，带 TTL 为正值。
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("live"), b("v"))) instanceof ReplySimpleString);
                Assert.assertEquals(-1L, ((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("live")))).value());
                Assert.assertEquals(-1L, ((ReplyInteger) client.execute(Arrays.asList(b("PTTL"), b("live")))).value());
                Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), b("live"), b("60")))).value());
                Assert.assertTrue(((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("live")))).value() > 0L);
                Assert.assertTrue(((ReplyInteger) client.execute(Arrays.asList(b("PTTL"), b("live")))).value() > 0L);

                // -2 表示 key 已不在：回答后 GET/EXISTS 必须观察不到它。
                long pastExat = (System.currentTimeMillis() / 1000L) - 60L;
                Assert.assertTrue(client.execute(Arrays.asList(
                        b("SET"), b("gone"), b("v"), b("EXAT"), b(Long.toString(pastExat))
                )) instanceof ReplySimpleString);
                Assert.assertEquals(-2L, ((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("gone")))).value());
                Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), b("gone")))).value());
                Assert.assertTrue(client.execute(Arrays.asList(b("GET"), b("gone"))) instanceof ReplyNull);

                Assert.assertTrue(client.execute(Arrays.asList(
                        b("SET"), b("gone2"), b("v"), b("EXAT"), b(Long.toString(pastExat))
                )) instanceof ReplySimpleString);
                Assert.assertEquals(-2L, ((ReplyInteger) client.execute(Arrays.asList(b("PTTL"), b("gone2")))).value());
                Assert.assertEquals(0L, ((ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), b("gone2")))).value());
            }
        });
    }

    @Test
    public void ttlRoundsRemainingMillisToNearestSecondLikeRedis() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k"), b("v")))).value());
                Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("PEXPIRE"), b("k"), b("99900")))).value());
                long ttl = ((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value();
                Assert.assertEquals("TTL must round (ms+500)/1000 like Redis", 100L, ttl);
                long pttl = ((ReplyInteger) client.execute(Arrays.asList(b("PTTL"), b("k")))).value();
                Assert.assertTrue("PTTL must stay close to 99.9s but got " + pttl, pttl > 98_900L && pttl <= 99_900L);
            }
        });
    }
}
