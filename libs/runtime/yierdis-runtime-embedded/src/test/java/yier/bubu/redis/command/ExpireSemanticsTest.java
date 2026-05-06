package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

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
}
