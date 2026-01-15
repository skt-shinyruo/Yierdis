package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ExpireSemanticsTest {
    @Test
    public void expireZeroRemovesListAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("list");
            Assert.assertEquals(2L, ((RespInteger) client.execute(Arrays.asList(b("RPUSH"), key, b("a"), b("b")))).value());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals(-2L, ((RespInteger) client.execute(Arrays.asList(b("TTL"), key))).value());
            Assert.assertEquals("none", ((RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            RespArray range = (RespArray) client.execute(Arrays.asList(b("LRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(range.values().isEmpty());

            RespBulkString lpop = (RespBulkString) client.execute(Arrays.asList(b("LPOP"), key));
            Assert.assertTrue(lpop.isNull());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("RPUSH"), key, b("x")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesHashAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("hash");
            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("HSET"), key, b("f"), b("v")))).value());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            RespBulkString hget = (RespBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f")));
            Assert.assertTrue(hget.isNull());

            RespArray all = (RespArray) client.execute(Arrays.asList(b("HGETALL"), key));
            Assert.assertTrue(all.values().isEmpty());

            Assert.assertEquals(0L, ((RespInteger) client.execute(Arrays.asList(b("HLEN"), key))).value());
            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("HSET"), key, b("f2"), b("v2")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesSetAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("set");
            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("SADD"), key, b("a")))).value());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            Assert.assertEquals(0L, ((RespInteger) client.execute(Arrays.asList(b("SISMEMBER"), key, b("a")))).value());
            Assert.assertEquals(0L, ((RespInteger) client.execute(Arrays.asList(b("SCARD"), key))).value());

            RespArray members = (RespArray) client.execute(Arrays.asList(b("SMEMBERS"), key));
            Assert.assertTrue(members.values().isEmpty());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("SADD"), key, b("x")))).value());
            }
        });
    }

    @Test
    public void expireZeroRemovesZsetAndSubsequentWritesRecreate() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("zset");
            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), b("a")))).value());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("EXPIRE"), key, b("0")))).value());
            Assert.assertEquals("none", ((RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key))).value());

            RespArray range = (RespArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
            Assert.assertTrue(range.values().isEmpty());

            Assert.assertEquals(1L, ((RespInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), b("x")))).value());
            }
        });
    }
}
