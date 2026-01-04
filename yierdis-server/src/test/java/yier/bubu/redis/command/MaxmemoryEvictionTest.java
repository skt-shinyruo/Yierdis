package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.forEachDbWithMaxmemory;

public class MaxmemoryEvictionTest {
    @Test
    public void noevictionRejectsWritesWhenFull() {
        forEachDbWithMaxmemory(3000, "noeviction", 5, db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v1600 = repeat((byte) 'x', 1600);

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof RespSimpleString);

            RespObject err = client.execute(List.of(b("SET"), b("b"), v1600));
            Assert.assertTrue(err instanceof RespError);
            Assert.assertEquals("OOM command not allowed when used memory > 'maxmemory'.", ((RespError) err).message());

            RespBulkString getB = (RespBulkString) client.execute(List.of(b("GET"), b("b")));
            Assert.assertTrue(getB.isNull());
            }
        });
    }

    @Test
    public void allkeysRandomEvictsToStayWithinLimit() {
        forEachDbWithMaxmemory(3000, "allkeys-random", 5, db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v1600 = repeat((byte) 'x', 1600);

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof RespSimpleString);
            Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof RespSimpleString);

            RespInteger exists = (RespInteger) client.execute(cmd("EXISTS", "a", "b"));
            Assert.assertEquals(1, exists.value());
            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= 3000);

            }
        });
    }

    @Test
    public void allkeysLruEvictsLeastRecentlyUsedWhenSamplesCoverAllKeys() {
        // samples >= total keys triggers a deterministic full scan in eviction.
        forEachDbWithMaxmemory(4500, "allkeys-lru", 10, db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v1600 = repeat((byte) 'x', 1600);

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof RespSimpleString);
            Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof RespSimpleString);

            // Make "a" more recently used than "b".
            RespBulkString getA = (RespBulkString) client.execute(List.of(b("GET"), b("a")));
            Assert.assertFalse(getA.isNull());

            // This write triggers eviction; the least recently used key ("b") should be evicted.
            Assert.assertTrue(client.execute(List.of(b("SET"), b("c"), v1600)) instanceof RespSimpleString);

            RespBulkString getB = (RespBulkString) client.execute(List.of(b("GET"), b("b")));
            Assert.assertTrue(getB.isNull());

            RespBulkString getC = (RespBulkString) client.execute(List.of(b("GET"), b("c")));
            Assert.assertFalse(getC.isNull());

            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= 4500);
            }
        });
    }

    @Test
    public void objectEncodingAndMemoryUsageAreExposed() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

        Assert.assertTrue(client.execute(cmd("SET", "k", "1")) instanceof RespSimpleString);
        Assert.assertEquals("int", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "k"))).value());
        Assert.assertTrue(client.execute(cmd("MEMORY", "USAGE", "k")) instanceof RespInteger);

        Assert.assertTrue(client.execute(cmd("SET", "k", "abc")) instanceof RespSimpleString);
        Assert.assertEquals("embstr", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "k"))).value());

        Assert.assertTrue(client.execute(List.of(b("SET"), b("k"), repeat((byte) 'x', 50))) instanceof RespSimpleString);
        Assert.assertEquals("raw", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "k"))).value());

        // Collection encodings.
        Assert.assertTrue(client.execute(cmd("LPUSH", "l", "a")) instanceof RespInteger);
        Assert.assertEquals("listpack", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "l"))).value());

        Assert.assertTrue(client.execute(cmd("SADD", "s", "1", "2")) instanceof RespInteger);
        Assert.assertEquals("intset", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "s"))).value());
        Assert.assertTrue(client.execute(cmd("SADD", "s", "x")) instanceof RespInteger);
        Assert.assertEquals("hashtable", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "s"))).value());

        // Missing keys return nil.
        RespObject missing = client.execute(cmd("OBJECT", "ENCODING", "missing"));
        Assert.assertTrue(missing instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) missing).isNull());

        RespObject missingUsage = client.execute(cmd("MEMORY", "USAGE", "missing"));
        Assert.assertTrue(missingUsage instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) missingUsage).isNull());

        RespObject wrongArityMemory = client.execute(cmd("MEMORY"));
        Assert.assertTrue(wrongArityMemory instanceof RespError);
        Assert.assertEquals("ERR wrong number of arguments for 'memory' command", ((RespError) wrongArityMemory).message());

        RespObject wrongArityObject = client.execute(cmd("OBJECT"));
        Assert.assertTrue(wrongArityObject instanceof RespError);
        Assert.assertEquals("ERR wrong number of arguments for 'object' command", ((RespError) wrongArityObject).message());

        RespObject syntaxMemory = client.execute(cmd("MEMORY", "FOO", "k"));
        Assert.assertTrue(syntaxMemory instanceof RespError);
        Assert.assertEquals("ERR syntax error", ((RespError) syntaxMemory).message());

        RespObject syntaxObject = client.execute(cmd("OBJECT", "FOO", "k"));
        Assert.assertTrue(syntaxObject instanceof RespError);
        Assert.assertEquals("ERR syntax error", ((RespError) syntaxObject).message());

            }
        });
    }

    private static byte[] repeat(byte b, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = b;
        }
        return out;
    }
}
