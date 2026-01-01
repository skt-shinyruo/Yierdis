package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
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
        forEachDbWithMaxmemory(80, "noeviction", 5, db -> {
            CommandProcessor cp = new CommandProcessor(db);

            byte[] v50 = repeat((byte) 'x', 50);

            Assert.assertTrue(cp.execute(List.of(b("SET"), b("a"), v50)) instanceof RespSimpleString);

            RespObject err = cp.execute(List.of(b("SET"), b("b"), v50));
            Assert.assertTrue(err instanceof RespError);
            Assert.assertEquals("OOM command not allowed when used memory > 'maxmemory'.", ((RespError) err).message());

            RespBulkString getB = (RespBulkString) cp.execute(List.of(b("GET"), b("b")));
            Assert.assertTrue(getB.isNull());
        });
    }

    @Test
    public void allkeysRandomEvictsToStayWithinLimit() {
        forEachDbWithMaxmemory(80, "allkeys-random", 5, db -> {
            CommandProcessor cp = new CommandProcessor(db);

            byte[] v50 = repeat((byte) 'x', 50);

            Assert.assertTrue(cp.execute(List.of(b("SET"), b("a"), v50)) instanceof RespSimpleString);
            Assert.assertTrue(cp.execute(List.of(b("SET"), b("b"), v50)) instanceof RespSimpleString);

            RespInteger exists = (RespInteger) cp.execute(cmd("EXISTS", "a", "b"));
            Assert.assertEquals(1, exists.value());
            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= 80);

        });
    }

    @Test
    public void allkeysLruEvictsLeastRecentlyUsedWhenSamplesCoverAllKeys() {
        // samples >= total keys triggers a deterministic full scan in eviction.
        forEachDbWithMaxmemory(150, "allkeys-lru", 10, db -> {
            CommandProcessor cp = new CommandProcessor(db);

            byte[] v50 = repeat((byte) 'x', 50);

            Assert.assertTrue(cp.execute(List.of(b("SET"), b("a"), v50)) instanceof RespSimpleString);
            Assert.assertTrue(cp.execute(List.of(b("SET"), b("b"), v50)) instanceof RespSimpleString);

            // Make "a" more recently used than "b".
            RespBulkString getA = (RespBulkString) cp.execute(List.of(b("GET"), b("a")));
            Assert.assertFalse(getA.isNull());

            // This write triggers eviction; the least recently used key ("b") should be evicted.
            Assert.assertTrue(cp.execute(List.of(b("SET"), b("c"), v50)) instanceof RespSimpleString);

            RespBulkString getB = (RespBulkString) cp.execute(List.of(b("GET"), b("b")));
            Assert.assertTrue(getB.isNull());

            RespBulkString getC = (RespBulkString) cp.execute(List.of(b("GET"), b("c")));
            Assert.assertFalse(getC.isNull());

            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= 150);
        });
    }

    @Test
    public void objectEncodingAndMemoryUsageAreExposed() {
        forEachDb(db -> {
            CommandProcessor cp = new CommandProcessor(db);

        Assert.assertTrue(cp.execute(cmd("SET", "k", "1")) instanceof RespSimpleString);
        Assert.assertEquals("int", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "k"))).value());
        Assert.assertTrue(cp.execute(cmd("MEMORY", "USAGE", "k")) instanceof RespInteger);

        Assert.assertTrue(cp.execute(cmd("SET", "k", "abc")) instanceof RespSimpleString);
        Assert.assertEquals("embstr", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "k"))).value());

        Assert.assertTrue(cp.execute(List.of(b("SET"), b("k"), repeat((byte) 'x', 50))) instanceof RespSimpleString);
        Assert.assertEquals("raw", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "k"))).value());

        // Collection encodings.
        Assert.assertTrue(cp.execute(cmd("LPUSH", "l", "a")) instanceof RespInteger);
        Assert.assertEquals("listpack", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "l"))).value());

        Assert.assertTrue(cp.execute(cmd("SADD", "s", "1", "2")) instanceof RespInteger);
        Assert.assertEquals("intset", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "s"))).value());
        Assert.assertTrue(cp.execute(cmd("SADD", "s", "x")) instanceof RespInteger);
        Assert.assertEquals("hashtable", ((RespSimpleString) cp.execute(cmd("OBJECT", "ENCODING", "s"))).value());

        // Missing keys return nil.
        RespObject missing = cp.execute(cmd("OBJECT", "ENCODING", "missing"));
        Assert.assertTrue(missing instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) missing).isNull());

        RespObject missingUsage = cp.execute(cmd("MEMORY", "USAGE", "missing"));
        Assert.assertTrue(missingUsage instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) missingUsage).isNull());

        RespObject wrongArityMemory = cp.execute(cmd("MEMORY"));
        Assert.assertTrue(wrongArityMemory instanceof RespError);
        Assert.assertEquals("ERR wrong number of arguments for 'memory' command", ((RespError) wrongArityMemory).message());

        RespObject wrongArityObject = cp.execute(cmd("OBJECT"));
        Assert.assertTrue(wrongArityObject instanceof RespError);
        Assert.assertEquals("ERR wrong number of arguments for 'object' command", ((RespError) wrongArityObject).message());

        RespObject syntaxMemory = cp.execute(cmd("MEMORY", "FOO", "k"));
        Assert.assertTrue(syntaxMemory instanceof RespError);
        Assert.assertEquals("ERR syntax error", ((RespError) syntaxMemory).message());

        RespObject syntaxObject = cp.execute(cmd("OBJECT", "FOO", "k"));
        Assert.assertTrue(syntaxObject instanceof RespError);
        Assert.assertEquals("ERR syntax error", ((RespError) syntaxObject).message());

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
