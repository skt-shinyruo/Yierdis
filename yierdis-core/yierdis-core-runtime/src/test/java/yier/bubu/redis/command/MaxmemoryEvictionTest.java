package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

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

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof ReplySimpleString);

            ReplyObject err = client.execute(List.of(b("SET"), b("b"), v1600));
            Assert.assertTrue(err instanceof ReplyError);
            Assert.assertEquals("OOM command not allowed when used memory > 'maxmemory'.", ((ReplyError) err).message());

	            ReplyObject getB = client.execute(List.of(b("GET"), b("b")));
	            Assert.assertTrue(getB instanceof ReplyNull);
            }
        });
    }

    @Test
    public void noevictionRejectsCollectionGrowthWritesBeforeTheyMutate() {
        forEachDbWithMaxmemory(1, "noeviction", 5, db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                assertCollectionWriteRejected(client, List.of(b("LPUSH"), b("l"), b("a")), b("l"));
                assertCollectionWriteRejected(client, List.of(b("HSET"), b("h"), b("f"), b("v")), b("h"));
                assertCollectionWriteRejected(client, List.of(b("SADD"), b("s"), b("m")), b("s"));
                assertCollectionWriteRejected(client, List.of(b("ZADD"), b("z"), b("1"), b("m")), b("z"));
                assertCollectionWriteRejected(client, List.of(b("PFADD"), b("p"), b("e")), b("p"));
            }
        });
    }

    @Test
    public void noevictionAllowsCollectionWritesWhenHeadroomMatchesRealDelta() {
        assertCollectionWriteSucceedsAtRealDelta(
                db -> db.rpush(b("l"), List.of(b("a"), b("b"), b("c"), b("d"))),
                db -> db.lpush(b("l"), List.of(repeat((byte) 'x', 24))),
                List.of(b("LPUSH"), b("l"), repeat((byte) 'x', 24))
        );
        assertCollectionWriteSucceedsAtRealDelta(
                db -> db.hset(b("h"), List.of(b("f1"), b("v1"), b("f2"), b("v2"))),
                db -> db.hset(b("h"), List.of(b("f3"), repeat((byte) 'v', 24))),
                List.of(b("HSET"), b("h"), b("f3"), repeat((byte) 'v', 24))
        );
        assertCollectionWriteSucceedsAtRealDelta(
                db -> db.sadd(b("s"), List.of(b("alpha"), b("beta"), b("gamma"))),
                db -> db.sadd(b("s"), List.of(repeat((byte) 'm', 24))),
                List.of(b("SADD"), b("s"), repeat((byte) 'm', 24))
        );
        assertCollectionWriteSucceedsAtRealDelta(
                db -> db.zadd(b("z"), List.of(b("1"), b("alpha"), b("2"), b("beta"), b("3"), b("gamma"))),
                db -> db.zadd(b("z"), List.of(b("4"), repeat((byte) 'z', 24))),
                List.of(b("ZADD"), b("z"), b("4"), repeat((byte) 'z', 24))
        );
    }

    @Test
    public void allkeysRandomEvictsToStayWithinLimit() {
        forEachDbWithMaxmemory(3000, "allkeys-random", 5, db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] v1600 = repeat((byte) 'x', 1600);

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof ReplySimpleString);

            ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b"));
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

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof ReplySimpleString);
            Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof ReplySimpleString);

            // Make "a" more recently used than "b".
            Assert.assertTrue(client.execute(List.of(b("GET"), b("a"))) instanceof ReplyBulkString);

            // This write triggers eviction; the least recently used key ("b") should be evicted.
            Assert.assertTrue(client.execute(List.of(b("SET"), b("c"), v1600)) instanceof ReplySimpleString);

	            ReplyObject getB = client.execute(List.of(b("GET"), b("b")));
	            Assert.assertTrue(getB instanceof ReplyNull);

	            Assert.assertTrue(client.execute(List.of(b("GET"), b("c"))) instanceof ReplyBulkString);

            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= 4500);
            }
        });
    }

    @Test
    public void objectEncodingAndMemoryUsageAreExposed() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

        Assert.assertTrue(client.execute(cmd("SET", "k", "1")) instanceof ReplySimpleString);
        Assert.assertEquals("int", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "k"))).asString());
        Assert.assertTrue(client.execute(cmd("MEMORY", "USAGE", "k")) instanceof ReplyInteger);

        Assert.assertTrue(client.execute(cmd("SET", "k", "abc")) instanceof ReplySimpleString);
        Assert.assertEquals("embstr", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "k"))).asString());

        Assert.assertTrue(client.execute(List.of(b("SET"), b("k"), repeat((byte) 'x', 50))) instanceof ReplySimpleString);
        Assert.assertEquals("raw", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "k"))).asString());

        // Collection encodings.
        Assert.assertTrue(client.execute(cmd("LPUSH", "l", "a")) instanceof ReplyInteger);
        Assert.assertEquals("listpack", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "l"))).asString());

        Assert.assertTrue(client.execute(cmd("SADD", "s", "1", "2")) instanceof ReplyInteger);
        Assert.assertEquals("intset", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "s"))).asString());
        Assert.assertTrue(client.execute(cmd("SADD", "s", "x")) instanceof ReplyInteger);
        Assert.assertEquals("hashtable", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "s"))).asString());

	        // Missing keys return nil.
	        ReplyObject missing = client.execute(cmd("OBJECT", "ENCODING", "missing"));
	        Assert.assertTrue(missing instanceof ReplyNull);

	        ReplyObject missingUsage = client.execute(cmd("MEMORY", "USAGE", "missing"));
	        Assert.assertTrue(missingUsage instanceof ReplyNull);

        ReplyObject wrongArityMemory = client.execute(cmd("MEMORY"));
        Assert.assertTrue(wrongArityMemory instanceof ReplyError);
        Assert.assertEquals("ERR wrong number of arguments for 'memory' command", ((ReplyError) wrongArityMemory).message());

        ReplyObject wrongArityObject = client.execute(cmd("OBJECT"));
        Assert.assertTrue(wrongArityObject instanceof ReplyError);
        Assert.assertEquals("ERR wrong number of arguments for 'object' command", ((ReplyError) wrongArityObject).message());

        ReplyObject syntaxMemory = client.execute(cmd("MEMORY", "FOO", "k"));
        Assert.assertTrue(syntaxMemory instanceof ReplyError);
        Assert.assertEquals("ERR syntax error", ((ReplyError) syntaxMemory).message());

        ReplyObject syntaxObject = client.execute(cmd("OBJECT", "FOO", "k"));
        Assert.assertTrue(syntaxObject instanceof ReplyError);
        Assert.assertEquals("ERR syntax error", ((ReplyError) syntaxObject).message());

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

    private static void assertCollectionWriteSucceedsAtRealDelta(
            DbMutation setup,
            DbMutation measuredWrite,
            List<byte[]> commandWrite
    ) {
        long usedBefore;
        long actualDelta;
        try (DbFixture measured = new DbFixture(0)) {
            setup.apply(measured.db);
            usedBefore = measured.db.estimatedUsedBytes();
            measuredWrite.apply(measured.db);
            actualDelta = measured.db.estimatedUsedBytes() - usedBefore;
        }

        Assert.assertTrue("actual delta must be positive for this regression harness", actualDelta > 0);

        try (DbFixture bounded = new DbFixture(usedBefore + actualDelta)) {
            setup.apply(bounded.db);
            bounded.setMaxmemory(usedBefore + actualDelta);

            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(bounded.db);
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyObject reply = client.execute(commandWrite);
                Assert.assertFalse(reply instanceof ReplyError);
            }
        }
    }

    private static void assertCollectionWriteRejected(FastTestClient client, List<byte[]> writeCommand, byte[] key) {
        ReplyObject reply = client.execute(writeCommand);
        Assert.assertTrue(reply instanceof ReplyError);
        Assert.assertEquals(MaxmemoryErrors.OOM_ERR, ((ReplyError) reply).message());

        ReplyInteger exists = (ReplyInteger) client.execute(List.of(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());
    }

    @FunctionalInterface
    private interface DbMutation {
        void apply(YierdisDb db);
    }

    private static final class DbFixture implements AutoCloseable {
        private final YierdisDb db;

        private DbFixture(long maxmemoryBytes) {
            this.db = new YierdisDb(null, 0, "noeviction", 5, 5, 5);
            this.db.bindToCurrentThread();
        }

        private void setMaxmemory(long maxmemoryBytes) {
            try {
                java.lang.reflect.Field field = YierdisDb.class.getDeclaredField("maxmemoryBytes");
                field.setAccessible(true);
                field.setLong(db, maxmemoryBytes);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("failed to set maxmemoryBytes for test", e);
            }
        }

        @Override
        public void close() {
            db.shutdown();
        }
    }
}
