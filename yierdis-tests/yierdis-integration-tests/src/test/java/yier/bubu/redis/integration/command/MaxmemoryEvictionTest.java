package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.forEachDbWithMaxmemory;

public class MaxmemoryEvictionTest {
    @Test
    public void noevictionRejectsWritesWhenFull() {
        forEachDbWithMaxmemory(3000, MaxmemoryPolicy.NOEVICTION, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
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
    public void noevictionSetCommandAllowsOverwriteThatShrinksWhenUsedEqualsLimit() {
        byte[] key = b("k");
        byte[] largeValue = repeat((byte) 'x', 1600);
        byte[] smallValue = b("x");
        long maxmemoryBytes = usedAfterSet(key, largeValue);

        forEachDbWithMaxmemory(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(List.of(b("SET"), key, largeValue)) instanceof ReplySimpleString);
                long usedBefore = db.estimatedUsedBytes();
                Assert.assertEquals(maxmemoryBytes, usedBefore);

                ReplyObject reply = client.execute(List.of(b("SET"), key, smallValue));

                Assert.assertTrue(reply instanceof ReplySimpleString);
                Assert.assertArrayEquals(smallValue, ((ReplyBulkString) client.execute(List.of(b("GET"), key))).data());
                Assert.assertTrue("shrinking command should reduce used bytes",
                        db.estimatedUsedBytes() < usedBefore);
            }
        });
    }

    @Test
    public void noevictionRejectsCollectionGrowthWritesBeforeTheyMutate() {
        forEachDbWithMaxmemory(1, MaxmemoryPolicy.NOEVICTION, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
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
    public void noevictionCollectionGrowthUsesRealBoundedMaxmemory() {
        byte[] listValue = repeat((byte) 'x', 256);
        byte[] hashValue = repeat((byte) 'v', 256);
        byte[] setMember = repeat((byte) 'm', 256);
        byte[] zsetMember = repeat((byte) 'z', 256);

        assertCollectionWriteUsesRealBoundedMaxmemory(
                db -> db.writes().lists().rpush(b("l"), List.of(b("a"), b("b"), b("c"), b("d"))).value(),
                db -> db.writes().lists().lpush(b("l"), List.of(listValue)).value(),
                List.of(b("LPUSH"), b("l"), listValue)
        );
        assertCollectionWriteUsesRealBoundedMaxmemory(
                db -> db.writes().hashes().hset(b("h"), List.of(b("f1"), b("v1"), b("f2"), b("v2"))).value(),
                db -> db.writes().hashes().hset(b("h"), List.of(b("f3"), hashValue)).value(),
                List.of(b("HSET"), b("h"), b("f3"), hashValue)
        );
        assertCollectionWriteUsesRealBoundedMaxmemory(
                db -> db.writes().sets().sadd(b("s"), List.of(b("alpha"), b("beta"), b("gamma"))).value(),
                db -> db.writes().sets().sadd(b("s"), List.of(setMember)).value(),
                List.of(b("SADD"), b("s"), setMember)
        );
        assertCollectionWriteUsesRealBoundedMaxmemory(
                db -> db.writes().zsets().zadd(b("z"), List.of(b("1"), b("alpha"), b("2"), b("beta"), b("3"), b("gamma"))).value(),
                db -> db.writes().zsets().zadd(b("z"), List.of(b("4"), zsetMember)).value(),
                List.of(b("ZADD"), b("z"), b("4"), zsetMember)
        );
    }

    @Test
    public void boundedFixtureUsesConfiguredMaxmemoryForLedgerAndStats() {
        try (DbFixture fixture = new DbFixture(1234)) {
            Assert.assertEquals(1234, fixture.db.memoryLedger().limitBytes());
            Assert.assertEquals(1234, fixture.db.memory().memoryStats().maxmemoryBytes());
        }
    }

    @Test
    public void allkeysRandomEvictsToStayWithinLimit() {
        forEachDbWithMaxmemory(3000, MaxmemoryPolicy.ALLKEYS_RANDOM, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
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
        forEachDbWithMaxmemory(4500, MaxmemoryPolicy.ALLKEYS_LRU, 10, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
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

    private static long usedAfterSet(byte[] key, byte[] value) {
        try (DbFixture fixture = new DbFixture(0)) {
            Assert.assertTrue(fixture.db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());
            return fixture.db.estimatedUsedBytes();
        }
    }

    private static void assertCollectionWriteUsesRealBoundedMaxmemory(
            DbMutation setup,
            DbMutation measuredWrite,
            List<byte[]> commandWrite
    ) {
        long usedBefore;
        long actualDelta;
        long usedAfter;
        try (DbFixture measured = new DbFixture(0)) {
            setup.apply(measured.db);
            usedBefore = measured.db.estimatedUsedBytes();
            measuredWrite.apply(measured.db);
            usedAfter = measured.db.estimatedUsedBytes();
            actualDelta = usedAfter - usedBefore;
        }

        Assert.assertTrue("actual delta must be positive for this regression harness", actualDelta > 0);
        String commandName = commandName(commandWrite);
        long acceptedLimit = minimumAcceptedLimit(setup, commandWrite, usedAfter);
        AttemptResult accepted = attemptCollectionWrite(acceptedLimit, setup, commandWrite);
        Assert.assertEquals(commandName, AttemptOutcome.ACCEPTED, accepted.outcome());
        Assert.assertEquals(commandName, usedBefore, accepted.usedBefore());
        Assert.assertEquals(commandName, usedAfter, accepted.usedAfter());
        Assert.assertTrue(commandName + " accepted at limit " + acceptedLimit
                        + " but used " + accepted.usedAfter(),
                accepted.usedAfter() <= acceptedLimit);

        AttemptResult rejected = attemptCollectionWrite(acceptedLimit - 1, setup, commandWrite);
        Assert.assertEquals(commandName, AttemptOutcome.COMMAND_REJECTED, rejected.outcome());
        Assert.assertEquals(commandName, usedBefore, rejected.usedBefore());
        Assert.assertEquals(commandName + " rejected write must not mutate", usedBefore, rejected.usedAfter());
    }

    private static String commandName(List<byte[]> commandWrite) {
        if (commandWrite == null || commandWrite.isEmpty() || commandWrite.get(0) == null) {
            return "<unknown>";
        }
        return new String(commandWrite.get(0), StandardCharsets.UTF_8);
    }

    private static long minimumAcceptedLimit(DbMutation setup, List<byte[]> commandWrite, long seedLimit) {
        long high = Math.max(1L, seedLimit);
        while (attemptCollectionWrite(high, setup, commandWrite).outcome() != AttemptOutcome.ACCEPTED) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1 < high) {
            long mid = low + (high - low) / 2;
            if (attemptCollectionWrite(mid, setup, commandWrite).outcome() == AttemptOutcome.ACCEPTED) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static AttemptResult attemptCollectionWrite(long maxmemoryBytes, DbMutation setup, List<byte[]> commandWrite) {
        try (DbFixture fixture = new DbFixture(maxmemoryBytes)) {
            try {
                setup.apply(fixture.db);
            } catch (YierdisCommandException e) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
                return new AttemptResult(AttemptOutcome.SETUP_REJECTED, -1L, -1L);
            }

            long usedBefore = fixture.db.estimatedUsedBytes();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(fixture.db);
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyObject reply = client.execute(commandWrite);
                long usedAfter = fixture.db.estimatedUsedBytes();
                if (reply instanceof ReplyError err) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, err.message());
                    return new AttemptResult(AttemptOutcome.COMMAND_REJECTED, usedBefore, usedAfter);
                }
                return new AttemptResult(AttemptOutcome.ACCEPTED, usedBefore, usedAfter);
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

    private enum AttemptOutcome {
        SETUP_REJECTED,
        COMMAND_REJECTED,
        ACCEPTED
    }

    private record AttemptResult(AttemptOutcome outcome, long usedBefore, long usedAfter) {
    }

    private static final class DbFixture implements AutoCloseable {
        private final YierdisDb db;

        private DbFixture(long maxmemoryBytes) {
            this.db = YierdisDb.createWithOwnedFfmRuntime(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            this.db.bindToCurrentThread();
        }

        @Override
        public void close() {
            db.shutdown();
        }
    }
}
