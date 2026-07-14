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
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.forEachDbWithMaxmemory;

public class MaxmemoryEvictionTest {
    @Test
    public void noevictionRejectsWritesWhenFull() {
        byte[] value = repeat((byte) 'x', 40_000);
        long maxmemoryBytes = minMaxmemoryThatAllowsSetKeys(value, List.of(b("a")));
        forEachDbWithMaxmemory(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), value)) instanceof ReplySimpleString);

            ReplyObject err = client.execute(List.of(b("SET"), b("b"), value));
            Assert.assertTrue(err instanceof ReplyError);
            Assert.assertEquals("OOM command not allowed when used memory > 'maxmemory'.", ((ReplyError) err).message());

	            ReplyObject getB = client.execute(List.of(b("GET"), b("b")));
	            Assert.assertTrue(getB instanceof ReplyNull);
            }
        });
    }

    @Test
    public void noevictionSetCommandAllowsOverwriteThatShrinksWithTransientHeadroom() {
        byte[] key = b("k");
        byte[] largeValue = repeat((byte) 'x', 1600);
        byte[] smallValue = b("x");
        long maxmemoryBytes = maxmemoryThatAllowsSetAndOverwrite(key, largeValue, smallValue);

        forEachDbWithMaxmemory(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(List.of(b("SET"), key, largeValue)) instanceof ReplySimpleString);
                long usedBefore = db.estimatedUsedBytes();
                Assert.assertTrue(usedBefore <= maxmemoryBytes);

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
        byte[] value = repeat((byte) 'x', 40_000);
        long maxmemoryBytes = minMaxmemoryThatAllowsSetAfterDeletion(value);
        forEachDbWithMaxmemory(maxmemoryBytes, MaxmemoryPolicy.ALLKEYS_RANDOM, 5, db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), value)) instanceof ReplySimpleString);
            long usedBeforeCandidate = db.estimatedUsedBytes();
            ReplyObject candidateSet = client.execute(List.of(b("SET"), b("b"), value));
            Assert.assertTrue(
                    "candidate SET must succeed after eviction: reply=" + replyDescription(candidateSet)
                            + ", usedBefore=" + usedBeforeCandidate
                            + ", usedAfter=" + db.estimatedUsedBytes()
                            + ", limit=" + maxmemoryBytes
                            + ", keyCount=" + db.size(),
                    candidateSet instanceof ReplySimpleString
            );

            ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b"));
            Assert.assertEquals(1, exists.value());
            Assert.assertTrue("used bytes must be <= maxmemory", db.estimatedUsedBytes() <= maxmemoryBytes);

            }
        });
    }

    @Test
    public void allkeysLruEvictsLeastRecentlyUsedWhenSamplesCoverAllKeys() {
        // samples >= total keys triggers a deterministic full scan in eviction.
        byte[] value = repeat((byte) 'x', 30_000);
        LruAdmissionBoundary boundary = findLruAdmissionBoundary(value);
        forEachDbWithMaxmemory(
                boundary.maxmemoryBytes(),
                MaxmemoryPolicy.ALLKEYS_LRU,
                boundary.residentKeys().size(),
                100,
                db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                for (byte[] key : boundary.residentKeys()) {
                    Assert.assertTrue(client.execute(List.of(b("SET"), key, value)) instanceof ReplySimpleString);
                }

                // Make "a" more recently used than "b".
                Assert.assertTrue(client.execute(List.of(b("GET"), boundary.residentKeys().get(1)))
                        instanceof ReplyBulkString);

                // This write crosses a measured admission boundary and must evict the oldest key.
                long usedBeforeCandidate = db.estimatedUsedBytes();
                ReplyObject candidateSet = client.execute(List.of(b("SET"), boundary.candidateKey(), value));
                Assert.assertTrue(
                        "candidate SET must succeed after eviction: reply=" + replyDescription(candidateSet)
                                + ", residentKeys=" + boundary.residentKeys().size()
                                + ", usedBefore=" + usedBeforeCandidate
                                + ", limit=" + boundary.maxmemoryBytes(),
                        candidateSet instanceof ReplySimpleString
                );

                ReplyObject getB = client.execute(List.of(b("GET"), boundary.residentKeys().get(0)));
                Assert.assertTrue(getB instanceof ReplyNull);

                Assert.assertTrue(client.execute(List.of(b("GET"), boundary.candidateKey())) instanceof ReplyBulkString);

                Assert.assertTrue(
                        "used bytes must be <= maxmemory",
                        db.estimatedUsedBytes() <= boundary.maxmemoryBytes()
                );
            }
                }
        );
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

    private static String replyDescription(ReplyObject reply) {
        if (reply instanceof ReplyError error) {
            return error.message();
        }
        return reply.getClass().getSimpleName();
    }

    private static long maxmemoryThatAllowsSetAndOverwrite(byte[] key, byte[] initialValue, byte[] overwriteValue) {
        long limit = minMaxmemoryThatAllowsSetCount(initialValue, 1);
        while (!allowsSetAndOverwrite(limit, key, initialValue, overwriteValue)) {
            limit = Math.multiplyExact(limit, 2L);
        }
        return limit;
    }

    private static boolean allowsSetAndOverwrite(
            long maxmemoryBytes,
            byte[] key,
            byte[] initialValue,
            byte[] overwriteValue
    ) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5
        );
        try {
            db.bindToCurrentThread();
            return db.writes().strings().setString(key, initialValue, SetMode.NORMAL, null).value()
                    && db.writes().strings().setString(key, overwriteValue, SetMode.NORMAL, null).value();
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        } finally {
            db.shutdown();
        }
    }

    private static long minMaxmemoryThatAllowsSetCount(byte[] value, int count) {
        ArrayList<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(b("k" + i));
        }
        return minMaxmemoryThatAllowsSetKeys(value, keys);
    }

    private static LruAdmissionBoundary findLruAdmissionBoundary(byte[] value) {
        byte[] candidateKey = b("c00000");
        List<byte[]> residentKeys = lruResidentKeys(2);
        long residentMinimum = minMaxmemoryThatAllowsSetKeys(value, residentKeys);

        for (int residentKeyCount = residentKeys.size(); residentKeyCount <= 64; residentKeyCount++) {
            ArrayList<byte[]> keysWithCandidate = new ArrayList<>(residentKeys);
            keysWithCandidate.add(candidateKey);
            long nextMinimum = minMaxmemoryThatAllowsSetKeys(value, keysWithCandidate);
            if (nextMinimum > residentMinimum) {
                return new LruAdmissionBoundary(residentKeys, candidateKey, nextMinimum - 1L);
            }

            if (residentKeyCount < 64) {
                residentKeys = lruResidentKeys(residentKeyCount + 1);
                residentMinimum = minMaxmemoryThatAllowsSetKeys(value, residentKeys);
            }
        }

        throw new AssertionError("no physical admission boundary found for up to 64 LRU resident keys");
    }

    private static List<byte[]> lruResidentKeys(int count) {
        ArrayList<byte[]> keys = new ArrayList<>(count);
        keys.add(b("b00000"));
        keys.add(b("a00000"));
        for (int i = 2; i < count; i++) {
            keys.add(b("d" + String.format("%05d", i)));
        }
        return List.copyOf(keys);
    }

    private static long minMaxmemoryThatAllowsSetKeys(byte[] value, List<byte[]> keys) {
        long high = 1L;
        while (!allowsSetKeys(high, value, keys)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsSetKeys(mid, value, keys)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static long minMaxmemoryThatAllowsSetAfterDeletion(byte[] value) {
        long high = 1L;
        while (!allowsSetAfterDeletion(high, value)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsSetAfterDeletion(mid, value)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsSetKeys(long maxmemoryBytes, byte[] value, List<byte[]> keys) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5
        );
        try {
            db.bindToCurrentThread();
            for (byte[] key : keys) {
                if (!db.writes().strings().setString(key, value, SetMode.NORMAL, null).value()) {
                    return false;
                }
            }
            return true;
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        } finally {
            db.shutdown();
        }
    }

    private static boolean allowsSetAfterDeletion(long maxmemoryBytes, byte[] value) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5
        );
        try {
            db.bindToCurrentThread();
            if (!db.writes().strings().setString(b("a"), value, SetMode.NORMAL, null).value()) {
                return false;
            }
            if (db.writes().keyspace().del(List.of(b("a"))).value() != 1L) {
                throw new AssertionError("test setup did not delete the resident key");
            }
            return db.writes().strings().setString(b("b"), value, SetMode.NORMAL, null).value();
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        } finally {
            db.shutdown();
        }
    }

    private record LruAdmissionBoundary(List<byte[]> residentKeys, byte[] candidateKey, long maxmemoryBytes) {
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
                        + " but used " + accepted.usedAfter()
                        + " (before=" + accepted.usedBefore()
                        + ", measuredAfter=" + usedAfter
                        + ", actualDelta=" + actualDelta + ")",
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
