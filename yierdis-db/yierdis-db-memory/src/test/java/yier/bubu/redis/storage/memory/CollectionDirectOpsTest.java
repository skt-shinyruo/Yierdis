package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class CollectionDirectOpsTest {
    @Test
    public void hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertEquals(0L, db.reads().hashes().hlen(b("missing")));
            Assert.assertNull(db.reads().hashes().hget(b("missing"), b("f")));
            Assert.assertEquals(0, db.reads().hashes().hgetall(b("missing")).pairCount());
            Assert.assertEquals(0L, db.writes().hashes().hdel(b("missing"), List.of(b("f"))).value().longValue());

            Assert.assertEquals(2L, db.writes().hashes().hset(b("h"), List.of(b("a"), b("1"), b("b"), b("2"))).value().longValue());
            Assert.assertEquals(2L, db.reads().hashes().hlen(b("h")));
            Assert.assertArrayEquals(b("1"), db.reads().hashes().hget(b("h"), b("a")));
            Assert.assertNull(db.reads().hashes().hget(b("h"), b("missing-field")));
            Assert.assertEquals(2, db.reads().hashes().hgetall(b("h")).pairCount());

            db.writes().ttl().pexpire(view("h"), 5000);
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("h")) > 0L);
            Assert.assertEquals(0L, db.writes().hashes().hset(b("h"), List.of(b("a"), b("updated"))).value().longValue());
            Assert.assertArrayEquals(b("updated"), db.reads().hashes().hget(b("h"), b("a")));
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("h")) > 0L);

            Assert.assertEquals(0L, db.writes().hashes().hdel(b("h"), List.of(b("x"))).value().longValue());
            Assert.assertEquals(1L, db.writes().hashes().hdel(b("h"), List.of(b("a"))).value().longValue());
            Assert.assertEquals(1L, db.reads().hashes().hlen(b("h")));

            db.writes().ttl().pexpire(view("h"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.reads().hashes().hlen(b("h")));
            Assert.assertNull(db.reads().keyspace().typeOf(view("h")));

            db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.reads().hashes().hget(b("s"), b("f")));
            expectWrongType(() -> db.reads().hashes().hgetall(b("s")).pairCount());
            expectWrongType(() -> db.reads().hashes().hlen(b("s")));
            expectWrongType(() -> db.writes().hashes().hset(b("s"), List.of(b("f"), b("v"))));
            expectWrongType(() -> db.writes().hashes().hdel(b("s"), List.of(b("f"))));
        });
    }

    @Test
    public void listPushPopCoverBothEndsMissingWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertTrue(isNullPop(db.writes().lists().lpop(b("missing"), 1).value()));
            Assert.assertTrue(isNullPop(db.writes().lists().rpop(b("missing"), 1).value()));
            Assert.assertTrue(db.writes().lists().lpop(b("missing"), 1).mutationOutcome() == MutationOutcome.NONE);

            Assert.assertEquals(2L, db.writes().lists().lpush(b("list"), List.of(b("b"), b("a"))).value().longValue());
            Assert.assertEquals(4L, db.writes().lists().rpush(b("list"), List.of(b("c"), b("d"))).value().longValue());
            Assert.assertEquals(List.of("a", "b", "c", "d"), sequence(db.reads().lists().lrange(b("list"), 0, -1)));

            Assert.assertEquals(List.of("a", "b"), strings(db.writes().lists().lpop(b("list"), 2).value()));
            Assert.assertEquals(List.of("d"), strings(db.writes().lists().rpop(b("list"), 1).value()));
            Assert.assertEquals(List.of("c"), sequence(db.reads().lists().lrange(b("list"), 0, -1)));

            Assert.assertEquals(List.of("c"), strings(db.writes().lists().lpop(b("list"), 5).value()));
            Assert.assertNull(db.reads().keyspace().typeOf(view("list")));

            db.writes().lists().rpush(b("ttl-list"), List.of(b("x")));
            db.writes().ttl().pexpire(view("ttl-list"), 1);
            sleepPastTtl();
            Assert.assertTrue(isNullPop(db.writes().lists().rpop(b("ttl-list"), 1).value()));
            Assert.assertNull(db.reads().keyspace().typeOf(view("ttl-list")));

            db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.writes().lists().lpush(b("s"), List.of(b("x"))));
            expectWrongType(() -> db.writes().lists().rpop(b("s"), 1));
        });
    }

    @Test
    public void listDeletionPopBypassesPositiveGrowthAdmission() {
        withDb(db -> {
            Assert.assertEquals(2L, db.writes().lists().rpush(b("drop"), List.of(b("a"), b("b"))).value().longValue());
            Assert.assertEquals(1L, db.writes().lists().rpush(b("expired"), List.of(b("x"))).value().longValue());
            db.writes().ttl().pexpire(view("expired"), 1);
            sleepPastTtl();

            RejectingMaxmemoryCoordinator coordinator = new RejectingMaxmemoryCoordinator();
            db.attachMaxmemoryCoordinator(coordinator);

            Assert.assertEquals(List.of("a", "b"), strings(db.writes().lists().lpop(b("drop"), 10).value()));
            Assert.assertNull(db.reads().keyspace().typeOf(view("drop")));
            Assert.assertTrue(isNullPop(db.writes().lists().rpop(b("expired"), 1).value()));
            Assert.assertNull(db.reads().keyspace().typeOf(view("expired")));
            Assert.assertEquals(0, coordinator.prepareWrites());
        });
    }

    @Test
    public void partialListPopRetainsOnlyPoppedNativePayloads() {
        withDb(db -> {
            byte[] left = repeatedBytes('a', 128);
            byte[] middle = repeatedBytes('b', 128);
            byte[] right = repeatedBytes('c', 128);
            Assert.assertEquals(3L, db.writes().lists().rpush(b("list"), List.of(left, middle, right)).value().longValue());

            try (PoppedValueSequence popped = db.writes().lists().lpop(b("list"), 1).value()) {
                Assert.assertEquals(1, popped.count());
                Assert.assertEquals(136L, popped.encodedElementBytes());
                Assert.assertEquals(128L, popped.retainedMemoryBytes());
                Assert.assertEquals(
                        List.of(new String(middle, StandardCharsets.UTF_8), new String(right, StandardCharsets.UTF_8)),
                        sequence(db.reads().lists().lrange(b("list"), 0, -1))
                );
                Assert.assertEquals(List.of(new String(left, StandardCharsets.UTF_8)), sequence(popped));
            }
        });
    }

    @Test
    public void setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion() {
        withDb(db -> {
            Assert.assertEquals(0, db.reads().sets().smembers(b("missing")).count());
            Assert.assertFalse(db.reads().sets().sismember(b("missing"), b("a")));
            Assert.assertEquals(0L, db.reads().sets().scard(b("missing")));
            Assert.assertEquals(0L, db.writes().sets().srem(b("missing"), List.of(b("a"))).value().longValue());

            Assert.assertEquals(3L, db.writes().sets().sadd(b("set"), List.of(b("a"), b("b"), b("c"))).value().longValue());
            Assert.assertEquals(0L, db.writes().sets().sadd(b("set"), List.of(b("a"), b("b"))).value().longValue());
            Assert.assertEquals(3L, db.reads().sets().scard(b("set")));
            Assert.assertTrue(db.reads().sets().sismember(b("set"), b("a")));
            Assert.assertFalse(db.reads().sets().sismember(b("set"), b("missing-member")));
            Assert.assertEquals(0L, db.writes().sets().srem(b("set"), List.of(b("x"))).value().longValue());
            Assert.assertEquals(2L, db.writes().sets().srem(b("set"), List.of(b("a"), b("c"))).value().longValue());
            Assert.assertEquals(Set.of("b"), new HashSet<>(sequence(db.reads().sets().smembers(b("set")))));

            Assert.assertEquals(1L, db.writes().sets().srem(b("set"), List.of(b("b"))).value().longValue());
            Assert.assertNull(db.reads().keyspace().typeOf(view("set")));

            db.writes().sets().sadd(b("ttl-set"), List.of(b("x")));
            db.writes().ttl().pexpire(view("ttl-set"), 1);
            sleepPastTtl();
            Assert.assertFalse(db.reads().sets().sismember(b("ttl-set"), b("x")));
            Assert.assertEquals(0L, db.reads().sets().scard(b("ttl-set")));
            Assert.assertEquals(0L, db.writes().sets().srem(b("ttl-set"), List.of(b("x"))).value().longValue());
            Assert.assertNull(db.reads().keyspace().typeOf(view("ttl-set")));

            db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.reads().sets().smembers(b("s")).count());
            expectWrongType(() -> db.reads().sets().sismember(b("s"), b("v")));
            expectWrongType(() -> db.reads().sets().scard(b("s")));
            expectWrongType(() -> db.writes().sets().sadd(b("s"), List.of(b("x"))));
            expectWrongType(() -> db.writes().sets().srem(b("s"), List.of(b("x"))));
        });
    }

    @Test
    public void zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertEquals(0, db.reads().zsets().zrange(b("missing"), 0, -1, false).count());
            Assert.assertEquals(0, db.reads().zsets().zrevrange(b("missing"), 0, -1, false).count());
            Assert.assertEquals(0, db.reads().zsets().zrangeByScore(b("missing"), 0, true, 1, true, false, 0, 10).count());
            Assert.assertEquals(0, db.reads().zsets().zrevrangeByScore(b("missing"), 0, true, 1, true, false, 0, 10).count());
            Assert.assertEquals(0L, db.writes().zsets().zremrangeByScore(b("missing"), 0, true, 1, true).value().longValue());
            Assert.assertEquals(0L, db.writes().zsets().zremrangeByRank(b("missing"), 0, -1).value().longValue());
            Assert.assertEquals(0L, db.writes().zsets().zrem(b("missing"), List.of(b("a"))).value().longValue());

            db.writes().zsets().zadd(b("ttl-z-update"), List.of(b("1"), b("a")));
            db.writes().ttl().pexpire(view("ttl-z-update"), 5000);
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("ttl-z-update")) > 0L);
            Assert.assertEquals(0L, db.writes().zsets().zadd(b("ttl-z-update"), List.of(b("5"), b("a"))).value().longValue());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("ttl-z-update")) > 0L);

            Assert.assertEquals(4L, db.writes().zsets().zadd(b("z"), List.of(b("1"), b("a"), b("2"), b("b"), b("3"), b("c"), b("4"), b("d"))).value().longValue());

            Assert.assertEquals(List.of("d", "c", "b", "a"), sequence(db.reads().zsets().zrevrange(b("z"), 0, -1, false)));
            Assert.assertEquals(List.of("b", "c"), sequence(db.reads().zsets().zrangeByScore(b("z"), 1, true, 4, true, false, 0, 10)));
            Assert.assertEquals(List.of("c", "3", "b", "2"), sequence(db.reads().zsets().zrevrangeByScore(b("z"), 1, true, 4, true, true, 0, 2)));

            Assert.assertEquals(1L, db.writes().zsets().zremrangeByScore(b("z"), 2, false, 2, false).value().longValue());
            Assert.assertEquals(List.of("a", "c", "d"), sequence(db.reads().zsets().zrange(b("z"), 0, -1, false)));

            Assert.assertEquals(1L, db.writes().zsets().zremrangeByRank(b("z"), 0, 0).value().longValue());
            Assert.assertEquals(List.of("c", "d"), sequence(db.reads().zsets().zrange(b("z"), 0, -1, false)));

            Assert.assertEquals(1L, db.writes().zsets().zrem(b("z"), List.of(b("d"), b("missing"))).value().longValue());
            Assert.assertEquals(List.of("c"), sequence(db.reads().zsets().zrange(b("z"), 0, -1, false)));
            Assert.assertEquals(1L, db.writes().zsets().zremrangeByRank(b("z"), 0, -1).value().longValue());
            Assert.assertNull(db.reads().keyspace().typeOf(view("z")));

            db.writes().zsets().zadd(b("ttl-z"), List.of(b("1"), b("x")));
            db.writes().ttl().pexpire(view("ttl-z"), 1);
            sleepPastTtl();
            Assert.assertEquals(0, db.reads().zsets().zrevrange(b("ttl-z"), 0, -1, false).count());
            Assert.assertNull(db.reads().keyspace().typeOf(view("ttl-z")));

            db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.reads().zsets().zrange(b("s"), 0, -1, false).count());
            expectWrongType(() -> db.reads().zsets().zrevrange(b("s"), 0, -1, false).count());
            expectWrongType(() -> db.reads().zsets().zrangeByScore(b("s"), 0, true, 1, true, false, 0, 10).count());
            expectWrongType(() -> db.reads().zsets().zrevrangeByScore(b("s"), 0, true, 1, true, false, 0, 10).count());
            expectWrongType(() -> db.writes().zsets().zadd(b("s"), List.of(b("1"), b("v"))));
            expectWrongType(() -> db.writes().zsets().zremrangeByScore(b("s"), 0, true, 1, true));
            expectWrongType(() -> db.writes().zsets().zremrangeByRank(b("s"), 0, -1));
            expectWrongType(() -> db.writes().zsets().zrem(b("s"), List.of(b("v"))));
        });
    }

    @Test
    public void hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics() {
        withDb(db -> {
            Assert.assertEquals(0L, db.reads().hll().pfcount(List.of(b("missing"))));

            Assert.assertEquals(1, db.writes().hll().pfadd(b("h1"), List.of(b("a"), b("b"), b("c"))).value().intValue());
            Assert.assertEquals(1, db.writes().hll().pfadd(b("h2"), List.of(b("c"), b("d"), b("e"))).value().intValue());
            long h1 = db.reads().hll().pfcount(List.of(b("h1")));
            long mergedEstimate = db.reads().hll().pfcount(List.of(b("h1"), b("h2")));
            Assert.assertTrue(h1 >= 1L);
            Assert.assertTrue(mergedEstimate >= h1);

            db.writes().hll().pfadd(b("dest"), List.of(b("seed")));
            db.writes().ttl().pexpire(view("dest"), 5000);
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("dest")) > 0L);

            Assert.assertTrue(db.writes().hll().pfmerge(b("dest"), List.of(b("h1"), b("h2"))).changedAny());
            Assert.assertEquals(-1L, db.reads().ttl().ttlMillis(view("dest")));
            Assert.assertTrue(db.reads().hll().pfcount(List.of(b("dest"))) >= h1);

            db.writes().ttl().pexpire(view("h1"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.reads().hll().pfcount(List.of(b("h1"))));
            Assert.assertNull(db.reads().keyspace().typeOf(view("h1")));

            db.writes().strings().setString(b("plain"), b("not-hll"), SetMode.NORMAL, null);
            expectWrongType(() -> db.reads().hll().pfcount(List.of(b("plain"))));
            expectWrongType(() -> db.writes().hll().pfmerge(b("other"), List.of(b("plain"))));
        });
    }

    private static void withDb(DbConsumer consumer) {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            consumer.accept(db);
        } finally {
            db.shutdown();
        }
    }

    private static List<String> sequence(BulkStringSequence sequence) {
        RecordingBulkStringSink sink = new RecordingBulkStringSink();
        sequence.emitTo(sink);
        return sink.values;
    }

    private static boolean isNullPop(PoppedValueSequence values) {
        try (PoppedValueSequence owned = values) {
            return owned == null || owned.isNull();
        }
    }

    private static List<String> strings(PoppedValueSequence values) {
        if (values == null) {
            return null;
        }
        try (PoppedValueSequence owned = values) {
            if (owned.isNull()) {
                return null;
            }
            return sequence(owned);
        }
    }

    private static BytesSlice view(String text) {
        return new ArrayBytesSlice(b(text));
    }

    private static byte[] repeatedBytes(char value, int count) {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static void sleepPastTtl() {
        try {
            Thread.sleep(20L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void expectWrongType(ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected WrongTypeException");
        } catch (WrongTypeException expected) {
            // expected
        }
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    private static final class RejectingMaxmemoryCoordinator implements MaxmemoryCoordinator {
        private int prepareWrites;

        @Override
        public void prepareWrite(long estimatedExtraBytes) {
            prepareWrites++;
            if (estimatedExtraBytes > 0) {
                throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
            }
        }

        @Override
        public long nextLruClock() {
            return 0L;
        }

        private int prepareWrites() {
            return prepareWrites;
        }
    }

    private static final class RecordingBulkStringSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
        }
    }

    private static final class ArrayBytesSlice implements BytesSlice {
        private final byte[] bytes;

        private ArrayBytesSlice(byte[] bytes) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public void writeTo(yier.bubu.redis.bytes.BytesSink out) {
            out.writeBytes(bytes, 0, bytes.length);
        }

        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }
    }
}
