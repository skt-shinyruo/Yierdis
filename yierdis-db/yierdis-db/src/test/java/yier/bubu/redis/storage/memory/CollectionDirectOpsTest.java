package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class CollectionDirectOpsTest {
    @Test
    public void packedZsetPromotionPublishesTargetEncodingAndStableHeapAccounting() {
        withDb(db -> {
            long ledgerBeforeCreate = db.memoryLedger().usedBytes();
            Assert.assertEquals(
                    3L,
                    db.zsets().zadd(
                            b("zset"),
                            List.of(b("1"), b("alpha"), b("2"), b("beta"), b("3"), b("gamma"))
                    ).value().longValue()
            );
            EntryRecord before = db.keyLifecycle().liveEntryRecord(b("zset"));
            ValueHandle rootHandle = before.valueHandle();
            long usedBeforePromotion = db.memoryLedger().usedBytes();
            long heapBeforePromotion = KeyLifecycleTestAccess.inspect(db.keyLifecycle()).zsetRoot().heapBytes();
            Assert.assertEquals(ValueEncoding.ZSET_PACKED, before.encoding());

            Assert.assertEquals(
                    1L,
                    db.zsets().zadd(b("zset"), List.of(b("4"), new byte[256])).value().longValue()
            );

            EntryRecord after = db.keyLifecycle().liveEntryRecord(b("zset"));
            Assert.assertEquals(rootHandle, after.valueHandle());
            Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, after.encoding());
            Assert.assertTrue(after.version() > before.version());
            Assert.assertEquals(usedBeforePromotion, db.memoryLedger().usedBytes());
            Assert.assertTrue(KeyLifecycleTestAccess.inspect(db.keyLifecycle()).zsetRoot().heapBytes() > heapBeforePromotion);

            Assert.assertEquals(1L, db.keyspace().del(List.of(b("zset"))).value().longValue());
            Assert.assertEquals(ledgerBeforeCreate, db.memoryLedger().usedBytes());
        });
    }

    @Test
    public void existingPackedListKeepsRootAndReplacesOnlyItsBoundedBlock() {
        withDb(db -> {
            Assert.assertEquals(2L, db.lists().rpush(b("list"), List.of(b("a"), b("b"))).value().longValue());
            EntryRecord before = db.keyLifecycle().liveEntryRecord(b("list"));
            ValueHandle rootHandle = before.valueHandle();
            Set<NativeHandle> beforeBlocks = listHandles(db, rootHandle);
            Assert.assertEquals(1, beforeBlocks.size());

            Assert.assertEquals(3L, db.lists().lpush(b("list"), List.of(b("c"))).value().longValue());

            EntryRecord after = db.keyLifecycle().liveEntryRecord(b("list"));
            Assert.assertEquals(rootHandle, after.valueHandle());
            Set<NativeHandle> afterBlocks = listHandles(db, rootHandle);
            Assert.assertEquals(1, afterBlocks.size());
            Assert.assertNotEquals(beforeBlocks, afterBlocks);
            Assert.assertEquals(List.of("c", "a", "b"), sequence(db.lists().lrange(b("list"), 0, -1)));
        });
    }

    @Test
    public void quicklistEdgeCowKeepsInteriorTopologyAndPopReleasesOnlyRemovedNode() {
        withDb(db -> {
            byte[] first = repeatedBytes('a', 4096);
            byte[] second = repeatedBytes('b', 4096);
            byte[] third = repeatedBytes('c', 4096);
            Assert.assertEquals(3L, db.lists().rpush(b("list"), List.of(first, second, third)).value().longValue());

            EntryRecord before = db.keyLifecycle().liveEntryRecord(b("list"));
            ValueHandle rootHandle = before.valueHandle();
            Set<NativeHandle> objectHandles = listHandles(db, rootHandle);
            Assert.assertEquals(6, objectHandles.size());

            Assert.assertEquals(4L, db.lists().rpush(b("list"), List.of(b("tail"))).value().longValue());
            Assert.assertEquals(rootHandle, db.keyLifecycle().liveEntryRecord(b("list")).valueHandle());
            Set<NativeHandle> afterPushHandles = listHandles(db, rootHandle);
            Assert.assertEquals(6, afterPushHandles.size());
            Set<NativeHandle> unchangedHandles = new HashSet<>(objectHandles);
            unchangedHandles.retainAll(afterPushHandles);
            Assert.assertEquals(5, unchangedHandles.size());

            try (PoppedValueSequence popped = TestDbSupport.commitPop(db.lists(), b("list"), 1, true).value()) {
                Assert.assertEquals(rootHandle, db.keyLifecycle().liveEntryRecord(b("list")).valueHandle());
                Assert.assertEquals(List.of(new String(first, StandardCharsets.UTF_8)), sequence(popped));
            }
            Assert.assertEquals(
                    List.of(
                            new String(second, StandardCharsets.UTF_8),
                            new String(third, StandardCharsets.UTF_8),
                            "tail"
                    ),
                    sequence(db.lists().lrange(b("list"), 0, -1))
            );
        });
    }

    @Test
    public void collectionWriteAdmissionCoversStableBackendGrowth() {
        assertCollectionWriteAdmissionCoversPhysicalGrowth(
                "hash",
                db -> db.hashes().hset(b("hash"), List.of(b("field"), b("value")))
        );
        assertCollectionWriteAdmissionCoversPhysicalGrowth(
                "set",
                db -> db.sets().sadd(b("set"), List.of(b("member")))
        );
        assertCollectionWriteAdmissionCoversPhysicalGrowth(
                "list",
                db -> db.lists().rpush(b("list"), List.of(b("value")))
        );
    }

    @Test
    public void collectionReplacementAdmissionCoversStableBackendGrowth() {
        assertCollectionReplacementAdmissionCoversPhysicalGrowth(
                "hash replacement",
                db -> {
                    db.hashes().hset(b("hash"), List.of(b("field"), b("before")));
                    db.hashes().hset(b("hash-keeper"), List.of(b("field"), b("keeper")));
                },
                db -> db.hashes().hset(b("hash"), List.of(b("next"), b("value")))
        );
        assertCollectionReplacementAdmissionCoversPhysicalGrowth(
                "set replacement",
                db -> {
                    db.sets().sadd(b("set"), List.of(b("before")));
                    db.sets().sadd(b("set-keeper"), List.of(b("keeper")));
                },
                db -> db.sets().sadd(b("set"), List.of(b("next")))
        );
        assertCollectionReplacementAdmissionCoversPhysicalGrowth(
                "list replacement",
                db -> {
                    db.lists().rpush(b("list"), List.of(b("before")));
                    db.lists().rpush(b("list-keeper"), List.of(b("keeper")));
                },
                db -> db.lists().rpush(b("list"), List.of(b("next")))
        );
    }

    @Test
    public void hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertEquals(0L, db.hashes().hlen(b("missing")));
            Assert.assertTrue(OwnedReplyValueAssertions.isNull(db.hashes().hget(b("missing"), b("f"))));
            Assert.assertEquals(0, db.hashes().hgetall(b("missing")).pairCount());
            Assert.assertEquals(0L, db.hashes().hdel(b("missing"), List.of(b("f"))).value().longValue());

            Assert.assertEquals(2L, db.hashes().hset(b("h"), List.of(b("a"), b("1"), b("b"), b("2"))).value().longValue());
            Assert.assertEquals(2L, db.hashes().hlen(b("h")));
            Assert.assertArrayEquals(b("1"), OwnedReplyValueAssertions.bytes(db.hashes().hget(b("h"), b("a"))));
            Assert.assertTrue(OwnedReplyValueAssertions.isNull(db.hashes().hget(b("h"), b("missing-field"))));
            Assert.assertEquals(2, db.hashes().hgetall(b("h")).pairCount());

            db.ttl().pexpire(view("h"), 5000);
            Assert.assertTrue(db.ttl().ttlMillis(view("h")) > 0L);
            Assert.assertEquals(0L, db.hashes().hset(b("h"), List.of(b("a"), b("updated"))).value().longValue());
            Assert.assertArrayEquals(b("updated"), OwnedReplyValueAssertions.bytes(db.hashes().hget(b("h"), b("a"))));
            Assert.assertTrue(db.ttl().ttlMillis(view("h")) > 0L);

            Assert.assertEquals(0L, db.hashes().hdel(b("h"), List.of(b("x"))).value().longValue());
            Assert.assertEquals(1L, db.hashes().hdel(b("h"), List.of(b("a"))).value().longValue());
            Assert.assertEquals(1L, db.hashes().hlen(b("h")));

            db.ttl().pexpire(view("h"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.hashes().hlen(b("h")));
            Assert.assertNull(db.keyspace().typeOf(view("h")));

            db.strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.hashes().hget(b("s"), b("f")));
            expectWrongType(() -> db.hashes().hgetall(b("s")).pairCount());
            expectWrongType(() -> db.hashes().hlen(b("s")));
            expectWrongType(() -> db.hashes().hset(b("s"), List.of(b("f"), b("v"))));
            expectWrongType(() -> db.hashes().hdel(b("s"), List.of(b("f"))));
        });
    }

    @Test
    public void listPushPopCoverBothEndsMissingWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertTrue(isNullPop(TestDbSupport.commitPop(db.lists(), b("missing"), 1, true).value()));
            Assert.assertTrue(isNullPop(TestDbSupport.commitPop(db.lists(), b("missing"), 1, false).value()));
            Assert.assertTrue(TestDbSupport.commitPop(db.lists(), b("missing"), 1, true).mutationOutcome() == MutationOutcome.NONE);

            Assert.assertEquals(2L, db.lists().lpush(b("list"), List.of(b("b"), b("a"))).value().longValue());
            Assert.assertEquals(4L, db.lists().rpush(b("list"), List.of(b("c"), b("d"))).value().longValue());
            Assert.assertEquals(List.of("a", "b", "c", "d"), sequence(db.lists().lrange(b("list"), 0, -1)));

            Assert.assertEquals(List.of("a", "b"), strings(TestDbSupport.commitPop(db.lists(), b("list"), 2, true).value()));
            Assert.assertEquals(List.of("d"), strings(TestDbSupport.commitPop(db.lists(), b("list"), 1, false).value()));
            Assert.assertEquals(List.of("c"), sequence(db.lists().lrange(b("list"), 0, -1)));

            Assert.assertEquals(List.of("c"), strings(TestDbSupport.commitPop(db.lists(), b("list"), 5, true).value()));
            Assert.assertNull(db.keyspace().typeOf(view("list")));

            db.lists().rpush(b("ttl-list"), List.of(b("x")));
            db.ttl().pexpire(view("ttl-list"), 1);
            sleepPastTtl();
            Assert.assertTrue(isNullPop(TestDbSupport.commitPop(db.lists(), b("ttl-list"), 1, false).value()));
            Assert.assertNull(db.keyspace().typeOf(view("ttl-list")));

            db.strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.lists().lpush(b("s"), List.of(b("x"))));
            expectWrongType(() -> TestDbSupport.commitPop(db.lists(), b("s"), 1, false));
        });
    }

    @Test
    public void listDeletionPopBypassesPositiveGrowthAdmission() {
        withDb(db -> {
            Assert.assertEquals(2L, db.lists().rpush(b("drop"), List.of(b("a"), b("b"))).value().longValue());
            Assert.assertEquals(1L, db.lists().rpush(b("expired"), List.of(b("x"))).value().longValue());
            db.ttl().pexpire(view("expired"), 1);
            sleepPastTtl();

            RejectingMaxmemoryCoordinator coordinator = new RejectingMaxmemoryCoordinator();
            db.attachMaxmemoryCoordinator(coordinator);

            Assert.assertEquals(List.of("a", "b"), strings(TestDbSupport.commitPop(db.lists(), b("drop"), 10, true).value()));
            Assert.assertNull(db.keyspace().typeOf(view("drop")));
            Assert.assertTrue(isNullPop(TestDbSupport.commitPop(db.lists(), b("expired"), 1, false).value()));
            Assert.assertNull(db.keyspace().typeOf(view("expired")));
            Assert.assertEquals(0, coordinator.prepareWrites());
        });
    }

    @Test
    public void partialPackedListPopRetainsTheSharedNativeBlockOnce() {
        withDb(db -> {
            byte[] left = repeatedBytes('a', 128);
            byte[] middle = repeatedBytes('b', 128);
            byte[] right = repeatedBytes('c', 128);
            Assert.assertEquals(3L, db.lists().rpush(b("list"), List.of(left, middle, right)).value().longValue());

            try (PoppedValueSequence popped = TestDbSupport.commitPop(db.lists(), b("list"), 1, true).value()) {
                Assert.assertEquals(1, popped.elementCount());
                Assert.assertEquals(128L, payloadLengthTotal(popped));
                Assert.assertTrue(popped.retainedMemoryBytes() >= payloadLengthTotal(popped));
                Assert.assertEquals(
                        List.of(new String(middle, StandardCharsets.UTF_8), new String(right, StandardCharsets.UTF_8)),
                        sequence(db.lists().lrange(b("list"), 0, -1))
                );
                Assert.assertEquals(List.of(new String(left, StandardCharsets.UTF_8)), sequence(popped));
            }
        });
    }

    @Test
    public void setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion() {
        withDb(db -> {
            Assert.assertEquals(0, db.sets().smembers(b("missing")).elementCount());
            Assert.assertFalse(db.sets().sismember(b("missing"), b("a")));
            Assert.assertEquals(0L, db.sets().scard(b("missing")));
            Assert.assertEquals(0L, db.sets().srem(b("missing"), List.of(b("a"))).value().longValue());

            Assert.assertEquals(3L, db.sets().sadd(b("set"), List.of(b("a"), b("b"), b("c"))).value().longValue());
            Assert.assertEquals(0L, db.sets().sadd(b("set"), List.of(b("a"), b("b"))).value().longValue());
            Assert.assertEquals(3L, db.sets().scard(b("set")));
            Assert.assertTrue(db.sets().sismember(b("set"), b("a")));
            Assert.assertFalse(db.sets().sismember(b("set"), b("missing-member")));
            Assert.assertEquals(0L, db.sets().srem(b("set"), List.of(b("x"))).value().longValue());
            Assert.assertEquals(2L, db.sets().srem(b("set"), List.of(b("a"), b("c"))).value().longValue());
            Assert.assertEquals(Set.of("b"), new HashSet<>(sequence(db.sets().smembers(b("set")))));

            Assert.assertEquals(1L, db.sets().srem(b("set"), List.of(b("b"))).value().longValue());
            Assert.assertNull(db.keyspace().typeOf(view("set")));

            db.sets().sadd(b("ttl-set"), List.of(b("x")));
            db.ttl().pexpire(view("ttl-set"), 1);
            sleepPastTtl();
            Assert.assertFalse(db.sets().sismember(b("ttl-set"), b("x")));
            Assert.assertEquals(0L, db.sets().scard(b("ttl-set")));
            Assert.assertEquals(0L, db.sets().srem(b("ttl-set"), List.of(b("x"))).value().longValue());
            Assert.assertNull(db.keyspace().typeOf(view("ttl-set")));

            db.strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.sets().smembers(b("s")).elementCount());
            expectWrongType(() -> db.sets().sismember(b("s"), b("v")));
            expectWrongType(() -> db.sets().scard(b("s")));
            expectWrongType(() -> db.sets().sadd(b("s"), List.of(b("x"))));
            expectWrongType(() -> db.sets().srem(b("s"), List.of(b("x"))));
        });
    }

    @Test
    public void zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl() {
        withDb(db -> {
            Assert.assertEquals(0, db.zsets().zrange(b("missing"), 0, -1, false).elementCount());
            Assert.assertEquals(0, db.zsets().zrevrange(b("missing"), 0, -1, false).elementCount());
            Assert.assertEquals(0, db.zsets().zrangeByScore(b("missing"), 0, true, 1, true, false, 0, 10).elementCount());
            Assert.assertEquals(0, db.zsets().zrevrangeByScore(b("missing"), 0, true, 1, true, false, 0, 10).elementCount());
            Assert.assertEquals(0L, db.zsets().zremrangeByScore(b("missing"), 0, true, 1, true).value().longValue());
            Assert.assertEquals(0L, db.zsets().zremrangeByRank(b("missing"), 0, -1).value().longValue());
            Assert.assertEquals(0L, db.zsets().zrem(b("missing"), List.of(b("a"))).value().longValue());

            db.zsets().zadd(b("ttl-z-update"), List.of(b("1"), b("a")));
            db.ttl().pexpire(view("ttl-z-update"), 5000);
            Assert.assertTrue(db.ttl().ttlMillis(view("ttl-z-update")) > 0L);
            Assert.assertEquals(0L, db.zsets().zadd(b("ttl-z-update"), List.of(b("5"), b("a"))).value().longValue());
            Assert.assertTrue(db.ttl().ttlMillis(view("ttl-z-update")) > 0L);

            Assert.assertEquals(4L, db.zsets().zadd(b("z"), List.of(b("1"), b("a"), b("2"), b("b"), b("3"), b("c"), b("4"), b("d"))).value().longValue());

            Assert.assertEquals(List.of("d", "c", "b", "a"), sequence(db.zsets().zrevrange(b("z"), 0, -1, false)));
            Assert.assertEquals(List.of("b", "c"), sequence(db.zsets().zrangeByScore(b("z"), 1, true, 4, true, false, 0, 10)));
            Assert.assertEquals(List.of("c", "3", "b", "2"), sequence(db.zsets().zrevrangeByScore(b("z"), 1, true, 4, true, true, 0, 2)));

            Assert.assertEquals(1L, db.zsets().zremrangeByScore(b("z"), 2, false, 2, false).value().longValue());
            Assert.assertEquals(List.of("a", "c", "d"), sequence(db.zsets().zrange(b("z"), 0, -1, false)));

            Assert.assertEquals(1L, db.zsets().zremrangeByRank(b("z"), 0, 0).value().longValue());
            Assert.assertEquals(List.of("c", "d"), sequence(db.zsets().zrange(b("z"), 0, -1, false)));

            Assert.assertEquals(1L, db.zsets().zrem(b("z"), List.of(b("d"), b("missing"))).value().longValue());
            Assert.assertEquals(List.of("c"), sequence(db.zsets().zrange(b("z"), 0, -1, false)));
            Assert.assertEquals(1L, db.zsets().zremrangeByRank(b("z"), 0, -1).value().longValue());
            Assert.assertNull(db.keyspace().typeOf(view("z")));

            db.zsets().zadd(b("ttl-z"), List.of(b("1"), b("x")));
            db.ttl().pexpire(view("ttl-z"), 1);
            sleepPastTtl();
            Assert.assertEquals(0, db.zsets().zrevrange(b("ttl-z"), 0, -1, false).elementCount());
            Assert.assertNull(db.keyspace().typeOf(view("ttl-z")));

            db.strings().setString(b("s"), b("v"), SetMode.NORMAL, null);
            expectWrongType(() -> db.zsets().zrange(b("s"), 0, -1, false).elementCount());
            expectWrongType(() -> db.zsets().zrevrange(b("s"), 0, -1, false).elementCount());
            expectWrongType(() -> db.zsets().zrangeByScore(b("s"), 0, true, 1, true, false, 0, 10).elementCount());
            expectWrongType(() -> db.zsets().zrevrangeByScore(b("s"), 0, true, 1, true, false, 0, 10).elementCount());
            expectWrongType(() -> db.zsets().zadd(b("s"), List.of(b("1"), b("v"))));
            expectWrongType(() -> db.zsets().zremrangeByScore(b("s"), 0, true, 1, true));
            expectWrongType(() -> db.zsets().zremrangeByRank(b("s"), 0, -1));
            expectWrongType(() -> db.zsets().zrem(b("s"), List.of(b("v"))));
        });
    }

    @Test
    public void hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics() {
        withDb(db -> {
            Assert.assertEquals(0L, db.hll().pfcount(List.of(b("missing"))));

            Assert.assertEquals(1, db.hll().pfadd(b("h1"), List.of(b("a"), b("b"), b("c"))).value().intValue());
            Assert.assertEquals(1, db.hll().pfadd(b("h2"), List.of(b("c"), b("d"), b("e"))).value().intValue());
            long h1 = db.hll().pfcount(List.of(b("h1")));
            long mergedEstimate = db.hll().pfcount(List.of(b("h1"), b("h2")));
            Assert.assertTrue(h1 >= 1L);
            Assert.assertTrue(mergedEstimate >= h1);

            db.hll().pfadd(b("dest"), List.of(b("seed")));
            db.ttl().pexpire(view("dest"), 5000);
            Assert.assertTrue(db.ttl().ttlMillis(view("dest")) > 0L);

            Assert.assertTrue(db.hll().pfmerge(b("dest"), List.of(b("h1"), b("h2")))
                    .mutationOutcome().changedAny());
            Assert.assertEquals(-1L, db.ttl().ttlMillis(view("dest")));
            Assert.assertTrue(db.hll().pfcount(List.of(b("dest"))) >= h1);

            db.ttl().pexpire(view("h1"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.hll().pfcount(List.of(b("h1"))));
            Assert.assertNull(db.keyspace().typeOf(view("h1")));

            db.strings().setString(b("plain"), b("not-hll"), SetMode.NORMAL, null);
            expectWrongType(() -> db.hll().pfcount(List.of(b("plain"))));
            expectWrongType(() -> db.hll().pfmerge(b("other"), List.of(b("plain"))));
        });
    }

    private static void withDb(DbConsumer consumer) {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            consumer.accept(db);
        } finally {
            db.shutdown();
        }
    }

    private static void assertCollectionWriteAdmissionCoversPhysicalGrowth(
            String label,
            DbMutation mutation
    ) {
        assertCollectionReplacementAdmissionCoversPhysicalGrowth(label, db -> {
        }, mutation);
    }

    private static void assertCollectionReplacementAdmissionCoversPhysicalGrowth(
            String label,
            DbMutation setup,
            DbMutation mutation
    ) {
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                yier.bubu.redis.storage.api.MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                null,
                8_192
        );
        try {
            db.bindToCurrentThread();
            setup.apply(db);

            RecordingMaxmemoryCoordinator coordinator = new RecordingMaxmemoryCoordinator();
            db.attachMaxmemoryCoordinator(coordinator);
            long before = db.memoryUsage().effectiveBytesForMaxmemory();
            mutation.apply(db);
            long after = db.memoryUsage().effectiveBytesForMaxmemory();

            Assert.assertTrue(label + " write must grow the physical snapshot", after > before);
            Assert.assertTrue(
                    label + " admission did not cover committed physical growth: admission="
                            + coordinator.maximumEstimatedExtraBytes() + ", growth=" + (after - before),
                    coordinator.maximumEstimatedExtraBytes() >= after - before
            );
        } finally {
            db.shutdown();
        }
    }

    private static List<String> sequence(ByteSequenceSource sequence) {
        RecordingByteValueSink sink = new RecordingByteValueSink();
        sequence.emitTo(sink);
        return sink.values;
    }

    private static long payloadLengthTotal(ByteSequenceSource source) {
        long[] total = {0L};
        source.visitElementLengths(length -> {
            if (length >= 0) {
                total[0] += length;
            }
        });
        return total[0];
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

    private static Set<NativeHandle> listHandles(YierdisDb db, ValueHandle rootHandle) {
        Set<NativeHandle> handles = new HashSet<>();
        KeyLifecycleTestAccess.inspect(db.keyLifecycle()).listRoot().forEachNativeHandle(rootHandle, handles::add);
        return handles;
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
    private interface DbMutation {
        void apply(YierdisDb db);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    private static final class RejectingMaxmemoryCoordinator implements MaxmemoryCoordinator {
        private int prepareWrites;

        @Override
        public void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes) {
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

    private static final class RecordingMaxmemoryCoordinator implements MaxmemoryCoordinator {
        private long maximumEstimatedExtraBytes;
        private boolean prepareWriteCalled;

        @Override
        public void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes) {
            prepareWriteCalled = true;
            maximumEstimatedExtraBytes = Math.max(maximumEstimatedExtraBytes, estimatedExtraBytes);
        }

        @Override
        public long nextLruClock() {
            return 0L;
        }

        private long maximumEstimatedExtraBytes() {
            Assert.assertTrue("collection write did not reserve admission", prepareWriteCalled);
            return maximumEstimatedExtraBytes;
        }
    }

    private static final class RecordingByteValueSink implements ByteValueSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            values.add(null);
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
