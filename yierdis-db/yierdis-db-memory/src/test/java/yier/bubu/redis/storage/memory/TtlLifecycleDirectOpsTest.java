package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class TtlLifecycleDirectOpsTest {
    @Test
    public void ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup() {
        withDb(db -> {
            Assert.assertEquals(-2L, db.reads().ttl().ttlMillis(view("missing")));

            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertEquals(-1L, db.reads().ttl().ttlMillis(view("k")));
            Assert.assertFalse(db.writes().ttl().expireAtSeconds(view("missing"), futureSeconds()).value());

            Assert.assertTrue(db.writes().ttl().expireAtSeconds(view("k"), futureSeconds()).value());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);
            Assert.assertTrue(db.writes().ttl().persist(view("k")).value());
            Assert.assertEquals(-1L, db.reads().ttl().ttlMillis(view("k")));

            Assert.assertTrue(db.writes().ttl().expireAtMillis(view("k"), System.currentTimeMillis() + 5000L).value());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);

            db.writes().strings().setString(b("expired"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expireAtMillis(view("expired"), System.currentTimeMillis() + 1L).value());
            sleepPastTtl();
            db.runMaintenance();
            Assert.assertNull(db.reads().keyspace().typeOf(view("expired")));
            Assert.assertEquals(-2L, db.reads().ttl().ttlMillis(view("expired")));
        });
    }

    @Test
    public void lifecycleFlushDbAndMemoryObjectApisCoverExistingMissingAndAccessors() {
        withDb(db -> {
            Assert.assertSame(db.reads(), db.reads());
            Assert.assertSame(db.writes(), db.writes());
            Assert.assertSame(db.memory(), db.memory());
            Assert.assertSame(db.lifecycle(), db.lifecycle());

            Assert.assertEquals(-1L, db.memory().memoryUsage(view("missing")));
            Assert.assertNull(db.memory().objectEncoding(view("missing")));

            db.writes().strings().setString(b("s"), b("123"), SetMode.NORMAL, null);
            db.writes().lists().rpush(b("l"), List.of(b("a"), b("b")));
            Assert.assertTrue(db.memory().memoryUsage(view("s")) > 0L);
            Assert.assertTrue(db.memory().memoryUsage(view("l")) > 0L);
            Assert.assertEquals("int", db.memory().objectEncoding(view("s")));
            Assert.assertNotNull(db.memory().objectEncoding(view("l")));
            Assert.assertTrue(db.memory().memoryStats().keyCount() >= 2L);

            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.lifecycle().flushDb());
            Assert.assertNull(db.reads().keyspace().typeOf(view("s")));
            Assert.assertEquals(-1L, db.memory().memoryUsage(view("s")));
            Assert.assertEquals(0L, db.memory().memoryStats().keyCount());
            NativeAllocatorStats empty = db.stableMemoryBackend().stats();
            Assert.assertEquals(0L, empty.objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(0L, empty.objectCount(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(0L, empty.liveObjects());
            Assert.assertSame(MutationOutcome.NONE, db.lifecycle().flushDb());
        });
    }

    @Test
    public void asyncFlushDetachesEachGenerationAndMaintenanceCannotDeleteNewKeys() {
        withDb(db -> {
            db.writes().strings().setString(b("same"), b("old"), SetMode.NORMAL, null);
            db.writes().lists().rpush(b("old-list"), List.of(b("a"), b("b")));

            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.lifecycle().flushDbAsync());
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(2, db.detachedEntryCount());
            NativeAllocatorStats firstDetached = db.stableMemoryBackend().stats();
            Assert.assertEquals(2L, firstDetached.objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(2L, firstDetached.objectCount(NativeObjectKind.ENTRY_RECORD));

            db.writes().strings().setString(b("same"), b("middle"), SetMode.NORMAL, null);
            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.lifecycle().flushDbAsync());
            Assert.assertEquals(3, db.detachedEntryCount());

            db.writes().strings().setString(b("same"), b("new"), SetMode.NORMAL, null);
            Assert.assertArrayEquals(b("new"), db.reads().strings().getStringBytes(b("same")));
            db.runDeferredReclamation();

            Assert.assertEquals(0, db.detachedEntryCount());
            Assert.assertArrayEquals(b("new"), db.reads().strings().getStringBytes(b("same")));
            Assert.assertNull(db.reads().keyspace().typeOf(view("old-list")));
            NativeAllocatorStats reclaimed = db.stableMemoryBackend().stats();
            Assert.assertEquals(1L, reclaimed.objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(1L, reclaimed.objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(1L, reclaimed.objectCount(NativeObjectKind.STRING_BYTES));
        });
    }

    @Test
    public void nonPositiveRelativeAndPastAbsoluteExpirationsDeleteImmediately() {
        withDb(db -> {
            Assert.assertFalse(db.writes().ttl().persist(view("missing")).value());

            db.writes().strings().setString(b("persistent"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.writes().ttl().persist(view("persistent")).value());

            db.writes().strings().setString(b("seconds"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view("seconds"), 0).value());
            Assert.assertNull(db.reads().keyspace().typeOf(view("seconds")));

            db.writes().strings().setString(b("millis"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().pexpire(view("millis"), -1).value());
            Assert.assertNull(db.reads().keyspace().typeOf(view("millis")));

            db.writes().strings().setString(b("absolute"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expireAtMillis(view("absolute"), 0).value());
            Assert.assertEquals(-2L, db.reads().ttl().ttlSeconds(view("absolute")));
        });
    }

    @Test
    public void overflowingRelativeAndAbsoluteExpirationsClampToFarFuture() {
        withDb(db -> {
            db.writes().strings().setString(b("seconds"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view("seconds"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("seconds")) > 0L);

            db.writes().strings().setString(b("millis"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().pexpire(view("millis"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.reads().ttl().ttlSeconds(view("millis")) > 0L);

            db.writes().strings().setString(b("absolute"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expireAtSeconds(view("absolute"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("absolute")) > 0L);
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

    private static long futureSeconds() {
        return (System.currentTimeMillis() / 1000L) + 60L;
    }

    private static BytesSlice view(String text) {
        return new ArrayBytesSlice(b(text));
    }

    private static void sleepPastTtl() {
        try {
            Thread.sleep(20L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }

    private static final class ArrayBytesSlice implements BytesSlice {
        private final byte[] bytes;

        private ArrayBytesSlice(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public void writeTo(BytesSink out) {
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
