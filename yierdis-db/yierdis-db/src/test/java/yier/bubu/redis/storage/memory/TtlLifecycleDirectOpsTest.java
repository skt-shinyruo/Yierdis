package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class TtlLifecycleDirectOpsTest {
    @Test
    public void ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup() {
        withDb(db -> {
            Assert.assertEquals(-2L, db.ttl().ttlMillis(view("missing")));

            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertEquals(-1L, db.ttl().ttlMillis(view("k")));
            Assert.assertFalse(db.ttl().expireAtSeconds(view("missing"), futureSeconds()).value());

            Assert.assertTrue(db.ttl().expireAtSeconds(view("k"), futureSeconds()).value());
            Assert.assertTrue(db.ttl().ttlMillis(view("k")) > 0L);
            Assert.assertTrue(db.ttl().persist(view("k")).value());
            Assert.assertEquals(-1L, db.ttl().ttlMillis(view("k")));

            Assert.assertTrue(db.ttl().expireAtMillis(view("k"), System.currentTimeMillis() + 5000L).value());
            Assert.assertTrue(db.ttl().ttlMillis(view("k")) > 0L);

            db.strings().setString(b("expired"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expireAtMillis(view("expired"), System.currentTimeMillis() + 1L).value());
            sleepPastTtl();
            db.runMaintenance();
            Assert.assertNull(db.keyspace().typeOf(view("expired")));
            Assert.assertEquals(-2L, db.ttl().ttlMillis(view("expired")));
        });
    }

    @Test
    public void lifecycleFlushDbAndMemoryObjectApisCoverExistingMissingAndAccessors() {
        withDb(db -> {
            Assert.assertEquals(-1L, db.memoryUsage(view("missing")));
            Assert.assertNull(db.objectEncoding(view("missing")));

            db.strings().setString(b("s"), b("123"), SetMode.NORMAL, null);
            db.lists().rpush(b("l"), List.of(b("a"), b("b")));
            Assert.assertTrue(db.memoryUsage(view("s")) > 0L);
            Assert.assertTrue(db.memoryUsage(view("l")) > 0L);
            Assert.assertEquals("int", db.objectEncoding(view("s")));
            Assert.assertNotNull(db.objectEncoding(view("l")));
            Assert.assertTrue(db.memoryStats().keyCount() >= 2L);

            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.flushDb());
            Assert.assertNull(db.keyspace().typeOf(view("s")));
            Assert.assertEquals(-1L, db.memoryUsage(view("s")));
            Assert.assertEquals(0L, db.memoryStats().keyCount());
            Assert.assertEquals(0L, KeyLifecycleTestAccess.backend(db).stats().liveObjects());
            Assert.assertSame(MutationOutcome.NONE, db.flushDb());
        });
    }

    @Test
    public void asyncFlushDetachesEachGenerationAndMaintenanceCannotDeleteNewKeys() {
        withDb(db -> {
            db.strings().setString(b("same"), b("old"), SetMode.NORMAL, null);
            db.lists().rpush(b("old-list"), List.of(b("a"), b("b")));

            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.flushDbAsync());
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(2, db.detachedEntryCount());

            db.strings().setString(b("same"), b("middle"), SetMode.NORMAL, null);
            Assert.assertSame(MutationOutcome.VALUE_CHANGED, db.flushDbAsync());
            Assert.assertEquals(3, db.detachedEntryCount());

            db.strings().setString(b("same"), b("new"), SetMode.NORMAL, null);
            Assert.assertArrayEquals(b("new"), OwnedReplyValueAssertions.stringValue(db.strings(), b("same")));
            db.runDeferredReclamation();

            Assert.assertEquals(0, db.detachedEntryCount());
            Assert.assertArrayEquals(b("new"), OwnedReplyValueAssertions.stringValue(db.strings(), b("same")));
            Assert.assertNull(db.keyspace().typeOf(view("old-list")));
        });
    }

    @Test
    public void nonPositiveRelativeAndPastAbsoluteExpirationsDeleteImmediately() {
        withDb(db -> {
            Assert.assertFalse(db.ttl().persist(view("missing")).value());

            db.strings().setString(b("persistent"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.ttl().persist(view("persistent")).value());

            db.strings().setString(b("seconds"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("seconds"), 0).value());
            Assert.assertNull(db.keyspace().typeOf(view("seconds")));

            db.strings().setString(b("millis"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().pexpire(view("millis"), -1).value());
            Assert.assertNull(db.keyspace().typeOf(view("millis")));

            db.strings().setString(b("absolute"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expireAtMillis(view("absolute"), 0).value());
            Assert.assertEquals(-2L, db.ttl().ttlSeconds(view("absolute")));
        });
    }

    @Test
    public void overflowingRelativeAndAbsoluteExpirationsClampToFarFuture() {
        withDb(db -> {
            db.strings().setString(b("seconds"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("seconds"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.ttl().ttlMillis(view("seconds")) > 0L);

            db.strings().setString(b("millis"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().pexpire(view("millis"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.ttl().ttlSeconds(view("millis")) > 0L);

            db.strings().setString(b("absolute"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expireAtSeconds(view("absolute"), Long.MAX_VALUE).value());
            Assert.assertTrue(db.ttl().ttlMillis(view("absolute")) > 0L);
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
