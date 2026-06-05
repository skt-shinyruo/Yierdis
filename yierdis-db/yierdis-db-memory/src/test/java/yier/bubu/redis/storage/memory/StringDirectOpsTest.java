package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class StringDirectOpsTest {
    @Test
    public void bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType() {
        withDb(db -> {
            db.writes().strings().setString(b("bits"), new byte[]{(byte) 0xF0, 0x0F, 0x55}, SetMode.NORMAL, null);

            Assert.assertEquals(12L, db.reads().strings().bitcount(view("bits")));
            Assert.assertEquals(8L, db.reads().strings().bitcount(view("bits"), 0, 1));
            Assert.assertEquals(8L, db.reads().strings().bitcount(view("bits"), -2, -1));
            Assert.assertEquals(0L, db.reads().strings().bitcount(view("bits"), 5, 6));
            Assert.assertEquals(0L, db.reads().strings().bitcount(view("missing")));

            db.writes().ttl().pexpire(view("bits"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.reads().strings().bitcount(view("bits")));
            Assert.assertNull(db.reads().keyspace().typeOf(view("bits")));

            db.writes().lists().rpush(b("list"), List.of(b("a")));
            expectWrongType(() -> db.reads().strings().bitcount(view("list")));
        });
    }

    @Test
    public void setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit() {
        withDb(db -> {
            Assert.assertSame(db.reads(), db.reads());
            Assert.assertSame(db.writes(), db.writes());
            Assert.assertSame(db.expiration(), db.expiration());
            Assert.assertSame(db.memory(), db.memory());
            Assert.assertSame(db.lifecycle(), db.lifecycle());

            Assert.assertTrue(db.writes().strings().setString(b("k"), b("one"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("one"), db.reads().strings().getStringBytes(b("k")));

            Assert.assertTrue(db.writes().strings().setString(b("slice"), slice("sliced"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("sliced"), db.reads().strings().getStringBytes(b("slice")));

            WriteResult<StringWriteOps.SetStringValue> nxExisting = db.writes().strings()
                    .set(b("k"), slice("two"), SetMode.NX, null, true);
            Assert.assertFalse(nxExisting.value().applied());
            Assert.assertNull(nxExisting.value().oldValue());
            Assert.assertSame(MutationOutcome.NONE, nxExisting.mutationOutcome());
            Assert.assertArrayEquals(b("one"), db.reads().strings().getStringBytes(b("k")));

            WriteResult<StringWriteOps.SetStringValue> nxMissing = db.writes().strings()
                    .set(b("new"), slice("created"), SetMode.NX, null, true);
            Assert.assertTrue(nxMissing.value().applied());
            Assert.assertNull(nxMissing.value().oldValue());
            Assert.assertArrayEquals(b("created"), db.reads().strings().getStringBytes(b("new")));

            WriteResult<StringWriteOps.SetStringValue> xxMissing = db.writes().strings()
                    .set(b("absent"), slice("ignored"), SetMode.XX, null, true);
            Assert.assertFalse(xxMissing.value().applied());
            Assert.assertNull(xxMissing.value().oldValue());
            Assert.assertSame(MutationOutcome.NONE, xxMissing.mutationOutcome());

            WriteResult<StringWriteOps.SetStringValue> xxExisting = db.writes().strings()
                    .set(b("k"), slice("three"), SetMode.XX, ExpireOption.px(5000), true);
            Assert.assertTrue(xxExisting.value().applied());
            Assert.assertArrayEquals(b("one"), xxExisting.value().oldValue());
            Assert.assertTrue(xxExisting.mutationOutcome().valueChanged());
            Assert.assertTrue(xxExisting.mutationOutcome().ttlChanged());
            Assert.assertArrayEquals(b("three"), db.reads().strings().getStringBytes(b("k")));
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);

            db.writes().strings().setString(b("k"), b("four"), SetMode.NORMAL, ExpireOption.keepTtl());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);

            db.writes().strings().setString(b("k"), b("five"), SetMode.NORMAL, null);
            Assert.assertEquals(-1L, db.reads().ttl().ttlMillis(view("k")));
        });

        YierdisDb small = new YierdisDb(null, 4, "noeviction", 5, 5, 5);
        try {
            small.bindToCurrentThread();
            try {
                small.writes().strings().setString(b("large"), b("123456789"), SetMode.NORMAL, null);
                Assert.fail("expected maxmemory rejection");
            } catch (YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("OOM"));
            }
            Assert.assertNull(small.reads().strings().getStringBytes(b("large")));
        } finally {
            small.shutdown();
        }
    }

    @Test
    public void noevictionAllowsSetOverwriteThatShrinksWhenUsedEqualsLimit() {
        byte[] key = b("k");
        byte[] largeValue = repeat((byte) 'x', 1600);
        byte[] smallValue = b("x");
        long maxmemoryBytes = usedAfterSet(key, largeValue);

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.writes().strings().setString(key, largeValue, SetMode.NORMAL, null).value());
            long usedBefore = db.usedBytesForMaxmemory();
            Assert.assertEquals(maxmemoryBytes, usedBefore);

            Assert.assertTrue(db.writes().strings().setString(key, smallValue, SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(smallValue, db.reads().strings().getStringBytes(key));
            Assert.assertTrue("shrinking overwrite should reduce used bytes",
                    db.usedBytesForMaxmemory() < usedBefore);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void noevictionRejectsSetOverwriteThatWouldGrowPastLimitWithoutChangingOldValue() {
        byte[] key = b("k");
        byte[] oldValue = b("old");
        byte[] largerValue = repeat((byte) 'x', 1600);
        long usedWithOldValue = usedAfterSet(key, oldValue);
        long maxmemoryBytes = usedWithOldValue + 1L;

        YierdisDb db = new YierdisDb(null, maxmemoryBytes, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.writes().strings().setString(key, oldValue, SetMode.NORMAL, null).value());
            long usedBeforeRejectedWrite = db.usedBytesForMaxmemory();

            try {
                db.writes().strings().setString(key, largerValue, SetMode.NORMAL, null);
                Assert.fail("expected maxmemory rejection");
            } catch (YierdisCommandException e) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
            }

            Assert.assertArrayEquals(oldValue, db.reads().strings().getStringBytes(key));
            Assert.assertEquals(usedBeforeRejectedWrite, db.usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    private static long usedAfterSet(byte[] key, byte[] value) {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());
            return db.usedBytesForMaxmemory();
        } finally {
            db.shutdown();
        }
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

    private static byte[] repeat(byte value, int len) {
        byte[] out = new byte[len];
        java.util.Arrays.fill(out, value);
        return out;
    }

    private static BytesView view(String text) {
        return slice(text);
    }

    private static BytesSlice slice(String text) {
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

        @Override
        public String toString() {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
