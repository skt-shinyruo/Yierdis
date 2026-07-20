package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

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
            try (StringWriteOps.SetStringValue value = nxExisting.value()) {
                Assert.assertFalse(value.applied());
                Assert.assertTrue(value.oldValue().isNull());
            }
            Assert.assertSame(MutationOutcome.NONE, nxExisting.mutationOutcome());
            Assert.assertArrayEquals(b("one"), db.reads().strings().getStringBytes(b("k")));

            WriteResult<StringWriteOps.SetStringValue> nxMissing = db.writes().strings()
                    .set(b("new"), slice("created"), SetMode.NX, null, true);
            try (StringWriteOps.SetStringValue value = nxMissing.value()) {
                Assert.assertTrue(value.applied());
                Assert.assertTrue(value.oldValue().isNull());
            }
            Assert.assertArrayEquals(b("created"), db.reads().strings().getStringBytes(b("new")));

            WriteResult<StringWriteOps.SetStringValue> xxMissing = db.writes().strings()
                    .set(b("absent"), slice("ignored"), SetMode.XX, null, true);
            try (StringWriteOps.SetStringValue value = xxMissing.value()) {
                Assert.assertFalse(value.applied());
                Assert.assertTrue(value.oldValue().isNull());
            }
            Assert.assertSame(MutationOutcome.NONE, xxMissing.mutationOutcome());

            WriteResult<StringWriteOps.SetStringValue> xxExisting = db.writes().strings()
                    .set(b("k"), slice("three"), SetMode.XX, ExpireOption.px(5000), true);
            try (StringWriteOps.SetStringValue value = xxExisting.value()) {
                Assert.assertTrue(value.applied());
                Assert.assertArrayEquals(b("one"), bulkStringBytes(value.oldValue()));
            }
            Assert.assertTrue(xxExisting.mutationOutcome().valueChanged());
            Assert.assertTrue(xxExisting.mutationOutcome().ttlChanged());
            Assert.assertArrayEquals(b("three"), db.reads().strings().getStringBytes(b("k")));
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);

            db.writes().strings().setString(b("k"), b("four"), SetMode.NORMAL, ExpireOption.keepTtl());
            Assert.assertTrue(db.reads().ttl().ttlMillis(view("k")) > 0L);

            db.writes().strings().setString(b("k"), b("five"), SetMode.NORMAL, null);
            Assert.assertEquals(-1L, db.reads().ttl().ttlMillis(view("k")));
        });

        YierdisDb small = YierdisDb.createWithOwnedFfmRuntime(4, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
    public void noevictionAllowsSetOverwriteThatShrinksWithTransientHeadroom() {
        byte[] key = b("k");
        byte[] largeValue = repeat((byte) 'x', 1600);
        byte[] smallValue = b("x");
        long maxmemoryBytes = maxmemoryThatAllowsSetAndOverwrite(key, largeValue, smallValue);

        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.writes().strings().setString(key, largeValue, SetMode.NORMAL, null).value());
            long usedBefore = db.usedBytesForMaxmemory();

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
        byte[] largerValue = repeat((byte) 'x', 64 * 1024);
        OverwriteAdmissionCase rejectionCase = maxmemoryThatAllowsInitialSetAndRejectsOverwrite(key, oldValue, largerValue);
        largerValue = rejectionCase.overwriteValue();
        long maxmemoryBytes = rejectionCase.maxmemoryBytes();

        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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

    @Test
    public void allkeysLruEvictsOldStringDuringSetWithoutInternalFailure() {
        byte[] value = repeat((byte) 'x', 64 * 1024);
        long maxmemoryBytes = maxmemoryThatFitsOneLocalStringButNotTwo(value);
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(maxmemoryBytes, MaxmemoryPolicy.ALLKEYS_LRU, 1000, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.writes().strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(db.writes().strings().setString(b("c"), value, SetMode.NORMAL, null).value());

            Assert.assertNull(db.reads().strings().getStringBytes(b("a")));
            Assert.assertArrayEquals(value, db.reads().strings().getStringBytes(b("c")));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void appendReservesTheFullReplacementValueAcrossNativePageBoundary() {
        withDb(db -> {
            byte[] initial = repeat((byte) 'x', 64 * 1024);
            Assert.assertTrue(db.writes().strings().setString(b("k"), initial, SetMode.NORMAL, null).value());

            Assert.assertEquals(Long.valueOf(initial.length + 1L),
                    db.writes().strings().append(b("k"), slice("!")).value());

            byte[] expected = java.util.Arrays.copyOf(initial, initial.length + 1);
            expected[expected.length - 1] = '!';
            Assert.assertArrayEquals(expected, db.reads().strings().getStringBytes(b("k")));
        });
    }

    @Test
    public void appendAndSetbitNoopsKeepTheExistingNativeString() {
        withDb(db -> {
            Assert.assertEquals(Long.valueOf(1L), db.writes().strings().incrBy(b("number"), 1L).value());
            EntryRecord integerBefore = db.keyLifecycle().liveEntryRecord(b("number"));
            long stringObjectsBefore = db.nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES);

            WriteResult<Long> append = db.writes().strings().append(b("number"), slice(""));

            Assert.assertEquals(Long.valueOf(1L), append.value());
            Assert.assertSame(MutationOutcome.NONE, append.mutationOutcome());
            EntryRecord integerAfter = db.keyLifecycle().liveEntryRecord(b("number"));
            Assert.assertEquals(integerBefore.valueHandle(), integerAfter.valueHandle());
            Assert.assertEquals(ValueEncoding.STRING_INT, integerAfter.encoding());
            Assert.assertEquals(integerBefore.version(), integerAfter.version());
            Assert.assertEquals(
                    stringObjectsBefore,
                    db.nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES)
            );

            Assert.assertTrue(db.writes().strings().setString(
                    b("bits"),
                    new byte[]{(byte) 0x80},
                    SetMode.NORMAL,
                    null
            ).value());
            EntryRecord bitsBefore = db.keyLifecycle().liveEntryRecord(b("bits"));
            stringObjectsBefore = db.nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES);

            WriteResult<Integer> setbit = db.writes().strings().setBit(b("bits"), 0L, 1);

            Assert.assertEquals(Integer.valueOf(1), setbit.value());
            Assert.assertSame(MutationOutcome.NONE, setbit.mutationOutcome());
            EntryRecord bitsAfter = db.keyLifecycle().liveEntryRecord(b("bits"));
            Assert.assertEquals(bitsBefore.valueHandle(), bitsAfter.valueHandle());
            Assert.assertEquals(
                    stringObjectsBefore,
                    db.nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES)
            );
        });
    }

    @Test
    public void setGetReturnsOwnedSupersededNativeValue() {
        withDb(db -> {
            byte[] key = b("k");
            Assert.assertTrue(db.writes().strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            long liveObjectsBeforeSetGet = db.nativeAllocator().stats().liveObjects();

            StringWriteOps.SetStringValue result = db.writes().strings()
                    .set(key, slice("new"), SetMode.NORMAL, null, true)
                    .value();
            Assert.assertTrue(result.applied());
            Assert.assertArrayEquals(b("old"), bulkStringBytes(result.oldValue()));
            Assert.assertArrayEquals(b("new"), db.reads().strings().getStringBytes(key));
            Assert.assertTrue("SET GET old value should retain the superseded native allocation",
                    result.oldValue().retainedMemoryBytes() > 0L);
            Assert.assertTrue("old and replacement values should both remain live until the result closes",
                    db.nativeAllocator().stats().liveObjects() > liveObjectsBeforeSetGet);

            result.close();
            result.close();
            Assert.assertEquals(liveObjectsBeforeSetGet, db.nativeAllocator().stats().liveObjects());
        });
    }

    @Test
    public void setOverExpiredLargeKeyReservesAsMissingKey() {
        withDb(db -> {
            byte[] key = repeat((byte) 'k', 33_000);
            Assert.assertTrue(db.writes().strings().setString(key, b("old"), SetMode.NORMAL, ExpireOption.px(0)).value());

            Assert.assertTrue(db.writes().strings().setString(key, b("new"), SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(b("new"), db.reads().strings().getStringBytes(key));
        });
    }

    private static OverwriteAdmissionCase maxmemoryThatAllowsInitialSetAndRejectsOverwrite(
            byte[] key,
            byte[] initialValue,
            byte[] overwriteValue
    ) {
        long initialLimit = minMaxmemoryThatAllowsSet(key, initialValue);
        byte[] candidate = overwriteValue;
        while (candidate.length <= 16 * 1024 * 1024) {
            if (allowsInitialSetAndRejectsOverwrite(initialLimit, key, initialValue, candidate)) {
                return new OverwriteAdmissionCase(initialLimit, candidate);
            }
            candidate = repeat((byte) 'x', Math.multiplyExact(candidate.length, 4));
        }
        throw new AssertionError("could not find maxmemory limit for overwrite rejection");
    }

    private record OverwriteAdmissionCase(long maxmemoryBytes, byte[] overwriteValue) {
    }

    private static long maxmemoryThatAllowsSetAndOverwrite(byte[] key, byte[] initialValue, byte[] overwriteValue) {
        long limit = minMaxmemoryThatAllowsSet(key, initialValue);
        while (!allowsInitialSetAndOverwrite(limit, key, initialValue, overwriteValue)) {
            if (limit > Long.MAX_VALUE / 2L) {
                throw new AssertionError("could not find maxmemory limit for overwrite success");
            }
            limit *= 2L;
        }
        return limit;
    }

    private static long minMaxmemoryThatAllowsSet(byte[] key, byte[] value) {
        long high = 1L;
        while (!allowsSet(high, key, value)) {
            if (high > Long.MAX_VALUE / 2L) {
                throw new AssertionError("could not find maxmemory limit for initial set");
            }
            high *= 2L;
        }

        long low = high / 2L;
        while (low + 1L < high) {
            long mid = low + ((high - low) / 2L);
            if (allowsSet(mid, key, value)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsSet(long limit, byte[] key, byte[] value) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(key, value, SetMode.NORMAL, null);
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

    private static boolean allowsInitialSetAndOverwrite(long limit, byte[] key, byte[] initialValue, byte[] overwriteValue) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(key, initialValue, SetMode.NORMAL, null);
            db.writes().strings().setString(key, overwriteValue, SetMode.NORMAL, null);
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

    private static boolean allowsInitialSetAndRejectsOverwrite(long limit, byte[] key, byte[] initialValue, byte[] overwriteValue) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(key, initialValue, SetMode.NORMAL, null);
            try {
                db.writes().strings().setString(key, overwriteValue, SetMode.NORMAL, null);
                return false;
            } catch (YierdisCommandException e) {
                return MaxmemoryErrors.OOM_ERR.equals(e.getMessage());
            }
        } catch (YierdisCommandException ignored) {
            if (MaxmemoryErrors.OOM_ERR.equals(ignored.getMessage())) {
                return false;
            }
            throw ignored;
        } finally {
            db.shutdown();
        }
    }

    private static long maxmemoryThatFitsOneLocalStringButNotTwo(byte[] value) {
        long usedAfterOne = probeLocalStringUsedBytes(value, 1);
        long usedAfterTwo = probeLocalStringUsedBytes(value, 2);
        Assert.assertTrue("probe must show physical growth for the second local key", usedAfterTwo > usedAfterOne);
        return usedAfterOne + ((usedAfterTwo - usedAfterOne) / 2L);
    }

    private static long probeLocalStringUsedBytes(byte[] value, int keyCount) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(100_000_000L, MaxmemoryPolicy.ALLKEYS_LRU, 1000, 5, 5);
        try {
            db.bindToCurrentThread();
            if (keyCount >= 1) {
                db.writes().strings().setString(b("a"), value, SetMode.NORMAL, null);
            }
            if (keyCount >= 2) {
                db.writes().strings().setString(b("c"), value, SetMode.NORMAL, null);
            }
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

    private static byte[] bulkStringBytes(BulkStringValue value) {
        final byte[][] captured = new byte[1][];
        value.writeTo(new BulkStringSink() {
            @Override
            public void bulkString(byte[] data) {
                captured[0] = data == null ? null : java.util.Arrays.copyOf(data, data.length);
            }

            @Override
            public void bulkString(byte[] data, int off, int len) {
                captured[0] = java.util.Arrays.copyOfRange(data, off, off + len);
            }

            @Override
            public void bulkString(BytesSlice slice) {
                byte[] data = new byte[slice.length()];
                slice.getBytes(0, data, 0, data.length);
                captured[0] = data;
            }

            @Override
            public void bulkStringLongAscii(long value) {
                captured[0] = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
            }
        });
        return captured[0];
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
