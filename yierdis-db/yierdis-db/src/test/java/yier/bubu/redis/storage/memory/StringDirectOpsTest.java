package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class StringDirectOpsTest {
    @Test
    public void bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType() {
        withDb(db -> {
            db.strings().setString(b("bits"), new byte[]{(byte) 0xF0, 0x0F, 0x55}, SetMode.NORMAL, null);

            Assert.assertEquals(12L, db.strings().bitcount(view("bits")));
            Assert.assertEquals(8L, db.strings().bitcount(view("bits"), 0, 1));
            Assert.assertEquals(8L, db.strings().bitcount(view("bits"), -2, -1));
            Assert.assertEquals(0L, db.strings().bitcount(view("bits"), 5, 6));
            Assert.assertEquals(0L, db.strings().bitcount(view("missing")));

            db.ttl().pexpire(view("bits"), 1);
            sleepPastTtl();
            Assert.assertEquals(0L, db.strings().bitcount(view("bits")));
            Assert.assertNull(db.keyspace().typeOf(view("bits")));

            db.lists().rpush(b("list"), List.of(b("a")));
            expectWrongType(() -> db.strings().bitcount(view("list")));
        });
    }

    @Test
    public void setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit() {
        withDb(db -> {
            Assert.assertTrue(db.strings().setString(b("k"), b("one"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("one"), OwnedReplyValueAssertions.stringValue(db.strings(), b("k")));

            Assert.assertTrue(db.strings().setString(b("slice"), slice("sliced"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("sliced"), OwnedReplyValueAssertions.stringValue(db.strings(), b("slice")));

            WriteResult<StringOps.SetStringValue> nxExisting = TestDbSupport.commitSetWithOldValue(
                    db.strings(), b("k"), slice("two"), SetMode.NX, null
            );
            try (StringOps.SetStringValue value = nxExisting.value()) {
                Assert.assertFalse(value.applied());
                Assert.assertArrayEquals(b("one"), byteValueBytes(value.oldValue()));
            }
            Assert.assertSame(MutationOutcome.NONE, nxExisting.mutationOutcome());
            Assert.assertArrayEquals(b("one"), OwnedReplyValueAssertions.stringValue(db.strings(), b("k")));

            WriteResult<StringOps.SetStringValue> nxMissing = TestDbSupport.commitSetWithOldValue(
                    db.strings(), b("new"), slice("created"), SetMode.NX, null
            );
            try (StringOps.SetStringValue value = nxMissing.value()) {
                Assert.assertTrue(value.applied());
                Assert.assertTrue(value.oldValue().payloadLength() < 0);
            }
            Assert.assertArrayEquals(b("created"), OwnedReplyValueAssertions.stringValue(db.strings(), b("new")));

            WriteResult<StringOps.SetStringValue> xxMissing = TestDbSupport.commitSetWithOldValue(
                    db.strings(), b("absent"), slice("ignored"), SetMode.XX, null
            );
            try (StringOps.SetStringValue value = xxMissing.value()) {
                Assert.assertFalse(value.applied());
                Assert.assertTrue(value.oldValue().payloadLength() < 0);
            }
            Assert.assertSame(MutationOutcome.NONE, xxMissing.mutationOutcome());

            WriteResult<StringOps.SetStringValue> xxExisting = TestDbSupport.commitSetWithOldValue(
                    db.strings(), b("k"), slice("three"), SetMode.XX, ExpireOption.px(5000)
            );
            try (StringOps.SetStringValue value = xxExisting.value()) {
                Assert.assertTrue(value.applied());
                Assert.assertArrayEquals(b("one"), byteValueBytes(value.oldValue()));
            }
            Assert.assertTrue(xxExisting.mutationOutcome().valueChanged());
            Assert.assertTrue(xxExisting.mutationOutcome().ttlChanged());
            Assert.assertArrayEquals(b("three"), OwnedReplyValueAssertions.stringValue(db.strings(), b("k")));
            Assert.assertTrue(db.ttl().ttlMillis(view("k")) > 0L);

            db.strings().setString(b("k"), b("four"), SetMode.NORMAL, ExpireOption.keepTtl());
            Assert.assertTrue(db.ttl().ttlMillis(view("k")) > 0L);

            db.strings().setString(b("k"), b("five"), SetMode.NORMAL, null);
            Assert.assertEquals(-1L, db.ttl().ttlMillis(view("k")));
        });

        YierdisDb small = TestDbSupport.open(4, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            small.bindToCurrentThread();
            try {
                small.strings().setString(b("large"), b("123456789"), SetMode.NORMAL, null);
                Assert.fail("expected maxmemory rejection");
            } catch (YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("OOM"));
            }
            Assert.assertNull(OwnedReplyValueAssertions.stringValue(small.strings(), b("large")));
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

        YierdisDb db = TestDbSupport.open(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.strings().setString(key, largeValue, SetMode.NORMAL, null).value());
            long usedBefore = db.usedBytesForMaxmemory();

            Assert.assertTrue(db.strings().setString(key, smallValue, SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(smallValue, OwnedReplyValueAssertions.stringValue(db.strings(), key));
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

        YierdisDb db = TestDbSupport.open(maxmemoryBytes, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.strings().setString(key, oldValue, SetMode.NORMAL, null).value());
            long usedBeforeRejectedWrite = db.usedBytesForMaxmemory();

            try {
                db.strings().setString(key, largerValue, SetMode.NORMAL, null);
                Assert.fail("expected maxmemory rejection");
            } catch (YierdisCommandException e) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
            }

            Assert.assertArrayEquals(oldValue, OwnedReplyValueAssertions.stringValue(db.strings(), key));
            Assert.assertEquals(usedBeforeRejectedWrite, db.usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void allkeysLruEvictsOldStringDuringSetWithoutInternalFailure() {
        byte[] value = repeat((byte) 'x', 64 * 1024);
        long maxmemoryBytes = maxmemoryThatFitsOneLocalStringButNotTwo(value);
        YierdisDb db = TestDbSupport.open(maxmemoryBytes, MaxmemoryPolicy.ALLKEYS_LRU, 1000, 5, 5);
        try {
            db.bindToCurrentThread();

            Assert.assertTrue(db.strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(db.strings().setString(b("c"), value, SetMode.NORMAL, null).value());

            Assert.assertNull(OwnedReplyValueAssertions.stringValue(db.strings(), b("a")));
            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(db.strings(), b("c")));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void appendReservesTheFullReplacementValueAcrossNativePageBoundary() {
        withDb(db -> {
            byte[] initial = repeat((byte) 'x', 64 * 1024);
            Assert.assertTrue(db.strings().setString(b("k"), initial, SetMode.NORMAL, null).value());

            Assert.assertEquals(Long.valueOf(initial.length + 1L),
                    db.strings().append(b("k"), slice("!")).value());

            byte[] expected = java.util.Arrays.copyOf(initial, initial.length + 1);
            expected[expected.length - 1] = '!';
            Assert.assertArrayEquals(expected, OwnedReplyValueAssertions.stringValue(db.strings(), b("k")));
        });
    }

    @Test
    public void appendAndSetbitNoopsKeepTheExistingNativeString() {
        withDb(db -> {
            Assert.assertEquals(Long.valueOf(1L), db.strings().incrBy(b("number"), 1L).value());
            EntryRecord integerBefore = db.keyLifecycle().liveEntryRecord(b("number"));

            WriteResult<Long> append = db.strings().append(b("number"), slice(""));

            Assert.assertEquals(Long.valueOf(1L), append.value());
            Assert.assertSame(MutationOutcome.NONE, append.mutationOutcome());
            EntryRecord integerAfter = db.keyLifecycle().liveEntryRecord(b("number"));
            Assert.assertEquals(integerBefore.valueHandle(), integerAfter.valueHandle());
            Assert.assertEquals(ValueEncoding.STRING_INT, integerAfter.encoding());
            Assert.assertEquals(integerBefore.version(), integerAfter.version());

            Assert.assertTrue(db.strings().setString(
                    b("bits"),
                    new byte[]{(byte) 0x80},
                    SetMode.NORMAL,
                    null
            ).value());
            EntryRecord bitsBefore = db.keyLifecycle().liveEntryRecord(b("bits"));

            WriteResult<Integer> setbit = db.strings().setBit(b("bits"), 0L, 1);

            Assert.assertEquals(Integer.valueOf(1), setbit.value());
            Assert.assertSame(MutationOutcome.NONE, setbit.mutationOutcome());
            EntryRecord bitsAfter = db.keyLifecycle().liveEntryRecord(b("bits"));
            Assert.assertEquals(bitsBefore.valueHandle(), bitsAfter.valueHandle());
        });
    }

    @Test
    public void setGetReturnsOwnedSupersededNativeValue() {
        withDb(db -> {
            byte[] key = b("k");
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            long liveObjectsBeforeSetGet = KeyLifecycleTestAccess.backend(db).stats().liveObjects();

            StringOps.SetStringValue result = TestDbSupport.commitSetWithOldValue(
                    db.strings(), key, slice("new"), SetMode.NORMAL, null
            ).value();
            Assert.assertTrue(result.applied());
            Assert.assertArrayEquals(b("old"), byteValueBytes(result.oldValue()));
            Assert.assertArrayEquals(b("new"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            Assert.assertTrue("SET GET old value should retain the superseded native allocation",
                    result.oldValue().retainedMemoryBytes() > 0L);
            Assert.assertTrue("old and replacement values should both remain live until the result closes",
                    KeyLifecycleTestAccess.backend(db).stats().liveObjects() > liveObjectsBeforeSetGet);

            result.close();
            result.close();
            Assert.assertEquals(liveObjectsBeforeSetGet, KeyLifecycleTestAccess.backend(db).stats().liveObjects());
        });
    }

    @Test
    public void setOverExpiredLargeKeyReservesAsMissingKey() {
        withDb(db -> {
            byte[] key = repeat((byte) 'k', 33_000);
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, ExpireOption.px(0)).value());

            Assert.assertTrue(db.strings().setString(key, b("new"), SetMode.NORMAL, null).value());

            Assert.assertArrayEquals(b("new"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
        });
    }

    @Test
    public void missingKeysAndWrongTypesCoverEveryStringReadAndMutationPath() {
        withDb(db -> {
            Assert.assertNull(OwnedReplyValueAssertions.stringValue(db.strings(), b("missing")));
            try (ByteValue missing = db.strings().getStringValue(view("missing"))) {
                Assert.assertTrue(missing.payloadLength() < 0);
            }
            Assert.assertEquals(0L, db.strings().strlen(view("missing")));
            Assert.assertEquals(0, db.strings().getBit(view("missing"), 10));

            Assert.assertEquals(Long.valueOf(0L), db.strings().append(b("empty"), slice("")).value());
            Assert.assertArrayEquals(new byte[0], OwnedReplyValueAssertions.stringValue(db.strings(), b("empty")));
            Assert.assertEquals(Long.valueOf(3L), db.strings().append(b("append"), slice("abc")).value());
            Assert.assertArrayEquals(b("abc"), OwnedReplyValueAssertions.stringValue(db.strings(), b("append")));

            Assert.assertEquals(Integer.valueOf(0), db.strings().setBit(b("bits"), 9L, 1).value());
            Assert.assertArrayEquals(new byte[]{0, 0x40}, OwnedReplyValueAssertions.stringValue(db.strings(), b("bits")));
            Assert.assertEquals(Long.valueOf(-2L), db.strings().incrBy(b("counter"), -2L).value());
            Assert.assertArrayEquals(b("-2"), OwnedReplyValueAssertions.stringValue(db.strings(), b("counter")));

            db.lists().rpush(b("list"), List.of(b("a")));
            expectWrongType(() -> OwnedReplyValueAssertions.stringValue(db.strings(), b("list")));
            expectWrongType(() -> db.strings().getStringValue(view("list")));
            expectWrongType(() -> db.strings().strlen(view("list")));
            expectWrongType(() -> db.strings().getBit(view("list"), 0));
            expectWrongType(() -> db.strings().append(b("list"), slice("x")));
            expectWrongType(() -> db.strings().setBit(b("list"), 0, 1));
            expectWrongType(() -> db.strings().incrBy(b("list"), 1));
        });
    }

    @Test
    public void bitOperationsRejectInvalidValuesAndOffsetsBeforeAllocation() {
        withDb(db -> {
            expectCommandError(
                    "ERR bit is not an integer or out of range",
                    () -> db.strings().setBit(b("bits"), 0, -1)
            );
            expectCommandError(
                    "ERR bit is not an integer or out of range",
                    () -> db.strings().setBit(b("bits"), 0, 2)
            );
            expectCommandError(
                    "ERR bit offset is not an integer or out of range",
                    () -> db.strings().setBit(b("bits"), -1, 0)
            );
            expectCommandError(
                    "ERR bit offset is not an integer or out of range",
                    () -> db.strings().getBit(view("missing"), -1)
            );
            expectCommandError(
                    "ERR bit offset is not an integer or out of range",
                    () -> db.strings().getBit(
                            view("missing"),
                            ((long) Integer.MAX_VALUE + 1L) << 3
                    )
            );
            expectCommandError(
                    "ERR string exceeds maximum allowed size",
                    () -> db.strings().setBit(
                            b("bits"),
                            512L * 1024L * 1024L * 8L,
                            1
                    )
            );
            Assert.assertNull(db.keyspace().typeOf(view("bits")));
        });
    }

    @Test
    public void incrByAcceptsSignsAndRejectsMalformedOrOverflowingIntegers() {
        withDb(db -> {
            db.strings().setString(b("positive"), b("+41"), SetMode.NORMAL, null);
            Assert.assertEquals(Long.valueOf(42L), db.strings().incrBy(b("positive"), 1L).value());
            db.strings().setString(b("negative"), b("-41"), SetMode.NORMAL, null);
            Assert.assertEquals(Long.valueOf(-40L), db.strings().incrBy(b("negative"), 1L).value());

            for (String invalid : List.of(
                    "",
                    "+",
                    "-",
                    "12x",
                    " 1",
                    "9223372036854775808",
                    "-9223372036854775809"
            )) {
                byte[] key = b("invalid-" + invalid);
                db.strings().setString(key, b(invalid), SetMode.NORMAL, null);
                expectCommandError(
                        "ERR value is not an integer or out of range",
                        () -> db.strings().incrBy(key, 1L)
                );
                Assert.assertArrayEquals(b(invalid), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            }

            db.strings().setString(
                    b("max"),
                    b(Long.toString(Long.MAX_VALUE)),
                    SetMode.NORMAL,
                    null
            );
            expectCommandError(
                    "ERR value is not an integer or out of range",
                    () -> db.strings().incrBy(b("max"), 1L)
            );
            db.strings().setString(
                    b("min"),
                    b(Long.toString(Long.MIN_VALUE)),
                    SetMode.NORMAL,
                    null
            );
            expectCommandError(
                    "ERR value is not an integer or out of range",
                    () -> db.strings().incrBy(b("min"), -1L)
            );
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
        YierdisDb db = TestDbSupport.open(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.strings().setString(key, value, SetMode.NORMAL, null);
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
        YierdisDb db = TestDbSupport.open(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.strings().setString(key, initialValue, SetMode.NORMAL, null);
            db.strings().setString(key, overwriteValue, SetMode.NORMAL, null);
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
        YierdisDb db = TestDbSupport.open(limit, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.strings().setString(key, initialValue, SetMode.NORMAL, null);
            try {
                db.strings().setString(key, overwriteValue, SetMode.NORMAL, null);
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
        YierdisDb db = TestDbSupport.open(100_000_000L, MaxmemoryPolicy.ALLKEYS_LRU, 1000, 5, 5);
        try {
            db.bindToCurrentThread();
            if (keyCount >= 1) {
                db.strings().setString(b("a"), value, SetMode.NORMAL, null);
            }
            if (keyCount >= 2) {
                db.strings().setString(b("c"), value, SetMode.NORMAL, null);
            }
            return db.usedBytesForMaxmemory();
        } finally {
            db.shutdown();
        }
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

    private static void expectCommandError(String expectedMessage, ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected YierdisCommandException");
        } catch (YierdisCommandException expected) {
            Assert.assertEquals(expectedMessage, expected.getMessage());
        }
    }

    private static byte[] byteValueBytes(ByteValue value) {
        final byte[][] captured = new byte[1][];
        value.emitTo(new ByteValueSink() {
            @Override
            public void value(byte[] data) {
                captured[0] = data == null ? null : java.util.Arrays.copyOf(data, data.length);
            }

            @Override
            public void value(byte[] data, int off, int len) {
                captured[0] = java.util.Arrays.copyOfRange(data, off, off + len);
            }

            @Override
            public void value(BytesSlice slice) {
                byte[] data = new byte[slice.length()];
                slice.getBytes(0, data, 0, data.length);
                captured[0] = data;
            }

            @Override
            public void longAscii(long value) {
                captured[0] = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
            }

            @Override
            public void nullValue() {
                captured[0] = null;
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
