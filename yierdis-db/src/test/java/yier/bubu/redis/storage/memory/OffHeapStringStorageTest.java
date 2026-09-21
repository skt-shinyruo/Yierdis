package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.ByteValue;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class OffHeapStringStorageTest {
    @Test
    public void setGetUsesNativeStringSliceAndDelReleasesLiveAllocatorObjects() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                byte[] value = b("hello");

                Assert.assertTrue(db.strings().setString(key, value, SetMode.NORMAL, null).value());
                Assert.assertTrue(runtime.usedBytes() > 0);

                try (ByteValue replyValue = db.strings().getStringValue(view(key))) {
                    RecordingByteValueOutput out = new RecordingByteValueOutput();
                    replyValue.emitTo(out);
                    Assert.assertTrue(out.usedBytesSlice);
                    Assert.assertArrayEquals(value, out.bytes);
                    Assert.assertTrue("GET must retain the native reply source", replyValue.retainedMemoryBytes() > 0L);
                }

                Assert.assertEquals(1L, (long) db.keyspace().del(Collections.singletonList(key)).value());
                Assert.assertEquals(0, db.size());
                assertPhysicalStatsConsistent(db);
                Assert.assertEquals(0L, KeyLifecycleTestAccess.backend(db).stats().liveObjects());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void replyPreflightStringValueDoesNotTouchTheLruClock() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                byte[] value = b("value");
                db.strings().setString(key, value, SetMode.NORMAL, null).value();
                long accessClockBeforePreflight = db.keyLifecycle().entryRecord(key).lruOrLfu();

                try (ByteValue replyValue = db.strings().getStringValue(view(key))) {
                    RecordingByteValueOutput out = new RecordingByteValueOutput();
                    replyValue.emitTo(out);
                    Assert.assertTrue(out.usedBytesSlice);
                    Assert.assertArrayEquals(value, out.bytes);
                }

                Assert.assertEquals(
                        accessClockBeforePreflight,
                        db.keyLifecycle().entryRecord(key).lruOrLfu()
                );
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void cleanupExpiredReleasesFfmStringObjects() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();
                Assert.assertTrue(runtime.usedBytes() > 0);

                db.cleanupExpired();
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0, db.memoryStats().expireCount());
                assertPhysicalStatsConsistent(db);
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void expiredStringKeyCanBeOverwrittenByOtherCommand() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();

            db.lists().lpush(key, List.of(b("a")));

            Assert.assertEquals(ValueType.LIST, db.keyspace().typeOf(view(key)));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void overwritePublishesReplacementStringHandle() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            byte[] v1 = b("hello");
            byte[] v2 = b("world");

            Assert.assertTrue(db.strings().setString(key, v1, SetMode.NORMAL, null).value());
            NativeHandle firstHandle = db.keyLifecycle().liveEntryRecord(key).valueHandle().nativeHandle();

            Assert.assertTrue(db.strings().setString(key, v2, SetMode.NORMAL, null).value());
            Assert.assertNotEquals(firstHandle, db.keyLifecycle().liveEntryRecord(key).valueHandle().nativeHandle());

            try (ByteValue replyValue = db.strings().getStringValue(view(key))) {
                RecordingByteValueOutput out = new RecordingByteValueOutput();
                replyValue.emitTo(out);
                Assert.assertTrue(out.usedBytesSlice);
                Assert.assertArrayEquals(v2, out.bytes);
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void shrinkingSetReleasesSupersededNativeCapacity() {
        try (TestBackend runtime = TestBackend.open("string-shrink-release")) {
            YierdisDb db = TestDbSupport.open(
                    runtime,
                    10_000_000L,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                byte[] largeValue = new byte[24 * 1024];

                Assert.assertTrue(db.strings().setString(
                        key,
                        largeValue,
                        SetMode.NORMAL,
                        null
                ).value());
                NativeHandle supersededHandle = db.keyLifecycle().liveEntryRecord(key).valueHandle().nativeHandle();
                long supersededCapacity;
                try (NativeObjectView view = KeyLifecycleTestAccess.backend(db)
                        .resolve(supersededHandle, NativeAccessMode.READ_ONLY)) {
                    supersededCapacity = view.capacity();
                }
                long committedBefore = KeyLifecycleTestAccess.backend(db).stats().committedBytes();

                Assert.assertTrue(db.strings().setString(key, b("x"), SetMode.NORMAL, null).value());

                NativeHandle replacementHandle = db.keyLifecycle().liveEntryRecord(key).valueHandle().nativeHandle();
                Assert.assertNotEquals(supersededHandle, replacementHandle);
                Assert.assertThrows(StaleNativeHandleException.class, () -> {
                    try (NativeObjectView ignored = KeyLifecycleTestAccess.backend(db)
                            .resolve(supersededHandle, NativeAccessMode.READ_ONLY)) {
                    }
                });
                try (NativeObjectView replacement = KeyLifecycleTestAccess.backend(db)
                        .resolve(replacementHandle, NativeAccessMode.READ_ONLY)) {
                    Assert.assertTrue(replacement.capacity() < supersededCapacity);
                }
                Assert.assertTrue(
                        "release plus trim must return the superseded string page",
                        KeyLifecycleTestAccess.backend(db).stats().committedBytes() < committedBefore
                );
                Assert.assertArrayEquals(b("x"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            } finally {
                db.shutdown();
            }
        }
    }

    private static final class RecordingByteValueOutput implements ByteValueSink {
        private byte[] bytes;
        private boolean usedBytesSlice;

        @Override
        public void value(byte[] data) {
            usedBytesSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[data.length];
            System.arraycopy(data, 0, bytes, 0, bytes.length);
        }

        @Override
        public void value(byte[] data, int off, int len) {
            usedBytesSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[len];
            System.arraycopy(data, off, bytes, 0, len);
        }

        @Override
        public void value(BytesSlice slice) {
            usedBytesSlice = slice != null;
            if (slice == null) {
                bytes = null;
                return;
            }
            bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
        }

        @Override
        public void longAscii(long value) {
            usedBytesSlice = false;
            bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }
    }

    private static void assertPhysicalStatsConsistent(YierdisDb db) {
        YierdisMemoryStats stats = db.memoryStats();
        Assert.assertEquals(db.usedBytesForMaxmemory(), stats.usedBytesForMaxmemory());
        Assert.assertEquals(stats.usedBytesForMaxmemory(), stats.totalEstimatedBytes());
        Assert.assertEquals(
                MemoryUsageSnapshot.addSaturating(
                        stats.nativeMetadataCommittedBytes(),
                        stats.nativeDataCommittedBytes()
                ),
                stats.offHeapUsedBytes()
        );
        Assert.assertTrue(stats.usedBytesForMaxmemory() > 0L);
        Assert.assertTrue(stats.nativeReclaimableBytes() <= stats.nativeDataCommittedBytes());
    }

}
