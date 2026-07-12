package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapStringStorageTest {
    @Test
    public void setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                byte[] value = b("hello");

                Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());
                Assert.assertTrue(runtime.usedBytes() > 0);
                Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);

                RecordingBulkOutput out = new RecordingBulkOutput();
                db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
                Assert.assertTrue(out.usedBytesSlice);
                Assert.assertArrayEquals(value, out.bytes);

                Assert.assertEquals(1L, (long) db.writes().keyspace().del(Collections.singletonList(key)).value());
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0L, db.usedBytesForMaxmemory());
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.ENTRY_RECORD));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().liveObjects());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void cleanupExpiredFreesFfmStrings() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();
                Assert.assertTrue(runtime.usedBytes() > 0);

                db.cleanupExpired();
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0, db.memory().memoryStats().expireCount());
                Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void expiredKeyStringPayloadIsReleasedWhenOverwrittenByOtherCommand() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();
            Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);

            db.writes().lists().lpush(key, List.of(b("a")));

            Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(ValueType.LIST, db.reads().keyspace().typeOf(new TestBytesView(key)));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void overwritePublishesReplacementStringHandle() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            byte[] v1 = b("hello");
            byte[] v2 = b("world");

            Assert.assertTrue(db.writes().strings().setString(key, v1, SetMode.NORMAL, null).value());
            long raw = db.keyLifecycle().liveEntryRecord(key).valueHandle().raw();

            Assert.assertTrue(db.writes().strings().setString(key, v2, SetMode.NORMAL, null).value());
            Assert.assertNotEquals(raw, db.keyLifecycle().liveEntryRecord(key).valueHandle().raw());

            RecordingBulkOutput out = new RecordingBulkOutput();
            db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
            Assert.assertTrue(out.usedBytesSlice);
            Assert.assertArrayEquals(v2, out.bytes);
        } finally {
            db.shutdown();
        }
    }

    private static final class RecordingBulkOutput implements BulkStringSink {
        private byte[] bytes;
        private boolean usedBytesSlice;

        @Override
        public void bulkString(byte[] data) {
            usedBytesSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[data.length];
            System.arraycopy(data, 0, bytes, 0, bytes.length);
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            usedBytesSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[len];
            System.arraycopy(data, off, bytes, 0, len);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            usedBytesSlice = slice != null;
            if (slice == null) {
                bytes = null;
                return;
            }
            bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            usedBytesSlice = false;
            bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        }
    }

    private static final class TestBytesView implements yier.bubu.redis.bytes.BytesView {
        private final byte[] data;

        private TestBytesView(byte[] data) {
            this.data = data;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }
    }
}
