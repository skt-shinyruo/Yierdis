package yier.bubu.redis.db;

import yier.bubu.redis.ops.ValueType;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.db.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapStringStorageTest {
    @Test
    public void setGetUsesFfmSliceAndDelFrees() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                byte[] value = b("hello");

                Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());
                Assert.assertTrue(runtime.usedBytes() > 0);

                RecordingBulkOutput out = new RecordingBulkOutput();
                db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
                Assert.assertTrue(out.usedOffHeapSlice);
                Assert.assertArrayEquals(value, out.bytes);

                Assert.assertEquals(1L, (long) db.writes().keyspace().del(Collections.singletonList(key)).value());
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0L, runtime.usedBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void cleanupExpiredFreesFfmStrings() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                byte[] key = b("k");
                db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();
                Assert.assertTrue(runtime.usedBytes() > 0);

                db.cleanupExpired();
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0, db.memory().memoryStats().expireCount());
                Assert.assertEquals(0L, runtime.usedBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void expiredKeyStringPayloadIsReleasedWhenOverwrittenByOtherCommand() {
        YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0)).value();
            Assert.assertTrue(allocator.usedBytes() > 0);

            db.writes().lists().lpush(key, List.of(b("a")));

            Assert.assertEquals(0L, allocator.usedBytes());
            Assert.assertEquals(ValueType.LIST, db.reads().keyspace().typeOf(new TestBytesView(key)));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void overwriteReusesFfmBufferUnderHardCap() {
        YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(5);
        YierdisDb db = new YierdisDb(allocator, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            byte[] key = b("k");
            byte[] v1 = b("hello");
            byte[] v2 = b("world");

            Assert.assertTrue(db.writes().strings().setString(key, v1, SetMode.NORMAL, null).value());
            Assert.assertEquals(5L, allocator.usedBytes());

            Assert.assertTrue(db.writes().strings().setString(key, v2, SetMode.NORMAL, null).value());
            Assert.assertEquals(5L, allocator.usedBytes());

            RecordingBulkOutput out = new RecordingBulkOutput();
            db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
            Assert.assertTrue(out.usedOffHeapSlice);
            Assert.assertArrayEquals(v2, out.bytes);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void ffmMaxBytesRejectsOversizedSet() {
        YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(4);
        YierdisDb db = new YierdisDb(allocator, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            try {
                db.writes().strings().setString(b("k"), b("hello"), SetMode.NORMAL, null).value();
                Assert.fail("expected YierdisCommandException");
            } catch (YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("off-heap"));
            }
        } finally {
            db.shutdown();
        }
    }

    private static final class RecordingBulkOutput implements BulkStringSink {
        private byte[] bytes;
        private boolean usedOffHeapSlice;

        @Override
        public void bulkString(byte[] data) {
            usedOffHeapSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[data.length];
            System.arraycopy(data, 0, bytes, 0, bytes.length);
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            usedOffHeapSlice = false;
            if (data == null) {
                bytes = null;
                return;
            }
            bytes = new byte[len];
            System.arraycopy(data, off, bytes, 0, len);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            usedOffHeapSlice = slice instanceof OffHeapSlice;
            if (slice == null) {
                bytes = null;
                return;
            }
            bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            usedOffHeapSlice = false;
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
