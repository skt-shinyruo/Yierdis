package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.db.offheap.netty.YierdisNettyOffHeapAllocator;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static yier.bubu.redis.testutil.TestBytes.b;

public class OffHeapStringStorageTest {
    @Test
    public void setGetUsesOffHeapSliceAndDelFrees() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            byte[] key = b("k");
            byte[] value = b("hello");

            Assert.assertTrue(db.setString(key, value, YierdisDb.SetMode.NORMAL, null));
            Assert.assertTrue(allocator.usedBytes() > 0);

            RecordingBulkOutput out = new RecordingBulkOutput();
            db.getStringForReply(key, out);
            Assert.assertTrue(out.usedOffHeapSlice);
            Assert.assertArrayEquals(value, out.bytes);

            Assert.assertEquals(1L, db.del(Collections.singletonList(key)));
            Assert.assertEquals(0L, allocator.usedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredFreesOffHeapStrings() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            byte[] key = b("k");
            db.setString(key, b("v"), YierdisDb.SetMode.NORMAL, new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, 0));
            Assert.assertTrue(allocator.usedBytes() > 0);

            db.cleanupExpired();
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, allocator.usedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void expiredKeyStringPayloadIsReleasedWhenOverwrittenByOtherCommand() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            byte[] key = b("k");
            db.setString(key, b("v"), YierdisDb.SetMode.NORMAL, new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, 0));
            Assert.assertTrue(allocator.usedBytes() > 0);

            db.lpush(key, List.of(b("a")));

            Assert.assertEquals(0L, allocator.usedBytes());
            Assert.assertEquals(ValueType.LIST, db.typeOf(key));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void overwriteReusesOffHeapBufferUnderHardCap() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(5);
        YierdisDb db = new YierdisDb(allocator);
        try {
            byte[] key = b("k");
            byte[] v1 = b("hello");
            byte[] v2 = b("world");

            Assert.assertTrue(db.setString(key, v1, YierdisDb.SetMode.NORMAL, null));
            Assert.assertEquals(5L, allocator.usedBytes());

            Assert.assertTrue(db.setString(key, v2, YierdisDb.SetMode.NORMAL, null));
            Assert.assertEquals(5L, allocator.usedBytes());

            RecordingBulkOutput out = new RecordingBulkOutput();
            db.getStringForReply(key, out);
            Assert.assertTrue(out.usedOffHeapSlice);
            Assert.assertArrayEquals(v2, out.bytes);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void offHeapMaxBytesRejectsOversizedSet() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(4);
        YierdisDb db = new YierdisDb(allocator);
        try {
            try {
                db.setString(b("k"), b("hello"), YierdisDb.SetMode.NORMAL, null);
                Assert.fail("expected YierdisDb.YierdisCommandException");
            } catch (YierdisDb.YierdisCommandException e) {
                Assert.assertTrue(e.getMessage().contains("off-heap"));
            }
        } finally {
            db.shutdown();
        }
    }

    private static final class RecordingBulkOutput implements YierdisBulkStringOutput {
        private byte[] bytes;
        private boolean usedOffHeapSlice;

        @Override
        public void bulkString(byte[] buf, int off, int len) {
            usedOffHeapSlice = false;
            bytes = new byte[len];
            System.arraycopy(buf, off, bytes, 0, len);
        }

        @Override
        public void bulkString(YierdisOffHeapSlice slice) {
            usedOffHeapSlice = true;
            if (slice == null) {
                bytes = null;
                return;
            }
            bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
        }

        @Override
        public void bulkStringNull() {
            usedOffHeapSlice = false;
            bytes = null;
        }

        @Override
        public void bulkStringLongAscii(long value) {
            usedOffHeapSlice = false;
            bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        }
    }
}

