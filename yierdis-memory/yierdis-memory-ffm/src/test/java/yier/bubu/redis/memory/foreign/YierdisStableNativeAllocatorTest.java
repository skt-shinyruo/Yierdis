package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.OffHeapSlice;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class YierdisStableNativeAllocatorTest {
    @Test
    public void allocatesResolvesAndFreesObject() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertFalse(handle.isNull());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.domain(), handle.domain());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                Assert.assertEquals(8, view.size());
                Assert.assertEquals(8, view.capacity());
                view.setByte(0, (byte) 42);
            }

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(42, view.getByte(0));
            }

            NativeAllocatorStats beforeFree = allocator.stats();
            Assert.assertEquals(8L, beforeFree.logicalUsedBytes());
            Assert.assertEquals(1L, beforeFree.liveObjects());

            allocator.free(handle);

            NativeAllocatorStats afterFree = allocator.stats();
            Assert.assertEquals(0L, afterFree.logicalUsedBytes());
            Assert.assertEquals(0L, afterFree.liveObjects());
        }
    }

    @Test
    public void detectsUseAfterFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            allocator.free(handle);

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void detectsNullHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            try {
                allocator.resolve(null, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void detectsNativeNullHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            try {
                allocator.resolve(NativeHandle.NULL, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void oldViewFailsAfterFreeAndSlotReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle first = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            NativeObjectView oldView = allocator.resolve(first, NativeAccessMode.READ_WRITE);
            oldView.setByte(0, (byte) 11);

            allocator.free(first);

            NativeHandle second = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertNotEquals(first.generation(), second.generation());
            try (NativeObjectView newView = allocator.resolve(second, NativeAccessMode.READ_WRITE)) {
                newView.setByte(0, (byte) 22);
            }

            try {
                oldView.getByte(0);
                Assert.fail("expected stale view");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            } finally {
                oldView.close();
            }

            try (NativeObjectView view = allocator.resolve(second, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(22, view.getByte(0));
            }
        }
    }

    @Test
    public void readOnlyViewRejectsMutation() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                try {
                    view.setByte(0, (byte) 42);
                    Assert.fail("expected read-only rejection");
                } catch (NativeMemoryException expected) {
                    Assert.assertTrue(expected.getMessage().contains("read-only"));
                }
            }
        }
    }

    @Test
    public void reallocPreservesHandleAndPrefixWhenMoved() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            NativeHandle resized = allocator.realloc(handle, 8, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle, resized);

            try (NativeObjectView view = allocator.resolve(resized, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(8, view.size());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(8L, stats.logicalUsedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(1L, stats.reallocMovedCount());
        }
    }

    @Test
    public void reallocNoMoveGrowsWithinCapacityAfterShrink() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            NativeHandle shrunk = allocator.realloc(handle, 4, NativeReallocPolicy.NO_MOVE);
            Assert.assertEquals(handle, shrunk);

            NativeHandle grown = allocator.realloc(handle, 6, NativeReallocPolicy.NO_MOVE);
            Assert.assertEquals(handle, grown);

            try (NativeObjectView view = allocator.resolve(grown, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(6, view.size());
                Assert.assertEquals(8, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(6L, stats.logicalUsedBytes());
            Assert.assertEquals(2L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());
        }
    }

    @Test
    public void reallocNoMoveFailsWithoutChangingObjectWhenGrowthNeedsMove() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            try {
                allocator.realloc(handle, 8, NativeReallocPolicy.NO_MOVE);
                Assert.fail("expected no-move realloc failure");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("cannot grow in place"));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(4L, stats.logicalUsedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(0L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(4, view.size());
                Assert.assertEquals(4, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }
        }
    }

    @Test
    public void preservePrefixGrowsWithinCapacityAfterShrinkWithoutMove() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 5);
                view.setByte(1, (byte) 6);
                view.setByte(2, (byte) 7);
                view.setByte(3, (byte) 8);
            }

            allocator.realloc(handle, 4, NativeReallocPolicy.PRESERVE_PREFIX);
            NativeHandle grown = allocator.realloc(handle, 6, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle, grown);

            try (NativeObjectView view = allocator.resolve(grown, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(6, view.size());
                Assert.assertEquals(8, view.capacity());
                Assert.assertEquals(5, view.getByte(0));
                Assert.assertEquals(6, view.getByte(1));
                Assert.assertEquals(7, view.getByte(2));
                Assert.assertEquals(8, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(6L, stats.logicalUsedBytes());
            Assert.assertEquals(2L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());
        }
    }

    @Test
    public void retiresSlotWhenGenerationSpaceIsExhausted() {
        try (TestOffHeapAllocator payload = new TestOffHeapAllocator();
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(payload, 1)) {

            NativeHandle original = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            allocator.free(original);

            for (int generation = 2; generation <= 0x0fff; generation++) {
                NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
                Assert.assertEquals(generation, handle.generation());
                allocator.free(handle);
            }

            try {
                allocator.resolve(original, NativeAccessMode.READ_ONLY);
                Assert.fail("expected original handle to remain stale");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
                Assert.fail("expected retired slot exhaustion");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }
        }
    }

    @Test
    public void reallocMoveCloseFailureLeavesOriginalObjectReadable() {
        TestOffHeapAllocator payload = new TestOffHeapAllocator();
        try (YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(payload, 4)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            TestOffHeapBuf originalBuffer = payload.buffer(0);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            originalBuffer.throwOnClose = true;
            try {
                allocator.realloc(handle, 8, NativeReallocPolicy.PRESERVE_PREFIX);
                Assert.fail("expected close failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("close failed"));
            }
            originalBuffer.throwOnClose = false;

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(4L, stats.logicalUsedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(0L, stats.reallocMovedCount());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(4, view.size());
                Assert.assertEquals(4, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }
        }
    }

    @Test
    public void freeCloseFailureStalesHandleAndDoesNotReuseSlot() {
        TestOffHeapAllocator payload = new TestOffHeapAllocator();
        try (YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(payload, 1)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            TestOffHeapBuf buffer = payload.buffer(0);
            buffer.throwOnClose = true;

            try {
                allocator.free(handle);
                Assert.fail("expected close failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("close failed"));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(0L, stats.logicalUsedBytes());
            Assert.assertEquals(0L, stats.liveObjects());
            Assert.assertEquals(4L, stats.reservedBytes());

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            try {
                allocator.free(handle);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
                Assert.fail("expected slot exhaustion");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            buffer.throwOnClose = false;
            allocator.close();
            Assert.assertEquals(0L, payload.usedBytes());
        }
    }

    @Test
    public void closeFailureKeepsBufferForRetry() {
        TestOffHeapAllocator payload = new TestOffHeapAllocator();
        YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(payload, 1);
        NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
        TestOffHeapBuf buffer = payload.buffer(0);
        buffer.throwOnClose = true;

        try {
            allocator.close();
            Assert.fail("expected close failure");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("close failed"));
        }

        NativeAllocatorStats stats = allocator.stats();
        Assert.assertEquals(0L, stats.logicalUsedBytes());
        Assert.assertEquals(0L, stats.liveObjects());
        Assert.assertEquals(4L, stats.reservedBytes());

        buffer.throwOnClose = false;
        allocator.close();
        Assert.assertEquals(0L, payload.usedBytes());

        try {
            allocator.resolve(handle, NativeAccessMode.READ_ONLY);
            Assert.fail("expected allocator closed");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("closed"));
        }
    }

    private static final class TestOffHeapAllocator implements OffHeapAllocator {
        private TestOffHeapBuf[] buffers = new TestOffHeapBuf[8];
        private int bufferCount;
        private long usedBytes;

        @Override
        public OffHeapBuf allocate(int capacity) {
            TestOffHeapBuf buffer = new TestOffHeapBuf(this, capacity);
            if (bufferCount == buffers.length) {
                TestOffHeapBuf[] next = new TestOffHeapBuf[buffers.length * 2];
                System.arraycopy(buffers, 0, next, 0, buffers.length);
                buffers = next;
            }
            buffers[bufferCount++] = buffer;
            usedBytes += capacity;
            return buffer;
        }

        @Override
        public long usedBytes() {
            return usedBytes;
        }

        @Override
        public long maxBytes() {
            return 0;
        }

        @Override
        public void close() {
        }

        private TestOffHeapBuf buffer(int index) {
            return buffers[index];
        }

        private void onClosed(int capacity) {
            usedBytes -= capacity;
        }
    }

    private static final class TestOffHeapBuf implements OffHeapBuf {
        private final TestOffHeapAllocator owner;
        private final byte[] bytes;
        private boolean closed;
        private boolean throwOnClose;

        private TestOffHeapBuf(TestOffHeapAllocator owner, int capacity) {
            this.owner = owner;
            this.bytes = new byte[capacity];
        }

        @Override
        public int capacity() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }

        @Override
        public void setByte(int index, byte value) {
            bytes[index] = value;
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(bytes, index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            System.arraycopy(src, srcOff, bytes, index, len);
        }

        @Override
        public void setBytes(int index, BytesSource src, int srcIndex, int len) {
            for (int i = 0; i < len; i++) {
                bytes[index + i] = src.getByte(srcIndex + i);
            }
        }

        @Override
        public OffHeapSlice slice(int index, int len) {
            throw new UnsupportedOperationException("slice not needed");
        }

        @Override
        public void close() {
            if (throwOnClose) {
                throw new IllegalStateException("close failed");
            }
            if (closed) {
                return;
            }
            closed = true;
            owner.onClosed(bytes.length);
        }
    }
}
