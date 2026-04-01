package yier.bubu.redis.db.memory.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.offheap.api.OffHeapSlice;

public abstract class YierdisOffHeapAllocatorContractTest {
    protected abstract OffHeapAllocator newAllocator(long maxBytes);

    @Test
    public void allocateWriteReadRoundTripWorks() {
        try (OffHeapAllocator allocator = newAllocator(0)) {
            try (OffHeapBuf buf = allocator.allocate(16)) {
                byte[] src = new byte[16];
                for (int i = 0; i < src.length; i++) {
                    src[i] = (byte) i;
                }
                buf.setBytes(0, src, 0, src.length);

                byte[] dst = new byte[16];
                buf.getBytes(0, dst, 0, dst.length);
                Assert.assertArrayEquals(src, dst);
            }
            Assert.assertEquals(0L, allocator.usedBytes());
        }
    }

    @Test
    public void sliceReadAndWriteToByteBufWork() {
        try (OffHeapAllocator allocator = newAllocator(0)) {
            try (OffHeapBuf buf = allocator.allocate(16)) {
                byte[] src = new byte[16];
                for (int i = 0; i < src.length; i++) {
                    src[i] = (byte) (i + 1);
                }
                buf.setBytes(0, src, 0, src.length);

                OffHeapSlice slice = buf.slice(3, 5);
                byte[] sliced = new byte[5];
                slice.getBytes(0, sliced, 0, sliced.length);
                Assert.assertArrayEquals(new byte[]{4, 5, 6, 7, 8}, sliced);

                YierdisByteArraySink out = new YierdisByteArraySink();
                slice.writeTo(out);
                Assert.assertArrayEquals(sliced, out.toByteArray());
            }
        }
    }

    @Test
    public void accessAfterCloseThrows() {
        OffHeapAllocator allocator = newAllocator(0);
        OffHeapBuf buf = allocator.allocate(8);
        buf.close();

        try {
            buf.getByte(0);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException ignored) {
            // expected
        } finally {
            allocator.close();
        }
    }

    @Test
    public void memoryLimitIsEnforced() {
        try (OffHeapAllocator allocator = newAllocator(8)) {
            try (OffHeapBuf ignored = allocator.allocate(8)) {
                // ok
            }
            try {
                allocator.allocate(9);
                Assert.fail("expected YierdisOffHeapOutOfMemoryException");
            } catch (OffHeapOutOfMemoryException ignored) {
                // expected
            }
        }
    }

    @Test
    public void allocatorCloseDetectsLeaks() {
        OffHeapAllocator allocator = newAllocator(0);
        OffHeapBuf buf = allocator.allocate(8);
        try {
            allocator.close();
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException ignored) {
            // expected
        } finally {
            // Ensure we don't leak native memory in the test process.
            buf.close();
        }
    }
}
