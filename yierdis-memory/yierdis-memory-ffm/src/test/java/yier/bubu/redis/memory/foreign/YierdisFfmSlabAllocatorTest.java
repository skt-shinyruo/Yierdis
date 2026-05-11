package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.OffHeapBuf;

public class YierdisFfmSlabAllocatorTest {
    @Test
    public void slabAllocatorSuballocatesFromOneRuntimeRegion() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slab-test")) {
            YierdisFfmSlabAllocator allocator = new YierdisFfmSlabAllocator(runtime, 64);
            try {
                OffHeapBuf first = allocator.allocate(16);
                OffHeapBuf second = allocator.allocate(16);

                first.setByte(0, (byte) 7);
                second.setByte(0, (byte) 9);

                Assert.assertEquals(7, first.getByte(0));
                Assert.assertEquals(9, second.getByte(0));
                Assert.assertEquals(64L, runtime.usedBytes());

                first.close();
                second.close();

                Assert.assertEquals(0L, allocator.usedBytes());
            } finally {
                allocator.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void slabAllocatorAccountingRecoversAfterLeakCloseAndLateBufferClose() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slab-late-close");
        YierdisFfmSlabAllocator allocator = new YierdisFfmSlabAllocator(runtime, 64);
        OffHeapBuf buf = allocator.allocate(16);
        try {
            try {
                allocator.close();
                Assert.fail("expected off-heap leak");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("off-heap leak"));
            }

            Assert.assertEquals(16L, allocator.usedBytes());

            buf.close();
            Assert.assertEquals(0L, allocator.usedBytes());

            allocator.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        } finally {
            closeQuietly(buf);
            closeQuietly(allocator);
            closeQuietly(runtime);
        }
    }

    @Test
    public void slabAllocatorCoalescesAdjacentFreeBlocksForReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("slab-reuse")) {
            YierdisFfmSlabAllocator allocator = new YierdisFfmSlabAllocator(runtime, 64);
            try {
                OffHeapBuf first = allocator.allocate(16);
                OffHeapBuf second = allocator.allocate(16);
                OffHeapBuf third = allocator.allocate(16);
                OffHeapBuf fourth = allocator.allocate(16);
                try {
                    Assert.assertEquals(64L, runtime.usedBytes());

                    second.close();
                    third.close();

                    OffHeapBuf merged = allocator.allocate(32);
                    try {
                        Assert.assertEquals(64L, runtime.usedBytes());
                    } finally {
                        merged.close();
                    }
                } finally {
                    first.close();
                    second.close();
                    third.close();
                    fourth.close();
                }

                Assert.assertEquals(0L, allocator.usedBytes());
            } finally {
                allocator.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
