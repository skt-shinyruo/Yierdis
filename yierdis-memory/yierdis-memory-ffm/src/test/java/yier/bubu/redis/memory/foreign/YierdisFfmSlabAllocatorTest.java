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
}
