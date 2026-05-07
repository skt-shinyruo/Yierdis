package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.OffHeapAllocator;

public class YierdisForeignOffHeapAllocatorTest {
    @Test
    public void allocatorInstantiatesWithoutIncubatorModuleFlags() {
        try (OffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(0)) {
            Assert.assertNotNull(allocator);
            Assert.assertTrue(allocator instanceof YierdisForeignOffHeapAllocator);
            Assert.assertEquals(0L, allocator.usedBytes());
        }
    }

    @Test
    public void foreignAllocatorUsesFfmRuntimeForBufOwnership() {
        try (YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(0)) {
            var buf = allocator.allocate(16);
            buf.setByte(0, (byte) 'x');
            Assert.assertEquals('x', buf.getByte(0));
            buf.close();
            Assert.assertEquals(0L, allocator.usedBytes());
        }
    }

    @Test
    public void maxBytesLimitRejectsAllocationOverCap() {
        try (YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(4)) {
            try {
                allocator.allocate(8);
                Assert.fail("expected OffHeapOutOfMemoryException");
            } catch (yier.bubu.redis.memory.api.OffHeapOutOfMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("off-heap"));
            }
        }
    }

    @Test
    public void legacyAllocatorDiscoveryClassIsAbsent() {
        try {
            Class.forName("yier.bubu.redis.memory.api.YierdisOffHeapAllocators");
            Assert.fail("legacy allocator discovery should be deleted");
        } catch (ClassNotFoundException expected) {
            Assert.assertTrue(true);
        }
    }
}
