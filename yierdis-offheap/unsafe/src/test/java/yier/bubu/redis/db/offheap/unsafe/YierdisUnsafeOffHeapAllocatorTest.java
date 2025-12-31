package yier.bubu.redis.db.offheap.unsafe;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;

public class YierdisUnsafeOffHeapAllocatorTest extends YierdisOffHeapAllocatorContractTest {
    @Override
    protected YierdisOffHeapAllocator newAllocator(long maxBytes) {
        return new YierdisUnsafeOffHeapAllocator(maxBytes);
    }

    @Test
    public void factoryCreatesUnsafeAllocatorWhenAvailable() {
        try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("unsafe", 0)) {
            Assert.assertNotNull(allocator);
            Assert.assertEquals(YierdisOffHeapBackend.UNSAFE, allocator.backend());
        }
    }

    @Test
    public void allocateBlockAccountsAndFrees() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        Assert.assertEquals(0L, allocator.usedBytes());

        YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock block = allocator.allocateBlock(16);
        Assert.assertTrue(block.address() != 0);
        Assert.assertEquals(16, block.capacity());
        Assert.assertEquals(16L, allocator.usedBytes());

        block.close();
        Assert.assertEquals(0L, allocator.usedBytes());

        allocator.close();
    }

    @Test
    public void allocateBlockEnforcesMaxBytes() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(4);
        try {
            allocator.allocateBlock(5);
            Assert.fail("expected YierdisOffHeapOutOfMemoryException");
        } catch (YierdisOffHeapOutOfMemoryException expected) {
            // ok
        } finally {
            allocator.close();
        }
    }
}
