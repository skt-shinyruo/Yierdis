package yier.bubu.redis.db.memory.unsafe;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBlock;
import yier.bubu.redis.db.memory.api.YierdisOffHeapOutOfMemoryException;

import java.util.ServiceLoader;

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
    public void serviceLoaderProviderIsPresent() {
        boolean found = false;
        for (YierdisOffHeapAllocatorProvider p : ServiceLoader.load(YierdisOffHeapAllocatorProvider.class)) {
            if (p.backend() == YierdisOffHeapBackend.UNSAFE) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void allocateBlockAccountsAndFrees() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        Assert.assertEquals(0L, allocator.usedBytes());

        YierdisOffHeapBlock block = allocator.allocateBlock(16);
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
