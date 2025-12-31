package yier.bubu.redis.db.offheap.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;

public class YierdisForeignOffHeapAllocatorTest extends YierdisOffHeapAllocatorContractTest {
    @Override
    protected YierdisOffHeapAllocator newAllocator(long maxBytes) {
        return new YierdisForeignOffHeapAllocator(maxBytes);
    }

    @Test
    public void factoryCreatesForeignAllocatorWhenAvailable() {
        try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
            Assert.assertNotNull(allocator);
            Assert.assertEquals(YierdisOffHeapBackend.FOREIGN, allocator.backend());
        }
    }
}
