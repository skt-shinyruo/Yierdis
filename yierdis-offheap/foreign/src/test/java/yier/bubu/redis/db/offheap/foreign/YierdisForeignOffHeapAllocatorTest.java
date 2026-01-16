package yier.bubu.redis.db.offheap.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;

import java.util.ServiceLoader;

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

    @Test
    public void serviceLoaderProviderIsPresent() {
        boolean found = false;
        for (YierdisOffHeapAllocatorProvider p : ServiceLoader.load(YierdisOffHeapAllocatorProvider.class)) {
            if (p.backend() == YierdisOffHeapBackend.FOREIGN) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
    }
}
