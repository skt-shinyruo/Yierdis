package yier.bubu.redis.db.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;

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
