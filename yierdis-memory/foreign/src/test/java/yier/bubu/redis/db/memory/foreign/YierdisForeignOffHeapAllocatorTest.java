package yier.bubu.redis.db.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

import java.util.ServiceLoader;

public class YierdisForeignOffHeapAllocatorTest extends YierdisOffHeapAllocatorContractTest {
    @Override
    protected OffHeapAllocator newAllocator(long maxBytes) {
        return new YierdisForeignOffHeapAllocator(maxBytes);
    }

    @Test
    public void factoryCreatesForeignAllocatorWhenAvailable() {
        try (OffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
            Assert.assertNotNull(allocator);
            Assert.assertTrue(allocator instanceof YierdisForeignOffHeapAllocator);
        }
    }

    @Test
    public void allocatorInstantiatesWithoutIncubatorModuleFlags() {
        try (OffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(0)) {
            Assert.assertNotNull(allocator);
            Assert.assertEquals(0L, allocator.usedBytes());
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
