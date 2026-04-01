package yier.bubu.redis.db.memory.netty;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;

import java.util.ServiceLoader;

public class YierdisNettyOffHeapAllocatorTest extends YierdisOffHeapAllocatorContractTest {
    @Override
    protected OffHeapAllocator newAllocator(long maxBytes) {
        return new YierdisNettyOffHeapAllocator(maxBytes);
    }

    @Test
    public void nettyBufIsReleasedOnClose() {
        try (OffHeapAllocator allocator = newAllocator(0)) {
            OffHeapBuf buf = allocator.allocate(8);
            YierdisNettyOffHeapBuf netty = (YierdisNettyOffHeapBuf) buf;
            Assert.assertEquals(1, netty.refCnt());
            buf.close();
            Assert.assertEquals(0, netty.refCnt());
        }
    }

    @Test
    public void factoryCreatesNettyAllocatorWhenAvailable() {
        try (OffHeapAllocator allocator = YierdisOffHeapAllocators.create("netty", 0)) {
            Assert.assertNotNull(allocator);
            Assert.assertTrue(allocator instanceof YierdisNettyOffHeapAllocator);
        }
    }

    @Test
    public void serviceLoaderProviderIsPresent() {
        boolean found = false;
        for (YierdisOffHeapAllocatorProvider p : ServiceLoader.load(YierdisOffHeapAllocatorProvider.class)) {
            if (p.backend() == YierdisOffHeapBackend.NETTY) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
    }
}
