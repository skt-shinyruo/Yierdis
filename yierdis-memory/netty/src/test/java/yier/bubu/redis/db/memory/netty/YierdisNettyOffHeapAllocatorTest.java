package yier.bubu.redis.db.memory.netty;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBuf;

import java.util.ServiceLoader;

public class YierdisNettyOffHeapAllocatorTest extends YierdisOffHeapAllocatorContractTest {
    @Override
    protected YierdisOffHeapAllocator newAllocator(long maxBytes) {
        return new YierdisNettyOffHeapAllocator(maxBytes);
    }

    @Test
    public void nettyBufIsReleasedOnClose() {
        try (YierdisOffHeapAllocator allocator = newAllocator(0)) {
            YierdisOffHeapBuf buf = allocator.allocate(8);
            YierdisNettyOffHeapBuf netty = (YierdisNettyOffHeapBuf) buf;
            Assert.assertEquals(1, netty.refCnt());
            buf.close();
            Assert.assertEquals(0, netty.refCnt());
        }
    }

    @Test
    public void factoryCreatesNettyAllocatorWhenAvailable() {
        try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("netty", 0)) {
            Assert.assertNotNull(allocator);
            Assert.assertEquals(YierdisOffHeapBackend.NETTY, allocator.backend());
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
