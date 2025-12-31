package yier.bubu.redis.db.offheap.netty;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorContractTest;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;

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
}
