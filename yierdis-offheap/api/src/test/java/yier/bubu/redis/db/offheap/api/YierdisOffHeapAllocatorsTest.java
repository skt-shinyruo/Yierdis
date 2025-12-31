package yier.bubu.redis.db.offheap.api;

import org.junit.Assert;
import org.junit.Test;

public class YierdisOffHeapAllocatorsTest {
    @Test
    public void createReturnsNullForNone() {
        Assert.assertNull(YierdisOffHeapAllocators.create("none", 0));
        Assert.assertNull(YierdisOffHeapAllocators.create("", 0));
        Assert.assertNull(YierdisOffHeapAllocators.create((String) null, 0));
    }

    @Test
    public void createNettyDependsOnRuntimeClasspath() {
        boolean nettyPresent = isNettyAllocatorPresent();
        if (nettyPresent) {
            try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("netty", 0)) {
                Assert.assertNotNull(allocator);
                Assert.assertEquals(YierdisOffHeapBackend.NETTY, allocator.backend());
                Assert.assertEquals(0L, allocator.usedBytes());
            }
            return;
        }

        try {
            YierdisOffHeapAllocators.create("netty", 0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("yierdis-offheap-netty"));
        }
    }

    @Test
    public void createForeignDependsOnBuildProfile() {
        boolean foreignPresent = isForeignAllocatorPresent();
        if (foreignPresent) {
            try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
                Assert.assertEquals(YierdisOffHeapBackend.FOREIGN, allocator.backend());
            }
            return;
        }

        try {
            YierdisOffHeapAllocators.create("foreign", 0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("foreign-memory"));
        }
    }

    private static boolean isForeignAllocatorPresent() {
        try {
            Class.forName("yier.bubu.redis.db.offheap.foreign.YierdisForeignOffHeapAllocator");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean isNettyAllocatorPresent() {
        try {
            Class.forName("yier.bubu.redis.db.offheap.netty.YierdisNettyOffHeapAllocator");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
