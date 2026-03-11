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
    public void createNettyDependsOnServiceLoaderProviders() {
        boolean nettyPresent = hasProvider(YierdisOffHeapBackend.NETTY);
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
            Assert.fail("expected YierdisOffHeapBackendUnavailableException");
        } catch (YierdisOffHeapBackendUnavailableException e) {
            Assert.assertTrue(e.getMessage().contains("ServiceLoader"));
            Assert.assertTrue(e.getMessage().contains("providers"));
        }
    }

    @Test
    public void createForeignDependsOnServiceLoaderProviders() {
        boolean foreignPresent = hasProvider(YierdisOffHeapBackend.FOREIGN);
        if (foreignPresent) {
            // Provider is present; runtime availability depends on JVM flags (incubator module).
            // We only assert that resolution is attempted (no hard-coded class/reflection fallback here).
            try (YierdisOffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
                Assert.assertNotNull(allocator);
                Assert.assertEquals(YierdisOffHeapBackend.FOREIGN, allocator.backend());
            } catch (YierdisOffHeapBackendUnavailableException e) {
                Assert.assertTrue(e.getMessage().contains("foreign"));
            }
            return;
        }

        try {
            YierdisOffHeapAllocators.create("foreign", 0);
            Assert.fail("expected YierdisOffHeapBackendUnavailableException");
        } catch (YierdisOffHeapBackendUnavailableException e) {
            Assert.assertTrue(e.getMessage().contains("ServiceLoader"));
            Assert.assertTrue(e.getMessage().contains("providers"));
        }
    }

    private static boolean hasProvider(YierdisOffHeapBackend backend) {
        for (YierdisOffHeapAllocators.ProviderInfo info : YierdisOffHeapAllocators.availableProviders()) {
            if (info != null && info.backend() == backend) {
                return true;
            }
        }
        return false;
    }
}
