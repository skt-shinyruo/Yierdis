package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.netty.YierdisNettyOffHeapAllocator;
import yier.bubu.redis.db.memory.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class OffHeapKeysToggleTest {
    @Test
    public void unsafeBackendDoesNotStoreKeysOffHeapByDefault() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            db.bindToCurrentThread();
            db.setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void offHeapKeysEnabledStoresKeysAndExpiresOffHeap() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, true, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            db.setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void offHeapKeysEnabledRequiresAddressAllocator() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(0);
        try {
            new YierdisDb(allocator, true, 0, "noeviction", 5, 5, 5);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("offHeapKeysEnabled"));
        } finally {
            allocator.close();
        }
    }
}
