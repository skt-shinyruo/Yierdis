package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class UnsafeOffHeapKeyspaceTest {
    @Test
    public void cleanupExpiredAndShutdownDoNotLeakOffHeapMemory() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            db.bindToCurrentThread();
            db.setString(b("k"), b("v"), SetMode.NORMAL, ExpireOption.px(0));
            Assert.assertEquals(1, db.size());
            Assert.assertTrue(allocator.usedBytes() > 0);

            db.cleanupExpired();
            Assert.assertEquals(0, db.size());
        } finally {
            // db.shutdown() closes the allocator and will throw on leaks.
            db.shutdown();
        }
    }
}
