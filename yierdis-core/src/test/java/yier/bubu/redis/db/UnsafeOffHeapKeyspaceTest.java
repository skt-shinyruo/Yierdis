package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

import java.util.concurrent.TimeUnit;

import static yier.bubu.redis.testutil.TestBytes.b;

public class UnsafeOffHeapKeyspaceTest {
    @Test
    public void cleanupExpiredAndShutdownDoNotLeakOffHeapMemory() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator);
        try {
            db.setString(b("k"), b("v"), YierdisDb.SetMode.NORMAL, new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, 0));
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

