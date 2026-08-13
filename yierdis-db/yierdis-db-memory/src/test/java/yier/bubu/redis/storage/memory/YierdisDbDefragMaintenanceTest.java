package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbDefragMaintenanceTest {
    @Test
    public void disabledDefragMaintenanceIsNoOp() {
        YierdisDb db = TestDbSupport.open();
        try {
            Assert.assertTrue(db.writes().strings().setString(b("k"), b("value"), SetMode.NORMAL, null).value());
            NativeAllocatorStats before = KeyLifecycleTestAccess.backend(db).stats();

            db.defragMaintenance();

            YierdisMemoryStats stats = db.memory().memoryStats();
            NativeAllocatorStats after = KeyLifecycleTestAccess.backend(db).stats();
            Assert.assertEquals(before.defragMovedBytes(), after.defragMovedBytes());
            Assert.assertEquals(0L, stats.nativeDefragLastMovedObjects());
            Assert.assertEquals(0L, stats.nativeDefragLastMovedBytes());
        } finally {
            db.shutdown();
        }
    }
}
