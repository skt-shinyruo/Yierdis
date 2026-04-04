package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class UnsafeOffHeapKeyspaceTest {
    @Test
    public void cleanupExpiredAndShutdownDoNotLeakOffHeapMemory() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, ExpireOption.px(0));
                Assert.assertEquals(1, db.size());
                Assert.assertTrue(runtime.usedBytes() > 0);

                db.cleanupExpired();
                Assert.assertEquals(0, db.size());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
