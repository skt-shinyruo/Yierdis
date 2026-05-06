package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapKeysToggleTest {
    @Test
    public void defaultDbStoresKeysOffHeap() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void sharedRuntimeDbDoesNotOwnCallerRuntime() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 64, "noeviction", 5, 5, 5);
            db.bindToCurrentThread();
            db.shutdown();
            Assert.assertEquals(0L, runtime.usedBytes());
            var region = runtime.allocateRegion("probe", 1);
            region.close();
        }
    }

    @Test
    public void defaultDbOwnsAndClosesItsRuntimeOnShutdown() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }
}
