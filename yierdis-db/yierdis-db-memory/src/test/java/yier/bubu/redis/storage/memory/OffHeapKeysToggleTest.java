package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapKeysToggleTest {
    @Test
    public void defaultDbStoresKeysInNativeAllocator() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.memory().memoryStats().keysStoredOffHeap());
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
            Assert.assertFalse(db.memory().memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }
}
