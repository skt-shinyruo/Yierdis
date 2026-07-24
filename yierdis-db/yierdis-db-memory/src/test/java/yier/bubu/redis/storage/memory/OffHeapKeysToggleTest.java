package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapKeysToggleTest {
    @Test
    public void defaultDbStoresKeysInStableMemoryBackend() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void databaseOwnsTheBackendCreatedByItsFactory() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 64, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            db.shutdown();
            Assert.assertThrows(IllegalStateException.class, runtime::usedBytes);
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> runtime.allocateRegion("probe", 1)
            );
        }
    }

    @Test
    public void defaultDbOwnsAndClosesItsRuntimeOnShutdown() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());
        } finally {
            db.shutdown();
        }
    }
}
