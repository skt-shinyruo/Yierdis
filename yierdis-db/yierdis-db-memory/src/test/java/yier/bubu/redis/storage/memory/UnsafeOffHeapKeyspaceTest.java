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
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class UnsafeOffHeapKeyspaceTest {
    @Test
    public void cleanupExpiredAndShutdownDoNotLeakOffHeapMemory() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
