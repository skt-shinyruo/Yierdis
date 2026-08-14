package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class UnsafeOffHeapKeyspaceTest {
    @Test
    public void cleanupExpiredAndShutdownDoNotLeakOffHeapMemory() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
        }
    }
}
