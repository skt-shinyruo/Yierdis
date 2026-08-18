package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class MemoryStatsAccountingConsistencyTest {
    @Test
    public void memoryStatsUsedBytesForMaxmemoryMatchesEnforcementIncludingTtlEstimate() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().pexpire(view(b("k")), 10_000).value());

            long enforcement = db.usedBytesForMaxmemory();
            long stats = db.memoryStats().usedBytesForMaxmemory();
            Assert.assertEquals(enforcement, stats);
        } finally {
            db.shutdown();
        }
    }

}
