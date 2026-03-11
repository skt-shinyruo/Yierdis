package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class MemoryStatsAccountingConsistencyTest {
    @Test
    public void memoryStatsUsedBytesForMaxmemoryMatchesEnforcementIncludingTtlEstimate() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.pexpire(b("k"), 10_000));

            long enforcement = db.usedBytesForMaxmemory();
            long stats = db.memoryStats().usedBytesForMaxmemory();
            Assert.assertEquals(enforcement, stats);
        } finally {
            db.shutdown();
        }
    }
}

