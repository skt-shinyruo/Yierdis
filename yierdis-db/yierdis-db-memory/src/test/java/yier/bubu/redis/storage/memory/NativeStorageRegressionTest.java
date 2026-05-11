package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class NativeStorageRegressionTest {
    @Test
    public void allNativeRootsReleaseToZeroAfterDelete() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("l"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("h"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("z"), List.of(b("1"), b("m"))).value());
            Assert.assertEquals(Integer.valueOf(1), db.writes().hll().pfadd(b("hll"), List.of(b("x"))).value());

            Assert.assertEquals(6, db.size());
            Assert.assertTrue(db.memory().memoryStats().usedBytesForMaxmemory() > 0);

            Assert.assertEquals(Long.valueOf(6L), db.writes().keyspace().del(List.of(
                    b("s"),
                    b("l"),
                    b("h"),
                    b("set"),
                    b("z"),
                    b("hll")
            )).value());

            YierdisMemoryStats stats = db.memory().memoryStats();
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            Assert.assertEquals(0L, stats.usedBytesForMaxmemory());
            Assert.assertEquals(0L, stats.heapDataBytesEstimate());
            Assert.assertEquals(0L, stats.offHeapUsedBytes());
            Assert.assertEquals(0L, stats.totalEstimatedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void deleteUsesEntryMetadataInsteadOfCompatibilityObjectEstimate() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            long before = db.usedBytesForMaxmemory();
            Assert.assertTrue(before > 0);

            db.keyLifecycle().getLiveObject(b("set")).estimatedBytes = 0L;
            Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("set"))).value());

            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            Assert.assertEquals(0L, db.memory().memoryStats().usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }
}
