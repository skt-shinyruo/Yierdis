package yier.bubu.redis.storage.memory;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.testutil.TestDbs;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.byteValue;
import static yier.bubu.redis.testutil.TestBytes.stringValue;

public class FfmDefragMaintenanceIntegrationTest {
    @Test
    public void defragMovesKeysStringsAndCollectionRootsWithoutChangingReads() {
        YierdisDb db = openDefragEnabledDb();
        try {
            Assert.assertTrue(db.strings().setString(b("string"), b("hello"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.lists().rpush(b("list"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.hashes().hset(b("hash"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.zsets().zadd(b("zset"), List.of(b("1"), b("m"))).value());

            NativeAllocatorStats before = YierdisDbTestAccess.backend(db).stats();
            db.defragMaintenance();

            Assert.assertArrayEquals(b("hello"), stringValue(db.strings(), b("string")));
            try (ByteSequenceSource values = db.lists().lrange(b("list"), 0, -1)) {
                Assert.assertEquals(1, values.elementCount());
            }
            Assert.assertArrayEquals(b("v"), byteValue(db.hashes().hget(b("hash"), b("f"))));
            Assert.assertTrue(db.sets().sismember(b("set"), b("m")));
            try (ByteSequenceSource values = db.zsets().zrange(b("zset"), 0, -1, false)) {
                Assert.assertEquals(1, values.elementCount());
            }

            YierdisMemoryStats stats = db.memoryStats();
            NativeAllocatorStats after = YierdisDbTestAccess.backend(db).stats();
            Assert.assertTrue(stats.nativeDefragLastMovedObjects() > 0L);
            Assert.assertTrue(stats.nativeDefragLastMovedBytes() > 0L);
            Assert.assertTrue(stats.nativeDefragMovedBytes() >= stats.nativeDefragLastMovedBytes());
            Assert.assertEquals(after.quarantinedObjects(), stats.nativeDefragQuarantinedObjects());
            Assert.assertEquals(after.quarantineBytes(), stats.nativeDefragQuarantineBytes());
            Assert.assertEquals(before.liveObjects(), after.liveObjects());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void defragSkipsPinnedValuesAndRecordsTheSkip() {
        YierdisDb db = openDefragEnabledDb();
        EntryRecord pinnedRecord = null;
        try {
            Assert.assertTrue(db.strings().setString(b("pinned"), b("value"), SetMode.NORMAL, null).value());
            Assert.assertTrue(db.strings().setString(b("moved"), b("other"), SetMode.NORMAL, null).value());
            pinnedRecord = db.keyLifecycle().liveEntryRecord(b("pinned"));
            YierdisDbTestAccess.backend(db).pin(pinnedRecord.valueHandle().nativeHandle());

            db.defragMaintenance();

            YierdisMemoryStats stats = db.memoryStats();
            Assert.assertTrue(stats.nativeDefragLastSkippedPinnedObjects() >= 1L);
            Assert.assertTrue(stats.nativeDefragSkippedPinnedObjects() >= 1L);
            Assert.assertArrayEquals(b("value"), stringValue(db.strings(), b("pinned")));
            Assert.assertArrayEquals(b("other"), stringValue(db.strings(), b("moved")));
        } finally {
            if (pinnedRecord != null) {
                YierdisDbTestAccess.backend(db).unpin(pinnedRecord.valueHandle().nativeHandle());
            }
            db.shutdown();
        }
    }

    @Test
    public void repeatedDefragReportsStableObjectCounts() {
        Long expectedMovedObjects = null;
        Long expectedLiveObjects = null;

        for (int cycle = 0; cycle < 4; cycle++) {
            YierdisDb db = openDefragEnabledDb();
            try {
                populate(db, cycle);
                NativeAllocatorStats before = YierdisDbTestAccess.backend(db).stats();
                db.defragMaintenance();

                YierdisMemoryStats memory = db.memoryStats();
                NativeAllocatorStats after = YierdisDbTestAccess.backend(db).stats();
                Assert.assertTrue(memory.nativeDefragLastMovedObjects() > 0L);
                Assert.assertTrue(memory.nativeDefragLastMovedBytes() > 0L);
                Assert.assertEquals(before.liveObjects(), after.liveObjects());

                if (expectedMovedObjects == null) {
                    expectedMovedObjects = memory.nativeDefragLastMovedObjects();
                    expectedLiveObjects = after.liveObjects();
                } else {
                    Assert.assertEquals(expectedMovedObjects.longValue(), memory.nativeDefragLastMovedObjects());
                    Assert.assertEquals(expectedLiveObjects.longValue(), after.liveObjects());
                }
            } finally {
                db.shutdown();
            }
        }
    }

    private static YierdisDb openDefragEnabledDb() {
        YierdisDb db = TestDbs.createFfmDb(
                new DbEngineConfig(
                        0,
                        0L,
                        MaxmemoryPolicy.NOEVICTION,
                        5,
                        5L,
                        5L,
                        new DbDefragConfig(true, 1_000_000L, 1_000L, 60_000L)
                ),
                4_096
        );
        db.bindToCurrentThread();
        return db;
    }

    private static void populate(YierdisDb db, int cycle) {
        Assert.assertTrue(db.strings().setString(b("stable:string:0"), b("hello-" + cycle), SetMode.NORMAL, null).value());
        Assert.assertTrue(db.strings().setString(b("stable:string:1"), b("world-" + cycle), SetMode.NORMAL, null).value());
        Assert.assertEquals(Long.valueOf(1L), db.lists().rpush(b("stable:list"), List.of(b("a"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.hashes().hset(b("stable:hash"), List.of(b("f"), b("v"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.sets().sadd(b("stable:set"), List.of(b("m"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.zsets().zadd(b("stable:zset"), List.of(b("1"), b("m"))).value());
    }

}
