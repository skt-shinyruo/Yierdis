package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.testutil.TestDbs;

import static yier.bubu.redis.testutil.TestBytes.b;

public class FfmDefragMaintenanceIntegrationTest {
    @Test
    public void defragMovesKeysStringsAndCollectionRootsWithoutChangingReads() {
        YierdisDb db = openDefragEnabledDb();
        try {
            Assert.assertTrue(db.writes().strings().setString(b("string"), b("hello"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("m"))).value());

            NativeAllocatorStats before = db.stableMemoryBackend().stats();
            db.defragMaintenance();

            Assert.assertArrayEquals(b("hello"), db.reads().strings().getStringBytes(b("string")));
            Assert.assertEquals(1, db.reads().lists().lrange(b("list"), 0, -1).count());
            Assert.assertArrayEquals(b("v"), copy(db.reads().hashes().hget(b("hash"), b("f"))));
            Assert.assertTrue(db.reads().sets().sismember(b("set"), b("m")));
            Assert.assertEquals(1, db.reads().zsets().zrange(b("zset"), 0, -1, false).count());

            YierdisMemoryStats stats = db.memory().memoryStats();
            NativeAllocatorStats after = db.stableMemoryBackend().stats();
            Assert.assertTrue(stats.nativeDefragLastMovedObjects() > 0L);
            Assert.assertTrue(stats.nativeDefragLastMovedBytes() > 0L);
            Assert.assertTrue(stats.nativeDefragMovedBytes() >= stats.nativeDefragLastMovedBytes());
            Assert.assertEquals(after.quarantinedObjects(), stats.nativeDefragQuarantinedObjects());
            Assert.assertEquals(after.quarantineBytes(), stats.nativeDefragQuarantineBytes());
            Assert.assertEquals(before.objectCount(NativeObjectKind.KEY_BYTES), after.objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(before.objectCount(NativeObjectKind.STRING_BYTES), after.objectCount(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(before.objectCount(NativeObjectKind.LIST_ROOT), after.objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(before.objectCount(NativeObjectKind.HASH_ROOT), after.objectCount(NativeObjectKind.HASH_ROOT));
            Assert.assertEquals(before.objectCount(NativeObjectKind.SET_ROOT), after.objectCount(NativeObjectKind.SET_ROOT));
            Assert.assertEquals(before.objectCount(NativeObjectKind.ZSET_ROOT), after.objectCount(NativeObjectKind.ZSET_ROOT));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void defragSkipsPinnedValuesAndRecordsTheSkip() {
        YierdisDb db = openDefragEnabledDb();
        EntryRecord pinnedRecord = null;
        try {
            Assert.assertTrue(db.writes().strings().setString(b("pinned"), b("value"), SetMode.NORMAL, null).value());
            Assert.assertTrue(db.writes().strings().setString(b("moved"), b("other"), SetMode.NORMAL, null).value());
            pinnedRecord = db.keyLifecycle().liveEntryRecord(b("pinned"));
            db.stableMemoryBackend().pin(pinnedRecord.valueHandle().nativeHandle());

            db.defragMaintenance();

            YierdisMemoryStats stats = db.memory().memoryStats();
            Assert.assertTrue(stats.nativeDefragLastSkippedPinnedObjects() >= 1L);
            Assert.assertTrue(stats.nativeDefragSkippedPinnedObjects() >= 1L);
            Assert.assertArrayEquals(b("value"), db.reads().strings().getStringBytes(b("pinned")));
            Assert.assertArrayEquals(b("other"), db.reads().strings().getStringBytes(b("moved")));
        } finally {
            if (pinnedRecord != null) {
                db.stableMemoryBackend().unpin(pinnedRecord.valueHandle().nativeHandle());
            }
            db.shutdown();
        }
    }

    @Test
    public void repeatedDefragReportsStableObjectCounts() {
        Long expectedMovedObjects = null;
        Long expectedKeyCount = null;
        Long expectedStringCount = null;

        for (int cycle = 0; cycle < 4; cycle++) {
            YierdisDb db = openDefragEnabledDb();
            try {
                populate(db, cycle);
                NativeAllocatorStats before = db.stableMemoryBackend().stats();
                db.defragMaintenance();

                YierdisMemoryStats memory = db.memory().memoryStats();
                NativeAllocatorStats after = db.stableMemoryBackend().stats();
                Assert.assertTrue(memory.nativeDefragLastMovedObjects() > 0L);
                Assert.assertTrue(memory.nativeDefragLastMovedBytes() > 0L);
                Assert.assertEquals(before.objectCount(NativeObjectKind.KEY_BYTES), after.objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(before.objectCount(NativeObjectKind.STRING_BYTES), after.objectCount(NativeObjectKind.STRING_BYTES));

                if (expectedMovedObjects == null) {
                    expectedMovedObjects = memory.nativeDefragLastMovedObjects();
                    expectedKeyCount = after.objectCount(NativeObjectKind.KEY_BYTES);
                    expectedStringCount = after.objectCount(NativeObjectKind.STRING_BYTES);
                } else {
                    Assert.assertEquals(expectedMovedObjects.longValue(), memory.nativeDefragLastMovedObjects());
                    Assert.assertEquals(expectedKeyCount.longValue(), after.objectCount(NativeObjectKind.KEY_BYTES));
                    Assert.assertEquals(expectedStringCount.longValue(), after.objectCount(NativeObjectKind.STRING_BYTES));
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
        Assert.assertTrue(db.writes().strings().setString(b("stable:string:0"), b("hello-" + cycle), SetMode.NORMAL, null).value());
        Assert.assertTrue(db.writes().strings().setString(b("stable:string:1"), b("world-" + cycle), SetMode.NORMAL, null).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("stable:list"), List.of(b("a"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("stable:hash"), List.of(b("f"), b("v"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("stable:set"), List.of(b("m"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("stable:zset"), List.of(b("1"), b("m"))).value());
    }

    private static byte[] copy(BulkStringValue value) {
        try (value) {
            byte[][] captured = new byte[1][];
            value.writeTo(new BulkStringSink() {
                @Override
                public void bulkString(byte[] data) {
                    captured[0] = data == null ? null : data.clone();
                }

                @Override
                public void bulkString(byte[] data, int off, int len) {
                    byte[] copy = new byte[len];
                    System.arraycopy(data, off, copy, 0, len);
                    captured[0] = copy;
                }

                @Override
                public void bulkString(BytesSlice slice) {
                    byte[] copy = new byte[slice.length()];
                    slice.getBytes(0, copy, 0, copy.length);
                    captured[0] = copy;
                }

                @Override
                public void bulkStringLongAscii(long value) {
                    captured[0] = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
                }
            });
            return captured[0];
        }
    }
}
