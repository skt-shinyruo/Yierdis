package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbDefragMaintenanceTest {
    @Test
    public void disabledDefragMaintenanceIsNoOp() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-defrag-disabled")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                Assert.assertTrue(db.writes().strings().setString(b("k"), b("value"), SetMode.NORMAL, null).value());
                NativeAllocatorStats before = db.keyLifecycle().nativeAllocator().stats();

                db.defragMaintenance();

                YierdisMemoryStats stats = db.memory().memoryStats();
                NativeAllocatorStats after = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertEquals(before.defragMovedBytes(), after.defragMovedBytes());
                Assert.assertEquals(0L, stats.nativeDefragLastMovedObjects());
                Assert.assertEquals(0L, stats.nativeDefragLastMovedBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void deterministicDefragMovesNativeKeysStringsAndCollectionRootsWithoutChangingReads() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-defrag-deterministic")) {
            YierdisDb db = createDefragEnabledDb(runtime, new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE));
            db.bindToCurrentThread();
            try {
                Assert.assertTrue(db.writes().strings().setString(b("string"), b("hello"), SetMode.NORMAL, null).value());
                Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
                Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("f"), b("v"))).value());
                Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
                Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("m"))).value());

                NativeAllocatorStats before = db.keyLifecycle().nativeAllocator().stats();

                db.defragMaintenance();

                Assert.assertArrayEquals(b("hello"), db.reads().strings().getStringBytes(b("string")));
                Assert.assertEquals(1, db.reads().lists().lrange(b("list"), 0, -1).count());
                Assert.assertArrayEquals(b("v"), db.reads().hashes().hget(b("hash"), b("f")));
                Assert.assertTrue(db.reads().sets().sismember(b("set"), b("m")));
                Assert.assertEquals(1, db.reads().zsets().zrange(b("zset"), 0, -1, false).count());

                YierdisMemoryStats stats = db.memory().memoryStats();
                NativeAllocatorStats after = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertTrue(stats.nativeDefragLastMovedObjects() > 0L);
                Assert.assertTrue(stats.nativeDefragLastMovedBytes() > 0L);
                Assert.assertTrue(stats.nativeDefragMovedBytes() >= stats.nativeDefragLastMovedBytes());
                Assert.assertEquals(after.quarantinedObjects(), stats.nativeDefragQuarantinedObjects());
                Assert.assertEquals(after.quarantineBytes(), stats.nativeDefragQuarantineBytes());
                Assert.assertEquals(before.objectCount(NativeObjectKind.KEY_BYTES), after.objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(before.objectCount(NativeObjectKind.STRING_BYTES), after.objectCount(NativeObjectKind.STRING_BYTES));
                Assert.assertEquals(before.objectCount(NativeObjectKind.LIST_NODE), after.objectCount(NativeObjectKind.LIST_NODE));
                Assert.assertEquals(before.objectCount(NativeObjectKind.HASH_NODE), after.objectCount(NativeObjectKind.HASH_NODE));
                Assert.assertEquals(before.objectCount(NativeObjectKind.SET_NODE), after.objectCount(NativeObjectKind.SET_NODE));
                Assert.assertEquals(before.objectCount(NativeObjectKind.ZSET_NODE), after.objectCount(NativeObjectKind.ZSET_NODE));
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void pinnedNativeObjectIsSkippedAndRecorded() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-defrag-pinned")) {
            YierdisDb db = createDefragEnabledDb(runtime, new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE));
            db.bindToCurrentThread();
            NativeAllocator allocator = null;
            EntryRecord pinnedRecord = null;
            try {
                Assert.assertTrue(db.writes().strings().setString(b("pinned"), b("value"), SetMode.NORMAL, null).value());
                Assert.assertTrue(db.writes().strings().setString(b("moved"), b("other"), SetMode.NORMAL, null).value());
                allocator = db.keyLifecycle().nativeAllocator();
                pinnedRecord = db.keyLifecycle().liveEntryRecord(b("pinned"));
                allocator.pin(pinnedRecord.valueHandle().nativeHandle());

                db.defragMaintenance();

                YierdisMemoryStats stats = db.memory().memoryStats();
                Assert.assertTrue(stats.nativeDefragLastSkippedPinnedObjects() >= 1L);
                Assert.assertTrue(stats.nativeDefragSkippedPinnedObjects() >= 1L);
                Assert.assertArrayEquals(b("value"), db.reads().strings().getStringBytes(b("pinned")));
                Assert.assertArrayEquals(b("other"), db.reads().strings().getStringBytes(b("moved")));
            } finally {
                if (allocator != null && pinnedRecord != null) {
                    allocator.unpin(pinnedRecord.valueHandle().nativeHandle());
                }
                db.shutdown();
            }
        }
    }

    @Test
    public void defragMaintenanceDoesNotTouchLegacyOffHeapAdapterBytes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-defrag-legacy-offheap")) {
            YierdisDb db = createDefragEnabledDb(runtime, new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE));
            db.bindToCurrentThread();
            OffHeapBuf blob = null;
            try {
                blob = db.offHeapAllocator.allocate(32);
                blob.setBytes(0, b("adapter-native-bytes"), 0, "adapter-native-bytes".length());
                long usedBefore = db.offHeapAllocator.usedBytes();
                byte firstBefore = blob.getByte(0);

                Assert.assertTrue(db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null).value());
                db.defragMaintenance();

                Assert.assertEquals(usedBefore, db.offHeapAllocator.usedBytes());
                Assert.assertEquals(firstBefore, blob.getByte(0));
            } finally {
                if (blob != null) {
                    blob.close();
                }
                db.shutdown();
            }
        }
    }

    private static YierdisDb createDefragEnabledDb(YierdisFfmMemoryRuntime runtime, NativeDefragOptions options) {
        return YierdisDb.createWithSharedFfmRuntime(
                runtime,
                0,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                options
        );
    }
}
