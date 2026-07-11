package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
    public void productionCollectionRootsUseSharedNativeAllocatorAndReleaseAfterDeleteAndShutdown() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-collection-root-counts")) {
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
                writeOneOfEachCollection(db);

                assertCollectionRootCounts(db, 1L);

                Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                        b("list"),
                        b("hash"),
                        b("set"),
                        b("zset")
                )).value());
                assertCollectionRootCounts(db, 0L);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-collection-root-shutdown")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            writeOneOfEachCollection(db);
            assertCollectionRootCounts(db, 1L);

            db.shutdown();

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void repeatedNativeShutdownCleanupReleasesKeysStringsCollectionsAndQuarantine() {
        for (int cycle = 0; cycle < 6; cycle++) {
            try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-shutdown-cleanup-repeat-" + cycle)) {
                YierdisDb db = createNativeRegressionDb(runtime, 0, MaxmemoryPolicy.NOEVICTION);
                db.bindToCurrentThread();
                try {
                    Assert.assertTrue(db.writes().strings().setString(
                            b("cleanup:string:" + cycle),
                            b("value"),
                            SetMode.NORMAL,
                            null
                    ).value());
                    writeOneOfEachCollection(db);
                    assertCollectionRootCounts(db, 1L);

                    try (NativeEpochScope epoch = db.keyLifecycle().nativeAllocator().beginEpoch(NativeEpochKind.SNAPSHOT)) {
                        Assert.assertEquals(
                                Long.valueOf(1L),
                                db.writes().keyspace().del(List.of(b("cleanup:string:" + cycle))).value()
                        );
                        YierdisMemoryStats during = db.memory().memoryStats();
                        Assert.assertTrue(during.nativeDefragQuarantinedObjects() > 0L);
                        Assert.assertTrue(during.nativeDefragQuarantineBytes() > 0L);
                    }

                    YierdisMemoryStats afterEpoch = db.memory().memoryStats();
                    Assert.assertEquals(0L, afterEpoch.nativeDefragQuarantinedObjects());
                    Assert.assertEquals(0L, afterEpoch.nativeDefragQuarantineBytes());

                    Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                            b("list"),
                            b("hash"),
                            b("set"),
                            b("zset")
                    )).value());
                    assertCollectionRootCounts(db, 0L);
                    assertNativeDbEmpty(db);
                } finally {
                    db.shutdown();
                }
                Assert.assertEquals("cycle " + cycle + " leaked runtime bytes", 0L, runtime.usedBytes());
            }
        }
    }

    @Test
    public void collectionReadsRemainValidAfterNativeDefragTraversalAndReleaseAllInternalHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-collection-defrag")) {
            YierdisDb db = createNativeRegressionDb(runtime, 0, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            try {
                Assert.assertEquals(Long.valueOf(3L), db.writes().lists().rpush(
                        b("list"),
                        List.of(b("a"), b("b"), b("c"))
                ).value());
                Assert.assertEquals(Long.valueOf(2L), db.writes().hashes().hset(
                        b("hash"),
                        List.of(b("f1"), b("v1"), b("f2"), b("v2"))
                ).value());
                Assert.assertEquals(Long.valueOf(2L), db.writes().sets().sadd(
                        b("set"),
                        List.of(b("m1"), b("m2"))
                ).value());
                Assert.assertEquals(Long.valueOf(2L), db.writes().zsets().zadd(
                        b("zset"),
                        List.of(b("1"), b("z1"), b("2"), b("z2"))
                ).value());

                db.defragMaintenance();

                Assert.assertEquals(List.of("a", "b", "c"), strings(db.reads().lists().lrange(b("list"), 0, -1)));
                Assert.assertArrayEquals(b("v1"), db.reads().hashes().hget(b("hash"), b("f1")));
                Assert.assertArrayEquals(b("v2"), db.reads().hashes().hget(b("hash"), b("f2")));
                Assert.assertTrue(strings(db.reads().sets().smembers(b("set"))).containsAll(List.of("m1", "m2")));
                Assert.assertEquals(List.of("z1", "z2"), strings(db.reads().zsets().zrange(b("zset"), 0, -1, false)));

                Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                        b("list"),
                        b("hash"),
                        b("set"),
                        b("zset")
                )).value());
                assertNativeDbEmpty(db);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
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

            Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("set"))).value());

            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            Assert.assertEquals(0L, db.memory().memoryStats().usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keyLifecycleReadsKeysFromNativeDirectoryWithoutCompatibilityStoreEntry() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            byte[] key = b("native-only");
            EntryRecord record = new EntryRecord(
                    1L,
                    ValueHandle.NULL,
                    31,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    0,
                    -1L,
                    64L,
                    0L
            );
            EntryHandle handle = lifecycle.entryTable().allocate(record);
            lifecycle.keyDirectory().compute(key, (ignored, oldHandle) -> handle);

            Assert.assertEquals(1, lifecycle.keyCount());
            Assert.assertEquals(1, db.size());
            Assert.assertNotNull(lifecycle.keyHandle(key));
            Assert.assertNotNull(lifecycle.liveEntryRecord(key));
            Assert.assertArrayEquals(key, copy(lifecycle.randomKeyHandle()));

            List<String> scanned = new ArrayList<>();
            ScanCursorV2 cursor = lifecycle.scan(ScanCursorV2.start(), 16, (keyHandle, scannedRecord) -> {
                scanned.add(new String(copy(keyHandle), StandardCharsets.UTF_8));
                Assert.assertNotNull(scannedRecord);
                return true;
            });

            Assert.assertEquals(0L, cursor.value());
            Assert.assertEquals(List.of("native-only"), scanned);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keyspaceScanKeepsFreedBytesQuarantinedUntilTheBatchEnds() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-scan-epoch")) {
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
                byte[] keep = b("scan-keep");
                Assert.assertTrue(db.writes().strings().setString(keep, b("value"), SetMode.NORMAL, null).value());

                List<byte[]> scanned = new ArrayList<byte[]>() {
                    private boolean deleted;

                    @Override
                    public boolean add(byte[] value) {
                        boolean added = super.add(value);
                        if (added && !deleted) {
                            deleted = true;
                            Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(keep)).value());
                            YierdisMemoryStats quarantined = db.memory().memoryStats();
                            Assert.assertTrue(quarantined.nativeDefragQuarantinedObjects() > 0L);
                            Assert.assertTrue(quarantined.nativeDefragQuarantineBytes() > 0L);
                        }
                        return added;
                    }
                };

                ScanCursorV2 cursor = db.reads().keyspace().scan(ScanCursorV2.start(), b("scan-*"), 2, scanned);

                Assert.assertEquals(0L, cursor.value());
                Assert.assertEquals(1, scanned.size());
                Assert.assertArrayEquals(keep, scanned.get(0));
                Assert.assertNull(db.reads().strings().getStringBytes(keep));

                YierdisMemoryStats after = db.memory().memoryStats();
                Assert.assertEquals(0L, after.nativeDefragQuarantinedObjects());
                Assert.assertEquals(0L, after.nativeDefragQuarantineBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void snapshotCopiesHeapOwnedBytesAndReleasesFreedBytesAfterTheBatchEnds() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-snapshot-epoch")) {
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
                byte[] key = b("snapshot-keep");
                byte[] value = b("snapshot-value");
                Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());

                List<YierdisSnapshotEntry> entries = new ArrayList<YierdisSnapshotEntry>() {
                    private boolean deleted;

                    @Override
                    public boolean add(YierdisSnapshotEntry entry) {
                        boolean added = super.add(entry);
                        if (added && !deleted) {
                            deleted = true;
                            Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());
                            YierdisMemoryStats quarantined = db.memory().memoryStats();
                            Assert.assertTrue(quarantined.nativeDefragQuarantinedObjects() > 0L);
                            Assert.assertTrue(quarantined.nativeDefragQuarantineBytes() > 0L);
                        }
                        return added;
                    }
                };

                ScanCursorV2 cursor = db.introspection().snapshot(ScanCursorV2.start(), 2, entries);

                Assert.assertEquals(0L, cursor.value());
                Assert.assertEquals(1, entries.size());
                YierdisSnapshotEntry entry = entries.get(0);
                Assert.assertArrayEquals(key, entry.keyBytes());
                Assert.assertEquals(ValueType.STRING, entry.type());
                Assert.assertArrayEquals(value, entry.stringValueBytes());
                Assert.assertNull(db.reads().strings().getStringBytes(key));

                YierdisMemoryStats after = db.memory().memoryStats();
                Assert.assertEquals(0L, after.nativeDefragQuarantinedObjects());
                Assert.assertEquals(0L, after.nativeDefragQuarantineBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void snapshotValueBytesStayStableAfterLaterOverwriteDeleteAndDefrag() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-snapshot-stability")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5,
                    new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE)
            );
            db.bindToCurrentThread();
            try {
                byte[] key = b("snapshot-stable");
                byte[] initial = b("value-v1");
                byte[] updated = b("value-v2");
                Assert.assertTrue(db.writes().strings().setString(key, initial, SetMode.NORMAL, null).value());

                List<YierdisSnapshotEntry> entries = new ArrayList<>();
                Assert.assertEquals(0L, db.introspection().snapshot(ScanCursorV2.start(), 2, entries).value());
                Assert.assertEquals(1, entries.size());
                YierdisSnapshotEntry entry = entries.get(0);
                Assert.assertArrayEquals(key, entry.keyBytes());
                Assert.assertArrayEquals(initial, entry.stringValueBytes());

                Assert.assertTrue(db.writes().strings().setString(key, updated, SetMode.NORMAL, null).value());
                Assert.assertArrayEquals(updated, db.reads().strings().getStringBytes(key));

                Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());
                Assert.assertNull(db.reads().strings().getStringBytes(key));

                db.defragMaintenance();

                Assert.assertArrayEquals(initial, entry.stringValueBytes());
                Assert.assertArrayEquals(initial, entries.get(0).stringValueBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void snapshotEpochDelaysStringReleaseUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-snapshot-allocator-epoch")) {
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
                byte[] key = b("epoch-key");
                Assert.assertTrue(db.writes().strings().setString(key, b("epoch-value"), SetMode.NORMAL, null).value());

                try (NativeEpochScope epoch = db.keyLifecycle().nativeAllocator().beginEpoch(NativeEpochKind.SNAPSHOT)) {
                    Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());

                    NativeAllocatorStats during = db.keyLifecycle().nativeAllocator().stats();
                    Assert.assertTrue(during.logicalUsedBytes() > 0L);
                    Assert.assertTrue(during.reservedBytes() > 0L);
                    Assert.assertTrue(during.quarantinedObjects() > 0L);
                    Assert.assertTrue(during.liveObjects() > 0L);

                    YierdisMemoryStats memoryDuring = db.memory().memoryStats();
                    Assert.assertTrue(memoryDuring.nativeDefragQuarantinedObjects() > 0L);
                    Assert.assertTrue(memoryDuring.nativeDefragQuarantineBytes() > 0L);
                }

                NativeAllocatorStats after = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertEquals(0L, after.logicalUsedBytes());
                Assert.assertEquals(0L, after.reservedBytes());
                Assert.assertEquals(0L, after.quarantinedObjects());
                Assert.assertEquals(0L, after.liveObjects());

                YierdisMemoryStats memoryAfter = db.memory().memoryStats();
                Assert.assertEquals(0L, memoryAfter.nativeDefragQuarantinedObjects());
                Assert.assertEquals(0L, memoryAfter.nativeDefragQuarantineBytes());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void defaultSharedNativeSlotCapacityStillOverflowsAroundNinetyThousandStringKeys() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-string-slot-capacity-default")) {
            YierdisDb db = createNativeRegressionDb(runtime, 0, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            int keyCount = 90_000;
            try {
                try {
                    for (int i = 0; i < keyCount; i++) {
                        Assert.assertTrue(db.writes().strings().setString(
                                b("slot:string:" + i),
                                b("v"),
                                SetMode.NORMAL,
                                null
                        ).value());
                    }
                    Assert.fail("expected default shared native slot capacity to overflow before 90k string keys");
                } catch (YierdisCommandException e) {
                    Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
                }
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void explicitNativeSlotCapacitySupportsNinetyThousandStringKeysWithoutLeaks() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-string-slot-capacity-override")) {
            YierdisDb db = createNativeRegressionDb(runtime, 0, MaxmemoryPolicy.NOEVICTION, 512 * 1024);
            db.bindToCurrentThread();
            int keyCount = 90_000;
            try {
                for (int i = 0; i < keyCount; i++) {
                    Assert.assertTrue(db.writes().strings().setString(
                            b("slot:string:" + i),
                            b("v"),
                            SetMode.NORMAL,
                            null
                    ).value());
                }

                Assert.assertEquals(keyCount, db.size());
                NativeAllocatorStats populated = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertEquals(keyCount, populated.objectCount(NativeObjectKind.ENTRY_RECORD));
                Assert.assertEquals(keyCount, populated.objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(keyCount, populated.objectCount(NativeObjectKind.STRING_BYTES));

                Assert.assertEquals(
                        Long.valueOf(keyCount),
                        Long.valueOf(deleteDeterministicKeysInBatches(db, "slot:string:", keyCount, 1024))
                );
                assertNativeDbEmpty(db);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] stringKey = b("native-string");
            byte[] counterKey = b("native-counter");

            Assert.assertTrue(db.writes().strings().setString(stringKey, b("hello"), SetMode.NORMAL, null).value());
            assertNativeStringOnly(db, stringKey, b("hello"));
            Assert.assertTrue(db.reads().keyspace().existsKey(view(stringKey)));
            Assert.assertEquals(ValueType.STRING, db.reads().keyspace().typeOf(view(stringKey)));
            Assert.assertEquals(List.of("native-string"), strings(db.reads().keyspace().keys(b("native-*"), 16, 0)));

            List<byte[]> scanned = new ArrayList<>();
            ScanCursorV2 cursor = db.reads().keyspace().scan(ScanCursorV2.start(), b("native-*"), 16, scanned);
            Assert.assertEquals(0L, cursor.value());
            Assert.assertEquals(List.of("native-string"), strings(scanned));

            Assert.assertEquals(Long.valueOf(11L), db.writes().strings().append(stringKey, sliceOf(b(" world"))).value());
            assertNativeStringOnly(db, stringKey, b("hello world"));

            Assert.assertEquals(Integer.valueOf(0), db.writes().strings().setBit(stringKey, 0, 1).value());
            Assert.assertEquals(1, db.reads().strings().getBit(view(stringKey), 0));
            assertNativeStringOnly(db, stringKey, db.reads().strings().getStringBytes(stringKey));

            Assert.assertTrue(db.writes().strings().setString(counterKey, b("41"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(42L), db.writes().strings().incrBy(counterKey, 1L).value());
            assertNativeStringOnly(db, counterKey, b("42"));

            EntryRecord counterRecord = db.keyLifecycle().liveEntryRecord(counterKey);
            Assert.assertEquals(ValueEncoding.STRING_INT, counterRecord.encoding());

            Assert.assertEquals(Long.valueOf(2L), db.writes().keyspace().del(List.of(stringKey, counterKey)).value());
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-db-churn")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    2_000_000L,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                List<byte[]> keys = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    byte[] key = b("churn-" + i);
                    keys.add(key);
                    Assert.assertTrue(db.writes().strings().setString(
                            key,
                            b("value-" + i),
                            SetMode.NORMAL,
                            null
                    ).value());
                    if ((i & 1) == 0) {
                        Assert.assertEquals(Long.valueOf(("value-" + i + "-tail").length()),
                                db.writes().strings().append(key, sliceOf(b("-tail"))).value());
                    }
                    if (i % 5 == 0) {
                        Assert.assertTrue(db.writes().ttl().pexpire(view(key), 60_000L).value());
                    }
                }

                YierdisMemoryStats populated = db.memory().memoryStats();
                Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
                Assert.assertEquals(db.size(),
                        db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes() > 0L);
                Assert.assertEquals(db.size(), populated.keyCount());
                Assert.assertEquals(db.usedBytesForMaxmemory(), populated.usedBytesForMaxmemory());
                Assert.assertTrue(populated.offHeapUsedBytes() > 0L);

                List<byte[]> evens = new ArrayList<>();
                for (int i = 0; i < keys.size(); i += 2) {
                    evens.add(keys.get(i));
                }
                Assert.assertEquals(Long.valueOf(evens.size()), db.writes().keyspace().del(evens).value());

                YierdisMemoryStats afterDelete = db.memory().memoryStats();
                Assert.assertEquals(db.size(), afterDelete.keyCount());
                Assert.assertEquals(db.usedBytesForMaxmemory(), afterDelete.usedBytesForMaxmemory());
                Assert.assertTrue(afterDelete.usedBytesForMaxmemory() <= populated.usedBytesForMaxmemory());

                Assert.assertEquals(Long.valueOf(keys.size() - evens.size()), db.writes().keyspace().del(keys).value());
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0L, db.memory().memoryStats().usedBytesForMaxmemory());
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeAllocatorCleanupRemainsStableUnderNarrowMaxmemory() {
        for (int cycle = 0; cycle < 4; cycle++) {
            try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-maxmemory-repeat-" + cycle)) {
                YierdisDb db = createNativeRegressionDb(runtime, 4096L, MaxmemoryPolicy.NOEVICTION);
                db.bindToCurrentThread();
                List<byte[]> written = new ArrayList<>();
                try {
                    for (int i = 0; i < 16; i++) {
                        byte[] key = b("maxmemory:" + cycle + ":" + i);
                        byte[] value = b("value-" + i + "-native-maxmemory");
                        boolean accepted;
                        try {
                            accepted = db.writes().strings().setString(key, value, SetMode.NORMAL, null).value();
                        } catch (YierdisCommandException e) {
                            Assert.assertTrue(e.getMessage().contains("OOM"));
                            Assert.assertTrue(
                                    "expected at least one accepted write before maxmemory rejection",
                                    written.size() > 0
                            );
                            break;
                        }
                        if (!accepted) {
                            break;
                        }
                        written.add(key);
                        assertMemoryStatsCoherent(db);
                        Assert.assertTrue(db.usedBytesForMaxmemory() <= 4096L);
                    }

                    Assert.assertTrue("expected at least one accepted write", written.size() > 0);
                    NativeAllocatorStats populated = db.keyLifecycle().nativeAllocator().stats();
                    Assert.assertEquals(db.size(), populated.objectCount(NativeObjectKind.KEY_BYTES));
                    Assert.assertEquals(db.size(), populated.objectCount(NativeObjectKind.STRING_BYTES));
                    Assert.assertEquals(db.size(), populated.objectCount(NativeObjectKind.ENTRY_RECORD));
                    Assert.assertTrue(populated.logicalUsedBytes() > 0L);

                    Assert.assertEquals(Long.valueOf(written.size()), db.writes().keyspace().del(written).value());
                    assertNativeDbEmpty(db);
                } finally {
                    db.shutdown();
                }
                Assert.assertEquals(0L, runtime.usedBytes());
            }
        }
    }

    @Test
    public void deterministicMixedNativeDbChurnPreservesResultsAndReleasesRuntime() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-db-mixed-churn")) {
            YierdisDb db = createNativeRegressionDb(runtime, 2_000_000L, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            try {
                runDeterministicMixedNativeDbChurn(db, 0x5EED_7A11L, 128);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void repeatedDeterministicMixedNativeDbChurnReleasesRuntimeEveryCycle() {
        for (int cycle = 0; cycle < 5; cycle++) {
            try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-db-mixed-churn-repeat-" + cycle)) {
                YierdisDb db = createNativeRegressionDb(runtime, 2_000_000L, MaxmemoryPolicy.NOEVICTION);
                db.bindToCurrentThread();
                try {
                    runDeterministicMixedNativeDbChurn(db, 0x5EED_7A11L + cycle, 128);
                    assertNativeDbEmpty(db);
                } finally {
                    db.shutdown();
                }
                Assert.assertEquals("cycle " + cycle + " leaked runtime bytes", 0L, runtime.usedBytes());
            }
        }
    }

    private static YierdisDb createNativeRegressionDb(
            YierdisFfmMemoryRuntime runtime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy
    ) {
        return YierdisDb.createWithSharedFfmRuntimeAndNativeSlotCapacity(
                runtime,
                maxmemoryBytes,
                maxmemoryPolicy,
                5,
                5,
                5,
                new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE),
                0
        );
    }

    private static YierdisDb createNativeRegressionDb(
            YierdisFfmMemoryRuntime runtime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int nativeSlotCapacity
    ) {
        return YierdisDb.createWithSharedFfmRuntimeAndNativeSlotCapacity(
                runtime,
                maxmemoryBytes,
                maxmemoryPolicy,
                5,
                5,
                5,
                new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE),
                nativeSlotCapacity
        );
    }

    private static long deleteDeterministicKeysInBatches(YierdisDb db, String keyPrefix, int keyCount, int batchSize) {
        long deleted = 0L;
        List<byte[]> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < keyCount; i++) {
            batch.add(b(keyPrefix + i));
            if (batch.size() == batchSize) {
                deleted += db.writes().keyspace().del(batch).value();
                batch = new ArrayList<>(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            deleted += db.writes().keyspace().del(batch).value();
        }
        return deleted;
    }

    private static void runDeterministicMixedNativeDbChurn(YierdisDb db, long seed, int operationCount) {
        Random random = new Random(seed);
        Map<String, String> strings = new HashMap<>();
        Set<String> trackedKeys = new LinkedHashSet<>();
        String[] stringKeys = {"mix:s:0", "mix:s:1", "mix:s:2", "mix:s:3", "mix:s:4"};
        String[] collectionKeys = {"mix:list", "mix:hash", "mix:set", "mix:zset", "mix:hll"};
        boolean expiredTtlCleanup = false;
        boolean scannedAndSnapshotted = false;
        boolean ranDefragMaintenance = false;

        for (int op = 0; op < operationCount; op++) {
            int choice = random.nextInt(14);
            String key = stringKeys[random.nextInt(stringKeys.length)];
            switch (choice) {
                case 0, 1, 2 -> {
                    String value = "v" + op + ":" + random.nextInt(1000);
                    Assert.assertTrue(db.writes().strings().setString(
                            b(key),
                            b(value),
                            SetMode.NORMAL,
                            null
                    ).value());
                    strings.put(key, value);
                    trackedKeys.add(key);
                    assertStringMatchesModel(db, key, value);
                }
                case 3, 4 -> {
                    if (strings.containsKey(key)) {
                        String suffix = ":a" + op;
                        String expected = strings.get(key) + suffix;
                        Assert.assertEquals(Long.valueOf(expected.length()),
                                db.writes().strings().append(b(key), sliceOf(b(suffix))).value());
                        strings.put(key, expected);
                        assertStringMatchesModel(db, key, expected);
                    } else {
                        Assert.assertNull(db.reads().strings().getStringBytes(b(key)));
                    }
                }
                case 5 -> {
                    if (strings.containsKey(key)) {
                        Assert.assertArrayEquals(b(strings.get(key)), db.reads().strings().getStringBytes(b(key)));
                        Assert.assertEquals(strings.get(key).length(), db.reads().strings().strlen(view(b(key))));
                    } else {
                        Assert.assertNull(db.reads().strings().getStringBytes(b(key)));
                        Assert.assertEquals(0L, db.reads().strings().strlen(view(b(key))));
                    }
                }
                case 6 -> {
                    if (strings.containsKey(key)) {
                        Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b(key))).value());
                        strings.remove(key);
                    } else {
                        Assert.assertEquals(Long.valueOf(0L), db.writes().keyspace().del(List.of(b(key))).value());
                    }
                    trackedKeys.add(key);
                }
                case 7 -> {
                    if (!expiredTtlCleanup) {
                        String ttlKey = "mix:ttl-expired";
                        Assert.assertTrue(db.writes().strings().setString(
                                b(ttlKey),
                                b("ttl-" + op),
                                SetMode.NORMAL,
                                ExpireOption.px(0)
                        ).value());
                        trackedKeys.add(ttlKey);
                        db.cleanupExpired();
                        Assert.assertNull(db.reads().strings().getStringBytes(b(ttlKey)));
                        Assert.assertEquals(-2L, db.reads().ttl().ttlMillis(view(b(ttlKey))));
                        expiredTtlCleanup = true;
                    } else {
                        String ttlKey = stringKeys[op % stringKeys.length];
                        Assert.assertTrue(db.writes().strings().setString(b(ttlKey), b("ttl-" + op), SetMode.NORMAL, null).value());
                        Assert.assertTrue(db.writes().ttl().pexpire(view(b(ttlKey)), 60_000L).value());
                        Assert.assertTrue(db.reads().ttl().ttlMillis(view(b(ttlKey))) > 0L);
                        strings.put(ttlKey, "ttl-" + op);
                        trackedKeys.add(ttlKey);
                    }
                }
                case 8 -> {
                    Assert.assertEquals(Long.valueOf(1L),
                            db.writes().lists().rpush(b("mix:list"), List.of(b("l" + op))).value());
                    Assert.assertEquals(List.of("l" + op),
                            strings(db.writes().lists().lpop(b("mix:list"), 1).value()));
                    trackedKeys.add("mix:list");
                }
                case 9 -> {
                    Assert.assertEquals(Long.valueOf(1L),
                            db.writes().hashes().hset(b("mix:hash"), List.of(b("f" + op), b("h" + op))).value());
                    Assert.assertArrayEquals(b("h" + op), db.reads().hashes().hget(b("mix:hash"), b("f" + op)));
                    Assert.assertEquals(Long.valueOf(1L),
                            db.writes().hashes().hdel(b("mix:hash"), List.of(b("f" + op))).value());
                    trackedKeys.add("mix:hash");
                }
                case 10 -> {
                    Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("mix:set"), List.of(b("m" + op))).value());
                    Assert.assertTrue(db.reads().sets().sismember(b("mix:set"), b("m" + op)));
                    if ((op & 1) == 0) {
                        Assert.assertEquals(Long.valueOf(1L), db.writes().sets().srem(b("mix:set"), List.of(b("m" + op))).value());
                    }
                    trackedKeys.add("mix:set");
                }
                case 11 -> {
                    Assert.assertEquals(Long.valueOf(1L),
                            db.writes().zsets().zadd(b("mix:zset"), List.of(b(String.valueOf(op)), b("z" + op))).value());
                    Assert.assertTrue(db.reads().zsets().zrange(b("mix:zset"), 0, -1, false).count() >= 1);
                    if ((op & 1) == 0) {
                        Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zrem(b("mix:zset"), List.of(b("z" + op))).value());
                    }
                    trackedKeys.add("mix:zset");
                }
                case 12 -> {
                    Assert.assertEquals(Integer.valueOf(1), db.writes().hll().pfadd(b("mix:hll"), List.of(b("hll-" + op))).value());
                    Assert.assertTrue(db.reads().hll().pfcount(List.of(b("mix:hll"))) >= 1L);
                    trackedKeys.add("mix:hll");
                }
                case 13 -> {
                    List<byte[]> scanned = new ArrayList<>();
                    ScanCursorV2 scan = db.reads().keyspace().scan(ScanCursorV2.start(), b("mix:*"), 32, scanned);
                    Assert.assertEquals(0L, scan.value());
                    List<YierdisSnapshotEntry> snapshot = new ArrayList<>();
                    ScanCursorV2 snapshotCursor = db.introspection().snapshot(ScanCursorV2.start(), 32, snapshot);
                    Assert.assertEquals(0L, snapshotCursor.value());
                    db.defragMaintenance();
                    scannedAndSnapshotted = true;
                    ranDefragMaintenance = true;
                }
                default -> throw new AssertionError("unexpected op choice " + choice);
            }

            if (op % 8 == 0) {
                assertMemoryStatsCoherent(db);
            }
        }

        for (Map.Entry<String, String> entry : strings.entrySet()) {
            assertStringMatchesModel(db, entry.getKey(), entry.getValue());
        }
        Assert.assertTrue("expected TTL expiry cleanup branch", expiredTtlCleanup);
        Assert.assertTrue("expected scan/snapshot branch", scannedAndSnapshotted);
        Assert.assertTrue("expected defrag maintenance branch", ranDefragMaintenance);
        for (String collectionKey : collectionKeys) {
            trackedKeys.add(collectionKey);
        }
        for (String stringKey : stringKeys) {
            trackedKeys.add(stringKey);
        }
        trackedKeys.add("mix:ttl-expired");
        assertMemoryStatsCoherent(db);

        List<byte[]> deleteKeys = new ArrayList<>();
        for (String trackedKey : trackedKeys) {
            deleteKeys.add(b(trackedKey));
        }
        int sizeBeforeDelete = db.size();
        Assert.assertEquals(Long.valueOf(sizeBeforeDelete), db.writes().keyspace().del(deleteKeys).value());
        assertNativeDbEmpty(db);
    }

    private static void assertNativeDbEmpty(YierdisDb db) {
        YierdisMemoryStats empty = db.memory().memoryStats();
        NativeAllocatorStats allocator = db.keyLifecycle().nativeAllocator().stats();
        Assert.assertEquals(0, db.size());
        Assert.assertEquals(0L, db.usedBytesForMaxmemory());
        Assert.assertEquals(0L, empty.keyCount());
        Assert.assertEquals(0L, empty.usedBytesForMaxmemory());
        Assert.assertEquals(0L, empty.heapDataBytesEstimate());
        Assert.assertEquals(0L, empty.offHeapUsedBytes());
        Assert.assertEquals(0L, empty.totalEstimatedBytes());
        Assert.assertEquals(0L, empty.nativeDefragQuarantinedObjects());
        Assert.assertEquals(0L, empty.nativeDefragQuarantineBytes());
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.STRING_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.ENTRY_RECORD));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.KEY_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.LIST_ROOT));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.HASH_ROOT));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.SET_ROOT));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.ZSET_ROOT));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.LIST_NODE));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.LISTPACK_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.HASH_FIELD_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.HASH_VALUE_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.SET_MEMBER_BYTES));
        Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.ZSET_MEMBER_BYTES));
        Assert.assertEquals(0L, allocator.logicalUsedBytes());
        Assert.assertEquals(0L, allocator.quarantinedObjects());
    }

    private static void writeOneOfEachCollection(YierdisDb db) {
        Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("field"), b("value"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("member"))).value());
        Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("member"))).value());
    }

    private static void assertCollectionRootCounts(YierdisDb db, long expected) {
        Assert.assertEquals(expected, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.LIST_ROOT));
        Assert.assertEquals(expected, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.HASH_ROOT));
        Assert.assertEquals(expected, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.SET_ROOT));
        Assert.assertEquals(expected, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.ZSET_ROOT));
    }

    private static void assertNativeStringOnly(YierdisDb db, byte[] key, byte[] expectedBytes) {
        YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
        KeyHandle keyHandle = lifecycle.keyHandle(key);
        Assert.assertNotNull(keyHandle);
        EntryRecord record = lifecycle.liveEntryRecord(key);
        Assert.assertNotNull(record);
        Assert.assertEquals(ValueType.STRING, record.type());
        Assert.assertArrayEquals(expectedBytes, lifecycle.stringRoot().copy(record.valueHandle()));
        Assert.assertArrayEquals(expectedBytes, db.reads().strings().getStringBytes(key));
        Assert.assertEquals(expectedBytes.length, db.reads().strings().strlen(view(key)));
    }

    private static void assertStringMatchesModel(YierdisDb db, String key, String expected) {
        byte[] keyBytes = b(key);
        byte[] expectedBytes = b(expected);
        Assert.assertArrayEquals(expectedBytes, db.reads().strings().getStringBytes(keyBytes));
        Assert.assertEquals(expected.length(), db.reads().strings().strlen(view(keyBytes)));
    }

    private static void assertMemoryStatsCoherent(YierdisDb db) {
        YierdisMemoryStats stats = db.memory().memoryStats();
        Assert.assertEquals(db.size(), stats.keyCount());
        Assert.assertEquals(db.usedBytesForMaxmemory(), stats.usedBytesForMaxmemory());
    }

    private static byte[] copy(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.getByte(i);
        }
        return out;
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }

    private static BytesSlice sliceOf(byte[] data) {
        return new BytesSlice() {
            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(data, 0, data.length);
            }

            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }

    private static List<String> strings(List<byte[]> values) {
        List<String> out = new ArrayList<>(values.size());
        for (byte[] value : values) {
            out.add(new String(value, StandardCharsets.UTF_8));
        }
        return out;
    }

    private static List<String> strings(BulkStringSequence values) {
        List<String> out = new ArrayList<>(values.count());
        values.emitTo(new BulkStringSink() {
            @Override
            public void bulkString(byte[] data) {
                out.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
            }

            @Override
            public void bulkString(byte[] data, int off, int len) {
                out.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
            }

            @Override
            public void bulkString(BytesSlice slice) {
                if (slice == null) {
                    out.add(null);
                    return;
                }
                byte[] data = new byte[slice.length()];
                slice.getBytes(0, data, 0, data.length);
                out.add(new String(data, StandardCharsets.UTF_8));
            }

            @Override
            public void bulkStringLongAscii(long value) {
                out.add(Long.toString(value));
            }
        });
        return out;
    }
}
