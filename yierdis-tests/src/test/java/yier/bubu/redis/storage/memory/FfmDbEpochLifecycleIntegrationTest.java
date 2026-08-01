package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.testutil.TestDbs;

import static yier.bubu.redis.testutil.TestBytes.b;

public class FfmDbEpochLifecycleIntegrationTest {
    @Test
    public void snapshotEpochDefersReleaseUntilClosed() {
        YierdisDb db = openFfm();
        try {
            byte[] key = b("epoch-key");
            Assert.assertTrue(db.writes().strings().setString(key, b("epoch-value"), SetMode.NORMAL, null).value());

            try (NativeEpochScope ignored = db.stableMemoryBackend().beginEpoch(NativeEpochKind.SNAPSHOT)) {
                Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());

                NativeAllocatorStats during = db.stableMemoryBackend().stats();
                Assert.assertTrue(during.logicalUsedBytes() > 0L);
                Assert.assertTrue(during.reservedBytes() > 0L);
                Assert.assertTrue(during.quarantinedObjects() > 0L);
                Assert.assertTrue(during.liveObjects() > 0L);

                YierdisMemoryStats memoryDuring = db.memory().memoryStats();
                Assert.assertTrue(memoryDuring.nativeDefragQuarantinedObjects() > 0L);
                Assert.assertTrue(memoryDuring.nativeDefragQuarantineBytes() > 0L);
            }

            NativeAllocatorStats after = db.stableMemoryBackend().stats();
            Assert.assertEquals(0L, after.logicalUsedBytes());
            Assert.assertEquals(0L, after.reservedBytes());
            Assert.assertEquals(0L, after.quarantinedObjects());
            Assert.assertEquals(0L, after.liveObjects());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void scanSourceRetainsDeletedKeyUntilWindowCloses() {
        YierdisDb db = openFfm();
        try {
            byte[] key = b("scan-keep");
            Assert.assertTrue(db.writes().strings().setString(key, b("value"), SetMode.NORMAL, null).value());

            List<byte[]> scanned = new ArrayList<>();
            try (KeyScanWindow window = db.reads().keyspace().scan(ScanCursorV2.start(), b("scan-*"), 2)) {
                Assert.assertEquals(0L, window.nextCursor().value());
                window.emitTo(new CapturingSink(scanned, () -> {
                    Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());
                    YierdisMemoryStats during = db.memory().memoryStats();
                    Assert.assertTrue(during.nativeDefragQuarantinedObjects() > 0L);
                    Assert.assertTrue(during.nativeDefragQuarantineBytes() > 0L);
                }));
            }

            Assert.assertEquals(1, scanned.size());
            Assert.assertArrayEquals(key, scanned.get(0));
            Assert.assertNull(db.reads().strings().getStringBytes(key));
            assertNoQuarantine(db);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void snapshotCopiesValuesBeforeTheSourceKeyIsDeleted() {
        YierdisDb db = openFfm();
        try {
            byte[] key = b("snapshot-keep");
            byte[] value = b("snapshot-value");
            Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null).value());

            List<YierdisSnapshotEntry> entries = new ArrayList<>() {
                private boolean deleted;

                @Override
                public boolean add(YierdisSnapshotEntry entry) {
                    boolean added = super.add(entry);
                    if (added && !deleted) {
                        deleted = true;
                        Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());
                        YierdisMemoryStats during = db.memory().memoryStats();
                        Assert.assertTrue(during.nativeDefragQuarantinedObjects() > 0L);
                        Assert.assertTrue(during.nativeDefragQuarantineBytes() > 0L);
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
            assertNoQuarantine(db);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void shutdownReleasesCollectionsAfterACompletedEpoch() {
        YierdisDb db = openFfm();
        try {
            Assert.assertTrue(db.writes().strings().setString(b("cleanup:string"), b("value"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("m"))).value());

            try (NativeEpochScope ignored = db.stableMemoryBackend().beginEpoch(NativeEpochKind.SNAPSHOT)) {
                Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("cleanup:string"))).value());
                Assert.assertTrue(db.memory().memoryStats().nativeDefragQuarantinedObjects() > 0L);
            }

            Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                    b("list"), b("hash"), b("set"), b("zset")
            )).value());
            NativeAllocatorStats stats = db.stableMemoryBackend().stats();
            Assert.assertEquals(0L, stats.objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(0L, stats.objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(0L, stats.liveObjects());
        } finally {
            db.shutdown();
        }
    }

    private static YierdisDb openFfm() {
        YierdisDb db = TestDbs.createFfmDb(
                new DbEngineConfig(
                        0,
                        0L,
                        MaxmemoryPolicy.NOEVICTION,
                        5,
                        5L,
                        5L,
                        new DbDefragConfig(false, 0L, 0L, 0L)
                ),
                0
        );
        Assert.assertTrue(db.stableMemoryBackend() instanceof YierdisFfmStableMemoryBackend);
        db.bindToCurrentThread();
        return db;
    }

    private static void assertNoQuarantine(YierdisDb db) {
        YierdisMemoryStats after = db.memory().memoryStats();
        Assert.assertEquals(0L, after.nativeDefragQuarantinedObjects());
        Assert.assertEquals(0L, after.nativeDefragQuarantineBytes());
    }

    private static final class CapturingSink implements ByteValueSink {
        private final List<byte[]> captured;
        private final Runnable firstValueAction;
        private boolean actionRun;

        private CapturingSink(List<byte[]> captured, Runnable firstValueAction) {
            this.captured = captured;
            this.firstValueAction = firstValueAction;
        }

        @Override
        public void value(byte[] data) {
            capture(data == null ? null : data.clone());
        }

        @Override
        public void value(byte[] data, int off, int len) {
            byte[] copy = new byte[len];
            System.arraycopy(data, off, copy, 0, len);
            capture(copy);
        }

        @Override
        public void value(BytesSlice slice) {
            byte[] copy = new byte[slice.length()];
            slice.getBytes(0, copy, 0, copy.length);
            capture(copy);
        }

        @Override
        public void longAscii(long value) {
            capture(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
        }

        @Override
        public void nullValue() {
            capture(null);
        }

        private void capture(byte[] value) {
            captured.add(value);
            if (!actionRun) {
                actionRun = true;
                firstValueAction.run();
            }
        }
    }
}
