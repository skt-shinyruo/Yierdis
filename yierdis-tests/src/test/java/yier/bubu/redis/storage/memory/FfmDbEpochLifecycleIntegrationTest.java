package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.testutil.TestDbs;

import static yier.bubu.redis.testutil.TestBytes.b;

public class FfmDbEpochLifecycleIntegrationTest {
    @Test
    public void scanEpochDefersReleaseUntilClosed() {
        YierdisDb db = openFfm();
        try {
            byte[] key = b("epoch-key");
            Assert.assertTrue(db.writes().strings().setString(key, b("epoch-value"), SetMode.NORMAL, null).value());

            try (NativeEpochScope ignored = YierdisDbTestAccess.backend(db).beginEpoch()) {
                Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(key)).value());

                NativeAllocatorStats during = YierdisDbTestAccess.backend(db).stats();
                Assert.assertTrue(during.logicalUsedBytes() > 0L);
                Assert.assertTrue(during.reservedBytes() > 0L);
                Assert.assertTrue(during.quarantinedObjects() > 0L);
                Assert.assertTrue(during.liveObjects() > 0L);

                YierdisMemoryStats memoryDuring = db.memory().memoryStats();
                Assert.assertTrue(memoryDuring.nativeDefragQuarantinedObjects() > 0L);
                Assert.assertTrue(memoryDuring.nativeDefragQuarantineBytes() > 0L);
            }

            NativeAllocatorStats after = YierdisDbTestAccess.backend(db).stats();
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
    public void shutdownReleasesCollectionsAfterACompletedEpoch() {
        YierdisDb db = openFfm();
        try {
            Assert.assertTrue(db.writes().strings().setString(b("cleanup:string"), b("value"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("m"))).value());

            try (NativeEpochScope ignored = YierdisDbTestAccess.backend(db).beginEpoch()) {
                Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("cleanup:string"))).value());
                Assert.assertTrue(db.memory().memoryStats().nativeDefragQuarantinedObjects() > 0L);
            }

            Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                    b("list"), b("hash"), b("set"), b("zset")
            )).value());
            NativeAllocatorStats stats = YierdisDbTestAccess.backend(db).stats();
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
        Assert.assertTrue(YierdisDbTestAccess.backend(db) instanceof YierdisFfmStableMemoryBackend);
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
