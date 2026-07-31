package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

import java.nio.charset.StandardCharsets;

public class YierdisDbMemoryReporterTest {
    @Test
    public void directMemoryUsageReadsNativeEntryAndValueMetadata() {
        try (TestBackend runtime = TestBackend.open("memory-reporter")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                byte[] key = bytes("report-key");
                db.writes().strings().setString(key, bytes("value-with-native-storage"), SetMode.NORMAL, null);

                long usageFromBytes = db.memory().memoryUsage(view(key));
                long missing = db.memory().memoryUsage(view(bytes("missing")));

                Assert.assertTrue("memory usage should include native string bytes", usageFromBytes > key.length);
                Assert.assertEquals(-1L, missing);
                Assert.assertEquals(1, db.keyCountEstimate());
                Assert.assertTrue(db.estimatedUsedBytes() >= 0L);
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void memoryStatsIncludesFfmNativeBytesWhenEnabledForMaxmemory() {
        try (TestBackend runtime = TestBackend.open("memory-stats")) {
            YierdisDb db = TestDbSupport.open(runtime, 1_000_000L, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                byte[] key = bytes("ttl-key");
                db.writes().strings().setString(key, bytes("ttl-value"), SetMode.NORMAL, null);
                db.writes().ttl().pexpire(view(key), 60_000L);

                YierdisMemoryStats stats = db.memory().memoryStats();

                Assert.assertEquals(1, stats.keyCount());
                Assert.assertTrue("used bytes for maxmemory should include native accounting", stats.usedBytesForMaxmemory() > 0L);
                Assert.assertTrue("used bytes for maxmemory should include native accounting", db.usedBytesForMaxmemory() > 0L);
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void memoryStatsReportsPhysicalCommittedBytesAndDisablesSharedRuntimeSampling() {
        try (TestBackend runtime = TestBackend.open("memory-stats-shared-native")) {
            YierdisDb db = TestDbSupport.open(runtime, 1_000_000L, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                db.writes().strings().setString(bytes("k"), bytes("value"), SetMode.NORMAL, null);

                MemoryUsageSnapshot usage = db.memoryUsage();
                YierdisMemoryStats stats = db.memory().memoryStats();

                Assert.assertEquals(usage.nativeMetadataCommittedBytes(), stats.nativeMetadataCommittedBytes());
                Assert.assertEquals(usage.nativeDataCommittedBytes(), stats.nativeDataCommittedBytes());
                Assert.assertEquals(usage.nativeDataLiveBytes(), stats.nativeDataLiveBytes());
                Assert.assertEquals(usage.nativeReclaimableBytes(), stats.nativeReclaimableBytes());
                Assert.assertEquals(
                        MemoryUsageSnapshot.addSaturating(
                                usage.nativeMetadataCommittedBytes(),
                                usage.nativeDataCommittedBytes()
                        ),
                        stats.offHeapUsedBytes()
                );
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void deferredExpirationGaugeIsDeduplicatedAndConvergesAfterReplaceDeleteAndFlush() {
        try (TestBackend runtime = TestBackend.open("deferred-expiration-gauge")) {
            YierdisDb db = TestDbSupport.open(runtime, 0L, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();

                MarkedEntry replaced = markExpiredEntry(db, lifecycle, "replace");
                Assert.assertFalse(lifecycle.markExpiredEntryAwaitingPhysicalDeletion(
                        replaced.keyHandle(),
                        replaced.entryHandle(),
                        lifecycle.entryRecord(replaced.entryHandle()),
                        System.currentTimeMillis()
                ));
                Assert.assertEquals(1L, db.memory().memoryStats().expiredEntriesAwaitingPhysicalDeletion());

                EntryRecord current = lifecycle.entryRecord(replaced.entryHandle());
                lifecycle.replaceEntry(replaced.entryHandle(), current, withoutFlags(current));
                Assert.assertEquals(0L, db.memory().memoryStats().expiredEntriesAwaitingPhysicalDeletion());

                MarkedEntry deleted = markExpiredEntry(db, lifecycle, "delete");
                Assert.assertTrue(lifecycle.removeEntry(deleted.keyHandle(), deleted.record()));
                Assert.assertEquals(0L, db.memory().memoryStats().expiredEntriesAwaitingPhysicalDeletion());

                markExpiredEntry(db, lifecycle, "flush");
                Assert.assertEquals(1L, db.memory().memoryStats().expiredEntriesAwaitingPhysicalDeletion());
                db.flushDb();
                Assert.assertEquals(0L, db.memory().memoryStats().expiredEntriesAwaitingPhysicalDeletion());
            } finally {
                db.shutdown();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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

    private static MarkedEntry markExpiredEntry(YierdisDb db, YierdisDbKeyLifecycle lifecycle, String keyText) {
        byte[] key = bytes(keyText);
        db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, ExpireOption.px(0));
        KeyHandle keyHandle = lifecycle.keyHandle(key);
        EntryHandle entryHandle = lifecycle.entryHandle(key);
        EntryRecord record = lifecycle.entryRecord(entryHandle);
        Assert.assertTrue(lifecycle.markExpiredEntryAwaitingPhysicalDeletion(
                keyHandle,
                entryHandle,
                record,
                System.currentTimeMillis()
        ));
        return new MarkedEntry(keyHandle, entryHandle, lifecycle.entryRecord(entryHandle));
    }

    private static EntryRecord withoutFlags(EntryRecord record) {
        return new EntryRecord(
                record.keyHandle(),
                record.valueHandle(),
                record.keyHash(),
                record.type(),
                record.encoding(),
                0,
                record.expireAtMillis(),
                record.version(),
                record.lruOrLfu()
        );
    }

    private record MarkedEntry(KeyHandle keyHandle, EntryHandle entryHandle, EntryRecord record) {
    }

    private enum TestMaxmemoryCoordinator implements MaxmemoryCoordinator {
        INSTANCE;

        @Override
        public void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes) {
        }

        @Override
        public long nextLruClock() {
            return 0L;
        }
    }
}
