package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.nio.charset.StandardCharsets;

public class YierdisDbMemoryReporterTest {
    @Test
    public void directMemoryUsageReadsNativeEntryAndValueMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("memory-reporter")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void memoryStatsIncludesFfmNativeBytesWhenEnabledForMaxmemory() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("memory-stats")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 1_000_000L, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void memoryStatsReportsPhysicalCommittedBytesAndDisablesSharedRuntimeSampling() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("memory-stats-shared-native")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 1_000_000L, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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
            Assert.assertEquals(0L, runtime.usedBytes());
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
