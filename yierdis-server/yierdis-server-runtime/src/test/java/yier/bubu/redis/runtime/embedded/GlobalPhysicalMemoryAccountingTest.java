package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.util.concurrent.atomic.AtomicInteger;

public class GlobalPhysicalMemoryAccountingTest {
    @Test
    public void globalAdmissionSumsDbPhysicalSnapshotsExactlyOnce() {
        TrackingEngine first = new TrackingEngine(new MemoryUsageSnapshot(10L, 20L, 30L, 25L, 5L));
        TrackingEngine second = new TrackingEngine(new MemoryUsageSnapshot(1L, 2L, 3L, 2L, 1L));
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(engineConfig -> engineConfig.dbIndex() == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(100L)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance ignored = YierdisInstance.create(config)) {
            MaxmemoryCoordinator coordinator = first.attachedCoordinator;
            Assert.assertNotNull(coordinator);
            Assert.assertSame(coordinator, second.attachedCoordinator);

            coordinator.prepareWrite(null, 34L);
            try {
                coordinator.prepareWrite(null, 35L);
                Assert.fail("expected OOM when summed DB physical usage exceeds the admitted limit");
            } catch (RuntimeException e) {
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
            }
        }

        Assert.assertTrue(first.memoryUsageCalls.get() > 0);
        Assert.assertTrue(second.memoryUsageCalls.get() > 0);
        Assert.assertEquals("global admission must not read memory stats through MemoryOps", 0,
                first.memoryAccessCalls.get() + second.memoryAccessCalls.get());
    }

    @Test
    public void observabilitySumsDbPhysicalStatsAndNativeFieldsAcrossDatabases() {
        TrackingEngine first = new TrackingEngine(
                new MemoryUsageSnapshot(7L, 11L, 13L, 9L, 4L),
                stats(7L, 11L, 13L, 9L, 4L, 2, 1)
        );
        TrackingEngine second = new TrackingEngine(
                new MemoryUsageSnapshot(5L, 3L, 17L, 15L, 2L),
                stats(5L, 3L, 17L, 15L, 2L, 3, 2)
        );
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(engineConfig -> engineConfig.dbIndex() == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(123L)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            YierdisMemoryStats stats = instance.observability().memoryStats();

            Assert.assertEquals(123L, stats.maxmemoryBytes());
            Assert.assertEquals(12L, stats.heapDataBytesEstimate());
            Assert.assertEquals(44L, stats.offHeapUsedBytes());
            Assert.assertEquals(56L, stats.usedBytesForMaxmemory());
            Assert.assertEquals(56L, stats.effectiveUsedBytesForMaxmemory());
            Assert.assertEquals(56L, stats.totalEstimatedBytes());
            Assert.assertEquals(14L, stats.nativeMetadataCommittedBytes());
            Assert.assertEquals(30L, stats.nativeDataCommittedBytes());
            Assert.assertEquals(24L, stats.nativeDataLiveBytes());
            Assert.assertEquals(6L, stats.nativeReclaimableBytes());
            Assert.assertTrue(stats.offHeapIncludedInMaxmemory());
            Assert.assertEquals(5, stats.keyCount());
            Assert.assertEquals(3, stats.expireCount());
        }
    }

    @Test
    public void observabilityProjectsPhysicalFieldsFromSnapshotsWhenDbStatsAreStaleAndSaturates() {
        TrackingEngine engine = new TrackingEngine(
                new MemoryUsageSnapshot(Long.MAX_VALUE, 0L, 1L, 1L, 0L),
                stats(1L, 2L, 3L, 3L, 0L, 1, 0)
        );
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory(ignored -> engine)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(0L)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            YierdisMemoryStats stats = instance.observability().memoryStats();

            Assert.assertEquals(0L, stats.maxmemoryBytes());
            Assert.assertTrue(stats.offHeapIncludedInMaxmemory());
            Assert.assertEquals(Long.MAX_VALUE, stats.heapDataBytesEstimate());
            Assert.assertEquals(1L, stats.nativeDataCommittedBytes());
            Assert.assertEquals(1L, stats.offHeapUsedBytes());
            Assert.assertEquals(Long.MAX_VALUE, stats.totalEstimatedBytes());
            Assert.assertEquals(Long.MAX_VALUE, stats.usedBytesForMaxmemory());
            Assert.assertEquals(Long.MAX_VALUE, stats.effectiveUsedBytesForMaxmemory());
        }
    }

    @Test
    public void observabilityProjectsPhysicalUsageFromSemanticStatsForBaselineEngine() {
        YierdisMemoryStats dbStats = stats(7L, 11L, 13L, 9L, 4L, 2, 1);
        BaselineTrackingEngine engine = new BaselineTrackingEngine(dbStats);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .engineFactory(ignored -> engine)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            YierdisMemoryStats stats = instance.observability().memoryStats();

            Assert.assertEquals(7L, stats.heapDataBytesEstimate());
            Assert.assertEquals(11L, stats.nativeMetadataCommittedBytes());
            Assert.assertEquals(13L, stats.nativeDataCommittedBytes());
            Assert.assertEquals(9L, stats.nativeDataLiveBytes());
            Assert.assertEquals(4L, stats.nativeReclaimableBytes());
            Assert.assertEquals(31L, stats.usedBytesForMaxmemory());
            Assert.assertEquals(1, engine.memoryStatsCalls.get());
        }
    }

    private static YierdisMemoryStats stats(
            long heap,
            long nativeMetadataCommitted,
            long nativeDataCommitted,
            long nativeDataLive,
            long nativeReclaimable,
            int keyCount,
            int expireCount
    ) {
        long offHeap = MemoryUsageSnapshot.addSaturating(nativeMetadataCommitted, nativeDataCommitted);
        long physical = MemoryUsageSnapshot.addSaturating(heap, offHeap);
        return new YierdisMemoryStats(
                0L,
                physical,
                heap,
                offHeap,
                0L,
                physical,
                true,
                false,
                keyCount,
                expireCount,
                false,
                0,
                0,
                0L,
                false,
                0,
                0,
                0L,
                0L,
                physical,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                nativeMetadataCommitted,
                nativeDataCommitted,
                nativeDataLive,
                nativeReclaimable,
                0,
                "COMPLETE",
                0L,
                0L,
                0L
        );
    }

    private static final class TrackingEngine implements GlobalMaxmemoryDbEngine {
        private final MemoryUsageSnapshot usage;
        private final YierdisMemoryStats stats;
        private final AtomicInteger memoryUsageCalls = new AtomicInteger();
        private final AtomicInteger memoryAccessCalls = new AtomicInteger();
        private MaxmemoryCoordinator attachedCoordinator;

        private TrackingEngine(MemoryUsageSnapshot usage) {
            this(usage, null);
        }

        private TrackingEngine(MemoryUsageSnapshot usage, YierdisMemoryStats stats) {
            this.usage = usage;
            this.stats = stats;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void runMaintenance() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
            this.attachedCoordinator = coordinator;
        }

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            memoryUsageCalls.incrementAndGet();
            return usage;
        }

        @Override
        public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.empty();
        }

        @Override
        public int keyCountEstimate() {
            return 0;
        }

        @Override
        public void cleanupExpired(long nowMillis) {
        }

        @Override
        public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return null;
        }

        @Override
        public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return null;
        }

        @Override
        public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
            return false;
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            memoryAccessCalls.incrementAndGet();
            if (stats == null) {
                throw new AssertionError("global admission should use MemoryUsageSnapshot, not MemoryOps");
            }
            return new MemoryOps() {
                @Override
                public long memoryUsage(BytesView keyView) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public YierdisMemoryStats memoryStats() {
                    return stats;
                }

                @Override
                public String objectEncoding(BytesView keyView) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class BaselineTrackingEngine implements RuntimeDbEngine {
        private final YierdisMemoryStats stats;
        private final AtomicInteger memoryStatsCalls = new AtomicInteger();

        private BaselineTrackingEngine(YierdisMemoryStats stats) {
            this.stats = stats;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void runMaintenance() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            return new MemoryOps() {
                @Override
                public long memoryUsage(BytesView keyView) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public YierdisMemoryStats memoryStats() {
                    memoryStatsCalls.incrementAndGet();
                    return stats;
                }

                @Override
                public String objectEncoding(BytesView keyView) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }
}
