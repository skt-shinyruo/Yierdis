package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DbEngineFactoryInjectionTest {
    @Test
    public void createRequiresInjectedFactory() {
        try {
            YierdisInstance.create(YierdisInstanceConfig.builder().build());
            Assert.fail("expected create(config) to reject missing engineFactory");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("engineFactory"));
        }
    }

    @Test
    public void createUsesInjectedFactory() {
        AtomicInteger created = new AtomicInteger(0);
        AtomicReference<MaxmemoryPolicy> receivedPolicy = new AtomicReference<>();
        RuntimeDbEngine[] createdEngines = new RuntimeDbEngine[2];

        DbEngineFactory factory = new DbEngineFactory() {
            @Override
            public RuntimeDbEngine create(DbEngineConfig config) {
                created.incrementAndGet();
                receivedPolicy.set(config.maxmemoryPolicy());
                StubEngine engine = new StubEngine();
                if (config.dbIndex() >= 0 && config.dbIndex() < createdEngines.length) {
                    createdEngines[config.dbIndex()] = engine;
                }
                return engine;
            }
        };

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(factory)
                .maxmemoryBytes(0)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            Assert.assertEquals(2, created.get());
            Assert.assertSame(createdEngines[0], instance.engine(0));
            Assert.assertSame(createdEngines[1], instance.engine(1));

            DbEngine[] engines = instance.engines();
            Assert.assertEquals(2, engines.length);
            Assert.assertSame(createdEngines[0], engines[0]);
            Assert.assertSame(createdEngines[1], engines[1]);
            Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, receivedPolicy.get());
        }
    }

    @Test
    public void engineFactoryBindingClosesOwnedResourceWhenInstanceCloses() {
        List<String> closeOrder = new ArrayList<>();
        DbEngineFactory factory = config ->
                new ShutdownTrackingEngine("db-" + config.dbIndex(), closeOrder);
        AutoCloseable ownedResource = () -> closeOrder.add("runtime");

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactoryBinding(new YierdisInstanceConfig.EngineFactoryBinding(factory, ownedResource))
                .build();

        try (YierdisInstance ignored = YierdisInstance.create(config)) {
            // close happens at scope exit
        }

        Assert.assertEquals(Arrays.asList("db-0", "runtime"), closeOrder);
    }

    @Test
    public void engineFactoryBindingClosesOwnedResourceWhenStartupFails() {
        List<String> closeOrder = new ArrayList<>();
        DbEngineFactory factory = config -> {
            if (config.dbIndex() == 0) {
                return new ShutdownTrackingEngine("db-" + config.dbIndex(), closeOrder);
            }
            throw new IllegalStateException("boom-create-" + config.dbIndex());
        };
        AutoCloseable ownedResource = () -> closeOrder.add("runtime");

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactoryBinding(new YierdisInstanceConfig.EngineFactoryBinding(factory, ownedResource))
                .build();

        try {
            YierdisInstance.create(config);
            Assert.fail("expected startup failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("boom-create-1", e.getMessage());
        }

        Assert.assertEquals(Arrays.asList("db-0", "runtime"), closeOrder);
    }

    @Test
    public void globalMaxmemoryUsesDbPhysicalSnapshotsForAdmission() {
        PhysicalTrackingEngine first = new PhysicalTrackingEngine(new MemoryUsageSnapshot(10L, 0L, 0L, 0L, 0L));
        PhysicalTrackingEngine second = new PhysicalTrackingEngine(new MemoryUsageSnapshot(10L, 0L, 0L, 0L, 0L));
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(engineConfig -> engineConfig.dbIndex() == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(100L)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance ignored = YierdisInstance.create(config)) {
            MaxmemoryCoordinator coordinator = first.attachedCoordinator;
            Assert.assertNotNull("global scope should attach a coordinator", coordinator);
            Assert.assertSame("all DBs should share the same coordinator", coordinator, second.attachedCoordinator);
            coordinator.prepareWrite(null, 80L);
            try {
                coordinator.prepareWrite(null, 81L);
                Assert.fail("expected physical snapshots to be enforced");
            } catch (RuntimeException expected) {
                // expected OOM
            }
        }

        Assert.assertTrue("global governor should sample participant physical usage", first.memoryUsageCalls.get() > 0);
        Assert.assertTrue("global governor should sample participant physical usage", second.memoryUsageCalls.get() > 0);
        Assert.assertEquals("maxmemory admission must not touch engine memory ops", 0, first.memoryAccessCalls.get() + second.memoryAccessCalls.get());
    }

    @Test
    public void globalObservabilitySumsDbPhysicalStatsAcrossDatabases() {
        ObservabilityTrackingEngine first = new ObservabilityTrackingEngine(40L, 10L, 1, 1);
        ObservabilityTrackingEngine second = new ObservabilityTrackingEngine(40L, 20L, 2, 0);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(engineConfig -> engineConfig.dbIndex() == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(100L)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            YierdisMemoryStats stats = instance.observability().memoryStats();

            Assert.assertEquals("instance/global off-heap should sum each DB's physical off-heap", 80L, stats.offHeapUsedBytes());
            Assert.assertEquals("global usedBytesForMaxmemory should equal summed physical usage", 110L, stats.usedBytesForMaxmemory());
            Assert.assertEquals("global effectiveUsedBytesForMaxmemory should match summed physical usage without reservations", 110L, stats.effectiveUsedBytesForMaxmemory());
            Assert.assertEquals(30L, stats.heapDataBytesEstimate());
            Assert.assertEquals(3, stats.keyCount());
            Assert.assertEquals(1, stats.expireCount());
        }
    }

    private static final class StubEngine implements RuntimeDbEngine {
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
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ShutdownTrackingEngine implements RuntimeDbEngine {
        private final String name;
        private final List<String> closeOrder;

        private ShutdownTrackingEngine(String name, List<String> closeOrder) {
            this.name = name;
            this.closeOrder = closeOrder;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void runMaintenance() {
        }

        @Override
        public void shutdown() {
            closeOrder.add(name);
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
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class PhysicalTrackingEngine implements GlobalMaxmemoryDbEngine {
        private final MemoryUsageSnapshot usage;
        private MaxmemoryCoordinator attachedCoordinator;
        private final AtomicInteger memoryUsageCalls = new AtomicInteger();
        private final AtomicInteger memoryAccessCalls = new AtomicInteger();

        private PhysicalTrackingEngine(MemoryUsageSnapshot usage) {
            this.usage = usage;
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
        public MemoryOps memory() {
            memoryAccessCalls.incrementAndGet();
            return new TrackingMemoryOps();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TrackingMemoryOps implements MemoryOps {
        @Override
        public long memoryUsage(yier.bubu.redis.bytes.BytesView keyView) {
            throw new UnsupportedOperationException();
        }

        @Override
        public YierdisMemoryStats memoryStats() {
            throw new AssertionError("memoryStats() should not be used for shared off-heap maxmemory accounting");
        }

        @Override
        public String objectEncoding(yier.bubu.redis.bytes.BytesView keyView) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ObservabilityTrackingEngine implements GlobalMaxmemoryDbEngine {
        private final YierdisMemoryStats stats;
        private final MemoryUsageSnapshot usage;

        private ObservabilityTrackingEngine(
                long offHeapBytes,
                long heapBytes,
                int keyCount,
                int expireCount
        ) {
            long physical = heapBytes + offHeapBytes;
            this.usage = new MemoryUsageSnapshot(heapBytes, 0L, offHeapBytes, offHeapBytes, 0L);
            this.stats = new YierdisMemoryStats(
                    100L,
                    physical,
                    heapBytes,
                    offHeapBytes,
                    0L,
                    physical,
                    true,
                    true,
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
                    0L,
                    0L,
                    0L,
                    offHeapBytes,
                    0,
                    "COMPLETE",
                    0L,
                    0L,
                    0L
            );
        }

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            return usage;
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
        public MemoryOps memory() {
            return new MemoryOps() {
                @Override
                public long memoryUsage(yier.bubu.redis.bytes.BytesView keyView) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public YierdisMemoryStats memoryStats() {
                    return stats;
                }

                @Override
                public String objectEncoding(yier.bubu.redis.bytes.BytesView keyView) {
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
