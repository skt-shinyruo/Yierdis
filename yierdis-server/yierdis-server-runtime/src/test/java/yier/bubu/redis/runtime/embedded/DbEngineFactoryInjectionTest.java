package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.ExpirationManager;
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
            public RuntimeDbEngine create(
                    int dbIndex,
                    long maxmemoryBytes,
                    MaxmemoryPolicy maxmemoryPolicy,
                    int maxmemorySamples,
                    long evictionTimeLimitMillis,
                    long expireCleanupTimeLimitMillis
            ) {
                created.incrementAndGet();
                receivedPolicy.set(maxmemoryPolicy);
                StubEngine engine = new StubEngine();
                if (dbIndex >= 0 && dbIndex < createdEngines.length) {
                    createdEngines[dbIndex] = engine;
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
        DbEngineFactory factory = (dbIndex,
                                   maxmemoryBytes,
                                   maxmemoryPolicy,
                                   maxmemorySamples,
                                   evictionTimeLimitMillis,
                                   expireCleanupTimeLimitMillis) ->
                new ShutdownTrackingEngine("db-" + dbIndex, closeOrder);
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
        DbEngineFactory factory = (dbIndex,
                                   maxmemoryBytes,
                                   maxmemoryPolicy,
                                   maxmemorySamples,
                                   evictionTimeLimitMillis,
                                   expireCleanupTimeLimitMillis) -> {
            if (dbIndex == 0) {
                return new ShutdownTrackingEngine("db-" + dbIndex, closeOrder);
            }
            throw new IllegalStateException("boom-create-" + dbIndex);
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
    public void globalMaxmemoryUsesCheapSharedOffHeapUsagePath() {
        Object sharedIdentity = new Object();
        SharedOffHeapTrackingEngine first = new SharedOffHeapTrackingEngine(sharedIdentity, 40L);
        SharedOffHeapTrackingEngine second = new SharedOffHeapTrackingEngine(sharedIdentity, 40L);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> dbIndex == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(100L)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance ignored = YierdisInstance.create(config)) {
            MaxmemoryCoordinator coordinator = first.attachedCoordinator;
            Assert.assertNotNull("global scope should attach a coordinator", coordinator);
            Assert.assertSame("all DBs should share the same coordinator", coordinator, second.attachedCoordinator);
            coordinator.prepareWrite(50L);
        }

        Assert.assertEquals("shared runtime bytes should be sampled once for the shared identity", 1, first.sharedOffHeapUsageCalls.get() + second.sharedOffHeapUsageCalls.get());
        Assert.assertEquals("global shared-off-heap hot path must not touch engine memory ops", 0, first.memoryAccessCalls.get() + second.memoryAccessCalls.get());
    }

    @Test
    public void globalObservabilityCountsSharedOffHeapOnceAcrossDatabases() {
        Object sharedIdentity = new Object();
        ObservabilityTrackingEngine first = new ObservabilityTrackingEngine(sharedIdentity, 40L, 10L, 1, 1);
        ObservabilityTrackingEngine second = new ObservabilityTrackingEngine(sharedIdentity, 40L, 20L, 2, 0);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> dbIndex == 0 ? first : second)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(100L)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            YierdisMemoryStats stats = instance.observability().memoryStats();

            Assert.assertEquals("instance/global off-heap should count the shared runtime once", 40L, stats.offHeapUsedBytes());
            Assert.assertEquals("global usedBytesForMaxmemory should be heap plus shared off-heap once", 70L, stats.usedBytesForMaxmemory());
            Assert.assertEquals("global effectiveUsedBytesForMaxmemory should include reserved bytes on top of shared off-heap once", 70L, stats.effectiveUsedBytesForMaxmemory());
            Assert.assertEquals(30L, stats.heapDataBytesEstimate());
            Assert.assertEquals(3, stats.keyCount());
            Assert.assertEquals(1, stats.expireCount());
            Assert.assertEquals("shared off-heap usage should be sampled once for observability too", 1, first.sharedOffHeapUsageCalls.get() + second.sharedOffHeapUsageCalls.get());
        }
    }

    private static final class StubEngine implements RuntimeDbEngine {
        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
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
        public void enforceMaxmemoryMaintenance() {
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
        public ExpirationManager expiration() {
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

    private static final class SharedOffHeapTrackingEngine implements RuntimeDbEngine {
        private final Object sharedIdentity;
        private final long sharedOffHeapUsedBytes;

        private MaxmemoryCoordinator attachedCoordinator;
        private final AtomicInteger sharedOffHeapUsageCalls = new AtomicInteger();
        private final AtomicInteger memoryAccessCalls = new AtomicInteger();

        private SharedOffHeapTrackingEngine(Object sharedIdentity, long sharedOffHeapUsedBytes) {
            this.sharedIdentity = sharedIdentity;
            this.sharedOffHeapUsedBytes = sharedOffHeapUsedBytes;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
            this.attachedCoordinator = coordinator;
        }

        @Override
        public long usedBytesForMaxmemory() {
            return 0L;
        }

        @Override
        public Object globalSharedOffHeapUsageIdentity() {
            return sharedIdentity;
        }

        @Override
        public long globalSharedOffHeapUsedBytes() {
            sharedOffHeapUsageCalls.incrementAndGet();
            return sharedOffHeapUsedBytes;
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

    private static final class ObservabilityTrackingEngine implements RuntimeDbEngine {
        private final Object sharedIdentity;
        private final long sharedOffHeapUsedBytes;
        private final YierdisMemoryStats stats;
        private final AtomicInteger sharedOffHeapUsageCalls = new AtomicInteger();

        private ObservabilityTrackingEngine(
                Object sharedIdentity,
                long sharedOffHeapUsedBytes,
                long heapBytes,
                int keyCount,
                int expireCount
        ) {
            this.sharedIdentity = sharedIdentity;
            this.sharedOffHeapUsedBytes = sharedOffHeapUsedBytes;
            this.stats = new YierdisMemoryStats(
                    100L,
                    heapBytes,
                    heapBytes,
                    sharedOffHeapUsedBytes,
                    0L,
                    heapBytes,
                    false,
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
                    heapBytes + sharedOffHeapUsedBytes,
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
                    0L
            );
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public Object globalSharedOffHeapUsageIdentity() {
            return sharedIdentity;
        }

        @Override
        public long globalSharedOffHeapUsedBytes() {
            sharedOffHeapUsageCalls.incrementAndGet();
            return sharedOffHeapUsedBytes;
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
