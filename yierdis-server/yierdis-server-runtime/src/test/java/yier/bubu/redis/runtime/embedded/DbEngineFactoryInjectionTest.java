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
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
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
}
