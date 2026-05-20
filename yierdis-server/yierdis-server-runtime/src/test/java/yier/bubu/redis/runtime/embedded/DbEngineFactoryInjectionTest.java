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
}
