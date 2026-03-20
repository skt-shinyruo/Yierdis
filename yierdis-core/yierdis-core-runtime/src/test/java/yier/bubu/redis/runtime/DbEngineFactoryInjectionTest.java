package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.EvictionCoordinator;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.ValueOps;

import java.util.concurrent.atomic.AtomicInteger;

public class DbEngineFactoryInjectionTest {
    @Test
    public void createUsesInjectedFactory() {
        AtomicInteger created = new AtomicInteger(0);
        RuntimeDbEngine[] createdEngines = new RuntimeDbEngine[2];

        DbEngineFactory factory = new DbEngineFactory() {
            @Override
            public RuntimeDbEngine create(
                    int dbIndex,
                    OffHeapAllocator offHeapAllocator,
                    boolean ownsOffHeapAllocator,
                    boolean offHeapKeysEnabled,
                    long maxmemoryBytes,
                    String maxmemoryPolicy,
                    int maxmemorySamples,
                    long evictionTimeLimitMillis,
                    long expireCleanupTimeLimitMillis
            ) {
                created.incrementAndGet();
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
        public ValueOps values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EvictionCoordinator eviction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyspaceOps keyspace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TtlOps ttl() {
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
