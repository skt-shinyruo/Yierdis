package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

import java.util.concurrent.atomic.AtomicReference;

public class YierdisInstanceObservabilityTest {
    @Test
    public void healthSnapshotAggregatesOwnerThreadDbSnapshots() {
        DbHealthSnapshot failure = DbHealthSnapshot.degraded(
                new IllegalStateException("broken-db"),
                123L
        );
        try (YierdisInstance instance = createInstance(
                new OwnerCheckedEngine(DbHealthSnapshot.healthy()),
                new OwnerCheckedEngine(failure)
        )) {
            instance.bindToCurrentThread();

            YierdisInstanceObservability.RuntimeHealthSnapshot snapshot = instance.observability().healthSnapshot();

            Assert.assertEquals(2, snapshot.databaseCount());
            Assert.assertEquals(1, snapshot.degradedDatabaseCount());
            Assert.assertEquals(IllegalStateException.class.getName(), snapshot.firstFailureType());
            Assert.assertEquals("broken-db", snapshot.firstFailureMessage());
            Assert.assertEquals(123L, snapshot.firstFailureAtMillis());
        }
    }

    @Test
    public void healthSnapshotDoesNotTranslateNonOwnerAccessIntoDbDegradation() throws Exception {
        try (YierdisInstance instance = createInstance(
                new OwnerCheckedEngine(DbHealthSnapshot.healthy()),
                new OwnerCheckedEngine(DbHealthSnapshot.healthy())
        )) {
            instance.bindToCurrentThread();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread foreign = new Thread(() -> {
                try {
                    instance.observability().healthSnapshot();
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "observability-non-owner");
            foreign.start();
            foreign.join();

            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            Assert.assertEquals("health accessed from non-owner thread", failure.get().getMessage());
        }
    }

    private static YierdisInstance createInstance(OwnerCheckedEngine... engines) {
        return YierdisInstance.create(
                YierdisInstanceConfig.builder()
                        .databases(engines.length)
                        .maxmemoryBytes(0L)
                        .engineFactory(config -> engines[config.dbIndex()])
                        .build()
        );
    }

    private static final class OwnerCheckedEngine implements RuntimeDbEngine {
        private final DbHealthSnapshot health;
        private Thread owner;

        private OwnerCheckedEngine(DbHealthSnapshot health) {
            this.health = health;
        }

        @Override
        public void bindToCurrentThread() {
            owner = Thread.currentThread();
        }

        @Override
        public DbHealthSnapshot health() {
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("health accessed from non-owner thread");
            }
            return health;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }
}
