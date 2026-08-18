package yier.bubu.redis.testutil;

import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

import java.util.Objects;
import java.util.function.Consumer;

public final class TestDbs {
    private TestDbs() {
    }

    public static void forEachDb(Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        runDefaultFfm(test);
    }

    public static void runDefaultFfm(Consumer<YierdisDb> test) {
        Objects.requireNonNull(test, "test");
        YierdisDb db = createFfmDb(defaultConfig(), 0);
        try {
            db.bindToCurrentThread();
            test.accept(db);
        } finally {
            db.shutdown();
        }
    }

    public static void forEachDbWithMaxmemory(long maxmemoryBytes, MaxmemoryPolicy maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test) {
        forEachDbWithMaxmemory(maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, 5, test);
    }

    public static void forEachDbWithMaxmemory(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            Consumer<YierdisDb> test
    ) {
        Objects.requireNonNull(test, "test");
        YierdisDb db = createFfmDb(
                new DbEngineConfig(
                        0,
                        maxmemoryBytes,
                        maxmemoryPolicy,
                        maxmemorySamples,
                        evictionTimeLimitMillis,
                        5L,
                        new DbDefragConfig(false, 0L, 0L, 0L)
                ),
                0
        );
        try {
            db.bindToCurrentThread();
            test.accept(db);
        } finally {
            db.shutdown();
        }
    }

    public static YierdisDb createFfmDb(DbEngineConfig config, int nativeSlotCapacity) {
        StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new;
        RuntimeDbEngine engine = new YierdisDbEngineFactory(
                backendFactory,
                nativeSlotCapacity
        ).create(Objects.requireNonNull(config, "config"));
        if (engine instanceof YierdisDb db) {
            return db;
        }
        engine.shutdown();
        throw new IllegalStateException("YierdisDbEngineFactory did not create YierdisDb");
    }

    private static DbEngineConfig defaultConfig() {
        return new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        );
    }
}
