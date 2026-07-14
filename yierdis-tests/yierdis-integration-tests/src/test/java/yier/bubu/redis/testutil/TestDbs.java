package yier.bubu.redis.testutil;

import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

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
        YierdisDb db = new YierdisDb();
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    maxmemoryBytes,
                    maxmemoryPolicy,
                    maxmemorySamples,
                    evictionTimeLimitMillis,
                    5
            );
            try {
                db.bindToCurrentThread();
                test.accept(db);
            } finally {
                db.shutdown();
            }
        }
    }
}
