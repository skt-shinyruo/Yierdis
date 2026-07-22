package yier.bubu.redis.testutil;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class TestYierdisInstances {
    private TestYierdisInstances() {
    }

    public static YierdisInstance createWithDefaultMemory(YierdisInstanceConfig config) {
        Objects.requireNonNull(config, "config");
        if (config.engineFactory() != null) {
            return YierdisInstance.create(config);
        }

        NativeDefragOptions nativeDefragOptions = nativeDefragOptions(config);
        if (config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB) {
            return YierdisInstance.create(
                    copyConfig(config)
                            .engineFactory(new YierdisDbEngineFactory(nativeDefragOptions))
                            .build()
            );
        }

        YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
        YierdisDbEngineFactory engineFactory = new YierdisDbEngineFactory(memoryRuntime, nativeDefragOptions);
        return YierdisInstance.create(
                copyConfig(config)
                        .engineFactoryBinding(new YierdisInstanceConfig.EngineFactoryBinding(engineFactory, memoryRuntime))
                        .build()
        );
    }

    private static YierdisInstanceConfig.Builder copyConfig(YierdisInstanceConfig config) {
        return YierdisInstanceConfig.builder()
                .databases(config.databases())
                .changeSink(config.changeSink())
                .maxmemoryBytes(config.maxmemoryBytes())
                .maxmemoryScope(config.maxmemoryScope())
                .maxmemoryPolicy(config.maxmemoryPolicy())
                .maxmemorySamples(config.maxmemorySamples())
                .evictionTimeLimitMillis(config.evictionTimeLimitMillis())
                .expireCleanupTimeLimitMillis(config.expireCleanupTimeLimitMillis())
                .defrag(config.defrag())
                .commitStreamMaxEvents(config.commitStreamMaxEvents())
                .commitStreamMaxRetainedBytes(config.commitStreamMaxRetainedBytes())
                .commitStreamShutdownTimeoutMillis(config.commitStreamShutdownTimeoutMillis());
    }

    private static NativeDefragOptions nativeDefragOptions(YierdisInstanceConfig config) {
        DbDefragConfig defrag = config.defrag();
        if (!defrag.enabled()) {
            return null;
        }
        return new NativeDefragOptions(
                defrag.maxMoveBytes(),
                defrag.maxObjects(),
                TimeUnit.MILLISECONDS.toNanos(defrag.timeLimitMillis())
        );
    }
}
