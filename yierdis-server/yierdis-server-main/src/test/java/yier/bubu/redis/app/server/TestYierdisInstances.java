package yier.bubu.redis.app.server;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

import java.util.Objects;

final class TestYierdisInstances {
    private TestYierdisInstances() {
    }

    static YierdisInstance createWithDefaultMemory(YierdisInstanceConfig config) {
        Objects.requireNonNull(config, "config");
        if (config.engineFactory() != null) {
            return YierdisInstance.create(config);
        }
        if (config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB) {
            return YierdisInstance.create(copyConfig(config)
                    .engineFactory(new YierdisDbEngineFactory())
                    .build());
        }

        YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
        YierdisDbEngineFactory engineFactory = new YierdisDbEngineFactory(memoryRuntime);
        return YierdisInstance.create(
                copyConfig(config)
                        .engineFactoryBinding(new YierdisInstanceConfig.EngineFactoryBinding(engineFactory, memoryRuntime))
                        .build()
        );
    }

    private static YierdisInstanceConfig.Builder copyConfig(YierdisInstanceConfig config) {
        return YierdisInstanceConfig.builder()
                .databases(config.databases())
                .maxmemoryBytes(config.maxmemoryBytes())
                .maxmemoryScope(config.maxmemoryScope())
                .maxmemoryPolicy(config.maxmemoryPolicy())
                .maxmemorySamples(config.maxmemorySamples())
                .evictionTimeLimitMillis(config.evictionTimeLimitMillis())
                .expireCleanupTimeLimitMillis(config.expireCleanupTimeLimitMillis())
                .defrag(config.defrag());
    }
}
