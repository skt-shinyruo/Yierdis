package yier.bubu.redis.app.server;

import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.memory.YierdisDbBackendConfig;
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
        return YierdisInstance.create(copyConfig(config)
                .engineFactory(defaultEngineFactory())
                .build());
    }

    private static YierdisDbEngineFactory defaultEngineFactory() {
        StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new;
        return new YierdisDbEngineFactory(backendFactory, new YierdisDbBackendConfig(0));
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
