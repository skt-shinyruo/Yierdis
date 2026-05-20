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
            return YierdisInstance.create(configWithFactory(config, new YierdisDbEngineFactory()).build());
        }

        YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
        return YierdisInstance.create(
                configWithFactory(config, new YierdisDbEngineFactory(memoryRuntime))
                        .engineFactoryOwnedResource(memoryRuntime)
                        .build()
        );
    }

    private static YierdisInstanceConfig.Builder configWithFactory(
            YierdisInstanceConfig config,
            YierdisDbEngineFactory engineFactory
    ) {
        return YierdisInstanceConfig.builder()
                .databases(config.databases())
                .engineFactory(engineFactory)
                .changeSink(config.changeSink())
                .maxmemoryBytes(config.maxmemoryBytes())
                .maxmemoryScope(config.maxmemoryScope())
                .maxmemoryPolicy(config.maxmemoryPolicy())
                .maxmemorySamples(config.maxmemorySamples())
                .evictionTimeLimitMillis(config.evictionTimeLimitMillis())
                .expireCleanupTimeLimitMillis(config.expireCleanupTimeLimitMillis())
                .nativeDefragEnabled(config.nativeDefragEnabled())
                .nativeDefragMaxMoveBytes(config.nativeDefragMaxMoveBytes())
                .nativeDefragMaxObjects(config.nativeDefragMaxObjects())
                .nativeDefragTimeLimitMillis(config.nativeDefragTimeLimitMillis());
    }
}
