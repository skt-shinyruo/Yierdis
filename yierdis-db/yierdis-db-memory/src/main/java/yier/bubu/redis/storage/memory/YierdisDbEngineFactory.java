package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;

/**
 * 内存 DB 的唯一公开组合入口。
 */
public final class YierdisDbEngineFactory {
    private final StableMemoryBackendFactory backendFactory;
    private final YierdisDbBackendConfig backendConfig;
    private final HashSeed hashSeed;

    public YierdisDbEngineFactory(
            StableMemoryBackendFactory backendFactory,
            YierdisDbBackendConfig backendConfig
    ) {
        this.backendFactory = Objects.requireNonNull(backendFactory, "backendFactory");
        this.backendConfig = Objects.requireNonNull(backendConfig, "backendConfig");
        this.hashSeed = HashSeed.random();
    }

    public YierdisDb create(DbEngineConfig config) {
        Objects.requireNonNull(config, "config");
        DbThreadGuard owner = new DbThreadGuard();
        StableMemoryBackend backend = backendFactory.create(
                "db-" + config.dbIndex(),
                backendConfig.nativeSlotCapacity(),
                owner
        );
        if (backend == null) {
            throw new IllegalStateException("StableMemoryBackendFactory returned null");
        }
        return YierdisDb.create(config, backend, owner, hashSeed);
    }
}
