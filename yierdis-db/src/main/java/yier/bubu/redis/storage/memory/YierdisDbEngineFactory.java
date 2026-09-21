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
    private final int nativeSlotCapacity;
    private final HashSeed hashSeed;

    public YierdisDbEngineFactory(
            StableMemoryBackendFactory backendFactory,
            int nativeSlotCapacity
    ) {
        this.backendFactory = Objects.requireNonNull(backendFactory, "backendFactory");
        if (nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("nativeSlotCapacity must be non-negative");
        }
        this.nativeSlotCapacity = nativeSlotCapacity;
        this.hashSeed = HashSeed.random();
    }

    public YierdisDb create(DbEngineConfig config) {
        Objects.requireNonNull(config, "config");
        DbThreadGuard owner = new DbThreadGuard();
        StableMemoryBackend backend = backendFactory.create(
                "db-" + config.dbIndex(),
                nativeSlotCapacity,
                owner
        );
        if (backend == null) {
            throw new IllegalStateException("StableMemoryBackendFactory returned null");
        }
        return YierdisDb.create(config, backend, owner, hashSeed);
    }
}
