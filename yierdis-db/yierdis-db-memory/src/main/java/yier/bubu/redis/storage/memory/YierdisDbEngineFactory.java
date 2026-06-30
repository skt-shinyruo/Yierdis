package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

import java.util.Objects;

/**
 * Default {@link DbEngineFactory} that creates {@link YierdisDb} engines.
 */
public final class YierdisDbEngineFactory implements DbEngineFactory {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final NativeDefragOptions nativeDefragOptions;
    private final int nativeSlotCapacity;

    public YierdisDbEngineFactory() {
        this.memoryRuntime = null;
        this.nativeDefragOptions = null;
        this.nativeSlotCapacity = 0;
    }

    public YierdisDbEngineFactory(NativeDefragOptions nativeDefragOptions) {
        this.memoryRuntime = null;
        this.nativeDefragOptions = nativeDefragOptions;
        this.nativeSlotCapacity = 0;
    }

    public YierdisDbEngineFactory(NativeDefragOptions nativeDefragOptions, int nativeSlotCapacity) {
        this.memoryRuntime = null;
        this.nativeDefragOptions = nativeDefragOptions;
        this.nativeSlotCapacity = nativeSlotCapacity;
    }

    public YierdisDbEngineFactory(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.nativeDefragOptions = null;
        this.nativeSlotCapacity = 0;
    }

    public YierdisDbEngineFactory(YierdisFfmMemoryRuntime memoryRuntime, NativeDefragOptions nativeDefragOptions) {
        this(memoryRuntime, nativeDefragOptions, 0);
    }

    public YierdisDbEngineFactory(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeDefragOptions nativeDefragOptions,
            int nativeSlotCapacity
    ) {
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.nativeDefragOptions = nativeDefragOptions;
        this.nativeSlotCapacity = nativeSlotCapacity;
    }

    @Override
    public RuntimeDbEngine create(
            int dbIndex,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        MaxmemoryPolicy policy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        if (memoryRuntime == null) {
            if (nativeSlotCapacity > 0) {
                return YierdisDb.createWithOwnedFfmRuntime(
                        maxmemoryBytes,
                        policy,
                        maxmemorySamples,
                        evictionTimeLimitMillis,
                        expireCleanupTimeLimitMillis,
                        nativeDefragOptions,
                        nativeSlotCapacity,
                        dbIndex
                );
            }
            return YierdisDb.createWithOwnedFfmRuntime(
                    maxmemoryBytes,
                    policy,
                    maxmemorySamples,
                    evictionTimeLimitMillis,
                    expireCleanupTimeLimitMillis,
                    nativeDefragOptions,
                    dbIndex
            );
        }
        if (nativeSlotCapacity <= 0) {
            return YierdisDb.createWithSharedFfmRuntime(
                    memoryRuntime,
                    maxmemoryBytes,
                    policy,
                    maxmemorySamples,
                    evictionTimeLimitMillis,
                    expireCleanupTimeLimitMillis,
                    nativeDefragOptions,
                    dbIndex
                );
        }
        return YierdisDb.createWithSharedFfmRuntime(
                memoryRuntime,
                maxmemoryBytes,
                policy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                nativeSlotCapacity,
                dbIndex
        );
    }
}
