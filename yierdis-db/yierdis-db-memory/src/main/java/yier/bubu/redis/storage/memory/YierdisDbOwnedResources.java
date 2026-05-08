package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.api.OffHeapAllocator;

public final class YierdisDbOwnedResources implements AutoCloseable {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final OffHeapAllocator offHeapAllocator;
    private final boolean ownsMemoryRuntime;
    private final boolean ownsOffHeapAllocator;

    YierdisDbOwnedResources(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsMemoryRuntime,
            boolean ownsOffHeapAllocator
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.ownsMemoryRuntime = ownsMemoryRuntime;
        this.ownsOffHeapAllocator = ownsOffHeapAllocator;
    }

    void clearData(YierdisKeyspace<YierdisObject> store, YierdisExpireIndex expires) {
        if (store != null) {
            store.forEach((k, e) -> e.releasePayloadIfAny());
            store.clear();
        }
        if (expires != null) {
            expires.clear();
        }
    }

    void releaseAll(YierdisKeyspace<YierdisObject> store, YierdisExpireIndex expires) {
        clearData(store, expires);
        close();
    }

    @Override
    public void close() {
        Throwable failure = null;
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            try {
                offHeapAllocator.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (ownsMemoryRuntime && memoryRuntime != null) {
            try {
                memoryRuntime.close();
            } catch (Throwable t) {
                failure = recordFailure(failure, t);
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("db resource shutdown failed", failure);
        }
    }

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }
}
