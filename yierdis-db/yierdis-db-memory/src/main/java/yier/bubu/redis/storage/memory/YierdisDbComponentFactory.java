package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

public final class YierdisDbComponentFactory {
    private YierdisDbComponentFactory() {
    }

    static YierdisDbComponents create(
            OwnerCallbacks owner,
            YierdisDbRuntimeState runtimeState,
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions
    ) {
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(
                memoryRuntime,
                offHeapAllocator,
                ownsOffHeapAllocator,
                ownsMemoryRuntime
        );
        YierdisDbConfig config = YierdisDbConfig.create(
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions
        );
        YierdisDbMemoryEstimator memoryEstimator = new YierdisDbMemoryEstimator(
                storage.keysStoredOffHeap,
                storage.offHeapAllocator
        );
        YierdisDbMemoryBudgetCallbacks memoryBudgetCallbacks = new YierdisDbMemoryBudgetCallbacks();
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                config.maxmemoryBytes,
                config.maxmemoryPolicy,
                memoryBudgetCallbacks::cleanupExpired,
                memoryBudgetCallbacks::evictUntilUnder,
                memoryBudgetCallbacks::usedBytesForMaxmemory,
                runtimeState::maxmemoryCoordinator
        );
        YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(owner::checkThread, ledger);
        YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
                storage.expires,
                storage.offHeapAllocator,
                storage.nativeAllocator,
                storage.memoryRuntime,
                storage.entries,
                storage.keyDirectory,
                storage.stringRoot,
                storage.listRoot,
                storage.hashRoot,
                storage.setRoot,
                storage.zsetRoot,
                runtimeState::nextLruClock,
                runtimeState::adjustUsedBytes,
                owner.dbIndex()
        );
        YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                owner::checkThread,
                keyLifecycle,
                config.expireCleanupTimeLimitNanos
        );
        YierdisDbInternals internals = new YierdisDbRuntimeInternals(
                owner::checkThread,
                mutationExecutor,
                keyLifecycle,
                ledger
        );
        YierdisStringOps stringOps = new YierdisStringOps(internals);
        YierdisHashOps hashOps = new YierdisHashOps(internals);
        YierdisListOps listOps = new YierdisListOps(internals);
        YierdisSetOps setOps = new YierdisSetOps(internals);
        YierdisZSetOps zsetOps = new YierdisZSetOps(internals);
        YierdisHllOps hllOps = new YierdisHllOps(internals);
        YierdisTtlOps ttlOps = new YierdisTtlOps(internals);
        YierdisKeyspaceOps keyspaceOps = new YierdisKeyspaceOps(internals);
        YierdisDbMemoryReporter memoryReporter = new YierdisDbMemoryReporter(
                owner::checkThread,
                keyLifecycle,
                storage.expires,
                config.maxmemoryBytes,
                storage.keysStoredOffHeap,
                ledger,
                runtimeState::hasNoMaxmemoryCoordinator,
                memoryEstimator,
                runtimeState::lastNativeDefragReport
        );
        YierdisDbIntrospection introspection = new YierdisDbIntrospection(owner::checkThread, keyLifecycle);
        YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                owner::checkThread,
                keyLifecycle,
                memoryReporter::usedBytesForMaxmemory,
                expirationSupport::cleanupExpired,
                runtimeState::adjustUsedBytes,
                config.maxmemoryPolicy,
                config.maxmemorySamples,
                config.evictionTimeLimitNanos
        );
        memoryBudgetCallbacks.bind(
                () -> expirationSupport.cleanupExpired(0L),
                maxmemorySupport::evictUntilUnder,
                memoryReporter::usedBytesForMaxmemory
        );
        runtimeState.bind(config, storage, ledger, keyLifecycle);
        YierdisDbDataMaintenance maintenance = new YierdisDbDataMaintenance(
                runtimeState,
                expirationSupport,
                maxmemorySupport,
                memoryReporter
        );

        return new YierdisDbComponents(
                runtimeState,
                storage,
                storage.entries,
                storage.keyDirectory,
                config,
                ledger,
                mutationExecutor,
                expirationSupport,
                maxmemorySupport,
                keyLifecycle,
                internals,
                stringOps,
                hashOps,
                listOps,
                setOps,
                zsetOps,
                hllOps,
                ttlOps,
                keyspaceOps,
                memoryReporter,
                introspection,
                new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps),
                new YierdisDbWrites(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps),
                new YierdisDbExpirationManager(expirationSupport),
                new YierdisDbMemoryOps(memoryReporter, introspection),
                new YierdisDbLifecycleOps(maintenance::flushDb),
                maintenance
        );
    }

    interface OwnerCallbacks {
        int dbIndex();

        void checkThread();
    }
}
