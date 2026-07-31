package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.memory.internal.expire.YierdisDbExpirationManager;
import yier.bubu.redis.storage.memory.internal.expire.YierdisDbExpirationSupport;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

final class YierdisDbComponentFactory {
    private YierdisDbComponentFactory() {
    }

    static YierdisDbComponents create(
            OwnerCallbacks owner,
            YierdisDbRuntimeState runtimeState,
            StableMemoryBackend stableMemoryBackend,
            DbEngineConfig engineConfig,
            HashSeed hashSeed
    ) {
        OwnerCallbacks checkedOwner = Objects.requireNonNull(owner, "owner");
        YierdisDbRuntimeState checkedRuntimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        DbEngineConfig checkedEngineConfig = Objects.requireNonNull(engineConfig, "engineConfig");
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(
                Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend"),
                Objects.requireNonNull(hashSeed, "hashSeed")
        );
        YierdisDbConfig config = YierdisDbConfig.create(
                checkedEngineConfig.maxmemoryBytes(),
                checkedEngineConfig.maxmemoryPolicy(),
                checkedEngineConfig.maxmemorySamples(),
                checkedEngineConfig.evictionTimeLimitMillis(),
                checkedEngineConfig.expireCleanupTimeLimitMillis(),
                nativeDefragOptions(checkedEngineConfig.defrag())
        );
        YierdisDbMemoryEstimator memoryEstimator = new YierdisDbMemoryEstimator();
        YierdisDbHealth health = new YierdisDbHealth(checkedOwner::checkThread);
        // ledger 需要在写入前触发过期清理和淘汰，但这两个组件又依赖 keyLifecycle；先绑定可回填的回调。
        YierdisDbMemoryBudgetCallbacks memoryBudgetCallbacks = new YierdisDbMemoryBudgetCallbacks();
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                config.maxmemoryBytes,
                config.maxmemoryPolicy,
                memoryBudgetCallbacks::cleanupExpired,
                memoryBudgetCallbacks::evictUntilUnder,
                memoryBudgetCallbacks::usedBytesForMaxmemory,
                checkedRuntimeState::maxmemoryCoordinator,
                checkedRuntimeState::maxmemoryParticipant
        );
        YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(
                checkedOwner::checkThread,
                ledger,
                storage.stableMemoryBackend,
                health,
                checkedRuntimeState::commitPublisher,
                checkedRuntimeState::commitDbIndex
        );
        YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
                storage.stableMemoryBackend,
                storage.entries,
                storage.keyDirectory,
                storage.stringRoot,
                storage.listRoot,
                storage.hashRoot,
                storage.setRoot,
                storage.zsetRoot,
                checkedRuntimeState::nextLruClock
        );
        DbComponentMemoryUsage memoryUsage = new DbComponentMemoryUsage(
                checkedOwner::checkThread,
                keyLifecycle,
                storage.hashTableMaintenanceRegistry
        );
        YierdisDbInternals internals = new YierdisDbRuntimeInternals(
                checkedOwner::checkThread,
                mutationExecutor,
                keyLifecycle,
                ledger
        );
        YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                checkedOwner::checkThread,
                internals,
                keyLifecycle,
                config.expireCleanupTimeLimitNanos
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
                checkedOwner::checkThread,
                internals,
                memoryUsage,
                keyLifecycle,
                storage.hashTableMaintenanceRegistry,
                config.maxmemoryBytes,
                ledger,
                memoryEstimator,
                checkedRuntimeState::lastNativeDefragReport,
                storage.stableMemoryBackend::liveRegionCount
        );
        YierdisDbIntrospection introspection = new YierdisDbIntrospection(
                checkedOwner::checkThread,
                internals,
                keyLifecycle
        );
        YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                checkedOwner::checkThread,
                internals,
                keyLifecycle,
                memoryReporter::usedBytesForMaxmemory,
                memoryReporter::memoryUsage,
                expirationSupport::cleanupExpired,
                config.maxmemoryPolicy,
                config.maxmemorySamples,
                config.evictionTimeLimitNanos
        );
        memoryBudgetCallbacks.bind(
                () -> expirationSupport.cleanupExpired(0L),
                maxmemorySupport::evictUntilUnder,
                memoryReporter::usedBytesForMaxmemory
        );
        checkedRuntimeState.bind(config, storage, ledger, keyLifecycle);
        YierdisDbDataMaintenance maintenance = new YierdisDbDataMaintenance(
                checkedRuntimeState,
                health,
                storage.hashTableMaintenanceRegistry,
                mutationExecutor,
                expirationSupport,
                maxmemorySupport,
                memoryReporter,
                config.expireCleanupTimeLimitNanos
        );

        return new YierdisDbComponents(
                checkedRuntimeState,
                storage,
                storage.entries,
                storage.keyDirectory,
                config,
                health,
                memoryUsage,
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
                new YierdisDbWrites(
                        internals,
                        stringOps,
                        hashOps,
                        listOps,
                        setOps,
                        zsetOps,
                        hllOps,
                        keyspaceOps,
                        ttlOps
                ),
                new YierdisDbExpirationManager(expirationSupport, health),
                new YierdisDbMemoryOps(memoryReporter, introspection),
                new YierdisDbLifecycleOps(checkedOwner::checkThread, maintenance::flushDb),
                maintenance
        );
    }

    private static NativeDefragOptions nativeDefragOptions(DbDefragConfig defrag) {
        DbDefragConfig checkedDefrag = Objects.requireNonNull(defrag, "defrag");
        if (!checkedDefrag.enabled()) {
            return null;
        }
        return new NativeDefragOptions(
                checkedDefrag.maxMoveBytes(),
                checkedDefrag.maxObjects(),
                TimeUnit.MILLISECONDS.toNanos(checkedDefrag.timeLimitMillis())
        );
    }

    interface OwnerCallbacks {
        void checkThread();
    }
}
