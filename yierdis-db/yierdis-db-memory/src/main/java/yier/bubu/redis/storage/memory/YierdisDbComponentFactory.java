package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
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
            YierdisDbRuntimeState runtimeState,
            DbEngineConfig engineConfig,
            HashSeed hashSeed
    ) {
        YierdisDbRuntimeState checkedRuntimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        Runnable checkedThreadChecker = checkedRuntimeState::checkThread;
        DbEngineConfig checkedEngineConfig = Objects.requireNonNull(engineConfig, "engineConfig");
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(
                checkedRuntimeState.stableMemoryBackend(),
                Objects.requireNonNull(hashSeed, "hashSeed")
        );
        long maxmemoryBytes = checkedEngineConfig.maxmemoryBytes();
        MaxmemoryPolicy maxmemoryPolicy = checkedEngineConfig.maxmemoryPolicy();
        int maxmemorySamples = checkedEngineConfig.maxmemorySamples();
        long evictionTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                checkedEngineConfig.evictionTimeLimitMillis()
        );
        long expireCleanupTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                checkedEngineConfig.expireCleanupTimeLimitMillis()
        );
        NativeDefragOptions nativeDefragOptions = nativeDefragOptions(checkedEngineConfig.defrag());
        boolean lruEnabled = maxmemoryBytes > 0L && maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU;
        YierdisDbMemoryEstimator memoryEstimator = new YierdisDbMemoryEstimator();
        YierdisDbHealth health = new YierdisDbHealth(checkedThreadChecker);
        // ledger 需要在写入前触发过期清理和淘汰，但这两个组件又依赖 keyLifecycle；先绑定可回填的回调。
        YierdisDbMemoryBudgetCallbacks memoryBudgetCallbacks = new YierdisDbMemoryBudgetCallbacks();
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                maxmemoryBytes,
                maxmemoryPolicy,
                memoryBudgetCallbacks::cleanupExpired,
                memoryBudgetCallbacks::evictUntilUnder,
                memoryBudgetCallbacks::usedBytesForMaxmemory,
                checkedRuntimeState::maxmemoryCoordinator,
                checkedRuntimeState::maxmemoryParticipant
        );
        YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(
                checkedThreadChecker,
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
                checkedThreadChecker,
                keyLifecycle,
                storage.hashTableMaintenanceRegistry
        );
        YierdisDbRuntimeInternals internals = new YierdisDbRuntimeInternals(
                checkedThreadChecker,
                mutationExecutor,
                keyLifecycle,
                ledger
        );
        YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                internals,
                expireCleanupTimeLimitNanos
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
                internals,
                memoryUsage,
                storage.hashTableMaintenanceRegistry,
                maxmemoryBytes,
                ledger,
                memoryEstimator,
                checkedRuntimeState::lastNativeDefragReport,
                storage.stableMemoryBackend::liveRegionCount
        );
        YierdisDbIntrospection introspection = new YierdisDbIntrospection(internals);
        YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                internals,
                memoryReporter::usedBytesForMaxmemory,
                memoryReporter::memoryUsage,
                expirationSupport::cleanupExpired,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitNanos
        );
        memoryBudgetCallbacks.bind(
                () -> expirationSupport.cleanupExpired(0L),
                maxmemorySupport::evictUntilUnder,
                memoryReporter::usedBytesForMaxmemory
        );
        checkedRuntimeState.bind(lruEnabled, nativeDefragOptions, storage, ledger, keyLifecycle);
        YierdisDbDataMaintenance maintenance = new YierdisDbDataMaintenance(
                checkedRuntimeState,
                health,
                storage.hashTableMaintenanceRegistry,
                mutationExecutor,
                expirationSupport,
                maxmemorySupport,
                memoryReporter,
                expireCleanupTimeLimitNanos
        );

        return new YierdisDbComponents(
                health,
                memoryUsage,
                ledger,
                keyLifecycle,
                introspection,
                maintenance,
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
                new YierdisDbLifecycleOps(checkedThreadChecker, maintenance::flushDb)
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
}
