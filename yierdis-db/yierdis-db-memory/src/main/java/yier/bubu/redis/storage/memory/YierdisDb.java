package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.CommitPublishingDbEngine;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.DefragmentableDbEngine;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.expire.YierdisDbExpirationSupport;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

/**
 * 基于稳定内存后端的单 owner DB；只能由 {@link YierdisDbEngineFactory} 组合。
 */
public final class YierdisDb
        implements CommitPublishingDbEngine, GlobalMaxmemoryDbEngine, DefragmentableDbEngine, MemoryOps, AutoCloseable {
    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbStorage storage;
    private final YierdisDbHealth health;
    private final DbComponentMemoryUsage memoryUsage;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbIntrospection introspection;
    private final YierdisDbMemoryReporter memoryReporter;
    private final YierdisDbDataMaintenance maintenance;
    private final YierdisDbOperationViews operations;

    static YierdisDb create(
            DbEngineConfig config,
            StableMemoryBackend stableMemoryBackend,
            DbThreadGuard threadGuard,
            HashSeed hashSeed
    ) {
        return new YierdisDb(config, stableMemoryBackend, threadGuard, hashSeed);
    }

    private YierdisDb(
            DbEngineConfig config,
            StableMemoryBackend stableMemoryBackend,
            DbThreadGuard threadGuard,
            HashSeed hashSeed
    ) {
        StableMemoryBackend backend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        YierdisDbStorage storage = null;
        try {
            DbEngineConfig checkedConfig = Objects.requireNonNull(config, "config");
            long maxmemoryBytes = checkedConfig.maxmemoryBytes();
            MaxmemoryPolicy maxmemoryPolicy = checkedConfig.maxmemoryPolicy();
            YierdisDbRuntimeState runtimeState = new YierdisDbRuntimeState(
                    checkedConfig.dbIndex(),
                    Objects.requireNonNull(threadGuard, "threadGuard"),
                    backend,
                    maxmemoryBytes > 0L && maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU,
                    nativeDefragOptions(checkedConfig.defrag())
            );
            storage = YierdisDbStorage.create(
                    backend,
                    Objects.requireNonNull(hashSeed, "hashSeed"),
                    runtimeState::nextLruClock
            );
            Runnable threadChecker = runtimeState::checkThread;
            long evictionTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                    checkedConfig.evictionTimeLimitMillis()
            );
            long expireCleanupTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                    checkedConfig.expireCleanupTimeLimitMillis()
            );
            YierdisDbHealth health = new YierdisDbHealth(threadChecker);
            // ledger 的 admission 回调依赖稍后创建的 expiry/maxmemory 模块；该占位对象只负责收敛这条构造环。
            YierdisDbMemoryBudgetCallbacks memoryBudgetCallbacks = new YierdisDbMemoryBudgetCallbacks();
            YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                    maxmemoryBytes,
                    maxmemoryPolicy,
                    memoryBudgetCallbacks::cleanupExpired,
                    memoryBudgetCallbacks::evictUntilUnder,
                    memoryBudgetCallbacks::usedBytesForMaxmemory,
                    runtimeState::maxmemoryCoordinator,
                    runtimeState::maxmemoryParticipant
            );
            YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(
                    threadChecker,
                    ledger,
                    storage.stableMemoryBackend(),
                    health,
                    runtimeState::commitPublisher,
                    runtimeState::commitDbIndex
            );
            YierdisDbKeyLifecycle keyLifecycle = storage.keyLifecycle();
            DbComponentMemoryUsage memoryUsage = new DbComponentMemoryUsage(
                    threadChecker,
                    keyLifecycle,
                    storage.hashTableMaintenanceRegistry()
            );
            YierdisDbRuntimeInternals internals = new YierdisDbRuntimeInternals(
                    threadChecker,
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
                    storage.hashTableMaintenanceRegistry(),
                    maxmemoryBytes,
                    ledger,
                    new YierdisDbMemoryEstimator(),
                    runtimeState::lastNativeDefragReport,
                    storage.stableMemoryBackend()::liveRegionCount
            );
            YierdisDbIntrospection introspection = new YierdisDbIntrospection(internals);
            YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                    internals,
                    memoryReporter::usedBytesForMaxmemory,
                    memoryReporter::memoryUsage,
                    expirationSupport::cleanupExpired,
                    maxmemoryPolicy,
                    checkedConfig.maxmemorySamples(),
                    evictionTimeLimitNanos
            );
            memoryBudgetCallbacks.bind(
                    () -> expirationSupport.cleanupExpired(0L),
                    maxmemorySupport::evictUntilUnder,
                    memoryReporter::usedBytesForMaxmemory
            );
            YierdisDbDataMaintenance maintenance = new YierdisDbDataMaintenance(
                    runtimeState,
                    storage,
                    ledger,
                    health,
                    storage.hashTableMaintenanceRegistry(),
                    mutationExecutor,
                    expirationSupport,
                    maxmemorySupport,
                    memoryReporter,
                    expireCleanupTimeLimitNanos
            );
            YierdisDbOperationViews operations = new YierdisDbOperationViews(
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
                    new YierdisDbLifecycleOps(threadChecker, maintenance::flushDb, maintenance::flushDbAsync)
            );

            this.runtimeState = runtimeState;
            this.storage = storage;
            this.health = health;
            this.memoryUsage = memoryUsage;
            this.ledger = ledger;
            this.introspection = introspection;
            this.memoryReporter = memoryReporter;
            this.maintenance = maintenance;
            this.operations = operations;
            runtimeState.bindMaxmemoryParticipant(this);
        } catch (Throwable failure) {
            try {
                if (storage == null) {
                    backend.close();
                } else {
                    storage.close();
                }
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
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

    @Override
    public DbReads reads() {
        return operations.reads();
    }

    @Override
    public DbWrites writes() {
        return operations.writes();
    }

    @Override
    public MemoryOps memory() {
        return this;
    }

    @Override
    public long memoryUsage(BytesView keyView) {
        return memoryReporter.memoryUsage(keyView);
    }

    @Override
    public YierdisMemoryStats memoryStats() {
        return memoryReporter.memoryStats();
    }

    @Override
    public String objectEncoding(BytesView keyView) {
        return introspection.objectEncoding(keyView);
    }

    @Override
    public DbLifecycleOps lifecycle() {
        return operations.lifecycle();
    }

    @Override
    public DbHealthSnapshot health() {
        return health.snapshot();
    }

    @Override
    public void bindToCurrentThread() {
        runtimeState.bindToCurrentThread();
    }

    @Override
    public void runMaintenance() {
        maintenance.runMaintenance();
    }

    @Override
    public void runDeferredReclamation() {
        maintenance.runDeferredReclamation();
    }

    @Override
    public void shutdown() {
        maintenance.shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        runtimeState.attachMaxmemoryCoordinator(coordinator);
    }

    @Override
    public void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex) {
        runtimeState.attachCommitPublisher(publisher, dbIndex);
    }

    @Override
    public void defragMaintenance() {
        maintenance.defragMaintenance();
    }

    public long usedBytesForMaxmemory() {
        return maintenance.usedBytesForMaxmemory();
    }

    @Override
    public int keyCountEstimate() {
        return maintenance.keyCountEstimate();
    }

    @Override
    public void cleanupExpired(long nowMillis) {
        maintenance.cleanupExpired(nowMillis);
    }

    @Override
    public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return maintenance.sampleCandidate(this, policy, nowMillis);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return maintenance.scanBestCandidate(this, policy, nowMillis);
    }

    @Override
    public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        return maintenance.evict(this, candidate, nowMillis);
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return memoryUsage.snapshot();
    }

    @Override
    public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
        runtimeState.checkThread();
        return storage.stableMemoryBackend().trimEmptyPages(
                Objects.requireNonNull(budget, "budget")
        );
    }

    void enforceMaxmemory() {
        maintenance.enforceMaxmemory();
    }

    HashTableMaintenanceResult rehashMaintenance(HashTableWorkBudget budget) {
        return maintenance.rehashMaintenance(budget);
    }

    NativeDefragReport lastNativeDefragReport() {
        return runtimeState.lastNativeDefragReport();
    }

    static byte[] toByteArray(BytesView view) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        int length = view.length();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
            bytes[index] = view.getByte(index);
        }
        return bytes;
    }

    void checkThread() {
        runtimeState.checkThread();
    }

    YierdisDbMemoryLedger memoryLedger() {
        return ledger;
    }

    YierdisDbHealth healthMonitor() {
        return health;
    }

    StableMemoryBackend stableMemoryBackend() {
        return storage.stableMemoryBackend();
    }

    DbCommitPublisher commitPublisher() {
        return runtimeState.commitPublisher();
    }

    int commitDbIndex() {
        return runtimeState.commitDbIndex();
    }

    MemoryOwner memoryOwnerForTesting() {
        return runtimeState.memoryOwnerForTesting();
    }

    void armMemoryUsageIterationTrapsForTesting() {
        storage.keyLifecycle().keyDirectory().armIterationTrapForTesting();
        storage.keyLifecycle().listRoot().armIterationTrapForTesting();
        storage.keyLifecycle().hashRoot().armIterationTrapForTesting();
        storage.keyLifecycle().setRoot().armIterationTrapForTesting();
        storage.keyLifecycle().zsetRoot().armIterationTrapForTesting();
    }

    void disarmMemoryUsageIterationTrapsForTesting() {
        storage.keyLifecycle().keyDirectory().disarmIterationTrapForTesting();
        storage.keyLifecycle().listRoot().disarmIterationTrapForTesting();
        storage.keyLifecycle().hashRoot().disarmIterationTrapForTesting();
        storage.keyLifecycle().setRoot().disarmIterationTrapForTesting();
        storage.keyLifecycle().zsetRoot().disarmIterationTrapForTesting();
    }

    MutationOutcome flushDb() {
        return maintenance.flushDb(MutationContext.none());
    }

    int size() {
        return maintenance.size();
    }

    long detachedEntryCount() {
        return maintenance.detachedEntryCount();
    }

    long estimatedUsedBytes() {
        return maintenance.estimatedUsedBytes();
    }

    YierdisDbIntrospection introspection() {
        return introspection;
    }

    void cleanupExpired() {
        maintenance.cleanupExpired();
    }

    boolean isKeyExpired(byte[] keyBytes, long nowMillis) {
        KeyHandle handle = storage.keyLifecycle().keyHandle(keyBytes);
        return handle != null && isKeyExpired(handle, nowMillis);
    }

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        return storage.keyLifecycle().isKeyExpired(keyHandle, nowMillis);
    }

    YierdisDbKeyLifecycle keyLifecycle() {
        return storage.keyLifecycle();
    }
}
