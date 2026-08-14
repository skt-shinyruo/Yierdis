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
    private final YierdisDbKernel kernel;
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
        boolean backendOwnershipTransferred = false;
        try {
            DbEngineConfig checkedConfig = Objects.requireNonNull(config, "config");
            long maxmemoryBytes = checkedConfig.maxmemoryBytes();
            MaxmemoryPolicy maxmemoryPolicy = checkedConfig.maxmemoryPolicy();
            YierdisDbRuntimeState runtimeState = new YierdisDbRuntimeState(
                    checkedConfig.dbIndex(),
                    Objects.requireNonNull(threadGuard, "threadGuard"),
                    maxmemoryBytes > 0L && maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU,
                    nativeDefragOptions(checkedConfig.defrag())
            );
            HashSeed checkedHashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
            backendOwnershipTransferred = true;
            storage = YierdisDbStorage.create(
                    backend,
                    checkedHashSeed,
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
                    backend,
                    health,
                    runtimeState::commitPublisher,
                    runtimeState::commitDbIndex
            );
            YierdisDbKeyLifecycle keyLifecycle = storage.keyLifecycle();
            YierdisDbMemoryContext memoryContext = new YierdisDbMemoryContext(ledger, backend);
            YierdisDbKernel kernel = new YierdisDbKernel(
                    threadChecker,
                    mutationExecutor,
                    keyLifecycle,
                    memoryContext
            );
            DbComponentMemoryUsage memoryUsage = new DbComponentMemoryUsage(
                    threadChecker,
                    memoryContext,
                    keyLifecycle,
                    storage.hashTableMaintenanceRegistry()
            );
            YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                    kernel,
                    keyLifecycle,
                    expireCleanupTimeLimitNanos
            );
            YierdisStringOps stringOps = keyLifecycle.createStringOps(kernel, memoryContext);
            YierdisHashOps hashOps = keyLifecycle.createHashOps(kernel, memoryContext);
            YierdisListOps listOps = keyLifecycle.createListOps(kernel, memoryContext);
            YierdisSetOps setOps = keyLifecycle.createSetOps(kernel, memoryContext);
            YierdisZSetOps zsetOps = keyLifecycle.createZSetOps(kernel, memoryContext);
            YierdisHllOps hllOps = keyLifecycle.createHllOps(kernel, memoryContext);
            YierdisTtlOps ttlOps = new YierdisTtlOps(kernel, keyLifecycle, memoryContext);
            YierdisKeyspaceOps keyspaceOps = new YierdisKeyspaceOps(kernel, keyLifecycle, memoryContext);
            YierdisDbMemoryReporter memoryReporter = new YierdisDbMemoryReporter(
                    kernel,
                    memoryUsage,
                    storage.hashTableMaintenanceRegistry(),
                    maxmemoryBytes,
                    ledger,
                    new YierdisDbMemoryEstimator(),
                    runtimeState::lastNativeDefragReport
            );
            YierdisDbIntrospection introspection = new YierdisDbIntrospection(kernel);
            YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                    kernel,
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
                    kernel,
                    memoryContext,
                    ledger,
                    health,
                    storage.hashTableMaintenanceRegistry(),
                    expirationSupport,
                    maxmemorySupport,
                    memoryReporter,
                    expireCleanupTimeLimitNanos
            );
            YierdisDbOperationViews operations = new YierdisDbOperationViews(
                    new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps),
                    new YierdisDbWrites(
                            kernel,
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
            this.kernel = kernel;
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
                if (!backendOwnershipTransferred) {
                    backend.close();
                } else if (storage != null) {
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
        kernel.bindToCurrentThread();
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
        return storage.keyLifecycle().trimEmptyNativePages(budget);
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
        storage.keyLifecycle().armMemoryUsageIterationTrapsForTesting();
    }

    void disarmMemoryUsageIterationTrapsForTesting() {
        storage.keyLifecycle().disarmMemoryUsageIterationTrapsForTesting();
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
