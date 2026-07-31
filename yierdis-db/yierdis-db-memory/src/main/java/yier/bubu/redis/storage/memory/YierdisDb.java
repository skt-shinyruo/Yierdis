package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.CommitPublishingDbEngine;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.DefragmentableDbEngine;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

/**
 * 基于稳定内存后端的单 owner DB；只能由 {@link YierdisDbEngineFactory} 组合。
 */
public final class YierdisDb
        implements CommitPublishingDbEngine, GlobalMaxmemoryDbEngine, DefragmentableDbEngine, AutoCloseable {
    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbHealth health;
    private final DbComponentMemoryUsage memoryUsage;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbIntrospection introspection;
    private final YierdisDbDataMaintenance maintenance;
    private final DbReads reads;
    private final DbWrites writes;
    private final ExpirationManager expirationManager;
    private final MemoryOps memoryOps;
    private final DbLifecycleOps lifecycleOps;

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
        DbEngineConfig checkedConfig = Objects.requireNonNull(config, "config");
        YierdisDbRuntimeState composedRuntimeState = new YierdisDbRuntimeState(
                checkedConfig.dbIndex(),
                Objects.requireNonNull(threadGuard, "threadGuard"),
                Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend")
        );
        YierdisDbComponents components = YierdisDbComponentFactory.create(
                composedRuntimeState::checkThread,
                composedRuntimeState,
                stableMemoryBackend,
                checkedConfig,
                Objects.requireNonNull(hashSeed, "hashSeed")
        );

        this.runtimeState = components.runtimeState;
        this.health = components.health;
        this.memoryUsage = components.memoryUsage;
        this.ledger = components.ledger;
        this.keyLifecycle = components.keyLifecycle;
        this.introspection = components.introspection;
        this.maintenance = components.maintenance;
        this.reads = components.reads;
        this.writes = components.writes;
        this.expirationManager = components.expirationManager;
        this.memoryOps = components.memoryOps;
        this.lifecycleOps = components.lifecycleOps;
        runtimeState.bindMaxmemoryParticipant(this);
    }

    @Override
    public DbReads reads() {
        return reads;
    }

    @Override
    public DbWrites writes() {
        return writes;
    }

    @Override
    public ExpirationManager expiration() {
        return expirationManager;
    }

    @Override
    public MemoryOps memory() {
        return memoryOps;
    }

    @Override
    public DbLifecycleOps lifecycle() {
        return lifecycleOps;
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
        return keyLifecycle.stableMemoryBackend().trimEmptyPages(
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
        return keyLifecycle.stableMemoryBackend();
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
        keyLifecycle.keyDirectory().armIterationTrapForTesting();
        keyLifecycle.listRoot().armIterationTrapForTesting();
        keyLifecycle.hashRoot().armIterationTrapForTesting();
        keyLifecycle.setRoot().armIterationTrapForTesting();
        keyLifecycle.zsetRoot().armIterationTrapForTesting();
    }

    void disarmMemoryUsageIterationTrapsForTesting() {
        keyLifecycle.keyDirectory().disarmIterationTrapForTesting();
        keyLifecycle.listRoot().disarmIterationTrapForTesting();
        keyLifecycle.hashRoot().disarmIterationTrapForTesting();
        keyLifecycle.setRoot().disarmIterationTrapForTesting();
        keyLifecycle.zsetRoot().disarmIterationTrapForTesting();
    }

    MutationOutcome flushDb() {
        return maintenance.flushDb(MutationContext.none());
    }

    int size() {
        return maintenance.size();
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
        KeyHandle handle = keyLifecycle.keyHandle(keyBytes);
        return handle != null && isKeyExpired(handle, nowMillis);
    }

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        return keyLifecycle.isKeyExpired(keyHandle, nowMillis);
    }

    YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }
}
