package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

import java.util.Objects;

final class YierdisDbRuntimeState {
    private static final NativeDefragReport EMPTY_NATIVE_DEFRAG_REPORT = new NativeDefragReport(
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            false,
            false
    );

    private final int dbIndex;
    private final DbThreadGuard threadGuard;
    private final StableMemoryBackend stableMemoryBackend;

    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private volatile MaxmemoryParticipant maxmemoryParticipant;
    private volatile DbCommitPublisher commitPublisher = DbCommitPublisher.NOOP;
    private volatile int commitDbIndex;
    private NativeDefragReport lastNativeDefragReport = EMPTY_NATIVE_DEFRAG_REPORT;
    private boolean lruEnabled;
    private long lruClock;
    private NativeDefragOptions nativeDefragOptions;
    private YierdisDbStorageComponents storage;
    private YierdisDbMemoryLedger ledger;
    private YierdisDbKeyLifecycle keyLifecycle;

    YierdisDbRuntimeState(
            int dbIndex,
            DbThreadGuard threadGuard,
            StableMemoryBackend stableMemoryBackend
    ) {
        this.dbIndex = Math.max(0, dbIndex);
        this.threadGuard = Objects.requireNonNull(threadGuard, "threadGuard");
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        this.commitDbIndex = this.dbIndex;
    }

    int dbIndex() {
        return dbIndex;
    }

    void bind(
            YierdisDbConfig config,
            YierdisDbStorageComponents storage,
            YierdisDbMemoryLedger ledger,
            YierdisDbKeyLifecycle keyLifecycle
    ) {
        this.lruEnabled = Objects.requireNonNull(config, "config").lruEnabled;
        this.nativeDefragOptions = config.nativeDefragOptions;
        this.storage = Objects.requireNonNull(storage, "storage");
        if (storage.stableMemoryBackend != stableMemoryBackend) {
            throw new IllegalArgumentException("storage must use the composed stable memory backend");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    void bindToCurrentThread() {
        stableMemoryBackend.bindToCurrentThread();
    }

    void checkThread() {
        threadGuard.checkDbAccess();
    }

    void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        this.maxmemoryCoordinator = coordinator;
    }

    void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex) {
        if (dbIndex < 0) {
            throw new IllegalArgumentException("dbIndex must be non-negative");
        }
        this.commitPublisher = Objects.requireNonNull(publisher, "publisher");
        this.commitDbIndex = dbIndex;
    }

    DbCommitPublisher commitPublisher() {
        return commitPublisher;
    }

    int commitDbIndex() {
        return commitDbIndex;
    }

    void bindMaxmemoryParticipant(MaxmemoryParticipant participant) {
        this.maxmemoryParticipant = Objects.requireNonNull(participant, "participant");
    }

    MaxmemoryParticipant maxmemoryParticipant() {
        return maxmemoryParticipant;
    }

    MaxmemoryCoordinator maxmemoryCoordinator() {
        return maxmemoryCoordinator;
    }

    boolean hasNoMaxmemoryCoordinator() {
        return maxmemoryCoordinator == null;
    }

    long nextLruClock() {
        if (!lruEnabled) {
            return 0L;
        }
        MaxmemoryCoordinator coordinator = maxmemoryCoordinator;
        if (coordinator != null) {
            return coordinator.nextLruClock();
        }
        // 没有 coordinator 时使用本 DB 的本地递增时钟；它只保证本 DB 内 ALLKEYS_LRU 的相对访问顺序。
        return ++lruClock;
    }

    void adjustUsedBytes(long deltaBytes) {
        ledger().commit(null, deltaBytes);
    }

    void enforceMaxmemory() {
        checkThread();
        try {
            // reserve(0) 复用 ledger 的过期清理、淘汰和 OOM 映射路径，避免另起一套预算判定口径。
            ledger().reserve(0);
        } catch (MemoryLedgerOutOfMemoryException e) {
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        }
    }

    void defragMaintenance() {
        checkThread();
        if (nativeDefragOptions == null) {
            return;
        }
        lastNativeDefragReport = keyLifecycle().stableMemoryBackend().defragCycle(nativeDefragOptions);
    }

    NativeDefragReport lastNativeDefragReport() {
        return lastNativeDefragReport;
    }

    YierdisDbMemoryLedger memoryLedger() {
        return ledger();
    }

    StableMemoryBackend stableMemoryBackend() {
        return stableMemoryBackend;
    }

    void shutdown() {
        if (!threadGuard.beginShutdown()) {
            return;
        }
        Throwable failure = null;
        try {
            ledger().resetUsage();
        } catch (Throwable next) {
            failure = next;
        }
        try {
            YierdisDbStorageComponents currentStorage = storage();
            currentStorage.resources.releaseAll(
                    currentStorage.expires,
                    currentStorage.entries,
                    currentStorage.keyDirectory,
                    currentStorage.stringRoot,
                    currentStorage.listRoot,
                    currentStorage.hashRoot,
                    currentStorage.setRoot,
                    currentStorage.zsetRoot
            );
        } catch (Throwable next) {
            if (failure == null) {
                failure = next;
            } else {
                failure.addSuppressed(next);
            }
        } finally {
            threadGuard.finishShutdown();
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        if (failure != null) {
            throw new IllegalStateException("YierdisDb shutdown failed", failure);
        }
    }

    MemoryOwner memoryOwnerForTesting() {
        return threadGuard;
    }

    FlushPreparation prepareFlushDb() {
        checkThread();
        YierdisDbKeyLifecycle currentKeyLifecycle = keyLifecycle();
        YierdisDbStorageComponents currentStorage = storage();
        boolean hadKeys = currentKeyLifecycle.keyCount() != 0;
        boolean hadTtl = currentStorage.expires.size() != 0;
        return new FlushPreparation(MutationOutcome.of(hadKeys, hadTtl), -ledger().usedBytes());
    }

    void commitFlushDb() {
        checkThread();
        YierdisDbStorageComponents currentStorage = storage();
        currentStorage.resources.clearData(
                currentStorage.expires,
                currentStorage.entries,
                currentStorage.keyDirectory,
                currentStorage.stringRoot,
                currentStorage.listRoot,
                currentStorage.hashRoot,
                currentStorage.setRoot,
                currentStorage.zsetRoot
        );
        keyLifecycle().resetExpiredEntriesAwaitingPhysicalDeletion();
    }

    int size() {
        checkThread();
        return keyLifecycle().keyCount();
    }

    private YierdisDbStorageComponents storage() {
        if (storage == null) {
            throw new IllegalStateException("runtime state is not bound");
        }
        return storage;
    }

    private YierdisDbMemoryLedger ledger() {
        if (ledger == null) {
            throw new IllegalStateException("runtime state is not bound");
        }
        return ledger;
    }

    private YierdisDbKeyLifecycle keyLifecycle() {
        if (keyLifecycle == null) {
            throw new IllegalStateException("runtime state is not bound");
        }
        return keyLifecycle;
    }

    record FlushPreparation(MutationOutcome outcome, long committedMemoryDelta) {
    }
}
