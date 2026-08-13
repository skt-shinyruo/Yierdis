package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;

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
    private final boolean lruEnabled;
    private final NativeDefragOptions nativeDefragOptions;

    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private volatile MaxmemoryParticipant maxmemoryParticipant;
    private volatile DbCommitPublisher commitPublisher = DbCommitPublisher.NOOP;
    private volatile int commitDbIndex;
    private NativeDefragReport lastNativeDefragReport = EMPTY_NATIVE_DEFRAG_REPORT;
    private long lruClock;

    YierdisDbRuntimeState(
            int dbIndex,
            DbThreadGuard threadGuard,
            StableMemoryBackend stableMemoryBackend,
            boolean lruEnabled,
            NativeDefragOptions nativeDefragOptions
    ) {
        this.dbIndex = Math.max(0, dbIndex);
        this.threadGuard = Objects.requireNonNull(threadGuard, "threadGuard");
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        this.lruEnabled = lruEnabled;
        this.nativeDefragOptions = nativeDefragOptions;
        this.commitDbIndex = this.dbIndex;
    }

    int dbIndex() {
        return dbIndex;
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

    void defragMaintenance() {
        checkThread();
        if (nativeDefragOptions == null) {
            return;
        }
        lastNativeDefragReport = stableMemoryBackend.defragCycle(nativeDefragOptions);
    }

    NativeDefragReport lastNativeDefragReport() {
        return lastNativeDefragReport;
    }

    StableMemoryBackend stableMemoryBackend() {
        return stableMemoryBackend;
    }

    boolean beginShutdown() {
        return threadGuard.beginShutdown();
    }

    void finishShutdown() {
        threadGuard.finishShutdown();
    }

    MemoryOwner memoryOwnerForTesting() {
        return threadGuard;
    }

}
