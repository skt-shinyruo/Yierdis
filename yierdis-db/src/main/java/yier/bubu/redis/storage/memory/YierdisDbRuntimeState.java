package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;

import java.util.Objects;
import java.util.function.Function;

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
    private final boolean lruEnabled;
    private final NativeDefragOptions nativeDefragOptions;

    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private volatile MaxmemoryParticipant maxmemoryParticipant;
    private NativeDefragReport lastNativeDefragReport = EMPTY_NATIVE_DEFRAG_REPORT;
    private long lruClock;

    YierdisDbRuntimeState(
            int dbIndex,
            DbThreadGuard threadGuard,
            boolean lruEnabled,
            NativeDefragOptions nativeDefragOptions
    ) {
        this.dbIndex = Math.max(0, dbIndex);
        this.threadGuard = Objects.requireNonNull(threadGuard, "threadGuard");
        this.lruEnabled = lruEnabled;
        this.nativeDefragOptions = nativeDefragOptions;
    }

    int dbIndex() {
        return dbIndex;
    }

    void checkThread() {
        threadGuard.checkDbAccess();
    }

    void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        this.maxmemoryCoordinator = coordinator;
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

    void defragMaintenance(Function<NativeDefragOptions, NativeDefragReport> defragCycle) {
        checkThread();
        Objects.requireNonNull(defragCycle, "defragCycle");
        if (nativeDefragOptions == null) {
            return;
        }
        lastNativeDefragReport = defragCycle.apply(nativeDefragOptions);
    }

    NativeDefragReport lastNativeDefragReport() {
        return lastNativeDefragReport;
    }

    boolean beginShutdown() {
        return threadGuard.beginShutdown();
    }

    void finishShutdown() {
        threadGuard.finishShutdown();
    }

}
