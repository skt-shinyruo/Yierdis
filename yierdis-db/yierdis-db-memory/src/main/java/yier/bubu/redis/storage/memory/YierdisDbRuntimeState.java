package yier.bubu.redis.storage.memory;

import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
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
    private final DbThreadGuard threadGuard = new DbThreadGuard();

    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private NativeDefragReport lastNativeDefragReport = EMPTY_NATIVE_DEFRAG_REPORT;
    private boolean lruEnabled;
    private long lruClock;
    private NativeDefragOptions nativeDefragOptions;
    private YierdisDbStorageComponents storage;
    private YierdisDbMemoryLedger ledger;
    private YierdisDbKeyLifecycle keyLifecycle;

    YierdisDbRuntimeState(int dbIndex) {
        this.dbIndex = Math.max(0, dbIndex);
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
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    void bindToCurrentThread() {
        threadGuard.bindToCurrentThread();
        keyLifecycle().nativeAllocator().bindToCurrentThread();
    }

    void checkThread() {
        threadGuard.checkThread();
    }

    void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        this.maxmemoryCoordinator = coordinator;
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
        lastNativeDefragReport = keyLifecycle().nativeAllocator().defragCycle(nativeDefragOptions);
    }

    NativeDefragReport lastNativeDefragReport() {
        return lastNativeDefragReport;
    }

    YierdisDbMemoryLedger memoryLedger() {
        return ledger();
    }

    void shutdown() {
        threadGuard.checkThreadForShutdown();
        if (!threadGuard.tryMarkClosed()) {
            return;
        }
        // 先把实例标记为 closed，再释放 native 图；重复 shutdown 只能快速返回，避免二次 free 同一批 handle。
        ledger().resetUsage();
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
    }

    MutationOutcome flushDb() {
        checkThread();
        YierdisDbKeyLifecycle currentKeyLifecycle = keyLifecycle();
        YierdisDbStorageComponents currentStorage = storage();
        boolean hadKeys = currentKeyLifecycle.keyCount() != 0;
        boolean hadTtl = currentStorage.expires.size() != 0;
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
        ledger().resetUsage();
        return MutationOutcome.of(hadKeys, hadTtl);
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
}
