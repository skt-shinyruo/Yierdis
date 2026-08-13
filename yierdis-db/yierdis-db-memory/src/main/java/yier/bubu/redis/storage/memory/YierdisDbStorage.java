package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.LongSupplier;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

/**
 * DB storage graph 的单一 ownership 记录；关闭该记录会按依赖顺序释放整张图和 backend。
 */
record YierdisDbStorage(
        HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
        YierdisDbKeyLifecycle keyLifecycle
) implements AutoCloseable {
    YierdisDbStorage {
        Objects.requireNonNull(hashTableMaintenanceRegistry, "hashTableMaintenanceRegistry");
        Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    static YierdisDbStorage create(
            StableMemoryBackend stableMemoryBackend,
            HashSeed hashSeed,
            LongSupplier lruClockSupplier
    ) {
        StableMemoryBackend backend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        HashTableMaintenanceRegistry maintenanceRegistry = null;
        EntryTable entries = null;
        NativeKeyDirectory keyDirectory = null;
        StringRoot stringRoot = null;
        ListRoot listRoot = null;
        HashRoot hashRoot = null;
        SetRoot setRoot = null;
        ZSetRoot zsetRoot = null;
        try {
            HashSeed resolvedHashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
            LongSupplier resolvedLruClockSupplier = Objects.requireNonNull(
                    lruClockSupplier,
                    "lruClockSupplier"
            );
            maintenanceRegistry = new HashTableMaintenanceRegistry();
            entries = new EntryTable(backend);
            keyDirectory = new NativeKeyDirectory(backend, resolvedHashSeed, maintenanceRegistry);
            stringRoot = new StringRoot(backend);
            listRoot = new ListRoot(backend);
            hashRoot = new HashRoot(backend, resolvedHashSeed, maintenanceRegistry);
            setRoot = new SetRoot(backend, resolvedHashSeed, maintenanceRegistry);
            zsetRoot = new ZSetRoot(backend, resolvedHashSeed, maintenanceRegistry);
            YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
                    backend,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    resolvedLruClockSupplier
            );
            return new YierdisDbStorage(maintenanceRegistry, keyLifecycle);
        } catch (Throwable failure) {
            try {
                // create 一旦开始就消费 backend 所有权；失败路径与正常 close 使用同一释放顺序。
                YierdisDbKeyLifecycle.closePartiallyConstructed(
                        backend,
                        entries,
                        keyDirectory,
                        stringRoot,
                        listRoot,
                        hashRoot,
                        setRoot,
                        zsetRoot
                );
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    void clearData() {
        keyLifecycle.clearData();
    }

    void detachEntries() {
        keyLifecycle.detachEntries();
    }

    long detachedEntryCount() {
        return keyLifecycle.detachedEntryCount();
    }

    void reclaimDetachedEntry() {
        keyLifecycle.reclaimDetachedEntry();
    }

    @Override
    public void close() {
        keyLifecycle.close();
    }
}
