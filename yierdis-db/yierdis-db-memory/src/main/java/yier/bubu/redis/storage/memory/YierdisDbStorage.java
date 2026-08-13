package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.LongSupplier;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
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
        StableMemoryBackend stableMemoryBackend,
        HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
        YierdisDbKeyLifecycle keyLifecycle,
        YierdisDbOwnedResources resources
) implements AutoCloseable {
    YierdisDbStorage {
        Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        Objects.requireNonNull(hashTableMaintenanceRegistry, "hashTableMaintenanceRegistry");
        Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        Objects.requireNonNull(resources, "resources");
        if (keyLifecycle.stableMemoryBackend() != stableMemoryBackend) {
            throw new IllegalArgumentException("key lifecycle must use the storage backend");
        }
    }

    static YierdisDbStorage create(
            StableMemoryBackend stableMemoryBackend,
            HashSeed hashSeed,
            LongSupplier lruClockSupplier
    ) {
        StableMemoryBackend backend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        HashSeed resolvedHashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        LongSupplier resolvedLruClockSupplier = Objects.requireNonNull(lruClockSupplier, "lruClockSupplier");
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(backend);
        HashTableMaintenanceRegistry maintenanceRegistry = new HashTableMaintenanceRegistry();
        EntryTable entries = null;
        NativeKeyDirectory keyDirectory = null;
        StringRoot stringRoot = null;
        ListRoot listRoot = null;
        HashRoot hashRoot = null;
        SetRoot setRoot = null;
        ZSetRoot zsetRoot = null;
        try {
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
            return new YierdisDbStorage(backend, maintenanceRegistry, keyLifecycle, resources);
        } catch (Throwable failure) {
            try {
                // backend 仍由调用方持有；这里只释放已经进入 storage graph 的子资源。
                resources.releaseComponents(
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
        resources.clearData(
                keyLifecycle.entryTable(),
                keyLifecycle.keyDirectory(),
                keyLifecycle.stringRoot(),
                keyLifecycle.listRoot(),
                keyLifecycle.hashRoot(),
                keyLifecycle.setRoot(),
                keyLifecycle.zsetRoot()
        );
    }

    void detachEntries() {
        keyLifecycle.keyDirectory().detachEntries();
    }

    long detachedEntryCount() {
        return keyLifecycle.keyDirectory().detachedEntryCount();
    }

    void reclaimDetachedEntry() {
        keyLifecycle.keyDirectory().reclaimDetachedEntry((ignoredKey, entryHandle) -> releaseEntry(entryHandle));
    }

    private void releaseEntry(EntryHandle entryHandle) {
        resources.releaseEntry(
                keyLifecycle.entryTable(),
                keyLifecycle.stringRoot(),
                keyLifecycle.listRoot(),
                keyLifecycle.hashRoot(),
                keyLifecycle.setRoot(),
                keyLifecycle.zsetRoot(),
                entryHandle
        );
    }

    @Override
    public void close() {
        resources.releaseAll(
                keyLifecycle.entryTable(),
                keyLifecycle.keyDirectory(),
                keyLifecycle.stringRoot(),
                keyLifecycle.listRoot(),
                keyLifecycle.hashRoot(),
                keyLifecycle.setRoot(),
                keyLifecycle.zsetRoot()
        );
    }
}
