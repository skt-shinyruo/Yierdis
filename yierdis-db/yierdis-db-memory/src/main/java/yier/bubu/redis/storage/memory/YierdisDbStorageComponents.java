package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

public final class YierdisDbStorageComponents {
    private static final int ENTRY_TABLE_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int STRING_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int KEY_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int COLLECTION_ROOT_NATIVE_SLOT_CAPACITY = 64 * 1024;

    final YierdisFfmMemoryRuntime memoryRuntime;
    final NativeAllocator nativeAllocator;
    final YierdisDbOwnedResources resources;
    final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    final YierdisExpireIndex expires;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final StringRoot stringRoot;
    final ListRoot listRoot;
    final HashRoot hashRoot;
    final SetRoot setRoot;
    final ZSetRoot zsetRoot;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            YierdisDbOwnedResources resources,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot
    ) {
        this.memoryRuntime = memoryRuntime;
        this.nativeAllocator = nativeAllocator;
        this.resources = resources;
        this.hashTableMaintenanceRegistry = hashTableMaintenanceRegistry;
        this.expires = expires;
        this.entries = entries;
        this.keyDirectory = keyDirectory;
        this.stringRoot = stringRoot;
        this.listRoot = listRoot;
        this.hashRoot = hashRoot;
        this.setRoot = setRoot;
        this.zsetRoot = zsetRoot;
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime
    ) {
        return create(memoryRuntime, ownsMemoryRuntime, 0, HashSeed.random());
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            int nativeSlotCapacity
    ) {
        return create(memoryRuntime, ownsMemoryRuntime, nativeSlotCapacity, HashSeed.random());
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            int nativeSlotCapacity,
            HashSeed hashSeed
    ) {
        HashSeed resolvedHashSeed = java.util.Objects.requireNonNull(hashSeed, "hashSeed");
        YierdisFfmMemoryRuntime resolvedRuntime =
                memoryRuntime == null ? new YierdisFfmMemoryRuntime("db") : memoryRuntime;
        boolean resolvedOwnsRuntime = memoryRuntime == null || ownsMemoryRuntime;
        int resolvedNativeSlotCapacity = nativeSlotCapacity > 0
                ? nativeSlotCapacity
                : sharedNativeSlotCapacity();

        // Entry、key bytes、string bytes 和 collection roots 共享一个 stable allocator，
        // 使 defrag/释放时可以按统一的 native handle 域验证对象类型与存活状态。
        NativeAllocator nativeAllocator = new YierdisStableNativeAllocator(
                resolvedRuntime,
                resolvedNativeSlotCapacity
        );
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                resolvedRuntime,
                nativeAllocator,
                resolvedOwnsRuntime,
                true
        );
        HashTableMaintenanceRegistry hashTableMaintenanceRegistry = new HashTableMaintenanceRegistry();
        EntryTable entries = new EntryTable(resolvedRuntime, nativeAllocator);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(
                nativeAllocator,
                resolvedHashSeed,
                hashTableMaintenanceRegistry
        );
        StringRoot stringRoot = new StringRoot(nativeAllocator);
        ListRoot listRoot = new ListRoot(nativeAllocator);
        HashRoot hashRoot = new HashRoot(nativeAllocator, resolvedHashSeed, hashTableMaintenanceRegistry);
        SetRoot setRoot = new SetRoot(nativeAllocator, resolvedHashSeed, hashTableMaintenanceRegistry);
        ZSetRoot zsetRoot = new ZSetRoot(nativeAllocator, resolvedHashSeed, hashTableMaintenanceRegistry);
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                nativeAllocator,
                resources,
                hashTableMaintenanceRegistry,
                new YierdisFfmExpireIndex(
                        resolvedRuntime,
                        nativeAllocator,
                        resolvedHashSeed,
                        hashTableMaintenanceRegistry
                ),
                entries,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot
        );
    }

    static int sharedNativeSlotCapacity() {
        return ENTRY_TABLE_NATIVE_SLOT_CAPACITY
                + STRING_NATIVE_SLOT_CAPACITY
                + KEY_NATIVE_SLOT_CAPACITY
                + COLLECTION_ROOT_NATIVE_SLOT_CAPACITY;
    }
}
