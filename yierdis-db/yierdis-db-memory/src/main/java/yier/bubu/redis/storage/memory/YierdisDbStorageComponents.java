package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapAllocator;

public final class YierdisDbStorageComponents {
    private static final int ENTRY_TABLE_NATIVE_SLOT_CAPACITY = 64 * 1024;
    private static final int STRING_NATIVE_SLOT_CAPACITY = 64 * 1024;

    final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    final NativeAllocator nativeAllocator;
    final YierdisDbOwnedResources resources;
    final YierdisExpireIndex expires;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final StringRoot stringRoot;
    final ListRoot listRoot;
    final HashRoot hashRoot;
    final SetRoot setRoot;
    final ZSetRoot zsetRoot;
    final boolean keysStoredOffHeap;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            NativeAllocator nativeAllocator,
            YierdisDbOwnedResources resources,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            boolean keysStoredOffHeap
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.nativeAllocator = nativeAllocator;
        this.resources = resources;
        this.expires = expires;
        this.entries = entries;
        this.keyDirectory = keyDirectory;
        this.stringRoot = stringRoot;
        this.listRoot = listRoot;
        this.hashRoot = hashRoot;
        this.setRoot = setRoot;
        this.zsetRoot = zsetRoot;
        this.keysStoredOffHeap = keysStoredOffHeap;
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime
    ) {
        YierdisFfmMemoryRuntime resolvedRuntime = memoryRuntime;
        OffHeapAllocator resolvedAllocator = offHeapAllocator;
        boolean resolvedOwnsAllocator = ownsOffHeapAllocator;
        boolean resolvedOwnsRuntime = ownsMemoryRuntime;

        if (resolvedRuntime == null && resolvedAllocator == null) {
            resolvedRuntime = new YierdisFfmMemoryRuntime("db");
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
            resolvedOwnsRuntime = true;
        } else if (resolvedRuntime == null) {
            if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator foreignAllocator)) {
                throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
            }
            resolvedRuntime = foreignAllocator.memoryRuntime();
        } else if (resolvedAllocator == null) {
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
        } else if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator)) {
            throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
        }

        NativeAllocator nativeAllocator = new YierdisStableNativeAllocator(
                resolvedRuntime,
                sharedNativeSlotCapacity()
        );
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                resolvedRuntime,
                resolvedAllocator,
                nativeAllocator,
                resolvedOwnsRuntime,
                resolvedOwnsAllocator,
                true
        );
        EntryTable entries = new EntryTable(resolvedRuntime, nativeAllocator);
        YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(resolvedRuntime, "ffm-key");
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(blobStore);
        StringRoot stringRoot = new StringRoot(nativeAllocator);
        ListRoot listRoot = new ListRoot(resolvedRuntime);
        HashRoot hashRoot = new HashRoot(resolvedRuntime);
        SetRoot setRoot = new SetRoot(resolvedRuntime);
        ZSetRoot zsetRoot = new ZSetRoot(resolvedRuntime);
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                resolvedAllocator,
                nativeAllocator,
                resources,
                new YierdisFfmExpireIndex(blobStore),
                entries,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot,
                true
        );
    }

    static int sharedNativeSlotCapacity() {
        return ENTRY_TABLE_NATIVE_SLOT_CAPACITY + STRING_NATIVE_SLOT_CAPACITY;
    }
}
