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
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmSlabAllocator;
import yier.bubu.redis.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapAllocator;

public final class YierdisDbStorageComponents {
    final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    final YierdisDbOwnedResources resources;
    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final boolean keysStoredOffHeap;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            YierdisDbOwnedResources resources,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            boolean keysStoredOffHeap
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.resources = resources;
        this.store = store;
        this.expires = expires;
        this.entries = entries;
        this.keyDirectory = keyDirectory;
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

        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                resolvedRuntime,
                resolvedAllocator,
                resolvedOwnsRuntime,
                resolvedOwnsAllocator
        );
        EntryTable entries = new EntryTable(resolvedRuntime, new YierdisFfmSlabAllocator(resolvedRuntime), 64);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(entries);
        YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(resolvedRuntime, "ffm-key");
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                resolvedAllocator,
                resources,
                new YierdisFfmKeyspace<>(blobStore),
                new YierdisFfmExpireIndex(blobStore),
                entries,
                keyDirectory,
                true
        );
    }
}
