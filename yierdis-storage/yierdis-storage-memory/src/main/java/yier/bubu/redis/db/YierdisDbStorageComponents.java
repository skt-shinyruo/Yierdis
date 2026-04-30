package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.db.memory.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.db.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

final class YierdisDbStorageComponents {
    final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    final YierdisDbOwnedResources resources;
    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    final boolean keysStoredOffHeap;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            YierdisDbOwnedResources resources,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            boolean keysStoredOffHeap
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.resources = resources;
        this.store = store;
        this.expires = expires;
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
        YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(resolvedRuntime, "ffm-key");
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                resolvedAllocator,
                resources,
                new YierdisFfmKeyspace<>(blobStore),
                new YierdisFfmExpireIndex(blobStore),
                true
        );
    }
}
