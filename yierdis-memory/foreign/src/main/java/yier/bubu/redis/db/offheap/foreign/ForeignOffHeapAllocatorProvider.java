package yier.bubu.redis.db.offheap.foreign;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;

public final class ForeignOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.FOREIGN;
    }

    @Override
    public YierdisOffHeapAllocator create(long maxBytes) {
        return new YierdisForeignOffHeapAllocator(maxBytes);
    }
}

