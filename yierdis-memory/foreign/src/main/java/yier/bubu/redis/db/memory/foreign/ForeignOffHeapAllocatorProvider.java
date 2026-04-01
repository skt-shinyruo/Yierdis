package yier.bubu.redis.db.memory.foreign;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

public final class ForeignOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.FOREIGN;
    }

    @Override
    public OffHeapAllocator create(long maxBytes) {
        return new YierdisForeignOffHeapAllocator(maxBytes);
    }
}
