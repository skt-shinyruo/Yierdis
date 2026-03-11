package yier.bubu.redis.db.memory.foreign;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;

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

