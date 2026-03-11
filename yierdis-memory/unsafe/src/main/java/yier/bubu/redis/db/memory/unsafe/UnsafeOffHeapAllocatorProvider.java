package yier.bubu.redis.db.memory.unsafe;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;

public final class UnsafeOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.UNSAFE;
    }

    @Override
    public YierdisOffHeapAllocator create(long maxBytes) {
        return new YierdisUnsafeOffHeapAllocator(maxBytes);
    }
}

