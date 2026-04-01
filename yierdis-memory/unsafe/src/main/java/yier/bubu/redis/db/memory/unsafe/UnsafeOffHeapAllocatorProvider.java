package yier.bubu.redis.db.memory.unsafe;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

public final class UnsafeOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.UNSAFE;
    }

    @Override
    public OffHeapAllocator create(long maxBytes) {
        return new YierdisUnsafeOffHeapAllocator(maxBytes);
    }
}
