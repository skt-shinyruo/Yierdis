package yier.bubu.redis.db.offheap.unsafe;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;

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

