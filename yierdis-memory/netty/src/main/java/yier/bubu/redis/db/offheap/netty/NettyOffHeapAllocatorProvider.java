package yier.bubu.redis.db.offheap.netty;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;

public final class NettyOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.NETTY;
    }

    @Override
    public YierdisOffHeapAllocator create(long maxBytes) {
        return new YierdisNettyOffHeapAllocator(maxBytes);
    }
}

