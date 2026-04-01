package yier.bubu.redis.db.memory.netty;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

public final class NettyOffHeapAllocatorProvider implements YierdisOffHeapAllocatorProvider {
    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.NETTY;
    }

    @Override
    public OffHeapAllocator create(long maxBytes) {
        return new YierdisNettyOffHeapAllocator(maxBytes);
    }
}
