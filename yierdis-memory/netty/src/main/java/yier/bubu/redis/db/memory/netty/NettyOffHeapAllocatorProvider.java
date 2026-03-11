package yier.bubu.redis.db.memory.netty;

import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocatorProvider;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;

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

