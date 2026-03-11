package yier.bubu.redis.db.memory.api;

public interface YierdisOffHeapAllocator extends yier.bubu.redis.offheap.api.OffHeapAllocator {
    YierdisOffHeapBuf allocate(int capacity);

    YierdisOffHeapBackend backend();
}
