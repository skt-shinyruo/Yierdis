package yier.bubu.redis.db.offheap.api;

public interface YierdisOffHeapAllocator extends AutoCloseable {
    YierdisOffHeapBuf allocate(int capacity);

    long usedBytes();

    long maxBytes();

    YierdisOffHeapBackend backend();

    @Override
    void close();
}
