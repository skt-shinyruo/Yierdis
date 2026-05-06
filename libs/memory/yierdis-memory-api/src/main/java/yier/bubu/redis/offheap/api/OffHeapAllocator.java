package yier.bubu.redis.offheap.api;

public interface OffHeapAllocator extends AutoCloseable {
    OffHeapBuf allocate(int capacity);

    long usedBytes();

    /**
     * Hard cap in bytes, or 0 when unlimited.
     */
    long maxBytes();

    @Override
    void close();
}

