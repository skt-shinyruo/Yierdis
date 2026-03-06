package yier.bubu.redis.offheap.api;

/**
 * A raw off-heap memory block backed by an address with deterministic free via {@link #close()}.
 */
public interface OffHeapBlock extends AutoCloseable {
    long address();

    int capacity();

    @Override
    void close();
}

