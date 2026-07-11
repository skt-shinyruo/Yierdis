package yier.bubu.redis.memory.api;

public interface NativeAllocationScope extends AutoCloseable {
    NativeAllocationGrowth growth();

    void promote();

    void abort();

    @Override
    default void close() {
        abort();
    }
}
