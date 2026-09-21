package yier.bubu.redis.storage.memory.internal.ledger;

public interface PreparedMutation<T> extends AutoCloseable {
    long actualDeltaBytes();

    long stagedNonNativeGrowthBytes();

    T commit();

    void releaseSuperseded();

    void abort();

    @Override
    default void close() {
        abort();
    }
}
