package yier.bubu.redis.storage.memory.internal.expire;

public interface PreparedTtlMutation extends AutoCloseable {
    PreparedTtlMutation NONE = new PreparedTtlMutation() {
        @Override
        public long stagedNonNativeGrowthBytes() {
            return 0L;
        }

        @Override
        public void commit() {
        }

        @Override
        public void releaseSuperseded() {
        }

        @Override
        public void abort() {
        }
    };

    long stagedNonNativeGrowthBytes();

    void commit();

    void releaseSuperseded();

    void abort();

    @Override
    default void close() {
        abort();
    }
}
