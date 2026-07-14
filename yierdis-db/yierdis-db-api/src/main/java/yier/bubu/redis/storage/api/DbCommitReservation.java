package yier.bubu.redis.storage.api;

/**
 * 提交可见性之前持有的 stream 容量令牌。
 */
public interface DbCommitReservation extends AutoCloseable {
    DbCommitReservation NOOP = new DbCommitReservation() {
        @Override
        public long reservedMemoryBytes() {
            return 0L;
        }

        @Override
        public boolean noop() {
            return true;
        }

        @Override
        public void close() {
        }
    };

    long reservedMemoryBytes();

    boolean noop();

    @Override
    void close();
}
