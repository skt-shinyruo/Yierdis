package yier.bubu.redis.storage.api;

import java.util.Objects;
import yier.bubu.redis.common.command.ImmutableCommandRecord;

/**
 * DB 在提交边界使用的有界变更发布端口。
 */
public interface DbCommitPublisher {
    DbCommitPublisher NOOP = new DbCommitPublisher() {
        @Override
        public DbCommitReservation reserve(
                int dbIndex,
                DbCommitKind kind,
                ImmutableCommandRecord record,
                long committedMemoryDelta,
                long commitAttemptTimestampMillis
        ) {
            if (dbIndex < 0) {
                throw new IllegalArgumentException("dbIndex must be non-negative");
            }
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(record, "record");
            if (record.retainedMemoryBytes() < 0L) {
                throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
            }
            return DbCommitReservation.NOOP;
        }

        @Override
        public long publish(DbCommitReservation reservation) {
            Objects.requireNonNull(reservation, "reservation");
            return 0L;
        }

        @Override
        public void failAfterCommit(DbCommitReservation reservation) {
            Objects.requireNonNull(reservation, "reservation");
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public boolean available() {
            return true;
        }
    };

    DbCommitReservation reserve(
            int dbIndex,
            DbCommitKind kind,
            ImmutableCommandRecord record,
            long committedMemoryDelta,
            long commitAttemptTimestampMillis
    );

    /**
     * 对有效令牌执行无分配、无保留的发布转换。
     */
    long publish(DbCommitReservation reservation);

    /**
     * 提交开始后保留令牌对应记录并将 publisher 转为失败状态。
     */
    void failAfterCommit(DbCommitReservation reservation);

    boolean enabled();

    boolean available();
}
