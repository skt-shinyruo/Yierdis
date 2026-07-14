package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.command.CommandRecordView;

/**
 * sink callback 期间有效的已提交变更视图。
 */
public interface DbCommitEvent {
    long sequence();

    int dbIndex();

    DbCommitKind kind();

    CommandRecordView record();

    long committedMemoryDelta();

    long commitAttemptTimestampMillis();

    default boolean synthetic() {
        return kind() != DbCommitKind.USER;
    }
}
